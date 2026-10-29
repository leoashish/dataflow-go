package conc_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/conc"
	"dataflow/internal/testutil"
)

// Chapter 02 -- structured concurrency.   Run:  go test ./internal/conc -run Ch02 -race -v

// wait is g.Wait with a deadline: if a function started with Go never finishes, a bare Wait would freeze `go test` for its
// default ten minutes instead of telling you what is wrong.
func wait(t *testing.T, g *conc.Group) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- g.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return within 10s: a function started with Go never finished (is a goroutine stuck, e.g. releasing a semaphore slot it never acquired?)")
		return nil
	}
}

func TestCh02_Group_RunsEveryFunction(t *testing.T) {
	testutil.NoLeaks(t)
	g, _ := conc.NewGroup(context.Background())
	var n atomic.Int64
	for range 50 {
		g.Go(func() error { n.Add(1); return nil })
	}
	if err := wait(t, g); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n.Load() != 50 {
		t.Fatalf("%d functions ran, want 50 (Wait must wait for all of them)", n.Load())
	}
}

func TestCh02_Group_WithNoFunctions_WaitReturnsNil(t *testing.T) {
	g, _ := conc.NewGroup(context.Background())
	if err := wait(t, g); err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
}

func TestCh02_Group_WaitReturnsTheFirstErrorThatHappened(t *testing.T) {
	testutil.NoLeaks(t)
	g, ctx := conc.NewGroup(context.Background())
	errFirst, errLater := errors.New("first"), errors.New("later")

	g.Go(func() error { <-ctx.Done(); return errLater }) // started FIRST, but fails LAST (after the cancellation)
	g.Go(func() error { return errFirst })

	if err := wait(t, g); !errors.Is(err, errFirst) {
		t.Fatalf("Wait = %v, want the error that happened first in time", err)
	}
}

func TestCh02_Group_CancelsTheContext_WhenAFunctionFails(t *testing.T) {
	testutil.NoLeaks(t)
	g, ctx := conc.NewGroup(context.Background())
	sawCancel := make(chan struct{})
	g.Go(func() error { <-ctx.Done(); close(sawCancel); return nil })
	g.Go(func() error { return errors.New("boom") })

	_ = wait(t, g)

	testutil.WaitClosed(t, sawCancel, testutil.Timeout)
}

func TestCh02_Group_WaitCancelsTheContext_EvenWithoutErrors(t *testing.T) {
	g, ctx := conc.NewGroup(context.Background())
	g.Go(func() error { return nil })
	_ = wait(t, g)
	if ctx.Err() == nil {
		t.Fatal("the derived context should be cancelled once Wait returns (it releases its resources)")
	}
}

func TestCh02_Group_SetLimit_BoundsTheParallelism_ButUsesAllOfIt(t *testing.T) {
	testutil.NoLeaks(t)
	g, _ := conc.NewGroup(context.Background())
	g.SetLimit(3)
	var running, maxSeen atomic.Int64
	for range 12 {
		g.Go(func() error {
			now := running.Add(1)
			for {
				seen := maxSeen.Load()
				if now <= seen || maxSeen.CompareAndSwap(seen, now) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			running.Add(-1)
			return nil
		})
	}
	_ = wait(t, g)
	if maxSeen.Load() != 3 {
		t.Fatalf("at most %d functions ran at once, want exactly 3", maxSeen.Load())
	}
}

func TestCh02_Group_Go_BlocksWhileTheLimitIsReached(t *testing.T) {
	testutil.NoLeaks(t)
	g, _ := conc.NewGroup(context.Background())
	g.SetLimit(1)
	gate := make(chan struct{})
	g.Go(func() error { <-gate; return nil })

	returned := make(chan struct{})
	go func() { g.Go(func() error { return nil }); close(returned) }() // the 2nd Go must wait for a free slot

	select {
	case <-returned:
		t.Fatal("Go returned while the limit was reached; it should block (back-pressure)")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	testutil.WaitClosed(t, returned, testutil.Timeout)
	_ = wait(t, g)
}

func TestCh02_Semaphore_LimitsHolders(t *testing.T) {
	testutil.NoLeaks(t)
	sem := conc.NewSemaphore(2)
	var holders, maxSeen atomic.Int64
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.Background()); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			now := holders.Add(1)
			for {
				seen := maxSeen.Load()
				if now <= seen || maxSeen.CompareAndSwap(seen, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			holders.Add(-1)
			sem.Release()
		}()
	}
	wg.Wait()
	if maxSeen.Load() != 2 {
		t.Fatalf("saw %d simultaneous holders, want exactly 2", maxSeen.Load())
	}
}

func TestCh02_Semaphore_AcquireBlocksWhenFull_AndReleaseUnblocksIt(t *testing.T) {
	testutil.NoLeaks(t)
	sem := conc.NewSemaphore(1)
	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- sem.Acquire(context.Background()) }()

	select {
	case <-got:
		t.Fatal("Acquire returned while the semaphore was full")
	case <-time.After(50 * time.Millisecond):
	}
	sem.Release()
	if err := testutil.Receive(t, got, testutil.Timeout); err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
}

func TestCh02_Semaphore_AcquireGivesUp_WhenTheContextEnds(t *testing.T) {
	sem := conc.NewSemaphore(1)
	_ = sem.Acquire(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if err := sem.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire = %v, want context.DeadlineExceeded", err)
	}
	sem.Release() // the failed Acquire must NOT have taken a slot, so exactly one Release balances the books
	if !sem.TryAcquire() {
		t.Fatal("a slot should be free again")
	}
}

func TestCh02_Semaphore_TryAcquire_NeverBlocks(t *testing.T) {
	sem := conc.NewSemaphore(2)
	if !sem.TryAcquire() || !sem.TryAcquire() {
		t.Fatal("the first two TryAcquire calls should succeed")
	}
	if sem.TryAcquire() {
		t.Fatal("a third TryAcquire should fail: the semaphore is full")
	}
}

func TestCh02_Semaphore_ReleasingMoreThanAcquiredPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	conc.NewSemaphore(1).Release()
}

func TestCh02_ParallelMap_ReturnsResultsInInputOrder(t *testing.T) {
	testutil.NoLeaks(t)
	items := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	got, err := conc.ParallelMap(context.Background(), items, 10, func(ctx context.Context, i int) (int, error) {
		time.Sleep(time.Duration(10-i) * 4 * time.Millisecond) // item 0 finishes LAST
		return i * 2, nil
	})
	if err != nil {
		t.Fatalf("ParallelMap: %v", err)
	}
	if want := []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh02_ParallelMap_RespectsTheWorkerLimit(t *testing.T) {
	testutil.NoLeaks(t)
	var running, maxSeen atomic.Int64
	_, err := conc.ParallelMap(context.Background(), make([]int, 20), 3, func(ctx context.Context, _ int) (int, error) {
		now := running.Add(1)
		for {
			seen := maxSeen.Load()
			if now <= seen || maxSeen.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		running.Add(-1)
		return 0, nil
	})
	if err != nil || maxSeen.Load() != 3 {
		t.Fatalf("err=%v, max concurrent=%d, want nil and exactly 3", err, maxSeen.Load())
	}
}

func TestCh02_ParallelMap_FailsFast_WithTheRootCause(t *testing.T) {
	testutil.NoLeaks(t)
	root := errors.New("root cause")
	var started atomic.Int64
	_, err := conc.ParallelMap(context.Background(), make([]int, 100), 2, func(ctx context.Context, _ int) (int, error) {
		if started.Add(1) == 1 {
			return 0, root
		}
		<-ctx.Done() // everybody else waits to be cancelled
		return 0, errors.New("follow-up")
	})
	if !errors.Is(err, root) {
		t.Fatalf("error = %v, want the root cause", err)
	}
	if started.Load() > 2 {
		t.Fatalf("%d calls started although the first one failed; with 2 workers at most 2 can be in flight", started.Load())
	}
}

func TestCh02_ParallelMap_ReturnsTheContextError_AndNeverCallsFn_WhenAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int64
	_, err := conc.ParallelMap(ctx, []int{1, 2, 3}, 2, func(ctx context.Context, i int) (int, error) { calls.Add(1); return i, nil })
	if !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d, want context.Canceled and no calls", err, calls.Load())
	}
}

func TestCh02_ParallelMap_ReturnsAnEmptyNonNilSlice_ForEmptyInput(t *testing.T) {
	got, err := conc.ParallelMap(context.Background(), []int{}, 3, func(ctx context.Context, i int) (int, error) { return i, nil })
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("got (%v, %v), want an empty non-nil slice and nil", got, err)
	}
}
