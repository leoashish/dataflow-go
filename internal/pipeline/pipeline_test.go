package pipeline_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/pipeline"
	"dataflow/internal/testutil"
)

// Chapter 01 -- pipelines with channels.   Run:  go test ./internal/pipeline -run Ch01 -race -v

func TestCh01_Generate_EmitsItemsInOrder_ThenCloses(t *testing.T) {
	testutil.NoLeaks(t)
	got := testutil.Drain(t, pipeline.Generate(context.Background(), 1, 2, 3, 4), testutil.Timeout)
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// no items: the channel is simply closed
	if empty := testutil.Drain(t, pipeline.Generate[int](context.Background()), testutil.Timeout); len(empty) != 0 {
		t.Fatalf("Generate with no items emitted %v", empty)
	}
}

func TestCh01_Generate_StopsEarly_WhenTheContextIsCancelled(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	items := make([]int, 1000)
	out := pipeline.Generate(ctx, items...)

	testutil.Receive(t, out, testutil.Timeout) // take one, then walk away
	cancel()
	rest := testutil.Drain(t, out, testutil.Timeout) // must CLOSE promptly even though nobody wants the items

	if len(rest) >= 999 {
		t.Fatalf("received %d more items after cancelling; the generator should have stopped", len(rest))
	}
}

func TestCh01_Map_TransformsEveryValue_InOrder(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	got := testutil.Drain(t, pipeline.Map(ctx, pipeline.Generate(ctx, 1, 2, 3), func(n int) string { return string(rune('a' + n - 1)) }), testutil.Timeout)
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh01_Map_StopsOnCancel_EvenWhenTheInputNeverCloses(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int) // we never close it: a stage that only does `range in` could never stop
	out := pipeline.Map(ctx, in, func(n int) int { return n * 10 })

	select { // first prove the stage is alive and working...
	case in <- 5:
	case <-time.After(testutil.Timeout):
		t.Fatal("the stage is not reading its input")
	}
	if got := testutil.Receive(t, out, testutil.Timeout); got != 50 {
		t.Fatalf("got %d, want 50", got)
	}

	cancel() // ...then cancel: it must stop although `in` is still open

	testutil.Drain(t, out, testutil.Timeout)
}

func TestCh01_Filter_KeepsOnlyMatchingValues(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	got := testutil.Drain(t, pipeline.Filter(ctx, pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6), func(n int) bool { return n%2 == 0 }), testutil.Timeout)
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh01_Take_ForwardsTheFirstN_ThenCloses(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	got := testutil.Drain(t, pipeline.Take(ctx, src, 3), testutil.Timeout)

	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	cancel() // the upstream Generate is still blocked trying to send item 4; cancelling frees it (NoLeaks checks)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if none := testutil.Drain(t, pipeline.Take(ctx2, pipeline.Generate(ctx2, 1, 2), 0), testutil.Timeout); len(none) != 0 {
		t.Fatalf("Take(0) emitted %v, want nothing", none)
	}
}

func TestCh01_FanOut_ProcessesEveryItem_Concurrently(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	var inside atomic.Int64
	allFourInside := make(chan struct{})
	var once sync.Once

	// Each call waits until FOUR calls are running at the same moment: only true parallelism gets past this.
	fn := func(n int) int {
		if inside.Add(1) == 4 {
			once.Do(func() { close(allFourInside) })
		}
		select {
		case <-allFourInside:
		case <-time.After(2 * time.Second):
			return -1
		}
		return n * n
	}
	got := testutil.Drain(t, pipeline.FanOut(ctx, pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8), 4, fn), 10*time.Second)

	slices.Sort(got)
	if want := []int{1, 4, 9, 16, 25, 36, 49, 64}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v (-1 means fewer than 4 calls ran at once)", got, want)
	}
}

func TestCh01_FanOut_ClosesTheOutput_OnlyAfterEveryWorkerIsDone(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	var finished atomic.Int64
	out := pipeline.FanOut(ctx, pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6), 3, func(n int) int {
		time.Sleep(time.Duration(n) * 5 * time.Millisecond)
		finished.Add(1)
		return n
	})

	got := testutil.Drain(t, out, testutil.Timeout)

	if len(got) != 6 || finished.Load() != 6 {
		t.Fatalf("received %d values, %d calls finished; want 6 and 6 (output closed too early?)", len(got), finished.Load())
	}
}

func TestCh01_FanOut_TreatsWorkersBelowOneAsOne(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	got := testutil.Drain(t, pipeline.FanOut(ctx, pipeline.Generate(ctx, 1, 2, 3), 0, func(n int) int { return n }), testutil.Timeout)
	slices.Sort(got)
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh01_FanIn_MergesEveryInput_ThenCloses(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	got := testutil.Drain(t, pipeline.FanIn(ctx, pipeline.Generate(ctx, 1, 2, 3), pipeline.Generate(ctx, 10, 20), pipeline.Generate(ctx, 100)), testutil.Timeout)
	slices.Sort(got)
	if want := []int{1, 2, 3, 10, 20, 100}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// no inputs at all: closes immediately
	if none := testutil.Drain(t, pipeline.FanIn[int](ctx), testutil.Timeout); len(none) != 0 {
		t.Fatalf("FanIn of nothing emitted %v", none)
	}
}

func TestCh01_FanIn_StopsOnCancel_EvenWhenInputsNeverClose(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	a, b := make(chan int), make(chan int) // never closed
	out := pipeline.FanIn(ctx, a, b)

	select { // prove it is alive first
	case a <- 1:
	case <-time.After(testutil.Timeout):
		t.Fatal("FanIn is not reading its inputs")
	}
	if got := testutil.Receive(t, out, testutil.Timeout); got != 1 {
		t.Fatalf("got %d, want 1", got)
	}

	cancel()

	testutil.Drain(t, out, testutil.Timeout)
}

func TestCh01_Pipeline_Composition(t *testing.T) {
	testutil.NoLeaks(t)
	ctx := context.Background()
	nums := pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	squares := pipeline.FanOut(ctx, nums, 3, func(n int) int { return n * n })
	even := pipeline.Filter(ctx, squares, func(n int) bool { return n%2 == 0 })

	got, err := pipeline.Drain(ctx, even)

	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	slices.Sort(got)
	if want := []int{4, 16, 36, 64, 100}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh01_Drain_ReturnsTheContextError_WhenCancelled(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	never := make(chan int)
	time.AfterFunc(30*time.Millisecond, cancel)

	got, err := pipeline.Drain(ctx, never)

	if !errors.Is(err, context.Canceled) || len(got) != 0 {
		t.Fatalf("Drain = (%v, %v), want (empty, context.Canceled)", got, err)
	}
}
