package batch_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/batch"
	"dataflow/internal/testutil"
)

// Chapter 04 -- batching and back-pressure.   Run:  go test ./internal/batch -run Ch04 -race -v

// fakeTimers is an injectable timer factory: each call to After is announced on `requests` and the test decides when
// (whether) it fires.
type timerReq struct {
	d    time.Duration
	fire chan time.Time
}

type fakeTimers struct{ requests chan timerReq }

func newFakeTimers() *fakeTimers { return &fakeTimers{requests: make(chan timerReq, 64)} }

func (f *fakeTimers) After(d time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	select {
	case f.requests <- timerReq{d: d, fire: c}: // announced; the test may fire it
	default: // nobody is watching the requests (a test that does not care about timers): just hand out a timer
	}
	return c
}

func send[T any](t *testing.T, ch chan<- T, vals ...T) {
	t.Helper()
	for _, v := range vals {
		select {
		case ch <- v:
		case <-time.After(testutil.Timeout):
			t.Fatalf("timed out sending %v: the batcher is not reading its input", v)
		}
	}
}

// trySend is like send but safe to call from a goroutine other than the test's: it returns false instead of failing the
// test, and it gives up when ctx ends (producers must never outlive their test).
func trySend[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(testutil.Timeout):
		return false
	}
}

func TestCh04_Batches_FlushesWhenTheBatchIsFull(t *testing.T) {
	testutil.NoLeaks(t)
	ft := newFakeTimers()
	in := make(chan int)
	out := batch.Batches(context.Background(), in, batch.Options{Size: 3, MaxWait: time.Second, After: ft.After})

	pctx, pcancel := context.WithCancel(context.Background())
	t.Cleanup(pcancel)
	go func() {
		defer close(in)
		for i := 1; i <= 7; i++ {
			if !trySend(pctx, in, i) {
				if pctx.Err() == nil {
					t.Error("the batcher stopped reading its input")
				}
				return
			}
		}
	}()

	var got [][]int
	for b := range out {
		got = append(got, b)
	}
	want := [][]int{{1, 2, 3}, {4, 5, 6}, {7}}
	if len(got) != 3 || !slices.Equal(got[0], want[0]) || !slices.Equal(got[1], want[1]) || !slices.Equal(got[2], want[2]) {
		t.Fatalf("got %v, want %v (full batches, then the remainder when the input closes)", got, want)
	}
}

func TestCh04_Batches_FlushesAPartialBatch_WhenTheTimerFires(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ft := newFakeTimers()
	in := make(chan int)
	out := batch.Batches(ctx, in, batch.Options{Size: 100, MaxWait: 5 * time.Second, After: ft.After})

	send(t, in, 1, 2)
	req := testutil.Receive(t, ft.requests, testutil.Timeout)
	if req.d != 5*time.Second {
		t.Fatalf("timer requested for %v, want MaxWait (5s)", req.d)
	}
	testutil.NoBlock(t, out) // nothing is flushed before the timer fires
	req.fire <- time.Now()

	if got := testutil.Receive(t, out, testutil.Timeout); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestCh04_Batches_DoesNotStartATimer_UntilTheFirstItemArrives(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ft := newFakeTimers()
	in := make(chan int)
	_ = batch.Batches(ctx, in, batch.Options{Size: 10, MaxWait: time.Second, After: ft.After})

	time.Sleep(30 * time.Millisecond)
	testutil.NoBlock(t, ft.requests) // an idle batcher must not be ticking: it would flush empty batches
	send(t, in, 1)
	testutil.Receive(t, ft.requests, testutil.Timeout)
}

func TestCh04_Batches_IgnoresTheStaleTimerOfAnAlreadyFlushedBatch(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ft := newFakeTimers()
	in := make(chan int)
	out := batch.Batches(ctx, in, batch.Options{Size: 2, MaxWait: time.Second, After: ft.After})

	send(t, in, 1, 2) // flushed by SIZE; the timer started by item 1 is still pending
	stale := testutil.Receive(t, ft.requests, testutil.Timeout)
	testutil.Receive(t, out, testutil.Timeout)

	send(t, in, 3) // a NEW batch begins and gets its own timer
	fresh := testutil.Receive(t, ft.requests, testutil.Timeout)
	stale.fire <- time.Now() // the old timer fires late: it must not cut the new batch short

	time.Sleep(30 * time.Millisecond)
	testutil.NoBlock(t, out)
	fresh.fire <- time.Now()
	if got := testutil.Receive(t, out, testutil.Timeout); !slices.Equal(got, []int{3}) {
		t.Fatalf("got %v, want [3]", got)
	}
}

func TestCh04_Batches_FlushesThePartialBatch_WhenTheInputCloses(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	out := batch.Batches(context.Background(), in, batch.Options{Size: 10, MaxWait: time.Hour, After: newFakeTimers().After})
	pctx, pcancel := context.WithCancel(context.Background())
	t.Cleanup(pcancel)
	go func() { defer close(in); trySend(pctx, in, 1); trySend(pctx, in, 2) }()

	got := testutil.Drain(t, out, testutil.Timeout)

	if len(got) != 1 || !slices.Equal(got[0], []int{1, 2}) {
		t.Fatalf("got %v, want [[1 2]]", got)
	}

	// and an input that closes with nothing in it must not produce an empty batch
	empty := make(chan int)
	close(empty)
	if none := testutil.Drain(t, batch.Batches(context.Background(), empty, batch.Options{Size: 3}), testutil.Timeout); len(none) != 0 {
		t.Fatalf("got %v, want no batches for an empty input", none)
	}
}

func TestCh04_Batches_WithoutMaxWait_NeverCallsTheTimer(t *testing.T) {
	testutil.NoLeaks(t)
	ft := newFakeTimers()
	in := make(chan int)
	out := batch.Batches(context.Background(), in, batch.Options{Size: 2, After: ft.After})
	pctx, pcancel := context.WithCancel(context.Background())
	t.Cleanup(pcancel)
	go func() { defer close(in); trySend(pctx, in, 1); trySend(pctx, in, 2); trySend(pctx, in, 3) }()

	got := testutil.Drain(t, out, testutil.Timeout)
	if len(got) != 2 || !slices.Equal(got[0], []int{1, 2}) || !slices.Equal(got[1], []int{3}) {
		t.Fatalf("got %v, want [[1 2] [3]]", got)
	}

	testutil.NoBlock(t, ft.requests)
}

func TestCh04_Batches_StopsOnCancel_WithoutFlushing(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	out := batch.Batches(ctx, in, batch.Options{Size: 10, MaxWait: time.Hour, After: newFakeTimers().After})
	send(t, in, 1, 2)

	cancel()

	if got := testutil.Drain(t, out, testutil.Timeout); len(got) != 0 {
		t.Fatalf("got %v; a cancelled batcher must close its output without flushing", got)
	}
}

func TestCh04_Batches_BackPressure_AStalledConsumerStopsTheProducer(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan int)
	_ = batch.Batches(ctx, in, batch.Options{Size: 2, After: newFakeTimers().After}) // nobody reads the output

	var sent atomic.Int64
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case in <- i:
				sent.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)

	if got := sent.Load(); got != 2 {
		t.Fatalf("the producer got %d items through; the first batch (2 items) is stuck waiting for the consumer, so it should be exactly 2", got)
	}
}

func TestCh04_Batches_KeepsEveryItemInOrder(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	out := batch.Batches(context.Background(), in, batch.Options{Size: 7, MaxWait: time.Hour, After: newFakeTimers().After})
	pctx, pcancel := context.WithCancel(context.Background())
	t.Cleanup(pcancel)
	go func() {
		defer close(in)
		for i := 0; i < 1000; i++ {
			if !trySend(pctx, in, i) {
				if pctx.Err() == nil {
					t.Errorf("the batcher stopped reading its input at item %d", i)
				}
				return
			}
		}
	}()

	var all []int
	var sizes []int
	for b := range out {
		all = append(all, b...)
		sizes = append(sizes, len(b))
	}

	if len(all) != 1000 || !slices.IsSorted(all) {
		t.Fatalf("lost or reordered items: got %d", len(all))
	}
	for i, s := range sizes[:len(sizes)-1] {
		if s != 7 {
			t.Fatalf("batch %d has %d items, want 7 (only the last one may be smaller)", i, s)
		}
	}
}

// ---- elastic buffer ----

func feed(t *testing.T, ch chan<- int, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		send(t, ch, i)
	}
}

func TestCh04_Buffer_DropNewest_KeepsTheOldestItems(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	b := batch.NewBuffer(context.Background(), in, 3, batch.DropNewest)

	feed(t, in, 10) // nobody is consuming, yet the producer is never blocked
	close(in)
	got := testutil.Drain(t, b.Out(), testutil.Timeout)

	if !slices.Equal(got, []int{1, 2, 3}) || b.Dropped() != 7 {
		t.Fatalf("got %v dropped=%d, want [1 2 3] and 7 dropped", got, b.Dropped())
	}
}

func TestCh04_Buffer_DropOldest_KeepsTheNewestItems(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	b := batch.NewBuffer(context.Background(), in, 3, batch.DropOldest)

	feed(t, in, 10)
	close(in)
	got := testutil.Drain(t, b.Out(), testutil.Timeout)

	if !slices.Equal(got, []int{8, 9, 10}) || b.Dropped() != 7 {
		t.Fatalf("got %v dropped=%d, want [8 9 10] and 7 dropped", got, b.Dropped())
	}
}

func TestCh04_Buffer_Block_SlowsTheProducerDown_AndLosesNothing(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	b := batch.NewBuffer(context.Background(), in, 3, batch.Block)

	var sent atomic.Int64
	go func() {
		defer close(in)
		for i := 1; i <= 10; i++ {
			in <- i
			sent.Add(1)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	if got := sent.Load(); got != 3 {
		t.Fatalf("%d items were accepted while nobody consumed; a full Block buffer of capacity 3 accepts exactly 3", got)
	}

	got := testutil.Drain(t, b.Out(), testutil.Timeout)
	if !slices.Equal(got, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) || b.Dropped() != 0 {
		t.Fatalf("got %v dropped=%d, want everything in order and nothing dropped", got, b.Dropped())
	}
}

func TestCh04_Buffer_DeliversWhatIsBuffered_AfterTheInputCloses(t *testing.T) {
	testutil.NoLeaks(t)
	in := make(chan int)
	b := batch.NewBuffer(context.Background(), in, 10, batch.Block)
	feed(t, in, 4)
	close(in)

	if got := testutil.Drain(t, b.Out(), testutil.Timeout); !slices.Equal(got, []int{1, 2, 3, 4}) {
		t.Fatalf("got %v, want [1 2 3 4]", got)
	}
}

func TestCh04_Buffer_ClosesItsOutput_OnCancel(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int) // never closed
	b := batch.NewBuffer(ctx, in, 3, batch.Block)
	feed(t, in, 2)

	cancel()

	testutil.Drain(t, b.Out(), testutil.Timeout)
}
