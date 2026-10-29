package reliable_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/reliable"
	"dataflow/internal/testutil"
)

// Chapter 05 -- retries, dead letters, at-least-once.   Run:  go test ./internal/reliable -run Ch05 -race -v

func feed(ids ...string) <-chan reliable.Message {
	ch := make(chan reliable.Message, len(ids))
	for _, id := range ids {
		ch <- reliable.Message{ID: id, Payload: []byte("payload-" + id)}
	}
	close(ch)
	return ch
}

func doubling(attempt int) time.Duration { return time.Duration(1<<(attempt-1)) * time.Second }

// callCounter counts handler calls per message ID.
type callCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func newCounter() *callCounter { return &callCounter{n: map[string]int{}} }

func (c *callCounter) inc(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[id]++
	return c.n[id]
}

func TestCh05_Run_ProcessesEveryMessage(t *testing.T) {
	testutil.NoLeaks(t)
	counter := newCounter()
	res, err := reliable.Run(context.Background(), feed("a", "b", "c", "d"), func(ctx context.Context, m reliable.Message) error {
		counter.inc(m.ID)
		return nil
	}, reliable.Config{Workers: 2, MaxAttempts: 3})

	if err != nil || res.Succeeded != 4 || len(res.DeadLetters) != 0 || res.TotalAttempts != 4 {
		t.Fatalf("res=%+v err=%v, want 4 succeeded, none dead, 4 attempts", res, err)
	}
}

func TestCh05_Run_RetriesATransientFailure_WithBackoff_ThenSucceeds(t *testing.T) {
	testutil.NoLeaks(t)
	sleeper := &testutil.RecordingSleeper{}
	counter := newCounter()
	res, err := reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error {
		if counter.inc(m.ID) < 3 {
			return errors.New("timeout")
		}
		return nil
	}, reliable.Config{MaxAttempts: 5, Backoff: doubling, Sleeper: sleeper})

	if err != nil || res.Succeeded != 1 || res.TotalAttempts != 3 || len(res.DeadLetters) != 0 {
		t.Fatalf("res=%+v err=%v, want success on the 3rd attempt", res, err)
	}
	if want := []time.Duration{time.Second, 2 * time.Second}; !slices.Equal(sleeper.Delays(), want) {
		t.Fatalf("delays = %v, want %v (Backoff(1), Backoff(2))", sleeper.Delays(), want)
	}
}

func TestCh05_Run_DeadLettersAMessage_AfterTheLastAttempt_WithoutAFinalSleep(t *testing.T) {
	testutil.NoLeaks(t)
	sleeper := &testutil.RecordingSleeper{}
	boom := errors.New("always broken")
	res, _ := reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error { return boom },
		reliable.Config{MaxAttempts: 3, Backoff: doubling, Sleeper: sleeper})

	if len(res.DeadLetters) != 1 || res.Succeeded != 0 {
		t.Fatalf("res=%+v, want one dead letter", res)
	}
	dl := res.DeadLetters[0]
	if dl.Message.ID != "a" || dl.Attempts != 3 || !errors.Is(dl.Err, boom) {
		t.Fatalf("dead letter = %+v, want message a after 3 attempts with the last error", dl)
	}
	if len(sleeper.Delays()) != 2 {
		t.Fatalf("slept %d times, want 2: no sleep after the final attempt", len(sleeper.Delays()))
	}
}

func TestCh05_Run_DoesNotRetryAPermanentError(t *testing.T) {
	testutil.NoLeaks(t)
	sleeper := &testutil.RecordingSleeper{}
	counter := newCounter()
	res, _ := reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error {
		counter.inc(m.ID)
		return fmt.Errorf("bad payload: %w", reliable.ErrPermanent)
	}, reliable.Config{MaxAttempts: 5, Backoff: doubling, Sleeper: sleeper})

	if counter.n["a"] != 1 || len(res.DeadLetters) != 1 || res.DeadLetters[0].Attempts != 1 || len(sleeper.Delays()) != 0 {
		t.Fatalf("calls=%d res=%+v delays=%v, want exactly one attempt and no waiting", counter.n["a"], res, sleeper.Delays())
	}
}

func TestCh05_Run_TurnsAPanicIntoADeadLetter_AndKeepsGoing(t *testing.T) {
	testutil.NoLeaks(t)
	counter := newCounter()
	res, err := reliable.Run(context.Background(), feed("a", "boom", "c"), func(ctx context.Context, m reliable.Message) error {
		if counter.inc(m.ID) > 0 && m.ID == "boom" {
			panic("handler bug")
		}
		return nil
	}, reliable.Config{Workers: 2, MaxAttempts: 5, Backoff: doubling, Sleeper: &testutil.RecordingSleeper{}})

	if err != nil || res.Succeeded != 2 || len(res.DeadLetters) != 1 {
		t.Fatalf("res=%+v err=%v, want 2 succeeded and the panicking message dead-lettered", res, err)
	}
	if counter.n["boom"] != 1 {
		t.Fatalf("a panicking message was called %d times; a panic is a bug, not a transient fault: no retry", counter.n["boom"])
	}
}

func TestCh05_Run_ReturnsDeadLettersSortedByID(t *testing.T) {
	testutil.NoLeaks(t)
	res, _ := reliable.Run(context.Background(), feed("d", "b", "e", "a", "c"), func(ctx context.Context, m reliable.Message) error {
		return reliable.ErrPermanent
	}, reliable.Config{Workers: 4, MaxAttempts: 1})

	var ids []string
	for _, dl := range res.DeadLetters {
		ids = append(ids, dl.Message.ID)
	}
	if !slices.Equal(ids, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("dead letters in order %v, want sorted by ID", ids)
	}
}

func TestCh05_Run_UsesAllTheWorkers(t *testing.T) {
	testutil.NoLeaks(t)
	var inside atomic.Int64
	allIn := make(chan struct{})
	var once sync.Once
	res, err := reliable.Run(context.Background(), feed("1", "2", "3", "4", "5", "6", "7", "8"), func(ctx context.Context, m reliable.Message) error {
		if inside.Add(1) == 4 {
			once.Do(func() { close(allIn) })
		}
		select {
		case <-allIn:
			return nil
		case <-time.After(2 * time.Second):
			return reliable.ErrPermanent
		}
	}, reliable.Config{Workers: 4, MaxAttempts: 1})

	if err != nil || res.Succeeded != 8 {
		t.Fatalf("res=%+v err=%v, want 8 succeeded (all 4 workers must run at the same time)", res, err)
	}
}

func TestCh05_Run_WithoutABackoff_NeverSleeps(t *testing.T) {
	testutil.NoLeaks(t)
	sleeper := &testutil.RecordingSleeper{}
	res, _ := reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error { return errors.New("x") },
		reliable.Config{MaxAttempts: 3, Sleeper: sleeper})
	if len(sleeper.Delays()) != 0 || res.TotalAttempts != 3 {
		t.Fatalf("delays=%v attempts=%d, want no sleeping and 3 attempts", sleeper.Delays(), res.TotalAttempts)
	}
}

func TestCh05_Run_ZeroValuesMeanOne(t *testing.T) {
	testutil.NoLeaks(t)
	counter := newCounter()
	res, _ := reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error {
		counter.inc(m.ID)
		return errors.New("x")
	}, reliable.Config{}) // Workers 0 -> 1, MaxAttempts 0 -> 1
	if counter.n["a"] != 1 || len(res.DeadLetters) != 1 {
		t.Fatalf("calls=%d res=%+v, want one attempt then a dead letter", counter.n["a"], res)
	}
}

func TestCh05_Run_StopsPromptlyOnCancel_AndReturnsTheContextError(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() {
		_, err := reliable.Run(ctx, feed("a", "b"), func(ctx context.Context, m reliable.Message) error {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}, reliable.Config{Workers: 2, MaxAttempts: 5, Backoff: doubling, Sleeper: &testutil.RecordingSleeper{}})
		done <- err
	}()
	testutil.WaitClosed(t, started, testutil.Timeout)

	cancel()

	if err := testutil.Receive(t, done, testutil.Timeout); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestCh05_Run_InterruptedMessagesAreNeitherSucceededNorDeadLettered(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var once sync.Once
	type out struct {
		res reliable.Result
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := reliable.Run(ctx, feed("a"), func(ctx context.Context, m reliable.Message) error {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ctx.Err()
		}, reliable.Config{MaxAttempts: 3})
		done <- out{res, err}
	}()
	testutil.WaitClosed(t, started, testutil.Timeout)
	cancel()

	o := testutil.Receive(t, done, testutil.Timeout)

	if o.res.Succeeded != 0 || len(o.res.DeadLetters) != 0 {
		t.Fatalf("res=%+v: a message cut short by shutdown was not finished, so it is not a failure to record", o.res)
	}
}

func TestCh05_Handlers_SeeTheSameMessageAgainOnRetry_SoTheyMustBeRepeatSafe(t *testing.T) {
	testutil.NoLeaks(t)
	counter := newCounter()
	var mu sync.Mutex
	effects := map[string]int{} // a non-idempotent side effect: "send an email"
	_, _ = reliable.Run(context.Background(), feed("a"), func(ctx context.Context, m reliable.Message) error {
		mu.Lock()
		effects[m.ID]++ // the side effect happens BEFORE the failure...
		mu.Unlock()
		if counter.inc(m.ID) < 3 {
			return errors.New("timeout after sending") // ...so the retry repeats it
		}
		return nil
	}, reliable.Config{MaxAttempts: 5})

	if effects["a"] != 3 {
		t.Fatalf("the side effect ran %d times; this test documents that retries repeat it (3). Chapter 07 shows how to make it safe", effects["a"])
	}
}
