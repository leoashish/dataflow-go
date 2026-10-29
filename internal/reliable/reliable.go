// Package reliable processes messages so that transient failures are retried and hopeless ones are set aside.
package reliable

import (
	"context"
	"errors"
	"time"
)

// ErrPermanent is wrapped by a handler to say "retrying will never help" (malformed payload, unknown customer...).
// Detect it with errors.Is(err, ErrPermanent). GIVEN.
var ErrPermanent = errors.New("permanent failure")

// Message is one unit of work. ID identifies it (retries and replays carry the same ID).
type Message struct {
	ID      string
	Payload []byte
}

// Handler processes one message. It MAY BE CALLED MORE THAN ONCE for the same message (retries): at-least-once
// delivery means handlers must be safe to repeat. (Chapter 07 shows how.)
type Handler func(ctx context.Context, m Message) error

// Sleeper waits for d or until ctx ends (then it returns ctx.Err()). Tests inject a recording one. GIVEN.
type Sleeper interface {
	Sleep(ctx context.Context, d time.Duration) error
}

// SystemSleeper really sleeps. GIVEN.
type SystemSleeper struct{}

func (SystemSleeper) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Config tunes Run. Zero values are replaced by safe defaults (see each field).
type Config struct {
	Workers     int                                   // concurrent workers; < 1 means 1
	MaxAttempts int                                   // TOTAL handler calls per message; < 1 means 1
	Backoff     func(failedAttempt int) time.Duration // wait after failed attempt n (n = 1, 2, ...); nil means don't wait
	Sleeper     Sleeper                               // nil means SystemSleeper{}
}

// DeadLetter is a message that could not be processed: kept (with the reason) for inspection and replay.
type DeadLetter struct {
	Message  Message
	Err      error // the LAST error (for a panic: an error describing it)
	Attempts int   // how many times the handler was called
}

// Result summarises a run.
type Result struct {
	Succeeded     int
	DeadLetters   []DeadLetter // sorted by Message.ID
	TotalAttempts int          // total handler calls, retries included
}
