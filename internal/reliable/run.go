package reliable

import (
	"context"

	"dataflow/internal/todo"
)

// Run reads messages from `in` until it is closed and processes each with h using cfg.Workers goroutines.
// It blocks until everything is done and returns the Result.
//
// For EACH message, loop:
//  1. Call h (attempt n = 1, 2, ...). A PANIC in h must be recovered and treated as a PERMANENT failure (a bug is not
//     going to fix itself on retry) -- the program must not crash and other messages must continue.
//  2. nil -> success (Succeeded++), done.
//  3. error wrapping ErrPermanent -> dead letter immediately, no retry.
//  4. any other error: if n < MaxAttempts, wait cfg.Backoff(n) via the Sleeper and try again; otherwise dead-letter it
//     with the last error. NO wait after the final attempt.
//
// Other rules:
//   - If ctx is cancelled, stop promptly and return what is done so far together with ctx.Err(). Messages that were in
//     flight are abandoned (neither succeeded nor dead-lettered) -- they were never finished, so the source must replay them.
//   - Result.DeadLetters is sorted by Message.ID so results are deterministic whatever the workers' timing.
//   - Workers share Result: protect it with a sync.Mutex (or give each worker its own and merge at the end).
func Run(ctx context.Context, in <-chan Message, h Handler, cfg Config) (Result, error) {
	return Result{}, todo.NotImplemented("Ch05: Run")
}
