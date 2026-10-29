package pipeline

import (
	"context"

	"dataflow/internal/todo"
)

// Every function here follows the SAME contract. Learn it once:
//
//  1. The function RETURNS a receive-only channel and starts goroutine(s) that feed it.
//  2. Whoever creates a channel and sends on it is the ONLY one that closes it -- and it closes it exactly once,
//     when it will never send again (use `defer close(out)` at the top of the goroutine).
//  3. Every send and every receive that could block is wrapped in a `select` that also watches ctx.Done(), so that
//     cancelling the context makes every goroutine return and close its channel -- no goroutine is left blocked
//     forever (a "goroutine leak").
//  4. Output channels are unbuffered: a slow consumer slows the producer down. That is BACK-PRESSURE, for free.

// Generate emits items in order, then closes the channel. If ctx is cancelled first it stops early.
func Generate[T any](ctx context.Context, items ...T) <-chan T {
	return closedChan[T]() // TODO Ch01
}

// Map applies fn to every value from in and emits the results IN THE SAME ORDER (one goroutine, so order is kept).
// Closes its output when `in` is closed or ctx is cancelled.
func Map[T, R any](ctx context.Context, in <-chan T, fn func(T) R) <-chan R {
	return closedChan[R]() // TODO Ch01
}

// Filter emits only the values for which keep returns true, in order.
func Filter[T any](ctx context.Context, in <-chan T, keep func(T) bool) <-chan T {
	return closedChan[T]() // TODO Ch01
}

// Take forwards the first n values and then closes its output. (n <= 0 closes immediately.)
// Note it stops READING from `in` after n values; the upstream stage is then blocked on its send until ctx is
// cancelled -- which is exactly why every stage must select on ctx.Done().
func Take[T any](ctx context.Context, in <-chan T, n int) <-chan T {
	return closedChan[T]() // TODO Ch01
}

// FanOut starts `workers` goroutines that all read from the same `in` channel and apply fn.
// Results are emitted in COMPLETION order (not input order). The output closes after ALL workers are done:
// that needs a sync.WaitGroup and one extra goroutine whose only job is `wg.Wait(); close(out)`.
// workers < 1 is treated as 1.
func FanOut[T, R any](ctx context.Context, in <-chan T, workers int, fn func(T) R) <-chan R {
	return closedChan[R]() // TODO Ch01
}

// FanIn merges several channels into one (arrival order). The output closes after ALL inputs are closed.
// With no inputs it closes immediately.
func FanIn[T any](ctx context.Context, ins ...<-chan T) <-chan T {
	return closedChan[T]() // TODO Ch01
}

// Drain reads `in` until it is closed and returns everything received. If ctx is cancelled first it returns what it
// has so far together with ctx.Err().
func Drain[T any](ctx context.Context, in <-chan T) ([]T, error) {
	return nil, todo.NotImplemented("Ch01: Drain")
}
