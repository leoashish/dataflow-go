package batch

import "context"

// Batches groups items from `in` into slices.
//
// Why batch? Databases, object stores and APIs are far faster per item when you hand them 500 rows at once than 500
// times one row. But waiting for a full batch can take forever on a quiet stream, so a batch is also flushed on a timer.
//
// A batch is emitted when
//   - it reaches Size items, or
//   - MaxWait has passed since its FIRST item arrived (start the timer when the first item of a batch arrives, not
//     before; with MaxWait == 0 never start a timer), or
//   - `in` is closed: flush the partial batch (if it is not empty), then close the output.
//
// More rules:
//   - NEVER emit an empty batch. Preserve the input order.
//   - Cancelling ctx stops everything at once and closes the output WITHOUT flushing the pending items.
//   - The output channel is unbuffered, so while the consumer is busy the batcher is blocked on its send and does not
//     read more input: back-pressure travels upstream.
//   - A timer that belongs to a batch that was already flushed (by size) must NOT flush the NEXT batch early: after every
//     flush, forget the old timer channel (set it to nil -- a nil channel in a select is never ready).
//
// Shape of the solution: one goroutine, a `current []T`, a `timeout <-chan time.Time` that is nil when no batch is
// pending, and a `select` over {ctx.Done(), in, timeout}.
func Batches[T any](ctx context.Context, in <-chan T, o Options) <-chan []T {
	return closedBatches[T]() // TODO Ch04
}
