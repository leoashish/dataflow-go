package batch

import "context"

// Buffer sits between a producer and a consumer and absorbs bursts. What happens when it fills up is a POLICY.
//
//   - capacity < 1 is treated as 1.
//   - Out() delivers items in order. It is closed when the input is closed AND everything buffered has been delivered
//     (or when ctx is cancelled: then buffered items are abandoned).
//   - Block: when full, the buffer stops reading from `in`, so the producer's sends block.
//   - DropNewest: when full, a new item is read and discarded (counted in Dropped); the producer is never blocked.
//   - DropOldest: when full, the oldest buffered item is discarded to make room for the new one (counted in Dropped).
//   - Dropped() is safe to call from any goroutine.
//
// The trick is ONE goroutine and ONE select that is built differently on every loop iteration, using NIL channels:
//
//	var out chan T          // stays nil (never ready) when the queue is empty: nothing to send
//	if len(queue) > 0 { out = b.out }
//	var inCh <-chan T       // stays nil when we must not read: queue full AND policy is Block
//	if len(queue) < capacity || policy != Block { inCh = in }
//	select { case <-ctx.Done(): ...; case inCh <- ...: ...; case out <- queue[0]: ... }
//
// A send or receive on a nil channel blocks forever, so that case is simply never chosen. When `in` closes, set your
// local `in` variable to nil (a closed channel is ALWAYS ready, so you would spin) and remember that input has ended.
type Buffer[T any] struct {
	out     chan T
	dropped atomicCounter
}

func NewBuffer[T any](ctx context.Context, in <-chan T, capacity int, policy Policy) *Buffer[T] {
	b := &Buffer[T]{out: make(chan T)}
	close(b.out) // TODO Ch04: start the goroutine described above; the real output must be closed by that goroutine
	return b
}

func (b *Buffer[T]) Out() <-chan T { return b.out }

func (b *Buffer[T]) Dropped() int64 { return b.dropped.n.Load() }
