package batch

import "context"

type Buffer[T any] struct {
	out     chan T
	dropped atomicCounter
}

func NewBuffer[T any](ctx context.Context, in <-chan T, capacity int, policy Policy) *Buffer[T] {
	capacity = max(capacity, 1)
	b := &Buffer[T]{out: make(chan T)}
	go func() {
		defer close(b.out)
		var queue []T
		inputOpen := true
		for {
			if !inputOpen && len(queue) == 0 {
				return // input ended and everything has been delivered
			}

			var send chan T // nil unless there is something to send
			var head T
			if len(queue) > 0 {
				send, head = b.out, queue[0]
			}
			var recv <-chan T // nil unless we are willing to read
			if inputOpen && (len(queue) < capacity || policy != Block) {
				recv = in
			}

			select {
			case <-ctx.Done():
				return
			case v, ok := <-recv:
				if !ok {
					inputOpen = false
					continue
				}
				switch {
				case len(queue) < capacity:
					queue = append(queue, v)
				case policy == DropNewest:
					b.dropped.n.Add(1)
				case policy == DropOldest:
					queue = append(queue[1:], v)
					b.dropped.n.Add(1)
				}
			case send <- head:
				queue = queue[1:]
			}
		}
	}()
	return b
}

func (b *Buffer[T]) Out() <-chan T { return b.out }

func (b *Buffer[T]) Dropped() int64 { return b.dropped.n.Load() }
