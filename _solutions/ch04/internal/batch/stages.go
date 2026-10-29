package batch

import (
	"context"
	"time"
)

func Batches[T any](ctx context.Context, in <-chan T, o Options) <-chan []T {
	size := max(o.Size, 1)
	out := make(chan []T)
	go func() {
		defer close(out)
		var current []T
		var timeout <-chan time.Time // nil while no batch is pending: a nil channel is never ready

		flush := func() bool {
			if len(current) == 0 {
				return true
			}
			select {
			case out <- current:
				current = nil
				timeout = nil // forget the timer of the batch we just sent
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					flush() // the final partial batch
					return
				}
				current = append(current, v)
				if len(current) == 1 && o.MaxWait > 0 {
					timeout = o.after(o.MaxWait) // the clock starts when the FIRST item arrives
				}
				if len(current) >= size && !flush() {
					return
				}
			case <-timeout:
				if !flush() {
					return
				}
			}
		}
	}()
	return out
}
