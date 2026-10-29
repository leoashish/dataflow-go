package pipeline

import (
	"context"
	"sync"
)

func Generate[T any](ctx context.Context, items ...T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, item := range items {
			select {
			case out <- item:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func Map[T, R any](ctx context.Context, in <-chan T, fn func(T) R) <-chan R {
	out := make(chan R)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- fn(v):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func Filter[T any](ctx context.Context, in <-chan T, keep func(T) bool) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				if !keep(v) {
					continue
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func Take[T any](ctx context.Context, in <-chan T, n int) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func FanOut[T, R any](ctx context.Context, in <-chan T, workers int, fn func(T) R) <-chan R {
	if workers < 1 {
		workers = 1
	}
	out := make(chan R)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- fn(v):
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() { // the closer: only after EVERY worker has finished may the shared output be closed
		wg.Wait()
		close(out)
	}()
	return out
}

func FanIn[T any](ctx context.Context, ins ...<-chan T) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup
	for _, in := range ins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func Drain[T any](ctx context.Context, in <-chan T) ([]T, error) {
	var out []T
	for {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case v, ok := <-in:
			if !ok {
				return out, nil
			}
			out = append(out, v)
		}
	}
}
