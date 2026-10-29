package conc

import (
	"context"
	"sync"
)

type Group struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	sem     chan struct{}
	errOnce sync.Once
	err     error
}

func NewGroup(ctx context.Context) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{cancel: cancel}, ctx
}

func (g *Group) SetLimit(n int) {
	if n <= 0 {
		g.sem = nil
		return
	}
	g.sem = make(chan struct{}, n)
}

func (g *Group) Go(fn func() error) {
	if g.sem != nil {
		g.sem <- struct{}{} // blocks while `limit` functions are running: back-pressure on the caller
	}
	g.wg.Add(1)
	go func() {
		defer func() {
			if g.sem != nil {
				<-g.sem
			}
			g.wg.Done()
		}()
		if err := fn(); err != nil {
			g.errOnce.Do(func() { // only the FIRST error is kept; cancelling tells the others to stop
				g.err = err
				g.cancel()
			})
		}
	}()
}

func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

type Semaphore struct {
	slots chan struct{}
}

func NewSemaphore(n int) *Semaphore {
	if n < 1 {
		n = 1
	}
	return &Semaphore{slots: make(chan struct{}, n)}
}

func (s *Semaphore) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Semaphore) TryAcquire() bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Semaphore) Release() {
	select {
	case <-s.slots:
	default:
		panic("conc: Semaphore.Release called more times than Acquire")
	}
}

func ParallelMap[T, R any](ctx context.Context, items []T, workers int, fn func(ctx context.Context, item T) (R, error)) ([]R, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results := make([]R, len(items))
	g, gctx := NewGroup(ctx)
	g.SetLimit(workers)
	for i, item := range items {
		if gctx.Err() != nil {
			break // somebody failed or the caller cancelled: start nothing new
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil // we waited for a free slot, and meanwhile somebody failed or the caller cancelled: don't START
			}
			r, err := fn(gctx, item)
			if err != nil {
				return err
			}
			results[i] = r // each index is written by exactly one goroutine
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil { // the caller's context, not the group's (Wait cancels that one)
		return nil, err
	}
	return results, nil
}
