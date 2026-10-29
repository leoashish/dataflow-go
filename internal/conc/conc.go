// Package conc holds the structured-concurrency tools: a goroutine group with a limit, and a semaphore.
package conc

import (
	"context"

	"dataflow/internal/todo"
)

// Group runs functions in goroutines and collects the FIRST error -- a hand-made version of
// golang.org/x/sync/errgroup (which is not in the standard library).
//
// Contract:
//   - NewGroup returns the group and a context derived from ctx. That derived context is CANCELLED the first time
//     any function returns an error, and also when Wait returns. Functions should watch it to stop early.
//   - Go(fn) starts fn in a new goroutine. If a limit is set (SetLimit) and that many functions are already running,
//     Go BLOCKS until one finishes (this is how a group limits parallelism and gives back-pressure to the caller).
//   - SetLimit(n): at most n functions run at once; n <= 0 means "no limit". Call it before the first Go.
//   - Wait blocks until every started function has returned, cancels the derived context, and returns the FIRST error
//     that happened (first in TIME, not first in the order of Go calls), or nil.
//
// Tools: sync.WaitGroup, sync.Once (to remember only the first error), and a buffered channel used as a semaphore
// (send = take a slot, receive = free a slot).
type Group struct{}

func NewGroup(ctx context.Context) (*Group, context.Context) { return &Group{}, ctx } // TODO Ch02

func (g *Group) SetLimit(n int) {} // TODO Ch02

func (g *Group) Go(fn func() error) {} // TODO Ch02

func (g *Group) Wait() error { return todo.NotImplemented("Ch02: Group.Wait") }

// Semaphore limits how many holders there can be at once.
//   - Acquire blocks until a slot is free or ctx ends (then it returns ctx.Err() and takes nothing).
//   - TryAcquire never blocks: true if it got a slot.
//   - Release frees a slot. Releasing more than you acquired is a bug: panic with a clear message.
type Semaphore struct{}

func NewSemaphore(n int) *Semaphore { return &Semaphore{} } // TODO Ch02

func (s *Semaphore) Acquire(ctx context.Context) error {
	return todo.NotImplemented("Ch02: Semaphore.Acquire")
}

func (s *Semaphore) TryAcquire() bool { return false } // TODO Ch02

func (s *Semaphore) Release() {} // TODO Ch02

// ParallelMap applies fn to every item with at most `workers` calls at a time and returns the results IN INPUT ORDER.
// Build it on top of Group (that is the point of the exercise):
//   - the first error cancels the context handed to the other calls, no new calls are started, and that first error
//     is returned (results are nil);
//   - if the CALLER's ctx is cancelled, return its error; an already-cancelled ctx means fn is never called;
//   - never return while a call is still running;
//   - empty input -> an empty, non-nil slice.
func ParallelMap[T, R any](ctx context.Context, items []T, workers int, fn func(ctx context.Context, item T) (R, error)) ([]R, error) {
	return nil, todo.NotImplemented("Ch02: ParallelMap")
}
