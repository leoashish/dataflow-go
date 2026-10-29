// Package dag is a small workflow orchestrator: tasks with dependencies, run in the right order, in parallel where possible.
package dag

import (
	"context"
	"errors"
)

var (
	// ErrCycle: the dependencies form a loop, so no valid order exists. The error text must show the loop ("a -> b -> a"). GIVEN.
	ErrCycle = errors.New("dag: dependency cycle")
	// ErrUnknownDependency: a task depends on a name that is not a task. GIVEN.
	ErrUnknownDependency = errors.New("dag: unknown dependency")
	// ErrDuplicateTask: two tasks share a name. GIVEN.
	ErrDuplicateTask = errors.New("dag: duplicate task name")
	// ErrInvalidTask: an empty name or a nil Run function. GIVEN.
	ErrInvalidTask = errors.New("dag: invalid task")
	// ErrBadParallelism: maxParallel below 1. GIVEN.
	ErrBadParallelism = errors.New("dag: maxParallel must be at least 1")
	// ErrRunFailed: Run finished but not every task succeeded (some failed, were skipped or were canceled). GIVEN.
	ErrRunFailed = errors.New("dag: run did not complete successfully")
)

// Task is one unit of work. GIVEN.
type Task struct {
	Name    string
	Deps    []string // names of tasks that must SUCCEED before this one may start
	Run     func(ctx context.Context) error
	Retries int // extra attempts after the first failure (0 = run once)
}

// Status is the outcome of one task in a Run. GIVEN.
type Status int

const (
	Succeeded Status = iota + 1
	Failed           // Run returned an error (or panicked) on every attempt
	Skipped          // never started because a (transitive) dependency Failed
	Canceled         // never started because the context was canceled
)

func (s Status) String() string {
	return [...]string{"unknown", "succeeded", "failed", "skipped", "canceled"}[s]
}

// Result is what a Run reports for each task. GIVEN.
type Result struct {
	Status   Status
	Err      error // the last error for Failed; ctx.Err() for Canceled; nil otherwise
	Attempts int   // how many times Run was called (0 for Skipped and Canceled)
}
