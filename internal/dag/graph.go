package dag

import "dataflow/internal/todo"

// Graph is a validated set of tasks.
//
// WHY A DAG: real pipelines are "extract orders, extract customers -> join them -> aggregate -> publish the report". Express
// the dependencies as a DIRECTED ACYCLIC GRAPH and the scheduler can (1) run independent steps in parallel, (2) never start a
// step before its inputs exist, (3) re-run only what failed, (4) refuse a configuration that cannot be ordered at all.
//
// New(tasks) validates and returns an error (wrapping the matching sentinel) for:
//   - an empty Name or a nil Run                     -> ErrInvalidTask
//   - two tasks with the same Name                   -> ErrDuplicateTask
//   - a dependency that names no task                -> ErrUnknownDependency (the message names BOTH tasks)
//   - a cycle, including a task that depends on itself -> ErrCycle, with the loop in the message, e.g. "a -> b -> c -> a"
//     (read the arrow as "depends on"). Find it with KAHN's algorithm: repeatedly remove tasks with no remaining dependencies;
//     if tasks remain, they are on or behind a cycle. Every remaining task still has a remaining dependency, so starting anywhere
//     and always stepping to the (alphabetically first) remaining dependency must eventually revisit a task: that is a cycle.
//     The empty graph is valid.
//
// Order() returns the task names in a valid TOPOLOGICAL ORDER (every task after all of its dependencies), made DETERMINISTIC by
// Kahn's algorithm with a tie-break: among the tasks that are ready, always take the alphabetically smallest.
//
//	z; a (deps z); m          ready {m, z} -> m, then z, which releases a  ->  [m z a]
//
// Levels() groups the tasks into LAYERS: level 0 = tasks without dependencies; any other task is at level 1 + the maximum level
// of its dependencies (its LONGEST dependency chain). Names inside a level are sorted. Everything in one level could run in
// parallel; the number of levels is the length of the critical path -- the fastest the pipeline can possibly finish.
//
//	a; b,c (dep a); d (deps b,c); f (deps d,a)   ->  [[a] [b c] [d] [f]]
//
// Run(ctx, maxParallel) executes the graph and reports a Result for EVERY task:
//   - maxParallel < 1 -> (nil, ErrBadParallelism).
//   - a task may start only when ALL its dependencies Succeeded; at most maxParallel tasks run at once.
//   - a task that Fails (after its retries are used up: 1 + Retries attempts in total, stopping at the first success) makes ALL
//     its transitive dependents Skipped (their Run is never called, Attempts 0); independent branches keep going.
//   - a panic inside Run is recovered and counts as a failed attempt (Err mentions "panic").
//   - if ctx is canceled: tasks that have not started are marked Canceled (Err = ctx.Err()); running tasks are waited for (they
//     should watch ctx); a task that FAILED because of the cancellation still counts as Failed and still skips its dependents;
//     retrying stops once ctx is done. Run only returns when every goroutine it started has finished (no leaks).
//   - the returned error is nil iff every task Succeeded; otherwise it wraps ErrRunFailed AND the failed tasks' errors
//     (errors.Join, each prefixed with the task name) so errors.Is(err, theTaskError) works.
//     The scheduler is a single loop that owns all state and receives completions on a channel: no locks needed.
type Graph struct{}

func New(tasks []Task) (*Graph, error) { return nil, todo.NotImplemented("Ch21: New") }

func (g *Graph) Order() []string { return nil } // TODO Ch21

func (g *Graph) Levels() [][]string { return nil } // TODO Ch21
