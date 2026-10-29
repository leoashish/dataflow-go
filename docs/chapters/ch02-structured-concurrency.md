# Chapter 02 — Structured concurrency: groups, limits, semaphores

| | |
|---|---|
| **Build** | `Group`, `Semaphore`, `ParallelMap` — `internal/conc/conc.go` |
| **Tests** | `internal/conc/conc_test.go` — 17 tests |
| **Run** | `go test ./internal/conc -run Ch02 -race -v` |
| **Prerequisites** | Chapter 01; [prereq 02](../prereq/02-concurrency-primitives.md) (`sync.WaitGroup`, `sync.Once`, `context`) |
| **Time** | ~90 min |

## 1. Why this matters

Chapter 01 started goroutines freely. Real jobs need *discipline*: "process these 10 000 files with at most 8 at a time; if one fails, stop the rest and tell me **the first** error; never leave a goroutine running after I return." That discipline has a name — **structured concurrency**: goroutines live inside a scope, and the scope doesn't end until they all have.

Go's standard library has no `errgroup`; it lives in `golang.org/x/sync`. You will build it — one of the most useful 40 lines in Go.

## 2. The lesson, step by step

### Step 1 — wait for goroutines: `sync.WaitGroup`
`wg.Add(1)` *before* starting each goroutine, `defer wg.Done()` inside, `wg.Wait()` to block until the counter hits zero. (Calling `Add` inside the goroutine is a race: `Wait` may run first.)

### Step 2 — remember only the **first** error: `sync.Once`
Many goroutines may fail. Which error do you report? The one that happened **first**, because later ones are usually *consequences* (you cancelled the others, and they fail with `context.Canceled`). `sync.Once` runs a function exactly once, even if called from many goroutines at the same moment:

```go
g.errOnce.Do(func() { g.err = err; g.cancel() })   // record + cancel together, atomically with respect to other failures
```

### Step 3 — tell the others to stop: derive a context
`ctx, cancel := context.WithCancel(parent)`. Cancelling `ctx` does **not** affect `parent`, but cancelling `parent` cancels `ctx`. Hand `ctx` to the work; the first failure calls `cancel()` and everyone else sees `ctx.Done()`.

### Step 4 — limit parallelism: a channel as a semaphore
A buffered channel of capacity N is a semaphore: **send = take a slot** (blocks when N are taken), **receive = free a slot**.

```go
sem := make(chan struct{}, limit)
sem <- struct{}{}            // acquire (blocks if full)  — in Group.Go, BEFORE starting the goroutine
defer func() { <-sem }()     // release when the goroutine finishes
```
Acquiring in the *caller* (inside `Go`) means `Go` itself blocks when the group is full: the producer of work is slowed to the pace of the workers. That is **back-pressure at the scope level** and it is why a loop `for _, f := range files { g.Go(...) }` never creates 10 000 goroutines at once.

### Step 5 — `Semaphore` with context
The same idea, but `Acquire(ctx)` must also give up if the context ends: `select { case s.slots <- struct{}{}: …; case <-ctx.Done(): … }`. A failed `Acquire` must **not** consume a slot.

### Step 6 — build `ParallelMap` on top
Preserve input order by writing `results[i]` (each goroutine its own index: no lock needed). Subtle point the tests catch: the loop checks `ctx.Err()` **before** `g.Go`, but `g.Go` can then *block* waiting for a slot — and by the time it unblocks, a sibling may have failed. So **check again inside the goroutine** before calling `fn`.

## 3. Your challenge
`Group` (`NewGroup`, `SetLimit`, `Go`, `Wait`), `Semaphore` (`Acquire`, `TryAcquire`, `Release`), and `ParallelMap`. Contracts are in the doc comments.

## 4. Hints
<details><summary>Hint 1 — Group fields</summary>

`cancel context.CancelFunc`, `wg sync.WaitGroup`, `sem chan struct{}` (nil = unlimited), `errOnce sync.Once`, `err error`.
</details>
<details><summary>Hint 2 — Go</summary>

```go
if g.sem != nil { g.sem <- struct{}{} }
g.wg.Add(1)
go func() {
    defer func() { if g.sem != nil { <-g.sem }; g.wg.Done() }()
    if err := fn(); err != nil { g.errOnce.Do(func() { g.err = err; g.cancel() }) }
}()
```
</details>
<details><summary>Hint 3 — Wait</summary>

`g.wg.Wait(); g.cancel(); return g.err` — cancelling at the end releases the context's resources (that's why the test `WaitCancelsTheContext_EvenWithoutErrors` exists). Careful: after `Wait`, *your own* derived context is cancelled, so in `ParallelMap` check the **caller's** `ctx.Err()`, not the group's.
</details>

## 5. Stretch challenges
1. `TryGo(fn) bool` — start only if a slot is free.
2. A **weighted** semaphore: `Acquire(ctx, n)` for tasks that need different amounts of a resource (e.g. memory). What breaks if a 10-unit request waits behind many 1-unit ones? (Starvation: study how `x/sync/semaphore` queues waiters.)
3. Recover panics inside `Go`, turning them into errors. Where must `recover` be called?
4. Replace your `Group` with the real `golang.org/x/sync/errgroup` and diff the behaviour.

## 6. Reflect
- Why is `Wait` returning the first error *by time* better than the first error *by goroutine index*?
- What goes wrong if you call `wg.Add(1)` inside the goroutine?
- Why does acquiring the semaphore in the caller (not in the goroutine) matter for memory?
- `ParallelMap` returns `nil` results on error. What would a partial-results API look like, and who would want it?
