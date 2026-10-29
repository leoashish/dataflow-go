# Prerequisite 02 — Concurrency primitives, step by step

*Chapters 1, 2, 4, 5, 8 and 21 depend on this. ~3 hours.*

## 1. Concurrency vs parallelism
**Concurrency** is *structuring* a program as independently progressing pieces; **parallelism** is *running* pieces at the same instant on several cores. Go gives you the first cheaply (goroutines) and the runtime provides the second when cores are available. Data processing wants both: I/O waits overlap, CPU work spreads.

## 2. Goroutines
`go f(x)` starts `f` concurrently. A goroutine costs a few KB of stack, so tens of thousands are fine. Two facts to burn in:
- **`main` (or the test) returning ends the program — it does not wait for goroutines.**
- **A goroutine that never returns is a leak.** It holds memory and whatever it references forever. `testutil.NoLeaks` fails a test that leaves goroutines behind.

## 3. Channels
```go
ch := make(chan int)      // unbuffered: send and receive meet (a rendezvous)
ch := make(chan int, 8)   // buffered: sends block only when full
ch <- v;  v := <-ch;  v, ok := <-ch    // ok == false: channel closed AND drained
close(ch)                              // only the SENDER closes; closing twice or sending after close panics
for v := range ch { ... }              // ends when ch is closed
```
Rules: *the owner of a channel (usually its sole sender) closes it*; a receive from a closed channel returns the zero value immediately; a send/receive on a **nil** channel blocks forever (useful in `select` to disable a case).

## 4. `select`
```go
select {
case v := <-in:        // whichever is ready; if several are, one is chosen at random
case out <- x:
case <-ctx.Done():     // cancellation
case <-time.After(d):  // timeout
default:               // never block
}
```
**Every blocking send or receive in a long-running goroutine should also select on `ctx.Done()`** — otherwise it cannot be cancelled and leaks (this is the main lesson of Chapter 1).

## 5. `context.Context`
A context carries a **cancellation signal** (and deadlines) down a call tree. `ctx, cancel := context.WithCancel(parent)`; call `cancel()` (usually `defer cancel()`) to release resources and tell everything below to stop. Functions that block take `ctx` as their *first* parameter and return `ctx.Err()` (`context.Canceled` / `DeadlineExceeded`) when it fires. Never store a context in a struct field for later use.

## 6. `sync`: the shared-memory tools
| Type | Use |
|---|---|
| `sync.WaitGroup` | wait for N goroutines: `wg.Add(1)` **before** `go`, `defer wg.Done()` inside, `wg.Wait()` |
| `sync.Mutex` / `RWMutex` | protect shared data; keep the critical section tiny; always `defer mu.Unlock()` |
| `sync.Once` | run something exactly once (the first error in Chapter 2's `Group`) |
| `sync/atomic` | lock-free counters and flags (`atomic.Int64`) |
| `sync.Pool` | reuse temporary objects to relieve the GC (Chapter 3) |

## 7. Data races — the bug that hides
A **data race** is two goroutines touching the same memory, at least one writing, with no synchronisation. The result is undefined: wrong numbers, rare crashes, "works on my machine". The Go **race detector** (`go test -race`) instruments memory accesses and reports races it *observes* — so run tests with it, and repeatedly (`-count=20`). Typical causes: a shared map or slice written by several goroutines; a loop variable captured by a closure (fixed in Go 1.22, per-iteration variables); a counter incremented without `atomic`.

## 8. Three ways to share data safely
1. **Don't share:** give each goroutine its own copy/partition and merge at the end (Chapter 11's per-worker maps — no lock at all).
2. **Share by communicating:** one goroutine *owns* the data; others send it messages (Chapter 21's scheduler: one loop owns all bookkeeping).
3. **Lock it:** a mutex around the shared structure (Chapter 19's WAL, Chapter 20's DB).
Prefer 1 over 2 over 3.

## 9. The standard patterns (all built in Chapter 1–2)
- **Pipeline:** stages connected by channels, each `func(ctx, in) out`.
- **Fan-out / fan-in:** N workers read one channel; merge N channels into one.
- **Worker pool with a limit:** a semaphore (buffered channel of N slots) bounding concurrency.
- **errgroup:** run tasks, keep the *first* error, cancel the rest (`conc.Group`).
- **Back-pressure:** an unbuffered or bounded channel makes a fast producer *wait* for a slow consumer — memory stays bounded.

## 10. Deadlock, livelock, starvation
Deadlock = everyone waits for something that never happens (a `WaitGroup.Wait` whose `Done` is never called; two locks taken in opposite orders; a send on a channel nobody reads). Go detects only the case where *all* goroutines sleep; usually it just hangs. This course's tests carry deadlines and a **watchdog** that prints every goroutine's stack — **read the stacks**: the stuck goroutines name the line.

## 11. Testing concurrent code
Assert **invariants that hold for every interleaving**, never timings: "a task never starts before its dependencies end" (event counter), "never more than N at once" (max in-flight counter), "all N tasks were running simultaneously" (rendezvous). No `time.Sleep` for ordering. Repeat with `-count` and `-race`.

## Exercises
1. Write a function that squares numbers using 4 workers and returns results **in input order**.
2. Make a leaky goroutine and watch `testutil.NoLeaks` catch it; fix it with `ctx`.
3. Provoke a data race on a `map` and observe `-race`'s report; fix it three ways (mutex, per-worker maps, owner goroutine).
4. Explain why `wg.Add(1)` inside the goroutine is a bug.
