# Chapter 01 — Pipelines with channels

| | |
|---|---|
| **Build** | `Generate`, `Map`, `Filter`, `Take`, `FanOut`, `FanIn`, `Drain` — `internal/pipeline/stages.go` |
| **Tests** | `internal/pipeline/pipeline_test.go` — 13 tests |
| **Run** | `go test ./internal/pipeline -run Ch01 -race -v` |
| **Prerequisites** | [prereq 02 — concurrency primitives](../prereq/02-concurrency-primitives.md) (goroutines, channels, `select`, `context`) |
| **Time** | ~90 min |

## 1. Why this matters for data engineering

Almost every data job has the same shape: **read → transform → write**. Done one record at a time, the CPU waits for the disk, the disk waits for the network, and nothing is ever busy at once. A **pipeline** runs the stages concurrently, like an assembly line: while stage 3 writes record 1, stage 2 transforms record 2 and stage 1 reads record 3.

```
 source ──▶ parse ──▶ ┌─ worker ─┐ ──▶ filter ──▶ sink
                      ├─ worker ─┤        (fan-out / fan-in: several workers for the slow stage)
                      └─ worker ─┘
```
Go gives you the two ingredients: **goroutines** (cheap threads) and **channels** (typed, synchronised queues between them).

## 2. The lesson, step by step

### Step 1 — one stage = one goroutine + one output channel

```go
func Double(in <-chan int) <-chan int {
    out := make(chan int)          // 1. the stage OWNS its output channel
    go func() {                    // 2. work happens in a goroutine, so the function returns at once
        defer close(out)           // 3. the owner closes it, exactly once, when it will never send again
        for v := range in { out <- v * 2 }
    }()
    return out                     // 4. return it receive-only (<-chan): consumers can read but not close
}
```
Stages **compose**: `Double(Double(source))`. Each call adds a goroutine; data flows through unbuffered channels.

### Step 2 — who closes a channel?

A closed channel tells every receiver "no more values" (`for v := range ch` ends; `v, ok := <-ch` gives `ok == false`). Three rules, burn them into memory:

1. **Only the sender closes**, never the receiver (a send on a closed channel *panics*).
2. **Close exactly once** (closing twice panics) — `defer close(out)` at the top of the owning goroutine guarantees it.
3. With **several senders** (fan-in, fan-out workers) none of them may close; a separate goroutine waits for all of them (`sync.WaitGroup`) and closes once.

### Step 3 — back-pressure for free

`make(chan T)` is unbuffered: a send blocks until a receiver is ready. So if the final consumer is slow, every stage upstream blocks on its send, all the way back to the source. Memory stays bounded. (Compare an unbounded queue: a slow consumer = out-of-memory.) A buffered channel `make(chan T, 100)` lets the producer run ahead by 100 items — a *tuning knob*, not a free lunch.

### Step 4 — cancellation, or: the goroutine leak

What if the consumer stops reading (found what it needed, hit an error)? The producer is blocked on `out <- v` **forever** — a *goroutine leak*: it holds memory and never ends. The cure: every blocking send/receive also watches a `context`:

```go
select {
case out <- v:           // delivered
case <-ctx.Done():       // somebody cancelled: give up and let `defer close(out)` run
    return
}
```
Do this for **receives** too (`case v, ok := <-in:`) — otherwise a stage whose input never closes can never stop. The test `Map_StopsOnCancel_EvenWhenTheInputNeverCloses` checks exactly that, and `testutil.NoLeaks` fails a test if goroutines outlive it.

### Step 5 — fan-out: parallelise the slow stage

If `fn` is slow (an HTTP call, a regex, a parse), one goroutine is the bottleneck. Start **N workers reading the same input channel**. Go's channel hands each value to exactly one waiting receiver, so you get load balancing without any code:

```go
for range workers { wg.Add(1); go func() { defer wg.Done(); for v := range in { out <- fn(v) } }() }
go func() { wg.Wait(); close(out) }()      // the closer
```
**Trade-off: order is lost.** Results come out in *completion* order. If you need input order back you must tag items with an index and re-sequence (you do that in Chapter 02 with `ParallelMap`, and in the capstone).

### Step 6 — fan-in: merge several sources

One goroutine per input forwards into a shared output; the same `WaitGroup` + closer pattern closes it. Arrival order is whatever the scheduler decides — don't rely on it.

### Step 7 — `Take` and why upstream needs the context

`Take(n)` stops reading after n values. The stage *before* it is now blocked sending item n+1. Only cancelling the context frees it. This is the single most common leak in real pipelines: **whoever stops consuming early must cancel**.

## 3. Your challenge

Implement the seven functions in `internal/pipeline/stages.go` (the doc comments restate the contract). All of them: unbuffered output, close exactly once, honour `ctx` on every blocking operation.

## 4. Hints

<details><summary>Hint 1 — the shape of Map</summary>

```go
for {
    select {
    case <-ctx.Done(): return
    case v, ok := <-in:
        if !ok { return }
        select { case out <- fn(v): case <-ctx.Done(): return }
    }
}
```
Filter and Take are the same skeleton; Filter `continue`s when `keep` is false.
</details>
<details><summary>Hint 2 — FanOut and FanIn</summary>

Both: `wg.Add(1)` per goroutine, `defer wg.Done()` inside, one extra `go func() { wg.Wait(); close(out) }()`. FanOut's goroutines all read the *same* `in`; FanIn starts one goroutine per input.
</details>
<details><summary>Hint 3 — "my test hangs"</summary>

A hang means some channel never closes or some goroutine is blocked without watching `ctx.Done()`. Run with `-timeout 20s` to get a goroutine dump showing exactly where each one is stuck.
</details>

## 5. Stretch challenges

1. `Tee(ctx, in, n)`: duplicate a stream to n consumers. What happens if one of them is slow?
2. `OrderedFanOut`: like `FanOut` but emits results in **input order** (tag with the index, hold results in a small map until the next expected index arrives). What is the worst-case memory?
3. `Batch`-less `Throttle(ctx, in, perSecond)` using `time.Ticker`.
4. Rewrite `Map` with `for v := range in` only. Which test fails, and why is that a real bug rather than a nitpick?

## 6. Reflect

- Why is the output channel *receive-only* (`<-chan T`) in the signature?
- What exactly leaks if `Generate` doesn't `select` on `ctx.Done()`, and the consumer walks away?
- Fan-out improved throughput but broke ordering. When is that acceptable, when not?
- Why `defer close(out)` and not `close(out)` at the end of the goroutine?
