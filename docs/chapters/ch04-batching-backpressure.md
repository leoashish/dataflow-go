# Chapter 04 — Batching and back-pressure

| | |
|---|---|
| **Build** | `Batches` (`internal/batch/stages.go`), `Buffer` (`internal/batch/buffer.go`) |
| **Tests** | `internal/batch/batch_test.go` — 14 tests |
| **Run** | `go test ./internal/batch -run Ch04 -race -v` |
| **Prerequisites** | Chapter 01 (channels, `select`, `ctx`) |
| **Time** | ~2 h |

## 1. Why this matters

Writing 1 000 000 rows to a database one `INSERT` at a time takes hours; in batches of 1 000 it takes seconds — every round trip has a fixed cost, so you amortise it. But batching has a trap: on a **quiet** stream a half-full batch could wait forever, so every batcher is "**full OR old**". And whenever a fast producer meets a slow consumer, *something* must give: block the producer, or drop data. Choosing that deliberately — rather than discovering an out-of-memory crash — is the heart of back-pressure.

## 2. The lesson, step by step

### Step 1 — the batching loop
One goroutine, one `current []T`. On each item: append. If `len(current) >= Size`: flush. On timer: flush. On input close: flush the remainder then stop. **Never flush an empty batch.**

### Step 2 — a timer per batch, started by the *first* item
If the timer ran continuously you'd flush empty batches; if it started at batch creation, a batch that sits idle would have an old clock. So: `if len(current) == 1 { timeout = After(MaxWait) }`. `MaxWait` therefore means "no item waits longer than this".

### Step 3 — nil channels: a `select` case you can switch off
A send or receive on a **nil** channel blocks forever, so a `select` case on a nil channel is *never chosen*. That lets one `select` adapt:

```go
var timeout <-chan time.Time          // nil: no batch pending → this case can't fire
...
timeout = After(d)                    // a batch started → now it can
...
timeout = nil                         // batch flushed → forget the timer
```
That last line is not decoration. After a size-flush the old timer is still alive; if you kept its channel, it would fire into the **next** batch and flush it prematurely. A test checks precisely this ("stale timer").

### Step 4 — injectable time
`Options.After` has the same signature as `time.After`. Production passes nothing (real clock); the test passes a fake that announces each request and lets the test decide when it fires. Result: "wait 5 seconds, then flush" runs in microseconds and never flakes. **Any code that sleeps or reads the clock should receive that dependency as a parameter.**

### Step 5 — back-pressure falls out of unbuffered channels
`out <- batch` blocks while the consumer is busy; the batcher therefore isn't reading `in`; the producer blocks on its send. The test: producer sends 10 items with Size 2 and a consumer that never reads → exactly 2 get through.

### Step 6 — the elastic buffer and its policies
Sometimes blocking the producer is unacceptable (a network socket you must keep reading; a metrics stream). A `Buffer` sits between them and **chooses**:

| Policy | When full | Use when |
|---|---|---|
| `Block` | stop reading input → producer waits | data must not be lost (orders, payments) |
| `DropNewest` | read and discard the incoming item | oldest data matters most (first N events) |
| `DropOldest` | evict the head, keep the newest | freshness matters (telemetry, "latest value") |

Always **count** what you drop (`Dropped()`): a silent drop is a data-quality incident waiting to happen.

Implementation: one goroutine, one `select` that is **rebuilt each loop** with nil-channel switches — `send` is nil when the queue is empty (nothing to send), `recv` is nil when the queue is full *and* the policy is `Block`. When input closes set the local `in` to nil (a closed channel is always ready and you'd spin) and finish when the queue drains.

## 3. Your challenge
`Batches` and `NewBuffer`. Contracts in the doc comments.

## 4. Hints
<details><summary>Hint 1 — Batches</summary>

`select { case <-ctx.Done(): return; case v, ok := <-in: …; case <-timeout: … }`. Put the flush in a closure that sends `current` on `out` (itself inside a `select` with `ctx.Done()`), then sets `current = nil; timeout = nil`.
</details>
<details><summary>Hint 2 — Buffer's per-loop select</summary>

```go
var send chan T; var head T
if len(queue) > 0 { send, head = b.out, queue[0] }
var recv <-chan T
if inputOpen && (len(queue) < capacity || policy != Block) { recv = in }
select { case <-ctx.Done(): return; case v, ok := <-recv: …; case send <- head: queue = queue[1:] }
```
</details>
<details><summary>Hint 3 — DropOldest</summary>

When full: `queue = append(queue[1:], v)` and `Dropped++`.
</details>

## 5. Stretch challenges
1. **Size in bytes:** batch by total payload size (`SizeFunc func(T) int`, `MaxBytes`) — databases cap request bodies.
2. **Flush on demand:** a `Flush()` channel/method for graceful shutdown.
3. **Keyed batches:** one batch per key (per table, per tenant) with an LRU cap on open batches.
4. Make `Buffer` spill to disk when full instead of dropping or blocking (what do you now need? — a durable queue, see Chapters 08 and 19).

## 6. Reflect
- Why must the timer start at the *first* item rather than at batch creation?
- You forget `timeout = nil` after a size-flush. Describe the exact bug a user would see.
- For each policy, name one real system where it is the right choice.
- Where in this chapter did *not* sleeping (injecting `After`) make the tests better, not just faster?
