# Chapter 05 — Reliable processing: retries, dead letters, at-least-once

| | |
|---|---|
| **Build** | `Run` — `internal/reliable/run.go` |
| **Given** | `Message`, `Handler`, `ErrPermanent`, `Sleeper`, `Config`, `Result`, `DeadLetter` |
| **Tests** | `internal/reliable/reliable_test.go` — 12 tests |
| **Run** | `go test ./internal/reliable -run Ch05 -race -v` |
| **Prerequisites** | Chapters 01–02; prereq 05 (delivery semantics) |
| **Time** | ~2 h |

## 1. Why this matters

In production, things fail constantly: a database blips, an API returns 503, one record has a field nobody anticipated. A pipeline that dies on the first error loses a night's work; one that ignores errors silently loses data. A *reliable* pipeline sorts failures into three buckets and treats each right:

| Failure kind | Example | What to do |
|---|---|---|
| **Transient** | timeout, 503, deadlock | **retry** with growing waits |
| **Permanent** | malformed JSON, unknown product | **don't retry** — park it in a *dead-letter queue* (DLQ) for a human |
| **Bug** | nil pointer → panic | **don't crash**; park it too (and fix the code) |

## 2. The lesson, step by step

### Step 1 — what "at-least-once" means
The pipeline guarantees every message is processed **at least once**: if it can't be sure a message succeeded, it *tries again*. The flip side: your handler may run **twice** for one message (the first attempt did its side effect, then timed out before replying). The test `Handlers_SeeTheSameMessageAgain…` makes this concrete. Chapter 07 shows how to make repetition harmless; for now the lesson is: *retry safety is the handler's job.*

### Step 2 — classify errors with `errors.Is`
`ErrPermanent` is a sentinel. A handler that decides "this will never work" returns `fmt.Errorf("bad payload: %w", reliable.ErrPermanent)`. The processor checks `errors.Is(err, ErrPermanent)` — it works through any wrapping. Everything else is assumed transient.

### Step 3 — exponential backoff (and why waiting matters)
After failure *n* wait `Backoff(n)` (1 s, 2 s, 4 s…). Immediate retries hammer a service that is already struggling and make the outage worse (a "retry storm"). The wait goes through an injected `Sleeper`, so tests record the delays instead of sleeping. **No sleep after the final attempt** — nothing is left to wait for.

### Step 4 — the dead-letter queue
After `MaxAttempts` the message goes to `DeadLetters` with the **last error** and the **attempt count** — enough for an operator to see *why* and replay it after the fix. Never just drop a failed message; a DLQ you never look at is still better than silent loss, and a metric on its size is an alarm.

### Step 5 — survive panics
A panic in a goroutine you didn't `recover` kills the whole program. Wrap the call:

```go
func call(ctx context.Context, h Handler, m Message) (err error, panicked bool) {
    defer func() { if r := recover(); r != nil { err, panicked = fmt.Errorf("handler panicked: %v", r), true } }()
    return h(ctx, m), false
}
```
`recover()` only works inside a **deferred** function in the **same goroutine** as the panic. A panic is a bug, so it is *not retried* — straight to the DLQ.

### Step 6 — workers and shared results
`cfg.Workers` goroutines read from `in`. They all update one `Result`, so guard it with a mutex (or keep per-worker results and merge). Sort the DLQ by message ID at the end: concurrency makes the *arrival* order random, but a result you can test (and diff between runs) must be deterministic.

### Step 7 — cancellation: "unfinished" is neither success nor failure
On `ctx` cancel, stop promptly. A message that was mid-flight is **not** a failure to record — it simply wasn't finished, and the *source* must replay it (that is what checkpoints are for: Chapter 07). Returning `ctx.Err()` tells the caller the run was cut short.

## 3. Your challenge
Implement `Run` in `internal/reliable/run.go`. Everything else is given.

## 4. Hints
<details><summary>Hint 1 — structure</summary>

`Run` = start workers (WaitGroup) that loop `select { ctx.Done / m, ok := <-in }` and call a helper `processOne`. `processOne` has the `for attempt := 1; ; attempt++` retry loop from steps 2–4.
</details>
<details><summary>Hint 2 — the retry loop</summary>

```go
err, panicked := call(ctx, h, m)
switch {
case err == nil:                                              // success
case panicked || errors.Is(err, ErrPermanent) || attempt >= maxAttempts:   // dead-letter
}
// otherwise: if backoff != nil { if sleeper.Sleep(ctx, backoff(attempt)) != nil { return } }  then loop
```
</details>
<details><summary>Hint 3 — don't forget</summary>

Defaults (`Workers`, `MaxAttempts` < 1 → 1; `Sleeper` nil → `SystemSleeper`), counting `TotalAttempts`, sorting `DeadLetters`, and returning `ctx.Err()` at the end.
</details>

## 5. Stretch challenges
1. **Jitter:** randomise each wait (`rand` in [0, delay]) — why do thousands of clients with identical backoffs retry in lock-step, and how does jitter fix it?
2. **Retry budget:** stop retrying everything when more than 30 % of recent calls fail (a broken dependency should trip a breaker, not be hammered by every worker).
3. **DLQ as a channel:** stream dead letters out as they happen instead of returning them at the end. What happens if nobody reads?
4. **Idempotent handler wrapper:** skip messages whose ID you've processed (use Chapter 07's `Deduper`).
5. **Poison-pill detection:** a message that crashes the *worker process* (not just the handler) — how would you spot it across restarts?

## 6. Reflect
- Why is "retry forever" almost never right?
- Why must `recover` be inside the goroutine that panics?
- Your handler sends an email, then the database write times out. With at-least-once, what does the customer receive, and how would you prevent it?
- What would a good alert on this system look like (hint: three numbers from `Result`)?
