# Chapter 06 — Event time, windows and watermarks

| | |
|---|---|
| **Build** | `Tumbling`, `Sliding`, `Sessions` (`assign.go`), `WatermarkTracker` (`watermark.go`), `Aggregator` (`aggregator.go`) — `internal/window/` |
| **Tests** | `internal/window/window_test.go` — 17 tests |
| **Run** | `go test ./internal/window -run Ch06 -race -v` |
| **Prerequisites** | prereq 05 (event time vs processing time); Chapter 04 helps |
| **Time** | ~3 h — the most conceptual chapter of Part B |

## 1. Why this matters

"How many clicks per minute?" sounds trivial until you ask: *which* minute? The one in which the click **happened** (event time), or the one in which your server **received** it (processing time)? Phones go through tunnels, batches arrive late, retries replay old events. If you use arrival time, the numbers change every time you re-run the job. Real analytics uses **event time** — and then you must answer the hard question: **how long do I wait for stragglers before declaring a minute finished?** That answer is the *watermark*.

## 2. The lesson, step by step

### Step 1 — windows turn an endless stream into finite pieces
You can't sum an infinite stream. A **window** is a finite slice of event time. Windows are half-open intervals `[Start, End)`: start **inside**, end **outside** — so an event at exactly 12:05:00 belongs to `[12:05,12:10)`, never to `[12:00,12:05)`.

### Step 2 — three kinds of window

```
tumbling (size 5m)    |--0:00-0:05--|--0:05-0:10--|--0:10-0:15--|          each event in exactly ONE window
sliding (10m by 5m)   |-----0:00-0:10-----|                               each event in size/slide windows
                              |-----0:05-0:15-----|
session (gap 30m)     |e e  e|            |e e|                            windows follow the DATA's bursts
                         [0:00 ... 0:50)       [1:30 ... 2:00)
```
- **Tumbling** — fixed, aligned, non-overlapping. `start = floor(t / size) * size` (aligned to the Unix epoch; use the given `alignedStart`). *Reports and dashboards.*
- **Sliding** — overlapping; "the last 10 minutes, updated every 5". Every window `[s, s+size)` with `s` a multiple of `slide` that contains `t`. The latest such window starts at `floor(t/slide)*slide`; walk **backwards** by `slide` while `s > t - size`. *Moving averages, alerting.*
- **Session** — a burst of activity ended by `gap` of silence. Each event opens `[t, t+gap)`; overlapping windows **merge**. Two events `gap` apart *just touch* (they don't overlap), so `next - prev >= gap` starts a new session. *User visits.*

Why **floorDiv** (given)? Integer division in Go truncates toward zero, which rounds *up* for negative numbers — a classic off-by-one source for times before 1970 (or any signed arithmetic). Read it once.

### Step 3 — out-of-order events and the watermark

```
arrival order:  e@12:00:05  e@12:00:30  e@12:01:10  e@12:00:50(!)   e@12:01:05
                                                     └─ older than something we already saw
```
When is window `[12:00, 12:01)` complete? You can never be *sure*. A **watermark** is a promise: *"I believe no event with time < W will still arrive."* The standard heuristic — **bounded out-of-orderness**:

```
watermark = (largest event time seen so far) − maxDelay
```
With `maxDelay = 10s` and the largest time 12:01:10, the watermark is 12:01:00: the window ending 12:01:00 can be closed. Two properties: (1) the watermark **never moves backwards** (an old event must not reopen the past); (2) a **bigger `maxDelay`** tolerates more disorder but delays *every* result by that much — a latency/completeness trade-off you tune.

### Step 4 — closing windows, lateness, and bounded memory
The `Aggregator` keeps `window → key → {count, sum}`. When the watermark passes `window.End + allowedLateness`, the window is **closed**: its result is emitted **and its state deleted**. That deletion is what lets a stream job run for years in constant memory.

- An event whose window is already closed is **late**: counted (`Late()`), dropped. *Never drop silently — the count is a data-quality metric.*
- `allowedLateness` keeps windows open a while *after* they end so stragglers can still update them (the result is then emitted once, when finally closed; real engines may also emit early "speculative" results and corrections).

### Step 5 — order of operations matters
For each event the pipeline does: **(1) `Add`** (judge the event against the watermark *so far*), **(2) `Observe`** (update the watermark with this event), **(3) `AdvanceWatermark`** (emit what that closes). Swap 1 and 2 and an event could be declared late *because of itself*. The end-to-end test walks through exactly such a scenario.

### Step 6 — flush at end of stream
When the input ends, the watermark would be +∞: `Flush()` emits everything that's left.

## 3. Your challenge
Implement `Tumbling`, `Sliding`, `Sessions`, `WatermarkTracker` and `Aggregator`. All the arithmetic helpers are given.

## 4. Hints
<details><summary>Hint 1 — Sliding</summary>

```go
for s := alignedStart(t, slide); s.After(t.Add(-size)); s = s.Add(-slide) { collect Window{s, s.Add(size)} }
// then reverse into ascending order
```
</details>
<details><summary>Hint 2 — Sessions</summary>

Keep `first` and `last`. For each next time: if `t.Sub(last) >= gap` → emit `[first, last+gap)` and set `first = t`. Always `last = t`. Emit the final session after the loop.
</details>
<details><summary>Hint 3 — closed?</summary>

`closed(w) = !w.End.Add(lateness).After(watermark)`, i.e. `End + lateness <= watermark`. Use it in both `Add` (reject) and `AdvanceWatermark` (emit).
</details>
<details><summary>Hint 4 — deterministic output</summary>

Go map iteration order is random. Emit results sorted by `(Window.Start, Key)` or your tests (and your users' diffs) flap.
</details>

## 5. Stretch challenges
1. **Per-key watermarks** (each key has its own clock). When is it better, when worse?
2. **Idle sources:** one partition stops sending, so the global watermark stalls forever. Add an "idle timeout" that excludes silent inputs.
3. **Early + late firings:** emit a speculative result every N events and a *correction* when a late event arrives within `allowedLateness`.
4. **Sliding aggregator:** implement it efficiently (don't re-add every event to size/slide windows… or do, and measure).
5. **Session merging on a stream:** `Sessions` here needs sorted input; extend it to handle out-of-order events by *merging* overlapping session windows as events arrive.

## 6. Reflect
- You raise `maxDelay` from 10 s to 10 min. What gets better? What gets worse?
- Why must the watermark never go backwards?
- A tumbling-window count is wrong by 3 % compared to the batch job that re-ran overnight. Name two possible causes.
- Why is `[Start, End)` (half-open) preferable to `[Start, End]`?
