# Chapter 16 — Count-Min sketch and heavy hitters

| | |
|---|---|
| **Build** | `CountMin` (`countmin.go`), `SpaceSaving` + `Counter` (`spacesaving.go`) — `internal/sketch/` |
| **Tests** | `internal/sketch/frequency_test.go` — 11 tests |
| **Run** | `go test ./internal/sketch -run Ch16 -race -v` |
| **Prerequisites** | Chapter 15; prereq 04 (probability, hash collisions) |
| **Time** | ~3 h |

## 1. Why this matters

"Which pages are the most viewed?" "Which IP address is flooding us?" "Which search queries are trending right now?" With 500 million distinct keys, `map[string]int` costs tens of GB — and the stream never ends. This chapter gives you two constant-memory tools: **Count-Min** (how often did *this* key occur?) and **Space-Saving** (which keys occurred *most*?), plus the one idea they share: *accept a small, bounded, one-sided error to get a fixed memory budget.*

## 2. The lesson, step by step

### Step 1 — the exact solution and why it breaks
`counts[key]++` is O(1) and exact — and its memory grows with **distinct** keys. 500 M URLs × ~100 bytes = 50 GB, per worker, and merging per-worker maps means shipping all of it. Two relaxations help: *estimate the count of any key* (Count-Min) and *keep only the top few* (Space-Saving).

### Step 2 — Count-Min: a grid of counters
`depth` rows × `width` columns, all zero, one hash per row.
```
Add(key, n):     in every row i:  grid[i][ h_i(key) % width ] += n
Estimate(key):   min over rows i of  grid[i][ h_i(key) % width ]
```
Why the **minimum**? Collisions with other keys can only *add* to a counter. So every counter ≥ the truth, and the *smallest* is the least polluted. Therefore **a Count-Min sketch never undercounts**; the tests verify `Estimate ≥ truth` for *every* key.

### Step 3 — the error guarantee and sizing
```
Estimate(key) ≤ truth + ε·N        with probability ≥ 1 − δ        (N = total of all counts added)
width = ceil(e / ε)      depth = ceil(ln(1/δ))
```
ε = 0.001, δ = 0.01 → 2719 × 5 counters ≈ 109 KB — **for any number of distinct keys**. Read the bound carefully: the error is relative to the *total*, so it is excellent for *heavy* keys (a key with 10 000 hits is estimated within ~2 %) and meaningless for rare keys (a key seen twice may be estimated at 150). Count-Min is for **heavy hitters**, not for the long tail.

### Step 4 — merging is free
Counters just add: sketch(A) + sketch(B) (cell by cell) *is exactly* the sketch of A's stream followed by B's. Shard the stream over 100 workers, sketch each, add the grids. (Only for same-shaped grids: `ErrShapeMismatch`.)

### Step 5 — Count-Min can't list the frequent keys
Given a key, it estimates its count. But it doesn't *remember* keys, so it can't answer "what are the top 10?". Either keep a small heap of candidates next to it (stretch) or use **Space-Saving**.

### Step 6 — Space-Saving: top-k with k counters
Keep at most `k` counters `{item, count, err}`. For each stream item `x`:
1. **monitored** → `count++`
2. **a counter is free** → new counter `{x, 1, 0}`
3. **full** → *evict the smallest counter* `m`; give its slot to `x` with `count = m.count + 1`, `err = m.count`. (`x` may already have occurred up to `m.count` times unnoticed — that is the error.)

Guarantees (proved by Metwally et al.; the tests check them): every item with true frequency **> N/k is always monitored**; and for every counter `Count − Err ≤ truth ≤ Count`. With ≤ k distinct items in the stream all counts are exact. So with `k = 100` you *surely* catch everything above 1 % of traffic.

### Step 7 — which one when?
| Question | Tool |
|---|---|
| "how many times did *this key* occur?" | Count-Min |
| "which keys are the most frequent?" | Space-Saving (or Count-Min + heap) |
| "how many *different* keys?" | HyperLogLog (Chapter 17) |
| "have I seen *this key at all*?" | Bloom filter (Chapter 15) |
| exact counts, modest cardinality | a plain map |

### Step 8 — in the real world
Redis `TOPK` and `CMS.*`, Twitter/X trending, DDoS detection (top talkers), ClickHouse `topK`, Spark `countMinSketch`, Kafka Streams' heavy-hitter demos. **Windowed** versions (Chapter 6) use one sketch per window and drop old ones.

## 3. Your challenge
`NewCountMin`, `Add`, `Estimate`, `Total`, `Width`, `Depth`, `Merge`; `NewSpaceSaving`, `Offer`, `N`, `Top`.

## 4. Hints
<details><summary>Hint 1 — the grid</summary>

One flat `[]uint64` of `depth*width`; cell `(row, col)` is `cells[row*width+col]`; column = `hashx.Sum64Seed(key, uint64(row)) % uint64(width)`.
</details>
<details><summary>Hint 2 — Space-Saving eviction</summary>

Scan the map for the smallest `Count`. Break ties by item name so the result is deterministic across runs (Go randomises map iteration order!). New counter: `Count: victim.Count + 1, Err: victim.Count`.
</details>
<details><summary>Hint 3 — Top()</summary>

Copy the counters *by value* into a slice (so callers can't alias your state) and `sort.Slice` by count descending, then item ascending.
</details>

## 5. Stretch challenges
1. **Conservative update:** on `Add`, only raise the counters that are *at the current minimum*. Measure how much it reduces over-estimation.
2. **Top-k with Count-Min:** keep a min-heap of the best `k` candidates; on each `Add`, `Estimate` and update the heap. Compare accuracy with Space-Saving.
3. **O(1) Space-Saving:** the *stream-summary* structure — buckets of equal count in a doubly linked list — removes the O(k) scan.
4. **Windowed heavy hitters:** a ring of per-minute sketches; the last 10 minutes = their merge.
5. **Count-Mean-Min:** subtract the estimated noise (`(N − count)/(width − 1)`) for better estimates on rare keys.
6. **Misra–Gries** (the simpler ancestor): implement it and compare its guarantee with Space-Saving's.

## 6. Reflect
- Why does taking the minimum across rows make sense, and what would taking the *average* or *maximum* do to the guarantee?
- Why is Count-Min's error "bad for the tail, good for the head"?
- Why must a Space-Saving eviction *inherit* the evicted count?
- You need the top 10 URLs per minute over 50 000 events per second. Sketch the whole design.
