# Chapter 18 — Reservoir sampling and streaming quantiles (P²)

| | |
|---|---|
| **Build** | `Reservoir[T]` (`reservoir.go`), `P2` (`p2.go`) — `internal/sketch/` |
| **Tests** | `internal/sketch/sampling_test.go` — 13 tests |
| **Run** | `go test ./internal/sketch -run Ch18 -race -v` |
| **Prerequisites** | Chapter 11; prereq 04 (probability, expectation, induction) |
| **Time** | ~3.5 h |

## 1. Why this matters

Two questions every data team asks of a stream they can't store:

1. **"Give me a representative handful"** — a random sample to debug, to train on, to eyeball, to estimate with. Not the *first* 1 000 rows (biased by time), a *uniform random* 1 000 from billions, in one pass, without knowing the stream length.
2. **"What's the p95 latency?"** Quantiles (median, p95, p99) are the numbers SLAs are written in — and the numbers you can **not** compute from per-shard summaries (Chapter 11: the median of medians is not the median). Exact quantiles need every value.

## 2. The lesson, step by step

### Part A — reservoir sampling

#### Step 1 — the puzzle
A stream of unknown length; memory for `k` items; at the end, every item must have had an *equal* chance of being in the sample. You can't pick random positions in advance (you don't know `n`), and "keep the first k" or "keep the last k" are heavily biased.

#### Step 2 — Algorithm R
- The first `k` items fill the reservoir.
- The `n`-th item (`n > k`) is **kept with probability `k/n`**, replacing a **uniformly random** current member: draw `j = rand.Intn(n)`; if `j < k`, then `sample[j] = item`.

#### Step 3 — why it's uniform (a proof worth keeping)
Induction on `n`. Suppose after `n−1` items every item is in the sample with probability `k/(n−1)`. The new item gets in with probability `k/n` ✔. An *old* item survives iff it was in **and** is not the one evicted. The new item is accepted with probability `k/n` and then evicts a given one of the `k` members with probability `1/k`, so it evicts our old item with probability `(k/n)(1/k) = 1/n`:
```
P(old item in) = k/(n−1) · (1 − 1/n) = k/(n−1) · (n−1)/n = k/n   ✔
```
Everyone is at `k/n` again. The test runs 20 000 trials and verifies that *every one of the 100 positions* is kept ≈ 2000 times — a sample biased towards early or late items fails it.

#### Step 4 — practicalities
- Pass the random source in (`*rand.Rand`): seeded → **reproducible** runs; that's a feature for debugging pipelines and for tests.
- Reservoir sampling is **mergeable only with care** (two reservoirs of different stream lengths must be combined with weights ∝ items seen). Stretch 3.
- Variants: **weighted** reservoir (A-Res, Efraimidis–Spirakis), **distinct** sampling, and **Algorithm L**, which *skips* ahead geometrically and does O(k·log(n/k)) random draws instead of n.

### Part B — streaming quantiles with P²

#### Step 5 — why it's hard
Sum, count, min, max: constant memory. Quantiles: the median needs to know where the *middle* is, which depends on everything. Exact streaming quantiles provably need Ω(n) memory. So we approximate.

#### Step 6 — five markers
P² (Jain & Chlamtac, 1985) tracks five points of the distribution: the min, the max, the wanted quantile `q` and two helpers at `q/2` and `(1+q)/2`. Each marker has a **height** (the value it estimates) and a **position** (its rank among the observations so far).

#### Step 7 — the update (it is only arithmetic)
For each new value `x`:
1. Find which two markers it falls between; **increment the positions** of all markers above it (the new value pushed them up one rank). Extend min/max if `x` is a new extreme.
2. Move every marker's **desired position** by its fixed increment (`0, q/2, q, (1+q)/2, 1`) — where it *should* be if the data were perfectly distributed.
3. For each of the three middle markers, if it's more than one rank away from where it should be (and moving doesn't cross a neighbour), **shift it one rank** and re-estimate its height by a **parabola** through its neighbours; if the parabola overshoots, fall back to a straight line.

The middle marker's height is your answer. Memory: five heights, five positions — forever.

#### Step 8 — behaviour and limits
- **< 5 values:** the answer is exact (sort and take the nearest-rank element).
- Excellent on smooth distributions (uniform, lognormal in the tests — within ~1 %); weakest in extreme tails and on adversarial orderings; **not mergeable**: two P² sketches cannot be combined. For distributed quantiles use **t-digest** (merges centroids), **KLL** (provable bounds), **DDSketch / HdrHistogram** (bucketed, relative-error). Real systems — Prometheus histograms, Datadog, Elasticsearch `percentiles` — use those.
- Test your quantile code on **sorted input** (the test does): it's a classic way to break streaming estimators.

## 3. Your challenge
`NewReservoir`, `Add`, `Sample`, `Seen`; `NewP2`, `Add`, `Value`, `Count`.

## 4. Hints
<details><summary>Hint 1 — Reservoir.Add</summary>

`r.seen++` first. If `len(sample) < k` append. Else `j := rnd.Intn(r.seen)` — *seen*, which already counts this item, is `n` — and `if j < k { sample[j] = v }`.
</details>
<details><summary>Hint 2 — P² structure</summary>

Fixed arrays `[5]float64` for heights, positions, desired positions, increments. Buffer the first 5 values in a slice, sort them when the 5th arrives and initialise `desired = {1, 1+2q, 1+4q, 3+2q, 5}`.
</details>
<details><summary>Hint 3 — P² adjustment</summary>

`d := desired[i] - pos[i]`; adjust only if `(d >= 1 && pos[i+1]-pos[i] > 1) || (d <= -1 && pos[i-1]-pos[i] < -1)`. Use `sign(d)` as the step. Compute the parabolic candidate; accept it only if it lies strictly between its neighbours' heights.
</details>

## 5. Stretch challenges
1. **Weighted reservoir** (A-Res): key `u^(1/w)` with `u ~ U(0,1)`; keep the `k` largest keys (a min-heap, Chapter 11's heap).
2. **Algorithm L:** skip ahead instead of drawing a random number per item; benchmark on 10⁸ items.
3. **Merge two reservoirs** correctly: draw from each with probability proportional to the number of items each has seen.
4. **Multiple quantiles at once:** extend P² to `2m+3` markers (the paper's generalisation) so one pass yields p50, p90, p99.
5. **A bucketed histogram quantile sketch** (like HdrHistogram): log-spaced buckets with a fixed *relative* error, and show it **merges** by adding counts. Compare with P² on the tail.
6. **Sliding-window quantiles:** keep one sketch per minute; why can't you simply merge P² sketches, and what would you use instead?

## 6. Reflect
- Why is "keep every 1000th item" a poor random sample, and "keep the first k" worse?
- What is the probability that item 5 of a 1 000 000-item stream is in a k=100 reservoir at the end? Item 999 999?
- Why can't you compute the p99 of a fleet from each server's p99?
- Your dashboard shows p95 latency per minute. What do you store per minute so that hour and day views are correct?
