# Chapter 11 — Aggregation: mergeable statistics, parallel group-by, MapReduce

| | |
|---|---|
| **Build** | `Stat` (`stat.go`), `GroupBy`, `ParallelGroupBy` (`groupby.go`), `MapReduce` (`mapreduce.go`) — `internal/agg/` |
| **Given** | `MapReduceOptions` |
| **Tests** | `internal/agg/agg_test.go` — 17 tests |
| **Run** | `go test ./internal/agg -run Ch11 -race -v` |
| **Prerequisites** | Chapters 01–02; prereq 04 (hashing, floating point) |
| **Time** | ~3 h |

## 1. Why this matters

`SELECT region, COUNT(*), AVG(amount), STDDEV(amount) FROM sales GROUP BY region` is the most common big-data query there is. To run it on all your cores — or all your machines — you need one idea: **aggregate locally, merge globally**. Each worker summarises its slice; only the tiny summaries are combined. That requires designing aggregates that are **mergeable**, and doing the arithmetic so that merging doesn't wreck the numbers.

## 2. The lesson, step by step

### Step 1 — which aggregates merge, and which don't?
| Aggregate | Partial state | Merge |
|---|---|---|
| count, sum, min, max | the value itself | `+`, `+`, `min`, `max` |
| mean | (sum, count) | add both — *don't* average the averages! |
| variance / stddev | (count, mean, M2) | Chan's formula (below) |
| **median / p95** | needs all the data | **doesn't merge exactly** — use a sketch (Chapter 18) |
| distinct count | needs all the values | doesn't merge exactly — HyperLogLog (Chapter 17) |

Rule of thumb: *if the partial state is smaller than the data and merging is associative and commutative, it parallelises.* Averaging averages is the classic wrong answer: the mean of [1,2,3] and [10] is not the mean of the means.

### Step 2 — why variance is a trap
The textbook shortcut `variance = E[x²] − (E[x])²` subtracts two enormous, almost equal numbers. For values near 10⁹ (timestamps in ms! prices in cents!) the squares are ~10¹⁸, beyond what `float64` (≈16 significant digits) can resolve: the answer is noise, sometimes negative. The test feeds `1e9+4, 1e9+7, 1e9+13, 1e9+16` — the true variance is exactly 30.

### Step 3 — Welford's online algorithm
Keep a running `mean` and `M2` (sum of squared deviations **from the current mean**):
```
Add(x):  count++; delta = x − mean; mean += delta/count; M2 += delta·(x − mean)
```
Stable because it only ever works with *deviations*, which are small. Sample variance = `M2/(count−1)`.

### Step 4 — merging two summaries (Chan et al.)
Combine A and B without revisiting data:
```
n     = nA + nB
delta = meanB − meanA
mean  = meanA + delta·nB/n
M2    = M2A + M2B + delta²·nA·nB/n
```
The last term is the part people forget: the two groups have *different means*, and that spread adds variance. The test checks merged-in-pieces equals single-pass. (Precision note from building this course: with values near 5×10⁸ the last digits legitimately differ between orders — compare floats with a *relative tolerance*, never `==`.)

### Step 5 — parallel group-by without locks
```
items ──split──▶ worker 0 → map₀ ─┐
                 worker 1 → map₁ ─┼─merge→ result        each worker owns its map: NO mutex in the hot loop
                 worker 2 → map₂ ─┘
```
A shared map behind a mutex serialises every update; per-worker maps are contention-free and the merge touches only *groups × workers* entries. Same shape as Spark's "map-side combine".

### Step 6 — MapReduce in one function
```
inputs ─▶ MAP (emit key,value) ─▶ SHUFFLE (hash(key) % reducers) ─▶ REDUCE (per key) ─▶ results
```
Each mapper writes into its **own** per-reducer buckets (no locks); a reducer then gathers *its* bucket from every mapper. `hash(key) % R` guarantees all values of a key meet at one reducer, so each key is reduced **exactly once**. Anything that can be phrased as "emit pairs, then fold per key" — word count, inverted index, log analysis — fits. `reduceFn` must not depend on value order because mappers finish in any order.

**Skew** is the enemy: if one key holds 50 % of the data, one reducer does half the work. (Stretch 3.)

## 3. Your challenge
`Stat.Add/Merge/Mean/Variance`, then `GroupBy`, `ParallelGroupBy`, `MapReduce`.

## 4. Hints
<details><summary>Hint 1 — Add</summary>

First value sets `Min`/`Max`. Then Welford: `s.Count++; delta := v - s.mean; s.mean += delta/float64(s.Count); s.m2 += delta*(v - s.mean)`.
</details>
<details><summary>Hint 2 — Merge</summary>

Handle the empty cases first. Then `n := float64(s.Count+o.Count)`, `delta := o.mean - s.mean`, update `m2` **before** updating `mean` (or use the old values consciously).
</details>
<details><summary>Hint 3 — chunking</summary>

`chunk := (len(items)+workers-1)/workers`; worker `w` handles `[w*chunk, min((w+1)*chunk, len))`. Clamp `lo` too, or the last workers get negative ranges.
</details>
<details><summary>Hint 4 — MapReduce buckets</summary>

`buckets[m][r] map[string][]V`. Mapper `m` handles inputs `m, m+mappers, …`. Reducer `r` loops over all mappers `m`, appending `buckets[m][r][key]` into one gathered map, then calls `reduceFn` once per key.
</details>

## 5. Stretch challenges
1. Add **skewness** and **kurtosis** (third and fourth central moments) with mergeable formulas.
2. **Weighted** `Add(v, w)` and its merge.
3. **Salting hot keys:** two-phase aggregation — first by `key#salt` (random 0–9) to spread a hot key over 10 reducers, then merge the 10 partials. Measure the speed-up on a skewed dataset.
4. **Streaming top-K per group** by combining this with a heap.
5. **Exact distributed median:** you can't merge medians; implement a two-pass "find the bucket containing the median, then drill down" over partitions.
6. Benchmark `ParallelGroupBy` vs a mutex-protected shared map. How does the gap scale with workers?

## 6. Reflect
- Why can't you merge two medians, and what do real systems do instead?
- Why does the per-worker-map design beat a single locked map even though it does extra merge work?
- Which part of Chan's merge formula corrects for the groups having different means? Try deleting it in a test.
- What goes wrong in MapReduce if two different reducers receive the same key?
