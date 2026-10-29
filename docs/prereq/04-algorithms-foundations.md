# Prerequisite 04 — Algorithm foundations for data at scale

*Chapters 10–18 lean on this. ~3 hours. No heavy maths: intuition plus the few formulas you will actually use.*

## 1. Big-O at data scale
At 10⁹ items the constants stop hiding. A feel for what "n" costs (assuming ~10⁹ simple operations per second):

| complexity | n = 10³ | n = 10⁶ | n = 10⁹ |
|---|---|---|---|
| O(1) | instant | instant | instant |
| O(log n) | 10 | 20 | 30 |
| O(n) | 1 µs | 1 ms | 1 s |
| O(n log n) | 10 µs | 20 ms | 30 s |
| O(n²) | 1 ms | **17 min** | **32 years** |

Two more costs matter in data work: **memory** (O(n) memory at n = 10⁹ is gigabytes) and **I/O passes** (reading 1 TB from disk takes minutes; every extra pass is another few minutes). That is why Chapter 10 counts *passes* and Chapter 20 counts *amplification*.

## 2. Hashing
A **hash function** maps a key to a fixed-size number. Properties you rely on: *deterministic* (same key → same hash, across machines), *uniform* (outputs spread evenly), *avalanche* (flip one input bit → about half the output bits flip). They give O(1) lookups (maps), partitioning (`hash % N`, Chapter 13), Bloom filters, HyperLogLog and sketches. FNV-1a (used in `internal/hashx`) is simple but its low bits are weak for similar keys — `hashx` adds a finalising mixer; read its comment. **Not cryptographic:** never use these for security.
**Collisions are inevitable** (pigeonhole). The **birthday paradox**: with `m` possible hashes you expect the first collision after about `√m` items — for 32-bit hashes that is only ~65 000 keys. Use 64-bit hashes for big data.

## 3. Probability you will actually use
- **Expectation is linear:** `E[X+Y] = E[X] + E[Y]`, even if dependent. Reservoir sampling's proof (Chapter 18) is just this.
- **Independence:** `P(A and B) = P(A)·P(B)`. A Bloom filter's false-positive rate `(fill)^k` assumes the `k` bits are independent.
- **Rare events need many trials:** an event of probability `1/2^k` needs ~`2^k` trials. HyperLogLog (Chapter 17) turns that into a counter.
- **Averages beat single estimates:** the standard error of an average of `m` independent estimates shrinks as `1/√m` — quadruple the buckets, halve the error.
- **Union bound / "at least one":** `P(at least one of k)` ≈ `k·p` for small `p`.

## 4. Sorting, heaps, trees
- **Comparison sorts** need Ω(n log n); `sort.Slice` (unstable) vs `sort.SliceStable` (equal items keep input order — needed by Chapter 10).
- **Binary heap** (`container/heap`): push/pop min in O(log n); the engine of top-K (keep the best K in a size-K heap) and of the **k-way merge** (Chapters 10, 20).
- **Binary search** (`sort.Search`): O(log n) on sorted data — sparse indexes (Chapter 20), range partitioning (Chapter 13), consistent hashing (Chapter 13).
- **Balanced trees / skip lists** keep data sorted under inserts; the LSM memtable would use one (stretch).
- **Stability and ties** matter: always define a tie-break (Chapters 13, 16, 21 do) or results change from run to run.

## 5. Graphs and topological order
A **DAG** is a directed graph without cycles. A **topological order** lists every node after its dependencies. **Kahn's algorithm**: repeatedly remove nodes with no remaining dependencies — O(V+E); if nodes remain, there is a cycle (Chapter 21). Applications: build systems, pipelines, query plans, spreadsheets.

## 6. Bit tricks
```go
x & (1<<k - 1)      // keep the low k bits         x >> 7        // drop 7 bits (varint)
x | 1<<k            // set bit k                    x &^ (1<<k)   // clear bit k
bits.OnesCount64(x) // population count             bits.LeadingZeros64(x), bits.Len32(x), bits.TrailingZeros(x)
uint64(v<<1) ^ uint64(v>>63)   // zigzag (Chapter 14)
```
Use them for Bloom filters (bit arrays), HyperLogLog (leading zeros), varints, bit-packing. Know that shifting by ≥ the width gives 0 in Go (no undefined behaviour) and that signed shifts are arithmetic.

## 7. Floating point
`float64` has ~15–16 significant decimal digits. Consequences: **never compare floats with `==`** (use a relative tolerance); **subtracting nearly equal numbers destroys precision** (the naive variance `E[x²]−E[x]²`, Chapter 11 — use Welford); summing millions of floats accumulates error (use compensated or pairwise summation); **money is never `float64`** — use integer cents. `NaN != NaN` and `NaN` poisons every sum: reject it at the door (Chapter 9).

## 8. The external-memory model
When data exceeds RAM, count **disk passes**, not CPU steps: an external sort reads/writes the data `1 + ⌈log_F(runs)⌉` times (F = merge fan-in). Algorithms that are *sequential and mergeable* (sort + merge, hash partitioning, LSM compaction) win; ones with random access to disk lose by orders of magnitude.

## 9. Mergeable summaries — the idea behind Chapters 11 and 15–18
A summary is **mergeable** if `summary(A ∪ B)` can be computed from `summary(A)` and `summary(B)` alone, with the same accuracy. Sum, count, min, max, (count, mean, M2) for variance, Bloom filters (OR), Count-Min (cell-wise add), HyperLogLog (register-wise max) are mergeable; the exact median, exact distinct count and P² are **not**. Mergeability is what lets you shard work across cores and machines.

## Exercises
1. Estimate how long an O(n²) deduplication of 10⁷ records would take, and design an O(n) one.
2. How many 64-bit hashes before a collision becomes likely (birthday bound)?
3. Show that a Bloom filter has no false negatives even when two of a key's `k` bit positions coincide — and explain why a *false positive* needs *all* `k` bits to have been set by other keys.
4. Compute the variance of {10⁹+4, 10⁹+7, 10⁹+13, 10⁹+16} both ways in a Go program; explain the difference.
