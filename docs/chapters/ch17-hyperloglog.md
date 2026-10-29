# Chapter 17 — HyperLogLog: counting distinct items in kilobytes

| | |
|---|---|
| **Build** | `HLL` — `internal/sketch/hll.go` |
| **Tests** | `internal/sketch/hll_test.go` — 10 tests |
| **Run** | `go test ./internal/sketch -run Ch17 -race -v` |
| **Prerequisites** | Chapters 15–16; prereq 04 (probability, bit tricks) |
| **Time** | ~3 h |

## 1. Why this matters

`SELECT COUNT(DISTINCT user_id)` is the most requested number in analytics — daily active users, unique visitors, distinct devices, unique IPs. Exactly, it needs a set of every id: a billion users ≈ tens of GB, and *merging* two days means unioning two giant sets. **HyperLogLog** gives the answer within about **1 %** using **16 KB**, merges by taking a maximum, and is built into Redis (`PFCOUNT`), BigQuery (`APPROX_COUNT_DISTINCT`), Presto, Spark, Postgres extensions and ClickHouse. The idea is one of the most beautiful in computer science, and it fits in 40 lines.

## 2. The lesson, step by step

### Step 1 — a coin-flip intuition
Flip a fair coin until you get heads and record how many tails came first. If someone tells you their *longest* streak of tails was 20, they have probably flipped about **2²⁰ ≈ a million** times: a streak of k tails has probability 1/2ᵏ, so you expect to need ~2ᵏ attempts to see one. **The longest rare event is a measuring stick for how many trials there were.**

### Step 2 — turn items into coin flips
Hash each item to a (uniformly random-looking) 64-bit number. Its binary digits are fair coin flips. "Number of **leading zeros**" is the tails-streak. Track the maximum over all items, `maxZeros`; the estimate is `2^maxZeros`.
Two beautiful properties:
- the **same item always has the same hash**, so duplicates change nothing — the sketch counts *distinct* items for free;
- the state is **one small number**.

### Step 3 — but one number is hopelessly noisy
One lucky hash with 30 leading zeros and you "estimate" a billion from ten items. Fix: **many independent estimators and average them**. Use the first `p` bits of the hash as a **bucket number** (`m = 2ᵖ` buckets); the remaining bits give the leading-zero count `ρ` for *that bucket*; each bucket keeps its own maximum. Now you have `m` noisy estimates (called **registers**).

### Step 4 — averaging correctly: the harmonic mean
An ordinary average is dominated by the occasional huge register. The **harmonic mean** (average the *reciprocals*) is robust against that:
```
E = α_m · m² / Σ 2^(−register[j])
α_m ≈ 0.7213 / (1 + 1.079/m)      a constant that removes the bias (0.673 / 0.697 / 0.709 for m = 16 / 32 / 64)
```
Standard error ≈ **1.04 / √m**: p = 12 (4 KB) → 1.6 %, p = 14 (16 KB) → 0.8 %, p = 16 (64 KB) → 0.4 %. Quadruple the memory, halve the error. The tests demand about 3 standard errors.

### Step 5 — the details of `Add`
```go
x   := hashx.Sum64(s)
idx := x >> (64 - p)                          // top p bits: which register
w   := x<<p | 1<<(p-1)                        // the remaining bits at the top; a GUARD bit so w is never 0
rho := uint8(bits.LeadingZeros64(w)) + 1      // position of the first 1-bit (1-based)
registers[idx] = max(registers[idx], rho)
```
Why `+1`? The first 1-bit at position `ρ` has probability 1/2^ρ, matching the coin-flip story. Why the guard bit? `LeadingZeros64(0)` is 64, which would pretend we saw a 64-zero miracle.

### Step 6 — small counts: linear counting
With 10 items and 4096 registers, almost every register is still 0 and the harmonic-mean formula is biased. When `E ≤ 2.5·m` and `V` registers are still zero, switch to **linear counting**: `E = m · ln(m / V)` — it is essentially "how many buckets got hit, corrected for collisions" and is nearly exact for small sets. (Real implementations add more corrections — e.g. HyperLogLog++ — see stretch 4.)

### Step 7 — merging: the union, losslessly
Register-wise **maximum**. The merged sketch is *identical* to the one you'd get by feeding both streams into one sketch — so it estimates `|A ∪ B|`, with overlap handled correctly, no matter how the shards overlap. This makes `COUNT(DISTINCT)` **distributable**: each worker sketches its shard (kilobytes), the driver takes the max. `Merge` is idempotent, commutative and associative — safe to retry, to re-order and to combine in a tree. The last test does a 4-worker distributed count with heavy overlap.

### Step 8 — what it cannot do
Tell you *which* items there are, or whether one is present (Bloom filter, Chapter 15), or how often (Count-Min). Intersections and differences are not directly supported (inclusion–exclusion on union estimates is shaky). And `p` is a memory/accuracy dial you must pick up front.

## 3. Your challenge
`NewHLL`, `Add`, `Count`, `Merge`, `Precision`, `SizeBytes`.

## 4. Hints
<details><summary>Hint 1 — registers</summary>

`[]uint8` of length `1<<p`. Store `p` as `uint` so shifts need no conversions.
</details>
<details><summary>Hint 2 — Count</summary>

Loop once: `sum += math.Pow(2, -float64(r))`, count zeros. `est := alpha(m) * m * m / sum`. If `est <= 2.5*m && zeros > 0`, `est = m * math.Log(m/float64(zeros))`. Return `uint64(math.Round(est))`.
</details>
<details><summary>Hint 3 — precision test failing at small n?</summary>

Did you apply the small-range correction? Without it a sketch with 100 items is badly off.
</details>

## 5. Stretch challenges
1. **Empirical error:** run 200 independent sketches of the same 100 000 items (different hash seeds) and plot the distribution of estimates; compare with `1.04/√m`.
2. **Sparse representation:** while few registers are non-zero, store them in a map and only switch to the dense array when it would be bigger (HLL++ does this).
3. **Serialise** the registers compactly (6 bits each suffice) and ship them between "workers".
4. **HyperLogLog++ bias correction** (Google): the empirical-bias tables for `n` between `m` and `5m`.
5. **Intersection estimate** via inclusion–exclusion: `|A∩B| = |A| + |B| − |A∪B|`. Measure how bad it gets when `A∩B` is tiny.
6. **Sliding-window distinct counts:** keep, per register, the *time* of its maximum, so old items can age out.

## 6. Reflect
- Why do repeated items not change the sketch?
- Why is the harmonic mean the right average here?
- Why does merge-by-maximum give the *union* even when the shards overlap?
- You must report unique users per hour, per day and per week from the same stream. What do you store?
