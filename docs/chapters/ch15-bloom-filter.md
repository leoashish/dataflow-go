# Chapter 15 — The Bloom filter: "definitely not" in 10 bits per item

| | |
|---|---|
| **Build** | `Bloom` — `internal/sketch/bloom.go` |
| **Given** | `ErrShapeMismatch`, `ErrBadParameter` (`sketch.go`) |
| **Tests** | `internal/sketch/bloom_test.go` — 8 tests |
| **Run** | `go test ./internal/sketch -run Ch15 -race -v` |
| **Prerequisites** | Chapter 13; prereq 04 (hashing, probability, bit tricks) |
| **Time** | ~2.5 h |

## 1. Why this matters

A database wants to know "is key X in this 2 GB file?" **before** paying for a disk read. A crawler wants "have I visited this URL?" over 50 billion URLs. A pipeline wants to skip a slow dedup lookup for items it has *certainly* never seen. A `map[string]bool` of a billion strings is 60+ GB. A Bloom filter answers the same question in **~1.2 bytes per item** — by being allowed to be wrong in *one direction only*.

## 2. The lesson, step by step

### Step 1 — the deal: one-sided errors
`Contains(x)` returns one of:
- **false → definitely not in the set** (never wrong);
- **true → probably in the set** (wrong with a probability *you choose*: a **false positive**).

There are **no false negatives**. That asymmetry is what makes it useful: a "no" lets you skip the expensive work with total confidence; a "yes" just means "go check properly".

### Step 2 — how it works
An array of `m` bits (all 0) and `k` hash functions.
```
Add("cat"):       set bits h1("cat"), h2("cat"), …, hk("cat")
Contains("dog"):  are ALL of h1("dog") … hk("dog") set?   one clear bit ⇒ definitely never added
```
A stranger can find all `k` of its bits already set — by coincidence, because *other* items set them. That is a false positive. The fuller the array, the more likely.

### Step 3 — the sizing maths
You choose `n` (items expected) and `p` (false-positive rate you can tolerate):
```
m = ceil( -n · ln(p) / (ln 2)² )          bits
k = round( (m / n) · ln 2 )               hash functions (at least 1)
```
`n = 1000, p = 1 %` → `m = 9586` bits (1.2 KB), `k = 7`. Rule of thumb: **~9.6 bits per item buys 1 %; every extra ~4.8 bits divides the error by 10.** At the optimum, exactly **half the bits are set** — the test checks it (`FillRatio ≈ 0.5`).
Overfilling (4× the planned items) makes the false-positive rate explode: a Bloom filter cannot grow; size it for the *largest* n you expect.

### Step 4 — you don't need k hash functions
Computing `k = 7` independent hashes per item is expensive. **Kirsch–Mitzenmacher:** compute two, `h1` and `h2`, and derive the rest: `index_i = (h1 + i·h2) mod m`. Statistically as good. Make `h2` odd (`| 1`) so the sequence visits many positions. `hashx.Sum64` and `hashx.Sum64Seed` give you the two.

### Step 5 — bits in words
`[]uint64`, bit `idx` lives in `words[idx/64]` at position `idx%64`: set with `|= 1 << (idx%64)`, test with `& (1 << (idx%64))`; count set bits with `bits.OnesCount64`. 64 bits per memory access.

### Step 6 — mergeable
Two filters with the *same shape* (same `m` and `k`) can be **OR-ed** into the filter of the union. In a distributed job each worker builds one over its shard and the driver ORs them. (Different shapes → `ErrShapeMismatch`.) Intersection by AND is *not* exactly the intersection filter — a good question to think about.

### Step 7 — what a Bloom filter cannot do
- **Delete** an item (clearing a bit might erase other items). Variant: *counting* Bloom filters.
- **List** its contents or **count** them (that is HyperLogLog's job, Chapter 17).
- **Grow.** Variant: *scalable* Bloom filters (a chain of filters).

### Step 8 — where you'll meet it
Every LSM-tree database (Cassandra, RocksDB, HBase) keeps one per file so a lookup can skip files that *certainly* lack the key (**you'll use exactly that in Chapter 20**); Chrome's safe-browsing pre-check; CDNs ("don't cache one-hit wonders": cache on the second sighting); join pre-filtering ("send only rows whose key might exist on the other side").

## 3. Your challenge
`NewBloom`, `Add`, `Contains`, `Bits`, `Hashes`, `FillRatio`, `EstimatedFPR`, `Merge`.

## 4. Hints
<details><summary>Hint 1 — a visitor for the indexes</summary>

One helper that loops `i := 0; i < k` computing `(h1 + uint64(i)*h2) % m` and calls a callback lets `Add` and `Contains` share the index logic (the callback returns `false` to stop early).
</details>
<details><summary>Hint 2 — sizing</summary>

`m := math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2))`; `k := max(int(math.Round(m/n*math.Ln2)), 1)`; `words := (m+63)/64`.
</details>
<details><summary>Hint 3 — estimated FPR</summary>

A random probe finds each of its `k` bits set with probability `fill`, independently, so the false-positive rate is `fill^k`: `math.Pow(fill, k)`.
</details>

## 5. Stretch challenges
1. **Counting Bloom filter:** 4-bit counters instead of bits, so `Remove` works. What happens on counter overflow?
2. **Scalable Bloom filter:** when the estimated FPR exceeds the target, start a new, larger filter; `Contains` checks all of them.
3. **Blocked Bloom filter:** put all `k` bits of an item in one 64-byte cache line. Benchmark against yours: why is it faster?
4. **Serialise** the filter (`WriteTo`/`ReadFrom`) so a filter built by one job can be loaded by another.
5. **Use it:** put a Bloom filter in front of Chapter 7's `Deduper` / Chapter 12's join to skip lookups, and measure the saved work.
6. Derive why `k = (m/n)·ln 2` minimises the false-positive rate.

## 6. Reflect
- Why can a Bloom filter have false positives but never false negatives?
- You have 100 million keys and 200 MB of RAM. What false-positive rate can you afford?
- Why is "an AND of two Bloom filters" not the Bloom filter of the intersection?
- Where in a database read path does a "definitely not" save the most?
