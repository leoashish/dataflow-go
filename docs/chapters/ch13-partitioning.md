# Chapter 13 — Partitioning, skew and consistent hashing

| | |
|---|---|
| **Build** | `Hash` (`hash.go`), `RangePartitioner` + `BoundariesFromSample` (`rangep.go`), `Ring` (`ring.go`) — `internal/partition/` |
| **Given** | `SkewRatio`, `ErrBadPartitionCount`; `internal/hashx` |
| **Tests** | `internal/partition/partition_test.go` — 13 tests |
| **Run** | `go test ./internal/partition -run Ch13 -race -v` |
| **Prerequisites** | Chapter 08; prereq 04 (hash functions and avalanche) |
| **Time** | ~3 h |

## 1. Why this matters

"Big data" means *more than one machine, one file or one thread can handle* — so you must **split it**, and the way you split decides everything: whether work is balanced, whether related rows meet, whether you can add a server without a week of migration. Every distributed system — Kafka, Cassandra, Spark, DynamoDB, Redis Cluster, a sharded Postgres — is built on one of three ideas in this chapter.

## 2. The lesson, step by step

### Step 1 — hash partitioning: `hash(key) % N`
Same key → same partition, always, with **zero coordination**: every worker computes the answer independently. A good hash scatters keys evenly, so loads balance. Perfect for lookups, `GROUP BY`, joins (Chapter 12), and writing the shuffle of MapReduce (Chapter 11). Useless for range queries: keys "between a and f" are *everywhere*.

The quality of the hash is not decoration. `hashx` (given) finishes FNV-1a with a mixer *because* FNV's low bits are weak for similar keys (`user-1`, `user-2`…) and `% N` looks at exactly the low bits. Read its comment; the test spreads 100 000 *sequential* keys over 16 partitions and demands a skew ratio under 1.08.

### Step 2 — measuring balance: the skew ratio
`SkewRatio = busiest partition / average partition`. 1.0 is perfect; 3.0 means one worker does 3× its share — and because the *job finishes when the slowest worker does*, a skew of 3 makes your 16-core job run at the speed of ~5 cores. **Skew is the #1 performance problem in distributed data processing.** Measure it.

### Step 3 — range partitioning: keep neighbours together
Cut the *sorted key space* into contiguous ranges (volumes of an encyclopedia): `b0 ≤ … < b1`. Range scans touch one or two partitions; a distributed sort produces globally ordered output files (partition i < partition i+1); Bigtable/HBase/Spanner shard this way. Rule: boundary-equal keys go to the **upper** partition. Look up with **binary search** (`sort.SearchStrings`), O(log n).

### Step 4 — choosing boundaries: sample, don't guess
Evenly spaced letters (`c|f|i`) assume keys are evenly spread over the alphabet. Real keys are not: 70 % of surnames/SKUs/URLs may share a few prefixes. The test builds exactly such data — naive boundaries give a skew above 2. The fix is **equi-depth** boundaries from a **sample**:

```
sample 2 000 keys → sort → boundary j = sorted[j · len/N]     (the j-th N-quantile)
```
A small random sample estimates the distribution well (this is TeraSort's trick). If one key dominates, several quantiles equal it → duplicates collapse (a single key can't be split by range — Chapter 12's hot-key limit again).

### Step 5 — the resharding problem
With `hash(key) % 4`, going to 5 servers changes `% 4` to `% 5` and **~80 % of all keys get a new home**: a cache cluster loses 80 % of its contents, a database must copy 80 % of its data, in the middle of production. (The test measures it.)

### Step 6 — consistent hashing
Picture a **circle** of 2⁶⁴ positions.
1. Hash every **node** onto the circle (many times — "virtual nodes").
2. To place a **key**: hash it onto the circle and walk **clockwise** to the first node point. That node owns it.

```
        node A·          ·key1 → owned by B (next point clockwise)
   ·B                 ·C
        key2→ owned by C            Add node D: it takes over only the arc just before ITS points.
```
**Add a node:** it steals only the arcs just before its points — keys move *only to the new node*, never between existing ones, and only about **1/(N+1)** of them (20 % for 4 → 5) instead of 80 %. **Remove a node:** only *its* keys move, to the next points clockwise. Tests check these exact properties.

### Step 7 — virtual nodes tame the randomness
One point per node means arcs of very different lengths — some nodes own 3× the data. Giving every node **100–200 points** averages the arcs out; the test shows 1 vnode vs 200 vnodes (skew ≈ 2 vs ≈ 1.1). Bonus: a failed node's load spreads over *all* the survivors instead of dumping on one neighbour. Implementation: one sorted slice of `(hash, node)`, `sort.Search` for the clockwise-next point, **wrap to index 0** past the end.

## 3. Your challenge
`Hash`, `NewRangePartitioner` / `Partition` / `Partitions`, `BoundariesFromSample`, and `Ring` (`Add`, `Remove`, `Get`, `Nodes`).

## 4. Hints
<details><summary>Hint 1 — Partition</summary>

`i := sort.SearchStrings(bounds, key)` gives the first boundary `>= key`; if `i < len(bounds) && bounds[i] == key` the key equals a boundary, so `i++`.
</details>
<details><summary>Hint 2 — Ring points</summary>

For each node and `i < vnodes`: `point{hash: hashx.Sum64(fmt.Sprintf("%s#%d", node, i)), node: node}`. After every Add, re-sort by `(hash, node)`. `Remove` filters the slice.
</details>
<details><summary>Hint 3 — Get</summary>

`h := hashx.Sum64(key); i := sort.Search(len(points), func(i int) bool { return points[i].hash >= h }); if i == len(points) { i = 0 }`.
</details>

## 5. Stretch challenges
1. **Weighted nodes:** a node with 2× the capacity gets 2× the virtual nodes.
2. **Replication:** `GetN(key, 3)` returns the next 3 *distinct* nodes clockwise (Dynamo/Cassandra replicas). Why *distinct*?
3. **Rendezvous (HRW) hashing:** the alternative with no ring: the owner is the node with the highest `hash(node+key)`. Compare movement and lookup cost.
4. **Jump consistent hash** (Google, ~10 lines, no memory): `N` buckets, minimal movement when N grows by one — but nodes can't be removed from the middle. Implement and compare.
5. **Hot-key salting:** append a random 0–9 to a hot key to spread it over 10 partitions, aggregate, then merge the 10 partials (Chapter 11).
6. **Bounded loads:** cap each node at 1.25× the average and spill overflow clockwise (Google's "consistent hashing with bounded loads").

## 6. Reflect
- Why does `hash % N` move ~80 % of keys going from 4 to 5 nodes, and consistent hashing only ~20 %?
- Why can range partitioning give *worse* balance than hash partitioning, and when would you still choose it?
- Why do virtual nodes help *both* balance *and* failure recovery?
- A tenant generates 40 % of your traffic. Which partitioning schemes survive that, and which don't?
