# Chapter 12 — Joins: hash, sort-merge and grace hash

| | |
|---|---|
| **Build** | `HashJoin` (`hash.go`), `SortMergeJoin` (`sortmerge.go`), `GraceHashJoin` (`grace.go`) — `internal/join/` |
| **Given** | `Row`, `Kind`, `Iter`, `SliceIter`, `merge`, `copyRow`, `keyOf`, `GraceOptions`, `GraceStats` |
| **Tests** | `internal/join/join_test.go` — 17 tests |
| **Run** | `go test ./internal/join -run Ch12 -race -v` |
| **Prerequisites** | Chapters 10–11; prereq 04 (hash tables, sorting) |
| **Time** | ~3.5 h |

## 1. Why this matters

Data lives in many tables: orders here, customers there, products somewhere else. Almost every report starts with a **join** — and the join algorithm decides whether the query takes a second or a day. There are exactly three families; every database, Spark, Flink and pandas uses one of them under the hood. You'll build all three and see *when each is the right one*.

## 2. The lesson, step by step

### Step 0 — what a join means
`orders JOIN customers ON orders.cust = customers.cid` pairs every order with the customer row that has the same key. **Inner** join keeps only pairs that match; **left outer** keeps *every* left row (an order with an unknown customer survives, alone). If a key appears 2× on the left and 3× on the right you get **6 rows** (the cross product of that key's groups) — "many-to-many" joins multiply; always know your key's uniqueness.

**NULL keys never match.** A row whose join key is missing or empty matches *nothing* — not even another row with a missing key (SQL's `NULL = NULL` is not true). The given `keyOf` encodes this so you can't forget it.

### Step 1 — the naive way: nested loop, O(n·m)
`for l in left { for r in right { if l.key == r.key { emit } } }`. 100 000 × 100 000 = 10 billion comparisons. Fine for 50 rows, hopeless beyond a few thousand. The tests use it as an independent *oracle* to check your cleverer algorithms.

### Step 2 — hash join: O(n + m)
```
BUILD: for each right row → table[key] = append(rows)       one pass over the smaller side
PROBE: for each left row  → table[left.key] → emit matches  one pass over the other side, O(1) per lookup
```
Build the **smaller** side (it must fit in memory). That is the default join in almost every engine.

### Step 3 — sort-merge join: when sorted data is free
Sort both sides by key, then walk them together like a zipper:
```
left : a a b d d        a=a → pair the 2 left a's with the right a's (cross product)
right: a b b c d        b=b ...   c has no left partner → skip   d=d ...
```
Cost O(n log n + m log m) for the sorts, then O(n + m) to merge, **and constant memory for the merge**. It shines when the inputs are *already sorted* (a clustered index, or the output of Chapter 10's external sort) — then the join is a pure streaming pass with no hash table at all. Output comes out **sorted by key**, which the next `GROUP BY` or `ORDER BY` can use for free.
Careful with **key groups**: when keys are equal you must find the *end of both groups* and emit every pair, then advance both. Keys compare as **text** here: `"10" < "9"` — in real engines the key *type* decides.

### Step 4 — when even the build side doesn't fit: grace hash join
200 GB ⋈ 150 GB, 16 GB of RAM. Idea: **rows can only match if they have the same key — so they have the same `hash(key) % P`.** Cut *both* inputs into P partitions on disk using that hash; then partition *p* of the left can only match partition *p* of the right. Each pair is small enough for an ordinary in-memory hash join:

```
pass 1:  left  ──hash%P──▶ left-0 left-1 … left-(P-1)      sequential writes only (like Chapter 10)
         right ──hash%P──▶ right-0 right-1 … right-(P-1)
pass 2:  for each p: load right-p into a hash table → stream left-p through it → emit
```
Memory = the size of **one** right partition ≈ |right|/P. `GraceStats.MaxBuildRows` measures exactly that so the test can prove you never held the whole build side.

### Step 5 — skew: the limit of partitioning
If one key is 90 % of the data, the partition that gets it is still 90 % of the data. No hash can split **one key**. Real engines handle it separately: broadcast the small side, *salt* the hot key, or fall back to a nested loop for just that key (Chapter 13 and the stretch below). The test `OneHotKey_CannotBeSplitByPartitioning` shows it.

### Step 6 — which one when?
| Situation | Algorithm |
|---|---|
| one side fits in RAM | **hash join** (build the small side) |
| both inputs already sorted | **sort-merge** (no extra memory) |
| neither fits, no useful order | **grace hash** (partition to disk) |
| tiny inputs | **nested loop** is fine |

## 3. Your challenge
`HashJoin`, `SortMergeJoin`, `GraceHashJoin`. Output rows are built with the given `merge` (right column names that clash with a left column get a `_r` suffix).

## 4. Hints
<details><summary>Hint 1 — HashJoin</summary>

`build := map[string][]Row{}` over right rows that have a key; for each left row look up `build[key]`; empty → `LeftOuter` emits `copyRow(l)`, `Inner` drops it; otherwise `merge(l, r)` for every match.
</details>
<details><summary>Hint 2 — SortMergeJoin's group handling</summary>

For each left key group `[i, iEnd)`: advance `j` while `right[j].key < lk`; then find `jEnd` where the right group ends; if `jEnd > j` emit all pairs, else (LeftOuter) emit the left rows alone. Set `i = iEnd` — never advance `j` past the group, because the *next* left group may have the same key only if it was part of this group.
</details>
<details><summary>Hint 3 — GraceHashJoin structure</summary>

Open `P` writers per side (`bufio.Writer` + `json.Encoder`) in a temp dir; stream both inputs through `partition(key) = hashx.Sum64(key) % P`; close (flush!) all writers; then for each `p`: read `right-p` into `map[string][]Row` (record the row count for `MaxBuildRows`), stream `left-p` with a `json.Decoder` and emit matches. `defer os.RemoveAll(dir)` first thing.
</details>

## 5. Stretch challenges
1. **Right and full outer joins**, and a **semi/anti join** (`EXISTS` / `NOT EXISTS`: emit left rows that do / don't have a match, without duplicating on many-to-many).
2. **Recursive grace join:** if a partition's build side is still too big, re-partition *that pair* with a different hash seed (`hashx.Sum64Seed`).
3. **Broadcast join:** join a big stream against a small table held in memory by every worker — no shuffle. When is this the fastest of all?
4. **Skew-aware join:** detect keys that exceed N rows in the build side and handle them with a nested loop; join the rest normally.
5. **Streaming sort-merge:** make `SortMergeJoin` accept two `Iter`s over already-sorted data and use O(group) memory — then feed it Chapter 10's `SortLines` output.
6. **Join on multiple columns / on a range** (`BETWEEN`): what breaks about hashing?

## 6. Reflect
- Why must the *build* side be the smaller one?
- Why does `NULL` never match `NULL`? What would happen to your row counts if it did?
- In the grace join, why do all matching rows end up in the same partition number?
- A join suddenly gets 100× slower after a new customer onboards. What do you check first?
