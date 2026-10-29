# Chapter 08 — A partitioned log with consumer groups (mini-Kafka)

| | |
|---|---|
| **Build** | `Log` (`log.go`), `Group` (`group.go`) — `internal/plog/` |
| **Given** | `Record`, `ErrInvalidPartition`, `ErrUnknownConsumer`; `internal/hashx` |
| **Tests** | `internal/plog/plog_test.go` — 15 tests |
| **Run** | `go test ./internal/plog -run Ch08 -race -v` |
| **Prerequisites** | Chapters 05 and 07; prereq 04 (hashing) |
| **Time** | ~2.5 h |

## 1. Why this matters

Message logs (Kafka, Pulsar, Kinesis, Redpanda) are the **spine of modern data platforms**: producers append events; many independent consumers read them at their own pace, can *replay* history, and recover from crashes by remembering an offset. Understanding this model — partitions, keys, offsets, consumer groups, rebalancing — is core data-engineering literacy, and the whole thing fits in 150 lines of Go.

## 2. The lesson, step by step

### Step 1 — an append-only log
A **log** is a list where you only ever add to the end. Each record gets an **offset** (0, 1, 2…) — its permanent address. Nothing is updated or deleted in place. Consequences: writes are fast (sequential), readers don't interfere with writers, and *any* reader can re-read from any offset (replay = reprocess after a bug fix, add a new consumer, rebuild a table).

### Step 2 — partitions: scaling by splitting
One log is one writer, one reader: a ceiling. Split it into **N partitions** (independent logs). Different partitions can be written and read in parallel. The cost: **ordering is only guaranteed within a partition**.

### Step 3 — the key decides the partition
If all events of user 42 must be processed in order, send them to the **same** partition:

```
partition = hash(key) % N        (same key → same partition → per-key ordering preserved)
```
That is why the hash function must be deterministic *and well distributed* — see `hashx` and prereq 04. A record with **no key** has nothing to keep together, so spread it **round-robin**.

*Hot keys:* if one key (say a celebrity user) sends 50 % of the traffic, its partition is overloaded. That is **skew**, the eternal enemy; Chapter 13 shows fixes.

### Step 4 — consumer groups: parallel reading, exactly one owner
Several consumers form a **group**; each partition is assigned to **exactly one** member at a time. So adding a consumer = more parallelism, and *no record is processed by two members of the same group*. A different group reads the same data independently (analytics vs billing).

Assignment here: sort members by name; partition `p` → `members[p % len]`. Members join/leave → **rebalance** (recompute). (Real Kafka has fancier, "sticky" strategies that move fewer partitions.)

### Step 5 — offsets belong to the *group*, and committing is explicit
The group stores, per partition, the **committed offset**: "everything before this is done". `Poll` reads from the committed offset **without moving it**; the consumer processes the records and *then* `Commit`s. If it crashes in between, the next poll re-delivers → **at-least-once** (Chapter 07's lesson again). Because the offset is the group's, not the consumer's, a **new owner after a rebalance continues exactly where the old one left off**. The test `ANewOwnerResumesFromTheCommittedOffset` shows it.

### Step 6 — concurrency
Producers append from many goroutines: guard the partitions with a `sync.RWMutex` (appends write-lock, reads read-lock). Copy values on the way in (callers reuse buffers) and copy slices on the way out.

## 3. Your challenge
`Log` (`New`, `Partitions`, `Append`, `Read`, `EndOffset`) and `Group` (`Join`, `Leave`, `Assignment`, `Poll`, `Commit`, `Committed`).

## 4. Hints
<details><summary>Hint 1 — Log storage</summary>

`partitions [][]Record`; the offset of a new record is `len(partitions[p])`. A single `roundRobin int` counter (under the same lock) for key-less appends.
</details>
<details><summary>Hint 2 — Assignment</summary>

Collect member names, `sort.Strings`, then `for p := 0; p < n; p++ { if names[p%len(names)] == consumer { owned = append(owned, p) } }`.
</details>
<details><summary>Hint 3 — Poll</summary>

For each owned partition (ascending): `recs, _ := log.Read(p, committed[p], max-len(out))`; stop when `len(out) == max`.
</details>

## 5. Stretch challenges
1. **Retention:** drop records older than N, keep absolute offsets (so offsets no longer equal slice indexes — what changes in `Read`?).
2. **Persist the log** with Chapter 19's WAL — each partition a file; recover on start.
3. **Sticky assignment:** on a rebalance move as few partitions as possible. Count how many move with the naive scheme when a 4th member joins 3.
4. **Compacted topics:** keep only the latest record per key (a changelog that rebuilds a table) — you've met this idea in Chapter 22.
5. **Back-pressure:** `Poll` that blocks (with a context) until data arrives instead of returning empty.

## 6. Reflect
- Why does Kafka only guarantee order *within a partition*, and how does the key work around that?
- More consumers than partitions: what happens to the extra ones?
- A consumer processes a batch, then crashes before `Commit`. What is re-delivered, and what must the processing be?
- Choosing `N` partitions up front: what goes wrong if you pick too few? Too many? Can you change N later, and what happens to key→partition?
