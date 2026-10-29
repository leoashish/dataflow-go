# Chapter 07 — Checkpoints, atomic writes and effectively-once processing

| | |
|---|---|
| **Build** | `Store` (`store.go`), `Deduper` (`dedup.go`), `Run` (`run.go`) — `internal/checkpoint/` |
| **Given** | `Record`, `Source`, `Sink`, `Options`, `MemorySource`, `ErrNotFound` |
| **Tests** | `internal/checkpoint/checkpoint_test.go` — 17 tests |
| **Run** | `go test ./internal/checkpoint -run Ch07 -race -v` |
| **Prerequisites** | Chapter 05; prereq 03 (files); prereq 05 (delivery semantics) |
| **Time** | ~3 h |

## 1. Why this matters

Your nightly job processes 40 million records and the machine reboots at record 31 million. Without a plan you restart from zero (hours lost) or — worse — restart from "where it seems to have got to" and silently skip or duplicate data. Every serious pipeline answers three questions: **Where was I?** (checkpoint), **Can I trust that answer?** (atomic writes), and **What if I repeat something?** (idempotence).

## 2. The lesson, step by step

### Step 1 — the three delivery guarantees

| Guarantee | Means | How you get it |
|---|---|---|
| at-most-once | each record 0 or 1 times — may **lose** data | save the checkpoint **before** processing |
| at-least-once | each record ≥ 1 times — may **duplicate** | save the checkpoint **after** processing |
| exactly-once | exactly 1 | **impossible** in general across a crash… but you can fake it: |

**Effectively-once = at-least-once delivery + idempotent processing.** Repeat a record and the end state is the same as if you hadn't. That is the whole trick, and it's how Kafka+database pipelines really work.

### Step 2 — the checkpoint ordering rule
`write to sink` → *then* `save checkpoint`. A crash in between replays a few records (harmless if idempotent) but loses none. The test `NeverCheckpointsPastWhatTheSinkAccepted` pins it down: a checkpoint covering an unwritten record would **lose** it forever.

```
offsets:   0 1 2 3 4 | 5 6 7 8 9 | 10 ...      CheckpointEvery = 5
written:   ✔ ✔ ✔ ✔ ✔   ✔ ✘(crash)
checkpoint saved at 5 (after record 4)
restart ⇒ resume at 5 ⇒ record 5 is written AGAIN (duplicate), 6.. are new
```

### Step 3 — idempotent vs not
- **Idempotent**: `UPSERT by key`, `SET x = 5`, `PUT /object/123`, "write file `part-0007`" (overwrite). Twice = once.
- **Not idempotent**: `INSERT` without a unique key, `counter += 1`, "append a line", "send an email".

The tests run the same crash against both sinks: the upsert ends with every record exactly once; the append sink shows `k4` twice. That is the lesson in one picture. To make non-idempotent effects safe you can **dedup by ID** (`Deduper`), use a **unique constraint**, or **record the ID in the same transaction** as the effect.

### Step 4 — atomic file replacement (the checkpoint must not break)
`os.WriteFile` = truncate to zero, then write. A crash (or a concurrent reader) in between sees an **empty or half-written** file. The checkpoint — the thing you use to *recover* — is corrupted exactly when you need it. The standard fix:

1. write the new content to a **temporary file in the same directory**;
2. `Sync()` it (force the bytes to the physical disk);
3. `os.Rename(temp, final)` — rename within a filesystem is **atomic**: readers see the whole old file or the whole new one;
4. (best effort) fsync the directory so the rename itself survives power loss.

Leftover `*.tmp` files from a crash are harmless because they never have the final name. One test hammers `Save` and `Load` from two goroutines and checks every read is a *complete* payload.

### Step 5 — bounded dedup with an LRU set
"Have I seen this ID?" A set of *all* IDs ever seen grows without bound. Keep only the most recent *N* (an LRU set): a duplicate within the window is caught; one arriving after eviction slips through. **Always state this trade-off**; size the window to your worst replay. (Chapter 15's Bloom filter answers "ever seen?" in tiny memory — at the price of occasional false alarms.)

## 3. Your challenge
`Store.Save/Load/NewStore`, `Deduper`, and `Run`. Start with `Store` (the idea), then `Run` (the lesson), then `Deduper`.

## 4. Hints
<details><summary>Hint 1 — Save</summary>

`os.CreateTemp(s.dir, name+".*.tmp")` → `Write` → `Sync` → `Close` → `os.Rename(tmp.Name(), filepath.Join(s.dir, name))`. A `defer` that removes the temp file when an error occurred keeps the directory clean.
</details>
<details><summary>Hint 2 — Run's loop</summary>

`next := loadOffset()`; loop `Read(next, batch)`; for each record `sink.Write` → on error return; `written++; sinceCheckpoint++; next = rec.Offset+1`; when `sinceCheckpoint >= every` save `next`. At EOF save once more if `sinceCheckpoint > 0`.
</details>
<details><summary>Hint 3 — Deduper</summary>

`map[string]*list.Element` + `container/list` (front = newest). `Seen`: found → `MoveToFront`, return true; else `PushFront`, evict `Back()` if over capacity, return false.
</details>

## 5. Stretch challenges
1. **Two-phase sink:** write to `part.tmp`, then rename to `part-<n>` after the checkpoint — a file-level exactly-once commit. Why does *that* work with a non-idempotent writer?
2. **Checksum the checkpoint** (CRC32 in the file); on a mismatch fall back to the previous checkpoint. Keep two generations.
3. **Transactional outbox:** store the offset *in the same database transaction* as the written rows. Why does this give true exactly-once into that database?
4. **Parallel partitions:** one checkpoint per partition (a `map[int]int64`), several `Run`s at once. What must be thread-safe?
5. Measure the cost of `Sync()`. Then batch checkpoints less often and quantify the trade-off between *throughput* and *replay size*.

## 6. Reflect
- Why must the temp file be in the **same directory** as the final one?
- If you checkpoint before writing, what failure mode do you get, and for which data would that be unacceptable?
- Is your `Run` + upsert sink *really* exactly-once if the **sink** is a remote service that sometimes succeeds-then-times-out? What does the retry do?
- What would make the `Deduper` window too small in production?
