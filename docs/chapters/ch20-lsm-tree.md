# Chapter 20 — A mini LSM-tree: memtable, SSTables, Bloom filters, compaction

| | |
|---|---|
| **Build** | `Memtable` (`memtable.go`), `WriteSSTable`, `OpenSSTable`, `SSTable` (`sstable.go`), `DB` (`db.go`) — `internal/lsm/` |
| **Given** | `Entry`, `KV`, `Options`, `Stats`, `BlockEntries`, `SSTableMagic`, the WAL operation codec (`lsm.go`) |
| **Tests** | `internal/lsm/lsm_test.go` — 23 tests, including a randomized model test |
| **Run** | `go test ./internal/lsm -run Ch20 -race -v` |
| **Prerequisites** | **Chapters 10, 15 and 19** — this chapter *integrates* the k-way merge, the Bloom filter and the WAL |
| **Time** | ~6 h — the capstone of the "systems" half |

## 1. Why this matters

LevelDB, RocksDB, Cassandra, HBase, ScyllaDB, InfluxDB, ClickHouse's MergeTree, and the storage engines under CockroachDB, TiDB and Kafka Streams all share one architecture: the **Log-Structured Merge tree**. It exists because of one fact — *on disks, sequential writes are 10–1000× faster than random writes* — and it turns *every* write into a sequential one. You now own all the parts; this chapter assembles them into a working key-value store and makes you feel every trade-off.

## 2. The lesson, step by step

### Step 1 — the design in one picture
```
Put(k,v) ──▶ WAL (append, durable) ──▶ MEMTABLE (sorted, in RAM)
                                           │ full
                                           ▼ flush (one sequential write)
        Get(k) looks here first ◀── SSTable 3 (newest)   ┐
                                    SSTable 2             │ immutable sorted files,
                                    SSTable 1 (oldest)    ┘ each with a Bloom filter + sparse index
                          Compaction merges many tables into one
```
No write ever modifies a file in place. Files are written once, read many times and eventually deleted.

### Step 2 — the memtable and the tombstone
New writes land in memory (after the WAL, Chapter 19) in a sorted structure. **A delete is a write, not an erase:** the key may exist in an older SSTable that you cannot modify, so you record a **tombstone** entry that *shadows* older versions. This is the idea people find least obvious — and the source of the nastiest LSM bugs ("my deleted row is back!"). `Memtable.Get` therefore must say "found, and it is a tombstone", which is different from "not found here — keep looking".

### Step 3 — the SSTable: sorted, immutable, self-indexed
A flush writes the memtable out as a **sorted string table**: entries in key order, plus acceleration structures built when it is opened:
- a **Bloom filter** of every key (Chapter 15) — "definitely not here" costs no disk access;
- a **sparse index**: the first key and file offset of every 16th entry (a *block*). A lookup binary-searches the index, then reads **one block** — not the file.

Write it to `name.tmp`, `fsync`, then **atomic `rename`** (Chapter 7's trick): a reader sees the whole table or none of it. The tests check the header bytes, one-block reads (also by *corrupting block 0 on disk and proving lookups never touch it*), and that damaged files are rejected with `ErrCorruptTable` rather than half-read.

### Step 4 — the read path: newest first, first answer wins
`Get`: memtable → table N (newest) → … → table 1. **The first entry found decides**; a tombstone means "deleted — stop". In the worst case a lookup touches every table. That's the price of fast writes — **read amplification** — and why the Bloom filters matter: for an absent key, 5 tables cost 5 in-memory filter checks instead of 5 disk reads (the test measures it).

### Step 5 — compaction: paying the debt
More tables ⇒ slower reads, and old versions and tombstones waste space (**space amplification**). **Compaction** merges tables: a k-way merge (Chapter 10's `KWayMerge`!) over the tables **newest first** — the merge's stability rule "equal keys: lower-numbered source first" means the first entry per key *is* the newest. Keep that one, skip the rest. When you merge **all** tables, nothing older exists below, so **tombstones can finally be dropped**. (If you merged only some tables you must keep them — a mistake that resurrects deleted data.) Then write the result, swap, delete the old files. The same data is rewritten again and again over its life: **write amplification** — the third corner of the LSM trade-off triangle (read / write / space). Real engines use *leveled* or *size-tiered* strategies to choose what to merge when.

### Step 6 — recovery
On `Open`: load the tables (higher file number = newer), **replay the WAL** into a fresh memtable, continue numbering after the highest table. After a flush the WAL is reset. A crash between "table written" and "WAL reset" replays entries that are already in the table — **harmless because every operation is idempotent** (the memtable just shadows identical data). Designing operations to be idempotent is what makes recovery simple.

### Step 7 — what this version deliberately cuts
- Compaction isn't crash-atomic: a crash between writing the merged table and deleting the old ones can resurrect deleted keys. Real engines keep a **MANIFEST** — a log of "table X added, tables Y,Z removed" — so the live set is always defined by one atomic record.
- `Open` reads every file fully to build filters/indexes; real tables store them in a **footer**.
- One big lock; no snapshots/iterators; no multi-level compaction; no compression; strings only.

### Step 8 — how it is tested
Besides targeted tests there is a **model-based test**: 4 000 random operations (put, delete, get, flush, compact, restart) are applied to your store *and* to a plain Go `map`; every `Get` must agree, and at the end a full `Scan` must equal the sorted map. This style — comparing a complex implementation against a trivially correct model — finds bugs no hand-written case would. (Planted bugs it caught while building this course: a tombstone that failed to shadow older tables; compaction keeping the *oldest* version; a WAL never replayed.)

## 3. Your challenge
`Memtable` → `WriteSSTable`/`OpenSSTable`/`Get`/`Entries` → `DB` (`Open`, `Put`, `Delete`, `Get`, `Flush`, `Scan`, `Compact`, `Close`, `Stats`).

## 4. Hints
<details><summary>Hint 1 — the entry codec</summary>

`binary.AppendUvarint(buf, uint64(len(key)))`, key bytes, one flag byte (0 put / 1 tombstone), `AppendUvarint` of the value length, value bytes. Parse with `binary.Uvarint` and check every length against the bytes you have — a damaged file must give `ErrCorruptTable`, not a panic.
</details>
<details><summary>Hint 2 — the sparse index</summary>

While scanning at `Open`, whenever `i % BlockEntries == 0` append `{key, offset}`. In `Get`: `b := sort.Search(len(index), func(i int) bool { return index[i].key > key }) - 1`; block bytes are `[index[b].off, index[b+1].off)` (or the end of the data for the last block); read with `f.ReadAt`.
</details>
<details><summary>Hint 3 — merging newest-first</summary>

One slice of entries per source, **newest first** (memtable, then tables in order). Feed them as iterators to `extsort.KWayMerge` with `less = a.Key < b.Key`; in `emit` remember the last key and skip repeats — the first one you see per key is the newest.
</details>
<details><summary>Hint 4 — flush</summary>

`WriteSSTable(next, mem.Entries())`, open it, prepend to `tables`, new memtable, then `wal.Close()`, `os.Remove(wal.log)`, `wal.Open` again. Don't forget auto-compaction afterwards — and don't recurse: `Compact` flushes first.
</details>

## 5. Stretch challenges
1. **MANIFEST:** an append-only file (use your WAL!) of "add table N / remove tables …" records; `Open` reads it instead of globbing, making compaction crash-atomic.
2. **Footer:** write the Bloom filter and sparse index at the end of the SSTable so `Open` reads only the footer (needs Bloom serialisation).
3. **Skip-list memtable** with an iterator that works while writes continue.
4. **Leveled compaction:** tables in levels 0..N; level *i+1* is 10× level *i* and non-overlapping; compact one table into the overlapping ones below. Compare write amplification with "merge everything".
5. **Snapshots / iterators:** a `Scan` cursor that sees a consistent view while writes continue (sequence numbers per entry).
6. **Make block reads observable:** inject an `io.ReaderAt` and count bytes — the current tests cannot distinguish "reads its own block" from "reads from its block to the end of the file".
7. **Block compression** and a block cache; measure the read/space trade-off.
8. **TTL:** entries with an expiry that compaction drops.

## 6. Reflect
- Why is a delete a *write* in an LSM-tree, and what goes wrong if compaction drops a tombstone too early?
- Why do reads touch many files, and which two mechanisms keep that affordable?
- Why is it safe to replay a WAL record whose data is already in an SSTable?
- Explain read, write and space amplification with an example for each. What does compaction trade?
- You see read latency rising steadily over a week while writes stay fast. What do you check?
