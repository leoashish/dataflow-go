# Chapter 19 — The write-ahead log: durability, checksums and crash recovery

| | |
|---|---|
| **Build** | `EncodeRecord`, `DecodeRecord`, `ScanRecords` (`record.go`); `WAL`, `Open`, `Append`, `AppendBatch`, `Sync`, `Close`, `ReadAll` (`log.go`) — `internal/wal/` |
| **Given** | `HeaderSize`, `MaxRecordSize`, `ErrTruncated`, `ErrCorrupt`, `ErrClosed`, `ErrTooLarge` (`wal.go`) |
| **Tests** | `internal/wal/wal_test.go` — 16 tests |
| **Run** | `go test ./internal/wal -run Ch19 -race -v` |
| **Prerequisites** | Chapter 07 (atomic files); prereq 03 (files, `fsync`, the page cache) |
| **Time** | ~4 h |

## 1. Why this matters

Every system that says "your write is safe" relies on a trick: **write the intention to a log first, flush it to disk, and only then do the slow work**. Postgres, MySQL, SQLite, Kafka, etcd, RocksDB and every message queue are built on a write-ahead log (WAL). After a crash the log is *replayed* and the system returns to the last acknowledged state. Get the details wrong and the failure appears once a year, in production, as silent data loss. This chapter builds the whole mechanism — and tests it by "crashing" at **every possible byte offset**.

## 2. The lesson, step by step

### Step 1 — "the write returned" does not mean "the data is on disk"
`f.Write(b)` copies bytes into the **operating system's page cache** and returns. Seconds later the OS flushes them to the disk — whose controller has its **own** cache. Pull the power in between and the data is gone, although your program was told "success". The system call that asks for the data to be made durable is **`fsync`** (`f.Sync()` in Go). It is *expensive* (milliseconds, sometimes tens), which shapes everything below.

### Step 2 — why a log, and why it is fast
A database stores data in complicated structures (B-trees, hash files) updated **in place** — random writes, several pages per update, and a crash in the middle leaves a half-updated, corrupt structure. A WAL instead **appends** one small record per change: sequential, one `fsync`, no corruption risk (we only ever add at the end). Acknowledge the client once the record is durable; update the real structures later; after a crash, **replay the log**. Chapter 20's LSM-tree uses exactly this.

### Step 3 — frame every record: length, checksum, payload
A log is a byte stream; you need to know where each record starts and ends and whether it is intact:
```
| 4 bytes  | 4 bytes | payload  |
| length   | CRC-32C | ...      |       little-endian; the CRC covers the LENGTH bytes followed by the payload
```
Why a **checksum**: after a crash the last record may be half written; a disk can flip bits; a bug can overwrite a block. Without a checksum the reader would parse garbage as data. Why it covers the **length** too: a corrupt length makes you misjudge where the *next* record begins. CRC-32 detects every single-bit error and every burst up to 32 bits — the test flips **every bit of a record, one at a time** and demands detection. (CRC protects against accidents, not attackers: use a MAC like HMAC for that.)

### Step 4 — two different failures: truncated vs corrupt
- **Truncated**: the bytes end in the middle of a record. This is the *normal* result of a crash during an append. Silently repair it.
- **Corrupt**: the record is all there but its checksum is wrong (bit rot, a stray write). Also, a length above `MaxRecordSize` is corrupt, **never merely "truncated"** — a legitimately torn write can't produce a 4 GiB length, and you must not try to read or allocate that much.

`DecodeRecord` checks, **in this order**: header complete? → length plausible (`> MaxRecordSize` = corrupt) → body complete? → checksum matches? Reordering changes the answers (the test for a huge length catches it).

### Step 5 — recovery: keep the valid prefix, cut the rest
`ScanRecords` decodes records until one fails and reports `validBytes`. **Everything after the first bad record is untrustworthy** — you cannot know where the next record starts. `Open` therefore **truncates the file to `validBytes`** before appending. If it didn't, new records would be written *behind* the garbage and every future reader would stop at the garbage and never see them (a classic bug: the database "works" until the next restart).

The crash test makes a six-record log, cuts it at **each of its 100-odd byte offsets**, opens each cut file, and demands: exactly the records that were *completely* on disk survive, and a new append afterwards is readable. If your `Open` passes that, it is correct for every possible crash point.

### Step 6 — group commit: amortise the fsync
One `fsync` per record caps you at ~100–1000 records/s. **Batch**: write 100 records with *one* write call and *one* `fsync` — 100× the throughput, same durability for each caller. `AppendBatch` does it; every production log has some form of this ("group commit"; Kafka's `linger.ms`, Postgres' `commit_delay`). The test counts `fsync` calls through the injectable `SyncFunc`.

### Step 7 — fsyncgate: never retry a failed fsync
If `fsync` fails, the kernel may have already marked the dirty pages clean and dropped them; a **second `fsync` then returns success although the data is gone**. PostgreSQL and others lost data to exactly this assumption (2018). The only safe reaction is to treat the log as **poisoned**: remember the first sync error and return it from every later call. The test injects a failure on the second sync and demands that the third `Append` fails *without calling fsync again*.

### Step 8 — details that bite
- **Atomicity of the framing, not of the write:** a record is "committed" only when the whole of it is intact; there is no partial record.
- **Concurrent appenders:** one mutex around "encode + write + (sync)"; sequence numbers come out contiguous, and each goroutine's own records stay in order.
- **Close syncs once** and is idempotent; use-after-close is an error, not a panic.
- **Oversize check before writing anything:** a rejected batch must leave *no* partial effect.
- **Directory entries:** creating a *new* log file also needs the *directory* fsynced to be durable (Chapter 7's store does this; the WAL stretch asks you to).

## 3. Your challenge
`EncodeRecord`, `DecodeRecord`, `ScanRecords`, then `Open`, `Append`, `AppendBatch`, `Sync`, `Close`, `Len`, `Recovered`, `ReadAll`.

## 4. Hints
<details><summary>Hint 1 — the CRC over two pieces</summary>

`table := crc32.MakeTable(crc32.Castagnoli)`; `crc := crc32.Update(0, table, lengthBytes)`; `crc = crc32.Update(crc, table, payload)`. Write it with `binary.LittleEndian.PutUint32`.
</details>
<details><summary>Hint 2 — DecodeRecord must copy</summary>

Return `append([]byte(nil), payload...)`: callers reuse their read buffer, and a record that aliases it changes under their feet.
</details>
<details><summary>Hint 3 — Open</summary>

`data, err := os.ReadFile(path)` (a missing file is fine); `records, valid, _ := ScanRecords(data)`; if `valid < len(data)` → `os.Truncate(path, valid)`; then `os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)`.
</details>
<details><summary>Hint 4 — poisoning</summary>

A `poisoned error` field. `syncLocked`: if poisoned return it; call `SyncFunc`; on error store it *and return it*. Check `poisoned` at the top of `AppendBatch` too.
</details>

## 5. Stretch challenges
1. **Segment the log:** roll to a new file at 64 MB and let `Truncate(beforeSeq)` delete old segments (how Kafka/etcd bound disk use). Recovery now scans only the newest segment.
2. **Checkpoint + replay:** snapshot a state machine (e.g. Chapter 22's table) every N records, store the snapshot's sequence number, and recover = load snapshot + replay the tail.
3. **fsync the directory** after creating the file (and after rolling segments). What can be lost without it?
4. **Background group commit:** collect concurrent `Append` calls for up to 1 ms, write them as one batch with one `fsync`, then release every caller — latency for throughput. Measure.
5. **Detect "lost write" corruption** by chaining: each record's CRC covers the previous record's CRC. What does that catch that per-record CRCs cannot?
6. **Run the crash test on a real "disk":** write a fault-injecting `io.Writer` that drops or tears the last write at a random point and run 10 000 random crash scenarios.

## 6. Reflect
- What exactly does `f.Write` guarantee, and what does `f.Sync` add?
- Why does recovery *truncate* the file instead of just ignoring the bad tail?
- Why is a 4 GiB length "corrupt" rather than "truncated"?
- Why must a failed `fsync` poison the log instead of being retried?
- Your service acknowledges writes at 50 000/s on a disk that does 500 `fsync`/s. What do you change?
