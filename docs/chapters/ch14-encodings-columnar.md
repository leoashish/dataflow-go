# Chapter 14 — Encodings and a mini columnar file format

| | |
|---|---|
| **Build** | `varint.go`, `intenc.go`, `strenc.go`, `file.go` — `internal/columnar/` |
| **Given** | `ErrCorrupt`, `MaxValues`, `Kind`, `Encoding`, `Column` |
| **Tests** | `internal/columnar/columnar_test.go` — 21 tests |
| **Run** | `go test ./internal/columnar -run Ch14 -race -v` |
| **Prerequisites** | Chapter 03; prereq 03 (bytes, bit operations, `math/bits`) |
| **Time** | ~4 h — the longest chapter in Part C |

## 1. Why this matters

Storage and network are the bill. A table of 50 000 events as JSON lines is **2.5 MB**; the same data stored column by column with the right encodings is **0.2 MB** — and a query that needs only `status` reads *one column's bytes*, not all three. That is *why* Parquet, ORC, ClickHouse, BigQuery and DuckDB store data by column. This chapter rebuilds the core ideas — varints, zigzag, delta, run-length, dictionary, bit-packing — and assembles them into a small file format you can read and write.

## 2. The lesson, step by step

### Step 1 — row vs column layout
```
row store:    (ts1,status1,user1) (ts2,status2,user2) ...      good for "fetch/update ONE record" (OLTP)
column store:  ts1 ts2 ts3 ...  | status1 status2 ...  | user1 user2 ...   good for "aggregate ONE column over MILLIONS of rows" (OLAP)
```
Same-column values are **similar** (timestamps close together, statuses repeating), so each column compresses with a tailor-made trick. A row layout mixes unrelated types and compresses badly.

### Step 2 — varint (LEB128): small numbers, few bytes
An `int64` always takes 8 bytes — wasteful, as most numbers are small. A varint uses **7 payload bits per byte** and the **top bit as "more bytes follow"**:
```
 1   → 01
 127 → 7F
 128 → 80 01      low 7 bits first (with the flag set), then the next 7 bits
 300 → AC 02
```
`for v >= 0x80 { emit(byte(v) | 0x80); v >>= 7 }; emit(byte(v))`. Decoding is the reverse; **careful with malformed input**: more than 10 bytes cannot be a `uint64` (overflow), and running out of bytes mid-number is *truncation*. (The standard library's `encoding/binary` has the same functions — the tests cross-check you against it. You're re-deriving them on purpose.)

### Step 3 — zigzag: negative numbers
`-1` as a `uint64` is `0xFFFFFFFFFFFFFFFF`: 10 varint bytes! **ZigZag** interleaves signs so small magnitudes stay small: `0, -1, 1, -2, 2 …` → `0, 1, 2, 3, 4 …`. `uint64((v << 1) ^ (v >> 63))`. Now `-1` is one byte.

### Step 4 — delta encoding: sorted and slowly changing data
Timestamps `1700000000000, 1700000001003, 1700000002001 …` are huge, but the **differences** (1003, 998 …) are tiny: store the first value then zigzag-varint deltas — 2 bytes per value instead of 6. Great for ids, timestamps, sorted keys, counters. (Arithmetic wraps on overflow and wraps back when decoding, so even `MinInt64, MaxInt64, …` round-trips.)

### Step 5 — run-length encoding: long runs of repeats
`0 0 0 0 0 1 1 1 2` → `(0×5) (1×3) (2×1)`. A column sorted by status, a flag that rarely changes, 1000 zeros → a handful of bytes. (Terrible on random data — it can *double* the size. That's why we **choose** an encoding per column.)

### Step 6 — dictionary encoding + bit-packing: low-cardinality strings
10 million `"country"` values, 200 distinct. Store the 200 strings **once** and replace each value by its **code** (index into the dictionary). Codes need only `BitsNeeded(199) = 8` bits — and with 4 distinct values just **2 bits** each. **Bit-packing** stores every value in exactly `width` bits, back to back, LSB first:
```
width 3, values 5 (101), 2 (010), 7 (111)   →   byte0 = 11010101 = 0xD5, byte1 = 00000001 = 0x01
```
`acc |= uint64(v) << nbits; nbits += width; while nbits >= 8 { emit(byte(acc)); acc >>= 8; nbits -= 8 }`.

### Step 7 — choosing per column
`ChooseIntEncoding` encodes the column all three ways and **keeps the smallest** (ties → the simplest). Real formats do the same with sampling. The file records which encoding each column used so the reader knows how to decode.

### Step 8 — the file format
```
"COL1" | ncols | per column: name, kind, encoding, rowCount, payloadLen, payload
```
Length-prefixed fields everywhere: a reader can **skip a column it doesn't need** by jumping `payloadLen` bytes (that's column pruning).

### Step 9 — untrusted bytes: never trust a length
A file can be truncated by a crash or crafted by an attacker. A decoder that does `make([]int64, count)` with `count = 2⁴⁰` **kills the process** (8 TB). Rules the tests enforce: check every length against the bytes you actually have, cap everything with `MaxValues`, return `ErrCorrupt` instead of panicking, and **never use a size you just decided is invalid** — one test feeds a run length just above `MaxValues` and a count that cannot fit in its few bytes, and *measures how much memory the decoder allocates* (a decoder that sizes an allocation from an unverified header fails fast instead of exhausting your machine); another corrupts bytes 3 000 times and requires *no panic*. (Building this chapter's reference solution, I shipped exactly that bug first: the count was flagged as an error but still returned, and the next line allocated it. The test caught it.)

## 3. Your challenge
`AppendUvarint`/`Uvarint`/`ZigZag`/`UnZigZag`; `Encode/Decode` Plain, Delta, RLE; `ChooseIntEncoding`; `EncodeDict`/`DecodeDict`; `BitsNeeded`/`BitPack`/`BitUnpack`; `WriteColumns`/`ReadColumns`. Layouts are in the doc comments.

## 4. Hints
<details><summary>Hint 1 — decoders share a cursor</summary>

A tiny `cursor{b []byte; err error}` with `uvarint()` that sets `err` on a bad varint and returns 0 afterwards makes every decoder a clean loop with *one* error check at the end.
</details>
<details><summary>Hint 2 — count sanity</summary>

Every value needs ≥ 1 byte, so a count larger than `len(b)` (or above `MaxValues`) is corrupt. Return 0 from your count helper when it is — *before* the caller allocates.
</details>
<details><summary>Hint 3 — RLE bounds</summary>

Reject run length 0, run length > `MaxValues`, and a running total above `MaxValues` — *before* appending.
</details>
<details><summary>Hint 4 — BitUnpack</summary>

Mirror of pack: refill `acc` a byte at a time while `nbits < width`, then `out[i] = uint32(acc & mask)`, `acc >>= width`, `nbits -= width`. Check `len(b) >= (count*width+7)/8` first.
</details>
<details><summary>Hint 5 — string payload</summary>

`u(dictSize)`, each string as `u(len)+bytes`, one byte `width = BitsNeeded(dictSize-1)` (0 for an empty dictionary), then the packed codes. The row count comes from the column header.
</details>

## 5. Stretch challenges
1. **Bit-packed integers (frame of reference):** store `value − min` in `BitsNeeded(max−min)` bits per value; usually beats varint for bounded ranges.
2. **Compress the payloads** with `compress/gzip`/`zstd`-style. Why does encoding-then-compression beat compression alone?
3. **Null support:** add a validity bitmap per column; what does RLE do with it?
4. **Column pruning:** `ReadColumns(r, "status")` that *skips* the other columns' payloads without decoding them. Measure the speed-up.
5. **Statistics for predicate pushdown:** store min/max per column chunk; skip chunks that can't match `ts > X` (this is Parquet's row-group statistics).
6. **Row groups:** write 100 000 rows per chunk so a reader can stream and a writer never holds everything.
7. **Float columns:** XOR-with-previous (Gorilla compression). Why does it work for slowly changing sensor data?

## 6. Reflect
- Why does a column store compress better than a row store holding the same data?
- Why can RLE make some data *bigger*, and how does `ChooseIntEncoding` protect you?
- Where does the "size of one column's bytes" saving come from when a query reads one of ten columns?
- What invariant must every decoder keep when the input is hostile?
