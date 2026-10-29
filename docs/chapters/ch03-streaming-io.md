# Chapter 03 — Streaming I/O, memory, and splitting big files

| | |
|---|---|
| **Build** | `Stats`, `WordFrequencies`, `ReadChunks`, `BufferPool`, `LineAlignedSplits`, `ProcessSplit` — `internal/streamio/streamio.go` |
| **Tests** | `internal/streamio/streamio_test.go` — 23 tests |
| **Run** | `go test ./internal/streamio -run Ch03 -race -v` |
| **Prerequisites** | [prereq 03 — I/O and memory](../prereq/03-io-and-memory.md) (`io.Reader`, `bufio`, allocation) |
| **Time** | ~2 h |

## 1. Why this matters

"Big data" on one machine starts the moment the file is bigger than RAM. `os.ReadFile` on a 40 GB log kills the process; reading line by line with the wrong tool silently fails on the one 100 KB line in the file. The cure is **streaming**: hold a tiny window of the data, process it, forget it. Memory use depends on the *answer's* size (distinct words), not the *input's* size.

## 2. The lesson, step by step

### Step 1 — `io.Reader` is the universal pipe
Anything that can produce bytes — a file, a socket, a gzip stream, an HTTP body, your own generator — implements `Read(p []byte) (n int, err error)`. Write functions against `io.Reader`, not `*os.File`, and they work on all of them (and are trivial to test with `strings.NewReader`).

### Step 2 — read rules to remember
- `Read` may return **fewer** bytes than the buffer holds, and may return `n > 0` **together with** an error — always process `p[:n]` first, *then* look at `err`.
- `io.EOF` is not a failure; it's the normal end.
- `io.ReadFull(r, buf)` loops until the buffer is full — the right tool for fixed-size chunks (returns `io.ErrUnexpectedEOF` for a short final chunk, `io.EOF` for none).

### Step 3 — lines: `bufio.Scanner` vs `bufio.Reader`
`Scanner` is convenient but has a hard 64 KiB line limit (`ErrTooLong`). Real data has long lines. `bufio.Reader.ReadSlice('\n')` returns a slice **into its internal buffer** (no allocation!) and signals `ErrBufferFull` if the line is longer than the buffer — then you accumulate pieces. The returned slice is only valid until the next read: copy it if you need to keep it.

### Step 4 — allocation is the hidden cost
Every `string(line)`, every `append` that grows, every `bytes.Buffer` is garbage the GC must collect. At 10 million records, garbage — not your logic — dominates. Three habits:
1. **Reuse buffers** (`ReadChunks` reuses one slice).
2. **Pool** short-lived objects with `sync.Pool` (`BufferPool`) — but never pool giant outliers (cap `maxKeep`).
3. **Measure** before optimising: `go test -bench . -benchmem` shows allocations per operation (see the prerequisite doc).

### Step 5 — streaming word count
Read rune by rune through a `bufio.Reader`, build the current word in a reused `[]rune`, `counts[string(word)]++` when it ends. Memory = number of distinct words. Run it on 200 000 generated lines in the test — the input never exists in memory (`lineGenerator`).

### Step 6 — the central big-data technique: split a file into line-aligned ranges
One reader on a 100 GB file uses one core. To use eight, give each a **byte range**. The problem: a cut at byte 1 000 000 lands in the middle of a line. Hadoop's answer (and now yours):

```
file:   |line 1|line 2 is long........|line 3|line 4|line 5|
ideal cuts:            ^ (middle of line 2)            ^ (middle of line 4)
moved forward to the NEXT LINE START:
                                        ^ (after line 2's '\n')   ^ (after line 4's '\n')
ranges: [0, A) [A, B) [B, size)         every line is wholly inside exactly one range
```
Algorithm: for each ideal cut `c = i*size/n`: if `c` is already a line start (previous byte is `\n`) keep it, else read forward to the next `\n` and cut right after it. Cuts that collapse onto the same line start are merged — that's why 3 lines with n=10 give ≤ 3 ranges. A cut that finds no newline before EOF becomes `size` (no empty range!). Workers then call `ProcessSplit` on their range **independently, no coordination**. This is how Spark, Hadoop, DuckDB and `xsv`/`ripgrep` parallelise a single file.

Edge cases the tests insist on: no trailing newline, one 300 KB line, empty file, `n` larger than the number of lines.

## 3. Your challenge
Implement the six functions in `internal/streamio/streamio.go`. Start with `Stats` (long-line handling is the trap), then `ReadChunks`, `BufferPool`, `WordFrequencies`, and finish with the splits.

## 4. Hints
<details><summary>Hint 1 — Stats with ReadSlice</summary>

Loop on `br.ReadSlice('\n')`. `err == nil` → a full line ended (`cur += len(piece)-1`, count it). `bufio.ErrBufferFull` → a piece of a long line (`cur += len(piece)`, keep going). `io.EOF` → count a last unterminated line only if bytes remain.
</details>
<details><summary>Hint 2 — nextLineStart</summary>

Open a `io.NewSectionReader(f, cut-1, size-(cut-1))`, `ReadSlice('\n')`: if the very first byte is `\n` the cut was already a line start. Handle `ErrBufferFull` by looping, and `io.EOF` by returning `size`.
</details>
<details><summary>Hint 3 — building the ranges</summary>

Start with `starts := []int64{0}`; append each computed start only if it is `> last` and `< size`; ends are the next start (the last end is `size`).
</details>

## 5. Stretch challenges
1. **Benchmark it.** Write `BenchmarkWordFrequencies` and compare `ReadRune` against reading 64 KiB chunks and using `bytes.IndexByte`/manual splitting. Use `-benchmem`; where do allocations come from?
2. Make `WordFrequencies` parallel: `LineAlignedSplits` → one goroutine per split → merge the per-worker maps. (You'll formalise this in Chapter 11.)
3. `gzip`: wrap the reader in `gzip.NewReader`. Why can a gzip file **not** be split into ranges? What do Parquet/Avro/`bgzip` do instead?
4. Replace `os.File` + `SectionReader` with `syscall.Mmap` (Linux/macOS). What does the OS page cache do for you, and what are the risks (page faults, `SIGBUS` on truncation)?

## 6. Reflect
- Why must `fn` in `ReadChunks` not keep the slice after returning?
- What does `bufio.Reader.ReadSlice` give you that `ReadString` doesn't, and what does it cost you?
- With a sync.Pool, why can a "pooled" buffer disappear at any time (hint: GC)?
- A cut falls exactly on a line start. Why must it **stay** rather than move to the next line?
