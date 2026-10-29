# Prerequisite 03 — Files, I/O and memory

*Chapters 3, 7, 10, 14, 19 and 20 depend on this. ~2.5 hours.*

## 1. The numbers every data engineer carries
| Operation | Rough latency |
|---|---|
| CPU cycle / L1 cache hit | 0.3–1 ns |
| main-memory access | 100 ns |
| read 1 MB **sequentially** from RAM | ~10 µs·ᵃ |
| SSD random read | 100 µs |
| read 1 MB sequentially from SSD | ~0.2–1 ms |
| spinning disk seek | 5–10 ms |
| `fsync` on an SSD | 0.1–10 ms |
| network round trip within a data centre | 0.5 ms |
| a cross-continent round trip | 100+ ms |

ᵃ order of magnitude. **Lesson:** sequential access is vastly cheaper than random access, disks are ~10⁵× slower than RAM, and `fsync` is the most expensive thing you do routinely. Chapters 10 (external sort), 19 (WAL) and 20 (LSM) are all designs that turn random I/O into sequential I/O.

## 2. `io.Reader`, `io.Writer` and friends
```go
type Reader interface{ Read(p []byte) (n int, err error) }
type Writer interface{ Write(p []byte) (n int, err error) }
```
Everything streams through these: files, network connections, gzip, buffers. **`Read` may return fewer bytes than asked, with or without an error**; `io.EOF` marks the end; use `io.ReadFull` when you need exactly `n` bytes. Compose them: `bufio.NewReader(gzip.NewReader(file))`. `io.SectionReader` reads a byte range of a file (Chapter 3's splits); `ReaderAt` (`file.ReadAt`) reads at an offset without moving a cursor — safe for concurrent use (Chapter 20).

## 3. Buffering
Each `Read`/`Write` on a file is a **system call** (microseconds). Wrap with `bufio.Reader`/`bufio.Writer` to batch thousands of small operations into few large ones. **Always `Flush()` a `bufio.Writer`** before closing the file — forgetting it silently loses the tail (a bug the Chapter 12 and 14 writers avoid).

## 4. Reading lines without surprises
`bufio.Scanner` is convenient but **stops with an error on lines longer than 64 KiB** by default. For arbitrary line lengths use `bufio.Reader.ReadString('\n')` / `ReadBytes('\n')` or `ReadSlice` with care (the returned slice is only valid until the next read — copy it if you keep it). The last line may have no trailing newline: handle `io.EOF` *with* data.

## 5. What "written" means: the page cache and `fsync`
`file.Write` copies bytes into the OS **page cache** and returns. The kernel writes them out later; the disk has its own cache. `file.Sync()` (`fsync`) forces the data to stable storage. Consequences:
- a crash can lose acknowledged writes unless you `fsync` (Chapter 19);
- to replace a file atomically: write a **temp file in the same directory**, `Sync` it, `os.Rename` it over the target (rename is atomic), and `Sync` the **directory** for durability (Chapter 7);
- if `fsync` fails, do **not** retry — the data may already be gone (Chapter 19).

## 6. Binary data
`encoding/binary` (`LittleEndian.PutUint32`, `binary.AppendUvarint`) writes numbers compactly; a **varint** uses 1 byte for small numbers (Chapter 14). Be explicit about **endianness** and about **lengths of untrusted data**: never allocate `make([]byte, n)` from a length you read from a file without checking it against what can possibly be there (Chapter 14's lesson, learned the hard way while building it).

## 7. Memory: stack, heap, garbage collection
Go allocates on the **stack** when it can prove a value does not outlive the function (fast, free) and on the **heap** otherwise (**escape analysis**; see with `go build -gcflags=-m`). The **garbage collector** reclaims the heap concurrently; the more you allocate, the more GC work. Reduce it by: pre-sizing slices/maps, reusing buffers (`sync.Pool`, Chapter 3), avoiding `string(bytes)` copies in hot loops, and processing in **batches** instead of building giant slices. Memory limits matter: a Go program that holds 10 GB because it read a whole file is a design bug, not a tuning problem.

## 8. Measuring
`runtime.ReadMemStats` (`TotalAlloc`, `HeapAlloc`) in a test measures allocations around a call (Chapter 14's tests); `testing.AllocsPerRun` counts allocations per call; `go test -benchmem`; `pprof` heap profiles (`-memprofile`). **Measure before optimising** — the capstone asks you to.

## 9. Bounded memory is a design constraint
Ask of every structure: *what grows with the input?* A `map` of every key ever seen grows forever (Chapter 7's LRU bounds it; Chapter 15's Bloom filter shrinks it; Chapter 16–18 sketches fix its size). External algorithms (Chapter 10) trade memory for sequential disk passes.

## Exercises
1. Count the lines of a 2 GB file in constant memory, with lines longer than 1 MB.
2. Write a file atomically (temp + sync + rename) and demonstrate with a "crash" (kill the process) that the target is never half-written.
3. Use `testing.AllocsPerRun` to compare building a string with `+=` in a loop, `strings.Builder`, and a pre-sized `[]byte`.
4. Explain why `defer f.Close()` after a `bufio.Writer` without `Flush` can lose data.
