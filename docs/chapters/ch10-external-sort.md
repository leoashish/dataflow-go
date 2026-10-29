# Chapter 10 — External merge sort and the k-way merge

| | |
|---|---|
| **Build** | `KWayMerge` (`merge.go`), `SortLines` (`sort.go`) — `internal/extsort/` |
| **Tests** | `internal/extsort/extsort_test.go` — 14 tests |
| **Run** | `go test ./internal/extsort -run Ch10 -race -v` |
| **Prerequisites** | Chapter 03; prereq 04 (sorting, heaps, disk vs memory) |
| **Time** | ~3 h |

## 1. Why this matters

Sorting is the building block of big data: `GROUP BY`, `JOIN`, `DISTINCT`, building indexes, deduplicating, producing sorted output files — and the *shuffle* in every distributed engine is a distributed sort. But `sort.Slice` needs the whole dataset in RAM. For 200 GB on a 16 GB machine you need an **external sort**. Here is the complete algorithm, in under 150 lines.

## 2. The lesson, step by step

### Step 1 — the memory/disk asymmetry
RAM is ~100× faster than SSD for random access, ~1000× faster than a network disk — but **sequential** disk I/O is fast (hundreds of MB/s). External sorting is the art of using the disk **only sequentially**: write big sorted chunks, read them back in order. Never seek around.

### Step 2 — phase 1: sorted runs
Read as many lines as the budget allows (`MaxLinesInMemory`), sort them in memory, write them to a temp file — a **run**. Repeat. 1 000 000 lines with a budget of 10 000 → 100 runs. Memory never exceeds one run.

### Step 3 — phase 2: k-way merge
You now have 100 sorted files. The smallest line overall is the smallest of the *first lines* of each file. Keep a **min-heap** with one entry per run (its current head); pop the smallest, write it, and refill the heap from that same run:

```
run0: 3 8 20      heap: {3(r0), 5(r1), 4(r2)}   pop 3 → write 3 → push 8(r0)
run1: 5 6 7             {8(r0), 5(r1), 4(r2)}   pop 4 → write 4 → push 9(r2)
run2: 4 9 11            ...
```
Cost **O(n log k)** time and **O(k)** memory — the heap is tiny. `KWayMerge` is generic and is reused in Chapter 20 (LSM compaction, merging tables newest-first); Chapter 12's stretch challenges suggest building a streaming sort-merge join on it too.

### Step 4 — too many runs: merge in passes
You can't open 100 000 files at once (file-descriptor limits, buffer memory: k × 64 KiB). Merge **groups** of `MaxMergeFanIn` runs into bigger runs, repeat, until few enough remain, then do the final merge into the output. 100 runs, fan-in 4: 100 → 25 → 7 → 2 → final. That's 3 intermediate passes (what `Stats.MergePasses` counts). More passes = more disk I/O: bigger fan-in is faster until memory runs out — a classic trade-off.

### Step 5 — stability
Sort is **stable** if equal items keep their input order — essential when sorting by one field of a multi-field record. Two places can break it: the in-memory sort (`sort.SliceStable`, not `sort.Slice`) and the merge. Fix the merge with a tie-break: when two heads compare equal, take the one from the **lower-numbered run** (earlier input). In multi-pass merges, group **consecutive** runs so the order relationship is preserved.

### Step 6 — hygiene: temp files and errors
Create one private temp directory and `defer os.RemoveAll` — **always**, including on error. A job that leaks 50 GB of temp files each time it fails fills the disk and takes the next job down. A test makes the writer fail mid-merge and checks the directory is empty afterwards.

### Step 7 — an iterator can't return an error
`KWayMerge` takes `func() (T, bool)` iterators. If reading a run file fails halfway, the iterator can only say "no more". Stash the first error in a variable captured by the closure and check it after the merge — a common Go idiom for adapting I/O to an error-free interface.

## 3. Your challenge
`KWayMerge` first (pure algorithm, easy to test), then `SortLines`.

## 4. Hints
<details><summary>Hint 1 — the heap</summary>

`container/heap` over `[]head[T]{value T, src int}`. `Less`: if `less(a,b)` true; else if `less(b,a)` false; else `a.src < b.src`. After emitting the root, either replace it with the next value from the same source and `heap.Fix(h, 0)`, or `heap.Pop` when that source is exhausted.
</details>
<details><summary>Hint 2 — phase 1</summary>

`bufio.Reader.ReadString('\n')` handles any line length. Trim the `\n`, append to `chunk`; when `len(chunk) == limit` sort (stably) and write to `run-000042` in your temp dir. Flush the last partial chunk at EOF.
</details>
<details><summary>Hint 3 — the pass loop</summary>

```go
for len(runs) > fanIn {
    var next []string
    for i := 0; i < len(runs); i += fanIn { merge runs[i:min(i+fanIn, len(runs))] into one new file; append to next }
    runs = next; st.MergePasses++
}
// final: mergeFiles(runs, less, write-to-w)
```
</details>

## 5. Stretch challenges
1. **Replacement selection:** produce runs about **twice** the memory size on average using a heap while reading. Why does that halve the work in phase 2?
2. **Parallel runs:** sort chunks concurrently with Chapter 02's `Group` while the reader keeps reading (bounded by a semaphore so memory stays bounded).
3. **Compress the runs** with `compress/gzip`: trade CPU for I/O. When does it pay?
4. **Dedupe while merging** (`sort -u`): skip a line equal to the previous output.
5. **Sort by key columns** of NDJSON/CSV (extract the key once per line, not on every comparison).
6. Compare with `sort -S 64M` on a large file. How close are you?

## 6. Reflect
- Why must the in-memory sort be `SliceStable` for the *whole* sort to be stable?
- What limits the fan-in in practice? What happens if it is too large? Too small?
- Where else have you seen "sorted runs + merge"? (Hint: LSM trees, Spark shuffles, `git gc`.)
- How would you sort 1 TB across 20 machines using only what you built here?
