# The Playbook — from a data-engineering problem to the Go technique

Use this when you meet a problem at work and want to know *which tool from the course applies*. Each row: the problem, why the naive approach fails, the technique, and where you built it.

## A. Throughput and flow
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| One slow step stalls everything | a loop doing read → parse → write serially: CPU idle while I/O waits | a **pipeline** of stages connected by channels; fan out the slow stage | 01 |
| CPU-bound work on millions of items | one goroutine uses one core | **worker pool** / `ParallelMap` preserving order, bounded by a semaphore | 02 |
| Too many goroutines overload a downstream service | `go f()` per item: unbounded concurrency | **semaphore** (`SetLimit`), bounded channels | 02 |
| First error should stop the rest | ignoring errors, or waiting for everyone | **errgroup** (`conc.Group`): keep the first error, cancel the others | 02 |
| A 50 GB file | `os.ReadFile` → out-of-memory | **stream** with `bufio`; read in chunks; split by **line-aligned byte ranges** to parallelise | 03 |
| Producer faster than consumer | an unbounded queue grows until OOM | **back-pressure**: bounded buffer with a policy (block / drop newest / drop oldest) | 04 |
| Per-item overhead dominates (one DB insert per row) | thousands of tiny round trips | **batching by size *or* time** | 04 |

## B. Failure and correctness
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| A flaky dependency | fail the whole job, or retry in a tight loop | **retry with exponential backoff + jitter**, permanent-error short-circuit, **dead-letter queue**, `recover` per record | 05 |
| One poison record kills the pipeline | a panic or error aborts the batch | isolate per record; DLQ with the reason and attempts | 05 |
| Restart after a crash | start over (hours lost) or lose progress | **checkpoint** the offset **atomically** (temp + fsync + rename); resume from it | 07 |
| Duplicates after a retry | double counting | **idempotent** writes; a bounded **dedup** (LRU) keyed by event id | 07, 22 |
| Out-of-order events | apply in arrival order → stale overwrite, resurrected deletes | **last-writer-wins by sequence number** per key + tombstones | 22 |
| "Did the write survive power loss?" | `Write` returned, so yes? (no: page cache) | **WAL**: CRC-framed records, `fsync`, group commit, torn-tail recovery, poison on fsync error | 19 |
| Reading corrupt or hostile bytes | trust lengths → panic or 8 TB allocation | check every length against reality, cap sizes, return a typed error, **measure allocation in tests** | 14 |

## C. Time and windows
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| "Count per 5 minutes" over a late-arriving stream | bucket by arrival time → wrong | **event-time windows** (tumbling/sliding/session), **watermark**, allowed lateness, count late events | 06 |
| Which events belong to a window that is already closed? | silently drop or silently include | explicit **lateness rule** and a `Late` counter that makes the loss visible | 06 |

## D. Batch at scale
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| Messy CSV/JSON | `strconv` and hope | a **schema**, strict typing, `json.Number` for big ints, error **policy** (fail-fast / skip / collect) and a reject file | 09 |
| Sort more data than RAM | `sort.Slice` → OOM | **external merge sort**: sorted runs on disk + **k-way merge** with a heap; multi-pass for large fan-in | 10 |
| `GROUP BY` on all cores | one shared map behind a mutex serialises | **per-worker maps then merge**; mergeable stats (Welford/Chan for variance), `MapReduce` shuffle | 11 |
| Join two big tables | nested loop O(n·m) | **hash join** (build the small side), **sort-merge**, **grace hash** (partition both sides to disk) | 12 |
| Spread data over N workers/shards | `i % N` or alphabet ranges → hot spots; adding a node moves 80 % | **hash/range partitioning**, equi-depth boundaries from a **sample**, **consistent hashing** with virtual nodes; measure the **skew ratio** | 13 |
| Storage and network cost | JSON lines: 10× bigger than needed | **columnar** layout; varint/zigzag, **delta**, **RLE**, **dictionary + bit-packing**; choose per column | 14 |

## E. Approximation (bounded memory)
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| "Have I seen this key?" over billions | a map of every key | **Bloom filter** (no false negatives; sized by n and the target rate) | 15 |
| "How often did this key occur? Which keys are the top K?" | a map of counts for every distinct key | **Count-Min sketch** (never undercounts), **Space-Saving** heavy hitters | 16 |
| `COUNT(DISTINCT user)` | a set of every user | **HyperLogLog** (16 KB, ~1 %), merge by max | 17 |
| A representative sample of an endless stream; p95 latency | keep the first N; sort everything | **reservoir sampling** (Algorithm R); **P²** streaming quantile (not mergeable! use t-digest/KLL when you need to merge) | 18 |

## F. Storage engines and orchestration
| Problem | Naive approach → why it fails | Go technique | Chapter |
|---|---|---|---|
| A write-heavy key-value store | update files in place → random I/O | **LSM-tree**: WAL → memtable → immutable SSTables (+ Bloom filter + sparse index) → **compaction**; tombstones for deletes | 20 |
| Many dependent jobs | a shell script with `&&` | **DAG orchestrator**: Kahn's algorithm, bounded parallelism, retries, skip downstream on failure, cancellation | 21 |
| Keep a warehouse copy of an operational table, with history | nightly full copy; overwrite and lose the past | **CDC** events, **upsert**, log **compaction**, **SCD Type 2** versions with half-open validity intervals | 22 |

## G. "Something is wrong" — where to look
| Symptom | First suspects |
|---|---|
| Throughput stops scaling with workers | an ordered hand-off point, one lock, one hot key (skew), GC pressure (allocations) — **profile** (`pprof`) |
| Memory grows without bound | a map/slice that grows with input; an unbounded channel; goroutines that never exit |
| The job hangs | a channel nobody reads/closes, `WaitGroup` without `Done`, a lock taken twice — read the watchdog's goroutine dump |
| Numbers differ from run to run | map iteration order, goroutine scheduling, missing tie-break, floating-point summation order |
| Numbers differ after a restart | a state that is not in the checkpoint; a side effect done before the checkpoint but not idempotent |
| Reads got slower over weeks | too many files/versions → compaction; Bloom filters too small; skew |
| Totals don't add up | no accounting identity (`read = accepted + rejected + duplicates + late`) — **add one and assert it** |

## H. Habits that make all of it work
1. **An independent oracle**: a slow, obvious implementation to compare against (the capstone has one).
2. **Property tests over random operations** against a trivial model (Chapter 20's map comparison, Chapter 22's shuffle-and-duplicate test).
3. **Crash at every step**: simulate failure at every byte/line/record boundary (Chapter 19, capstone).
4. **Make tests able to fail**: plant a bug, see red, remove it.
5. **Name your guarantees** (at-least-once? idempotent? bounded?) and test each one.
