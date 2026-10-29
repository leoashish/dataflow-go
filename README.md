# dataflow-go — async processing, big data and data engineering in Go

A hands-on, **test-first** course. Each of the 22 chapters gives you a lesson, a codebase skeleton with unfinished stubs, and a suite of tests that fail until you build the real thing — then a capstone project you implement and test yourself.

* **Go 1.22+**, standard library only (no third-party modules, no network needed to build).
* **343 tests** across 22 chapters, plus a capstone with a complete acceptance test, a data generator and an exact oracle.
* Every reference solution lives in `_solutions/` (ignored by `go test ./...`), so you can peek when stuck.

## Start here

```bash
go version                      # 1.22 or newer
go test ./...                   # everything fails CLEANLY at first: no panics, no hangs. That is the starting state.
go test ./internal/pipeline -run Ch01 -v
```

For each chapter:

1. **Read the prerequisite** you need (`docs/prereq/`, below), then the **chapter lesson** (`docs/chapters/chNN-*.md`): why it matters, a step-by-step lesson, the challenge, collapsible hints, stretch challenges, reflection questions.
2. **Run the chapter's tests** and read the failures — they are written to tell you what is missing.
3. **Implement** the stubs in `internal/<package>/` (every stub's doc comment is the specification).
4. Get green with `-race`: `go test ./internal/<pkg> -run ChNN -race -v`. Repeat with `-count=20` for the concurrent chapters.
5. Stuck? Use the hints, then read the reference: `_solutions/chNN/…` (same paths as the stubs). To *replace* your files with it: `scripts/apply-solution.sh NN` (**overwrites your work**, commit first).
6. Do the stretch challenges and the reflection questions. Then break your own code on purpose and watch the tests catch it.

Chapter tests are named `TestChNN_…`, so `-run 'Ch07_'` selects one chapter. Solutions to earlier chapters are used by later ones (Chapter 20 needs 10, 15 and 19), so do the chapters in order.

## Prerequisites (read as needed)

| | |
|---|---|
| [01 Go essentials](docs/prereq/01-go-essentials.md) | modules, slices/maps, errors, generics, the test tools and how this course writes tests |
| [02 Concurrency primitives](docs/prereq/02-concurrency-primitives.md) | goroutines, channels, select, context, sync, data races, deadlocks, how to test concurrent code |
| [03 Files, I/O and memory](docs/prereq/03-io-and-memory.md) | latency numbers, `io.Reader`, buffering, `fsync` and the page cache, GC, measuring allocation |
| [04 Algorithm foundations](docs/prereq/04-algorithms-foundations.md) | Big-O at data scale, hashing, probability, heaps, topological order, bit tricks, floating point, mergeable summaries |
| [05 Data-engineering vocabulary](docs/prereq/05-data-engineering-vocabulary.md) | batch vs stream, event time, delivery guarantees, partitioning, CDC, SCD, sketches, a design checklist |

The **[Playbook](docs/PLAYBOOK.md)** maps real problems ("my join is slow", "how do I resume after a crash?") to the technique and the chapter.

## Roadmap

### Part A — Foundations: moving data

| Ch | Topic | Package | Tests | Time | Lesson |
|---|---|---|---|---|---|
| 01 | Pipelines with channels | `internal/pipeline` | 13 | ~90 min | [docs/chapters/ch01-pipelines.md](docs/chapters/ch01-pipelines.md) |
| 02 | Structured concurrency: groups, limits, semaphores | `internal/conc` | 17 | ~90 min | [docs/chapters/ch02-structured-concurrency.md](docs/chapters/ch02-structured-concurrency.md) |
| 03 | Streaming I/O, memory, and splitting big files | `internal/streamio` | 23 | ~2 h | [docs/chapters/ch03-streaming-io.md](docs/chapters/ch03-streaming-io.md) |

### Part B — Streams: time, failure, state

| Ch | Topic | Package | Tests | Time | Lesson |
|---|---|---|---|---|---|
| 04 | Batching and back-pressure | `internal/batch` | 14 | ~2 h | [docs/chapters/ch04-batching-backpressure.md](docs/chapters/ch04-batching-backpressure.md) |
| 05 | Reliable processing: retries, dead letters, at-least-once | `internal/reliable` | 12 | ~2 h | [docs/chapters/ch05-reliable-processing.md](docs/chapters/ch05-reliable-processing.md) |
| 06 | Event time, windows and watermarks | `internal/window` | 17 | ~3 h — the most conceptual chapter of Part B | [docs/chapters/ch06-event-time-windows.md](docs/chapters/ch06-event-time-windows.md) |
| 07 | Checkpoints, atomic writes and effectively-once processing | `internal/checkpoint` | 17 | ~3 h | [docs/chapters/ch07-checkpoints-idempotence.md](docs/chapters/ch07-checkpoints-idempotence.md) |
| 08 | A partitioned log with consumer groups (mini-Kafka) | `internal/plog` | 15 | ~2.5 h | [docs/chapters/ch08-partitioned-log.md](docs/chapters/ch08-partitioned-log.md) |

### Part C — Batch at scale

| Ch | Topic | Package | Tests | Time | Lesson |
|---|---|---|---|---|---|
| 09 | Parsing at scale: CSV, NDJSON, schemas and bad records | `internal/ingest` | 19 | ~3 h | [docs/chapters/ch09-parsing-at-scale.md](docs/chapters/ch09-parsing-at-scale.md) |
| 10 | External merge sort and the k-way merge | `internal/extsort` | 14 | ~3 h | [docs/chapters/ch10-external-sort.md](docs/chapters/ch10-external-sort.md) |
| 11 | Aggregation: mergeable statistics, parallel group-by, MapReduce | `internal/agg` | 17 | ~3 h | [docs/chapters/ch11-aggregation.md](docs/chapters/ch11-aggregation.md) |
| 12 | Joins: hash, sort-merge and grace hash | `internal/join` | 17 | ~3.5 h | [docs/chapters/ch12-joins.md](docs/chapters/ch12-joins.md) |
| 13 | Partitioning, skew and consistent hashing | `internal/partition` | 13 | ~3 h | [docs/chapters/ch13-partitioning.md](docs/chapters/ch13-partitioning.md) |
| 14 | Encodings and a mini columnar file format | `internal/columnar` | 21 | ~4 h — the longest chapter in Part C | [docs/chapters/ch14-encodings-columnar.md](docs/chapters/ch14-encodings-columnar.md) |

### Part D — Approximation

| Ch | Topic | Package | Tests | Time | Lesson |
|---|---|---|---|---|---|
| 15 | The Bloom filter: "definitely not" in 10 bits per item | `internal/sketch` | 8 | ~2.5 h | [docs/chapters/ch15-bloom-filter.md](docs/chapters/ch15-bloom-filter.md) |
| 16 | Count-Min sketch and heavy hitters | `internal/sketch` | 11 | ~3 h | [docs/chapters/ch16-frequency-and-heavy-hitters.md](docs/chapters/ch16-frequency-and-heavy-hitters.md) |
| 17 | HyperLogLog: counting distinct items in kilobytes | `internal/sketch` | 10 | ~3 h | [docs/chapters/ch17-hyperloglog.md](docs/chapters/ch17-hyperloglog.md) |
| 18 | Reservoir sampling and streaming quantiles (P²) | `internal/sketch` | 13 | ~3.5 h | [docs/chapters/ch18-sampling-and-quantiles.md](docs/chapters/ch18-sampling-and-quantiles.md) |

### Part E — Engines and orchestration

| Ch | Topic | Package | Tests | Time | Lesson |
|---|---|---|---|---|---|
| 19 | The write-ahead log: durability, checksums and crash recovery | `internal/wal` | 16 | ~4 h | [docs/chapters/ch19-write-ahead-log.md](docs/chapters/ch19-write-ahead-log.md) |
| 20 | A mini LSM-tree: memtable, SSTables, Bloom filters, compaction | `internal/lsm` | 23 | ~6 h — the capstone of the "systems" half | [docs/chapters/ch20-lsm-tree.md](docs/chapters/ch20-lsm-tree.md) |
| 21 | A DAG orchestrator: dependencies, parallelism, retries and failure | `internal/dag` | 16 | ~4 h | [docs/chapters/ch21-dag-orchestrator.md](docs/chapters/ch21-dag-orchestrator.md) |
| 22 | Upserts, change data capture and slowly changing dimensions | `internal/cdc` | 17 | ~4 h | [docs/chapters/ch22-upserts-cdc-scd2.md](docs/chapters/ch22-upserts-cdc-scd2.md) |

## The capstone: Clickstream Analytics Pipeline

`capstone/` gives the **structure only**: stubs, a CLI, a deterministic dirty-data generator, an independent **oracle** that computes the exact expected answer, **one complete acceptance test** and **twelve test skeletons** for you to write. The implementation and the tests are yours. Start with [capstone/README.md](capstone/README.md) and [capstone/SPEC.md](capstone/SPEC.md).

## Scripts

| | |
|---|---|
| `scripts/test.sh [NN\|all]` (`.ps1`) | run one chapter's tests (or all) with `-race` and a timeout |
| `scripts/apply-solution.sh NN` / `--through NN` / `--list` (`.ps1`) | copy reference solution(s) over your files |

## Things to know

* **Hangs:** a concurrency bug usually hangs rather than crashes. Every test package runs under a **90-second watchdog** (override: `DATAFLOW_TEST_TIMEOUT=30s`) that prints a plain-English explanation and the stack of every goroutine — read the stacks; the stuck goroutines name the line.
* **`-race` needs cgo**, i.e. a C compiler (gcc/clang; on Windows, MinGW). Without one run the tests without `-race`; they still pass, you just lose race detection.
* A directory starting with `_` (`_solutions/`) is ignored by the Go tool. **Don't** copy solutions into `internal/` by hand unless you mean to (use the script).
* If you finish a chapter and a later one's tests fail strangely, check the earlier chapters first: later chapters build on them.

## How this course was verified

* Starting state: all 343 chapter tests fail cleanly — no panics, hangs, build errors or crashed packages — and **no test passes before you write code** (vacuous passes were hunted down and removed).
* Solved state: all 343 pass under `-race` with `go vet` and `gofmt` clean.
* Stress: every chapter's package was re-run repeatedly under `-race` (8–30 times; the concurrent ones also with shuffled test order).
* **Mutation testing:** over 100 realistic bugs were planted one at a time across all 22 chapters and the tests had to fail. Each real gap that surfaced (and there were several) got a new test.
* The capstone spec was proven achievable with a private reference pipeline: it matches the oracle exactly with 1 and 4 workers, resumes identically after being killed at eight points, and the course's own sketches stay within the stated tolerances.
* **Not verified:** Windows and macOS, Go versions other than 1.22.x, and the PowerShell scripts (`*.ps1`) have not been run.
