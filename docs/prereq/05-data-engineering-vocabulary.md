# Prerequisite 05 — Data-engineering vocabulary and mental models

*A reference to read once and return to. Every term is tied to the chapter that makes it concrete.*

## 1. The big picture
```
 sources ──▶ ingest ──▶ clean/validate ──▶ transform ──▶ store ──▶ serve
 (apps, DBs,   (files, logs,   (schemas, bad     (join, aggregate,  (files, columnar,   (dashboards, ML,
  devices)      streams, CDC)    records)          window)            warehouse)         APIs)
                       orchestration (when, in what order, with what retries)  ·  monitoring · lineage
```

## 2. Processing styles
- **Batch**: process a bounded dataset ("yesterday's files") on a schedule. Simple, high throughput, high latency. (Chapters 9–14.)
- **Stream**: process an unbounded flow as it arrives. Low latency, must handle disorder, state and failure. (Chapters 1, 4–8, 22.)
- **Micro-batch**: a stream processed in small batches (Chapter 4) — most "streaming" systems are this underneath.
- **Lambda / Kappa architecture**: batch + stream layers vs one replayable log as the source of truth (Chapter 8).

## 3. Time
- **Event time**: when it happened. **Processing time**: when you saw it. They differ — by seconds or days.
- **Window**: a finite slice of event time over an unbounded stream: **tumbling** (fixed, disjoint), **sliding** (overlapping), **session** (gaps of inactivity). (Chapter 6.)
- **Watermark**: the stream's estimate of "event time has progressed to here"; a window may close when the watermark passes it.
- **Late data / allowed lateness**: events arriving after their window closed — count them, drop them, or keep windows open longer.

## 4. Guarantees
- **At-most-once**: may lose, never duplicates. **At-least-once**: never loses, may duplicate. **Exactly-once**: *effect* happens once — achieved by *at-least-once delivery + idempotent processing* (Chapters 7, 22), not by magic.
- **Idempotent**: doing it twice equals doing it once. The most useful property in the field.
- **Checkpoint**: durable record of progress so a restart resumes instead of restarting (Chapter 7, capstone).
- **Back-pressure**: a slow consumer slowing the producer instead of memory exploding (Chapter 4).
- **Dead-letter queue (DLQ)**: where records that cannot be processed go, so one poison record doesn't stop the line (Chapter 5).

## 5. Data layout
- **Row store vs column store**: rows for transactions (OLTP), columns for analytics (OLAP) — less I/O, better compression. (Chapter 14.)
- **Partitioning**: splitting data by key (hash/range) across files or machines; **skew** is a partition much bigger than the rest. (Chapter 13.)
- **Shuffle**: redistributing data so equal keys meet (the expensive step of MapReduce, joins, group-bys). (Chapters 11, 12.)
- **Schema on write vs on read**; **schema evolution** (columns appear, types widen); **data contract** (an agreed schema between producer and consumer).
- **File formats**: CSV/NDJSON (text, easy, big), Avro (row, schema), Parquet/ORC (columnar). **Table formats** (Delta, Iceberg, Hudi) add transactions and snapshots on top of files — LSM and WAL ideas again (Chapters 19, 20).

## 6. Storage engines and logs
- **WAL**: append-only log making writes durable (Chapter 19). **LSM-tree**: memtable + sorted immutable files + compaction (Chapter 20). **B-tree**: in-place updated pages (Postgres, MySQL) — the other design.
- **Compaction**: merging files/versions to bound read cost and space. **Tombstone**: a delete marker. **Write/read/space amplification**: the three costs an engine trades.
- **Log (Kafka-style)**: an ordered, partitioned, replayable sequence with consumer offsets (Chapter 8).

## 7. Keeping copies right
- **OLTP** (operational DB) vs **OLAP** (analytical store). **ETL** (transform before load) vs **ELT** (load raw, transform in the warehouse).
- **CDC**: stream the source DB's changes (Chapter 22). **Upsert/merge**: insert-or-update by key.
- **SCD (slowly changing dimension)**: Type 1 overwrite, Type 2 keep history with validity intervals, Type 3 keep previous value (Chapter 22).
- **Medallion layers** (bronze raw → silver cleaned → gold aggregated), **backfill** (re-run history), **reprocessing** (replay the log through new logic).

## 8. Approximation
**Sketch**: a small summary answering one question approximately — membership (Bloom), frequency (Count-Min), distinct count (HyperLogLog), quantiles (P², t-digest), top-k (Space-Saving). Accuracy is *bounded and tunable*; memory is *fixed*; most are *mergeable*. (Chapters 15–18.)

## 9. Operations
- **Orchestration**: scheduling and dependency management of jobs (Chapter 21) — DAGs, retries, backfills, SLAs. Tools: Airflow, Dagster, Prefect.
- **Observability**: metrics (counts, lag, latency percentiles), logs, traces; **data quality** checks (row counts, null rates, freshness, duplicates, schema drift) at every stage boundary.
- **Lineage**: where did this number come from? **SLA/SLO**: the promised/target freshness and accuracy.
- **Idempotent, restartable, observable**: the three properties of a job you can sleep next to.

## 10. A checklist for any pipeline you design
1. What is the **key** and is it unique? What if it isn't?
2. Can input arrive **twice, late, out of order, malformed**? What happens to each?
3. What is **bounded** (memory, retries, queue size) and what grows?
4. What is the **unit of progress** and where is it checkpointed?
5. If it crashes **right now**, what is lost, duplicated, or corrupt?
6. How will you **know** it is wrong? (Counts that must add up; an independent oracle.)
7. How does it behave at **10× and 100×** the data — and with one hot key?

## Exercises
1. Take a pipeline you know and classify each stage: batch/stream, delivery guarantee, idempotent or not.
2. Draw the event-time/processing-time diagram for a mobile app that uploads events when it regains signal.
3. For each term in sections 4–7, name the chapter test that demonstrates it.
