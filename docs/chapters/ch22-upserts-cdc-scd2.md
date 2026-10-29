# Chapter 22 — Upserts, change data capture and slowly changing dimensions

| | |
|---|---|
| **Build** | `Table`, `Apply`, `ApplyAll`, `Get`, `Rows`, `Len`, `Compact` (`table.go`); `History`, `Apply`, `AsOf`, `Versions`, `Current` (`history.go`) — `internal/cdc/` |
| **Given** | `Op`, `Event`, `Version`, `OpenEnded`, `ErrOutOfOrder`, `cloneRow`, `sameRow` (`cdc.go`) |
| **Tests** | `internal/cdc/cdc_test.go` — 17 tests |
| **Run** | `go test ./internal/cdc -run Ch22 -race -v` |
| **Prerequisites** | Chapters 07 and 08 (at-least-once, ordering); prereq 05 (data-engineering vocabulary) |
| **Time** | ~4 h |

## 1. Why this matters

Your company's truth lives in operational databases (orders, customers, vessels). Analytics must not query them directly, so the warehouse keeps a **copy** — and keeping a copy *correct* is the oldest unglamorous problem in data engineering. The modern answer is **change data capture (CDC)**: read the database's own log (Postgres WAL, MySQL binlog — via Debezium) and ship each insert/update/delete as an event. Two questions then decide whether your numbers can be trusted: **(1)** can you apply that stream *repeatedly and out of order* and still end up right? **(2)** can you answer "what did this customer's record look like *last March*?" Chapter 22 answers both.

## 2. The lesson, step by step

### Step 1 — the delivery reality: duplicates and disorder
Event streams are **at-least-once** (Chapter 7) and **partitioned** (Chapter 8): you *will* see the same event twice, and an old update *will* arrive after a newer one (retries, replays, parallel consumers, a backfill). "Apply in arrival order" then corrupts the copy in two classic ways:
1. a **stale update overwrites fresh data**;
2. a **late update after a delete resurrects the deleted row** — the ghost that haunts every naive sync job.

### Step 2 — last-writer-wins by *sequence*, not by arrival
Every event carries a **sequence number** from the source log (Postgres' LSN, Kafka's offset). For each key remember the **highest sequence applied**; an event with `Seq ≤ that` is stale or a duplicate — **ignore it**. Now `Apply` is:
- **idempotent** — applying an event twice equals once;
- **commutative** — any arrival order gives the same table.

Those two properties are exactly what a distributed pipeline needs to **retry and reorder freely and still converge** (it is a tiny cousin of a CRDT). The test shuffles a 600-event log, sprinkles in 150 duplicates, applies it 60 different ways, and demands an identical table every time.

### Step 3 — tombstones: deletes must be remembered
Deleting the row isn't enough; record the **delete's sequence number** so an older update arriving later is rejected. And a delete can arrive *before* the insert it deletes: remember it for keys you have never seen. The cost: the tombstone must be kept (forever, or until you're sure no older event can still arrive — a retention-window decision). Tombstones are the same idea as Chapter 20's.

### Step 4 — upsert semantics
`INSERT` for an existing key overwrites; `UPDATE` for an unknown key creates it. Real feeds are messy (a snapshot starts mid-stream; an early event was lost), and the target must absorb that: **merge/upsert** (`MERGE INTO`, `INSERT … ON CONFLICT DO UPDATE`) rather than strict insert/update.

### Step 5 — never alias caller data
Store a **copy** of each `Row` and return copies. Otherwise a caller who later modifies "its" map silently changes your table (or the reverse) — a bug that shows up *far* from its cause. Three tests check input, `Get` and `Rows`.

### Step 6 — log compaction
A changelog grows forever; most of it is overwritten history. **Compaction** keeps only the **latest event per key** (Kafka's compacted topics). Two rules: keep the **deletes** (their tombstones must keep protecting you), and the result must **replay to exactly the same table** as the full log — which the test proves, then also verifies a late event is still rejected.

### Step 7 — the second problem: history (SCD Type 2)
The replicated table only knows *now*. But "which sales region did this customer belong to **when** they ordered in March?" needs the past. **Slowly changing dimensions** are the warehouse's answer:
- **Type 1:** overwrite — the past is lost.
- **Type 2:** keep **every version** with the interval in which it was true — `[ValidFrom, ValidTo)` and a `Current` flag.
- (Type 3: keep just the previous value in a column.)

```
customer 7:   {region: south}  [Jan 1, Mar 10)        closed when the customer moved
              {region: north}  [Mar 10, OpenEnded)    current
```

### Step 8 — SCD2 rules that are easy to get wrong
- **Version only on a real change.** An update carrying identical content creates **no** new version (otherwise the history bloats with noise on every re-sync). Compare *rows*, not events.
- **Half-open intervals:** `ValidFrom` inclusive, `ValidTo` exclusive. `AsOf(ts)` at exactly `ValidTo` belongs to the *next* version.
- **Delete closes** the current version; re-inserting later starts a new one, leaving a **gap** during which `AsOf` finds nothing.
- **Timelines only move forward:** an event dated *before* the latest version's start — or inside an already-closed interval — would make versions **overlap**, so it's rejected (`ErrOutOfOrder`) and changes nothing. (Handling genuinely late data is a stretch challenge.)
- **Invariants worth asserting:** versions don't overlap; only the **last** version can be `Current`; `Current ⇔ ValidTo == OpenEnded`. The tests check them over 1 500 random events — and that History's current rows equal the replicated Table's rows, and that `AsOf` reproduces the table as of any past moment.

### Step 9 — where this appears
Debezium + Kafka → warehouse "bronze/silver" tables; dbt snapshots (SCD2 for free); Delta/Iceberg `MERGE INTO`; Kafka Streams `KTable`; Flink changelog streams; Fivetran/Airbyte incremental syncs.

## 3. Your challenge
`Table` (`Apply`, `ApplyAll`, `Get`, `Rows`, `Len`), `Compact`, then `History` (`Apply`, `AsOf`, `Versions`, `Current`).

## 4. Hints
<details><summary>Hint 1 — Table state</summary>

`rows map[string]map[string]string` for live rows and `lastSeq map[string]uint64` for the highest sequence per key **including deleted keys**. Check staleness before anything else; then set `lastSeq`, then insert/update/delete.
</details>
<details><summary>Hint 2 — Compact</summary>

One pass keeping `latest[key]` (replace when `e.Seq > cur.Seq`), then a slice sorted by `Seq`. Do not filter out deletes.
</details>
<details><summary>Hint 3 — History.Apply</summary>

Get the key's last version. Order check first. Then: delete → close the current version (if any). Insert/update → if there is a current version with `sameRow`, return; otherwise close it at `e.TS` and append `{ValidFrom: e.TS, ValidTo: OpenEnded, Current: true}`.
</details>
<details><summary>Hint 4 — copies everywhere</summary>

`cloneRow` on the way in (`Apply`) and on the way out (`Get`, `Rows`, `AsOf`, `Versions`, `Current`).
</details>

## 5. Stretch challenges
1. **Late data in SCD2:** accept an out-of-order event by *splitting* the version that spans its timestamp and re-deriving the intervals (bi-temporal thinking).
2. **Bi-temporal tables:** keep both *valid time* (when it was true in the world) and *transaction time* (when we learned it). What can you answer now that you couldn't before?
3. **Tombstone retention:** drop a tombstone once the log guarantees no older event can arrive (a watermark, Chapter 6); prove with a test that dropping too early resurrects rows.
4. **Partial updates:** events carrying only changed columns (`before`/`after` diffs); merging them keeps unchanged columns — and what happens when two partial updates arrive out of order?
5. **Schema evolution:** a new column appears mid-stream; old versions have no value. What should `sameRow` do?
6. **Snapshot + stream bootstrap:** load a table snapshot taken at sequence S, then apply only events with `Seq > S`. Why is the overlap harmless here and a disaster for non-idempotent systems?
7. **Wire it up:** feed Chapter 19's WAL records through `Table`, and rebuild the table after a simulated crash.

## 6. Reflect
- Which two properties of `Apply` let you retry and reorder freely, and how does the sequence number provide them?
- Why must a delete be remembered, and for how long?
- When is it right to create a new SCD2 version, and when not?
- A customer moved on March 10th; a report for February orders now shows the new region. Which design choice caused that?
- How do Chapters 7, 8, 19, 20 and 22 fit together into one change-data pipeline?
