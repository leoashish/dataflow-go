# Clickstream Analytics Pipeline — the specification

This is the precise definition of "correct". If this document and your intuition disagree, **this document wins**; the oracle
(`capstone/oracle`) implements exactly this, slowly and obviously, and the acceptance test compares you with it.

## 1. Input

A directory containing files named `events-NNN.ndjson` (newline-delimited JSON). Process them **in file-name order**, and the lines
of each file **in order**. That sequence is the **input order**: everything below that depends on "before" and "after" means *before
and after in input order*, not by timestamp. Lines may be of any length; the last line of a file may lack its `\n`.

A line that is empty or only whitespace is ignored: not parsed, not counted.

Every other line is one JSON object:

| field | type | required | meaning |
|---|---|---|---|
| `event_id` | string | yes | globally unique id of the event (duplicates exist!) |
| `user_id` | string | yes | |
| `page` | string | yes | e.g. `/p/17` |
| `category` | string | yes | e.g. `shoes` |
| `action` | string | yes | `view`, `click` or `purchase` |
| `amount_cents` | integer | no (default 0) | the price, for purchases |
| `latency_ms` | number | yes | server latency, ≥ 0 |
| `ts` | string | yes | event time, RFC 3339 (`time.RFC3339Nano`), any zone; convert to UTC |

## 2. Validation

Check in this order and reject for the **first** reason that applies. A rejected line is counted and goes no further.

1. **`malformed_json`** — not valid JSON, not an object, or a field has the wrong JSON type (a string where a number is expected, a
   non-integer `amount_cents`, …).
2. **`missing_field`** — a required field is absent or (for strings) empty.
3. **`bad_action`** — `action` is not `view`, `click` or `purchase`.
4. **`bad_timestamp`** — `ts` does not parse as RFC 3339.
5. **`bad_number`** — `latency_ms < 0` or `amount_cents < 0`.

(`oracle.Parse` is the executable version of this section.)

## 3. Processing a valid line

Apply, in this order, to every valid line:

1. **Duplicate?** If an event with the same `event_id` was **seen before** (anywhere in the input, any file), the line is a
   **duplicate**: count it and stop. *Seen* means "passed validation earlier" — **including events that were later found to be late**
   (a late event's id is remembered).
2. **Late?** Let `maxTS` be the largest `ts` of all valid, non-duplicate events processed **before this one** (none yet ⇒ nothing is
   late). The **watermark** is `maxTS − WatermarkDelay`. The event's window is `[start, start + WindowSize)` with `start = ts` truncated to
   a multiple of `WindowSize` (aligned to the Unix epoch, UTC). The event is **late** iff
   `start + WindowSize + AllowedLateness ≤ watermark` — its window had already been closed. Late events are counted and **not
   aggregated** and **not written to `events.col`**.
   After the check, `maxTS = max(maxTS, ts)` (late events never raise it).
3. **Accept.** Otherwise the event is accepted: add it to its window's aggregates and append it to the accepted list.

Defaults: `WindowSize` 5 minutes, `AllowedLateness` 2 minutes, `WatermarkDelay` 1 minute.

Every non-empty line ends in exactly one bucket:
`Read = Σ Rejected[reason] + Duplicates + Late + Accepted`.

## 4. Aggregates

Only windows that received at least one accepted event appear, sorted by `Start`. Per window:

| field | meaning | exact? |
|---|---|---|
| `events` | accepted events in the window | exact |
| `purchases` | accepted events with `action == "purchase"` | exact |
| `revenue_cents` | sum of `amount_cents` over those purchases | exact |
| `by_category` | accepted events per category (zero entries may be omitted) | exact |
| `unique_users` | distinct `user_id` | within **3 %** (≥ 1) of the truth |
| `p95_latency_ms` | the 95th percentile of `latency_ms`; the oracle uses the *nearest-rank* method, element `ceil(0.95·n)` of the sorted values | within **5 %** |
| `top_pages` | the 5 most viewed pages (all actions count), by count descending then page ascending | see below |

`top_pages` is judged leniently so that heavy-hitter sketches qualify: the busiest page must be correct (when it is unambiguous),
every page in **your** top 3 must be in the **true** top 5, and counts of pages present in both lists must be within 2 %.
(An exact map always passes; a Space-Saving with enough counters passes too.)

## 5. Outputs (both written to `OutputDir`, each atomically)

**`report.json`** — the `model.Report` as indented JSON (`json.MarshalIndent`). `Run` also returns it.

**`events.col`** — the accepted events, in input order, in the columnar format of Chapter 14 (`columnar.WriteColumns`), with exactly
these columns in this order:

| # | name | kind | content |
|---|---|---|---|
| 0 | `ts_ms` | int | `ts.UnixMilli()` |
| 1 | `user_id` | string | |
| 2 | `category` | string | |
| 3 | `action` | string | |
| 4 | `amount_cents` | int | 0 when absent |
| 5 | `latency_x100` | int | `model.LatencyX100(latency_ms)` = `round(latency_ms·100)` |

If the run is killed (context canceled) **no** output file may be left half-written, and `report.json` must not exist unless the run
completed.

## 6. Determinism, resume and idempotence

* The output must **not depend on `Config.Workers`** nor on goroutine scheduling: same input ⇒ same bytes (apart from the
  approximate fields, which must also be deterministic for a given configuration).
* With `StateDir` set and `CheckpointEvery = N`, the run checkpoints after every N-th **input line**. Killing the process at *any* moment
  and starting it again with the same arguments must produce **exactly the report an uninterrupted run would have** (no line counted
  twice, none lost, no window double-counted). A torn or corrupt checkpoint must never produce a wrong answer.
* Running a finished job again must not change anything.

## 7. Non-functional goals (yours to measure and defend)

* **Never load a whole input file into memory.** Parse in streaming batches.
* State that grows with the input is allowed only if you can say *what bounds it* (the oracle's exact sets do **not** scale; yours
  should, or you must explain the limit). Think about dedup, per-window user sets, latency lists and the accepted-event buffer.
* Parsing is the CPU-heavy part and parallelises; windowing/dedup/lateness are **order-dependent** and run in one goroutine. Find
  where the ordered hand-off between the two is cheapest.
* Cancellation must be prompt; no goroutine may outlive `Run`.
