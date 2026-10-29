# Capstone — the Clickstream Analytics Pipeline

You have built the parts: streaming I/O, pipelines, windows, checkpoints, ingestion, sketches, columnar files. The capstone asks you to
**assemble them into one pipeline that is correct, resumable and fast**, and to **write the tests that prove it**.

> **What is given:** the structure — types, stubs with TODOs, a CLI, a deterministic dirty-data generator, an independent oracle that
> computes the exact expected answer, one complete acceptance test, and a list of test *skeletons*.
> **What is yours:** the implementation, **and the tests** for everything the skeletons describe, plus the stretch goals.

## The scenario

An online shop writes clickstream events as NDJSON files. The data is dirty: malformed lines, missing fields, bad timestamps, duplicate
events (some far apart, in different files), and events that arrive minutes late. You must produce, for every 5-minute event-time
window: counts, revenue, per-category counts, the number of unique users, the p95 latency and the most viewed pages — and store all
accepted events in a compact columnar file. A killed run must be resumable with **exactly** the same result.

`SPEC.md` defines "correct" precisely. Read it twice.

## Layout

```
capstone/
  README.md  SPEC.md
  model/        GIVEN  Config, Event, Report, Diff (the comparison with tolerances)
  gen/          GIVEN  deterministic dirty-data generator        go run ./capstone/cmd/gen -out data
  oracle/       GIVEN  the exact, slow, single-threaded judge     (do not call it from your pipeline!)
  cmd/pipeline/ GIVEN  command line:                              go run ./capstone/cmd/pipeline -in data -out out -state state
  pipeline/     YOURS  doc.go · types.go (given) · run.go · ingest.go · parse.go · state.go · checkpoint.go · sink.go
                       pipeline_test.go: 1 complete acceptance test + 12 skeletons for you to write
```

## Milestones (do them in order; run `go test ./capstone/pipeline -race -v` after each)

| # | Goal | Chapters you will reuse |
|---|---|---|
| **M0** | Read SPEC.md; run the generator and look at the data (`head -3 data/events-000.ndjson`); read `oracle.Parse` and `oracle.Compute` to see the semantics as code | — |
| **M1** | `ParseLine`: implement validation; **write `TestCapstone_ParseLineAgreesWithTheOracleOnEveryLine`** | 09 |
| **M2** | `ReadBatches`: stream the files in order, any line length, cancellable | 03, 01 |
| **M3** | `State.Observe` + `Report`: dedup, lateness, windows, aggregates — single-threaded, exact first. The acceptance test should now pass with `Workers: 1` | 06, 11 |
| **M4** | `WriteOutputs`: `report.json` + `events.col`, written atomically | 14, 07 |
| **M5** | Parallel parsing with **ordered** hand-off; the output must not depend on `Workers` | 01, 02 |
| **M6** | Checkpoint & resume; torn-checkpoint safety; prove it by killing the run at many points | 07, 19, 22 |
| **M7** | Bound the memory: replace exact structures where it is safe and justified (Bloom/HLL/P²/Space-Saving) — measure before and after | 15–18 |
| **M8** | Finish the remaining skeleton tests; run everything under `-race` and `-count=20` | all |

## Rules

* Use the standard library and your own packages from `internal/` (your solutions to the chapters). Do **not** import `capstone/oracle`
  from `capstone/pipeline`.
* You may restructure `capstone/pipeline` freely. The contract is `pipeline.Run`, `report.json`, `events.col`.
* A test you write must be able to **fail**: for each skeleton, break your implementation on purpose and watch the test catch it
  (mutation testing — the course's own tests were checked this way).

## Challenges

1. **Order vs parallelism.** Windows and lateness depend on input order, parsing does not. Design the hand-off so Workers=32 gives the
   same bytes as Workers=1. What happens to throughput as you add workers? Why?
2. **Exactly-once on resume.** After a crash the checkpoint is behind the outputs of the lines processed since. Why is it safe to
   re-process those lines? (Hint: where do side effects happen?) Which of your structures need to be in the checkpoint?
3. **Bound the state.** Dedup over 2 billion events cannot use a `map`. Compare: an LRU window (ch 07), a Bloom filter (ch 15), a
   partitioned on-disk set (ch 13, 20). What does each get wrong, and can you *prove* your choice cannot lose a real event?
4. **Sketches that survive restarts.** HyperLogLog and P² have no serialisation yet. Add it, test the round trip, and make sure a
   resumed run's *approximate* fields are identical to an uninterrupted run's.
5. **Streaming output.** Emit a window's result as soon as it closes (watermark passes `End + AllowedLateness`) instead of at the end —
   and write `events.col` in row groups so memory no longer grows with the accepted events.
6. **Scale it.** `go run ./capstone/cmd/gen -out big -files 40 -events 250000` is ten million lines. Profile (`pprof`), find the top
   three costs, fix them, and report the before/after numbers in `NOTES.md`.
7. **Be the adversary.** Write a *second* generator that produces the nastiest input you can think of (every window boundary, duplicate
   storms, a 50 MB line). Does your pipeline still match the oracle?

## Definition of done

* `go test ./capstone/... -race -count=5` is green, with **no skipped test left**.
* Every skeleton has been turned into a real test that you have seen fail against a deliberately broken implementation.
* `NOTES.md` explains: what bounds your memory, why resume is exactly-once, and what you would change at 100× the data.
