# Chapter 09 — Parsing at scale: CSV, NDJSON, schemas and bad records

| | |
|---|---|
| **Build** | `Coerce` (`coerce.go`), `ReadCSV`, `ReadNDJSON` (`read.go`) — `internal/ingest/` |
| **Given** | `FieldType`, `Field`, `Schema`, `Row`, `ErrorPolicy`, `BadRecord`, `ErrBadRecord`, `ErrMissingColumn` |
| **Tests** | `internal/ingest/ingest_test.go` — 19 tests |
| **Run** | `go test ./internal/ingest -run Ch09 -race -v` |
| **Prerequisites** | Chapter 03 (streaming I/O); prereq 05 (data quality) |
| **Time** | ~3 h |

## 1. Why this matters

Real data is dirty. A "clean" CSV from a partner has a row with an unescaped quote, a price written `N/A`, a date in the wrong format, and a column renamed last Tuesday. A pipeline that crashes on the first bad row never finishes; one that silently skips them produces numbers nobody can trust. **Ingestion is where data quality is won or lost**, so it deserves a deliberate design: a *schema*, *typed* values, an explicit *policy* for bad records, and *streaming* so file size never matters.

## 2. The lesson, step by step

### Step 1 — parse once, into types
Text is untyped. The sooner you convert `"42"` to `int64(42)` and `"2026-03-01T10:00:00Z"` to a `time.Time`, the sooner bugs surface *at the door* rather than three stages later as a mysterious `0`. A **schema** (`[]Field{Name, Type, Required}`) states what you expect; a `Row` is the typed result. `Coerce` is the one function that knows how to turn a cell into a Go value — keep all parsing rules in one place.

### Step 2 — be strict about what a number is
`strconv.ParseInt` rejects `"1.5"` and `"12x"` — good. `ParseFloat` happily accepts `NaN` and `Inf`, which poison every later `sum` and `mean` — so **reject them at the door** (a number you can't add is a data bug). Normalise times to **UTC** so `10:00Z` and `15:30+05:30` compare equal. Strings are returned **untouched** (whitespace can be data).

### Step 3 — CSV: header by name, not by position
Match columns by header *name*: files get reordered, columns get added. Extra columns are ignored; a **required** column missing from the header is a *file-level* error (the whole file is unusable — fail before reading any row, whatever the policy); a missing *optional* column is just `nil`. Use `encoding/csv` (it handles quotes, embedded commas and escaped `""` for you) with `FieldsPerRecord = -1` so ragged rows don't kill the reader, and `FieldPos` for the line number.

### Step 4 — one bad record is not a bad file: choose a policy
| Policy | Behaviour | When |
|---|---|---|
| `FailFast` | stop at the first bad record: error `line N: …` | trusted sources, tests, "this should never happen" |
| `Skip` | drop it silently | **rarely** — you will never learn how bad the data is |
| `Collect` | keep going, **return every bad record** (line, raw text, reason) | production: write a *reject file*, alert if the rate rises |

`Collect` is the grown-up default: the good data flows, the bad data is *kept and counted*. Wrap everything with `%w` onto `ErrBadRecord` so callers can recognise the class of problem with `errors.Is`.

### Step 5 — NDJSON and the precision trap
Newline-delimited JSON (one object per line) is the format of event logs. Two traps:

1. **Long lines.** `bufio.Scanner` stops at 64 KiB. Use `bufio.Reader.ReadBytes('\n')`.
2. **float64 corrupts big integers.** `json.Unmarshal` into `map[string]any` turns every number into `float64`, which can only represent integers exactly up to 2⁵³ ≈ 9.0×10¹⁵. A 64-bit ID like `9007199254740993` silently becomes `…992` — two different users collapse into one. Fix: `dec := json.NewDecoder(...); dec.UseNumber()` keeps the digits as a `json.Number` you convert with `Int64()`/`Float64()`.

Be **strict about types** too: the JSON string `"12"` is *not* a number; accept it and you'll accept `"12abc"` next week.

### Step 6 — streaming means a callback
Both readers take a `yield func(Row) error`. Rows are handed over one at a time and forgotten — a 100 GB file needs the memory of one row. The callback's error stops the read (back-pressure, or "the sink is full").

## 3. Your challenge
`Coerce`, `ReadCSV`, `ReadNDJSON`. The tests include a 200 000-row generated CSV and a 300 KB JSON line.

## 4. Hints
<details><summary>Hint 1 — a shared rejection helper</summary>

Both readers need the same policy logic; write one small type holding `policy` and `[]BadRecord` with a `reject(line, raw, err) error` that returns non-nil only when reading must stop (FailFast).
</details>
<details><summary>Hint 2 — CSV header & rows</summary>

`header, _ := cr.Read()`, build `index := map[string]int{}`, check required columns, then loop `record, err := cr.Read()`; on `*csv.ParseError` use `pe.StartLine`; otherwise `line, _ := cr.FieldPos(0)`.
</details>
<details><summary>Hint 3 — typed JSON values</summary>

After `dec.UseNumber()`, a JSON number arrives as `json.Number`, a string as `string`, `true` as `bool`, `null` as `nil`. `switch` on the schema type and type-assert the value.
</details>

## 5. Stretch challenges
1. **Reject file:** write each `BadRecord` to `rejects.ndjson` with line, raw and reason as you stream.
2. **Column profile:** while reading, track null rate, min/max and distinct-count estimate (you'll have HyperLogLog in Chapter 17) per column. Alert when a column's null rate jumps.
3. **Schema drift:** detect added/removed/renamed columns between files and report them instead of failing.
4. **Parallel parse:** use Chapter 03's `LineAlignedSplits` + Chapter 02's `ParallelMap` to parse one huge NDJSON file on all cores; merge results *in order*.
5. **Decimal money:** add a `Decimal` field type stored as integer cents. Why must money never be `float64`?

## 6. Reflect
- Why is "skip bad rows silently" dangerous even if 99.99 % of rows are fine?
- Why does a missing *required column* abort the whole file regardless of policy, while a bad *cell* does not?
- A user ID `9007199254740993` appears in your output as `…992`. Where was the bug, and how would you have caught it with a test?
- What should happen if the reject rate suddenly goes from 0.01 % to 8 %?
