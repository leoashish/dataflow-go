package pipeline_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"dataflow/capstone/gen"
	"dataflow/capstone/model"
	"dataflow/capstone/oracle"
	"dataflow/capstone/pipeline"
	"dataflow/internal/columnar"
)

// The capstone's tests. ONE is complete (the acceptance test: the judge). The rest are SKELETONS: each says what to prove; you write
// them, and the challenge is to write tests that would catch the bug you are most afraid of.
//
// Run:  go test ./capstone/... -race -v

func makeData(t *testing.T) (dir string, expected oracle.Expected) {
	t.Helper()
	dir = t.TempDir()
	if err := gen.Generate(dir, gen.DefaultParams()); err != nil {
		t.Fatalf("generating data: %v", err)
	}
	expected, err := oracle.Compute(dir, model.Config{})
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	return dir, expected
}

// ---------------------------------------------------------------------------------------------------------------------------
// COMPLETE: the acceptance test. Your pipeline must reproduce the oracle's answer on the default dataset.
// ---------------------------------------------------------------------------------------------------------------------------

func TestCapstone_AcceptanceMatchesTheOracle(t *testing.T) {
	in, want := makeData(t)
	out := t.TempDir()

	got, err := pipeline.Run(context.Background(), model.Config{InputDir: in, OutputDir: out, Workers: 4})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if diffs := model.Diff(got, want.Report, model.DefaultTolerances); len(diffs) > 0 {
		t.Fatalf("the report differs from the oracle in %d places, e.g.:\n  %v", len(diffs), diffs[:min(8, len(diffs))])
	}

	// report.json holds the same report
	data, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatalf("report.json: %v", err)
	}
	var fromFile model.Report
	if err := json.Unmarshal(data, &fromFile); err != nil {
		t.Fatalf("report.json is not valid JSON of a model.Report: %v", err)
	}
	if diffs := model.Diff(fromFile, want.Report, model.DefaultTolerances); len(diffs) > 0 {
		t.Fatalf("report.json differs from the oracle: %v", diffs[:min(5, len(diffs))])
	}

	// events.col holds exactly the accepted events, in order, in the documented columns
	f, err := os.Open(filepath.Join(out, "events.col"))
	if err != nil {
		t.Fatalf("events.col: %v", err)
	}
	defer f.Close()
	cols, err := columnar.ReadColumns(f)
	if err != nil {
		t.Fatalf("events.col is not a readable columnar file: %v", err)
	}
	wantCols := oracle.Columns(want.Accepted)
	if len(cols) != len(wantCols) {
		t.Fatalf("events.col has %d columns, want %d (%s)", len(cols), len(wantCols), "ts_ms, user_id, category, action, amount_cents, latency_x100")
	}
	for i, w := range wantCols {
		g := cols[i]
		if g.Name != w.Name || g.Kind != w.Kind || !slices.Equal(g.Ints, w.Ints) || !slices.Equal(g.Strs, w.Strs) {
			t.Fatalf("column %d (%s) of events.col differs from the oracle's accepted events", i, w.Name)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------------
// SKELETONS: yours to write. Delete the t.Skip line when you start one.
// ---------------------------------------------------------------------------------------------------------------------------

func TestCapstone_ParseLineAgreesWithTheOracleOnEveryLine(t *testing.T) {
	t.Skip("TODO(you): feed every line of a generated dataset (plus hand-made nasty ones: empty object, number as string, huge latency, " +
		"unicode, a 1 MB line) to pipeline.ParseLine and oracle.Parse; reasons and events must be identical.")
}

func TestCapstone_OutputDoesNotDependOnTheNumberOfWorkers(t *testing.T) {
	t.Skip("TODO(you): run with Workers 1, 2, 7 and 32 and require byte-identical report.json and events.col. " +
		"Parsing is parallel but the stateful stage must see lines in INPUT order.")
}

func TestCapstone_ResumeAfterBeingKilledGivesTheSameResult(t *testing.T) {
	t.Skip("TODO(you): cancel the context at line N (a hook, or a source that cancels), run again with the same StateDir, compare with " +
		"the oracle. Try N just before and just after a checkpoint, on a file boundary, and several kills in a row. " +
		"A killed run must not leave report.json behind.")
}

func TestCapstone_ACorruptOrTornCheckpointIsNeverTrusted(t *testing.T) {
	t.Skip("TODO(you): truncate or flip bytes in the checkpoint file, then Run: either fall back to a clean full run or return a clear " +
		"error -- never a wrong report. Also: a crash during SaveCheckpoint must leave the previous checkpoint usable.")
}

func TestCapstone_DuplicatesAcrossFilesAreCountedOnce(t *testing.T) {
	t.Skip("TODO(you): hand-made files where the same event_id appears in file 0 and file 2; first occurrence wins; Duplicates counts the rest. " +
		"Include a duplicate whose FIRST copy was dropped as late (it must still count as a duplicate the second time).")
}

func TestCapstone_LateEventsAreCountedNotAggregated(t *testing.T) {
	t.Skip("TODO(you): hand-made stream around the boundary: an event exactly at the lateness limit, one tick before, one tick after; " +
		"slightly out-of-order events inside the watermark delay must be ACCEPTED.")
}

func TestCapstone_EveryLineIsAccountedForExactlyOnce(t *testing.T) {
	t.Skip("TODO(you): property test over several seeds and dirt rates: Read == Accepted + Duplicates + Late + sum(Rejected), and " +
		"Accepted == sum of window Events == number of rows in events.col.")
}

func TestCapstone_MemoryStaysBoundedOnABigInput(t *testing.T) {
	t.Skip("TODO(you): generate ~2 million lines (-files 20 -events 100000), run, and assert peak heap (runtime.MemStats / a sampler goroutine) " +
		"stays under a budget you choose and justify. Which structure of yours grows with the input, and what bounds it?")
}

func TestCapstone_CancelStopsPromptlyAndLeaksNothing(t *testing.T) {
	t.Skip("TODO(you): cancel mid-run, require Run to return within a second, ctx.Err() as the error, and no goroutines left (testutil.NoLeaks).")
}

func TestCapstone_RunningAFinishedJobAgainChangesNothing(t *testing.T) {
	t.Skip("TODO(you): run twice with the same StateDir and OutputDir: identical Reports, identical output bytes, no duplicated state.")
}

func TestCapstone_EdgeCases(t *testing.T) {
	t.Skip("TODO(you): empty input directory; a file with only bad lines; a file without a trailing newline; a single event; " +
		"a window that receives only late events (it must NOT appear in the report).")
}

func TestCapstone_Throughput(t *testing.T) {
	t.Skip("TODO(you, stretch): a benchmark (go test -bench) of events/second at Workers 1..8. Where does it stop scaling, and why? (Profile it.)")
}
