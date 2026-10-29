package ingest_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"dataflow/internal/ingest"
	"dataflow/internal/testutil"
)

// Chapter 09 -- parsing at scale.   Run:  go test ./internal/ingest -run Ch09 -race -v

func TestCh09_Coerce_ParsesEachType(t *testing.T) {
	tests := []struct {
		raw  string
		typ  ingest.FieldType
		want any
	}{
		{" 42 ", ingest.Int, int64(42)},
		{"-7", ingest.Int, int64(-7)},
		{"3.25", ingest.Float, 3.25},
		{"1e3", ingest.Float, 1000.0},
		{"Yes", ingest.Bool, true},
		{" n ", ingest.Bool, false},
		{"1", ingest.Bool, true},
		{"FALSE", ingest.Bool, false},
		{"  keep my spaces ", ingest.String, "  keep my spaces "},
		{"2026-03-01T10:00:00Z", ingest.Time, time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)},
		{"2026-03-01T15:30:00+05:30", ingest.Time, time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		got, err := ingest.Coerce(tc.raw, tc.typ)
		if err != nil {
			t.Fatalf("Coerce(%q): %v", tc.raw, err)
		}
		if gt, ok := got.(time.Time); ok {
			if !gt.Equal(tc.want.(time.Time)) || gt.Location() != time.UTC {
				t.Fatalf("Coerce(%q) = %v, want %v in UTC", tc.raw, gt, tc.want)
			}
		} else if got != tc.want {
			t.Fatalf("Coerce(%q) = %#v, want %#v", tc.raw, got, tc.want)
		}
	}
}

func TestCh09_Coerce_RejectsGarbage(t *testing.T) {
	bad := []struct {
		raw string
		typ ingest.FieldType
	}{
		{"12x", ingest.Int}, {"1.5", ingest.Int}, {"", ingest.Int}, {"99999999999999999999", ingest.Int},
		{"abc", ingest.Float}, {"NaN", ingest.Float}, {"Inf", ingest.Float}, {"-inf", ingest.Float},
		{"maybe", ingest.Bool}, {"2", ingest.Bool},
		{"2026-03-01", ingest.Time}, {"yesterday", ingest.Time},
	}
	for _, tc := range bad {
		_, err := ingest.Coerce(tc.raw, tc.typ)
		testutil.ExpectError(t, err)
	}
}

var eventSchema = ingest.Schema{
	{Name: "id", Type: ingest.Int, Required: true},
	{Name: "name", Type: ingest.String, Required: true},
	{Name: "score", Type: ingest.Float},
	{Name: "active", Type: ingest.Bool},
	{Name: "seen", Type: ingest.Time},
}

func collectCSV(t *testing.T, csv string, policy ingest.ErrorPolicy) ([]ingest.Row, []ingest.BadRecord, error) {
	t.Helper()
	var rows []ingest.Row
	bad, err := ingest.ReadCSV(strings.NewReader(csv), eventSchema, policy, func(r ingest.Row) error { rows = append(rows, r); return nil })
	return rows, bad, err
}

func TestCh09_CSV_MatchesColumnsByName_IgnoresExtrasAndFillsOptionalsWithNil(t *testing.T) {
	rows, bad, err := collectCSV(t, "name,extra,id,score\nAda,zzz,1,9.5\nGrace,yyy,2,\n", ingest.FailFast)
	if err != nil || len(bad) != 0 || len(rows) != 2 {
		t.Fatalf("rows=%d bad=%v err=%v", len(rows), bad, err)
	}
	if rows[0]["id"] != int64(1) || rows[0]["name"] != "Ada" || rows[0]["score"] != 9.5 {
		t.Fatalf("row 0 = %#v", rows[0])
	}
	if v, present := rows[1]["score"]; !present || v != nil {
		t.Fatalf("an empty optional cell must be present as nil, got (%v, present=%v)", v, present)
	}
	if v, present := rows[1]["active"]; !present || v != nil {
		t.Fatalf("an optional column that does not exist must be nil in every row, got (%v, %v)", v, present)
	}
	if _, present := rows[0]["extra"]; present {
		t.Fatal("columns outside the schema must be ignored")
	}
}

func TestCh09_CSV_HandlesQuotedCellsWithCommasAndQuotes(t *testing.T) {
	rows, _, err := collectCSV(t, "id,name\n1,\"Lovelace, Ada\"\n2,\"say \"\"hi\"\"\"\n", ingest.FailFast)
	if err != nil || len(rows) != 2 || rows[0]["name"] != "Lovelace, Ada" || rows[1]["name"] != `say "hi"` {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}

func TestCh09_CSV_ARequiredColumnMissingFromTheHeader_IsAFileLevelError(t *testing.T) {
	called := false
	_, err := ingest.ReadCSV(strings.NewReader("id,score\n1,2\n"), eventSchema, ingest.Skip, func(ingest.Row) error { called = true; return nil })
	if !errors.Is(err, ingest.ErrMissingColumn) || called {
		t.Fatalf("err=%v called=%v, want ErrMissingColumn and no rows (even with the Skip policy: the whole file is unusable)", err, called)
	}
}

const dirtyCSV = "id,name,score\n1,Ada,1.5\nx,Bad Id,2\n3,,3\n4,Grace,oops\n5,Lin,5\n"

func TestCh09_CSV_FailFast_StopsAtTheFirstBadRecord_WithItsLineNumber(t *testing.T) {
	rows, _, err := collectCSV(t, dirtyCSV, ingest.FailFast)
	if !errors.Is(err, ingest.ErrBadRecord) || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("err = %v, want an ErrBadRecord that mentions line 3", err)
	}
	if len(rows) != 1 {
		t.Fatalf("%d rows were delivered before the failure, want 1", len(rows))
	}
}

func TestCh09_CSV_Skip_DeliversOnlyTheGoodRows_AndReportsNothing(t *testing.T) {
	rows, bad, err := collectCSV(t, dirtyCSV, ingest.Skip)
	if err != nil || len(rows) != 2 || len(bad) != 0 {
		t.Fatalf("rows=%d bad=%d err=%v, want 2 good rows, nothing reported", len(rows), len(bad), err)
	}
}

func TestCh09_CSV_Collect_ReturnsEveryBadRecordWithItsLine(t *testing.T) {
	rows, bad, err := collectCSV(t, dirtyCSV, ingest.Collect)
	if err != nil || len(rows) != 2 || len(bad) != 3 {
		t.Fatalf("rows=%d bad=%d err=%v, want 2 rows and 3 bad records", len(rows), len(bad), err)
	}
	lines := []int{bad[0].Line, bad[1].Line, bad[2].Line}
	if lines[0] != 3 || lines[1] != 4 || lines[2] != 5 {
		t.Fatalf("bad record lines = %v, want [3 4 5]", lines)
	}
	if bad[0].Raw != "x,Bad Id,2" {
		t.Fatalf("Raw = %q, want the offending cells joined by commas", bad[0].Raw)
	}
	for _, b := range bad {
		if !errors.Is(b.Err, ingest.ErrBadRecord) {
			t.Fatalf("bad record error %v does not wrap ErrBadRecord", b.Err)
		}
	}
}

func TestCh09_CSV_ACsvSyntaxError_IsABadRecord_AndReadingContinues(t *testing.T) {
	rows, bad, err := collectCSV(t, "id,name\n1,Ada\n2,Bad\"quote\n3,Grace\n", ingest.Collect)
	if err != nil || len(rows) != 2 || len(bad) != 1 || bad[0].Line != 3 {
		t.Fatalf("rows=%d bad=%v err=%v, want 2 rows and one bad record at line 3", len(rows), bad, err)
	}
}

func TestCh09_CSV_RaggedRecords_MissingTrailingCellsAreEmpty(t *testing.T) {
	rows, bad, err := collectCSV(t, "id,name,score\n1,Ada\n2,Grace,7\n", ingest.Collect)
	if err != nil || len(bad) != 0 || len(rows) != 2 || rows[0]["score"] != nil || rows[1]["score"] != 7.0 {
		t.Fatalf("rows=%v bad=%v err=%v", rows, bad, err)
	}
}

func TestCh09_CSV_StopsWhenYieldFails_AndOnlyHasAHeader_YieldsNothing(t *testing.T) {
	boom := errors.New("sink full")
	n := 0
	_, err := ingest.ReadCSV(strings.NewReader("id,name\n1,a\n2,b\n3,c\n"), eventSchema, ingest.FailFast, func(ingest.Row) error { n++; return boom })
	if !errors.Is(err, boom) || n != 1 {
		t.Fatalf("err=%v calls=%d, want the sink's error after one call", err, n)
	}
	rows, bad, err := collectCSV(t, "id,name\n", ingest.FailFast)
	if err != nil || len(rows) != 0 || len(bad) != 0 {
		t.Fatalf("header-only: rows=%d bad=%d err=%v", len(rows), len(bad), err)
	}
}

// csvStream generates a CSV of n rows on the fly: the file never exists in memory.
type csvStream struct {
	n, i    int
	pending []byte
	started bool
}

func (c *csvStream) Read(p []byte) (int, error) {
	if len(c.pending) == 0 {
		switch {
		case !c.started:
			c.started = true
			c.pending = []byte("id,name,score\n")
		case c.i < c.n:
			c.pending = []byte(fmt.Sprintf("%d,user-%d,%d.5\n", c.i, c.i, c.i%100))
			c.i++
		default:
			return 0, io.EOF
		}
	}
	k := copy(p, c.pending)
	c.pending = c.pending[k:]
	return k, nil
}

func TestCh09_CSV_StreamsAHugeFile(t *testing.T) {
	count, sum := 0, 0.0
	bad, err := ingest.ReadCSV(&csvStream{n: 200_000}, eventSchema, ingest.FailFast, func(r ingest.Row) error {
		count++
		sum += r["score"].(float64)
		return nil
	})
	if err != nil || len(bad) != 0 || count != 200_000 {
		t.Fatalf("count=%d bad=%d err=%v", count, len(bad), err)
	}
	if sum != 10_000_000 { // 2000 repetitions of the scores 0.5, 1.5, ... 99.5, which add up to 5000
		t.Fatalf("sum = %v, want 10000000", sum)
	}
}

func collectJSON(t *testing.T, text string, policy ingest.ErrorPolicy) ([]ingest.Row, []ingest.BadRecord, error) {
	t.Helper()
	var rows []ingest.Row
	bad, err := ingest.ReadNDJSON(strings.NewReader(text), eventSchema, policy, func(r ingest.Row) error { rows = append(rows, r); return nil })
	return rows, bad, err
}

func TestCh09_NDJSON_ParsesTypedRows_IgnoringUnknownKeys(t *testing.T) {
	rows, bad, err := collectJSON(t, `{"id":1,"name":"Ada","score":9.5,"active":true,"seen":"2026-03-01T10:00:00Z","junk":[1,2]}`+"\n"+`{"id":2,"name":"Grace","score":null}`+"\n", ingest.FailFast)
	if err != nil || len(bad) != 0 || len(rows) != 2 {
		t.Fatalf("rows=%d bad=%v err=%v", len(rows), bad, err)
	}
	if rows[0]["id"] != int64(1) || rows[0]["score"] != 9.5 || rows[0]["active"] != true {
		t.Fatalf("row 0 = %#v", rows[0])
	}
	if ts, ok := rows[0]["seen"].(time.Time); !ok || !ts.Equal(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("seen = %#v", rows[0]["seen"])
	}
	if v, present := rows[1]["score"]; !present || v != nil {
		t.Fatalf("null optional must be nil, got (%v, %v)", v, present)
	}
}

func TestCh09_NDJSON_KeepsLargeIntegersExact(t *testing.T) {
	rows, _, err := collectJSON(t, `{"id":9007199254740993,"name":"big"}`+"\n", ingest.FailFast)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if rows[0]["id"] != int64(9007199254740993) {
		t.Fatalf("id = %v, want 9007199254740993 exactly (float64 would give ...992: use json.Decoder.UseNumber)", rows[0]["id"])
	}
}

func TestCh09_NDJSON_StrictTypes_AndRequiredFields(t *testing.T) {
	input := strings.Join([]string{
		`{"id":1,"name":"ok"}`,
		`{"id":"2","name":"id is a string"}`,
		`{"id":3.5,"name":"id is not an integer"}`,
		`{"id":4}`,
		`{"id":5,"name":"x","active":"yes"}`,
		`{"id":6,"name":"x","seen":"yesterday"}`,
		`{"id":7,"name":"fine"}`,
	}, "\n")
	rows, bad, err := collectJSON(t, input, ingest.Collect)
	if err != nil || len(rows) != 2 || len(bad) != 5 {
		t.Fatalf("rows=%d bad=%d err=%v, want 2 good rows and 5 bad", len(rows), len(bad), err)
	}
	for i, wantLine := range []int{2, 3, 4, 5, 6} {
		if bad[i].Line != wantLine {
			t.Fatalf("bad[%d].Line = %d, want %d", i, bad[i].Line, wantLine)
		}
	}
}

func TestCh09_NDJSON_MalformedAndNonObjectLines_AreBadRecords_BlankLinesAreSkipped(t *testing.T) {
	input := "{\"id\":1,\"name\":\"a\"}\n\n{not json}\n[1,2]\n42\n{\"id\":2,\"name\":\"b\"}\n"
	rows, bad, err := collectJSON(t, input, ingest.Collect)
	if err != nil || len(rows) != 2 || len(bad) != 3 {
		t.Fatalf("rows=%d bad=%v err=%v", len(rows), bad, err)
	}
	if bad[0].Line != 3 || bad[0].Raw != "{not json}" || bad[1].Line != 4 || bad[2].Line != 5 {
		t.Fatalf("bad records = %+v, want lines 3, 4, 5 (a blank line still counts) with the raw text", bad)
	}
}

func TestCh09_NDJSON_Policies(t *testing.T) {
	input := "{\"id\":1,\"name\":\"a\"}\nnot json\n{\"id\":3,\"name\":\"c\"}\n"
	rows, _, err := collectJSON(t, input, ingest.FailFast)
	if !errors.Is(err, ingest.ErrBadRecord) || !strings.Contains(err.Error(), "line 2") || len(rows) != 1 {
		t.Fatalf("FailFast: rows=%d err=%v", len(rows), err)
	}
	rows, bad, err := collectJSON(t, input, ingest.Skip)
	if err != nil || len(rows) != 2 || len(bad) != 0 {
		t.Fatalf("Skip: rows=%d bad=%v err=%v", len(rows), bad, err)
	}
}

func TestCh09_NDJSON_HandlesAVeryLongLine(t *testing.T) {
	long := strings.Repeat("x", 300_000) // far beyond bufio.Scanner's 64 KiB default
	rows, bad, err := collectJSON(t, `{"id":1,"name":"`+long+`"}`+"\n"+`{"id":2,"name":"after"}`, ingest.Collect)
	if err != nil || len(bad) != 0 || len(rows) != 2 || len(rows[0]["name"].(string)) != 300_000 || rows[1]["name"] != "after" {
		t.Fatalf("rows=%d bad=%v err=%v", len(rows), bad, err)
	}
}

func TestCh09_NDJSON_StopsWhenYieldFails(t *testing.T) {
	boom := errors.New("sink full")
	n := 0
	_, err := ingest.ReadNDJSON(strings.NewReader(`{"id":1,"name":"a"}`+"\n"+`{"id":2,"name":"b"}`), eventSchema, ingest.FailFast, func(ingest.Row) error { n++; return boom })
	if !errors.Is(err, boom) || n != 1 {
		t.Fatalf("err=%v calls=%d", err, n)
	}
}
