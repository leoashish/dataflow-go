package ingest

import (
	"io"

	"dataflow/internal/todo"
)

// ReadCSV streams CSV from r, turns every data record into a typed Row and calls yield with it.
//
// The FIRST record is the header; schema fields are matched to columns BY NAME (case-sensitive), in any order; columns
// that are not in the schema are ignored; ragged records are allowed (cells that are missing at the end count as empty).
//
//   - A REQUIRED schema field with no column in the header: return an error wrapping ErrMissingColumn BEFORE yielding
//     anything. An optional field with no column is simply nil in every row.
//   - For each record and field: trim the cell; empty -> nil if optional, a bad record if Required; otherwise Coerce
//     (a failure is a bad record that names the field).
//   - A CSV syntax error (a bare quote inside an unquoted cell...) is a bad record too: encoding/csv can carry on with the
//     next record after reporting it. Use csv.Reader with FieldsPerRecord = -1; csv.ParseError has StartLine.
//     Blank lines are skipped by encoding/csv. reader.FieldPos(0) gives the line where the current record started.
//   - What happens to a bad record is the ErrorPolicy: FailFast returns fmt.Errorf("line %d: %w", line, bad.Err) at once
//     (the bad records found so far are returned too); Skip ignores it; Collect appends it to the returned slice.
//   - If yield returns an error, stop and return it (together with the bad records collected so far).
//
// It must stream: never hold more than the current record.
func ReadCSV(r io.Reader, schema Schema, policy ErrorPolicy, yield func(Row) error) ([]BadRecord, error) {
	return nil, todo.NotImplemented("Ch09: ReadCSV")
}

// ReadNDJSON streams newline-delimited JSON (one JSON object per line), the format of logs and event streams.
//
//   - Blank lines are skipped (but still count for line numbers). Lines can be LONG (hundreds of KB): do not use
//     bufio.Scanner with its default limit.
//   - A line that is not valid JSON, or is valid JSON but not an object, is a bad record (Raw = the line).
//   - Per schema field, with STRICT typing from the JSON value: String needs a JSON string; Int a JSON number that is an
//     integer (exactly -- see the trap below); Float a JSON number; Bool a JSON boolean; Time a JSON string in RFC 3339.
//     A JSON string for a number field ("12") is a wrong type. Missing key or null -> nil if optional, bad record if Required.
//   - Unknown keys are ignored.
//   - Same ErrorPolicy and yield semantics as ReadCSV.
//
// THE PRECISION TRAP: decoding JSON into map[string]any turns every number into float64, which cannot represent integers
// above 2^53 exactly: 9007199254740993 silently becomes ...992. IDs are exactly such numbers. Use a json.Decoder with
// UseNumber(), which gives you a json.Number (the original text) that you can convert with Int64() / Float64().
func ReadNDJSON(r io.Reader, schema Schema, policy ErrorPolicy, yield func(Row) error) ([]BadRecord, error) {
	return nil, todo.NotImplemented("Ch09: ReadNDJSON")
}
