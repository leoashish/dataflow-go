package ingest

import "dataflow/internal/todo"

// Coerce converts the text of one CSV cell into the Go value for type t.
//
//   - String: returned exactly as given (do not trim: whitespace may be data).
//   - Int:    strconv.ParseInt(strings.TrimSpace(raw), 10, 64) -> int64. "12x", "1.5", "" are errors.
//   - Float:  strconv.ParseFloat(trimmed, 64) -> float64. NaN and +/-Inf are REJECTED (they poison every sum and mean downstream:
//     in data engineering a number you can't add is a bug in the data).
//   - Bool:   trimmed, case-insensitive: true/t/yes/y/1 -> true ; false/f/no/n/0 -> false ; anything else is an error.
//   - Time:   time.Parse(time.RFC3339, trimmed), then .UTC() so that "+05:30" and "Z" instants compare equal.
//
// Return a real error for anything that does not parse.
func Coerce(raw string, t FieldType) (any, error) {
	return nil, todo.NotImplemented("Ch09: Coerce")
}
