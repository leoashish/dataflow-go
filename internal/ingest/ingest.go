// Package ingest turns raw text (CSV, NDJSON) into typed rows, one record at a time, without ever loading the file.
package ingest

import (
	"errors"
	"time"
)

// FieldType is the type a column must have after parsing.
type FieldType int

const (
	String FieldType = iota // Go type: string
	Int                     // int64
	Float                   // float64
	Bool                    // bool
	Time                    // time.Time (UTC), RFC 3339 text
)

// Field describes one column; Schema is the list of columns you care about.
type Field struct {
	Name     string
	Type     FieldType
	Required bool
}

type Schema []Field

// Row holds one record after parsing: EVERY schema field has a key, with a value of the Go type listed above, or nil if
// the field is optional and was empty/missing/null. Columns that are not in the schema are ignored.
type Row map[string]any

var (
	// ErrBadRecord wraps every problem with ONE record (malformed text, wrong type, missing required value).
	ErrBadRecord = errors.New("bad record")
	// ErrMissingColumn is returned when a REQUIRED schema column does not exist in the CSV header at all (a file-level problem).
	ErrMissingColumn = errors.New("missing column")
)

// ErrorPolicy says what to do when a record is bad. Real data is always dirty: choose deliberately.
type ErrorPolicy int

const (
	FailFast ErrorPolicy = iota // stop at the first bad record and return an error wrapping ErrBadRecord ("line N: ...")
	Skip                        // drop bad records silently (dangerous: you will never know how many)
	Collect                     // keep going and RETURN every bad record, so the caller can count them / write a reject file
)

// BadRecord describes a rejected record.
type BadRecord struct {
	Line int    // 1-based physical line where the record starts (a CSV header is line 1)
	Raw  string // the offending text (the line for NDJSON; the fields joined by "," for CSV; may be empty for a CSV syntax error)
	Err  error  // wraps ErrBadRecord
}

// Time is the layout used for the Time type. GIVEN.
const timeLayout = time.RFC3339
