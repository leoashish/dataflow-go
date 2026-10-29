// Package wal implements a write-ahead log: an append-only file of checksummed records that survives crashes.
package wal

import "errors"

// HeaderSize is the number of bytes in front of every record's payload: 4 bytes length + 4 bytes checksum. GIVEN.
const HeaderSize = 8

// MaxRecordSize caps a payload (16 MiB). A corrupt length field must never make the reader try to read or allocate
// gigabytes. GIVEN.
const MaxRecordSize = 16 << 20

var (
	// ErrTruncated: the bytes end in the middle of a record (the classic "torn write": the process died mid-append). GIVEN.
	ErrTruncated = errors.New("wal: truncated record")
	// ErrCorrupt: the record is complete but its checksum (or length) is wrong: bit rot, a bad disk, a partial overwrite. GIVEN.
	ErrCorrupt = errors.New("wal: corrupt record")
	// ErrClosed: the log was closed. GIVEN.
	ErrClosed = errors.New("wal: log is closed")
	// ErrTooLarge: the payload exceeds MaxRecordSize. GIVEN.
	ErrTooLarge = errors.New("wal: record too large")
)
