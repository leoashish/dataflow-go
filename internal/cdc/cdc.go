// Package cdc applies CHANGE DATA CAPTURE streams: the log of inserts, updates and deletes of a source database, replayed
// into a target table (and into a history table) -- the glue between operational databases and the data warehouse.
package cdc

import (
	"errors"
	"math"
)

// Op is the kind of change (the same letters Debezium uses). GIVEN.
type Op string

const (
	OpInsert Op = "c" // create
	OpUpdate Op = "u"
	OpDelete Op = "d"
)

// Event is one change from the source database's log. GIVEN.
type Event struct {
	Seq uint64            // position in the source log (e.g. the LSN): bigger = later. Unique per key.
	Op  Op                // insert / update / delete
	Key string            // primary key of the row
	Row map[string]string // the AFTER image: the full row after the change; nil for deletes
	TS  int64             // when the change happened (event time, e.g. Unix milliseconds)
}

// OpenEnded is the ValidTo of a version that is still current. GIVEN.
const OpenEnded = math.MaxInt64

// ErrOutOfOrder: a history event is older than the key's latest version. GIVEN.
var ErrOutOfOrder = errors.New("cdc: event is older than the current version of the key")

// Version is one period of a row's life: Row was the row's value during [ValidFrom, ValidTo). GIVEN.
type Version struct {
	Key       string
	Row       map[string]string
	ValidFrom int64 // inclusive
	ValidTo   int64 // exclusive; OpenEnded for the current version
	Current   bool
}

// cloneRow copies a row so tables never alias maps owned by callers. GIVEN.
func cloneRow(r map[string]string) map[string]string {
	if r == nil {
		return nil
	}
	out := make(map[string]string, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// sameRow reports whether two rows have identical columns and values. GIVEN.
func sameRow(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
