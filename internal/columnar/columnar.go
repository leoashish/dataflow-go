// Package columnar stores tables the way analytics engines do: one column at a time, each compressed with an encoding
// chosen for the data in it.
package columnar

import "errors"

// ErrCorrupt is returned (wrapped) by every decoder when the bytes are malformed or truncated. Decoders face UNTRUSTED
// bytes: they must return this error, never panic and never allocate a absurd amount of memory. GIVEN.
var ErrCorrupt = errors.New("columnar: corrupt data")

// MaxValues caps how many values any decoder will produce (16 million: 128 MB of int64): a corrupt (or malicious) run length
// or count must be rejected, not turned into a gigantic allocation. GIVEN.
const MaxValues = 1 << 24

// Kind is the type of a column.
type Kind byte

const (
	KindInt64 Kind = iota + 1
	KindString
)

// Encoding is how a column's values are stored on disk.
type Encoding byte

const (
	EncPlain Encoding = iota + 1 // each int as a zigzag varint
	EncDelta                     // first value, then the differences between neighbours
	EncRLE                       // (value, run length) pairs
	EncDict                      // distinct strings once + bit-packed codes (string columns)
)

// Column is one column of a table. Kind says which of Ints/Strs is used.
// Encoding is FILLED IN by ReadColumns (to tell you what was stored) and IGNORED by WriteColumns (which chooses itself).
type Column struct {
	Name     string
	Kind     Kind
	Ints     []int64
	Strs     []string
	Encoding Encoding
}
