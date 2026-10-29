// Package lsm is a miniature LSM-tree key-value store (the design of LevelDB, RocksDB, Cassandra, HBase and ScyllaDB).
// It INTEGRATES earlier chapters: the write-ahead log (Chapter 19), Bloom filters (Chapter 15) and the k-way merge
// (Chapter 10). Do those chapters first.
package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	// ErrEmptyKey: keys must not be empty. GIVEN.
	ErrEmptyKey = errors.New("lsm: empty key")
	// ErrClosed: the database was closed. GIVEN.
	ErrClosed = errors.New("lsm: database is closed")
	// ErrCorruptTable: an SSTable file is malformed or truncated. GIVEN.
	ErrCorruptTable = errors.New("lsm: corrupt sstable")
)

// BlockEntries is how many entries one SSTable BLOCK holds: the sparse index records the first key and file offset of every
// block, so a lookup reads at most ONE block (16 entries) instead of the whole file. GIVEN.
const BlockEntries = 16

// SSTableMagic starts every SSTable file; it is followed by the entry count as a 4-byte little-endian number. GIVEN.
const SSTableMagic = "SST1"

// Entry is one versioned key. A deletion is NOT an absence: it is a TOMBSTONE entry (Tombstone true, Value ""), which must
// shadow older values of the key living in older tables. GIVEN.
type Entry struct {
	Key       string
	Value     string
	Tombstone bool
}

// KV is a live key-value pair returned by Scan. GIVEN.
type KV struct{ Key, Value string }

// Options configure Open. Zero values mean the defaults noted. GIVEN.
type Options struct {
	Dir                string  // directory holding wal.log and NNNNNN.sst files (required; created if missing)
	MemtableMaxEntries int     // flush the memtable to a new SSTable when it holds this many keys (default 1000)
	CompactAfter       int     // compact all tables into one as soon as there are this many (0 = never automatically)
	BloomFPR           float64 // false-positive rate of each SSTable's Bloom filter (default 0.01)
	SyncWrites         bool    // fsync the WAL on every write
}

func (o Options) withDefaults() Options {
	if o.MemtableMaxEntries <= 0 {
		o.MemtableMaxEntries = 1000
	}
	if o.BloomFPR <= 0 || o.BloomFPR >= 1 {
		o.BloomFPR = 0.01
	}
	return o
}

// Stats is a snapshot of the store's internals (used by the tests to PROVE the Bloom filters and the sparse index work).
type Stats struct {
	MemtableEntries int    // keys currently in the memtable (including tombstones)
	Tables          int    // SSTables on disk
	BloomSkips      uint64 // table lookups answered "definitely absent" by a Bloom filter, without touching the file
	BlockReads      uint64 // SSTable blocks actually read from disk
}

// encodeOp / decodeOp are the WAL payload for one write: byte 'P' (put) or 'D' (delete), uvarint key length, the key, then
// (for a put) the value to the end of the payload. GIVEN: the WAL is Chapter 19's; this is only the message it carries.
func encodeOp(e Entry) []byte {
	op := byte('P')
	if e.Tombstone {
		op = 'D'
	}
	out := []byte{op}
	out = binary.AppendUvarint(out, uint64(len(e.Key)))
	out = append(out, e.Key...)
	if !e.Tombstone {
		out = append(out, e.Value...)
	}
	return out
}

func decodeOp(b []byte) (Entry, error) {
	if len(b) < 2 || (b[0] != 'P' && b[0] != 'D') {
		return Entry{}, fmt.Errorf("lsm: bad wal operation")
	}
	klen, n := binary.Uvarint(b[1:])
	if n <= 0 || uint64(len(b)-1-n) < klen {
		return Entry{}, fmt.Errorf("lsm: bad wal operation")
	}
	key := string(b[1+n : 1+n+int(klen)])
	if b[0] == 'D' {
		return Entry{Key: key, Tombstone: true}, nil
	}
	return Entry{Key: key, Value: string(b[1+n+int(klen):])}, nil
}
