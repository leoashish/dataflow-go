package lsm

import "dataflow/internal/todo"

// An SSTable ("sorted string table") is an IMMUTABLE file of entries sorted by key. Immutable means: no in-place updates, no
// locks for readers, no corruption from a half-finished update -- you only ever write new files and delete old ones.
//
// FILE FORMAT
//
//	"SST1"                       4 bytes (SSTableMagic)
//	count                        4 bytes, little-endian uint32: number of entries
//	entry * count                strictly ascending by key (plain byte-wise string comparison), each:
//	    uvarint keyLen | key bytes | 1 byte flag (0 = put, 1 = tombstone) | uvarint valueLen | value bytes
//
// WriteSSTable(path, entries, bloomFPR):
//   - entries must be STRICTLY ascending by key (no duplicates): otherwise return an error and leave NO file behind.
//   - Write to path+".tmp", fsync (f.Sync()), close, then os.Rename to path. A reader therefore never sees a half-written
//     table: either the whole file exists under its final name or it does not (rename is atomic). Clean the .tmp on errors.
//   - bloomFPR is only used when OPENING; it is a parameter here so the signature matches OpenSSTable's (ignore it).
//   - An EMPTY entries slice is valid (a table with count 0).
//
// OpenSSTable(path, bloomFPR) loads the table's ACCELERATION STRUCTURES by reading the file once:
//   - verify the magic and the entry count; any problem, truncation or trailing garbage -> an error wrapping ErrCorruptTable.
//   - a BLOOM FILTER (Chapter 15's sketch.NewBloom(max(count,1), bloomFPR)) holding EVERY key: a lookup of an absent key is
//     answered without touching the file at all;
//   - a SPARSE INDEX: for every BlockEntries-th entry (entries 0, 16, 32, ...) remember its key and its byte offset in the file.
//     A lookup binary-searches the index (sort.Search) for the last block whose first key <= the wanted key, reads JUST THAT
//     BLOCK from disk (file.ReadAt from the block's offset to the next block's offset, or to the end of the data) and scans it.
//     Keep the *os.File open for ReadAt. (Real engines store the filter and the index in a footer so Open need not read the
//     whole file -- stretch challenge 3.)
//
// Get(key) -> (Entry, true, nil) if the table has any entry for the key (a tombstone counts), (Entry{}, false, nil) otherwise.
//
//	Order of work: Bloom first (definitely absent -> bloomSkips++ and return); then the index (key before the first key ->
//	not found); then ONE block read (blockReads++).
//
// Entries() returns all entries in order (used by compaction and scans). Len() is the entry count. Close() closes the file.
// BloomSkips() and BlockReads() are the counters (use sync/atomic: Get may be called concurrently).
type SSTable struct{}

func WriteSSTable(path string, entries []Entry, bloomFPR float64) error {
	return todo.NotImplemented("Ch20: WriteSSTable")
}

func OpenSSTable(path string, bloomFPR float64) (*SSTable, error) {
	return nil, todo.NotImplemented("Ch20: OpenSSTable")
}

func (t *SSTable) Get(key string) (Entry, bool, error) {
	return Entry{}, false, todo.NotImplemented("Ch20: SSTable.Get")
}

func (t *SSTable) Entries() ([]Entry, error) {
	return nil, todo.NotImplemented("Ch20: SSTable.Entries")
}

func (t *SSTable) Len() int { return 0 } // TODO Ch20

func (t *SSTable) Path() string { return "" } // TODO Ch20

func (t *SSTable) BloomSkips() uint64 { return 0 } // TODO Ch20

func (t *SSTable) BlockReads() uint64 { return 0 } // TODO Ch20

func (t *SSTable) Close() error { return todo.NotImplemented("Ch20: SSTable.Close") }
