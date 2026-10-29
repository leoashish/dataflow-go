package lsm

import "dataflow/internal/todo"

// DB ties it together. THE WRITE PATH, and why it is fast:
//
//	Put/Delete  ->  1. append the operation to the WAL (sequential write; Chapter 19)          durable
//	                2. apply it to the MEMTABLE (in memory)                                       visible
//	                3. when the memtable holds MemtableMaxEntries keys: FLUSH it to a new SSTable
//	                   (sorted, sequential write), then start a fresh WAL and memtable.
//	No write ever modifies a file in place: that is why an LSM-tree absorbs enormous write rates.
//
// THE READ PATH, and why it is the price you pay: newest data first.
//
//	Get(key)    ->  memtable, then the SSTables NEWEST FIRST; the first entry found decides:
//	                a tombstone means "deleted" (return not found); do not look further down.
//	                Each table is guarded by its Bloom filter, so tables that cannot contain the key cost almost nothing.
//
// COMPACTION pays the debt: many tables mean slow reads and wasted space (old versions, tombstones).
//
//	Compact()   ->  Flush(), then merge ALL tables into ONE (Chapter 10's extsort.KWayMerge with the tables NEWEST FIRST: for equal
//	                keys the lower-numbered source comes first = the newest version), keeping only the FIRST entry of each key and
//	                DROPPING tombstones (safe: nothing older remains below). Write the result as a new table, then delete the old
//	                files. If no live entry is left, simply delete all tables.
//
// Details:
//   - Open(opts): apply withDefaults; MkdirAll(Dir); existing *.sst files (names NNNNNN.sst, 6 digits) are opened and ordered
//     by number (higher number = NEWER); REPLAY the WAL (wal.ReadAll(Dir/wal.log) if it exists, decodeOp each payload, apply to
//     the memtable); then open the WAL for appending (wal.Open, Options{SyncEveryAppend: opts.SyncWrites}). The next table
//     number is (highest existing)+1. Empty key -> ErrEmptyKey everywhere.
//   - Put/Delete: WAL first, memtable second; flush automatically when the memtable reaches MemtableMaxEntries keys; after a
//     flush, if CompactAfter > 0 and there are >= CompactAfter tables, Compact().
//   - Flush(): no-op on an empty memtable. Otherwise WriteSSTable(next path, mem.Entries()), OpenSSTable it, put it FIRST in the
//     list (newest), replace the memtable with a fresh one and RESET THE WAL (close it, remove wal.log, wal.Open again).
//     (If the process dies between writing the table and resetting the WAL, recovery replays entries that are already in the
//     table: harmless, the memtable simply shadows identical data.)
//   - Get(key) -> (value, found, err) as described. Scan(from, to) returns the LIVE pairs with from <= key < to in ascending key
//     order (to == "" means no upper bound), merging the memtable and all tables newest-wins and hiding tombstones.
//   - Stats() sums BloomSkips/BlockReads over the open tables. Close() closes the WAL and the tables; after it every method
//     returns ErrClosed (Close itself is idempotent). The DB is safe for concurrent use: guard everything with one sync.Mutex.
//   - Compaction here is NOT crash-atomic (a crash between writing the merged table and deleting the old ones can resurrect
//     deleted keys); real engines record table sets in a MANIFEST file -- stretch challenge 4.
type DB struct{}

func Open(opts Options) (*DB, error) { return nil, todo.NotImplemented("Ch20: Open") }

func (db *DB) Put(key, value string) error { return todo.NotImplemented("Ch20: Put") }

func (db *DB) Delete(key string) error { return todo.NotImplemented("Ch20: Delete") }

func (db *DB) Get(key string) (string, bool, error) {
	return "", false, todo.NotImplemented("Ch20: Get")
}

func (db *DB) Scan(from, to string) ([]KV, error) { return nil, todo.NotImplemented("Ch20: Scan") }

func (db *DB) Flush() error { return todo.NotImplemented("Ch20: Flush") }

func (db *DB) Compact() error { return todo.NotImplemented("Ch20: Compact") }

func (db *DB) Stats() Stats { return Stats{} } // TODO Ch20

func (db *DB) Close() error { return todo.NotImplemented("Ch20: Close") }
