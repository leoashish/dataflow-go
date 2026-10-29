package wal

import (
	"os"

	"dataflow/internal/todo"
)

// Options configure Open.
type Options struct {
	// SyncEveryAppend makes Append / AppendBatch call fsync before returning (the record is DURABLE when Append returns).
	// When false, durability is only guaranteed after an explicit Sync() or Close().
	SyncEveryAppend bool
	// SyncFunc is what "fsync" means; nil = (*os.File).Sync. Tests inject their own to count calls and to inject failures.
	SyncFunc func(*os.File) error
}

// WAL is an append-only log file.
//
// WHY DOES ANY OF THIS EXIST? "A write returned" does not mean "the data is on disk": the OS keeps it in a page cache for
// seconds, the disk keeps it in ITS cache. Pull the power and it is gone. A database promises that an acknowledged commit
// survives a crash, so it first APPENDS the change to a log and fsyncs (sequential appends are fast), and only later does
// the slow in-place update. After a crash it REPLAYS the log. This is the heart of every database (Postgres WAL, SQLite's
// journal, RocksDB's .log), of Kafka, and of Chapter 20's LSM tree.
//
// Behaviour to implement:
//
//   - Open(path, opts): read the whole file if it exists (a missing file is fine: start empty), ScanRecords it, and if
//     there are bytes after validBytes (a torn tail, or corruption) TRUNCATE THE FILE to validBytes (os.Truncate) so new
//     appends start on a clean boundary -- appending after garbage would hide every later record behind it.
//     Then open the file for appending (os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644). Len() counts the valid records;
//     Recovered() returns (records found at Open, bytes cut off the tail).
//   - Append(payload) writes ONE record; seq numbers are 1-based and contiguous: the first record ever is 1.
//     AppendBatch(payloads) writes several with ONE write call and (if SyncEveryAppend) ONE fsync -- "group commit": the
//     fsync costs milliseconds, so amortising it over 100 records is a 100x throughput gain. It returns the seq of the FIRST
//     record. A payload over MaxRecordSize -> ErrTooLarge and NOTHING is written (check all before writing any).
//   - Sync() calls the sync function. Close() syncs once, closes the file, and is idempotent; every use after Close
//     returns ErrClosed.
//   - POISONING: if a sync fails, the data's fate is unknowable (the kernel may have dropped the dirty pages and a retry
//     would "succeed" falsely -- the famous fsync-gate bug). So after the FIRST sync error the log is POISONED: that error is
//     returned by every later Append/AppendBatch/Sync.
//   - Everything is safe for concurrent use: guard the state with a sync.Mutex.
//
// ReadAll(path) returns every VALID record of a log file without opening it for writing (it stops at a torn or corrupt
// tail silently). A missing file is an error (os.ReadFile's).
type WAL struct{}

func Open(path string, opts Options) (*WAL, error) { return nil, todo.NotImplemented("Ch19: Open") }

func (w *WAL) Append(payload []byte) (seq uint64, err error) {
	return 0, todo.NotImplemented("Ch19: Append")
}

func (w *WAL) AppendBatch(payloads [][]byte) (firstSeq uint64, err error) {
	return 0, todo.NotImplemented("Ch19: AppendBatch")
}

func (w *WAL) Sync() error { return todo.NotImplemented("Ch19: Sync") }

func (w *WAL) Close() error { return todo.NotImplemented("Ch19: Close") }

func (w *WAL) Len() uint64 { return 0 } // TODO Ch19

func (w *WAL) Recovered() (records int, truncatedBytes int64) { return 0, 0 } // TODO Ch19

func ReadAll(path string) ([][]byte, error) { return nil, todo.NotImplemented("Ch19: ReadAll") }
