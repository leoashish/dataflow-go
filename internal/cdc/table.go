package cdc

// Table is the CURRENT STATE of a replicated table: the target of a CDC pipeline.
//
// THE PROBLEM: change events are delivered AT LEAST ONCE (Chapter 7) and often OUT OF ORDER (partitions, retries, replays).
// You will see duplicates, and an old update can arrive after a newer one. Naively "apply in arrival order" corrupts the
// table: a late, stale update overwrites fresh data; a late update after a delete RESURRECTS a deleted row.
//
// THE SOLUTION: LAST-WRITER-WINS BY SEQUENCE NUMBER, PER KEY (not by arrival order). For every key remember the highest Seq
// applied so far; an event with Seq <= that is STALE (or a duplicate) and is ignored. Deletes are remembered too, as
// TOMBSTONES carrying their Seq, so an older update cannot bring the row back. The result is an operation that is
//
//	idempotent (applying an event twice = once) and commutative (any arrival order gives the same final table).
//
// That is exactly what a distributed pipeline needs: you can replay, retry and reorder freely and still converge.
//
// Apply(e) reports whether the event CHANGED the table's knowledge (true) or was stale/duplicate (false):
//   - e.Seq <= lastSeq[key]              -> ignored, return false.
//   - OpInsert and OpUpdate               -> store a COPY of e.Row as the live row (UPSERT: an update for a key never seen simply
//     creates it, an insert for an existing key overwrites it), remember e.Seq.
//   - OpDelete                            -> remove the live row if any, REMEMBER e.Seq as a tombstone (even for a key never seen).
//     Any other Op is ignored (return false).
//
// Get(key) returns a COPY of the live row. Rows() returns a deep copy of all live rows. Len() counts live rows (not tombstones).
// ApplyAll applies events in the given order and returns how many were applied.
//
// Compact(events) is LOG COMPACTION (what Kafka does to a changelog topic): keep ONLY the event with the highest Seq for each key
// (deletes included -- they must survive so the tombstone still protects against late data), returned sorted by Seq ascending.
// Replaying the compacted log must give exactly the same table as replaying the full log; the log is just smaller.
type Table struct{}

func NewTable() *Table { return &Table{} } // TODO Ch22

func (t *Table) Apply(e Event) bool { return false } // TODO Ch22

func (t *Table) ApplyAll(events []Event) int { return 0 } // TODO Ch22

func (t *Table) Get(key string) (map[string]string, bool) { return nil, false } // TODO Ch22

func (t *Table) Rows() map[string]map[string]string { return nil } // TODO Ch22

func (t *Table) Len() int { return 0 } // TODO Ch22

func Compact(events []Event) []Event { return nil } // TODO Ch22
