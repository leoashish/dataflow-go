package lsm

// Memtable is the in-memory buffer of recent writes. Writes land here (after the WAL) so they are fast; when it is full it is
// written out as one sorted, immutable SSTable file.
//
//   - Put(key, value) stores an Entry (overwriting any earlier one for the key). Delete(key) stores a TOMBSTONE
//     Entry{Key: key, Tombstone: true} -- even for a key that was never written, because the key may exist in an OLDER table
//     that the tombstone has to hide.
//   - Get(key) returns the stored Entry and true if the memtable has ANY entry for the key (a tombstone counts! the caller needs
//     to know "deleted here", not just "not found here"), else (Entry{}, false).
//   - Len() is the number of distinct keys, tombstones included.
//   - Entries() returns every entry SORTED BY KEY ascending (sort.Slice over the keys) -- the order an SSTable needs.
//
// A map plus a sort on flush is plenty for this chapter. Real engines use a SKIP LIST (stretch challenge) so the memtable is
// always sorted and can be scanned while it is being written.
type Memtable struct{}

func NewMemtable() *Memtable { return &Memtable{} } // TODO Ch20

func (m *Memtable) Put(key, value string) {} // TODO Ch20

func (m *Memtable) Delete(key string) {} // TODO Ch20

func (m *Memtable) Get(key string) (Entry, bool) { return Entry{}, false } // TODO Ch20

func (m *Memtable) Len() int { return 0 } // TODO Ch20

func (m *Memtable) Entries() []Entry { return nil } // TODO Ch20
