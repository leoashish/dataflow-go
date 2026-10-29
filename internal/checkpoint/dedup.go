package checkpoint

// Deduper remembers the IDs it has seen so that a REPLAYED message can be recognised and skipped.
//
// An unbounded set of every ID ever seen grows forever (a billion IDs * 40 bytes...). So it keeps only the `capacity`
// most recently seen IDs -- an LRU set. The trade-off, always say it out loud: a duplicate that arrives after its ID was
// evicted slips through. Size the window to the longest replay you expect. (For "have I EVER seen this?" at huge scale
// the answer is a Bloom filter: Chapter 15.)
//
//   - Seen(id) returns true if id is currently remembered (a duplicate). Either way the id becomes the MOST recently
//     seen: a duplicate refreshes its position. If the set exceeds capacity, evict the least recently seen.
//   - Len() is the number of remembered ids (never more than capacity).
//   - capacity < 1 is treated as 1. Safe for concurrent use (sync.Mutex).
//
// Structure: map[string]*list.Element + container/list (front = most recent) -- the same two-structure trick as an LRU cache.
type Deduper struct{}

func NewDeduper(capacity int) *Deduper { return &Deduper{} } // TODO Ch07

func (d *Deduper) Seen(id string) bool { return false } // TODO Ch07

func (d *Deduper) Len() int { return 0 } // TODO Ch07
