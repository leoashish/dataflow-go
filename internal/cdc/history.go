package cdc

// History is a SLOWLY CHANGING DIMENSION, TYPE 2: instead of overwriting a row when it changes, it KEEPS EVERY VERSION with the
// time interval in which it was true. This is how a warehouse answers "which sales region did this customer belong to WHEN they
// placed that order in March?" -- not "which region are they in today?". (Type 1 overwrites and loses the past; Type 2 keeps it.)
//
//	customer 7:   {region: south}  [Jan 1, Mar 10)        <- closed when the customer moved
//	              {region: north}  [Mar 10, OpenEnded)    <- current
//
// Apply(e) processes one event IN ORDER (history needs ordered input -- late data is a stretch challenge):
//   - Look at the key's LATEST version. A key's timeline only moves FORWARD: if e.TS is EARLIER than that version's ValidFrom, or
//     (when that version is already closed by a delete) EARLIER than its ValidTo, return ErrOutOfOrder and change nothing --
//     accepting it would make two versions overlap in time. An event with exactly the same timestamp is accepted.
//   - OpInsert / OpUpdate:
//   - if the key has a CURRENT version whose row is the same as e.Row (sameRow): NOTHING CHANGED -> no new version (SCD2
//     creates versions only for real changes; a no-op update must not bloat the history).
//   - otherwise close the current version (if any): ValidTo = e.TS, Current = false; then append a new version
//     {Row: copy of e.Row, ValidFrom: e.TS, ValidTo: OpenEnded, Current: true}.
//   - OpDelete: close the current version at e.TS (ValidTo = e.TS, Current = false); no new version. A delete for a key with no
//     current version does nothing. After a delete the key may be inserted again later: that starts a new version (a GAP in time).
//   - any other Op: ignored, return nil.
//
// AsOf(key, ts) returns a COPY of the row that was valid at time ts, i.e. ValidFrom <= ts < ValidTo; (nil, false) if there was
// none (before the first version, or in a gap after a delete).
// Versions(key) returns copies of all versions in chronological order (nil for an unknown key). Current() returns a deep copy of
// the rows of all current versions, keyed by key.
type History struct{}

func NewHistory() *History { return &History{} } // TODO Ch22

func (h *History) Apply(e Event) error { return nil } // TODO Ch22

func (h *History) AsOf(key string, ts int64) (map[string]string, bool) { return nil, false } // TODO Ch22

func (h *History) Versions(key string) []Version { return nil } // TODO Ch22

func (h *History) Current() map[string]map[string]string { return nil } // TODO Ch22
