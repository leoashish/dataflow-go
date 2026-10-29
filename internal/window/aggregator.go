package window

import "time"

// Aggregator computes Count and Sum per (tumbling window, key) with event-time semantics.
//
// Life of a window: it collects events; when the WATERMARK passes the window's end (plus allowedLateness) the window is
// CLOSED: its result is emitted and its state is deleted (so memory stays bounded, however long the stream runs).
//
//	allowedLateness = how long after the window's end we still accept stragglers before closing it.
//
// Rules:
//   - Add(e): the event's window is Tumbling(e.Time, size). If that window is already closed -- i.e.
//     window.End + allowedLateness <= current watermark -- the event is LATE: return false, count it (Late()), drop it.
//     Otherwise add it to (window, key) and return true.
//   - AdvanceWatermark(wm): the watermark only moves FORWARD (ignore a wm that is not after the current one and return
//     nil). Then emit and delete every window with End + allowedLateness <= wm. Results are sorted by (Window.Start, Key).
//   - Flush(): emit and delete everything that is left (end of stream), sorted the same way.
//   - The initial watermark is the zero time.
//
// State: a map from window start to a map from key to {count, sum}. Sorting: sort.Slice (or slices.SortFunc).
type Aggregator struct{}

func NewAggregator(size, allowedLateness time.Duration) *Aggregator { return &Aggregator{} } // TODO Ch06

func (a *Aggregator) Add(e Event) (accepted bool) { return false } // TODO Ch06

func (a *Aggregator) AdvanceWatermark(wm time.Time) []Result { return nil } // TODO Ch06

func (a *Aggregator) Flush() []Result { return nil } // TODO Ch06

func (a *Aggregator) Late() int { return 0 } // TODO Ch06

func (a *Aggregator) Watermark() time.Time { return time.Time{} } // TODO Ch06
