package window

import "time"

// Tumbling returns THE window of the given size that contains t. Tumbling windows are fixed-size, aligned to the
// Unix epoch (so 5-minute windows start at :00, :05, :10 ...), and never overlap:
//
//	size = 5m:   [12:00,12:05) [12:05,12:10) [12:10,12:15) ...     12:04:59.999 -> first, 12:05:00 -> second
//
// Use the helper alignedStart(t, size).
func Tumbling(t time.Time, size time.Duration) Window {
	return Window{} // TODO Ch06
}

// Sliding returns EVERY window [s, s+size) that contains t, where s is a multiple of slide (aligned to the epoch),
// ordered by Start ascending. Sliding windows overlap: with size = 10m and slide = 5m every event belongs to
// size/slide = 2 windows:
//
//	t = 12:07  ->  [12:00,12:10)  [12:05,12:15)
//
// Method: the LATEST window containing t starts at alignedStart(t, slide). Walk backwards in steps of `slide` while the
// window still contains t, i.e. while s > t - size; then reverse (or fill the slice from the back).
// If size < slide there are gaps and t may be in no window at all: return nil.
func Sliding(t time.Time, size, slide time.Duration) []Window {
	return nil // TODO Ch06
}

// Sessions groups the (ascending) times of ONE key into sessions separated by inactivity. A session is a burst of
// activity; it ends when nothing happens for at least `gap`.
//
//	gap = 30m, times 12:00 12:10 12:20 | 13:00      ->  [12:00, 12:50)  [13:00, 13:30)
//
// Each event opens a window [t, t+gap); windows that OVERLAP merge. So: a new session starts when
// next - previous >= gap (exactly equal to the gap starts a new one: [t, t+gap) and [t+gap, ...) only touch). A session's
// window is [first time, last time + gap). No times -> nil.
func Sessions(times []time.Time, gap time.Duration) []Window {
	return nil // TODO Ch06
}
