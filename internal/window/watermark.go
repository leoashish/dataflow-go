package window

import "time"

// WatermarkTracker answers: "up to which event time do I believe I have seen (almost) everything?"
//
// Events arrive out of order. The watermark is a promise that no event OLDER than it is still expected (events that
// break the promise are "late"). The classic heuristic, bounded out-of-orderness:
//
//		watermark = (largest event time seen so far) - maxDelay
//
//	  - Observe(t) records an event time and returns the current watermark.
//	  - The watermark NEVER goes backwards (an old event must not move it back). Before any event it is the zero time.
//	  - A larger maxDelay tolerates more disorder but delays every result by that much.
type WatermarkTracker struct{}

func NewWatermarkTracker(maxDelay time.Duration) *WatermarkTracker { return &WatermarkTracker{} } // TODO Ch06

func (w *WatermarkTracker) Observe(t time.Time) time.Time { return time.Time{} } // TODO Ch06

func (w *WatermarkTracker) Watermark() time.Time { return time.Time{} } // TODO Ch06
