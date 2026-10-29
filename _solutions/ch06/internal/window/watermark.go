package window

import "time"

type WatermarkTracker struct {
	maxDelay  time.Duration
	maxSeen   time.Time
	watermark time.Time
}

func NewWatermarkTracker(maxDelay time.Duration) *WatermarkTracker {
	return &WatermarkTracker{maxDelay: maxDelay}
}

func (w *WatermarkTracker) Observe(t time.Time) time.Time {
	if t.After(w.maxSeen) {
		w.maxSeen = t
	}
	if candidate := w.maxSeen.Add(-w.maxDelay); candidate.After(w.watermark) && !w.maxSeen.IsZero() {
		w.watermark = candidate // never moves backwards
	}
	return w.watermark
}

func (w *WatermarkTracker) Watermark() time.Time { return w.watermark }
