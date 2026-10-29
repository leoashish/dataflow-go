// Package window groups events by EVENT time (when something happened), not processing time (when we saw it).
package window

import "time"

// Event is a record with the time it happened.
type Event struct {
	Key   string
	Time  time.Time
	Value float64
}

// Window is the half-open interval [Start, End): Start is inside, End is outside. GIVEN.
type Window struct {
	Start, End time.Time
}

func (w Window) Contains(t time.Time) bool { return !t.Before(w.Start) && t.Before(w.End) }

// Result is what a window produces for one key.
type Result struct {
	Key    string
	Window Window
	Count  int
	Sum    float64
}

// floorDiv rounds x down to a multiple of m (m > 0), correctly for negative x. GIVEN: it is the one subtle bit of arithmetic.
func floorDiv(x, m int64) int64 {
	q := x / m
	if x%m != 0 && (x < 0) != (m < 0) {
		q--
	}
	return q
}

// alignedStart is the start of the m-aligned slot that contains t, aligned to the Unix epoch. GIVEN.
func alignedStart(t time.Time, m time.Duration) time.Time {
	n := t.UnixNano()
	return time.Unix(0, floorDiv(n, int64(m))*int64(m)).UTC()
}
