package window

import "time"

func Tumbling(t time.Time, size time.Duration) Window {
	start := alignedStart(t, size)
	return Window{Start: start, End: start.Add(size)}
}

func Sliding(t time.Time, size, slide time.Duration) []Window {
	var windows []Window
	for s := alignedStart(t, slide); s.After(t.Add(-size)); s = s.Add(-slide) { // s > t-size  <=>  the window still contains t
		windows = append(windows, Window{Start: s, End: s.Add(size)})
	}
	for i, j := 0, len(windows)-1; i < j; i, j = i+1, j-1 { // collected newest-first: reverse into ascending order
		windows[i], windows[j] = windows[j], windows[i]
	}
	return windows
}

func Sessions(times []time.Time, gap time.Duration) []Window {
	if len(times) == 0 {
		return nil
	}
	var sessions []Window
	first, last := times[0], times[0]
	for _, t := range times[1:] {
		if t.Sub(last) >= gap { // inactivity long enough: close the session and open a new one
			sessions = append(sessions, Window{Start: first, End: last.Add(gap)})
			first = t
		}
		last = t
	}
	return append(sessions, Window{Start: first, End: last.Add(gap)})
}
