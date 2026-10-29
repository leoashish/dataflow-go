package window

import (
	"sort"
	"time"
)

type accumulator struct {
	count int
	sum   float64
}

type Aggregator struct {
	size, lateness time.Duration
	watermark      time.Time
	late           int
	windows        map[time.Time]map[string]*accumulator // window start -> key -> running totals
}

func NewAggregator(size, allowedLateness time.Duration) *Aggregator {
	return &Aggregator{size: size, lateness: allowedLateness, windows: map[time.Time]map[string]*accumulator{}}
}

func (a *Aggregator) closed(w Window) bool { return !w.End.Add(a.lateness).After(a.watermark) }

func (a *Aggregator) Add(e Event) bool {
	w := Tumbling(e.Time, a.size)
	if a.closed(w) {
		a.late++
		return false
	}
	byKey := a.windows[w.Start]
	if byKey == nil {
		byKey = map[string]*accumulator{}
		a.windows[w.Start] = byKey
	}
	acc := byKey[e.Key]
	if acc == nil {
		acc = &accumulator{}
		byKey[e.Key] = acc
	}
	acc.count++
	acc.sum += e.Value
	return true
}

func (a *Aggregator) AdvanceWatermark(wm time.Time) []Result {
	if !wm.After(a.watermark) {
		return nil
	}
	a.watermark = wm
	var starts []time.Time
	for start := range a.windows {
		if a.closed(Window{Start: start, End: start.Add(a.size)}) {
			starts = append(starts, start)
		}
	}
	return a.emit(starts)
}

func (a *Aggregator) Flush() []Result {
	starts := make([]time.Time, 0, len(a.windows))
	for start := range a.windows {
		starts = append(starts, start)
	}
	return a.emit(starts)
}

// emit turns the given windows into sorted results and deletes their state.
func (a *Aggregator) emit(starts []time.Time) []Result {
	var out []Result
	for _, start := range starts {
		w := Window{Start: start, End: start.Add(a.size)}
		for key, acc := range a.windows[start] {
			out = append(out, Result{Key: key, Window: w, Count: acc.count, Sum: acc.sum})
		}
		delete(a.windows, start)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Window.Start.Equal(out[j].Window.Start) {
			return out[i].Window.Start.Before(out[j].Window.Start)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func (a *Aggregator) Late() int { return a.late }

func (a *Aggregator) Watermark() time.Time { return a.watermark }
