package window_test

import (
	"testing"
	"time"

	"dataflow/internal/window"
)

// Chapter 06 -- event time, windows, watermarks.   Run:  go test ./internal/window -run Ch06 -race -v

var base = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

func win(start, end time.Duration) window.Window {
	return window.Window{Start: at(start), End: at(end)}
}

func sameWindow(a, b window.Window) bool { return a.Start.Equal(b.Start) && a.End.Equal(b.End) }

func TestCh06_Tumbling_PutsAnEventInItsFixedWindow(t *testing.T) {
	got := window.Tumbling(at(7*time.Minute), 5*time.Minute)
	if want := win(5*time.Minute, 10*time.Minute); !sameWindow(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh06_Tumbling_StartIsInside_EndIsOutside(t *testing.T) {
	size := 5 * time.Minute
	lastOfFirst := window.Tumbling(at(5*time.Minute-time.Millisecond), size)
	firstOfNext := window.Tumbling(at(5*time.Minute), size)
	if !sameWindow(lastOfFirst, win(0, 5*time.Minute)) || !sameWindow(firstOfNext, win(5*time.Minute, 10*time.Minute)) {
		t.Fatalf("boundary wrong: %v and %v", lastOfFirst, firstOfNext)
	}
}

func TestCh06_Tumbling_IsAlignedToTheUnixEpoch(t *testing.T) {
	w := window.Tumbling(time.Date(2026, 3, 9, 14, 37, 12, 0, time.UTC), time.Hour)
	if want := time.Date(2026, 3, 9, 14, 0, 0, 0, time.UTC); !w.Start.Equal(want) || !w.End.Equal(want.Add(time.Hour)) {
		t.Fatalf("got %v, want the hour 14:00-15:00", w)
	}
}

func TestCh06_Sliding_GivesSizeOverSlideWindows_Ascending(t *testing.T) {
	got := window.Sliding(at(7*time.Minute), 10*time.Minute, 5*time.Minute)
	want := []window.Window{win(0, 10*time.Minute), win(5*time.Minute, 15*time.Minute)}
	if len(got) != 2 || !sameWindow(got[0], want[0]) || !sameWindow(got[1], want[1]) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh06_Sliding_AnEventOnAnEdgeBelongsToTheWindowThatStartsThere_NotToTheOneThatEnds(t *testing.T) {
	got := window.Sliding(at(10*time.Minute), 10*time.Minute, 5*time.Minute)
	want := []window.Window{win(5*time.Minute, 15*time.Minute), win(10*time.Minute, 20*time.Minute)}
	if len(got) != 2 || !sameWindow(got[0], want[0]) || !sameWindow(got[1], want[1]) {
		t.Fatalf("got %v, want %v ([00:00,00:10) ends at the event and does not contain it)", got, want)
	}
}

func TestCh06_Sliding_WithSizeEqualToSlide_IsTumbling_AndWithGapsATimeCanBeInNoWindow(t *testing.T) {
	got := window.Sliding(at(7*time.Minute), 5*time.Minute, 5*time.Minute)
	if len(got) != 1 || !sameWindow(got[0], win(5*time.Minute, 10*time.Minute)) {
		t.Fatalf("got %v", got)
	}
	// size < slide leaves gaps: windows [0,5) and [10,15); 7 minutes falls in the gap
	if gap := window.Sliding(at(7*time.Minute), 5*time.Minute, 10*time.Minute); len(gap) != 0 {
		t.Fatalf("got %v, want none", gap)
	}
}

func TestCh06_Sliding_ManyOverlaps(t *testing.T) {
	got := window.Sliding(at(59*time.Second), time.Minute, 10*time.Second) // 6 windows
	if len(got) != 6 {
		t.Fatalf("got %d windows, want 6", len(got))
	}
	for i, w := range got {
		if !w.Contains(at(59 * time.Second)) {
			t.Fatalf("window %d %v does not contain the event", i, w)
		}
		if i > 0 && !w.Start.After(got[i-1].Start) {
			t.Fatalf("windows not in ascending order: %v", got)
		}
	}
}

func TestCh06_Sessions_SplitWhereTheInactivityIsAtLeastTheGap(t *testing.T) {
	times := []time.Time{at(0), at(10 * time.Minute), at(20 * time.Minute), at(60 * time.Minute)}
	got := window.Sessions(times, 30*time.Minute)
	want := []window.Window{win(0, 50*time.Minute), win(60*time.Minute, 90*time.Minute)}
	if len(got) != 2 || !sameWindow(got[0], want[0]) || !sameWindow(got[1], want[1]) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh06_Sessions_AGapExactlyEqualToTheThresholdStartsANewSession(t *testing.T) {
	got := window.Sessions([]time.Time{at(0), at(30 * time.Minute)}, 30*time.Minute)
	if len(got) != 2 {
		t.Fatalf("got %v, want two sessions: windows [0,30) and [30,60) only touch, they do not overlap", got)
	}
	if got := window.Sessions([]time.Time{at(0), at(30*time.Minute - time.Second)}, 30*time.Minute); len(got) != 1 {
		t.Fatalf("one second under the gap must stay in ONE session, got %v", got)
	}
}

func TestCh06_Sessions_SingleEventAndNoEvents(t *testing.T) {
	got := window.Sessions([]time.Time{at(5 * time.Minute)}, 10*time.Minute)
	if len(got) != 1 || !sameWindow(got[0], win(5*time.Minute, 15*time.Minute)) {
		t.Fatalf("got %v, want [5m,15m)", got)
	}
	if got := window.Sessions(nil, time.Minute); len(got) != 0 {
		t.Fatalf("got %v, want none", got)
	}
}

func TestCh06_Watermark_IsTheLargestEventTimeMinusTheDelay_AndNeverGoesBackwards(t *testing.T) {
	w := window.NewWatermarkTracker(10 * time.Second)
	if !w.Watermark().IsZero() {
		t.Fatalf("watermark before any event = %v, want the zero time", w.Watermark())
	}
	steps := []struct{ event, want time.Duration }{
		{100 * time.Second, 90 * time.Second},
		{95 * time.Second, 90 * time.Second}, // an OLD event must not pull it back
		{120 * time.Second, 110 * time.Second},
		{110 * time.Second, 110 * time.Second},
	}
	for _, s := range steps {
		if got := w.Observe(at(s.event)); !got.Equal(at(s.want)) {
			t.Fatalf("after event at +%v the watermark is %v, want +%v", s.event, got.Sub(base), s.want)
		}
	}
	if !w.Watermark().Equal(at(110 * time.Second)) {
		t.Fatalf("Watermark() = %v", w.Watermark())
	}
}

func ev(key string, offset time.Duration, v float64) window.Event {
	return window.Event{Key: key, Time: at(offset), Value: v}
}

func TestCh06_Aggregator_CountsAndSumsPerWindowAndKey(t *testing.T) {
	a := window.NewAggregator(time.Minute, 0)
	for _, e := range []window.Event{ev("x", 5*time.Second, 1), ev("x", 20*time.Second, 2), ev("y", 30*time.Second, 10), ev("x", 70*time.Second, 5)} {
		if !a.Add(e) {
			t.Fatalf("event %+v was rejected", e)
		}
	}
	got := a.Flush()

	want := []window.Result{
		{Key: "x", Window: win(0, time.Minute), Count: 2, Sum: 3},
		{Key: "y", Window: win(0, time.Minute), Count: 1, Sum: 10},
		{Key: "x", Window: win(time.Minute, 2*time.Minute), Count: 1, Sum: 5},
	}
	if len(got) != 3 {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key || !sameWindow(got[i].Window, want[i].Window) || got[i].Count != want[i].Count || got[i].Sum != want[i].Sum {
			t.Fatalf("result %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestCh06_Aggregator_EmitsOnlyClosedWindows_InOrder_AndForgetsThem(t *testing.T) {
	a := window.NewAggregator(time.Minute, 0)
	a.Add(ev("b", 10*time.Second, 1))
	a.Add(ev("a", 20*time.Second, 1))
	a.Add(ev("a", 70*time.Second, 1))

	if got := a.AdvanceWatermark(at(59 * time.Second)); len(got) != 0 {
		t.Fatalf("emitted %v before the window ended", got)
	}
	got := a.AdvanceWatermark(at(time.Minute)) // the first window ends exactly at the watermark: it closes
	if len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("got %v, want the first window's two keys sorted: a, b", got)
	}
	if again := a.AdvanceWatermark(at(90 * time.Second)); len(again) != 0 {
		t.Fatalf("a window must be emitted once only; got %v again", again)
	}
	if rest := a.Flush(); len(rest) != 1 || rest[0].Key != "a" {
		t.Fatalf("Flush = %v, want only the still-open second window", rest)
	}
}

func TestCh06_Aggregator_DropsAndCountsLateEvents(t *testing.T) {
	a := window.NewAggregator(time.Minute, 0)
	a.Add(ev("x", 10*time.Second, 1))
	a.AdvanceWatermark(at(2 * time.Minute)) // closes [0,1m) and [1m,2m)

	if a.Add(ev("x", 30*time.Second, 99)) {
		t.Fatal("an event for a closed window must be rejected as late")
	}
	if a.Late() != 1 {
		t.Fatalf("Late() = %d, want 1", a.Late())
	}
	if !a.Add(ev("x", 2*time.Minute+time.Second, 1)) {
		t.Fatal("an event for a window that is still open must be accepted")
	}
}

func TestCh06_Aggregator_AllowedLateness_KeepsWindowsOpenLonger(t *testing.T) {
	a := window.NewAggregator(time.Minute, 2*time.Minute)
	a.Add(ev("x", 10*time.Second, 1))

	if got := a.AdvanceWatermark(at(2 * time.Minute)); len(got) != 0 { // past the window's end, but within the lateness
		t.Fatalf("emitted %v; the window must stay open until end + lateness = 3m", got)
	}
	if !a.Add(ev("x", 20*time.Second, 5)) { // a straggler, still welcome
		t.Fatal("a late-but-allowed event was rejected")
	}
	got := a.AdvanceWatermark(at(3 * time.Minute))
	if len(got) != 1 || got[0].Count != 2 || got[0].Sum != 6 {
		t.Fatalf("got %v, want one result that includes the straggler (count 2, sum 6)", got)
	}
	if a.Add(ev("x", 30*time.Second, 1)) {
		t.Fatal("after the lateness has also passed, the event must be rejected")
	}
}

func TestCh06_Aggregator_IgnoresAWatermarkThatMovesBackwards(t *testing.T) {
	a := window.NewAggregator(time.Minute, 0)
	a.AdvanceWatermark(at(5 * time.Minute))
	if got := a.AdvanceWatermark(at(time.Minute)); len(got) != 0 || !a.Watermark().Equal(at(5*time.Minute)) {
		t.Fatalf("got %v watermark=%v: a smaller watermark must be ignored", got, a.Watermark().Sub(base))
	}
}

func TestCh06_EndToEnd_OutOfOrderStream_WithTrackerAndAggregator(t *testing.T) {
	// 1-minute windows, no lateness, the watermark trails the newest event by 10 seconds.
	// Per event: Add FIRST (judge it against the watermark so far), THEN observe it and advance.
	tracker := window.NewWatermarkTracker(10 * time.Second)
	agg := window.NewAggregator(time.Minute, 0)
	var results []window.Result
	events := []window.Event{
		ev("a", 5*time.Second, 1),
		ev("a", 30*time.Second, 2),
		ev("a", 70*time.Second, 3), // watermark -> 60s: window [0,1m) closes
		ev("a", 50*time.Second, 4), // 20s behind the newest event: LATE (its window is already closed)
		ev("a", 65*time.Second, 5), // slightly out of order but its window is open: accepted
	}
	for _, e := range events {
		agg.Add(e)
		results = append(results, agg.AdvanceWatermark(tracker.Observe(e.Time))...)
	}
	results = append(results, agg.Flush()...)

	want := []window.Result{
		{Key: "a", Window: win(0, time.Minute), Count: 2, Sum: 3},
		{Key: "a", Window: win(time.Minute, 2*time.Minute), Count: 2, Sum: 8},
	}
	if len(results) != 2 || agg.Late() != 1 {
		t.Fatalf("results=%v late=%d, want 2 results and exactly 1 late event", results, agg.Late())
	}
	for i := range want {
		if !sameWindow(results[i].Window, want[i].Window) || results[i].Count != want[i].Count || results[i].Sum != want[i].Sum {
			t.Fatalf("result %d = %+v, want %+v", i, results[i], want[i])
		}
	}
}
