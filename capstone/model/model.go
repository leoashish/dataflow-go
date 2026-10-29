// Package model holds the types shared by the capstone's pipeline, generator and oracle. GIVEN: do not change them -- the
// tests (and your own report.json) depend on them.
package model

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// The reasons a line can be rejected (see SPEC.md for the order in which they are checked).
const (
	ReasonMalformedJSON = "malformed_json"
	ReasonMissingField  = "missing_field"
	ReasonBadAction     = "bad_action"
	ReasonBadTimestamp  = "bad_timestamp"
	ReasonBadNumber     = "bad_number"
)

// Event is one valid, parsed clickstream event.
type Event struct {
	EventID     string
	UserID      string
	Page        string
	Category    string
	Action      string // view | click | purchase
	AmountCents int64  // only meaningful for purchases
	LatencyMS   float64
	TS          time.Time // UTC
}

// Config configures one run of the pipeline. A zero WindowSize/AllowedLateness/WatermarkDelay means the SPEC defaults.
type Config struct {
	InputDir        string        // directory with events-*.ndjson
	OutputDir       string        // where report.json and events.col are written
	StateDir        string        // where YOUR checkpoints live ("" = no checkpointing)
	Workers         int           // parallelism of the parsing stage (< 1 means 1)
	CheckpointEvery int           // checkpoint after this many INPUT LINES (0 = never)
	WindowSize      time.Duration // default 5 minutes
	AllowedLateness time.Duration // default 2 minutes
	WatermarkDelay  time.Duration // default 1 minute
}

// WithDefaults fills in the zero values.
func (c Config) WithDefaults() Config {
	if c.WindowSize <= 0 {
		c.WindowSize = 5 * time.Minute
	}
	if c.AllowedLateness <= 0 {
		c.AllowedLateness = 2 * time.Minute
	}
	if c.WatermarkDelay <= 0 {
		c.WatermarkDelay = time.Minute
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	return c
}

// Totals accounts for every non-empty input line exactly once: Read = sum(Rejected) + Duplicates + Late + Accepted.
type Totals struct {
	Read       int64            `json:"read"`
	Accepted   int64            `json:"accepted"`
	Duplicates int64            `json:"duplicates"`
	Late       int64            `json:"late"`
	Rejected   map[string]int64 `json:"rejected"`
}

// PageCount is one entry of a window's top-pages list.
type PageCount struct {
	Page  string `json:"page"`
	Count int64  `json:"count"`
}

// WindowReport summarises one 5-minute event-time window.
type WindowReport struct {
	Start        time.Time        `json:"start"`
	Events       int64            `json:"events"`
	Purchases    int64            `json:"purchases"`
	RevenueCents int64            `json:"revenue_cents"`
	ByCategory   map[string]int64 `json:"by_category"`
	UniqueUsers  int64            `json:"unique_users"`   // may be approximate (HyperLogLog)
	P95LatencyMS float64          `json:"p95_latency_ms"` // may be approximate (P-square)
	TopPages     []PageCount      `json:"top_pages"`      // the 5 most viewed pages, count descending then page ascending
}

// Report is the pipeline's result.
type Report struct {
	Totals  Totals         `json:"totals"`
	Windows []WindowReport `json:"windows"` // ascending by Start; only windows that received at least one accepted event
}

// LatencyX100 converts a latency to the integer stored in the "latency_x100" column of events.col.
func LatencyX100(ms float64) int64 { return int64(math.Round(ms * 100)) }

// Tolerances say how far an APPROXIMATE answer may be from the exact one.
type Tolerances struct {
	UniqueUsersRel float64 // relative error allowed for UniqueUsers
	P95Rel         float64 // relative error allowed for P95LatencyMS
	TopPageRel     float64 // relative error allowed for the counts in TopPages
}

// DefaultTolerances are what the acceptance test uses.
var DefaultTolerances = Tolerances{UniqueUsersRel: 0.03, P95Rel: 0.05, TopPageRel: 0.02}

// Diff compares a pipeline Report with the oracle's exact one and returns a human-readable list of differences (empty = match).
// Everything is compared EXACTLY except the three approximate fields, which use tol. GIVEN.
func Diff(got, want Report, tol Tolerances) []string {
	var d []string
	add := func(format string, args ...any) { d = append(d, fmt.Sprintf(format, args...)) }

	g, w := got.Totals, want.Totals
	if g.Read != w.Read || g.Accepted != w.Accepted || g.Duplicates != w.Duplicates || g.Late != w.Late {
		add("totals: got read=%d accepted=%d duplicates=%d late=%d, want read=%d accepted=%d duplicates=%d late=%d",
			g.Read, g.Accepted, g.Duplicates, g.Late, w.Read, w.Accepted, w.Duplicates, w.Late)
	}
	reasons := map[string]bool{}
	for k := range g.Rejected {
		reasons[k] = true
	}
	for k := range w.Rejected {
		reasons[k] = true
	}
	for r := range reasons {
		if g.Rejected[r] != w.Rejected[r] {
			add("rejected[%s]: got %d, want %d", r, g.Rejected[r], w.Rejected[r])
		}
	}

	if len(got.Windows) != len(want.Windows) {
		add("windows: got %d, want %d", len(got.Windows), len(want.Windows))
		return d
	}
	for i, ww := range want.Windows {
		gw := got.Windows[i]
		at := func(format string, args ...any) {
			add("window %s: %s", ww.Start.Format("15:04"), fmt.Sprintf(format, args...))
		}
		if !gw.Start.Equal(ww.Start) {
			at("start is %s, want %s", gw.Start, ww.Start)
			continue
		}
		if gw.Events != ww.Events || gw.Purchases != ww.Purchases || gw.RevenueCents != ww.RevenueCents {
			at("events/purchases/revenue = %d/%d/%d, want %d/%d/%d", gw.Events, gw.Purchases, gw.RevenueCents, ww.Events, ww.Purchases, ww.RevenueCents)
		}
		cats := map[string]bool{}
		for k := range gw.ByCategory {
			cats[k] = true
		}
		for k := range ww.ByCategory {
			cats[k] = true
		}
		for c := range cats {
			if gw.ByCategory[c] != ww.ByCategory[c] {
				at("category %s: got %d, want %d", c, gw.ByCategory[c], ww.ByCategory[c])
			}
		}
		if math.Abs(float64(gw.UniqueUsers-ww.UniqueUsers)) > math.Max(1, tol.UniqueUsersRel*float64(ww.UniqueUsers)) {
			at("unique users: got %d, want %d (tolerance %.0f%%)", gw.UniqueUsers, ww.UniqueUsers, tol.UniqueUsersRel*100)
		}
		if math.Abs(gw.P95LatencyMS-ww.P95LatencyMS) > tol.P95Rel*ww.P95LatencyMS+0.05 {
			at("p95 latency: got %.2f, want %.2f (tolerance %.0f%%)", gw.P95LatencyMS, ww.P95LatencyMS, tol.P95Rel*100)
		}
		// top pages: the busiest page must be right, every page you list in your top 3 must be in the true top 5,
		// and counts of pages present in both lists must be within tolerance
		truth := map[string]int64{}
		for _, p := range ww.TopPages {
			truth[p.Page] = p.Count
		}
		if len(ww.TopPages) > 0 {
			if len(gw.TopPages) == 0 {
				at("top pages missing")
				continue
			}
			if len(ww.TopPages) > 1 && ww.TopPages[0].Count > ww.TopPages[1].Count && gw.TopPages[0].Page != ww.TopPages[0].Page {
				at("busiest page is %s, want %s", gw.TopPages[0].Page, ww.TopPages[0].Page)
			}
			for i, p := range gw.TopPages {
				exact, inTruth := truth[p.Page]
				if i < 3 && !inTruth {
					at("page %s is in your top 3 but not in the true top 5", p.Page)
				}
				if inTruth && math.Abs(float64(p.Count-exact)) > math.Max(1, tol.TopPageRel*float64(exact)) {
					at("page %s count: got %d, want %d (tolerance %.0f%%)", p.Page, p.Count, exact, tol.TopPageRel*100)
				}
			}
		}
	}
	return d
}

// SortedKeys is a small helper for deterministic iteration. GIVEN.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
