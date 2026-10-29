// Package oracle computes the EXPECTED result of the capstone the slow, obvious way: single-threaded, everything in memory,
// exact statistics. It is written independently of any pipeline and is the judge in the acceptance test. GIVEN.
package oracle

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"dataflow/capstone/model"
	"dataflow/internal/columnar"
)

// Expected is the oracle's answer.
type Expected struct {
	Report   model.Report
	Accepted []model.Event // accepted events, in input order
}

type raw struct {
	EventID     *string  `json:"event_id"`
	UserID      *string  `json:"user_id"`
	Page        *string  `json:"page"`
	Category    *string  `json:"category"`
	Action      *string  `json:"action"`
	AmountCents *int64   `json:"amount_cents"`
	LatencyMS   *float64 `json:"latency_ms"`
	TS          *string  `json:"ts"`
}

// Parse validates one line (SPEC.md, "Validation") and returns the event or the rejection reason. Exposed so that the pipeline's
// parser can be tested against it line by line.
func Parse(line []byte) (model.Event, string) {
	var r raw
	if err := json.Unmarshal(line, &r); err != nil {
		return model.Event{}, model.ReasonMalformedJSON
	}
	empty := func(s *string) bool { return s == nil || *s == "" }
	if empty(r.EventID) || empty(r.UserID) || empty(r.Page) || empty(r.Category) || empty(r.Action) || empty(r.TS) || r.LatencyMS == nil {
		return model.Event{}, model.ReasonMissingField
	}
	if a := *r.Action; a != "view" && a != "click" && a != "purchase" {
		return model.Event{}, model.ReasonBadAction
	}
	ts, err := time.Parse(time.RFC3339Nano, *r.TS)
	if err != nil {
		return model.Event{}, model.ReasonBadTimestamp
	}
	amount := int64(0)
	if r.AmountCents != nil {
		amount = *r.AmountCents
	}
	if *r.LatencyMS < 0 || math.IsNaN(*r.LatencyMS) || math.IsInf(*r.LatencyMS, 0) || amount < 0 {
		return model.Event{}, model.ReasonBadNumber
	}
	return model.Event{EventID: *r.EventID, UserID: *r.UserID, Page: *r.Page, Category: *r.Category, Action: *r.Action,
		AmountCents: amount, LatencyMS: *r.LatencyMS, TS: ts.UTC()}, ""
}

type window struct {
	events, purchases, revenue int64
	byCategory                 map[string]int64
	users                      map[string]bool
	latencies                  []float64
	pages                      map[string]int64
}

// Compute reads dir/events-*.ndjson (in file-name order, lines in order) and applies the SPEC.
func Compute(dir string, cfg model.Config) (Expected, error) {
	cfg = cfg.WithDefaults()
	files, err := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if err != nil {
		return Expected{}, err
	}
	sort.Strings(files)

	var exp Expected
	exp.Report.Totals.Rejected = map[string]int64{}
	seen := map[string]bool{}
	windows := map[time.Time]*window{}
	var maxTS time.Time

	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			return Expected{}, err
		}
		br := bufio.NewReaderSize(f, 1<<20)
		for {
			line, rerr := br.ReadBytes('\n')
			if len(line) > 0 && string(line) != "\n" {
				exp.Report.Totals.Read++
				ev, reason := Parse(line)
				switch {
				case reason != "":
					exp.Report.Totals.Rejected[reason]++
				case seen[ev.EventID]: // first occurrence wins, across all files
					exp.Report.Totals.Duplicates++
				default:
					seen[ev.EventID] = true
					start := ev.TS.Truncate(cfg.WindowSize)
					watermark := maxTS.Add(-cfg.WatermarkDelay) // before this event
					late := !maxTS.IsZero() && !start.Add(cfg.WindowSize).Add(cfg.AllowedLateness).After(watermark)
					if ev.TS.After(maxTS) {
						maxTS = ev.TS
					}
					if late {
						exp.Report.Totals.Late++
						break
					}
					exp.Report.Totals.Accepted++
					exp.Accepted = append(exp.Accepted, ev)
					w := windows[start]
					if w == nil {
						w = &window{byCategory: map[string]int64{}, users: map[string]bool{}, pages: map[string]int64{}}
						windows[start] = w
					}
					w.events++
					if ev.Action == "purchase" {
						w.purchases++
						w.revenue += ev.AmountCents
					}
					w.byCategory[ev.Category]++
					w.users[ev.UserID] = true
					w.latencies = append(w.latencies, ev.LatencyMS)
					w.pages[ev.Page]++
				}
			}
			if rerr != nil {
				break
			}
		}
		f.Close()
	}

	starts := make([]time.Time, 0, len(windows))
	for s := range windows {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, s := range starts {
		w := windows[s]
		sort.Float64s(w.latencies)
		wr := model.WindowReport{Start: s, Events: w.events, Purchases: w.purchases, RevenueCents: w.revenue, ByCategory: w.byCategory,
			UniqueUsers: int64(len(w.users)), P95LatencyMS: w.latencies[int(math.Ceil(0.95*float64(len(w.latencies))))-1]}
		for page, n := range w.pages {
			wr.TopPages = append(wr.TopPages, model.PageCount{Page: page, Count: n})
		}
		sort.Slice(wr.TopPages, func(i, j int) bool {
			if wr.TopPages[i].Count != wr.TopPages[j].Count {
				return wr.TopPages[i].Count > wr.TopPages[j].Count
			}
			return wr.TopPages[i].Page < wr.TopPages[j].Page
		})
		if len(wr.TopPages) > 5 {
			wr.TopPages = wr.TopPages[:5]
		}
		exp.Report.Windows = append(exp.Report.Windows, wr)
	}
	return exp, nil
}

// Columns returns the accepted events as the columns that events.col must contain, in this order:
// ts_ms (int), user_id (string), category (string), action (string), amount_cents (int), latency_x100 (int).
func Columns(events []model.Event) []columnar.Column {
	n := len(events)
	ts, amount, lat := make([]int64, n), make([]int64, n), make([]int64, n)
	user, cat, act := make([]string, n), make([]string, n), make([]string, n)
	for i, e := range events {
		ts[i], amount[i], lat[i] = e.TS.UnixMilli(), e.AmountCents, model.LatencyX100(e.LatencyMS)
		user[i], cat[i], act[i] = e.UserID, e.Category, e.Action
	}
	return []columnar.Column{
		{Name: "ts_ms", Kind: columnar.KindInt64, Ints: ts},
		{Name: "user_id", Kind: columnar.KindString, Strs: user},
		{Name: "category", Kind: columnar.KindString, Strs: cat},
		{Name: "action", Kind: columnar.KindString, Strs: act},
		{Name: "amount_cents", Kind: columnar.KindInt64, Ints: amount},
		{Name: "latency_x100", Kind: columnar.KindInt64, Ints: lat},
	}
}

// Summary is a one-line description of an Expected, for logs.
func (e Expected) Summary() string {
	t := e.Report.Totals
	return fmt.Sprintf("read=%d accepted=%d duplicates=%d late=%d rejected=%v windows=%d", t.Read, t.Accepted, t.Duplicates, t.Late, t.Rejected, len(e.Report.Windows))
}
