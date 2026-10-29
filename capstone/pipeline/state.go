package pipeline

import "dataflow/capstone/model"

// State is everything the pipeline remembers between lines. ONE goroutine owns it (no locks), and it must be serialisable
// by SaveCheckpoint.
//
// TODO(capstone): decide what lives here and how big it may grow:
//   - dedup: which event ids were seen (an exact set is acceptable for this dataset; ch 07's Deduper bounds it, ch 15's Bloom
//     filter shrinks it -- but read the SPEC: duplicates may be far apart, and a false positive DROPS a real event);
//   - the watermark and the lateness rule (ch 06);
//   - per-window aggregates: counts, revenue, per-category counts (ch 11), unique users (ch 17 or exact), p95 latency (ch 18 or
//     exact), top pages (ch 16 or exact);
//   - the accepted events, or a streaming sink for them (ch 14).
type State struct{}

// NewState returns an empty state for a run with this configuration.
func NewState(cfg model.Config) *State { return &State{} }

// Observe applies ONE parsed line, in input order.
func (s *State) Observe(p Parsed) {}

// Report summarises everything observed so far (it may be called once at the end, or after every checkpoint).
func (s *State) Report() model.Report { return model.Report{} }

// Accepted returns the accepted events in input order.
func (s *State) Accepted() []model.Event { return nil }
