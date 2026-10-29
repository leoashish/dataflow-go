// Package gen writes a deterministic, DIRTY clickstream dataset: the same Seed always produces the same bytes. GIVEN.
package gen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// Params describe the dataset. Every output "slot" becomes exactly one line, so each file has exactly EventsPerFile lines.
type Params struct {
	Seed          int64
	Files         int
	EventsPerFile int
	Users         int
	Pages         int
	DupRate       float64 // chance a slot repeats an earlier line verbatim (possibly from the previous file)
	BadRate       float64 // chance a slot is a broken record
	LateRate      float64 // chance a slot is a valid NEW event whose timestamp is 10-30 minutes behind the stream
}

// DefaultParams is the dataset the acceptance test uses: 20 000 lines, ~2% duplicates, ~2% bad lines, ~1% late events.
func DefaultParams() Params {
	return Params{Seed: 1, Files: 4, EventsPerFile: 5000, Users: 2000, Pages: 300, DupRate: 0.02, BadRate: 0.02, LateRate: 0.01}
}

// Base is the timestamp of the first event.
var Base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Step is the (average) gap between consecutive events.
const Step = 500 * time.Millisecond

type wireEvent struct {
	EventID     string  `json:"event_id"`
	UserID      string  `json:"user_id"`
	Page        string  `json:"page"`
	Category    string  `json:"category"`
	Action      string  `json:"action"`
	AmountCents int64   `json:"amount_cents"`
	LatencyMS   float64 `json:"latency_ms"`
	TS          string  `json:"ts"`
}

var categories = []string{"shoes", "shirts", "hats", "bags", "watches", "books", "games", "toys"}

// Generate writes events-000.ndjson ... into dir.
func Generate(dir string, p Params) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	rnd := rand.New(rand.NewSource(p.Seed))
	users := rand.NewZipf(rnd, 1.1, 1, uint64(max(p.Users-1, 1)))
	pages := rand.NewZipf(rnd, 1.4, 1, uint64(max(p.Pages-1, 1)))

	var recent []string // ring of the last lines emitted, for duplicates
	next, slot := 0, 0
	maxTS := Base

	valid := func(ts time.Time) wireEvent {
		next++
		action := "view"
		amount := int64(0)
		switch r := rnd.Float64(); {
		case r < 0.05:
			action, amount = "purchase", int64(500+rnd.Intn(19500))
		case r < 0.20:
			action = "click"
		}
		lat := math.Round(math.Exp(rnd.NormFloat64()*0.6+4.0)*10) / 10 // lognormal, one decimal
		return wireEvent{
			EventID: fmt.Sprintf("e-%08d", next), UserID: fmt.Sprintf("u%d", users.Uint64()), Page: fmt.Sprintf("/p/%d", pages.Uint64()),
			Category: categories[rnd.Intn(len(categories))], Action: action, AmountCents: amount, LatencyMS: lat, TS: ts.Format(time.RFC3339Nano),
		}
	}

	for f := 0; f < p.Files; f++ {
		out, err := os.Create(filepath.Join(dir, fmt.Sprintf("events-%03d.ndjson", f)))
		if err != nil {
			return err
		}
		w := bufio.NewWriter(out)
		for i := 0; i < p.EventsPerFile; i++ {
			var line string
			switch r := rnd.Float64(); {
			case r < p.BadRate:
				line = badLine(rnd, valid(Base.Add(time.Duration(slot)*Step)))
			case r < p.BadRate+p.DupRate && len(recent) > 0:
				line = recent[rnd.Intn(len(recent))]
			case r < p.BadRate+p.DupRate+p.LateRate && slot > 2000:
				ts := maxTS.Add(-time.Duration(10+rnd.Intn(20)) * time.Minute)
				b, _ := json.Marshal(valid(ts))
				line = string(b)
				recent = remember(recent, line)
			default:
				ts := Base.Add(time.Duration(slot)*Step - time.Duration(rnd.Intn(45))*time.Second) // slightly out of order: inside the watermark delay
				if ts.After(maxTS) {
					maxTS = ts
				}
				b, _ := json.Marshal(valid(ts))
				line = string(b)
				recent = remember(recent, line)
			}
			w.WriteString(line)
			w.WriteByte('\n')
			slot++
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}

func remember(recent []string, line string) []string {
	recent = append(recent, line)
	if len(recent) > 3000 {
		recent = recent[1:]
	}
	return recent
}

// badLine breaks a valid event in one of five ways (one per rejection reason).
func badLine(rnd *rand.Rand, e wireEvent) string {
	b, _ := json.Marshal(e)
	switch rnd.Intn(5) {
	case 0: // malformed JSON: cut off the end
		return string(b[:len(b)-7])
	case 1: // missing field
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		delete(m, "user_id")
		b, _ = json.Marshal(m)
		return string(b)
	case 2:
		e.Action = "hover"
	case 3:
		e.TS = "yesterday-ish"
	default:
		e.LatencyMS = -5
	}
	b, _ = json.Marshal(e)
	return string(b)
}
