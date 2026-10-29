package cdc

import "sort"

type Table struct {
	rows    map[string]map[string]string
	lastSeq map[string]uint64 // highest Seq applied per key, deletes included (tombstones)
}

func NewTable() *Table {
	return &Table{rows: map[string]map[string]string{}, lastSeq: map[string]uint64{}}
}

func (t *Table) Apply(e Event) bool {
	if e.Op != OpInsert && e.Op != OpUpdate && e.Op != OpDelete {
		return false
	}
	if last, seen := t.lastSeq[e.Key]; seen && e.Seq <= last {
		return false // a duplicate, or a stale event that arrived late
	}
	t.lastSeq[e.Key] = e.Seq
	if e.Op == OpDelete {
		delete(t.rows, e.Key)
	} else {
		t.rows[e.Key] = cloneRow(e.Row)
	}
	return true
}

func (t *Table) ApplyAll(events []Event) int {
	applied := 0
	for _, e := range events {
		if t.Apply(e) {
			applied++
		}
	}
	return applied
}

func (t *Table) Get(key string) (map[string]string, bool) {
	row, ok := t.rows[key]
	if !ok {
		return nil, false
	}
	return cloneRow(row), true
}

func (t *Table) Rows() map[string]map[string]string {
	out := make(map[string]map[string]string, len(t.rows))
	for k, r := range t.rows {
		out[k] = cloneRow(r)
	}
	return out
}

func (t *Table) Len() int { return len(t.rows) }

func Compact(events []Event) []Event {
	latest := map[string]Event{}
	for _, e := range events {
		if cur, ok := latest[e.Key]; !ok || e.Seq > cur.Seq {
			latest[e.Key] = e
		}
	}
	out := make([]Event, 0, len(latest))
	for _, e := range latest {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}
