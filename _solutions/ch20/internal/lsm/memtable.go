package lsm

import "sort"

type Memtable struct {
	m map[string]Entry
}

func NewMemtable() *Memtable { return &Memtable{m: map[string]Entry{}} }

func (m *Memtable) Put(key, value string) { m.m[key] = Entry{Key: key, Value: value} }

func (m *Memtable) Delete(key string) { m.m[key] = Entry{Key: key, Tombstone: true} }

func (m *Memtable) Get(key string) (Entry, bool) {
	e, ok := m.m[key]
	return e, ok
}

func (m *Memtable) Len() int { return len(m.m) }

func (m *Memtable) Entries() []Entry {
	out := make([]Entry, 0, len(m.m))
	for _, e := range m.m {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
