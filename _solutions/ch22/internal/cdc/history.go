package cdc

type History struct {
	versions map[string][]Version // per key, chronological
}

func NewHistory() *History { return &History{versions: map[string][]Version{}} }

func (h *History) Apply(e Event) error {
	if e.Op != OpInsert && e.Op != OpUpdate && e.Op != OpDelete {
		return nil
	}
	vs := h.versions[e.Key]
	if n := len(vs); n > 0 {
		last := vs[n-1]
		if e.TS < last.ValidFrom || (!last.Current && e.TS < last.ValidTo) {
			return ErrOutOfOrder
		}
	}
	var current *Version
	if n := len(vs); n > 0 && vs[n-1].Current {
		current = &vs[n-1]
	}
	if e.Op == OpDelete {
		if current != nil {
			current.ValidTo, current.Current = e.TS, false
		}
		return nil
	}
	if current != nil {
		if sameRow(current.Row, e.Row) {
			return nil // nothing changed: no new version
		}
		current.ValidTo, current.Current = e.TS, false
	}
	h.versions[e.Key] = append(vs, Version{Key: e.Key, Row: cloneRow(e.Row), ValidFrom: e.TS, ValidTo: OpenEnded, Current: true})
	return nil
}

func (h *History) AsOf(key string, ts int64) (map[string]string, bool) {
	for _, v := range h.versions[key] {
		if v.ValidFrom <= ts && ts < v.ValidTo {
			return cloneRow(v.Row), true
		}
	}
	return nil, false
}

func (h *History) Versions(key string) []Version {
	vs := h.versions[key]
	if len(vs) == 0 {
		return nil
	}
	out := make([]Version, len(vs))
	for i, v := range vs {
		v.Row = cloneRow(v.Row)
		out[i] = v
	}
	return out
}

func (h *History) Current() map[string]map[string]string {
	out := map[string]map[string]string{}
	for key, vs := range h.versions {
		if n := len(vs); n > 0 && vs[n-1].Current {
			out[key] = cloneRow(vs[n-1].Row)
		}
	}
	return out
}
