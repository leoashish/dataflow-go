package join

func HashJoin(left, right []Row, leftKey, rightKey string, kind Kind) []Row {
	build := map[string][]Row{}
	for _, r := range right {
		if k, ok := keyOf(r, rightKey); ok {
			build[k] = append(build[k], r)
		}
	}
	var out []Row
	for _, l := range left {
		k, ok := keyOf(l, leftKey)
		var matches []Row
		if ok {
			matches = build[k]
		}
		if len(matches) == 0 {
			if kind == LeftOuter {
				out = append(out, copyRow(l))
			}
			continue
		}
		for _, r := range matches {
			out = append(out, merge(l, r))
		}
	}
	return out
}
