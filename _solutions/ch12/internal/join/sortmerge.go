package join

import "sort"

func SortMergeJoin(left, right []Row, leftKey, rightKey string, kind Kind) []Row {
	split := func(rows []Row, column string) (keyed, keyless []Row) {
		for _, r := range rows {
			if _, ok := keyOf(r, column); ok {
				keyed = append(keyed, r)
			} else {
				keyless = append(keyless, r)
			}
		}
		return
	}
	lrows, lkeyless := split(left, leftKey)
	rrows, _ := split(right, rightKey)
	sort.SliceStable(lrows, func(i, j int) bool { return lrows[i][leftKey] < lrows[j][leftKey] })
	sort.SliceStable(rrows, func(i, j int) bool { return rrows[i][rightKey] < rrows[j][rightKey] })

	var out []Row
	i, j := 0, 0
	for i < len(lrows) {
		lk := lrows[i][leftKey]
		for j < len(rrows) && rrows[j][rightKey] < lk {
			j++ // right rows with a smaller key can never match anything further on
		}
		// the left key group [i, iEnd) and, if it exists, the right key group [j, jEnd)
		iEnd := i
		for iEnd < len(lrows) && lrows[iEnd][leftKey] == lk {
			iEnd++
		}
		jEnd := j
		for jEnd < len(rrows) && rrows[jEnd][rightKey] == lk {
			jEnd++
		}
		for _, l := range lrows[i:iEnd] {
			if jEnd > j {
				for _, r := range rrows[j:jEnd] {
					out = append(out, merge(l, r))
				}
			} else if kind == LeftOuter {
				out = append(out, copyRow(l))
			}
		}
		i = iEnd
	}
	if kind == LeftOuter {
		for _, l := range lkeyless {
			out = append(out, copyRow(l))
		}
	}
	return out
}
