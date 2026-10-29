// Package join implements the three classic join algorithms. Every database and every Spark job uses one of them.
package join

// Row is one record: column name -> text value. (Real engines use typed columns; text keeps the focus on the algorithm.)
type Row map[string]string

// Kind selects which rows survive.
type Kind int

const (
	Inner     Kind = iota // only rows that have a match on both sides
	LeftOuter             // every LEFT row; matched ones are combined with each match, unmatched ones are kept alone
)

// Iter yields rows one at a time: (row, true) until exhausted, then (nil, false). It is how big inputs are fed in.
type Iter func() (Row, bool)

// SliceIter turns a slice into an Iter. GIVEN.
func SliceIter(rows []Row) Iter {
	i := 0
	return func() (Row, bool) {
		if i >= len(rows) {
			return nil, false
		}
		i++
		return rows[i-1], true
	}
}

// merge builds the output row for a matching pair. GIVEN: all LEFT columns, then all RIGHT columns; a right column whose
// name clashes with a LEFT column is stored as name+"_r" (so "id" on both sides gives "id" and "id_r").
func merge(l, r Row) Row {
	out := make(Row, len(l)+len(r))
	for k, v := range l {
		out[k] = v
	}
	for k, v := range r {
		if _, clash := l[k]; clash {
			out[k+"_r"] = v
		} else {
			out[k] = v
		}
	}
	return out
}

// copyRow returns an independent copy of r (an unmatched LEFT OUTER row is emitted alone). GIVEN.
func copyRow(r Row) Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// keyOf returns the join key of a row and whether it HAS one. A missing column or an empty value means "no key" -- like
// SQL NULL, which never equals anything (not even another NULL). Rows without a key never match. GIVEN.
func keyOf(r Row, column string) (string, bool) {
	k, ok := r[column]
	return k, ok && k != ""
}

// GraceOptions configures GraceHashJoin.
type GraceOptions struct {
	Partitions int    // how many partitions to cut both inputs into (>= 1)
	TmpDir     string // where the partition files go ("" = system temp dir)
}

// GraceStats reports what a GraceHashJoin did.
type GraceStats struct {
	Partitions   int // = GraceOptions.Partitions
	SpilledLeft  int // left rows (that have a key) written to partition files
	SpilledRight int // right rows (that have a key) written to partition files
	MaxBuildRows int // the most right-side rows held in a hash table AT ONE TIME (= the biggest right partition)
}
