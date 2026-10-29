package sketch

import "dataflow/internal/todo"

// CountMin is a Count-Min sketch: it estimates HOW MANY TIMES each key was seen in a stream, using memory that does NOT
// grow with the number of distinct keys. (A map[string]int over 500 million distinct URLs needs tens of GB.)
//
// STRUCTURE: a grid of counters, `depth` rows by `width` columns, all 0, and one hash function per row.
//
//	Add(key, n):    in EVERY row i, add n to the counter at column  h_i(key) mod width.
//	Estimate(key):  take the MINIMUM of the `depth` counters the key maps to.
//
// WHY THE MINIMUM: other keys that collide with yours can only ADD to a counter, never subtract. So every counter is
// >= the truth, and the smallest of them is the least polluted. Therefore:
//
//	Estimate(key) >= true count          ALWAYS (a CMS never undercounts), and
//	Estimate(key) <= true count + epsilon * N     with probability >= 1 - delta        (N = total of all Adds)
//
// SIZING: width = ceil(e / epsilon), depth = ceil(ln(1 / delta)), where e = math.E.
// (epsilon = 0.001, delta = 0.01 -> 2719 columns x 5 rows = 13 595 counters = 109 KB, for ANY number of keys.)
//
// Row hash: hashx.Sum64Seed(key, uint64(i)) % uint64(width) -- one independent-looking hash per row via the seed.
//
// Requirements:
//   - NewCountMin(epsilon, delta): epsilon and delta must each be strictly between 0 and 1 (else an error wrapping ErrBadParameter).
//   - Width(), Depth() report the grid; Total() = the sum of all counts added (N).
//   - Merge(o): add the counter grids cell by cell (same Width and Depth, else ErrShapeMismatch) and the totals.
//     Merging two sketches gives exactly the sketch you'd get from adding both streams to one: parallel counting for free.
//   - Counters are uint64.
type CountMin struct{}

func NewCountMin(epsilon, delta float64) (*CountMin, error) {
	return nil, todo.NotImplemented("Ch16: NewCountMin")
}

func (c *CountMin) Add(key string, n uint64) {} // TODO Ch16

func (c *CountMin) Estimate(key string) uint64 { return 0 } // TODO Ch16

func (c *CountMin) Total() uint64 { return 0 } // TODO Ch16

func (c *CountMin) Width() int { return 0 } // TODO Ch16

func (c *CountMin) Depth() int { return 0 } // TODO Ch16

func (c *CountMin) Merge(o *CountMin) error { return todo.NotImplemented("Ch16: CountMin.Merge") }
