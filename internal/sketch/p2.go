package sketch

import "dataflow/internal/todo"

// P2 estimates ONE quantile (say the 95th percentile of request latency) of a stream in O(1) memory -- five numbers, no
// matter how many values flow past. It is the P-square algorithm of Jain and Chlamtac (1985).
//
// WHY NOT JUST SORT? Exact quantiles need every value (a billion latencies = 8 GB, sorted). And a quantile, unlike a sum or a
// count, CANNOT be computed from per-shard quantiles: the median of medians is not the median (Chapter 11). So large systems
// use sketches (t-digest, KLL, DDSketch, HdrHistogram). P-square is the smallest possible one and shows the idea.
//
// THE IDEA: track FIVE MARKERS that estimate the minimum, the q/2 quantile, the q quantile, the (1+q)/2 quantile and the
// maximum. Each marker has a HEIGHT (the value it currently estimates) and a POSITION (its rank among the observations so
// far). After each new observation the markers are nudged toward the positions they SHOULD have, and their heights are
// re-estimated by fitting a parabola through the neighbours. The middle marker (index 2) is the answer.
//
// ALGORITHM (q = the wanted quantile, h = heights, n = actual positions, np = desired positions, dn = desired increments)
//
//	First five observations: store them. When the 5th arrives: sort them; h = the sorted values; n = [1 2 3 4 5];
//	    np = [1, 1+2q, 1+4q, 3+2q, 5];   dn = [0, q/2, q, (1+q)/2, 1].
//	Each further observation x:
//	  1. Find the cell k:  x < h0 -> h0 = x, k = 0 | x < h1 -> k = 0 | x < h2 -> k = 1 | x < h3 -> k = 2 | x <= h4 -> k = 3 | else h4 = x, k = 3.
//	  2. n[i]++ for every i > k.   np[i] += dn[i] for every i.
//	  3. For each MIDDLE marker i = 1, 2, 3:  d = np[i] - n[i].
//	     If (d >= 1 and n[i+1]-n[i] > 1) or (d <= -1 and n[i-1]-n[i] < -1):   (the marker is too far from where it belongs)
//	         d = sign(d)  (+1 or -1)
//	         try the PARABOLIC prediction
//	             h' = h[i] + d/(n[i+1]-n[i-1]) * ( (n[i]-n[i-1]+d) * (h[i+1]-h[i]) / (n[i+1]-n[i])
//	                                              + (n[i+1]-n[i]-d) * (h[i]-h[i-1]) / (n[i]-n[i-1]) )
//	         if h[i-1] < h' < h[i+1] use it, otherwise use the LINEAR one: h' = h[i] + d * (h[i+d]-h[i]) / (n[i+d]-n[i])
//	         n[i] += d
//
// Requirements:
//   - NewP2(q): q must be strictly between 0 and 1 (else an error wrapping ErrBadParameter).
//   - Add(x): NaN and +/-Inf are ignored (they would poison the markers). Count() is the number of values actually used.
//   - Value(): with FEWER THAN FIVE observations the answer is EXACT: sort what you have and take the nearest-rank element,
//     index ceil(q*n)-1; with none at all return math.NaN(). From five on, return the middle marker's height h[2].
type P2 struct{}

func NewP2(q float64) (*P2, error) { return nil, todo.NotImplemented("Ch18: NewP2") }

func (p *P2) Add(x float64) {} // TODO Ch18

func (p *P2) Value() float64 { return 0 } // TODO Ch18

func (p *P2) Count() int { return 0 } // TODO Ch18
