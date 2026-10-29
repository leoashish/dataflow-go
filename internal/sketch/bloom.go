package sketch

import "dataflow/internal/todo"

// Bloom is a Bloom filter: a SET that uses a tiny, fixed amount of memory and answers "have I seen x?" with
//
//	"definitely NOT"   -- always correct (no false negatives), or
//	"PROBABLY yes"     -- wrong with a probability you choose in advance (a false positive).
//
// HOW IT WORKS: an array of m bits, all 0, and k hash functions. Add(x): set the k bits h1(x)..hk(x). Contains(x): check
// whether ALL k bits are set. An item you never added can still find its k bits set by OTHER items -- a false positive --
// but an item you did add always finds its bits set -- never a false negative.
//
// SIZING (the maths, worth knowing): for n expected items and a target false-positive rate p
//
//	m = ceil( -n * ln(p) / (ln 2)^2 )      bits        (n=1000, p=1%  -> 9586 bits = 1.2 KB. A map of 1000 strings: 50+ KB)
//	k = round( m / n * ln 2 ), at least 1               (-> 7 hash functions)
//
// THE DOUBLE-HASHING TRICK (Kirsch-Mitzenmacher): you don't need k independent hashes. Compute two, h1 and h2, and use
// index_i = (h1 + i*h2) mod m for i in 0..k-1. Use h1 = hashx.Sum64(s) and h2 = hashx.Sum64Seed(s, 1) | 1 (the |1 makes h2
// odd, so the indexes cycle through all positions).
//
// WHERE IT IS USED: "skip the expensive lookup if the key is definitely absent": every LSM database (Chapter 20) keeps
// one per file; crawlers ("seen this URL?"), spell-checkers, CDN caches, `bloom` in front of a slow dedup store.
//
// Requirements:
//   - NewBloom(expectedItems, fpRate): expectedItems < 1 or fpRate not strictly between 0 and 1 -> an error wrapping ErrBadParameter.
//   - Bits() = m, Hashes() = k as above (math.Ceil, math.Round, math.Log).
//   - Add / Contains as described. Store the bits in a []uint64 (64 bits per word) and use shifts and masks.
//   - FillRatio() = the fraction of bits that are set (it should hover near 0.5 when the filter holds its expected n).
//   - EstimatedFPR() = FillRatio()^k : the false-positive rate RIGHT NOW (it grows as you add more than n items).
//   - Merge(o): UNION -- OR the bit arrays. Both must have the same Bits() and Hashes() (else ErrShapeMismatch). Do not modify o.
//     Merging is why Bloom filters are great in distributed jobs: each worker builds one, the driver ORs them together.
type Bloom struct{}

func NewBloom(expectedItems int, fpRate float64) (*Bloom, error) {
	return nil, todo.NotImplemented("Ch15: NewBloom")
}

func (b *Bloom) Add(s string) {} // TODO Ch15

func (b *Bloom) Contains(s string) bool { return false } // TODO Ch15

func (b *Bloom) Bits() int { return 0 } // TODO Ch15

func (b *Bloom) Hashes() int { return 0 } // TODO Ch15

func (b *Bloom) FillRatio() float64 { return 0 } // TODO Ch15

func (b *Bloom) EstimatedFPR() float64 { return 1 } // TODO Ch15

func (b *Bloom) Merge(o *Bloom) error { return todo.NotImplemented("Ch15: Bloom.Merge") }
