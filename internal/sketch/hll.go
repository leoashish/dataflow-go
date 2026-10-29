package sketch

import "dataflow/internal/todo"

// HLL is a HyperLogLog sketch: it estimates the number of DISTINCT items in a stream using a few kilobytes, however many
// items there are. ("How many unique users visited today?" -- an exact answer needs a set of every user id; a billion ids is
// tens of GB. A p=14 HyperLogLog is 16 KB and is wrong by about 0.8%.)
//
// THE IDEA (a probability fact): hash every item to a uniformly random 64-bit number. In a random bit string, the chance
// that it STARTS with k zeros is 1/2^k. So if the longest run of leading zeros you have EVER seen is 20, you have probably
// looked at about 2^20 different hashes -- a rare event only happens after many trials. Repeats of the same item produce the
// same hash and so change nothing: duplicates are free.
//
// One such "longest run" is a very noisy estimate (one lucky hash ruins it). So split the hashes into m = 2^p BUCKETS
// (registers), keep a longest-run per bucket, and AVERAGE them (a harmonic mean, which is robust against outliers).
// The standard error is about 1.04 / sqrt(m): p=12 (4096 registers, 4 KB) -> 1.6%; p=14 -> 0.8%; p=16 -> 0.4%.
//
// ALGORITHM
//
//	Add(s):  x = hashx.Sum64(s)
//	         idx = the TOP p bits of x                      (x >> (64-p))      -> which register
//	         w   = the REMAINING bits, shifted to the top   (x << p) with a guard bit set at position p-1 so it is never all zero
//	         rho = (number of leading zeros of w) + 1       (bits.LeadingZeros64)
//	         register[idx] = max(register[idx], rho)
//	Count(): E = alpha_m * m^2 / sum over registers of 2^(-register)         (a harmonic mean)
//	         alpha_m = 0.673 (m=16), 0.697 (m=32), 0.709 (m=64), else 0.7213 / (1 + 1.079/m)
//	         SMALL-RANGE CORRECTION: if E <= 2.5*m and V > 0 registers are still zero, use LINEAR COUNTING instead:
//	         E = m * ln(m / V). (With few items most registers are empty, and the harmonic mean is biased; counting the
//	         empty buckets is nearly exact there.)
//	         Return math.Round(E) as a uint64.
//
// Requirements:
//   - NewHLL(p): p must be 4..16 inclusive, else an error wrapping ErrBadParameter. Registers are one byte each: 2^p bytes.
//   - Add, Count as above. An empty sketch counts 0. Adding the same item again NEVER changes the result.
//   - Merge(o): the register-wise MAX (the union of the two sets!). Same p required (else ErrShapeMismatch). Do not modify o.
//     Merging is lossless: each worker keeps a sketch of its shard, the driver takes the max -- distributed COUNT(DISTINCT).
//   - Precision() = p; SizeBytes() = 2^p.
type HLL struct{}

func NewHLL(p int) (*HLL, error) { return nil, todo.NotImplemented("Ch17: NewHLL") }

func (h *HLL) Add(s string) {} // TODO Ch17

func (h *HLL) Count() uint64 { return 0 } // TODO Ch17

func (h *HLL) Merge(o *HLL) error { return todo.NotImplemented("Ch17: HLL.Merge") }

func (h *HLL) Precision() int { return 0 } // TODO Ch17

func (h *HLL) SizeBytes() int { return 0 } // TODO Ch17
