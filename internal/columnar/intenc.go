package columnar

import "dataflow/internal/todo"

// All three integer encodings start with a uvarint COUNT so they are self-describing, and all decoders must:
//   - return an error wrapping ErrCorrupt for truncated or malformed input (never panic);
//   - refuse to allocate more than MaxValues values, and never trust a count that cannot possibly fit in the bytes given
//     (every value needs at least one byte, so count > len(b) is corrupt).
//
// Layouts (v = zigzag-varint, u = plain uvarint):
//   Plain:  u(count)  v(x0) v(x1) v(x2) ...
//   Delta:  u(count)  v(x0) v(x1-x0) v(x2-x1) ...        (decode by running sum; arithmetic wraps, so any int64 works)
//   RLE:    u(runs)   v(value) u(runLength)  v(value) u(runLength) ...      (runLength >= 1; the total must be <= MaxValues)
//
// WHEN EACH WINS:  Plain for random numbers.  Delta for sorted / slowly-changing data such as timestamps and ids (deltas
// are small even when the values are huge).  RLE for data with long runs of repeats: a status column, a sorted category.

func EncodePlain(vals []int64) []byte { return nil } // TODO Ch14

func DecodePlain(b []byte) ([]int64, error) { return nil, todo.NotImplemented("Ch14: DecodePlain") }

func EncodeDelta(vals []int64) []byte { return nil } // TODO Ch14

func DecodeDelta(b []byte) ([]int64, error) { return nil, todo.NotImplemented("Ch14: DecodeDelta") }

func EncodeRLE(vals []int64) []byte { return nil } // TODO Ch14

func DecodeRLE(b []byte) ([]int64, error) { return nil, todo.NotImplemented("Ch14: DecodeRLE") }

// ChooseIntEncoding returns the encoding (EncPlain, EncDelta or EncRLE) whose output is SMALLEST for these values, trying
// them in that order and switching only when strictly smaller (so ties keep the earlier one). Empty input -> EncPlain.
func ChooseIntEncoding(vals []int64) Encoding { return 0 } // TODO Ch14
