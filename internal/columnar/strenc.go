package columnar

import "dataflow/internal/todo"

// EncodeDict replaces repeated strings by small integer CODES. dict lists each DISTINCT string once, in order of first
// appearance; codes[i] is the index in dict of vals[i].
//
//	vals:  "error" "ok" "ok" "ok" "error" "ok"        dict: ["error" "ok"]     codes: 0 1 1 1 0 1
//
// A column of 10 million "country" values with 200 distinct countries becomes 200 strings plus 10 million tiny numbers.
// Use a map[string]uint32 while encoding.
func EncodeDict(vals []string) (dict []string, codes []uint32) { return nil, nil } // TODO Ch14

// DecodeDict rebuilds the values. A code outside the dictionary is corrupt data: return an error wrapping ErrCorrupt.
func DecodeDict(dict []string, codes []uint32) ([]string, error) {
	return nil, todo.NotImplemented("Ch14: DecodeDict")
}

// BitsNeeded is the number of bits required to store any value in [0, max]: 0 for max = 0 (a column of all zeros needs
// no bits at all!), 1 for 1, 2 for 2..3, 3 for 4..7, ..., 32 for MaxUint32. Hint: math/bits.Len32.
func BitsNeeded(max uint32) int { return -1 } // TODO Ch14

// BitPack stores each value in exactly `width` bits (0..32), packed back to back, least-significant bits first. Codes that
// fit in 3 bits then take 3 bits each instead of 32 -- a 10x saving.
//
//	width 3, values 5 (101), 2 (010), 7 (111):   byte0 = 111 010 10 ... bits fill from the LOW end:  value i occupies
//	bits [i*width, (i+1)*width) of the stream, where bit 0 is the least significant bit of byte 0.
//
// Output length is ceil(len(vals)*width/8) bytes. A value that does not fit in `width` bits, or width outside 0..32, is an
// error. Width 0 is allowed (all values must be 0; the output is empty).
// Method: a uint64 accumulator: acc |= uint64(v) << nbits; nbits += width; while nbits >= 8 { emit byte(acc); acc >>= 8; nbits -= 8 }.
func BitPack(vals []uint32, width int) ([]byte, error) {
	return nil, todo.NotImplemented("Ch14: BitPack")
}

// BitUnpack reads `count` values of `width` bits each. If b is shorter than ceil(count*width/8) bytes, or width is outside
// 0..32, or count is negative or above MaxValues, return an error wrapping ErrCorrupt.
func BitUnpack(b []byte, width, count int) ([]uint32, error) {
	return nil, todo.NotImplemented("Ch14: BitUnpack")
}
