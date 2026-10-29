package columnar

// AppendUvarint appends v to dst in LEB128 ("varint") form and returns the extended slice.
//
// WHY: most integers in real data are small (counts, ids deltas, lengths), yet a fixed int64 spends 8 bytes on all of
// them. A varint spends 7 bits of payload per byte and uses the TOP bit as a "more bytes follow" flag:
//
//	  1 -> 0x01                (1 byte)
//	127 -> 0x7F                (1 byte)
//	128 -> 0x80 0x01           (2 bytes: low 7 bits first, flag set, then the rest)
//	300 -> 0xAC 0x02
//
// Algorithm: while v >= 0x80 { append(byte(v) | 0x80); v >>= 7 }; append(byte(v)). A uint64 needs at most 10 bytes.
func AppendUvarint(dst []byte, v uint64) []byte {
	return dst // TODO Ch14
}

// Uvarint decodes a varint from the start of src and returns (value, bytesConsumed).
//   - n > 0: success.
//   - n == 0: src ended before the number did (truncated input).
//   - n < 0: the number does not fit in 64 bits (more than 10 bytes, or a 10th byte > 1): corrupt.
//
// (Same contract as encoding/binary.Uvarint -- the tests cross-check you against it. You are re-deriving it on purpose.)
func Uvarint(src []byte) (uint64, int) {
	return 0, 0 // TODO Ch14
}

// ZigZag maps signed to unsigned so that SMALL MAGNITUDES stay small: 0->0, -1->1, 1->2, -2->3, 2->4 ...
// A raw int64 -1 is 0xFFFF...FF: ten varint bytes. ZigZag(-1) = 1: one byte. Formula: uint64((v << 1) ^ (v >> 63)).
func ZigZag(v int64) uint64 { return 0 } // TODO Ch14

// UnZigZag is the inverse: int64(u>>1) ^ -int64(u&1).
func UnZigZag(u uint64) int64 { return 0 } // TODO Ch14
