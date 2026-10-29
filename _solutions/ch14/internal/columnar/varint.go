package columnar

func AppendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func Uvarint(src []byte) (uint64, int) {
	var v uint64
	for i, b := range src {
		if i == 10 {
			return 0, -(i + 1) // more than 10 bytes cannot be a uint64
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, -(i + 1) // the 10th byte may only carry the single top bit
			}
			return v | uint64(b)<<(7*uint(i)), i + 1
		}
		v |= uint64(b&0x7f) << (7 * uint(i))
	}
	return 0, 0 // ran out of bytes in the middle of a number
}

func ZigZag(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }

func UnZigZag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }
