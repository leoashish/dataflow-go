package columnar

import (
	"fmt"
	"math/bits"
)

func EncodeDict(vals []string) ([]string, []uint32) {
	index := map[string]uint32{}
	dict := []string{}
	codes := make([]uint32, len(vals))
	for i, v := range vals {
		code, seen := index[v]
		if !seen {
			code = uint32(len(dict))
			index[v] = code
			dict = append(dict, v)
		}
		codes[i] = code
	}
	return dict, codes
}

func DecodeDict(dict []string, codes []uint32) ([]string, error) {
	out := make([]string, len(codes))
	for i, c := range codes {
		if int64(c) >= int64(len(dict)) {
			return nil, fmt.Errorf("%w: code %d outside a dictionary of %d", ErrCorrupt, c, len(dict))
		}
		out[i] = dict[c]
	}
	return out, nil
}

func BitsNeeded(max uint32) int { return bits.Len32(max) }

func BitPack(vals []uint32, width int) ([]byte, error) {
	if width < 0 || width > 32 {
		return nil, fmt.Errorf("columnar: bit width %d outside 0..32", width)
	}
	out := make([]byte, 0, (len(vals)*width+7)/8)
	var acc uint64
	var nbits uint
	for _, v := range vals {
		if uint64(v) >= uint64(1)<<uint(width) {
			return nil, fmt.Errorf("columnar: value %d does not fit in %d bits", v, width)
		}
		acc |= uint64(v) << nbits
		nbits += uint(width)
		for nbits >= 8 {
			out = append(out, byte(acc))
			acc >>= 8
			nbits -= 8
		}
	}
	if nbits > 0 {
		out = append(out, byte(acc))
	}
	return out, nil
}

func BitUnpack(b []byte, width, count int) ([]uint32, error) {
	if width < 0 || width > 32 || count < 0 || count > MaxValues {
		return nil, fmt.Errorf("%w: bad width %d or count %d", ErrCorrupt, width, count)
	}
	if need := (count*width + 7) / 8; len(b) < need {
		return nil, fmt.Errorf("%w: %d bytes given, %d needed", ErrCorrupt, len(b), need)
	}
	out := make([]uint32, count)
	var acc uint64
	var nbits uint
	pos := 0
	mask := uint64(1)<<uint(width) - 1
	for i := range out {
		for nbits < uint(width) {
			acc |= uint64(b[pos]) << nbits
			pos++
			nbits += 8
		}
		out[i] = uint32(acc & mask)
		acc >>= uint(width)
		nbits -= uint(width)
	}
	return out, nil
}
