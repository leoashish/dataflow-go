package columnar

import "fmt"

// cursor reads varints out of a byte slice with bounds checking; the first problem sticks in err.
type cursor struct {
	b   []byte
	err error
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := Uvarint(c.b)
	if n <= 0 {
		c.err = fmt.Errorf("%w: bad or truncated varint", ErrCorrupt)
		return 0
	}
	c.b = c.b[n:]
	return v
}

func (c *cursor) varint() int64 { return UnZigZag(c.uvarint()) }

func (c *cursor) count(maxPossible int) int {
	n := c.uvarint()
	if c.err == nil && (n > MaxValues || n > uint64(maxPossible)) {
		c.err = fmt.Errorf("%w: implausible count %d", ErrCorrupt, n)
	}
	if c.err != nil {
		return 0 // NEVER hand back a size we just decided is not credible: the caller will allocate with it
	}
	return int(n)
}

func EncodePlain(vals []int64) []byte {
	out := AppendUvarint(nil, uint64(len(vals)))
	for _, v := range vals {
		out = AppendUvarint(out, ZigZag(v))
	}
	return out
}

func DecodePlain(b []byte) ([]int64, error) {
	c := &cursor{b: b}
	n := c.count(len(b))
	vals := make([]int64, 0, n)
	for i := 0; i < n && c.err == nil; i++ {
		vals = append(vals, c.varint())
	}
	if c.err != nil {
		return nil, c.err
	}
	return vals, nil
}

func EncodeDelta(vals []int64) []byte {
	out := AppendUvarint(nil, uint64(len(vals)))
	var prev int64
	for _, v := range vals {
		out = AppendUvarint(out, ZigZag(v-prev)) // wraps on overflow: decoding adds it back and wraps again
		prev = v
	}
	return out
}

func DecodeDelta(b []byte) ([]int64, error) {
	c := &cursor{b: b}
	n := c.count(len(b))
	vals := make([]int64, 0, n)
	var prev int64
	for i := 0; i < n && c.err == nil; i++ {
		prev += c.varint()
		vals = append(vals, prev)
	}
	if c.err != nil {
		return nil, c.err
	}
	return vals, nil
}

func EncodeRLE(vals []int64) []byte {
	var body []byte
	runs := 0
	for i := 0; i < len(vals); {
		j := i
		for j < len(vals) && vals[j] == vals[i] {
			j++
		}
		body = AppendUvarint(body, ZigZag(vals[i]))
		body = AppendUvarint(body, uint64(j-i))
		runs++
		i = j
	}
	return append(AppendUvarint(nil, uint64(runs)), body...)
}

func DecodeRLE(b []byte) ([]int64, error) {
	c := &cursor{b: b}
	runs := c.count(len(b))
	var vals []int64
	for r := 0; r < runs && c.err == nil; r++ {
		v := c.varint()
		length := c.uvarint()
		if c.err != nil {
			break
		}
		if length == 0 || length > MaxValues || uint64(len(vals))+length > MaxValues {
			c.err = fmt.Errorf("%w: bad run length %d", ErrCorrupt, length)
			break
		}
		for k := uint64(0); k < length; k++ {
			vals = append(vals, v)
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	return vals, nil
}

func ChooseIntEncoding(vals []int64) Encoding {
	best, bestSize := EncPlain, len(EncodePlain(vals))
	if size := len(EncodeDelta(vals)); size < bestSize {
		best, bestSize = EncDelta, size
	}
	if size := len(EncodeRLE(vals)); size < bestSize {
		best = EncRLE
	}
	return best
}
