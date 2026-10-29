package columnar

import (
	"bytes"
	"fmt"
	"io"
)

const magic = "COL1"

func WriteColumns(w io.Writer, cols []Column) error {
	rows := -1
	var buf bytes.Buffer
	buf.WriteString(magic)
	buf.Write(AppendUvarint(nil, uint64(len(cols))))

	for _, col := range cols {
		var n int
		var enc Encoding
		var payload []byte
		switch col.Kind {
		case KindInt64:
			n = len(col.Ints)
			enc = ChooseIntEncoding(col.Ints)
			switch enc {
			case EncDelta:
				payload = EncodeDelta(col.Ints)
			case EncRLE:
				payload = EncodeRLE(col.Ints)
			default:
				payload = EncodePlain(col.Ints)
			}
		case KindString:
			n = len(col.Strs)
			enc = EncDict
			dict, codes := EncodeDict(col.Strs)
			payload = AppendUvarint(nil, uint64(len(dict)))
			for _, s := range dict {
				payload = AppendUvarint(payload, uint64(len(s)))
				payload = append(payload, s...)
			}
			width := 0
			if len(dict) > 0 {
				width = BitsNeeded(uint32(len(dict) - 1))
			}
			packed, err := BitPack(codes, width)
			if err != nil {
				return err
			}
			payload = append(payload, byte(width))
			payload = append(payload, packed...)
		default:
			return fmt.Errorf("columnar: column %q has unknown kind %d", col.Name, col.Kind)
		}
		if rows >= 0 && n != rows {
			return fmt.Errorf("columnar: column %q has %d rows, the others have %d", col.Name, n, rows)
		}
		rows = n

		buf.Write(AppendUvarint(nil, uint64(len(col.Name))))
		buf.WriteString(col.Name)
		buf.WriteByte(byte(col.Kind))
		buf.WriteByte(byte(enc))
		buf.Write(AppendUvarint(nil, uint64(n)))
		buf.Write(AppendUvarint(nil, uint64(len(payload))))
		buf.Write(payload)
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// reader pulls fixed pieces out of the file image with bounds checks; the first problem sticks in err.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrCorrupt, what)
	}
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := Uvarint(r.b)
	if n <= 0 {
		r.fail("bad or truncated varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.fail("a length runs past the end of the data")
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func ReadColumns(rd io.Reader) ([]Column, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	r := &reader{b: data}
	if string(r.bytes(uint64(len(magic)))) != magic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	ncols := r.uvarint()
	if r.err == nil && ncols > uint64(len(r.b)) {
		r.fail("implausible column count")
	}
	var cols []Column
	for i := uint64(0); i < ncols && r.err == nil; i++ {
		name := string(r.bytes(r.uvarint()))
		meta := r.bytes(2)
		rows := r.uvarint()
		payload := r.bytes(r.uvarint())
		if r.err != nil {
			break
		}
		col := Column{Name: name, Kind: Kind(meta[0]), Encoding: Encoding(meta[1])}
		if rows > MaxValues {
			r.fail("implausible row count")
			break
		}
		switch col.Kind {
		case KindInt64:
			var ints []int64
			switch col.Encoding {
			case EncPlain:
				ints, err = DecodePlain(payload)
			case EncDelta:
				ints, err = DecodeDelta(payload)
			case EncRLE:
				ints, err = DecodeRLE(payload)
			default:
				err = fmt.Errorf("%w: unknown int encoding %d", ErrCorrupt, col.Encoding)
			}
			if err == nil && uint64(len(ints)) != rows {
				err = fmt.Errorf("%w: column %q decodes to %d values, header says %d", ErrCorrupt, name, len(ints), rows)
			}
			col.Ints = ints
		case KindString:
			col.Strs, err = decodeStringPayload(payload, rows)
		default:
			err = fmt.Errorf("%w: unknown kind %d", ErrCorrupt, col.Kind)
		}
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if r.err != nil {
		return nil, r.err
	}
	return cols, nil
}

func decodeStringPayload(payload []byte, rows uint64) ([]string, error) {
	r := &reader{b: payload}
	dictSize := r.uvarint()
	if r.err == nil && dictSize > uint64(len(r.b)) {
		r.fail("implausible dictionary size")
	}
	if r.err != nil {
		return nil, r.err // before allocating anything sized by an untrusted number
	}
	dict := make([]string, 0, dictSize)
	for i := uint64(0); i < dictSize && r.err == nil; i++ {
		dict = append(dict, string(r.bytes(r.uvarint())))
	}
	widthByte := r.bytes(1)
	if r.err != nil {
		return nil, r.err
	}
	codes, err := BitUnpack(r.b, int(widthByte[0]), int(rows))
	if err != nil {
		return nil, err
	}
	return DecodeDict(dict, codes)
}
