package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checksum(lengthBytes, payload []byte) uint32 {
	return crc32.Update(crc32.Update(0, castagnoli, lengthBytes), castagnoli, payload)
}

func EncodeRecord(payload []byte) []byte {
	out := make([]byte, HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	copy(out[HeaderSize:], payload)
	binary.LittleEndian.PutUint32(out[4:8], checksum(out[0:4], payload))
	return out
}

func DecodeRecord(b []byte) ([]byte, int, error) {
	if len(b) < HeaderSize {
		return nil, 0, ErrTruncated
	}
	length := binary.LittleEndian.Uint32(b[0:4])
	if length > MaxRecordSize {
		return nil, 0, fmt.Errorf("%w: length field %d exceeds the maximum", ErrCorrupt, length)
	}
	total := HeaderSize + int(length)
	if len(b) < total {
		return nil, 0, ErrTruncated
	}
	payload := b[HeaderSize:total]
	if want, got := binary.LittleEndian.Uint32(b[4:8]), checksum(b[0:4], payload); want != got {
		return nil, 0, fmt.Errorf("%w: checksum %08x, computed %08x", ErrCorrupt, want, got)
	}
	return append([]byte(nil), payload...), total, nil
}

func ScanRecords(b []byte) ([][]byte, int, error) {
	var records [][]byte
	valid := 0
	for valid < len(b) {
		payload, n, err := DecodeRecord(b[valid:])
		if err != nil {
			return records, valid, err
		}
		records = append(records, payload)
		valid += n
	}
	return records, valid, nil
}
