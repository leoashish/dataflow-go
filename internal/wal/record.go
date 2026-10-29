package wal

import "dataflow/internal/todo"

// EncodeRecord frames one payload for the log:
//
//	offset 0   4 bytes  length of the payload, unsigned little-endian (encoding/binary.LittleEndian)
//	offset 4   4 bytes  checksum, little-endian: CRC-32C (the "Castagnoli" polynomial, crc32.MakeTable(crc32.Castagnoli))
//	                    computed over the 4 LENGTH bytes FOLLOWED BY the payload
//	offset 8   payload
//
// WHY A CHECKSUM: a crash can leave half a record on disk, a disk can flip bits, a bug can overwrite a block. Without a
// checksum the reader would happily parse garbage as data. WHY IT COVERS THE LENGTH TOO: a corrupt length makes the reader
// misjudge where the NEXT record starts, so it must be protected as well. (CRC-32 catches every single-bit error and every
// burst up to 32 bits; it is NOT a defence against a deliberate attacker -- use a MAC for that.)
// An empty payload is a valid record (length 0). Hint: crc32.Update(0, table, lengthBytes) then crc32.Update(crc, table, payload).
func EncodeRecord(payload []byte) []byte {
	return nil // TODO Ch19
}

// DecodeRecord parses ONE record from the start of b and returns (payload, bytesConsumed, nil). The payload is a COPY (b may
// be reused by the caller). Errors, checked IN THIS ORDER:
//
//  1. len(b) < HeaderSize                              -> ErrTruncated
//  2. the length field > MaxRecordSize                 -> an error wrapping ErrCorrupt  (before anything else is trusted!)
//  3. len(b) < HeaderSize + length                     -> ErrTruncated
//  4. the checksum does not match                      -> an error wrapping ErrCorrupt
//
// Truncated and corrupt are DIFFERENT: truncation at the very END of a log is the normal result of a crash and is silently
// repaired; corruption means the medium cannot be trusted.
func DecodeRecord(b []byte) (payload []byte, n int, err error) {
	return nil, 0, todo.NotImplemented("Ch19: DecodeRecord")
}

// ScanRecords decodes records back to back from b until the bytes run out or a record fails. It returns the records found,
// the number of bytes that make up those valid records (validBytes), and the error that stopped the scan (nil when every
// byte was consumed cleanly; otherwise ErrTruncated or an ErrCorrupt-wrapping error from DecodeRecord).
// Everything after validBytes is untrustworthy: after a bad record you cannot know where the next one starts.
func ScanRecords(b []byte) (records [][]byte, validBytes int, err error) {
	return nil, 0, todo.NotImplemented("Ch19: ScanRecords")
}
