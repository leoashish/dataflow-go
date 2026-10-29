package columnar

import (
	"io"

	"dataflow/internal/todo"
)

// WriteColumns writes a small columnar file. Layout:
//
//	"COL1"                       4 magic bytes
//	u(numColumns)
//	per column:
//	  u(len(name)) name-bytes
//	  kind byte, encoding byte
//	  u(rowCount)
//	  u(payloadLen) payload
//
// Payload by encoding: EncPlain/EncDelta/EncRLE = the output of the matching Encode* function (ChooseIntEncoding picks
// the smallest for each INT column). EncDict (every STRING column) =
//
//	u(dictSize)  { u(len) bytes }*dictSize   width byte (= BitsNeeded(dictSize-1), 0 if the dictionary is empty)   BitPack(codes, width)
//
// Every column must have the same number of rows, else an error. A column's Kind decides whether Ints or Strs is used.
// A 3-column table of timestamps, statuses and user ids that takes 4 MB as JSON lines shrinks by an order of magnitude --
// and an engine reading only the "status" column never touches the other two files' bytes. THAT is why analytics stores
// data by column.
func WriteColumns(w io.Writer, cols []Column) error {
	return todo.NotImplemented("Ch14: WriteColumns")
}

// ReadColumns parses what WriteColumns produced (reading the whole stream is fine) and returns the columns with Encoding
// filled in. Bad magic, truncation anywhere, an unknown kind/encoding, lengths that run past the data, a payload that does
// not decode to exactly rowCount values: all errors wrapping ErrCorrupt. NEVER panic and never trust a length before checking
// it against the bytes you actually have.
func ReadColumns(r io.Reader) ([]Column, error) {
	return nil, todo.NotImplemented("Ch14: ReadColumns")
}
