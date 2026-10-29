// Package streamio processes data bigger than memory: read a little, process it, forget it.
package streamio

import (
	"bytes"
	"io"

	"dataflow/internal/todo"
)

// LineStats describes a text stream.
type LineStats struct {
	Lines       int   // number of lines; a last line WITHOUT a trailing "\n" still counts
	Bytes       int64 // total bytes read, newlines included
	LongestLine int   // length in bytes of the longest line, not counting its "\n"
}

// Stats reads r to the end in a single pass using a constant amount of memory.
//
// Trap: bufio.Scanner refuses lines longer than 64 KiB by default (it returns bufio.ErrTooLong). Real data has such
// lines (a minified JSON blob, a base64 field). Use bufio.Reader (ReadSlice / ReadLine) and handle a line that does
// not fit in the buffer, OR give the Scanner a bigger buffer with scanner.Buffer(...) -- but then think about what
// "any line length" means. The test feeds a 1 MiB line.
// Any read error is returned.
func Stats(r io.Reader) (LineStats, error) {
	return LineStats{}, todo.NotImplemented("Ch03: Stats")
}

// WordFrequencies counts words in a stream of any size. A word is a maximal run of letters and digits
// (unicode.IsLetter / unicode.IsDigit), compared case-insensitively (stored lowercased).
// Memory use must depend on the number of DISTINCT words, never on the size of the input.
func WordFrequencies(r io.Reader) (map[string]int, error) {
	return nil, todo.NotImplemented("Ch03: WordFrequencies")
}

// ReadChunks reads r in chunks of exactly chunkSize bytes (the last chunk may be shorter, but never empty) and calls fn
// for each. It must reuse ONE buffer for all chunks -- allocating a new slice per chunk is exactly the garbage a
// big-data job cannot afford. So fn must not keep the slice after returning.
//
//   - chunkSize < 1 -> a real error.
//   - an error from fn stops the reading and is returned.
//   - a reader that returns data in tiny pieces (io/iotest.OneByteReader) must still produce full chunks: io.ReadFull.
func ReadChunks(r io.Reader, chunkSize int, fn func(chunk []byte) error) error {
	return todo.NotImplemented("Ch03: ReadChunks")
}

// BufferPool recycles *bytes.Buffer values so a hot loop does not allocate one per record (sync.Pool is the tool).
//   - Get returns an EMPTY buffer (Reset it: it may have been used before).
//   - Put gives it back. Buffers whose capacity grew beyond maxKeep are DROPPED, not pooled: one 500 MB outlier must
//     not stay in the pool forever. Put(nil) is a no-op.
type BufferPool struct{}

func NewBufferPool(maxKeep int) *BufferPool { return &BufferPool{} }

func (p *BufferPool) Get() *bytes.Buffer { return new(bytes.Buffer) } // TODO Ch03

func (p *BufferPool) Put(b *bytes.Buffer) {} // TODO Ch03

// Split is the half-open byte range [Start, End) of a file.
type Split struct {
	Start, End int64
}

// LineAlignedSplits cuts the file at path into at most n ranges so that
//
//   - every line of the file lies completely inside exactly ONE range (no line is cut in half),
//   - the ranges are in file order, contiguous (each Start is the previous End), non-empty, and cover the file,
//   - they are roughly the same size.
//
// This is how Hadoop/Spark read one huge file with many workers: each worker gets a range and needs no coordination.
//
// Algorithm: the ideal cut points are i*size/n. A cut in the middle of a line is moved FORWARD to the start of the next
// line (the byte after the next '\n'; a cut that is already at a line start stays). Cuts that land on the same line
// start collapse into one, so you may get fewer than n ranges (3 lines, n = 10 -> at most 3 ranges).
//
// n < 1 -> a real error. An empty file -> no ranges (nil) and no error.
func LineAlignedSplits(path string, n int) ([]Split, error) {
	return nil, todo.NotImplemented("Ch03: LineAlignedSplits")
}

// ProcessSplit calls fn for every line inside s (without its '\n'; a last line without '\n' counts; an empty line is a
// line of length 0). The slice passed to fn is only valid during the call. Stop at the first error from fn.
// Hint: io.NewSectionReader(file, s.Start, s.End-s.Start) gives you a reader limited to the range.
func ProcessSplit(path string, s Split, fn func(line []byte) error) error {
	return todo.NotImplemented("Ch03: ProcessSplit")
}
