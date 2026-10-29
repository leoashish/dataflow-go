package streamio

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"unicode"
)

type LineStats struct {
	Lines       int
	Bytes       int64
	LongestLine int
}

func Stats(r io.Reader) (LineStats, error) {
	var st LineStats
	br := bufio.NewReaderSize(r, 64*1024)
	cur := 0 // length of the line being read; a line may arrive in several pieces if it is longer than the buffer
	for {
		chunk, err := br.ReadSlice('\n')
		st.Bytes += int64(len(chunk))
		switch {
		case err == nil: // the chunk ends with '\n'
			cur += len(chunk) - 1
			st.Lines++
			st.LongestLine = max(st.LongestLine, cur)
			cur = 0
		case errors.Is(err, bufio.ErrBufferFull): // a piece of a very long line
			cur += len(chunk)
		case errors.Is(err, io.EOF):
			cur += len(chunk)
			if cur > 0 { // a final line without a trailing newline
				st.Lines++
				st.LongestLine = max(st.LongestLine, cur)
			}
			return st, nil
		default:
			return st, err
		}
	}
}

func WordFrequencies(r io.Reader) (map[string]int, error) {
	counts := map[string]int{}
	br := bufio.NewReader(r)
	var word []rune
	flush := func() {
		if len(word) > 0 {
			counts[string(word)]++
			word = word[:0]
		}
	}
	for {
		ch, _, err := br.ReadRune()
		if err != nil {
			flush()
			if errors.Is(err, io.EOF) {
				return counts, nil
			}
			return counts, err
		}
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) {
			word = append(word, unicode.ToLower(ch))
		} else {
			flush()
		}
	}
}

func ReadChunks(r io.Reader, chunkSize int, fn func(chunk []byte) error) error {
	if chunkSize < 1 {
		return fmt.Errorf("chunkSize must be at least 1, got %d", chunkSize)
	}
	buf := make([]byte, chunkSize) // the ONE buffer
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if ferr := fn(buf[:n]); ferr != nil {
				return ferr
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type BufferPool struct {
	pool    sync.Pool
	maxKeep int
}

func NewBufferPool(maxKeep int) *BufferPool {
	return &BufferPool{maxKeep: maxKeep, pool: sync.Pool{New: func() any { return new(bytes.Buffer) }}}
}

func (p *BufferPool) Get() *bytes.Buffer {
	b := p.pool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func (p *BufferPool) Put(b *bytes.Buffer) {
	if b == nil || b.Cap() > p.maxKeep {
		return
	}
	p.pool.Put(b)
}

type Split struct {
	Start, End int64
}

func LineAlignedSplits(path string, n int) ([]Split, error) {
	if n < 1 {
		return nil, fmt.Errorf("n must be at least 1, got %d", n)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}

	starts := []int64{0}
	for i := 1; i < n; i++ {
		ideal := size * int64(i) / int64(n)
		if ideal == 0 {
			continue
		}
		start, err := nextLineStart(f, ideal, size)
		if err != nil {
			return nil, err
		}
		if start > starts[len(starts)-1] && start < size { // collapse cuts that land on the same line; never create an empty range
			starts = append(starts, start)
		}
	}

	splits := make([]Split, len(starts))
	for i, s := range starts {
		end := size
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		splits[i] = Split{Start: s, End: end}
	}
	return splits, nil
}

// nextLineStart returns the first line start at or after pos: pos itself if the byte before it is '\n'.
func nextLineStart(f *os.File, pos, size int64) (int64, error) {
	br := bufio.NewReader(io.NewSectionReader(f, pos-1, size-(pos-1)))
	skipped, err := br.ReadSlice('\n')
	for errors.Is(err, bufio.ErrBufferFull) { // a line longer than the buffer: keep skipping
		pos += int64(len(skipped))
		skipped, err = br.ReadSlice('\n')
	}
	if errors.Is(err, io.EOF) {
		return size, nil // no newline after the cut: the rest of the file is one (last) line
	}
	if err != nil {
		return 0, err
	}
	return pos - 1 + int64(len(skipped)), nil
}

func ProcessSplit(path string, s Split, fn func(line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(io.NewSectionReader(f, s.Start, s.End-s.Start), 64*1024)
	var long []byte // only used for a line that does not fit the buffer
	for {
		piece, err := br.ReadSlice('\n')
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			long = append(long, piece...)
			continue
		case err == nil:
			line := piece[:len(piece)-1]
			if long != nil {
				long = append(long, line...)
				line, long = long, nil
			}
			if ferr := fn(line); ferr != nil {
				return ferr
			}
		case errors.Is(err, io.EOF):
			if long != nil || len(piece) > 0 { // a last line without a trailing newline
				line := piece
				if long != nil {
					long = append(long, piece...)
					line = long
				}
				if ferr := fn(line); ferr != nil {
					return ferr
				}
			}
			return nil
		default:
			return err
		}
	}
}
