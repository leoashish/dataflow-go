package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"dataflow/internal/sketch"
)

func appendEntry(buf []byte, e Entry) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(e.Key)))
	buf = append(buf, e.Key...)
	flag := byte(0)
	if e.Tombstone {
		flag = 1
	}
	buf = append(buf, flag)
	buf = binary.AppendUvarint(buf, uint64(len(e.Value)))
	return append(buf, e.Value...)
}

// parseEntry decodes one entry from b and returns it with the number of bytes used.
func parseEntry(b []byte) (Entry, int, error) {
	pos := 0
	klen, n := binary.Uvarint(b[pos:])
	if n <= 0 || klen > uint64(len(b)) {
		return Entry{}, 0, fmt.Errorf("%w: bad key length", ErrCorruptTable)
	}
	pos += n
	if uint64(len(b)-pos) < klen+1 {
		return Entry{}, 0, fmt.Errorf("%w: truncated key", ErrCorruptTable)
	}
	key := string(b[pos : pos+int(klen)])
	pos += int(klen)
	flag := b[pos]
	pos++
	if flag > 1 {
		return Entry{}, 0, fmt.Errorf("%w: bad flag %d", ErrCorruptTable, flag)
	}
	vlen, n := binary.Uvarint(b[pos:])
	if n <= 0 || vlen > uint64(len(b)) {
		return Entry{}, 0, fmt.Errorf("%w: bad value length", ErrCorruptTable)
	}
	pos += n
	if uint64(len(b)-pos) < vlen {
		return Entry{}, 0, fmt.Errorf("%w: truncated value", ErrCorruptTable)
	}
	value := string(b[pos : pos+int(vlen)])
	pos += int(vlen)
	return Entry{Key: key, Value: value, Tombstone: flag == 1}, pos, nil
}

func WriteSSTable(path string, entries []Entry, bloomFPR float64) error {
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Key >= entries[i].Key {
			return fmt.Errorf("lsm: entries must be strictly ascending by key (%q then %q)", entries[i-1].Key, entries[i].Key)
		}
	}
	buf := append([]byte(SSTableMagic), 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(entries)))
	for _, e := range entries {
		buf = appendEntry(buf, e)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, werr := f.Write(buf)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return werr
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

type indexEntry struct {
	key string
	off int64
}

type SSTable struct {
	path       string
	f          *os.File
	count      int
	dataEnd    int64
	bloom      *sketch.Bloom
	index      []indexEntry
	bloomSkips atomic.Uint64
	blockReads atomic.Uint64
}

func OpenSSTable(path string, bloomFPR float64) (*SSTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 8 || string(data[:4]) != SSTableMagic {
		return nil, fmt.Errorf("%w: bad header in %s", ErrCorruptTable, path)
	}
	count := int(binary.LittleEndian.Uint32(data[4:8]))
	bloom, err := sketch.NewBloom(max(count, 1), bloomFPR)
	if err != nil {
		return nil, err
	}
	t := &SSTable{path: path, count: count, dataEnd: int64(len(data)), bloom: bloom}
	pos := 8
	for i := 0; i < count; i++ {
		e, n, err := parseEntry(data[pos:])
		if err != nil {
			return nil, fmt.Errorf("%w (entry %d of %s)", err, i, path)
		}
		if i%BlockEntries == 0 {
			t.index = append(t.index, indexEntry{key: e.Key, off: int64(pos)})
		}
		bloom.Add(e.Key)
		pos += n
	}
	if pos != len(data) {
		return nil, fmt.Errorf("%w: %d trailing bytes in %s", ErrCorruptTable, len(data)-pos, path)
	}
	if t.f, err = os.Open(path); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *SSTable) Get(key string) (Entry, bool, error) {
	if !t.bloom.Contains(key) {
		t.bloomSkips.Add(1)
		return Entry{}, false, nil
	}
	// the last block whose first key is <= key
	b := sort.Search(len(t.index), func(i int) bool { return t.index[i].key > key }) - 1
	if b < 0 {
		return Entry{}, false, nil
	}
	start, end := t.index[b].off, t.dataEnd
	if b+1 < len(t.index) {
		end = t.index[b+1].off
	}
	block := make([]byte, end-start)
	if _, err := t.f.ReadAt(block, start); err != nil {
		return Entry{}, false, err
	}
	t.blockReads.Add(1)
	for len(block) > 0 {
		e, n, err := parseEntry(block)
		if err != nil {
			return Entry{}, false, err
		}
		if e.Key == key {
			return e, true, nil
		}
		if e.Key > key {
			break
		}
		block = block[n:]
	}
	return Entry{}, false, nil
}

func (t *SSTable) Entries() ([]Entry, error) {
	data := make([]byte, t.dataEnd-8)
	if _, err := t.f.ReadAt(data, 8); err != nil {
		return nil, err
	}
	out := make([]Entry, 0, t.count)
	for len(data) > 0 {
		e, n, err := parseEntry(data)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
		data = data[n:]
	}
	return out, nil
}

func (t *SSTable) Len() int           { return t.count }
func (t *SSTable) Path() string       { return t.path }
func (t *SSTable) BloomSkips() uint64 { return t.bloomSkips.Load() }
func (t *SSTable) BlockReads() uint64 { return t.blockReads.Load() }
func (t *SSTable) Close() error       { return t.f.Close() }
