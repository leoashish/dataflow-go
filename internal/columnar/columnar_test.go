package columnar_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"

	"dataflow/internal/columnar"
	"dataflow/internal/testutil"
)

// Chapter 14 -- encodings and a columnar file.   Run:  go test ./internal/columnar -run Ch14 -race -v

func TestCh14_Uvarint_RoundTripsAndMatchesTheStandardLibrary(t *testing.T) {
	cases := []struct {
		v    uint64
		size int
	}{{0, 1}, {1, 1}, {127, 1}, {128, 2}, {300, 2}, {16383, 2}, {16384, 3}, {math.MaxUint32, 5}, {math.MaxUint64, 10}}
	for _, c := range cases {
		enc := columnar.AppendUvarint(nil, c.v)
		if len(enc) != c.size {
			t.Fatalf("AppendUvarint(%d) is %d bytes, want %d", c.v, len(enc), c.size)
		}
		std := binary.AppendUvarint(nil, c.v)
		if !bytes.Equal(enc, std) {
			t.Fatalf("AppendUvarint(%d) = % x, the standard library gives % x", c.v, enc, std)
		}
		got, n := columnar.Uvarint(append(enc, 0xAA, 0xBB)) // trailing bytes must be ignored
		if got != c.v || n != c.size {
			t.Fatalf("Uvarint(% x) = (%d, %d), want (%d, %d)", enc, got, n, c.v, c.size)
		}
	}
}

func TestCh14_Uvarint_ReportsTruncationAndOverflow(t *testing.T) {
	if v, n := columnar.Uvarint(nil); v != 0 || n != 0 {
		t.Fatalf("empty input: (%d, %d), want (0, 0)", v, n)
	}
	if _, n := columnar.Uvarint([]byte{0x80, 0x80}); n != 0 { // "more bytes follow" but there are none
		t.Fatalf("truncated varint: n = %d, want 0", n)
	}
	eleven := bytes.Repeat([]byte{0xFF}, 11)
	if _, n := columnar.Uvarint(eleven); n >= 0 {
		t.Fatalf("an 11-byte varint: n = %d, want a negative value (overflow)", n)
	}
	tenthTooBig := append(bytes.Repeat([]byte{0xFF}, 9), 0x02)
	if _, n := columnar.Uvarint(tenthTooBig); n >= 0 {
		t.Fatalf("a 10th byte above 1 does not fit in 64 bits: n = %d, want negative", n)
	}
}

func TestCh14_AppendUvarint_AppendsToWhatIsThere(t *testing.T) {
	got := columnar.AppendUvarint([]byte{9, 9}, 300)
	if !bytes.Equal(got, []byte{9, 9, 0xAC, 0x02}) {
		t.Fatalf("got % x, want 09 09 ac 02", got)
	}
}

func TestCh14_ZigZag_KeepsSmallMagnitudesSmall_AndRoundTrips(t *testing.T) {
	for in, want := range map[int64]uint64{0: 0, -1: 1, 1: 2, -2: 3, 2: 4, -3: 5, math.MaxInt64: math.MaxUint64 - 1, math.MinInt64: math.MaxUint64} {
		if got := columnar.ZigZag(in); got != want {
			t.Fatalf("ZigZag(%d) = %d, want %d", in, got, want)
		}
		if back := columnar.UnZigZag(want); back != in {
			t.Fatalf("UnZigZag(%d) = %d, want %d", want, back, in)
		}
	}
}

type codec struct {
	name   string
	encode func([]int64) []byte
	decode func([]byte) ([]int64, error)
}

var intCodecs = []codec{
	{"plain", columnar.EncodePlain, columnar.DecodePlain},
	{"delta", columnar.EncodeDelta, columnar.DecodeDelta},
	{"rle", columnar.EncodeRLE, columnar.DecodeRLE},
}

func datasets() map[string][]int64 {
	rnd := testutil.Rand(14)
	random := make([]int64, 500)
	for i := range random {
		random[i] = rnd.Int63n(1<<40) - 1<<39
	}
	sorted := make([]int64, 500)
	for i := range sorted {
		sorted[i] = 1_700_000_000_000 + int64(i)*1000 + int64(rnd.Intn(10))
	}
	runs := make([]int64, 0, 500)
	for v := int64(0); len(runs) < 500; v++ {
		for k := 0; k < 25; k++ {
			runs = append(runs, v)
		}
	}
	return map[string][]int64{
		"empty":    {},
		"single":   {42},
		"random":   random,
		"sorted":   sorted,
		"runs":     runs,
		"negative": {-5, -5, 3, -100, 0, -1},
		"extremes": {math.MinInt64, math.MaxInt64, 0, math.MinInt64, math.MaxInt64}, // deltas overflow int64 and must wrap back
	}
}

func TestCh14_IntegerCodecs_RoundTripEveryKindOfData(t *testing.T) {
	for _, c := range intCodecs {
		for name, vals := range datasets() {
			got, err := c.decode(c.encode(vals))
			if err != nil {
				t.Fatalf("%s/%s: decode: %v", c.name, name, err)
			}
			if !slices.Equal(got, vals) {
				t.Fatalf("%s/%s: round trip changed the data (len %d -> %d)", c.name, name, len(vals), len(got))
			}
		}
	}
}

func TestCh14_EachCodecWinsOnTheDataItWasDesignedFor(t *testing.T) {
	d := datasets()
	plainSorted, deltaSorted := len(columnar.EncodePlain(d["sorted"])), len(columnar.EncodeDelta(d["sorted"]))
	if deltaSorted >= plainSorted/2 {
		t.Fatalf("delta takes %d bytes for sorted timestamps, plain %d: delta should be less than half", deltaSorted, plainSorted)
	}
	plainRuns, rleRuns := len(columnar.EncodePlain(d["runs"])), len(columnar.EncodeRLE(d["runs"]))
	if rleRuns >= plainRuns/4 {
		t.Fatalf("RLE takes %d bytes for long runs, plain %d: RLE should be under a quarter", rleRuns, plainRuns)
	}
}

func TestCh14_Decoders_RejectEveryTruncation_WithoutPanicking(t *testing.T) {
	for _, c := range intCodecs {
		enc := c.encode([]int64{300, -70000, 5, 5, 5, 1 << 40})
		if _, err := c.decode(enc); err != nil {
			t.Fatalf("%s: the full encoding must decode: %v", c.name, err)
		}
		for cut := 0; cut < len(enc); cut++ {
			_, err := c.decode(enc[:cut])
			testutil.ExpectError(t, err)
			if !errors.Is(err, columnar.ErrCorrupt) {
				t.Fatalf("%s: cut at %d: error %v does not wrap ErrCorrupt", c.name, cut, err)
			}
		}
	}
}

// allocatedBytes reports how much memory f allocated (TotalAlloc is cumulative: a garbage collection cannot hide it).
func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestCh14_Decoders_RefuseToAllocateWhatAMaliciousHeaderAsksFor(t *testing.T) {
	const budget = 1 << 20 // a decoder facing a few bytes of input has no business allocating a megabyte

	// RLE: ONE run of MaxValues+1 copies of the value 0 -- over the cap, so it must be rejected before anything is built.
	evilRLE := columnar.AppendUvarint(nil, 1)
	evilRLE = columnar.AppendUvarint(evilRLE, columnar.ZigZag(0))
	evilRLE = columnar.AppendUvarint(evilRLE, columnar.MaxValues+1)
	var err error
	if n := allocatedBytes(func() { _, err = columnar.DecodeRLE(evilRLE) }); n > budget {
		t.Fatalf("DecodeRLE allocated %d bytes for a %d-byte input that claims %d values: check the run length against MaxValues BEFORE expanding it", n, len(evilRLE), columnar.MaxValues+1)
	}
	testutil.ExpectError(t, err)

	// Plain/Delta: a count of MaxValues (within the cap!) followed by two bytes of data. Every value needs at least one byte, so
	// the count cannot possibly be true -- and the decoder must find that out WITHOUT first allocating room for it.
	evilCount := columnar.AppendUvarint(nil, columnar.MaxValues)
	evilCount = append(evilCount, 1, 2)
	for _, c := range intCodecs[:2] {
		if n := allocatedBytes(func() { _, err = c.decode(evilCount) }); n > budget {
			t.Fatalf("%s decoder allocated %d bytes for a 4-byte input: never size an allocation from an unverified header (count > len(input) is corrupt)", c.name, n)
		}
		testutil.ExpectError(t, err)
	}

	// RLE: a zero-length run is meaningless
	zeroRun := columnar.AppendUvarint(columnar.AppendUvarint(columnar.AppendUvarint(nil, 1), columnar.ZigZag(7)), 0)
	_, err = columnar.DecodeRLE(zeroRun)
	testutil.ExpectError(t, err)
}

func TestCh14_Dict_StoresEachDistinctStringOnce_InOrderOfFirstAppearance(t *testing.T) {
	vals := []string{"error", "ok", "ok", "ok", "error", "warn", "ok"}
	dict, codes := columnar.EncodeDict(vals)
	if !slices.Equal(dict, []string{"error", "ok", "warn"}) || !slices.Equal(codes, []uint32{0, 1, 1, 1, 0, 2, 1}) {
		t.Fatalf("dict=%v codes=%v", dict, codes)
	}
	back, err := columnar.DecodeDict(dict, codes)
	if err != nil || !slices.Equal(back, vals) {
		t.Fatalf("DecodeDict = (%v, %v), want the original values", back, err)
	}
	if _, err := columnar.DecodeDict(dict, []uint32{0, 3}); !errors.Is(err, columnar.ErrCorrupt) {
		t.Fatalf("a code outside the dictionary: error = %v, want ErrCorrupt", err)
	}
}

func TestCh14_BitsNeeded(t *testing.T) {
	for in, want := range map[uint32]int{0: 0, 1: 1, 2: 2, 3: 2, 4: 3, 7: 3, 8: 4, 255: 8, 256: 9, math.MaxUint32: 32} {
		if got := columnar.BitsNeeded(in); got != want {
			t.Fatalf("BitsNeeded(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestCh14_BitPack_HasTheDocumentedLayout(t *testing.T) {
	got, err := columnar.BitPack([]uint32{5, 2, 7}, 3)
	if err != nil || !bytes.Equal(got, []byte{0xD5, 0x01}) { // bits (LSB first): 101 010 111 -> 0xD5, 0x01
		t.Fatalf("BitPack = (% x, %v), want d5 01", got, err)
	}
}

func TestCh14_BitPack_RoundTripsEveryWidth_AndUsesExactlyTheBitsNeeded(t *testing.T) {
	rnd := testutil.Rand(3)
	for width := 0; width <= 32; width++ {
		n := 1 + rnd.Intn(300)
		vals := make([]uint32, n)
		for i := range vals {
			if width > 0 {
				vals[i] = uint32(rnd.Uint64() & (1<<uint(width) - 1))
			}
		}
		packed, err := columnar.BitPack(vals, width)
		if err != nil {
			t.Fatalf("width %d: BitPack: %v", width, err)
		}
		if want := (n*width + 7) / 8; len(packed) != want {
			t.Fatalf("width %d, %d values: %d bytes, want %d", width, n, len(packed), want)
		}
		back, err := columnar.BitUnpack(packed, width, n)
		if err != nil || !slices.Equal(back, vals) {
			t.Fatalf("width %d: round trip failed (err=%v)", width, err)
		}
	}
}

func TestCh14_BitPack_RejectsMisfits_AndBitUnpackRejectsShortOrBogusInput(t *testing.T) {
	_, err := columnar.BitPack([]uint32{8}, 3)
	testutil.ExpectError(t, err) // 8 needs 4 bits
	_, err = columnar.BitPack([]uint32{1}, 0)
	testutil.ExpectError(t, err) // width 0 can only hold zeros
	_, err = columnar.BitPack([]uint32{1}, 33)
	testutil.ExpectError(t, err)
	_, err = columnar.BitPack([]uint32{1}, -1)
	testutil.ExpectError(t, err)

	zeros, err := columnar.BitUnpack(nil, 0, 5)
	if err != nil || !slices.Equal(zeros, make([]uint32, 5)) {
		t.Fatalf("width 0: BitUnpack = (%v, %v), want five zeros from no bytes", zeros, err)
	}
	_, err = columnar.BitUnpack([]byte{0xFF}, 3, 5) // 5 values x 3 bits = 15 bits = 2 bytes needed
	if !errors.Is(err, columnar.ErrCorrupt) {
		t.Fatalf("short input: error = %v, want ErrCorrupt", err)
	}
	_, err = columnar.BitUnpack([]byte{0}, 3, -1)
	testutil.ExpectError(t, err)
	_, err = columnar.BitUnpack([]byte{0}, 40, 1)
	testutil.ExpectError(t, err)
}

func TestCh14_ChooseIntEncoding_PicksTheSmallestOutput(t *testing.T) {
	d := datasets()
	cases := map[string]columnar.Encoding{"sorted": columnar.EncDelta, "runs": columnar.EncRLE, "random": columnar.EncPlain, "empty": columnar.EncPlain, "single": columnar.EncPlain}
	for name, want := range cases {
		if got := columnar.ChooseIntEncoding(d[name]); got != want {
			t.Errorf("ChooseIntEncoding(%s) = %d, want %d", name, got, want)
		}
	}
	if got := columnar.ChooseIntEncoding(make([]int64, 1000)); got != columnar.EncRLE { // all zeros: one run
		t.Errorf("1000 zeros: %d, want RLE (%d)", got, columnar.EncRLE)
	}
}

func sampleTable() []columnar.Column {
	d := datasets()
	return []columnar.Column{
		{Name: "ts", Kind: columnar.KindInt64, Ints: d["sorted"]},
		{Name: "level", Kind: columnar.KindInt64, Ints: d["runs"]},
		{Name: "amount", Kind: columnar.KindInt64, Ints: d["random"]},
		{Name: "city", Kind: columnar.KindString, Strs: func() []string {
			cities := []string{"Bengaluru", "Zürich", "東京", "", "São Paulo"}
			out := make([]string, 500)
			for i := range out {
				out[i] = cities[(i*7)%len(cities)]
			}
			return out
		}()},
	}
}

func TestCh14_File_RoundTripsATable_AndRecordsTheEncodingItChose(t *testing.T) {
	cols := sampleTable()
	var buf bytes.Buffer
	if err := columnar.WriteColumns(&buf, cols); err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	if !bytes.HasPrefix(buf.Bytes(), []byte("COL1")) {
		t.Fatalf("the file must start with the magic bytes COL1, starts with %q", buf.Bytes()[:min(4, buf.Len())])
	}
	got, err := columnar.ReadColumns(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadColumns: %v", err)
	}
	if len(got) != len(cols) {
		t.Fatalf("read %d columns, wrote %d", len(got), len(cols))
	}
	wantEnc := []columnar.Encoding{columnar.EncDelta, columnar.EncRLE, columnar.EncPlain, columnar.EncDict}
	for i, want := range cols {
		g := got[i]
		if g.Name != want.Name || g.Kind != want.Kind || !slices.Equal(g.Ints, want.Ints) || !slices.Equal(g.Strs, want.Strs) {
			t.Fatalf("column %q changed in the round trip", want.Name)
		}
		if g.Encoding != wantEnc[i] {
			t.Fatalf("column %q was stored with encoding %d, want %d", want.Name, g.Encoding, wantEnc[i])
		}
	}
}

func TestCh14_File_HandlesNoColumnsAndEmptyColumns(t *testing.T) {
	for name, cols := range map[string][]columnar.Column{
		"no columns":    {},
		"empty columns": {{Name: "a", Kind: columnar.KindInt64}, {Name: "b", Kind: columnar.KindString}},
	} {
		var buf bytes.Buffer
		if err := columnar.WriteColumns(&buf, cols); err != nil {
			t.Fatalf("%s: WriteColumns: %v", name, err)
		}
		got, err := columnar.ReadColumns(&buf)
		if err != nil || len(got) != len(cols) {
			t.Fatalf("%s: ReadColumns = (%d columns, %v)", name, len(got), err)
		}
	}
}

func TestCh14_File_RejectsInconsistentTables(t *testing.T) {
	var buf bytes.Buffer
	err := columnar.WriteColumns(&buf, []columnar.Column{
		{Name: "a", Kind: columnar.KindInt64, Ints: []int64{1, 2, 3}},
		{Name: "b", Kind: columnar.KindString, Strs: []string{"x"}},
	})
	testutil.ExpectError(t, err)
	err = columnar.WriteColumns(&buf, []columnar.Column{{Name: "weird", Kind: 99}})
	testutil.ExpectError(t, err)
}

func TestCh14_File_ReaderRejectsBadMagic_AndEveryTruncation(t *testing.T) {
	var buf bytes.Buffer
	small := []columnar.Column{
		{Name: "n", Kind: columnar.KindInt64, Ints: []int64{5, 5, 5, 9}},
		{Name: "s", Kind: columnar.KindString, Strs: []string{"a", "b", "a", "a"}},
	}
	if err := columnar.WriteColumns(&buf, small); err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	file := buf.Bytes()
	if _, err := columnar.ReadColumns(bytes.NewReader(file)); err != nil {
		t.Fatalf("the intact file must read back: %v", err)
	}

	bad := slices.Clone(file)
	bad[0] = 'X'
	if _, err := columnar.ReadColumns(bytes.NewReader(bad)); !errors.Is(err, columnar.ErrCorrupt) {
		t.Fatalf("bad magic: error = %v, want ErrCorrupt", err)
	}
	for cut := 0; cut < len(file); cut++ {
		_, err := columnar.ReadColumns(bytes.NewReader(file[:cut]))
		testutil.ExpectError(t, err)
		if !errors.Is(err, columnar.ErrCorrupt) {
			t.Fatalf("file cut at byte %d of %d: error %v does not wrap ErrCorrupt", cut, len(file), err)
		}
	}
}

func TestCh14_File_ReaderNeverPanicsOnCorruptBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := columnar.WriteColumns(&buf, sampleTable()); err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	original := buf.Bytes()
	if cols, err := columnar.ReadColumns(bytes.NewReader(original)); err != nil || len(cols) != 4 { // the reader works on valid input...
		t.Fatalf("valid file: (%d columns, %v)", len(cols), err)
	}

	rnd := testutil.Rand(99) // ...and survives thousands of damaged copies
	for i := 0; i < 3000; i++ {
		mutated := slices.Clone(original)
		for k := 0; k < 1+rnd.Intn(4); k++ {
			mutated[rnd.Intn(len(mutated))] = byte(rnd.Intn(256))
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ReadColumns panicked on corrupted input (iteration %d): %v", i, r)
				}
			}()
			_, _ = columnar.ReadColumns(bytes.NewReader(mutated)) // an error or a different table are both acceptable; a panic or a giant allocation is not
		}()
	}
}

func TestCh14_File_IsMuchSmallerThanJSONLines(t *testing.T) {
	rnd := testutil.Rand(5)
	const rows = 50_000
	ts := make([]int64, rows)
	users := make([]int64, rows)
	status := make([]string, rows)
	states := []string{"queued", "running", "done", "failed"}
	var ndjson strings.Builder
	cur := states[0]
	for i := 0; i < rows; i++ {
		ts[i] = 1_767_225_600_000 + int64(i)*1000 + int64(rnd.Intn(10))
		users[i] = int64(1 + rnd.Intn(1000))
		if rnd.Intn(50) == 0 {
			cur = states[rnd.Intn(len(states))]
		}
		status[i] = cur
		fmt.Fprintf(&ndjson, `{"ts":%d,"status":"%s","user":%d}`+"\n", ts[i], status[i], users[i])
	}

	var buf bytes.Buffer
	err := columnar.WriteColumns(&buf, []columnar.Column{
		{Name: "ts", Kind: columnar.KindInt64, Ints: ts},
		{Name: "status", Kind: columnar.KindString, Strs: status},
		{Name: "user", Kind: columnar.KindInt64, Ints: users},
	})
	if err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	ratio := float64(buf.Len()) / float64(ndjson.Len())
	t.Logf("columnar %d bytes vs NDJSON %d bytes: %.1f%%", buf.Len(), ndjson.Len(), ratio*100)
	if ratio > 0.12 {
		t.Fatalf("the columnar file is %.1f%% of the JSON-lines size; with delta timestamps, dictionary+bit-packed statuses and small plain ids it should be under 12%%", ratio*100)
	}
}

func TestCh14_File_ReaderRejectsAHeaderThatLiesAboutTheRowCount(t *testing.T) {
	var buf bytes.Buffer
	if err := columnar.WriteColumns(&buf, []columnar.Column{{Name: "n", Kind: columnar.KindInt64, Ints: []int64{5, 9, 2, 7}}}); err != nil {
		t.Fatalf("WriteColumns: %v", err)
	}
	file := buf.Bytes()
	// layout: "COL1" | ncols | len(name) | name | kind | encoding | rowCount | payloadLen | payload   -> rowCount is byte 9 here
	const rowCountOffset = 4 + 1 + 1 + 1 + 1 + 1
	if file[rowCountOffset] != 4 {
		t.Fatalf("byte %d is %d, expected the row count 4: is the file layout as documented?", rowCountOffset, file[rowCountOffset])
	}
	for _, lie := range []byte{0, 3, 5, 100} {
		bad := slices.Clone(file)
		bad[rowCountOffset] = lie
		if _, err := columnar.ReadColumns(bytes.NewReader(bad)); !errors.Is(err, columnar.ErrCorrupt) {
			t.Fatalf("header says %d rows but the payload holds 4: error = %v, want ErrCorrupt", lie, err)
		}
	}
}
