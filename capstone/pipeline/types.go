package pipeline

import "dataflow/capstone/model"

// Position says where to resume reading: the next unread line of file number File (files in file-name order). GIVEN.
type Position struct {
	File int
	Line int64 // lines already consumed from that file (blank lines included)
}

// Parsed is one input line after parsing. Exactly one of the three cases holds. GIVEN.
type Parsed struct {
	Event  model.Event
	Reason string // non-empty: the line was rejected for this reason (model.Reason* constants)
	Blank  bool   // an empty line: ignored, not even counted as read
}

// Batch is a run of consecutive raw lines plus the position just AFTER the last of them. GIVEN.
type Batch struct {
	Lines [][]byte
	Next  Position
}
