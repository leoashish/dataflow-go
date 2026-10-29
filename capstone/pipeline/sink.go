package pipeline

import (
	"dataflow/capstone/model"
	"dataflow/internal/todo"
)

// WriteOutputs writes dir/report.json (the Report as indented JSON) and dir/events.col (the accepted events in the columnar format of
// chapter 14, with exactly the columns and order given in SPEC.md). Create dir if needed. Write each file atomically.
//
// TODO(capstone): chapter 14 (columnar encodings) and chapter 07 (atomic writes).
func WriteOutputs(dir string, rep model.Report, accepted []model.Event) error {
	return todo.NotImplemented("capstone: WriteOutputs")
}
