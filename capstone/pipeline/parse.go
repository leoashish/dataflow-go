package pipeline

import "dataflow/capstone/model"

// ParseLine validates one raw line exactly as SPEC.md ("Validation") says and returns the event, or a non-empty rejection reason
// (a model.Reason* constant). Whitespace-only lines are handled by the caller, not here.
//
// TODO(capstone): chapter 09 (schemas, strict typing: a "12" string is not a number; required vs optional fields).
func ParseLine(line []byte) (ev model.Event, reason string) {
	return model.Event{}, "todo: implement ParseLine"
}
