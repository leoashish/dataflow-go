// Package pipeline is YOUR capstone: the Clickstream Analytics Pipeline. Read README.md and SPEC.md first.
//
// This package contains STRUCTURE ONLY: types, function signatures and TODOs. You are free to reorganise it (add files, rename
// helpers, change internal types) -- the only contract is Run(ctx, cfg) and the two output files described in SPEC.md.
//
// A suggested architecture (every box is something you built in the course):
//
//	files ─▶ ReadBatches ─▶ parse in parallel ─▶ re-order ─▶ State.Observe (ONE goroutine owns the state) ─▶ Report
//	(ch 03)   line batches     (ch 01, 02, 09)   (ordered!)   dedup · late · windows · sketches (ch 06, 07, 15-18)
//	                                                                  │ every CheckpointEvery lines
//	                                                                  ▼
//	                                                        SaveCheckpoint (ch 07, 19) ◀─ LoadCheckpoint on start
//	accepted events ─▶ WriteOutputs: report.json + events.col (ch 14)
package pipeline
