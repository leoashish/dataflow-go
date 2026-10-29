package checkpoint

import (
	"context"

	"dataflow/internal/todo"
)

// Run copies records from src to sink, remembering its progress in store under o.Name, so that after a crash the next
// call RESUMES instead of starting over.
//
//  1. Load the checkpoint: the next offset to read (decimal text is fine). Not found -> start at 0.
//  2. Loop: Read a batch from that offset. An empty batch means the source is exhausted: save a final checkpoint (if
//     anything was written since the last one) and return.
//  3. For each record: sink.Write. If Write fails, return the error AT ONCE -- and do not save a checkpoint that covers
//     the failed record.
//  4. After every o.CheckpointEvery successful writes, save the checkpoint = (offset of the last written record) + 1.
//
// Returns how many records THIS call wrote. Check ctx before each Read; a cancelled ctx returns its error.
//
// THE ORDER IS EVERYTHING. "Write the sink, THEN save the checkpoint" can repeat work after a crash (records written
// after the last checkpoint are written again) but can never LOSE any: this is AT-LEAST-ONCE delivery. Reversing the
// order (checkpoint first) risks AT-MOST-ONCE: a crash between the two skips records forever.
// At-least-once + an IDEMPOTENT sink (writing the same record twice leaves the same state, e.g. upsert by key) gives
// "effectively once" -- the tests demonstrate both halves.
func Run(ctx context.Context, src Source, sink Sink, store *Store, o Options) (int, error) {
	return 0, todo.NotImplemented("Ch07: Run")
}
