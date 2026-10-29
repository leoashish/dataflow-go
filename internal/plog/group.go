package plog

import "dataflow/internal/todo"

// Group is a CONSUMER GROUP: several consumers share one Log, and every partition is read by exactly ONE member at a
// time -- that is how you scale reading without two workers processing the same record.
//
//   - Join(consumer) / Leave(consumer) change the membership (Join twice is harmless). Every change REBALANCES.
//   - Assignment(consumer): the partitions this member owns, ascending; nil if it is not a member.
//     Rule: sort the members by name; partition p belongs to members[p % len(members)].
//     (4 partitions, members "a","b" -> a: [0 2], b: [1 3]; if b leaves, a owns [0 1 2 3].)
//   - Poll(consumer, max): up to max records in total from the consumer's partitions (lowest partition first), each
//     partition read FROM ITS COMMITTED OFFSET. Not a member -> ErrUnknownConsumer. Polling does NOT move the committed
//     offset: if you crash before committing, the same records are delivered again (AT-LEAST-ONCE).
//   - Commit(partition, next): the group has processed everything before offset `next` in that partition.
//     Invalid partition -> ErrInvalidPartition. Committed(partition) is that offset (0 if never committed).
//   - Because offsets belong to the GROUP, not to the consumer, a consumer that takes over a partition after a
//     rebalance simply continues from the committed offset: nothing is lost, nothing is skipped.
//   - Safe for concurrent use (sync.Mutex).
type Group struct{}

func NewGroup(l *Log) *Group { return &Group{} }

func (g *Group) Join(consumer string) {}

func (g *Group) Leave(consumer string) {}

func (g *Group) Assignment(consumer string) []int { return nil }

func (g *Group) Poll(consumer string, max int) ([]Record, error) {
	return nil, todo.NotImplemented("Ch08: Group.Poll")
}

func (g *Group) Commit(partition int, next int64) error {
	return todo.NotImplemented("Ch08: Group.Commit")
}

func (g *Group) Committed(partition int) int64 { return 0 }
