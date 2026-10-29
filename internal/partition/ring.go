package partition

// Ring is a CONSISTENT-HASH ring: it maps keys to nodes (servers, shards) so that adding or removing a node moves only a
// SMALL fraction of the keys.
//
// WHY: with hash(key) % N, going from 4 servers to 5 changes the answer for about 80% of all keys -- a cache cluster
// loses 80% of its contents, a sharded database must move 80% of its data. Consistent hashing fixes it:
//
//	picture a circle of 2^64 positions. Hash every NODE onto the circle (several times: "virtual nodes").
//	To place a KEY: hash it onto the circle and walk clockwise to the first node point. That node owns the key.
//	Add a node: it takes over only the arcs just before its points -- keys never move BETWEEN old nodes.
//	Remove a node: only ITS keys move, to the next points clockwise.
//
// VIRTUAL NODES: with one point per node the arcs are very uneven (some nodes own 3x others). Giving every node `vnodes`
// points ("node#0", "node#1", ...) averages that out; 100-200 per node gives a spread within a few percent.
//
// Implementation notes:
//   - A point is (hash uint64, node string) with hash = hashx.Sum64(fmt.Sprintf("%s#%d", node, i)) for i in [0, vnodes).
//     Keep ALL points in one slice sorted by (hash, node) -- the tie-break keeps the ring deterministic.
//   - Get(key): h := hashx.Sum64(key); find the first point with hash >= h using sort.Search; if none, WRAP to point 0.
//     An empty ring returns ("", false).
//   - Add(node) is idempotent (adding twice changes nothing). Remove of an unknown node is a no-op.
//   - Nodes() returns the node names, sorted. vnodes < 1 is treated as 1.
type Ring struct{}

func NewRing(vnodes int) *Ring { return &Ring{} } // TODO Ch13

func (r *Ring) Add(node string) {} // TODO Ch13

func (r *Ring) Remove(node string) {} // TODO Ch13

func (r *Ring) Get(key string) (string, bool) { return "", false } // TODO Ch13

func (r *Ring) Nodes() []string { return nil } // TODO Ch13
