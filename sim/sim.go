// Package sim runs a Raft cluster, its clients and a hostile network inside
// one deterministic event loop.
//
// There is no real time and no goroutine. A virtual clock jumps from event
// to event: a node's tick, a message arriving, a client timing out, a fault.
// Every random choice (delays, drops, which node crashes, election timeouts)
// comes from one seeded generator, so a seed is a complete description of a
// run and any failure replays exactly.
//
// A run has two phases. During the chaos phase the network drops, duplicates
// and delays messages, partitions come and go, nodes crash and restart, and
// each node's clock runs at its own rate. Then everything heals and the
// cluster must finish every client's work: that is the liveness check.
// Safety is checked after every event in both phases.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/ahmed-hashim-pro/quorum-lab/kv"
	"github.com/ahmed-hashim-pro/quorum-lab/linz"
	"github.com/ahmed-hashim-pro/quorum-lab/raft"
)

type Config struct {
	Seed         uint64
	Nodes        int
	Clients      int
	OpsPerClient int
	Keys         int

	// Virtual milliseconds of chaos, then at most HealMs to finish the work.
	ChaosMs, HealMs int64
	// Partitions and crashes during the chaos phase, one every
	// FaultGapMs to 2*FaultGapMs.
	Faults     bool
	FaultGapMs int64
	// Per message, during chaos.
	DropRate, DupRate float64
	// Each node's tick period is TickMs scaled by a factor drawn from
	// [1-Skew, 1+Skew], redrawn on every restart.
	TickMs int64
	Skew   float64

	ElectionTicks, HeartbeatTicks, MaxEntries int
	// Message delay is 1..MaxDelayMs, with an occasional much slower one.
	MaxDelayMs int64

	Bugs    raft.Bugs
	NoDedup bool

	// Record the timeline for a picture.
	Record bool
}

// Default is the configuration for a seed. Beyond the fixed shape (5 nodes,
// 4 clients, 10 s of chaos), each seed draws its own fault profile: how
// lossy the network is, how often faults strike, how skewed the clocks are,
// how many entries fit in one message. This is swarm testing: a run with
// fewer kinds of trouble, but more of one, reaches corners a uniform mix of
// everything rarely does.
func Default(seed uint64) Config {
	r := rand.New(rand.NewPCG(seed, 0x5a4d))
	pick := func(xs ...float64) float64 { return xs[r.IntN(len(xs))] }
	// Short timeouts against slow links make elections collide.
	election := int(pick(5, 8, 15, 30))
	return Config{
		Seed: seed, Nodes: 5, Clients: 4, OpsPerClient: 60, Keys: 3,
		ChaosMs: 10_000, HealMs: 10_000,
		Faults:        true,
		FaultGapMs:    int64(pick(150, 400, 1000)),
		DropRate:      pick(0, 0.02, 0.1, 0.25),
		DupRate:       pick(0, 0.02, 0.1),
		TickMs:        10,
		Skew:          pick(0, 0.2, 0.5),
		ElectionTicks: election, HeartbeatTicks: max(1, election/5),
		MaxEntries: int(pick(1, 2, 4, 16)),
		MaxDelayMs: int64(pick(12, 40, 100)),
	}
}

type Violation struct {
	At     int64
	Kind   string
	Detail string
}

func (v *Violation) Error() string {
	return fmt.Sprintf("%s at %d ms: %s", v.Kind, v.At, v.Detail)
}

// Violation kinds. The first five are Raft's safety properties (Figure 3 of
// the paper) plus persistence; the last two are about what clients see.
const (
	ElectionSafety     = "election safety"
	LogMatching        = "log matching"
	LeaderCompleteness = "leader completeness"
	StateMachineSafety = "state machine safety"
	Monotonicity       = "monotonicity"
	Linearizability    = "linearizability"
	Liveness           = "liveness"
	Panic              = "panic"
)

type Stats struct {
	Events, Messages, Dropped, Duplicated int
	DoubleVotes                           int
	Partitions, Crashes                   int
	Leaders                               int // distinct (term, leader) pairs
	MaxTerm                               uint64
	Committed                             int // entries committed, cluster-wide
	OpsDone, OpsPending                   int
	EndMs                                 int64
}

type Result struct {
	Seed      uint64
	Violation *Violation
	Stats     Stats
	History   []linz.Operation
	// Notable events (elections, faults), oldest first; the whole run when
	// recording, else the last few.
	Trace    []string
	Timeline *Timeline
}

type evKind int

const (
	evTick evKind = iota
	evDeliver
	evRequest
	evReply
	evClientTimeout
	evClientNext
	evFault
	evRestart
	evHeal
)

type request struct {
	client, attempt int
	op              kv.Op
}

type reply struct {
	client, attempt int
	ok              bool
	output          string
	hint            raft.ID
}

type event struct {
	at              int64
	seq             uint64
	kind            evKind
	node            raft.ID
	inc             int
	msg             raft.Message
	req             request
	rep             reply
	client, attempt int
}

type queue []*event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

type pending struct {
	client, seq, attempt int
}

type node struct {
	id         raft.ID
	r          *raft.Node
	up         bool
	inc        int
	disk       raft.State
	store      *kv.Store
	period     int64
	pending    map[uint64]pending
	lastCommit uint64
	role       raft.Role
	term       uint64
}

type client struct {
	id        int
	seq       int
	op        kv.Op
	attempt   int
	hint      raft.ID
	done      int
	waiting   bool
	historyAt int
}

type world struct {
	cfg     Config
	rng     *rand.Rand
	now     int64
	seq     uint64
	q       queue
	nodes   []*node // index 0 unused
	clients []*client
	cut     [][]bool // cut[a][b]: a and b cannot exchange messages
	chaos   bool

	// What the checker knows.
	committed []raft.Entry // the agreed log prefix, as applied anywhere
	// commitTerm[i]: the term of the leader that first committed entry i+1.
	commitTerm []uint64
	leaders    map[uint64]raft.ID // term -> leader
	votes      map[[2]uint64]raft.ID
	history    []linz.Operation

	stats     Stats
	violation *Violation
	trace     []string
	timeline  *Timeline
}

func (w *world) schedule(after int64, e *event) {
	w.seq++
	e.at, e.seq = w.now+after, w.seq
	heap.Push(&w.q, e)
}

func (w *world) note(format string, args ...any) {
	line := fmt.Sprintf("%6d ms  ", w.now) + fmt.Sprintf(format, args...)
	w.trace = append(w.trace, line)
	if !w.cfg.Record && len(w.trace) > 60 {
		w.trace = w.trace[len(w.trace)-40:]
	}
}

func (w *world) fail(kind, format string, args ...any) {
	if w.violation == nil {
		w.violation = &Violation{At: w.now, Kind: kind, Detail: fmt.Sprintf(format, args...)}
		w.note("VIOLATION %s: %s", kind, w.violation.Detail)
	}
}

func (w *world) ids() []raft.ID {
	out := make([]raft.ID, w.cfg.Nodes)
	for i := range out {
		out[i] = raft.ID(i + 1)
	}
	return out
}

// Run executes one simulation. It is a pure function of cfg. A panic inside
// a node is reported as a violation, not a crash of the simulator: a node
// that falls over on its peers' messages is a bug like any other.
func Run(cfg Config) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res = Result{Seed: cfg.Seed, Violation: &Violation{Kind: Panic, Detail: fmt.Sprint(p)}}
		}
	}()
	return run(cfg)
}

func run(cfg Config) Result {
	w := &world{
		cfg:     cfg,
		rng:     rand.New(rand.NewPCG(cfg.Seed, 0x9e3779b97f4a7c15)),
		nodes:   make([]*node, cfg.Nodes+1),
		leaders: map[uint64]raft.ID{},
		votes:   map[[2]uint64]raft.ID{},
		chaos:   true,
	}
	w.cut = make([][]bool, cfg.Nodes+1)
	for i := range w.cut {
		w.cut[i] = make([]bool, cfg.Nodes+1)
	}
	if cfg.Record {
		w.timeline = &Timeline{Nodes: cfg.Nodes, ChaosMs: cfg.ChaosMs}
	}
	for _, id := range w.ids() {
		w.nodes[id] = &node{id: id}
		w.start(w.nodes[id])
	}
	for c := 0; c < cfg.Clients; c++ {
		w.clients = append(w.clients, &client{id: c + 1})
		w.schedule(int64(w.rng.IntN(100)), &event{kind: evClientNext, client: c + 1})
	}
	if cfg.Faults {
		w.schedule(w.faultGap(), &event{kind: evFault})
	}
	w.schedule(cfg.ChaosMs, &event{kind: evHeal})

	end := cfg.ChaosMs + cfg.HealMs
	for w.q.Len() > 0 && w.violation == nil {
		e := heap.Pop(&w.q).(*event)
		if e.at > end {
			break
		}
		w.now = e.at
		w.stats.Events++
		w.handle(e)
		if !w.chaos && w.allDone() {
			break
		}
	}
	w.stats.EndMs = w.now

	if w.violation == nil && !w.allDone() {
		var stuck []string
		for _, c := range w.clients {
			if c.done < cfg.OpsPerClient {
				stuck = append(stuck, fmt.Sprintf("client %d at op %d/%d", c.id, c.done+1, cfg.OpsPerClient))
			}
		}
		w.fail(Liveness, "%d ms after healing, %s", cfg.HealMs, strings.Join(stuck, ", "))
	}
	if w.violation == nil {
		if r := linz.Check(w.history); !r.OK {
			w.fail(Linearizability, "the history of key %q (%d operations) has no valid order", r.Key, len(r.Ops))
		}
	}
	for _, op := range w.history {
		if op.Return == linz.Pending {
			w.stats.OpsPending++
		} else {
			w.stats.OpsDone++
		}
	}
	w.stats.Committed = len(w.committed)
	if w.timeline != nil {
		w.timeline.close(w.now)
	}
	return Result{Seed: cfg.Seed, Violation: w.violation, Stats: w.stats, History: w.history, Trace: w.trace, Timeline: w.timeline}
}

func (w *world) allDone() bool {
	for _, c := range w.clients {
		if c.done < w.cfg.OpsPerClient {
			return false
		}
	}
	return true
}

func (w *world) faultGap() int64 { return w.cfg.FaultGapMs + int64(w.rng.IntN(int(w.cfg.FaultGapMs))) }

func (w *world) start(n *node) {
	n.inc++
	n.up = true
	factor := 1.0
	if w.cfg.Skew > 0 {
		factor += w.cfg.Skew * (2*w.rng.Float64() - 1)
	}
	n.period = max(1, int64(float64(w.cfg.TickMs)*factor+0.5))
	n.r = raft.New(raft.Config{
		ID: n.id, Peers: w.ids(),
		ElectionTicks: w.cfg.ElectionTicks, HeartbeatTicks: w.cfg.HeartbeatTicks, MaxEntries: w.cfg.MaxEntries,
		Rand: rand.New(rand.NewPCG(w.rng.Uint64(), uint64(n.id))),
		Bugs: w.cfg.Bugs,
	}, n.disk)
	n.store = kv.New(w.cfg.NoDedup)
	n.pending = map[uint64]pending{}
	n.lastCommit = 0
	n.role, n.term = raft.Follower, n.disk.Term
	w.record(n)
	w.schedule(n.period, &event{kind: evTick, node: n.id, inc: n.inc})
}

func (w *world) restart(n *node) {
	w.start(n)
	w.note("node %d restarts (term %d, %d entries on disk)", n.id, n.disk.Term, len(n.disk.Log))
}

func (w *world) crash(n *node) {
	n.up = false
	n.r = nil
	n.store = nil
	n.pending = nil
	w.stats.Crashes++
	w.note("node %d crashes", n.id)
	if w.timeline != nil {
		w.timeline.down(w.now, n.id)
	}
}

func (w *world) connected(a, b raft.ID) bool { return !w.cut[a][b] }

func (w *world) delay() int64 {
	if !w.chaos {
		// Healed: a network fast enough for Raft's timing requirement
		// (broadcast time well under the election timeout), without which
		// no consensus algorithm promises progress.
		return 1 + int64(w.rng.IntN(5))
	}
	if w.rng.Float64() < 0.05 {
		return w.cfg.MaxDelayMs + int64(w.rng.IntN(200)) // a slow path: reorders behind later traffic
	}
	return 1 + int64(w.rng.IntN(int(w.cfg.MaxDelayMs)))
}

func (w *world) lossy() bool { return w.chaos && w.rng.Float64() < w.cfg.DropRate }

func (w *world) handle(e *event) {
	switch e.kind {
	case evTick:
		n := w.nodes[e.node]
		if !n.up || n.inc != e.inc {
			return
		}
		n.r.Tick()
		w.process(n)
		w.schedule(n.period, &event{kind: evTick, node: n.id, inc: n.inc})
	case evDeliver:
		n := w.nodes[e.msg.To]
		if !n.up || !w.connected(e.msg.From, e.msg.To) {
			w.stats.Dropped++
			return
		}
		n.r.Step(e.msg)
		w.process(n)
	case evRequest:
		w.serve(w.nodes[e.node], e.req)
	case evReply:
		w.clientReply(e.rep)
	case evClientTimeout:
		c := w.clients[e.client-1]
		if c.waiting && c.attempt == e.attempt {
			w.send(c)
		}
	case evClientNext:
		w.nextOp(w.clients[e.client-1])
	case evFault:
		if w.chaos {
			w.fault()
			w.schedule(w.faultGap(), &event{kind: evFault})
		}
	case evRestart:
		if n := w.nodes[e.node]; !n.up {
			w.restart(n)
		}
	case evHeal:
		w.chaos = false
		w.links("", func(a, b raft.ID) bool { return true })
		for _, id := range w.ids() {
			if !w.nodes[id].up {
				w.start(w.nodes[id])
			}
		}
		w.note("chaos ends: network healed, every node up")
	}
}

// process carries out a node's Ready: persist, then send, then apply, and
// checks every safety property the step could have broken.
func (w *world) process(n *node) {
	rd := n.r.Ready()
	if rd.Persist != nil {
		if rd.Persist.Term < n.disk.Term {
			w.fail(Monotonicity, "node %d persisted term %d after term %d", n.id, rd.Persist.Term, n.disk.Term)
		}
		n.disk = *rd.Persist
		w.checkLogMatching(n)
	}
	for _, m := range rd.Messages {
		if m.Type == raft.MsgVoteResp && m.Granted {
			k := [2]uint64{uint64(n.id), m.Term}
			if prev, ok := w.votes[k]; ok && prev != m.To {
				w.stats.DoubleVotes++
			}
			w.votes[k] = m.To
		}
		w.stats.Messages++
		if w.lossy() {
			w.stats.Dropped++
			continue
		}
		w.schedule(w.delay(), &event{kind: evDeliver, msg: m})
		if w.chaos && w.rng.Float64() < w.cfg.DupRate {
			w.stats.Duplicated++
			w.schedule(w.delay(), &event{kind: evDeliver, msg: m})
		}
	}
	for _, a := range rd.Apply {
		w.apply(n, a)
	}
	if n.r.Role() == raft.Leader {
		for uint64(len(w.commitTerm)) < n.r.Commit() {
			w.commitTerm = append(w.commitTerm, n.r.Term())
		}
	}
	if c := n.r.Commit(); c < n.lastCommit {
		w.fail(Monotonicity, "node %d commit index went from %d to %d", n.id, n.lastCommit, c)
	} else {
		n.lastCommit = c
	}
	if role, term := n.r.Role(), n.r.Term(); role != n.role || term != n.term {
		n.role, n.term = role, term
		w.record(n)
		if role == raft.Leader {
			w.becameLeader(n)
		}
	}
}

func (w *world) record(n *node) {
	if w.timeline != nil {
		w.timeline.role(w.now, n.id, n.role, n.term)
	}
}

func (w *world) becameLeader(n *node) {
	term := n.r.Term()
	w.stats.Leaders++
	w.stats.MaxTerm = max(w.stats.MaxTerm, term)
	w.note("node %d is leader of term %d", n.id, term)
	if other, ok := w.leaders[term]; ok && other != n.id {
		w.fail(ElectionSafety, "nodes %d and %d are both leader of term %d", other, n.id, term)
		return
	}
	w.leaders[term] = n.id
	// Every entry committed in an earlier term must be in the new leader's
	// log. (A leader of an old term, elected behind a partition after later
	// terms committed more, owes nothing to those later terms.)
	log := n.r.Log()
	for i, e := range w.committed {
		if i >= len(w.commitTerm) || w.commitTerm[i] >= term {
			continue
		}
		if i >= len(log) || log[i] != e {
			got := "nothing there"
			if i < len(log) {
				got = describe(log[i])
			}
			w.fail(LeaderCompleteness, "node %d leads term %d without entry %d (%s), committed in term %d; it has %s",
				n.id, term, i+1, describe(e), w.commitTerm[i], got)
			return
		}
	}
}

func describe(e raft.Entry) string {
	if e.Data == "" {
		return fmt.Sprintf("the term-%d leader's no-op", e.Term)
	}
	if op, err := kv.Decode(e.Data); err == nil {
		return fmt.Sprintf("term %d: client %d %s %s %s", e.Term, op.Client, op.Kind, op.Key, op.Value)
	}
	return fmt.Sprintf("term %d %q", e.Term, e.Data)
}

// Log Matching: if two logs have an entry with the same index and term, the
// logs are identical up to and including it. Checked between the node that
// just persisted and every other node's durable log.
func (w *world) checkLogMatching(n *node) {
	a := n.disk.Log
	for _, id := range w.ids() {
		if id == n.id {
			continue
		}
		b := w.nodes[id].disk.Log
		i := min(len(a), len(b))
		for i > 0 && a[i-1].Term != b[i-1].Term {
			i--
		}
		for j := 0; j < i; j++ {
			if a[j] != b[j] {
				w.fail(LogMatching, "nodes %d and %d agree on entry %d (term %d) but differ at entry %d",
					n.id, id, i, a[i-1].Term, j+1)
				return
			}
		}
	}
}

func (w *world) apply(n *node, a raft.Applied) {
	idx := int(a.Index)
	switch {
	case idx <= len(w.committed):
		if w.committed[idx-1] != a.Entry {
			c := w.committed[idx-1]
			w.fail(StateMachineSafety, "node %d applied %s at index %d, where %s was applied before",
				n.id, describe(a.Entry), idx, describe(c))
			return
		}
	case idx == len(w.committed)+1:
		w.committed = append(w.committed, a.Entry)
	default:
		w.fail(StateMachineSafety, "node %d applied index %d before index %d", n.id, idx, len(w.committed)+1)
		return
	}
	if a.Entry.Data == "" {
		return // a leader's no-op
	}
	op, err := kv.Decode(a.Entry.Data)
	if err != nil {
		w.fail(StateMachineSafety, "%v", err)
		return
	}
	result := n.store.Apply(op)
	p, ok := n.pending[a.Index]
	if !ok {
		return
	}
	delete(n.pending, a.Index)
	// The index this node proposed at may hold another command now, if it
	// lost leadership; the client must retry.
	same := p.client == op.Client && p.seq == op.Seq
	w.reply(n, reply{client: p.client, attempt: p.attempt, ok: same, output: result})
}

func (w *world) reply(n *node, r reply) {
	if w.lossy() {
		return
	}
	w.schedule(w.delay(), &event{kind: evReply, rep: r})
}

// serve handles a client request arriving at a node.
func (w *world) serve(n *node, req request) {
	if !n.up {
		return
	}
	if n.r.Role() != raft.Leader {
		w.reply(n, reply{client: req.client, attempt: req.attempt, hint: n.r.Leader()})
		return
	}
	idx, _, ok := n.r.Propose(req.op.Encode())
	if !ok {
		return
	}
	n.pending[idx] = pending{client: req.client, seq: req.op.Seq, attempt: req.attempt}
	w.process(n)
}

func (w *world) nextOp(c *client) {
	if c.done >= w.cfg.OpsPerClient {
		return
	}
	c.seq++
	op := kv.Op{Client: c.id, Seq: c.seq, Key: fmt.Sprintf("k%d", w.rng.IntN(w.cfg.Keys))}
	switch w.rng.IntN(3) {
	case 0:
		op.Kind = kv.Get
	case 1:
		op.Kind = kv.Put
		op.Value = fmt.Sprintf("c%d.%d", c.id, c.seq)
	default:
		op.Kind = kv.Append
		op.Value = fmt.Sprintf("c%d.%d;", c.id, c.seq)
	}
	c.op = op
	c.waiting = true
	c.historyAt = len(w.history)
	w.history = append(w.history, linz.Operation{
		Client: c.id, Call: w.now, Return: linz.Pending,
		Kind: linz.Kind(op.Kind), Key: op.Key, Value: op.Value,
	})
	w.send(c)
}

// send (re)sends the client's current op, to the node it believes leads or,
// lacking a hint, to a random one. Same seq every time: a retry.
func (w *world) send(c *client) {
	c.attempt++
	to := c.hint
	if to == raft.None || w.rng.Float64() < 0.2 {
		to = raft.ID(1 + w.rng.IntN(w.cfg.Nodes))
	}
	if !w.lossy() {
		w.schedule(w.delay(), &event{kind: evRequest, node: to, req: request{client: c.id, attempt: c.attempt, op: c.op}})
	}
	w.schedule(300, &event{kind: evClientTimeout, client: c.id, attempt: c.attempt})
}

func (w *world) clientReply(r reply) {
	c := w.clients[r.client-1]
	if !c.waiting || r.attempt != c.attempt {
		return // a reply to an earlier attempt
	}
	if !r.ok {
		c.hint = r.hint
		c.attempt++ // the pending timeout no longer applies
		at := c.attempt
		w.schedule(10, &event{kind: evClientTimeout, client: c.id, attempt: at})
		return
	}
	c.waiting = false
	c.done++
	h := &w.history[c.historyAt]
	h.Return = w.now
	if c.op.Kind == kv.Get {
		h.Output = r.output
	}
	w.schedule(int64(w.rng.IntN(150)), &event{kind: evClientNext, client: c.id})
}

func (w *world) fault() {
	ids := w.ids()
	switch x := w.rng.Float64(); {
	case x < 0.35:
		w.partition()
	case x < 0.55:
		w.links("", func(a, b raft.ID) bool { return true })
	case x < 0.80:
		var up []*node
		for _, id := range ids {
			if w.nodes[id].up {
				up = append(up, w.nodes[id])
			}
		}
		if len(up) > 0 {
			n := w.nodes[w.victim()]
			if !n.up {
				n = up[w.rng.IntN(len(up))]
			}
			w.crash(n)
			if w.rng.Float64() < 0.5 {
				// A bounce: the process dies and its supervisor restarts it at once.
				w.schedule(1+int64(w.rng.IntN(30)), &event{kind: evRestart, node: n.id})
			}
		}
	default:
		var down []*node
		for _, id := range ids {
			if !w.nodes[id].up {
				down = append(down, w.nodes[id])
			}
		}
		if len(down) > 0 {
			w.restart(down[w.rng.IntN(len(down))])
		}
	}
}

// victim picks a node to cut off or crash: half the time the current
// leader, since the dangerous moments are mostly a leader's last ones.
func (w *world) victim() raft.ID {
	if w.rng.Float64() < 0.5 {
		var best *node
		for _, id := range w.ids() {
			n := w.nodes[id]
			if n.up && n.role == raft.Leader && (best == nil || n.term > best.term) {
				best = n
			}
		}
		if best != nil {
			return best.id
		}
	}
	return raft.ID(1 + w.rng.IntN(w.cfg.Nodes))
}

// partition cuts the network one of four ways. The last two are not clean
// splits: connectivity stops being transitive, so a node can hear two sides
// that cannot hear each other, and vote in both of their elections.
func (w *world) partition() {
	ids := w.ids()
	perm := append([]raft.ID(nil), ids...)
	w.rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
	pos := map[raft.ID]int{}
	for i, id := range perm {
		pos[id] = i
	}
	names := func(xs []raft.ID) string {
		var parts []string
		for _, x := range xs {
			parts = append(parts, fmt.Sprint(x))
		}
		return strings.Join(parts, ",")
	}
	switch w.rng.IntN(4) {
	case 0:
		v := w.victim()
		w.links(fmt.Sprintf("node %d isolated", v), func(a, b raft.ID) bool { return a != v && b != v })
	case 1:
		side := map[raft.ID]int{}
		var l, r []raft.ID
		for _, id := range ids {
			side[id] = w.rng.IntN(2)
			if side[id] == 0 {
				l = append(l, id)
			} else {
				r = append(r, id)
			}
		}
		w.links(fmt.Sprintf("split {%s} | {%s}", names(l), names(r)), func(a, b raft.ID) bool { return side[a] == side[b] })
	case 2:
		bridge, half := perm[0], (len(perm)+1)/2
		w.links(fmt.Sprintf("bridge: node %d alone links {%s} and {%s}", bridge, names(perm[1:half]), names(perm[half:])),
			func(a, b raft.ID) bool {
				return a == bridge || b == bridge || (pos[a] < half) == (pos[b] < half)
			})
	default:
		n := len(perm)
		w.links("ring "+strings.ReplaceAll(names(perm), ",", "-"), func(a, b raft.ID) bool {
			d := (pos[a] - pos[b] + n) % n
			return d <= 1 || d == n-1
		})
	}
	w.stats.Partitions++
}

// links sets which pairs can talk; label "" means fully healed.
func (w *world) links(label string, connected func(a, b raft.ID) bool) {
	for _, a := range w.ids() {
		for _, b := range w.ids() {
			w.cut[a][b] = a != b && !connected(a, b)
		}
	}
	if label == "" {
		w.note("network healed")
	} else {
		w.note("partition: %s", label)
	}
	if w.timeline != nil {
		var cutoff []bool
		if label != "" {
			// Shade the nodes that cannot reach a majority, themselves included.
			cutoff = make([]bool, w.cfg.Nodes+1)
			for _, a := range w.ids() {
				reach := 0
				for _, b := range w.ids() {
					if w.connected(a, b) {
						reach++
					}
				}
				cutoff[a] = reach*2 <= w.cfg.Nodes
			}
		}
		w.timeline.partition(w.now, label, cutoff)
	}
}
