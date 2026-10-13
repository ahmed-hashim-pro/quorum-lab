package raft

import (
	"math/rand/v2"
	"testing"
)

// cluster is a hand-driven harness: messages queue up until a test delivers
// them, so each test controls the interleaving exactly.
type cluster struct {
	t     *testing.T
	nodes map[ID]*Node
	disk  map[ID]State
	inbox []Message
	bugs  Bugs
	ids   []ID
	maxE  int
}

func newCluster(t *testing.T, n int, bugs Bugs, maxEntries int, states map[ID]State) *cluster {
	c := &cluster{t: t, nodes: map[ID]*Node{}, disk: map[ID]State{}, bugs: bugs, maxE: maxEntries}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, ID(i))
	}
	for _, id := range c.ids {
		c.disk[id] = states[id]
		c.start(id)
	}
	return c
}

func (c *cluster) start(id ID) {
	c.nodes[id] = New(Config{
		ID: id, Peers: c.ids, ElectionTicks: 10, HeartbeatTicks: 2, MaxEntries: c.maxE,
		Rand: rand.New(rand.NewPCG(uint64(id), 7)), Bugs: c.bugs,
	}, c.disk[id])
}

func (c *cluster) collect(id ID) []Applied {
	n := c.nodes[id]
	if n == nil {
		return nil
	}
	rd := n.Ready()
	if rd.Persist != nil {
		c.disk[id] = *rd.Persist
	}
	c.inbox = append(c.inbox, rd.Messages...)
	return rd.Apply
}

// elect makes id time out and campaign; nothing is delivered.
func (c *cluster) elect(id ID) {
	n := c.nodes[id]
	for n.Role() != Candidate && n.Role() != Leader {
		n.Tick()
	}
	c.collect(id)
}

// deliver delivers queued messages matching keep (all if nil), repeatedly,
// until none match. Messages to crashed nodes are lost.
func (c *cluster) deliver(keep func(Message) bool) {
	for {
		var now, later []Message
		for _, m := range c.inbox {
			if keep == nil || keep(m) {
				now = append(now, m)
			} else {
				later = append(later, m)
			}
		}
		if len(now) == 0 {
			return
		}
		c.inbox = later
		for _, m := range now {
			if n := c.nodes[m.To]; n != nil {
				n.Step(m)
				c.collect(m.To)
			}
		}
	}
}

func between(ids ...ID) func(Message) bool {
	in := map[ID]bool{}
	for _, id := range ids {
		in[id] = true
	}
	return func(m Message) bool { return in[m.From] && in[m.To] }
}

func (c *cluster) crash(id ID) { c.nodes[id] = nil }

func entries(terms ...uint64) []Entry {
	var out []Entry
	for i, t := range terms {
		out = append(out, Entry{Term: t, Data: string(rune('a' + i))})
	}
	return out
}

func TestOneLeaderIsElected(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, nil)
	c.elect(1)
	c.deliver(nil)
	if c.nodes[1].Role() != Leader || c.nodes[1].Term() != 1 {
		t.Fatalf("node 1: %v term %d", c.nodes[1].Role(), c.nodes[1].Term())
	}
	for _, id := range []ID{2, 3} {
		if n := c.nodes[id]; n.Role() != Follower || n.Leader() != 1 {
			t.Fatalf("node %d: %v, leader %d", id, n.Role(), n.Leader())
		}
	}
}

func TestReplicatesAndCommits(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, nil)
	c.elect(1)
	c.deliver(nil)
	if _, _, ok := c.nodes[1].Propose("x"); !ok {
		t.Fatal("leader refused a proposal")
	}
	c.collect(1)
	c.deliver(nil)
	if got := c.nodes[1].Commit(); got != 2 {
		t.Fatalf("leader commit %d, want 2 (no-op and x)", got)
	}
	// Followers learn the commit index on the next round.
	c.nodes[1].Tick()
	c.nodes[1].Tick()
	c.collect(1)
	c.deliver(nil)
	for _, id := range c.ids {
		var applied []Applied
		applied = append(applied, c.collect(id)...)
		if c.nodes[id].Commit() != 2 || c.nodes[id].Log()[1].Data != "x" {
			t.Fatalf("node %d: commit %d, log %v", id, c.nodes[id].Commit(), c.nodes[id].Log())
		}
	}
}

func TestFollowerRefusesProposals(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, nil)
	if _, _, ok := c.nodes[2].Propose("x"); ok {
		t.Fatal("a follower accepted a proposal")
	}
}

func TestStaleLeaderIsRejected(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, map[ID]State{2: {Term: 5}})
	c.nodes[2].Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Entries: entries(3)})
	rd := c.nodes[2].Ready()
	if len(c.nodes[2].Log()) != 0 {
		t.Fatal("follower took entries from a stale leader")
	}
	if len(rd.Messages) != 1 || rd.Messages[0].Success || rd.Messages[0].Term != 5 {
		t.Fatalf("reply %+v", rd.Messages)
	}

	bug := newCluster(t, 3, Bugs{AcceptStaleTerm: true}, 0, map[ID]State{2: {Term: 5}})
	bug.nodes[2].Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Entries: entries(3)})
	if len(bug.nodes[2].Log()) != 1 {
		t.Fatal("the accept-stale-term bug did not take effect")
	}
}

func TestVoteNeedsAnUpToDateLog(t *testing.T) {
	ask := Message{Type: MsgVote, From: 1, To: 2, Term: 3, LastIndex: 1, LastTerm: 1}
	for _, tc := range []struct {
		bugs  Bugs
		grant bool
	}{{Bugs{}, false}, {Bugs{VoteWithoutLogCheck: true}, true}} {
		c := newCluster(t, 3, tc.bugs, 0, map[ID]State{2: {Term: 2, Log: entries(1, 2)}})
		c.nodes[2].Step(ask)
		rd := c.nodes[2].Ready()
		if rd.Messages[0].Granted != tc.grant {
			t.Fatalf("%+v: granted %v", tc.bugs, rd.Messages[0].Granted)
		}
	}
}

// One vote per term, even across a restart: votedFor is durable.
func TestVoteSurvivesRestart(t *testing.T) {
	for _, tc := range []struct {
		bugs        Bugs
		secondGrant bool
	}{{Bugs{}, false}, {Bugs{ForgetVote: true}, true}} {
		c := newCluster(t, 3, tc.bugs, 0, nil)
		c.nodes[3].Step(Message{Type: MsgVote, From: 1, To: 3, Term: 4})
		c.collect(3)
		c.crash(3)
		c.start(3) // from what it persisted
		c.nodes[3].Step(Message{Type: MsgVote, From: 2, To: 3, Term: 4})
		rd := c.nodes[3].Ready()
		if got := rd.Messages[0].Granted; got != tc.secondGrant {
			t.Fatalf("%+v: second vote in term 4 granted=%v", tc.bugs, got)
		}
	}
}

func TestConflictingSuffixIsReplaced(t *testing.T) {
	for _, tc := range []struct {
		bugs Bugs
		want []uint64
	}{{Bugs{}, []uint64{1, 3}}, {Bugs{KeepConflicts: true}, []uint64{1, 2}}} {
		c := newCluster(t, 3, tc.bugs, 0, map[ID]State{2: {Term: 2, Log: entries(1, 2, 2)}})
		c.nodes[2].Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Term: 3, Data: "z"}}})
		var got []uint64
		for _, e := range c.nodes[2].Log() {
			got = append(got, e.Term)
		}
		if len(got) < 2 || got[0] != tc.want[0] || got[1] != tc.want[1] {
			t.Fatalf("%+v: log terms %v, want prefix %v", tc.bugs, got, tc.want)
		}
		if tc.bugs == (Bugs{}) && len(got) != 2 {
			t.Fatalf("the conflicting suffix survived: %v", got)
		}
	}
}

// A delayed AppendEntries that arrives after a newer one must not truncate
// entries the newer one added: only a conflict truncates.
func TestReorderedAppendDoesNotTruncate(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, nil)
	old := Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: entries(1)}
	newer := Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: entries(1, 1)}
	c.nodes[2].Step(newer)
	c.nodes[2].Step(old)
	if n := len(c.nodes[2].Log()); n != 2 {
		t.Fatalf("log has %d entries after a reordered append, want 2", n)
	}
}

// A peer's reply claiming a match past the end of the leader's log (a buggy
// or confused peer) must not crash the leader.
func TestLeaderSurvivesABogusReply(t *testing.T) {
	c := newCluster(t, 3, Bugs{}, 0, nil)
	c.elect(1)
	c.deliver(nil)
	c.nodes[1].Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, Match: 99})
	c.nodes[1].Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: false, Match: 99})
	c.collect(1)
	if c.nodes[1].Commit() > c.nodes[1].LastIndex() {
		t.Fatal("commit past the end of the log")
	}
}

// Figure 8 of the Raft paper, step by step. S1 leads term 4 holding an entry
// from term 2 at index 2 and replicates it to a majority (S1, S2, S3). It
// must not count that as committed: S5, holding a term-3 entry at index 2,
// can still win term 5 with S3 and S4 and overwrite it. The correct node
// waits; the buggy node commits, and the entry is lost.
func TestFigure8(t *testing.T) {
	for _, bug := range []bool{false, true} {
		states := map[ID]State{
			1: {Term: 3, Log: entries(1, 2)},
			2: {Term: 3, Log: entries(1, 2)},
			3: {Term: 3, Log: entries(1)},
			4: {Term: 3, Log: entries(1)},
			5: {Term: 4, Log: []Entry{{Term: 1, Data: "a"}, {Term: 3, Data: "S5"}}},
		}
		// One entry per message, so S3 acknowledges index 2 before S1's no-op.
		c := newCluster(t, 5, Bugs{CommitPreviousTerm: bug}, 1, states)
		c.elect(1) // S1 campaigns in term 4
		// It wins with S2 and S3. S2 takes the no-op too; S3 is sent index 2,
		// and S1 crashes before the no-op reaches it.
		inner := between(1, 2, 3)
		c.deliver(func(m Message) bool {
			// The first no-op to S3 fails (S3 lacks index 2) and is allowed;
			// once S3 holds index 2, the no-op would land, and is held back.
			landsNoop := m.To == 3 && m.Type == MsgApp && len(m.Entries) > 0 && m.Entries[0].Term == 4 &&
				m.PrevIndex <= c.nodes[3].LastIndex()
			return inner(m) && !landsNoop
		})
		s1 := c.nodes[1]
		if s1.Role() != Leader || s1.Term() != 4 {
			t.Fatalf("setup: S1 is %v in term %d", s1.Role(), s1.Term())
		}
		// S3 holds index 2 now; S1's no-op is only on S1 and S2.
		if got := c.nodes[3].Log(); len(got) < 2 || got[1].Term != 2 {
			t.Fatalf("setup: S3's log %v", got)
		}
		committed := s1.Commit() >= 2
		if committed != bug {
			t.Fatalf("bug=%v: S1 commit index %d", bug, s1.Commit())
		}
		c.crash(1)
		c.inbox = nil
		// S5 times out: term 5, and S3 and S4 (last terms 2 and 1) vote for it.
		c.elect(5)
		c.deliver(between(3, 4, 5))
		s5 := c.nodes[5]
		if s5.Role() != Leader || s5.Term() != 5 {
			t.Fatalf("S5 is %v in term %d", s5.Role(), s5.Term())
		}
		if got := c.nodes[3].Log()[1]; got.Data != "S5" {
			t.Fatalf("S3's index 2 is %+v, expected S5's entry", got)
		}
		// Index 2 now holds S5's entry on a majority. If S1 called index 2
		// committed (the bug), a committed entry has just been overwritten.
		t.Logf("bug=%v: S1 committed index 2: %v; S5 overwrote it on S3", bug, committed)
	}
}
