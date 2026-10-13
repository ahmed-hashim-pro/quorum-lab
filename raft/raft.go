// Package raft is Raft consensus as a pure state machine.
//
// A Node has no goroutines, no clock and no network. Time is Tick, input is
// Step, and output is Ready: what to persist, what to send, what to apply.
// That makes every run a deterministic function of its inputs, so a
// simulator can drive it through millions of interleavings and replay any
// one of them exactly from a seed.
//
// The contract for whoever drives a Node: after each Tick, Step or Propose,
// take Ready, save Persist durably before sending any of Messages, then
// apply Apply in order.
package raft

import (
	"fmt"
	"math/rand/v2"
)

// ID names a node. IDs are 1..n; None is 0.
type ID int

const None ID = 0

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// Entry is one log entry. Its index is its position in the log, from 1.
// A leader appends an entry with empty Data when it takes office.
type Entry struct {
	Term uint64
	Data string
}

// State is what survives a crash: the current term, the vote cast in it, and
// the log. Raft is only safe if this is on disk before any message that
// depends on it leaves the node.
type State struct {
	Term     uint64
	VotedFor ID
	Log      []Entry
}

func (s State) clone() State {
	s.Log = append([]Entry(nil), s.Log...)
	return s
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	return [...]string{"RequestVote", "RequestVoteResp", "AppendEntries", "AppendEntriesResp"}[t]
}

// Message is one RPC or its reply. Fields not used by a type stay zero.
type Message struct {
	Type     MsgType
	From, To ID
	Term     uint64

	// RequestVote: the candidate's last log position.
	LastIndex, LastTerm uint64
	// RequestVoteResp.
	Granted bool

	// AppendEntries.
	PrevIndex, PrevTerm uint64
	Entries             []Entry
	Commit              uint64
	// AppendEntriesResp: on success, the index the follower now matches up
	// to; on failure, a hint of where to retry from.
	Success bool
	Match   uint64
}

// Bugs are deliberate mistakes, each a known way to get Raft wrong. They
// exist so the simulator can prove it would notice: every one of them must
// be caught, and the correct node must never be.
type Bugs struct {
	// Grant votes without checking the candidate's log is up to date
	// (Raft §5.4.1). A leader can be elected without committed entries.
	VoteWithoutLogCheck bool
	// Commit an entry from an earlier term once a majority stores it
	// (the scenario of Figure 8 in the Raft paper).
	CommitPreviousTerm bool
	// Do not persist votedFor. After a restart the node can vote twice in
	// one term.
	ForgetVote bool
	// On a conflicting entry, keep the follower's own entry instead of
	// truncating its log from there.
	KeepConflicts bool
	// Accept AppendEntries from a leader of an older term.
	AcceptStaleTerm bool
}

type Config struct {
	ID    ID
	Peers []ID // every node in the cluster, including this one
	// The election timeout is drawn from [ElectionTicks, 2*ElectionTicks).
	ElectionTicks  int
	HeartbeatTicks int
	// MaxEntries caps the entries in one AppendEntries, as a size limit would.
	MaxEntries int
	Rand       *rand.Rand
	Bugs       Bugs
}

// Applied is a committed entry handed to the state machine.
type Applied struct {
	Index uint64
	Entry Entry
}

// Ready is the output of a step. Persist is nil when nothing durable changed.
type Ready struct {
	Persist  *State
	Messages []Message
	Apply    []Applied
}

type Node struct {
	cfg Config
	st  State

	role   Role
	leader ID
	commit uint64
	// applied is the last index handed out through Ready.Apply.
	applied uint64

	electionElapsed, electionTimeout, heartbeatElapsed int

	votes       map[ID]bool
	next, match map[ID]uint64
	msgs        []Message
	dirty       bool
}

// New starts a node from its durable state (zero State for a fresh one).
// Everything else, including the commit index, starts over, as after a crash.
func New(cfg Config, st State) *Node {
	if cfg.ElectionTicks <= 0 || cfg.HeartbeatTicks <= 0 || cfg.HeartbeatTicks >= cfg.ElectionTicks {
		panic(fmt.Sprintf("raft: need 0 < HeartbeatTicks < ElectionTicks, got %d, %d", cfg.HeartbeatTicks, cfg.ElectionTicks))
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 64
	}
	n := &Node{cfg: cfg, st: st.clone()}
	n.resetElectionTimer()
	return n
}

func (n *Node) ID() ID            { return n.cfg.ID }
func (n *Node) Role() Role        { return n.role }
func (n *Node) Term() uint64      { return n.st.Term }
func (n *Node) Leader() ID        { return n.leader }
func (n *Node) Commit() uint64    { return n.commit }
func (n *Node) LastIndex() uint64 { return uint64(len(n.st.Log)) }

// Log returns the node's log. Callers must not modify it.
func (n *Node) Log() []Entry { return n.st.Log }

func (n *Node) termAt(i uint64) uint64 {
	if i == 0 || i > n.LastIndex() {
		return 0
	}
	return n.st.Log[i-1].Term
}

func (n *Node) lastTerm() uint64 { return n.termAt(n.LastIndex()) }

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.cfg.Rand.IntN(n.cfg.ElectionTicks)
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	if m.Term == 0 {
		m.Term = n.st.Term
	}
	n.msgs = append(n.msgs, m)
}

// Ready returns and clears everything produced since the last call.
func (n *Node) Ready() Ready {
	var rd Ready
	if n.dirty {
		st := n.st.clone()
		if n.cfg.Bugs.ForgetVote {
			st.VotedFor = None
		}
		rd.Persist = &st
		n.dirty = false
	}
	rd.Messages, n.msgs = n.msgs, nil
	for n.applied < n.commit {
		n.applied++
		rd.Apply = append(rd.Apply, Applied{Index: n.applied, Entry: n.st.Log[n.applied-1]})
	}
	return rd
}

func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
		return
	}
	// A candidate retries RequestVote to peers that have not answered, as
	// any RPC without a response is retried (§5.1).
	if n.role == Candidate {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.requestVotes()
		}
	}
}

func (n *Node) campaign() {
	n.role = Candidate
	n.leader = None
	n.st.Term++
	n.st.VotedFor = n.cfg.ID
	n.dirty = true
	n.resetElectionTimer()
	n.votes = map[ID]bool{n.cfg.ID: true}
	n.heartbeatElapsed = 0
	if n.granted() >= n.quorum() {
		n.becomeLeader()
		return
	}
	n.requestVotes()
}

func (n *Node) requestVotes() {
	for _, p := range n.cfg.Peers {
		if _, answered := n.votes[p]; !answered && p != n.cfg.ID {
			n.send(Message{Type: MsgVote, To: p, LastIndex: n.LastIndex(), LastTerm: n.lastTerm()})
		}
	}
}

func (n *Node) granted() int {
	c := 0
	for _, g := range n.votes {
		if g {
			c++
		}
	}
	return c
}

func (n *Node) becomeFollower(term uint64, leader ID) {
	if term > n.st.Term {
		n.st.Term = term
		n.st.VotedFor = None
		n.dirty = true
	}
	n.role = Follower
	n.leader = leader
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.heartbeatElapsed = 0
	n.next = map[ID]uint64{}
	n.match = map[ID]uint64{}
	// A leader cannot count replicas of earlier terms' entries (§5.4.2), so
	// it commits an entry of its own term at once; that commits everything
	// before it too.
	n.st.Log = append(n.st.Log, Entry{Term: n.st.Term})
	n.dirty = true
	for _, p := range n.cfg.Peers {
		n.next[p] = n.LastIndex()
	}
	n.match[n.cfg.ID] = n.LastIndex()
	n.maybeCommit()
	n.broadcastAppend()
}

// Propose appends data to the log if this node is the leader, and returns
// the index and term it was appended at. That is not a promise it will
// commit: a new leader may overwrite it.
func (n *Node) Propose(data string) (index, term uint64, ok bool) {
	if n.role != Leader {
		return 0, 0, false
	}
	n.st.Log = append(n.st.Log, Entry{Term: n.st.Term, Data: data})
	n.dirty = true
	n.match[n.cfg.ID] = n.LastIndex()
	n.maybeCommit()
	n.broadcastAppend()
	return n.LastIndex(), n.st.Term, true
}

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to ID) {
	// next comes from peers' replies; never trust it past the end of the log.
	n.next[to] = max(1, min(n.next[to], n.LastIndex()+1))
	prev := n.next[to] - 1
	end := min(n.LastIndex(), prev+uint64(n.cfg.MaxEntries))
	n.send(Message{
		Type:      MsgApp,
		To:        to,
		PrevIndex: prev,
		PrevTerm:  n.termAt(prev),
		Entries:   append([]Entry(nil), n.st.Log[prev:end]...),
		Commit:    n.commit,
	})
}

func (n *Node) maybeCommit() {
	for i := n.LastIndex(); i > n.commit; i-- {
		if n.termAt(i) != n.st.Term && !n.cfg.Bugs.CommitPreviousTerm {
			// Entries are only ever more recent going up; nothing lower is of this term.
			return
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.match[p] >= i {
				count++
			}
		}
		if count >= n.quorum() {
			n.commit = i
			return
		}
	}
}

func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.st.Term:
		leader := None
		if m.Type == MsgApp {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	case m.Term < n.st.Term:
		if m.Type == MsgApp && n.cfg.Bugs.AcceptStaleTerm {
			n.handleAppend(m)
			return
		}
		// Tell a stale sender about the newer term; ignore stale replies.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From, Success: false})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		if n.role == Candidate {
			n.votes[m.From] = m.Granted
			if n.granted() >= n.quorum() {
				n.becomeLeader()
			}
		}
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	}
}

func (n *Node) handleVote(m Message) {
	canVote := n.st.VotedFor == None || n.st.VotedFor == m.From
	upToDate := m.LastTerm > n.lastTerm() || (m.LastTerm == n.lastTerm() && m.LastIndex >= n.LastIndex())
	if n.cfg.Bugs.VoteWithoutLogCheck {
		upToDate = true
	}
	grant := canVote && upToDate
	if grant {
		n.st.VotedFor = m.From
		n.dirty = true
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

func (n *Node) handleAppend(m Message) {
	if m.Term == n.st.Term {
		// A leader exists for this term; a candidate stands down.
		n.role = Follower
		n.leader = m.From
	}
	n.resetElectionTimer()

	if m.PrevIndex > n.LastIndex() || n.termAt(m.PrevIndex) != m.PrevTerm {
		hint := min(n.LastIndex(), m.PrevIndex-1)
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, Match: hint})
		return
	}
	for i, e := range m.Entries {
		idx := m.PrevIndex + 1 + uint64(i)
		if idx <= n.LastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			if n.cfg.Bugs.KeepConflicts {
				continue
			}
			n.st.Log = n.st.Log[:idx-1]
		}
		n.st.Log = append(n.st.Log, e)
		n.dirty = true
	}
	last := m.PrevIndex + uint64(len(m.Entries))
	if c := min(m.Commit, last); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, Match: last})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	if m.Success {
		if m.Match > n.match[m.From] && m.Match <= n.LastIndex() {
			n.match[m.From] = m.Match
		}
		n.next[m.From] = max(n.next[m.From], m.Match+1)
		n.maybeCommit()
		if n.next[m.From] <= n.LastIndex() {
			n.sendAppend(m.From)
		}
		return
	}
	// Back up, but never below what the follower is known to match. Probe
	// again only if the rejection moved next: a duplicated or stale
	// rejection teaches nothing, and answering every copy multiplies traffic.
	next := max(n.match[m.From]+1, min(n.next[m.From]-1, m.Match+1))
	if next < n.next[m.From] {
		n.next[m.From] = next
		n.sendAppend(m.From)
	}
}
