// Package linz checks that a history of key-value operations is
// linearizable: that there is one order of the operations, consistent with
// real time, in which every result is what a single copy of the store would
// have returned.
//
// It is the Wing & Gong search with Lowe's memoisation (the algorithm behind
// Porcupine): try to linearize the earliest pending call; on a dead end,
// undo the last choice; never revisit a (set of linearized operations,
// state) pair. Linearizability is compositional, so each key is checked on
// its own, which keeps the search small.
package linz

import (
	"math"
	"sort"
	"strings"
)

type Kind string

const (
	Get    Kind = "get"
	Put    Kind = "put"
	Append Kind = "append"
)

// Pending is the Return time of an operation that never got a reply. It may
// or may not have taken effect, and the checker considers both.
const Pending = math.MaxInt64

type Operation struct {
	Client       int
	Call, Return int64
	Kind         Kind
	Key, Value   string
	// Output is what a get returned.
	Output string
}

// Result of a check. When OK is false, Key is a key whose history cannot be
// linearized and Ops is that history.
type Result struct {
	OK  bool
	Key string
	Ops []Operation
}

// Check partitions the history by key and checks each part. Gets that never
// returned observed nothing and are dropped.
func Check(history []Operation) Result {
	byKey := map[string][]Operation{}
	var keys []string
	for _, op := range history {
		if op.Kind == Get && op.Return == Pending {
			continue
		}
		if _, ok := byKey[op.Key]; !ok {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			return Result{OK: false, Key: k, Ops: byKey[k]}
		}
	}
	return Result{OK: true}
}

// step applies op to the state of one key.
func step(state string, op Operation) (bool, string) {
	switch op.Kind {
	case Get:
		return op.Output == state, state
	case Put:
		return true, op.Value
	default:
		return true, state + op.Value
	}
}

type entry struct {
	op         int
	call       bool
	time       int64
	match      *entry // a call's return
	prev, next *entry
}

func checkKey(ops []Operation) bool {
	// Events in time order; a call before a return at the same instant,
	// so operations that touch are treated as concurrent.
	evs := make([]*entry, 0, 2*len(ops))
	for i, op := range ops {
		c := &entry{op: i, call: true, time: op.Call}
		r := &entry{op: i, time: op.Return}
		c.match = r
		evs = append(evs, c, r)
	}
	sort.SliceStable(evs, func(a, b int) bool {
		if evs[a].time != evs[b].time {
			return evs[a].time < evs[b].time
		}
		return evs[a].call && !evs[b].call
	})
	head := &entry{}
	prev := head
	for _, e := range evs {
		prev.next, e.prev = e, prev
		prev = e
	}

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		r := e.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(e *entry) {
		r := e.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e     *entry
		state string
	}
	linearized := make([]uint64, (len(ops)+63)/64)
	seen := map[string]struct{}{}
	key := func(state string) string {
		var b strings.Builder
		for _, w := range linearized {
			for i := 0; i < 8; i++ {
				b.WriteByte(byte(w >> (8 * i)))
			}
		}
		b.WriteByte(0)
		b.WriteString(state)
		return b.String()
	}

	var stack []frame
	state := ""
	e := head.next
	for {
		// Every completed operation is linearized once the search reaches the
		// end, or a return at Pending (all such returns sort last).
		if e == nil || (!e.call && e.time == Pending) {
			return true
		}
		if e.call {
			if ok, next := step(state, ops[e.op]); ok {
				linearized[e.op/64] |= 1 << (e.op % 64)
				k := key(next)
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					stack = append(stack, frame{e, state})
					state = next
					lift(e)
					e = head.next
					continue
				}
				linearized[e.op/64] &^= 1 << (e.op % 64)
			}
			e = e.next
			continue
		}
		// A completed operation's return with its call not yet linearized:
		// back up.
		if len(stack) == 0 {
			return false
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		linearized[f.e.op/64] &^= 1 << (f.e.op % 64)
		unlift(f.e)
		e = f.e.next
	}
}
