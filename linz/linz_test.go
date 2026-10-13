package linz

import (
	"math/rand/v2"
	"testing"
)

func op(client int, call, ret int64, kind Kind, value, output string) Operation {
	return Operation{Client: client, Call: call, Return: ret, Kind: kind, Key: "k", Value: value, Output: output}
}

func TestSequentialHistories(t *testing.T) {
	ok := []Operation{
		op(1, 0, 10, Put, "a", ""),
		op(2, 20, 30, Get, "", "a"),
		op(1, 40, 50, Append, "b", ""),
		op(2, 60, 70, Get, "", "ab"),
	}
	if !Check(ok).OK {
		t.Fatal("a sequential, correct history was rejected")
	}
	stale := []Operation{
		op(1, 0, 10, Put, "a", ""),
		op(1, 20, 30, Put, "b", ""),
		op(2, 40, 50, Get, "", "a"), // reads a value overwritten before it began
	}
	if Check(stale).OK {
		t.Fatal("a stale read was accepted")
	}
}

func TestConcurrencyAllowsEitherOrder(t *testing.T) {
	// The put overlaps both gets, so one may see the old value and one the new.
	h := []Operation{
		op(1, 0, 100, Put, "x", ""),
		op(2, 10, 20, Get, "", ""),
		op(3, 30, 40, Get, "", "x"),
	}
	if !Check(h).OK {
		t.Fatal("rejected a valid concurrent history")
	}
	// But not new then old: once a get has seen x, a later get cannot miss it.
	h = []Operation{
		op(1, 0, 100, Put, "x", ""),
		op(2, 10, 20, Get, "", "x"),
		op(3, 30, 40, Get, "", ""),
	}
	if Check(h).OK {
		t.Fatal("accepted a value going back in time")
	}
}

func TestDuplicateAppendIsCaught(t *testing.T) {
	h := []Operation{
		op(1, 0, 10, Append, "a", ""),
		op(2, 20, 30, Get, "", "aa"),
	}
	if Check(h).OK {
		t.Fatal("an append applied twice was accepted")
	}
}

func TestPendingMayOrMayNotHaveHappened(t *testing.T) {
	happened := []Operation{
		op(1, 0, Pending, Append, "a", ""),
		op(2, 20, 30, Get, "", "a"),
	}
	didnt := []Operation{
		op(1, 0, Pending, Append, "a", ""),
		op(2, 20, 30, Get, "", ""),
	}
	if !Check(happened).OK || !Check(didnt).OK {
		t.Fatal("a pending operation must be allowed either way")
	}
	// It cannot take effect before it was called.
	early := []Operation{
		op(2, 0, 10, Get, "", "a"),
		op(1, 20, Pending, Append, "a", ""),
	}
	if Check(early).OK {
		t.Fatal("a pending operation took effect before its call")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Operation{
		{Client: 1, Call: 0, Return: 10, Kind: Put, Key: "a", Value: "1"},
		{Client: 2, Call: 20, Return: 30, Kind: Get, Key: "b", Output: ""},
		{Client: 2, Call: 40, Return: 50, Kind: Get, Key: "a", Output: "2"},
	}
	r := Check(h)
	if r.OK || r.Key != "a" {
		t.Fatalf("%+v", r)
	}
}

// Histories produced by running a real sequential store with random, possibly
// overlapping timings: always linearizable, by construction, at the time each
// op took effect. Then corrupt one output and the check must fail (when the
// corruption is observable).
func TestRandomHistories(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for trial := 0; trial < 300; trial++ {
		var h []Operation
		state := ""
		at := int64(0)
		for i := 0; i < 30; i++ {
			at += int64(r.IntN(10) + 1)
			o := Operation{Client: r.IntN(4), Key: "k"}
			switch r.IntN(3) {
			case 0:
				o.Kind = Get
				o.Output = state
			case 1:
				o.Kind = Put
				o.Value = string(rune('a' + r.IntN(3)))
				state = o.Value
			default:
				o.Kind = Append
				o.Value = string(rune('a' + r.IntN(3)))
				state += o.Value
			}
			// Took effect at `at`; called before, returned after.
			o.Call = at - int64(r.IntN(25))
			o.Return = at + int64(r.IntN(25))
			h = append(h, o)
		}
		if !Check(h).OK {
			t.Fatalf("trial %d: a linearizable history was rejected", trial)
		}
		// A get whose output is a value the key never held is never linearizable.
		for i := range h {
			if h[i].Kind == Get {
				h[i].Output = "never"
				if Check(h).OK {
					t.Fatalf("trial %d: impossible read accepted", trial)
				}
				break
			}
		}
	}
}
