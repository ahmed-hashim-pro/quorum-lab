package kv

import "testing"

func TestRoundTrip(t *testing.T) {
	o := Op{Client: 3, Seq: 17, Kind: Append, Key: "k1", Value: "x3.17"}
	got, err := Decode(o.Encode())
	if err != nil || got != o {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []string{"", "1|2|get|k", "a|2|get|k|", "1|b|get|k|"} {
		if _, err := Decode(bad); err == nil {
			t.Errorf("%q decoded", bad)
		}
	}
}

func TestRetryTakesEffectOnce(t *testing.T) {
	s := New(false)
	a := Op{Client: 1, Seq: 1, Kind: Append, Key: "k", Value: "a"}
	s.Apply(a)
	s.Apply(a) // the retry
	if got := s.Apply(Op{Client: 2, Seq: 1, Kind: Get, Key: "k"}); got != "a" {
		t.Fatalf("got %q", got)
	}
	// The retried get returns its original result, not a fresh read.
	s.Apply(Op{Client: 1, Seq: 2, Kind: Put, Key: "k", Value: "b"})
	if got := s.Apply(Op{Client: 2, Seq: 1, Kind: Get, Key: "k"}); got != "a" {
		t.Fatalf("retried get: %q", got)
	}
}

func TestNoDedupAppliesTwice(t *testing.T) {
	s := New(true)
	a := Op{Client: 1, Seq: 1, Kind: Append, Key: "k", Value: "a"}
	s.Apply(a)
	s.Apply(a)
	if got := s.Apply(Op{Client: 2, Seq: 1, Kind: Get, Key: "k"}); got != "aa" {
		t.Fatalf("got %q", got)
	}
}
