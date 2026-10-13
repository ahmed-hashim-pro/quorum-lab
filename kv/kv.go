// Package kv is the replicated state machine: a key-value store with
// client sessions, so a retried command takes effect once.
//
// Clients retry. A command whose reply was lost may already be in the log,
// and a retry adds it again. Without the session table, an append would then
// happen twice, and the history would not be linearizable. The table
// remembers each client's last sequence number and its result.
package kv

import (
	"fmt"
	"strconv"
	"strings"
)

type Kind string

const (
	Get    Kind = "get"
	Put    Kind = "put"
	Append Kind = "append"
)

// Op is one client command. Seq increases by one per new command from a
// client; a retry reuses it.
type Op struct {
	Client int
	Seq    int
	Kind   Kind
	Key    string
	Value  string
}

// Encode turns an op into log entry data. Keys and values must not contain '|'.
func (o Op) Encode() string {
	return fmt.Sprintf("%d|%d|%s|%s|%s", o.Client, o.Seq, o.Kind, o.Key, o.Value)
}

func Decode(s string) (Op, error) {
	f := strings.Split(s, "|")
	if len(f) != 5 {
		return Op{}, fmt.Errorf("kv: bad entry %q", s)
	}
	c, err1 := strconv.Atoi(f[0])
	q, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil {
		return Op{}, fmt.Errorf("kv: bad entry %q", s)
	}
	return Op{Client: c, Seq: q, Kind: Kind(f[2]), Key: f[3], Value: f[4]}, nil
}

type session struct {
	seq    int
	result string
}

type Store struct {
	data     map[string]string
	sessions map[int]session
	// NoDedup is a deliberate bug: apply retried commands again.
	NoDedup bool
}

func New(noDedup bool) *Store {
	return &Store{data: map[string]string{}, sessions: map[int]session{}, NoDedup: noDedup}
}

// Apply executes an op and returns its result: the value for a get, "" otherwise.
func (s *Store) Apply(o Op) string {
	if last, ok := s.sessions[o.Client]; ok && o.Seq <= last.seq && !s.NoDedup {
		return last.result
	}
	var result string
	switch o.Kind {
	case Get:
		result = s.data[o.Key]
	case Put:
		s.data[o.Key] = o.Value
	case Append:
		s.data[o.Key] += o.Value
	}
	s.sessions[o.Client] = session{seq: o.Seq, result: result}
	return result
}
