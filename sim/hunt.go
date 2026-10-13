package sim

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// Mutant is a deliberately broken variant of the system. The simulator is
// only worth trusting if it catches every one.
type Mutant struct {
	Name, Description string
	apply             func(*Config)
}

func (m Mutant) Apply(c *Config) { m.apply(c) }

var Mutants = []Mutant{
	{"vote-without-log-check", "grant votes without checking the candidate's log is up to date (§5.4.1)",
		func(c *Config) { c.Bugs.VoteWithoutLogCheck = true }},
	{"commit-previous-term", "commit an earlier term's entry by counting replicas (Figure 8)",
		func(c *Config) { c.Bugs.CommitPreviousTerm = true }},
	{"forget-vote", "do not persist votedFor, so a restarted node can vote twice in a term",
		func(c *Config) { c.Bugs.ForgetVote = true }},
	{"keep-conflicts", "on a conflicting entry, keep the follower's own instead of truncating",
		func(c *Config) { c.Bugs.KeepConflicts = true }},
	{"accept-stale-term", "accept AppendEntries from the leader of an older term",
		func(c *Config) { c.Bugs.AcceptStaleTerm = true }},
	{"no-dedup", "apply a retried client command again instead of answering from the session table",
		func(c *Config) { c.NoDedup = true }},
}

func MutantByName(name string) (Mutant, bool) {
	for _, m := range Mutants {
		if m.Name == name {
			return m, true
		}
	}
	return Mutant{}, false
}

type HuntResult struct {
	Seeds    int      // seeds actually run
	Failures []Result // in seed order
	Kinds    map[string]int
}

// Hunt runs seeds from..from+count-1 on all cores. With stopAtFirst it
// works in seed order, batch by batch, and stops after the batch holding the
// first failure, so "seeds to first failure" does not depend on scheduling.
func Hunt(config func(seed uint64) Config, from uint64, count int, stopAtFirst bool) HuntResult {
	// Runs allocate fast and briefly; collecting less often keeps the cores
	// simulating instead of contending on the collector.
	defer debug.SetGCPercent(debug.SetGCPercent(400))
	workers := runtime.NumCPU()
	batch := count
	if stopAtFirst {
		batch = workers * 4
	}
	h := HuntResult{Kinds: map[string]int{}}
	for start := 0; start < count; start += batch {
		n := min(batch, count-start)
		results := make([]Result, n)
		var wg sync.WaitGroup
		next := make(chan int)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range next {
					r := Run(config(from + uint64(start+i)))
					r.History, r.Timeline = nil, nil
					results[i] = r
				}
			}()
		}
		for i := 0; i < n; i++ {
			next <- i
		}
		close(next)
		wg.Wait()
		h.Seeds += n
		for _, r := range results {
			if r.Violation != nil {
				h.Failures = append(h.Failures, r)
				h.Kinds[r.Violation.Kind]++
			}
		}
		if stopAtFirst && len(h.Failures) > 0 {
			break
		}
	}
	return h
}
