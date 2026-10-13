package sim

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/ahmed-hashim-pro/quorum-lab/raft"
)

func TestSameSeedSameRun(t *testing.T) {
	for _, seed := range []uint64{1, 7, 42} {
		a, b := Default(seed), Default(seed)
		a.Record, b.Record = true, true
		ra, rb := Run(a), Run(b)
		if !reflect.DeepEqual(ra, rb) {
			t.Fatalf("seed %d: two runs differ", seed)
		}
	}
	if reflect.DeepEqual(Run(Default(1)).Stats, Run(Default(2)).Stats) {
		t.Fatal("different seeds gave identical runs")
	}
}

func TestCorrectBuildHoldsUnderFaults(t *testing.T) {
	seeds := 400
	if testing.Short() {
		seeds = 60
	}
	h := Hunt(Default, 1, seeds, false)
	for _, f := range h.Failures {
		t.Errorf("seed %d: %v", f.Seed, f.Violation)
	}
}

// The runs must actually be hostile, or a clean result means nothing.
func TestFaultsHappen(t *testing.T) {
	var s Stats
	for seed := uint64(1); seed <= 30; seed++ {
		r := Run(Default(seed)).Stats
		s.Partitions += r.Partitions
		s.Crashes += r.Crashes
		s.Leaders += r.Leaders
		s.Dropped += r.Dropped
		s.Duplicated += r.Duplicated
		s.DoubleVotes += r.DoubleVotes
		s.OpsDone += r.OpsDone
	}
	t.Logf("30 seeds: %+v", s)
	if s.Partitions < 100 || s.Crashes < 100 || s.Leaders < 200 || s.Dropped < 10_000 || s.Duplicated < 1000 {
		t.Fatalf("not hostile enough: %+v", s)
	}
	if s.OpsDone != 30*4*60 {
		t.Fatalf("%d client operations completed, want every one", s.OpsDone)
	}
}

// Every deliberate bug must be found, within a stated number of seeds. The
// bounds are about twice the first failing seed today; a change to the
// simulator that makes it blind to one of these fails here.
func TestEveryBugIsCaught(t *testing.T) {
	bounds := map[string]int{
		"vote-without-log-check": 50,
		"commit-previous-term":   600,
		"forget-vote":            1500,
		"keep-conflicts":         50,
		"accept-stale-term":      50,
		"no-dedup":               50,
	}
	for _, m := range Mutants {
		m := m
		t.Run(m.Name, func(t *testing.T) {
			bound := bounds[m.Name]
			if testing.Short() && bound > 200 {
				t.Skip("long; run without -short")
			}
			h := Hunt(func(seed uint64) Config {
				c := Default(seed)
				m.Apply(&c)
				return c
			}, 1, bound, true)
			if len(h.Failures) == 0 {
				t.Fatalf("not caught in %d seeds", bound)
			}
			t.Logf("caught at seed %d: %v", h.Failures[0].Seed, h.Failures[0].Violation)
		})
	}
	if len(bounds) != len(Mutants) {
		t.Fatal("a mutant has no bound")
	}
}

// Without faults, partitions or drops, a cluster elects one leader and
// keeps it: the faults are what make the runs interesting, not noise in the
// harness.
func TestQuietNetworkKeepsOneLeader(t *testing.T) {
	c := Default(3)
	c.Faults, c.DropRate, c.DupRate, c.Skew = false, 0, 0, 0
	c.ElectionTicks, c.HeartbeatTicks, c.MaxDelayMs = 15, 3, 12
	r := Run(c)
	if r.Violation != nil || r.Stats.Leaders != 1 || r.Stats.MaxTerm != 1 {
		t.Fatalf("%v, %d leaders, max term %d", r.Violation, r.Stats.Leaders, r.Stats.MaxTerm)
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := Default(5)
	c.Nodes, c.Faults = 1, false
	r := Run(c)
	if r.Violation != nil || r.Stats.OpsDone != c.Clients*c.OpsPerClient {
		t.Fatalf("%v, %d ops", r.Violation, r.Stats.OpsDone)
	}
}

func TestTimelineSVG(t *testing.T) {
	c := Default(4)
	c.Record = true
	r := Run(c)
	svg := r.Timeline.SVG("title", "sub", 0, 0, r.Violation)
	if !strings.HasPrefix(svg, "<svg") || !strings.Contains(svg, "</svg>") {
		t.Fatal("not an svg")
	}
	for id := 1; id <= c.Nodes; id++ {
		if !strings.Contains(svg, fmt.Sprintf(">node %d<", id)) {
			t.Fatalf("no lane for node %d", id)
		}
	}
	leaders := 0
	for _, s := range r.Timeline.Segments {
		if s.Role == raft.Leader && !s.Down {
			leaders++
		}
		if s.To < s.From {
			t.Fatalf("segment ends before it starts: %+v", s)
		}
	}
	if leaders == 0 {
		t.Fatal("timeline has no leader")
	}
}
