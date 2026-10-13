// Command quorumlab runs the Raft simulator: one seed in detail, many seeds
// in search of a violation, or the deliberately broken variants to show
// the search would find one.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ahmed-hashim-pro/quorum-lab/sim"
)

const version = "0.1.0"

const usage = `quorumlab: Raft under a deterministic fault simulator

usage:
  quorumlab run     [--seed N] [--bug NAME] [--trace] [--svg FILE [--from MS] [--to MS]]
  quorumlab hunt    [--seeds N] [--start N] [--bug NAME]
  quorumlab mutants [--seeds N]
  quorumlab bugs
  quorumlab version

common options:
  --nodes N         cluster size (default 5)
  --no-faults       no partitions or crashes (drops, duplicates and skew remain)

exit codes: 0 no violation (mutants: every bug caught and the correct build clean),
            1 a violation (mutants: a bug missed or the correct build failed), 2 usage`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type common struct {
	nodes    int
	noFaults bool
	bug      string
}

func (c *common) register(fs *flag.FlagSet) {
	fs.IntVar(&c.nodes, "nodes", 5, "cluster size")
	fs.BoolVar(&c.noFaults, "no-faults", false, "no partitions or crashes")
	fs.StringVar(&c.bug, "bug", "", "run a deliberately broken variant (see `quorumlab bugs`)")
}

func (c *common) config() (func(uint64) sim.Config, error) {
	if c.nodes < 1 || c.nodes > 15 {
		return nil, fmt.Errorf("--nodes must be 1..15")
	}
	var m *sim.Mutant
	if c.bug != "" {
		found, ok := sim.MutantByName(c.bug)
		if !ok {
			return nil, fmt.Errorf("unknown bug %q; `quorumlab bugs` lists them", c.bug)
		}
		m = &found
	}
	return func(seed uint64) sim.Config {
		cfg := sim.Default(seed)
		cfg.Nodes = c.nodes
		cfg.Faults = !c.noFaults
		if m != nil {
			m.Apply(&cfg)
		}
		return cfg
	}, nil
}

func run(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, usage)
		return 2
	}
	var err error
	code := 0
	switch args[0] {
	case "run":
		code, err = cmdRun(args[1:], out)
	case "hunt":
		code, err = cmdHunt(args[1:], out)
	case "mutants":
		code, err = cmdMutants(args[1:], out)
	case "bugs":
		for _, m := range sim.Mutants {
			fmt.Fprintf(out, "%-24s %s\n", m.Name, m.Description)
		}
	case "version", "--version":
		fmt.Fprintf(out, "quorumlab %s\n", version)
	case "help", "-h", "--help":
		fmt.Fprintln(out, usage)
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
	if err != nil {
		fmt.Fprintf(errOut, "quorumlab: %v\n", err)
		return 2
	}
	return code
}

func parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

func cmdRun(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var c common
	c.register(fs)
	seed := fs.Uint64("seed", 1, "seed")
	trace := fs.Bool("trace", false, "print every election, fault and violation")
	svg := fs.String("svg", "", "write the timeline as SVG")
	from := fs.Int64("from", 0, "timeline start, ms")
	to := fs.Int64("to", 0, "timeline end, ms (default: end of run)")
	if err := parse(fs, args); err != nil {
		return 0, err
	}
	config, err := c.config()
	if err != nil {
		return 0, err
	}
	cfg := config(*seed)
	cfg.Record = *trace || *svg != ""
	r := sim.Run(cfg)
	if *trace {
		for _, line := range r.Trace {
			fmt.Fprintln(out, line)
		}
		fmt.Fprintln(out)
	}
	s := r.Stats
	label := "correct build"
	if c.bug != "" {
		label = "bug " + c.bug
	}
	fmt.Fprintf(out, "seed %d, %d nodes, %s\n", *seed, cfg.Nodes, label)
	fmt.Fprintf(out, "  %d ms simulated, %d events, %d messages (%d dropped, %d duplicated)\n", s.EndMs, s.Events, s.Messages, s.Dropped, s.Duplicated)
	fmt.Fprintf(out, "  %d partitions, %d crashes, %d leaders elected, highest term %d\n", s.Partitions, s.Crashes, s.Leaders, s.MaxTerm)
	fmt.Fprintf(out, "  %d entries committed; %d client operations completed, %d left pending\n", s.Committed, s.OpsDone, s.OpsPending)
	if *svg != "" {
		title := fmt.Sprintf("Seed %d: %d nodes under partitions, crashes, drops and clock skew", *seed, cfg.Nodes)
		sub := "Every role change as it happened in the simulation; all safety properties checked after every event."
		if r.Violation != nil {
			title = fmt.Sprintf("Seed %d with bug %s", *seed, c.bug)
			sub = r.Violation.Error()
		}
		if err := os.WriteFile(*svg, []byte(r.Timeline.SVG(title, sub, *from, *to, r.Violation)), 0o644); err != nil {
			return 0, err
		}
	}
	if r.Violation != nil {
		fmt.Fprintf(out, "VIOLATION: %v\n", r.Violation)
		if !*trace {
			fmt.Fprintln(out, "\nlast events:")
			tail := r.Trace
			if len(tail) > 12 {
				tail = tail[len(tail)-12:]
			}
			for _, line := range tail {
				fmt.Fprintln(out, "  "+line)
			}
		}
		return 1, nil
	}
	fmt.Fprintln(out, "  no violation: safety held after every event, the history is linearizable, all work finished after healing")
	return 0, nil
}

func cmdHunt(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("hunt", flag.ContinueOnError)
	var c common
	c.register(fs)
	seeds := fs.Int("seeds", 1000, "how many seeds")
	start := fs.Uint64("start", 1, "first seed")
	if err := parse(fs, args); err != nil {
		return 0, err
	}
	if *seeds < 1 {
		return 0, fmt.Errorf("--seeds must be positive")
	}
	config, err := c.config()
	if err != nil {
		return 0, err
	}
	t0 := time.Now()
	h := sim.Hunt(config, *start, *seeds, false)
	fmt.Fprintf(out, "%d seeds (%d..%d) in %.1fs: %d with a violation\n", h.Seeds, *start, *start+uint64(h.Seeds)-1, time.Since(t0).Seconds(), len(h.Failures))
	for _, k := range sortedKinds(h.Kinds) {
		fmt.Fprintf(out, "  %-22s %d\n", k, h.Kinds[k])
	}
	if len(h.Failures) > 0 {
		f := h.Failures[0]
		fmt.Fprintf(out, "first: seed %d, %v\nreplay: quorumlab run --seed %d%s --trace\n", f.Seed, f.Violation, f.Seed, bugFlag(c.bug))
		return 1, nil
	}
	return 0, nil
}

func cmdMutants(args []string, out io.Writer) (int, error) {
	fs := flag.NewFlagSet("mutants", flag.ContinueOnError)
	var c common
	c.register(fs)
	seeds := fs.Int("seeds", 500, "seeds per variant (the correct build runs all of them)")
	if err := parse(fs, args); err != nil {
		return 0, err
	}
	if c.bug != "" {
		return 0, fmt.Errorf("mutants runs every bug; use hunt --bug for one")
	}
	t0 := time.Now()
	code := 0
	fmt.Fprintf(out, "%-24s %-11s %-12s %s\n", "variant", "result", "first seed", "caught as")
	base, err := c.config()
	if err != nil {
		return 0, err
	}
	h := sim.Hunt(base, 1, *seeds, false)
	if len(h.Failures) == 0 {
		fmt.Fprintf(out, "%-24s %-11s %-12s %d seeds, no violation\n", "correct build", "clean", "-", h.Seeds)
	} else {
		code = 1
		fmt.Fprintf(out, "%-24s %-11s %-12d %v\n", "correct build", "FAILED", h.Failures[0].Seed, h.Failures[0].Violation)
	}
	for _, m := range sim.Mutants {
		mc := c
		mc.bug = m.Name
		config, _ := mc.config()
		h := sim.Hunt(config, 1, *seeds, true)
		if len(h.Failures) == 0 {
			code = 1
			fmt.Fprintf(out, "%-24s %-11s %-12s not caught in %d seeds\n", m.Name, "MISSED", "-", h.Seeds)
			continue
		}
		f := h.Failures[0]
		fmt.Fprintf(out, "%-24s %-11s %-12d %s\n", m.Name, "caught", f.Seed, f.Violation.Kind)
	}
	fmt.Fprintf(out, "\n%.1fs. Replay any line with: quorumlab run --seed N --bug NAME --trace\n", time.Since(t0).Seconds())
	return code, nil
}

func bugFlag(bug string) string {
	if bug == "" {
		return ""
	}
	return " --bug " + bug
}

func sortedKinds(m map[string]int) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if m[ks[i]] != m[ks[j]] {
			return m[ks[i]] > m[ks[j]]
		}
		return strings.Compare(ks[i], ks[j]) < 0
	})
	return ks
}
