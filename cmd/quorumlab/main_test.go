package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func call(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestRunCleanSeed(t *testing.T) {
	code, out, _ := call("run", "--seed", "3")
	if code != 0 || !strings.Contains(out, "no violation") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

func TestRunReplaysAViolation(t *testing.T) {
	code, out, _ := call("run", "--seed", "258", "--bug", "commit-previous-term")
	if code != 1 || !strings.Contains(out, "VIOLATION: leader completeness") || !strings.Contains(out, "last events:") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	// Deterministic: the same seed tells the same story.
	_, again, _ := call("run", "--seed", "258", "--bug", "commit-previous-term")
	if again != out {
		t.Fatal("a replay differed")
	}
}

func TestTraceAndSVG(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.svg")
	code, out, _ := call("run", "--seed", "2", "--trace", "--svg", path, "--from", "1000", "--to", "4000")
	if code != 0 || !strings.Contains(out, "is leader of term") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	b, err := os.ReadFile(path)
	if err != nil || !bytes.HasPrefix(b, []byte("<svg")) {
		t.Fatalf("svg: %v", err)
	}
}

func TestHunt(t *testing.T) {
	code, out, _ := call("hunt", "--seeds", "20")
	if code != 0 || !strings.Contains(out, "20 seeds (1..20)") || !strings.Contains(out, ": 0 with a violation") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	code, out, _ = call("hunt", "--seeds", "10", "--bug", "no-dedup")
	if code != 1 || !strings.Contains(out, "replay: quorumlab run --seed") || !strings.Contains(out, "--bug no-dedup") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}

func TestBugsList(t *testing.T) {
	_, out, _ := call("bugs")
	for _, name := range []string{"vote-without-log-check", "commit-previous-term", "forget-vote", "keep-conflicts", "accept-stale-term", "no-dedup"} {
		if !strings.Contains(out, name) {
			t.Errorf("bugs does not list %s", name)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"run", "--bug", "nope"},
		{"run", "--seed", "x"},
		{"run", "extra"},
		{"run", "--nodes", "0"},
		{"hunt", "--seeds", "0"},
		{"mutants", "--bug", "no-dedup"},
	} {
		if code, _, _ := call(args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
}
