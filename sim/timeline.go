package sim

import (
	"fmt"
	"math"
	"strings"

	"github.com/ahmed-hashim-pro/quorum-lab/raft"
)

// Timeline is what a run looked like: each node's role and term over time,
// when it was down, and when the network was split.
type Timeline struct {
	Nodes      int
	ChaosMs    int64
	EndMs      int64
	Segments   []Segment
	Partitions []Partition
	Violation  int64 // ms, or -1
}

// Segment is a stretch of time one node spent in one role and term, or down.
type Segment struct {
	Node     raft.ID
	From, To int64
	Role     raft.Role
	Term     uint64
	Down     bool
}

type Partition struct {
	From, To int64
	Label    string
	// Cutoff[id]: the node could not reach a majority (index 0 unused).
	Cutoff []bool
}

func (t *Timeline) open(n raft.ID) *Segment {
	for i := len(t.Segments) - 1; i >= 0; i-- {
		if t.Segments[i].Node == n {
			if t.Segments[i].To < 0 {
				return &t.Segments[i]
			}
			return nil
		}
	}
	return nil
}

func (t *Timeline) role(at int64, n raft.ID, role raft.Role, term uint64) {
	if s := t.open(n); s != nil {
		s.To = at
	}
	t.Segments = append(t.Segments, Segment{Node: n, From: at, To: -1, Role: role, Term: term})
}

func (t *Timeline) down(at int64, n raft.ID) {
	if s := t.open(n); s != nil {
		s.To = at
	}
	t.Segments = append(t.Segments, Segment{Node: n, From: at, To: -1, Down: true})
}

func (t *Timeline) partition(at int64, label string, cutoff []bool) {
	if k := len(t.Partitions); k > 0 && t.Partitions[k-1].To < 0 {
		t.Partitions[k-1].To = at
	}
	if label != "" {
		t.Partitions = append(t.Partitions, Partition{From: at, To: -1, Label: label, Cutoff: cutoff})
	}
}

func (t *Timeline) close(at int64) {
	t.EndMs = at
	for i := range t.Segments {
		if t.Segments[i].To < 0 {
			t.Segments[i].To = at
		}
	}
	t.partition(at, "", nil)
}

const (
	ink   = "#24292f"
	muted = "#57606a"
	grid  = "#d8dee4"
	font  = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
)

// Role fills: follower recedes, candidate and leader stand out, in the
// order of a validated categorical palette.
var roleFill = map[raft.Role]string{
	raft.Follower:  "#d8dee4",
	raft.Candidate: "#eb6834",
	raft.Leader:    "#2a78d6",
}

// SVG draws the timeline from fromMs to toMs: one lane per node, coloured by
// role, leader stretches labelled with their term, crashes hatched,
// partitions shaded with each side's nodes listed.
func (t *Timeline) SVG(title, subtitle string, fromMs, toMs int64, violation *Violation) string {
	if toMs <= fromMs {
		fromMs, toMs = 0, t.EndMs
	}
	const (
		left, right, top, lane, gap = 70.0, 24.0, 92.0, 30.0, 10.0
		width                       = 1000.0
	)
	plotW := width - left - right
	plotH := float64(t.Nodes)*(lane+gap) - gap
	height := top + plotH + 96
	x := func(ms int64) float64 {
		return left + float64(ms-fromMs)/float64(toMs-fromMs)*plotW
	}
	clip := func(a, b int64) (int64, int64, bool) {
		a, b = max(a, fromMs), min(b, toMs)
		return a, b, b > a
	}
	laneY := func(n raft.ID) float64 { return top + float64(int(n)-1)*(lane+gap) }

	var b strings.Builder
	f := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	f(`<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" font-family="%s">`+"\n", width, height, width, height, font)
	f(`<defs><pattern id="down" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="6" height="6" fill="#ffffff"/><line x1="0" y1="0" x2="0" y2="6" stroke="#8c959f" stroke-width="1.5"/></pattern></defs>` + "\n")
	f(`<rect width="100%%" height="100%%" fill="#ffffff"/>` + "\n")
	f(`<text x="24" y="30" font-size="17" font-weight="600" fill="%s">%s</text>`+"\n", ink, esc(title))
	f(`<text x="24" y="50" font-size="12.5" fill="%s">%s</text>`+"\n", muted, esc(subtitle))

	// Partitions: a band behind the lanes, nodes cut off from the majority side dimmed.
	for _, p := range t.Partitions {
		a, z, ok := clip(p.From, p.To)
		if !ok {
			continue
		}
		f(`<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="#fff8c5"/>`+"\n", x(a), top-14, x(z)-x(a), plotH+20)
		for id := 1; id <= t.Nodes; id++ {
			if p.Cutoff[id] {
				f(`<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="#d4a72c" fill-opacity="0.35"/>`+"\n", x(a), laneY(raft.ID(id))-3, x(z)-x(a), lane+6)
			}
		}
	}
	// Time grid.
	step := niceStep(float64(toMs-fromMs), 10)
	first := int64(math.Ceil(float64(fromMs)/step) * step)
	for at := first; at <= toMs; at += int64(step) {
		f(`<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s"/>`+"\n", x(at), top-14, x(at), top+plotH+6, grid)
		f(`<text x="%.1f" y="%.1f" font-size="11" text-anchor="middle" fill="%s">%.1f s</text>`+"\n", x(at), top+plotH+22, muted, float64(at)/1000)
	}
	if t.ChaosMs > fromMs && t.ChaosMs < toMs {
		f(`<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1.5" stroke-dasharray="5 3"/>`+"\n", x(t.ChaosMs), top-24, x(t.ChaosMs), top+plotH+6, ink)
		f(`<text x="%.1f" y="%.1f" font-size="11" fill="%s">network heals</text>`+"\n", x(t.ChaosMs)+5, top-18, ink)
	}
	for id := 1; id <= t.Nodes; id++ {
		f(`<text x="%.1f" y="%.1f" font-size="12.5" font-weight="600" text-anchor="end" fill="%s">node %d</text>`+"\n", left-10, laneY(raft.ID(id))+lane/2+4, ink, id)
	}
	for _, s := range t.Segments {
		a, z, ok := clip(s.From, s.To)
		if !ok {
			continue
		}
		y := laneY(s.Node)
		if s.Down {
			f(`<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="url(#down)" stroke="#8c959f" stroke-width="0.6"/>`+"\n", x(a), y+6, x(z)-x(a), lane-12)
			continue
		}
		h, dy := lane-12, 6.0
		if s.Role == raft.Leader {
			h, dy = lane, 0
		}
		f(`<rect x="%.1f" y="%.1f" width="%.2f" height="%.1f" fill="%s"/>`+"\n", x(a), y+dy, max(x(z)-x(a), 0.8), h, roleFill[s.Role])
		if s.Role == raft.Leader && x(z)-x(a) > 26 {
			f(`<text x="%.1f" y="%.1f" font-size="11" font-weight="600" text-anchor="middle" fill="#ffffff">t%d</text>`+"\n", (x(a)+x(z))/2, y+lane/2+4, s.Term)
		}
	}
	if violation != nil && violation.At >= fromMs && violation.At <= toMs {
		vx := x(violation.At)
		f(`<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#cf222e" stroke-width="2.5"/>`+"\n", vx, top-24, vx, top+plotH+6)
		anchor, dx := "start", 6.0
		if vx > left+plotW*0.6 {
			anchor, dx = "end", -6
		}
		f(`<text x="%.1f" y="%.1f" font-size="12" font-weight="600" text-anchor="%s" fill="#cf222e">%s</text>`+"\n", vx+dx, top-18, anchor, esc(violation.Kind+" violated"))
	}

	// Legend.
	ly := top + plotH + 52
	lx := left
	item := func(sw, label string) {
		f(`<g transform="translate(%.1f,%.1f)">%s</g>`+"\n", lx, ly-6, sw)
		f(`<text x="%.1f" y="%.1f" font-size="12" fill="%s">%s</text>`+"\n", lx+30, ly+4, muted, esc(label))
		lx += 30 + 6.6*float64(len(label)) + 22
	}
	item(`<rect width="22" height="12" fill="#2a78d6"/>`, "leader (tN = term)")
	item(`<rect y="2" width="22" height="8" fill="#eb6834"/>`, "candidate")
	item(`<rect y="2" width="22" height="8" fill="#d8dee4"/>`, "follower")
	item(`<rect width="22" height="12" fill="url(#down)" stroke="#8c959f" stroke-width="0.6"/>`, "crashed")
	item(`<rect width="22" height="12" fill="#fff8c5"/><rect y="2" width="22" height="8" fill="#d4a72c" fill-opacity="0.35"/>`, "partitioned: node cannot reach a majority")
	b.WriteString("</svg>\n")
	return b.String()
}

func niceStep(span, target float64) float64 {
	raw := span / target
	p := 1.0
	for p*10 <= raw {
		p *= 10
	}
	for p > raw {
		p /= 10
	}
	for _, m := range []float64{1, 2, 5, 10} {
		if m*p >= raw {
			return m * p
		}
	}
	return 10 * p
}

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;").Replace(s)
}
