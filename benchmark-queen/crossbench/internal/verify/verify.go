// Package verify proves the correctness contract of SPEC.md §3 from the stage
// logs alone.
//
// It is a faithful port of the verifier that certified the July 2026 Queen run
// (goload/cm.go). The semantics are deliberately NOT "improved": the whole point
// is that the same judge, unchanged, is applied to every system under test, and
// that a Queen result from this campaign is comparable with the July one.
//
// It reads only <dir>/<topic>_<group>.log plus <dir>/produced.meta, so any
// third party can re-run it over a published log directory and reach the same
// verdict without trusting the harness that produced them.
package verify

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// StageResult is the verdict for one consumer stream.
type StageResult struct {
	File     string
	Flow     string
	Msgs     int64
	Unique   int64
	Dups     int64
	Gaps     int64 // holes below the highest delivered seq = real losses
	Viols    int64 // first-occurrence order violations
	Inflight int64 // producedMax - maxSeen, summed over properties (tail, OK)
	Props    int
	Pass     bool
	Fail     bool
}

// Report is the whole run's verdict.
type Report struct {
	Stages   []StageResult
	Msgs     int64
	Unique   int64
	Dups     int64
	Gaps     int64
	Viols    int64
	Inflight int64
	AckErr   int64
	BaseSeq  int64
	Pass     bool

	// EmptyStreams names streams that recorded NOTHING. An empty log has no
	// gaps and no violations, so without this check a completely broken
	// pipeline verifies as a clean PASS — which is exactly what happened on
	// 2026-08-02 when a pgmq statement silently inserted no rows. A stream that
	// received nothing is a failed run, not a perfect one.
	EmptyStreams []string
}

type propAcc struct {
	seen     map[int64]struct{}
	maxFirst int64 // highest first-occurrence seq so far (order check)
	maxSeen  int64
}

// Stage names one consumer stream to verify.
type Stage struct {
	Topic string
	Group string
	Flow  string
}

// File streams one stage log and judges it.
//
// produced maps property -> the ground-truth max seq assigned for this file's
// flow; it is used ONLY to report the in-flight tail and never affects the
// verdict. baseSeq is the first seq every property is expected to have
// delivered. ackErr is the run's ack-failure count.
func File(path, flow string, produced map[int]int64, ackErr, baseSeq int64) (StageResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return StageResult{File: filepath.Base(path), Flow: flow}, err
	}
	defer f.Close()
	return parse(f, filepath.Base(path), flow, produced, ackErr, baseSeq)
}

func parse(r io.Reader, name, flow string, produced map[int]int64, ackErr, baseSeq int64) (StageResult, error) {
	accs := make(map[int]*propAcc)
	res := StageResult{File: name, Flow: flow}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		comma := strings.IndexByte(line, ',')
		if comma <= 0 {
			continue
		}
		prop, e1 := strconv.Atoi(line[:comma])
		seq, e2 := strconv.ParseInt(line[comma+1:], 10, 64)
		if e1 != nil || e2 != nil {
			continue
		}
		res.Msgs++
		a := accs[prop]
		if a == nil {
			a = &propAcc{seen: make(map[int64]struct{})}
			accs[prop] = a
		}
		if _, dup := a.seen[seq]; dup {
			res.Dups++
			continue // redelivery: dedup by first occurrence before the order check
		}
		a.seen[seq] = struct{}{}
		res.Unique++
		if seq < a.maxFirst {
			res.Viols++ // first occurrence of a lower seq after a higher one
		}
		if seq > a.maxFirst {
			a.maxFirst = seq
		}
		if seq > a.maxSeen {
			a.maxSeen = seq
		}
	}
	if err := sc.Err(); err != nil {
		return res, err
	}

	res.Props = len(accs)
	for prop, a := range accs {
		// Real losses: any seq in [baseSeq, maxSeen] never delivered — a HIGHER
		// seq arrived, so the missing one was not merely in flight. All seen
		// seqs are distinct and in [baseSeq, maxSeen], so the expected span is
		// maxSeen-baseSeq+1 and the shortfall against |seen| is the loss count.
		//
		// CLAMPED AT 0 PER PROPERTY, and that clamp is load-bearing: without it
		// a negative term (from a seq below baseSeq, e.g. verifying warm-up logs
		// against a meta that defaults baseSeq=1, or cross-run contamination)
		// would be summed into Gaps and could CANCEL a genuine gap in another
		// property, flipping an aggregate FAIL to PASS. That is a verifier
		// soundness hole, not a cosmetic detail.
		if g := a.maxSeen - baseSeq + 1 - int64(len(a.seen)); g > 0 {
			res.Gaps += g
		}
		if pm, ok := produced[prop]; ok && pm > a.maxSeen {
			res.Inflight += pm - a.maxSeen
		}
	}

	// A gap below the frontier is ALWAYS fatal. An ordering violation is fatal
	// UNLESS acks failed during the run, in which case a redelivery may
	// legitimately have reordered the stream: reported, not fatal.
	res.Fail = res.Gaps > 0 || (res.Viols > 0 && ackErr == 0)
	res.Pass = !res.Fail
	return res, nil
}

// Meta is the ground truth written by the run.
type Meta struct {
	ProducedA map[int]int64
	ProducedB map[int]int64
	AckErr    int64
	BaseSeq   int64
	Header    map[string]string
}

// LoadMeta reads produced.meta. A missing file is not an error: the verifier
// still judges gaps and order, it just cannot report the in-flight tail.
func LoadMeta(dir string) Meta {
	m := Meta{
		ProducedA: map[int]int64{},
		ProducedB: map[int]int64{},
		BaseSeq:   1,
		Header:    map[string]string{},
	}
	f, err := os.Open(filepath.Join(dir, "produced.meta"))
	if err != nil {
		return m
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			for _, tok := range strings.Fields(line) {
				k, v, ok := strings.Cut(tok, "=")
				if !ok {
					continue
				}
				m.Header[k] = v
				switch k {
				case "ackErr":
					m.AckErr, _ = strconv.ParseInt(v, 10, 64)
				case "base":
					m.BaseSeq, _ = strconv.ParseInt(v, 10, 64)
				}
			}
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 3 {
			continue
		}
		prop, _ := strconv.Atoi(fs[1])
		mx, _ := strconv.ParseInt(fs[2], 10, 64)
		switch fs[0] {
		case "A":
			m.ProducedA[prop] = mx
		case "B":
			m.ProducedB[prop] = mx
		}
	}
	return m
}

// Run verifies every stage in dir and returns the aggregate report.
// ackErr overrides the value in produced.meta when haveOverride is set (the
// live run knows its own ack failures; -verify-only takes them from the meta).
func Run(dir string, stages []Stage, ackErrOverride int64, haveOverride bool) (Report, error) {
	meta := LoadMeta(dir)
	ackErr := meta.AckErr
	if haveOverride {
		ackErr = ackErrOverride
	}

	rep := Report{Pass: true, AckErr: ackErr, BaseSeq: meta.BaseSeq}
	for _, s := range stages {
		produced := meta.ProducedA
		if s.Flow == "B" {
			produced = meta.ProducedB
		}
		path := filepath.Join(dir, s.Topic+"_"+s.Group+".log")
		r, err := File(path, s.Flow, produced, ackErr, meta.BaseSeq)
		if err != nil {
			r.Fail = true
			r.Pass = false
			rep.Stages = append(rep.Stages, r)
			rep.Pass = false
			continue
		}
		if r.Msgs == 0 {
			// Nothing delivered: the stream is broken, not clean.
			r.Fail = true
			r.Pass = false
			rep.EmptyStreams = append(rep.EmptyStreams, s.Topic+"_"+s.Group)
		}
		rep.Stages = append(rep.Stages, r)
		rep.Msgs += r.Msgs
		rep.Unique += r.Unique
		rep.Dups += r.Dups
		rep.Gaps += r.Gaps
		rep.Viols += r.Viols
		rep.Inflight += r.Inflight
		if r.Fail {
			rep.Pass = false
		}
	}
	return rep, nil
}

// Print writes the verdict table to w.
func (rep Report) Print(w io.Writer, dir string) {
	fmt.Fprintf(w, "\n=== VERIFIER (dir=%s, ackErr=%d, baseSeq=%d) ===\n", dir, rep.AckErr, rep.BaseSeq)
	fmt.Fprintf(w, "%-30s %10s %10s %8s %8s %8s %10s  %s\n",
		"stage(topic_group)", "msgs", "unique", "dups", "gaps", "viols", "inflight", "verdict")
	for _, r := range rep.Stages {
		verdict := "PASS"
		switch {
		case r.Msgs == 0:
			verdict = "FAIL(empty)"
		case r.Fail:
			verdict = "FAIL"
		case r.Viols > 0:
			verdict = "PASS(viol~redeliv)"
		case r.Dups > 0:
			verdict = "PASS(dups)"
		}
		fmt.Fprintf(w, "%-30s %10d %10d %8d %8d %8d %10d  %s\n",
			strings.TrimSuffix(r.File, ".log"), r.Msgs, r.Unique, r.Dups, r.Gaps, r.Viols, r.Inflight, verdict)
	}
	fmt.Fprintf(w, "%-30s %10d %10d %8d %8d %8d %10d\n",
		"TOTAL", rep.Msgs, rep.Unique, rep.Dups, rep.Gaps, rep.Viols, rep.Inflight)
	if len(rep.EmptyStreams) > 0 {
		fmt.Fprintf(w, "EMPTY STREAMS (%d): %s\n",
			len(rep.EmptyStreams), strings.Join(rep.EmptyStreams, ", "))
		fmt.Fprintf(w, "  a stream that received nothing cannot be judged clean — the pipeline is broken\n")
	}
	if rep.Pass {
		fmt.Fprintf(w, "VERDICT: PASS  (0 gaps below frontier, %d order-violations, %d dups, %d in-flight at cutoff)\n",
			rep.Viols, rep.Dups, rep.Inflight)
	} else {
		fmt.Fprintf(w, "VERDICT: FAIL  (gaps=%d order-violations=%d empty-streams=%d — see per-stage rows above)\n",
			rep.Gaps, rep.Viols, len(rep.EmptyStreams))
	}
}
