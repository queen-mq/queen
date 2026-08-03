package workload

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Recorder appends one "<prop>,<seq>\n" line per completed message to a
// per-stream file. The file IS the evidence: the verifier reads nothing else,
// so a run can be audited offline by someone who did not run it.
//
// The 1 MB buffered writer behind a single mutex is the shape the July run
// sustained at 150k lines/s; it is kept unchanged rather than "improved" so the
// recording cost is identical on every system under test.
type Recorder struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

// NewRecorder creates <dir>/<topic>_<group>.log.
func NewRecorder(dir, topic, group string) (*Recorder, error) {
	f, err := os.Create(filepath.Join(dir, topic+"_"+group+".log"))
	if err != nil {
		return nil, err
	}
	return &Recorder{w: bufio.NewWriterSize(f, 1<<20), f: f}, nil
}

// Write records one delivered message. Safe from all workers of the group.
func (r *Recorder) Write(prop int, seq int64) {
	var b [32]byte
	n := 0
	n += copy(b[n:], strconv.Itoa(prop))
	b[n] = ','
	n++
	n += copy(b[n:], strconv.FormatInt(seq, 10))
	b[n] = '\n'
	n++
	r.mu.Lock()
	_, _ = r.w.Write(b[:n])
	r.mu.Unlock()
}

// Close flushes and closes. Must run before verification.
func (r *Recorder) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.w.Flush()
	_ = r.f.Close()
}

// RecorderSet holds one Recorder per stage, keyed "<topic>/<group>".
type RecorderSet struct {
	m map[string]*Recorder
}

// NewRecorderSet opens a recorder for every stage of the topology.
func NewRecorderSet(dir string, t Topology) (*RecorderSet, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	rs := &RecorderSet{m: map[string]*Recorder{}}
	for _, s := range t.Stages() {
		r, err := NewRecorder(dir, s.Topic, s.Group)
		if err != nil {
			rs.Close()
			return nil, err
		}
		rs.m[s.Topic+"/"+s.Group] = r
	}
	return rs, nil
}

// For returns the recorder of one stage.
func (rs *RecorderSet) For(topic, group string) *Recorder { return rs.m[topic+"/"+group] }

// Close flushes every recorder.
func (rs *RecorderSet) Close() {
	for _, r := range rs.m {
		r.Close()
	}
}

// ---------------------------------------------------------------------------
// produced.meta — the run's ground truth
// ---------------------------------------------------------------------------

// WriteMeta records, per (flow, property), the highest seq the producer actually
// assigned, plus the run's ackErr and baseSeq. The verifier uses the per-property
// maxima ONLY to report the in-flight tail (never to pass or fail a run), and
// ackErr to decide whether an ordering violation is excusable as a redelivery.
//
// baseSeq is the first seq every property is expected to have delivered: 0 when
// the run pre-warmed the topology, else 1.
func WriteMeta(dir string, maxSeqA, maxSeqB []int64, ackErr int64, baseSeq int64,
	system string, t Topology) error {

	f, err := os.Create(filepath.Join(dir, "produced.meta"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	inv := t.Invariants()
	fmt.Fprintf(w, "# ackErr=%d base=%d system=%s properties=%d rate=%d\n",
		ackErr, baseSeq, system, t.Properties, t.RateEvents)
	fmt.Fprintf(w, "# invariants deliveries=%d lanes=%d publish_native=%d publish_copied=%d\n",
		inv.DeliveriesPerSec, inv.OrderedLanes, inv.PublishNativeFan, inv.PublishCopiedFan)
	// maxSeq slices start at -1 ("nothing produced"), so seq 0 from a warm-up
	// push is a real entry and must not be filtered out as if it were absent.
	for prop, mx := range maxSeqA {
		if mx >= 0 {
			fmt.Fprintf(w, "A %d %d\n", prop, mx)
		}
	}
	for prop, mx := range maxSeqB {
		if mx >= 0 {
			fmt.Fprintf(w, "B %d %d\n", prop, mx)
		}
	}
	return nil
}

// NewMaxSeq allocates a per-property high-water slice initialised to "nothing
// produced yet".
func NewMaxSeq(properties int) []int64 {
	s := make([]int64, properties)
	for i := range s {
		s[i] = -1
	}
	return s
}
