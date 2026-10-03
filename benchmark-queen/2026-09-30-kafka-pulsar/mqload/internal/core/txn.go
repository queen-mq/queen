package core

// Transactional pipeline mode (-txn), shared by kload, pload and qload so the three systems run the same
// exactly-once consume-transform-produce workload (txn/run.sh runs it point by point):
//
//   feeder   the core producer (pacer, units, shedding) writes <prefix>-in, every message stamped
//            {"ts":<scheduled µs>,"src":<loader index>,"seq":<n>,...}: (src, seq) is unique per run.
//   workers  read <prefix>-in, transform every message (Transform: one field appended) and in ONE transaction
//            write the results to <prefix>-out AND commit the input position; up to -txn-size messages per
//            transaction, waiting at most -txn-linger for a transaction to fill.
//   readers  read <prefix>-out read-committed: e2e = receive - scheduled send of the INPUT (the core
//            ConsumerStats, the standard window line).
//   ledger   every launched message's fate (broker confirmed / failed / never answered) -> -ids-out.
//   verify   (a separate pass after the run, -verify) reads <prefix>-out in full and the unprocessed rest of
//            <prefix>-in and checks every confirmed input id: in out exactly once, or still pending in in.
//
// Lines: "[txn] HH:MM:SS ..." after every window line, "[txn-final] ..." after [final], "[verify] ..." from
// -verify. None of them matches the window/[final] regexes of report.py or the 09-29 grid_report.py.

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TxnConfig holds the -txn flags common to the three loaders.
type TxnConfig struct {
	On         bool
	Size       int
	Linger     time.Duration
	Readers    int // e2e readers per process (-1 = -consumers)
	IdsOut     string
	Verify     bool
	IdsDir     string
	VerifyIdle time.Duration
	VerifyMax  time.Duration
	IdleExit   time.Duration // the drain ends early once workers and readers of this process were idle this long
}

// RegisterTxn adds the -txn flags to fs.
func (t *TxnConfig) RegisterTxn(fs *flag.FlagSet) {
	fs.BoolVar(&t.On, "txn", false, "transactional consume-transform-produce pipeline: feeder -> <topic>-in -> workers (one transaction = output to <topic>-out + input position) -> read-committed readers of <topic>-out")
	fs.IntVar(&t.Size, "txn-size", 10, "messages per transaction (a worker waits up to -txn-linger for a transaction to fill)")
	DurationVar(fs, &t.Linger, "txn-linger", time.Second, "longest wait for a transaction to fill to -txn-size after its first message (then it commits what it has)")
	fs.IntVar(&t.Readers, "readers", -1, "-txn: read-committed e2e readers of <topic>-out in this process (-1 = -consumers, same global indices as the workers)")
	fs.StringVar(&t.IdsOut, "ids-out", "", "-txn: write this process's id ledger (every launched message: confirmed / failed / unanswered) here at the end")
	fs.BoolVar(&t.Verify, "verify", false, "verifier pass: read <topic>-out in full (read committed) and the unprocessed rest of <topic>-in, check every id of the ledgers in -ids-dir, print [verify] and exit")
	fs.StringVar(&t.IdsDir, "ids-dir", "", "-verify: directory holding the processes' *.ids ledgers")
	DurationVar(fs, &t.VerifyIdle, "verify-idle", 10*time.Second, "-verify: a scan ends after this long without a new record")
	DurationVar(fs, &t.VerifyMax, "verify-max", 20*time.Minute, "-verify: give up a scan after this long")
	DurationVar(fs, &t.IdleExit, "idle-exit", 3*time.Second, "-txn drain: end early once this process's workers and readers received nothing for this long (and no transaction is open)")
}

// FinalizeTxn validates the -txn flags against the common config.
func (t *TxnConfig) FinalizeTxn(c *Config) error {
	if !t.On && !t.Verify {
		return nil
	}
	if c.Topics != 1 {
		return fmt.Errorf("-txn/-verify support -topics 1 only (topics <topic>-in and <topic>-out)")
	}
	if c.Entities != 0 {
		return fmt.Errorf("-txn/-verify support -entities 0 only (explicit partitions)")
	}
	if t.Size < 1 {
		return fmt.Errorf("-txn-size must be >= 1")
	}
	if t.Readers < 0 {
		t.Readers = c.Consumers
	}
	if t.Verify && t.IdsDir == "" {
		return fmt.Errorf("-verify needs -ids-dir")
	}
	return nil
}

// ReaderIndex returns the global index and the total of reader i of this process: the workers' indices when there
// are as many readers as consumers, else loader-major blocks.
func (t *TxnConfig) ReaderIndex(c *Config, i int) (ci, total int) {
	if t.Readers == c.Consumers {
		return c.ConsOffset + i, c.ConsTotal
	}
	return c.LoaderIndex*t.Readers + i, c.Loaders * t.Readers
}

// InTopic and OutTopic name the pipeline's two topics/queues.
func InTopic(c *Config) string  { return c.Topic + "-in" }
func OutTopic(c *Config) string { return c.Topic + "-out" }

// ---------------------------------------------------------------------------
// payload: the per-message id and the transform

// StampSeq is Stamp with the message's run-unique id: {"ts":<µs>,"src":<n>,"seq":<first+j>,<event>.
func (p *PayloadPool) StampSeq(schedMicros int64, n int, firstSeq int64) [][]byte {
	b := p.idx.Add(1) % PoolBatches
	raws := p.raws[b]
	if n > len(raws) {
		n = len(raws)
	}
	var hb [48]byte
	h := append(hb[:0], `{"ts":`...)
	h = strconv.AppendInt(h, schedMicros, 10)
	h = append(h, p.srcPart[:len(p.srcPart)-1]...) // `,"src":<n>` without the trailing comma
	size := 0
	for j := 0; j < n; j++ {
		size += len(h) + len(`,"seq":`) + 20 + len(raws[j])
	}
	slab := make([]byte, 0, size)
	out := make([][]byte, n)
	for j := 0; j < n; j++ {
		start := len(slab)
		slab = append(slab, h...)
		slab = append(slab, `,"seq":`...)
		slab = strconv.AppendInt(slab, firstSeq+int64(j), 10)
		slab = append(slab, ',')
		slab = append(slab, raws[j][1:]...)
		out[j] = slab[start:len(slab):len(slab)]
	}
	return out
}

// ParseSeq reads ts, src and seq from the head of a -txn message ({"ts":..,"src":..,"seq":..,). ok is false for
// anything else (warm-up messages, a plain-mode message without "seq").
func ParseSeq(v []byte) (ts int64, src int, seq int64, ok bool) {
	ts, src, ok = ParseStamp(v)
	if !ok || src < 0 {
		return 0, 0, 0, false
	}
	// skip `{"ts":<digits>,"src":<digits>`
	i := 6
	for i < len(v) && v[i] >= '0' && v[i] <= '9' {
		i++
	}
	i += len(`,"src":`)
	for i < len(v) && v[i] >= '0' && v[i] <= '9' {
		i++
	}
	const key = `,"seq":`
	if len(v) < i+len(key)+1 || string(v[i:i+len(key)]) != key {
		return 0, 0, 0, false
	}
	i += len(key)
	j := i
	for i < len(v) && v[i] >= '0' && v[i] <= '9' {
		seq = seq*10 + int64(v[i]-'0')
		i++
	}
	if i == j {
		return 0, 0, 0, false
	}
	return ts, src, seq, true
}

// Transform is the pipeline's processing step, identical for every system: the input JSON with one field
// appended, ,"by":<worker>} (the worker's global index). The head, and so ts/src/seq, is unchanged.
func Transform(v []byte, worker int) []byte {
	n := len(v)
	for n > 0 && (v[n-1] == ' ' || v[n-1] == '\n' || v[n-1] == '\r' || v[n-1] == '\t') {
		n--
	}
	if n == 0 || v[n-1] != '}' {
		out := make([]byte, n)
		copy(out, v[:n])
		return out
	}
	out := make([]byte, 0, n+16)
	out = append(out, v[:n-1]...)
	if n > 2 {
		out = append(out, `,"by":`...)
	} else {
		out = append(out, `"by":`...)
	}
	out = strconv.AppendInt(out, int64(worker), 10)
	return append(out, '}')
}

// ---------------------------------------------------------------------------
// transaction statistics

// TxnStats counts this process's transactions (all workers). Begin -> commit acknowledged is the commit latency.
type TxnStats struct {
	commitLat *olHist
	ok        atomic.Int64 // committed transactions
	msgs      atomic.Int64 // messages in committed transactions
	aborts    atomic.Int64 // transactions that ended without committing (rolled back, aborted, refused, failed)
	errs      atomic.Int64 // of those, the ones that ended with an error (transport, client state, unknown outcome)
	in        atomic.Int64 // messages received by the workers (redeliveries included)
	open      atomic.Int64 // transactions in progress right now
	lastIn    atomic.Int64 // unix ns of the last message received by a worker
	lastRead  atomic.Int64 // unix ns of the last message received by a reader
	run       *Run
}

// EnableTxn turns on the transaction statistics (the [txn] lines) and the id ledger (written to idsOut by Finish
// when not empty). Call it before Produce.
func (r *Run) EnableTxn(idsOut string) *TxnStats {
	t := &TxnStats{commitLat: newOLHist(), run: r}
	r.txn = t
	r.ids = &IdLedger{}
	r.idsOut = idsOut
	return t
}

// Txn returns the transaction statistics (nil outside -txn).
func (r *Run) Txn() *TxnStats { return r.txn }

// Received counts n input messages received by a worker.
func (t *TxnStats) Received(n int) {
	if n > 0 {
		t.in.Add(int64(n))
		t.lastIn.Store(time.Now().UnixNano())
	}
}

// Begin marks a transaction open (for the idle test) and returns its start instant.
func (t *TxnStats) Begin() time.Time {
	t.open.Add(1)
	return time.Now()
}

// Committed records a transaction of n messages begun at start that committed (commit latency = start -> the
// commit acknowledged).
func (t *TxnStats) Committed(start time.Time, n int) {
	t.open.Add(-1)
	t.commitLat.record(time.Since(start).Microseconds())
	t.ok.Add(1)
	t.msgs.Add(int64(n))
}

// Aborted records a transaction that ended without committing on a clean verdict (rolled back, aborted, refused
// by the broker); why is kept in the error summary under kind.
func (t *TxnStats) Aborted(kind string, why error) {
	t.open.Add(-1)
	t.aborts.Add(1)
	if why != nil {
		t.run.NoteErr(kind, why)
	}
}

// Failed records a transaction that ended with an error (transport, client state, unknown outcome): it counts as
// an abort AND an error.
func (t *TxnStats) Failed(kind string, err error) {
	t.open.Add(-1)
	t.aborts.Add(1)
	t.errs.Add(1)
	t.run.NoteErr(kind, err)
}

// Abandoned closes the books on a transaction begun but never ended (shutdown in the middle).
func (t *TxnStats) Abandoned() { t.open.Add(-1) }

// ReaderSaw stamps reader activity (for the idle test).
func (t *TxnStats) ReaderSaw() { t.lastRead.Store(time.Now().UnixNano()) }

// Idle reports whether no transaction is open and neither the workers nor the readers received anything for d.
func (t *TxnStats) Idle(d time.Duration) bool {
	if t.open.Load() > 0 {
		return false
	}
	now := time.Now().UnixNano()
	return now-t.lastIn.Load() >= int64(d) && now-t.lastRead.Load() >= int64(d)
}

func (t *TxnStats) snap(s *snap) {
	s.txOK, s.txMsgs, s.txAbort, s.txErr, s.txIn = t.ok.Load(), t.msgs.Load(), t.aborts.Load(), t.errs.Load(), t.in.Load()
	s.txH = make([]int64, olNumBuckets)
	t.commitLat.snapshot(s.txH)
}

// TxnWindow is one [txn] window, kept for the JSON result.
type TxnWindow struct {
	K        int     `json:"k"`
	End      string  `json:"end"`
	TxnPerS  float64 `json:"txn_per_s"`
	MsgsPerS float64 `json:"msgs_per_s"`
	InPerS   float64 `json:"in_per_s"`
	AvgSize  float64 `json:"avg_msgs_per_txn"`
	P50      float64 `json:"commit_p50_ms"`
	P99      float64 `json:"commit_p99_ms"`
	P999     float64 `json:"commit_p999_ms"`
	Aborts   int64   `json:"aborts"`
	Errs     int64   `json:"errors"`
	Open     int64   `json:"open"`
}

func (r *Run) txnLine(k int, prev, cur *snap) {
	secs := cur.t.Sub(prev.t).Seconds()
	if r.txn == nil || secs <= 0 || cur.txH == nil || prev.txH == nil {
		return
	}
	h := diff(prev.txH, cur.txH)
	dOK, dMsgs := cur.txOK-prev.txOK, cur.txMsgs-prev.txMsgs
	w := TxnWindow{K: k, End: cur.t.UTC().Format("15:04:05"),
		TxnPerS: float64(dOK) / secs, MsgsPerS: float64(dMsgs) / secs, InPerS: float64(cur.txIn-prev.txIn) / secs,
		P50: olPercentile(h, 0.50), P99: olPercentile(h, 0.99), P999: olPercentile(h, 0.999),
		Aborts: cur.txAbort - prev.txAbort, Errs: cur.txErr - prev.txErr, Open: r.txn.open.Load()}
	if dOK > 0 {
		w.AvgSize = float64(dMsgs) / float64(dOK)
	}
	fmt.Printf("[txn] %s txn=%8.0f/s msgs=%9.0f/s avg=%6.2f msg/txn | commit p50=%.2f p99=%.2f p999=%.2f ms | aborts=%d errs=%d open=%d | in=%9.0f/s | total txns=%d msgs=%d aborts=%d errs=%d\n",
		w.End, w.TxnPerS, w.MsgsPerS, w.AvgSize, w.P50, w.P99, w.P999, w.Aborts, w.Errs, w.Open, w.InPerS,
		cur.txOK, cur.txMsgs, cur.txAbort, cur.txErr)
	r.winMu.Lock()
	r.txnWindows = append(r.txnWindows, w)
	r.winMu.Unlock()
}

// txnFinal prints [txn-final] and returns the JSON object.
func (r *Run) txnFinal(fin *snap) map[string]any {
	if r.txn == nil || fin.txH == nil {
		return nil
	}
	avg := 0.0
	if fin.txOK > 0 {
		avg = float64(fin.txMsgs) / float64(fin.txOK)
	}
	h := fin.txH
	fmt.Printf("[txn-final] txns=%d msgs=%d aborts=%d errs=%d in=%d avg=%.2f msg/txn | commit p50=%.2f p99=%.2f p999=%.2f max=%.2f ms\n",
		fin.txOK, fin.txMsgs, fin.txAbort, fin.txErr, fin.txIn, avg,
		olPercentile(h, 0.50), olPercentile(h, 0.99), olPercentile(h, 0.999), maxOf(h))
	r.winMu.Lock()
	wins := append([]TxnWindow(nil), r.txnWindows...)
	r.winMu.Unlock()
	return map[string]any{"txns": fin.txOK, "msgs": fin.txMsgs, "aborts": fin.txAbort, "errors": fin.txErr, "worker_in": fin.txIn,
		"avg_msgs_per_txn": avg, "commit_ms": map[string]any{"p50": olPercentile(h, 0.50), "p90": olPercentile(h, 0.90),
			"p99": olPercentile(h, 0.99), "p999": olPercentile(h, 0.999), "max": maxOf(h), "n": countOf(h)},
		"hist_commit_us": sparse(h), "windows": wins}
}

// DrainTxn keeps the workers and readers going after the producers stopped: until -drain elapsed, or earlier once
// this process's workers and readers were idle for -idle-exit (nothing received, no transaction open). Returns the
// instant the drain ended.
func (r *Run) DrainTxn(ctxDone <-chan struct{}, idleExit time.Duration) time.Time {
	end := r.prodEnd.Add(r.Cfg.Drain)
	if r.txn != nil && time.Until(end) > 0 {
		fmt.Printf("[drain] workers and readers keep going for up to %v (ends early after %v idle; lag now %d)\n",
			time.Until(end).Round(time.Millisecond), idleExit, r.Pushed()-r.Popped())
	}
	for time.Now().Before(end) {
		if r.txn != nil && time.Since(r.prodEnd) > idleExit && r.txn.Idle(idleExit) {
			fmt.Printf("[drain] idle for %v after %.1fs: ending the drain (lag %d)\n", idleExit, time.Since(r.prodEnd).Seconds(), r.Pushed()-r.Popped())
			break
		}
		select {
		case <-ctxDone:
			return time.Now()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if now := time.Now(); now.Before(end) {
		end = now
	}
	return end
}

// ---------------------------------------------------------------------------
// id ledger

// Message fates in the ledger.
const (
	IdLaunched  = 0 // handed to the client, never answered (in flight at exit): treated as ambiguous
	IdConfirmed = 1 // the broker acknowledged it
	IdFailed    = 2 // the send failed (it may or may not have been written): ambiguous
	idUnused    = 3 // never launched (only past the end of the ledger)
)

// IdLedger records the fate of every launched message of this process, indexed by seq.
type IdLedger struct {
	mu   sync.Mutex
	st   []uint8
	next atomic.Int64
}

// Reserve hands out n consecutive seqs.
func (l *IdLedger) Reserve(n int) int64 { return l.next.Add(int64(n)) - int64(n) }

// Set records the fate of seqs [first, first+n).
func (l *IdLedger) Set(first int64, n int, fate uint8) {
	l.mu.Lock()
	need := int(first) + n
	if need > len(l.st) {
		if need > cap(l.st) {
			ns := make([]uint8, need, max(need, 2*cap(l.st)+1024))
			copy(ns, l.st)
			l.st = ns
		} else {
			l.st = l.st[:need]
		}
	}
	for i := int(first); i < need; i++ {
		l.st[i] = fate
	}
	l.mu.Unlock()
}

// WriteFile writes the ledger as ranges: "MQIDS 1 src=<src> n=<seqs>" then "c|f|u <from> <to>" lines (inclusive;
// c = confirmed, f = failed, u = launched but unanswered).
func (l *IdLedger) WriteFile(path string, src int) (confirmed, failed, unanswered int64, err error) {
	l.mu.Lock()
	n := int(l.next.Load())
	st := make([]uint8, n)
	copy(st, l.st)
	l.mu.Unlock()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, 0, 0, err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "MQIDS 1 src=%d n=%d\n", src, n)
	letter := map[uint8]string{IdLaunched: "u", IdConfirmed: "c", IdFailed: "f"}
	for i := 0; i < n; {
		j := i
		for j+1 < n && st[j+1] == st[i] {
			j++
		}
		fmt.Fprintf(w, "%s %d %d\n", letter[st[i]], i, j)
		switch st[i] {
		case IdConfirmed:
			confirmed += int64(j - i + 1)
		case IdFailed:
			failed += int64(j - i + 1)
		default:
			unanswered += int64(j - i + 1)
		}
		i = j + 1
	}
	if err = w.Flush(); err == nil {
		err = f.Close()
	} else {
		f.Close()
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	return
}

// ---------------------------------------------------------------------------
// verifier

// Expect holds every process's ledger: per src, the fate of every seq.
type Expect struct {
	Srcs  map[int][]uint8
	Files int
}

// LoadIds reads every *.ids ledger of dir.
func LoadIds(dir string) (*Expect, error) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.ids"))
	if len(files) == 0 {
		return nil, fmt.Errorf("no *.ids ledgers in %s", dir)
	}
	e := &Expect{Srcs: map[int][]uint8{}}
	letter := map[string]uint8{"u": IdLaunched, "c": IdConfirmed, "f": IdFailed}
	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		var st []uint8
		src := -1
		for sc.Scan() {
			l := sc.Text()
			if strings.HasPrefix(l, "MQIDS ") {
				var ver, n int
				if _, err := fmt.Sscanf(l, "MQIDS %d src=%d n=%d", &ver, &src, &n); err != nil {
					f.Close()
					return nil, fmt.Errorf("%s: bad header %q", fn, l)
				}
				st = make([]uint8, n)
				continue
			}
			var kind string
			var a, b int
			if _, err := fmt.Sscanf(l, "%s %d %d", &kind, &a, &b); err != nil || b < a || b >= len(st) {
				f.Close()
				return nil, fmt.Errorf("%s: bad line %q", fn, l)
			}
			for i := a; i <= b; i++ {
				st[i] = letter[kind]
			}
		}
		f.Close()
		if src < 0 {
			return nil, fmt.Errorf("%s: no header", fn)
		}
		if _, dup := e.Srcs[src]; dup {
			return nil, fmt.Errorf("%s: src %d appears in two ledgers", fn, src)
		}
		e.Srcs[src] = st
		e.Files++
	}
	return e, nil
}

// Tally is the verifier's account of one run: what <prefix>-out holds and what <prefix>-in still holds.
type Tally struct {
	exp     *Expect
	mu      sync.Mutex
	outCnt  map[int][]uint8  // per src, copies of each seq seen in out (saturating)
	outPos  map[int][]uint64 // per src, the position of the first copy (re-read detection)
	pending map[int][]uint8  // per src, 1 = still unprocessed in in

	OutRecords, Rereads, Unparsed, Warm, PendRecords int64
	firstDup, firstExtra                             []string
}

// NewTally prepares the counters for every src of the ledgers.
func NewTally(e *Expect) *Tally {
	t := &Tally{exp: e, outCnt: map[int][]uint8{}, outPos: map[int][]uint64{}, pending: map[int][]uint8{}}
	for src, st := range e.Srcs {
		t.outCnt[src] = make([]uint8, len(st))
		t.outPos[src] = make([]uint64, len(st))
		t.pending[src] = make([]uint8, len(st))
	}
	return t
}

// PosHash turns a broker position (partition + offset, message id, ...) into the re-read key.
func PosHash(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return h.Sum64() | 1 // never 0 (= no copy yet)
}

// PosNum is PosHash for two numbers (partition + offset, ledger + entry, ...).
func PosNum(a, b uint64) uint64 {
	h := a*0x9E3779B97F4A7C15 ^ (b + 0x632BE59BD9B4E019)
	h ^= h >> 31
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 29
	return h | 1
}

// Out records one message read from <prefix>-out at broker position pos (re-reading the same position is not a
// duplicate; the same id at another position is).
func (t *Tally) Out(v []byte, pos uint64) {
	_, src, seq, ok := ParseSeq(v)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.OutRecords++
	if !ok {
		if _, _, isLoad := ParseStamp(v); isLoad {
			t.Unparsed++
		} else {
			t.Warm++
		}
		return
	}
	cnt, ok := t.outCnt[src]
	if !ok || seq >= int64(len(cnt)) {
		if len(t.firstExtra) < 5 {
			t.firstExtra = append(t.firstExtra, fmt.Sprintf("src=%d seq=%d (beyond the ledger)", src, seq))
		}
		t.growExtra(src, seq)
		cnt = t.outCnt[src]
	}
	ps := t.outPos[src]
	switch {
	case cnt[seq] == 0:
		cnt[seq], ps[seq] = 1, pos
	case ps[seq] == pos:
		t.Rereads++
	default:
		if cnt[seq] < 255 {
			cnt[seq]++
		}
		if len(t.firstDup) < 5 {
			t.firstDup = append(t.firstDup, fmt.Sprintf("src=%d seq=%d", src, seq))
		}
	}
}

// growExtra makes room for an id outside every ledger (it will count as extra).
func (t *Tally) growExtra(src int, seq int64) {
	n := int(seq) + 1
	grow := func(a []uint8) []uint8 {
		if len(a) >= n {
			return a
		}
		b := make([]uint8, n)
		copy(b, a)
		return b
	}
	t.outCnt[src] = grow(t.outCnt[src])
	t.pending[src] = grow(t.pending[src])
	if p := t.outPos[src]; len(p) < n {
		b := make([]uint64, n)
		copy(b, p)
		t.outPos[src] = b
	}
}

// Pending records one message still unprocessed in <prefix>-in.
func (t *Tally) Pending(v []byte) {
	_, src, seq, ok := ParseSeq(v)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !ok {
		return
	}
	t.PendRecords++
	if p, ok := t.pending[src]; ok && seq < int64(len(p)) {
		p[seq] = 1
	}
}

// Seen returns the records counted so far (the scans' progress).
func (t *Tally) Seen() (out, pend int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.OutRecords, t.PendRecords
}

// Verdict is the verifier's result.
type Verdict struct {
	System      string   `json:"system"`
	Ledgers     int      `json:"ledgers"`
	Produced    int64    `json:"produced"`  // confirmed by the broker
	Ambiguous   int64    `json:"ambiguous"` // failed or unanswered sends: may or may not exist
	OutRecords  int64    `json:"out_records"`
	OutUnique   int64    `json:"out_unique"`
	Duplicates  int64    `json:"duplicates"`  // ids with more than one copy in out
	DupRecords  int64    `json:"dup_records"` // copies beyond the first
	Missing     int64    `json:"missing"`     // confirmed, neither in out nor pending in in
	PendingIn   int64    `json:"pending_in"`  // confirmed, not processed yet (still in in), not in out
	InAndOut    int64    `json:"in_and_out"`  // in out AND still pending in in: output committed without the input position
	Extra       int64    `json:"extra"`       // in out, never confirmed nor ambiguous
	AmbigInOut  int64    `json:"ambiguous_in_out"`
	Rereads     int64    `json:"rereads"`
	Unparsed    int64    `json:"unparsed"`
	Warm        int64    `json:"warm"`
	PendRecords int64    `json:"pending_records"`
	Pass        bool     `json:"pass"`
	FirstDup    []string `json:"first_duplicates,omitempty"`
	FirstMiss   []string `json:"first_missing,omitempty"`
	FirstExtra  []string `json:"first_extra,omitempty"`
	FirstBoth   []string `json:"first_in_and_out,omitempty"`
	ScanS       float64  `json:"scan_s"`
}

// Verdict compares the ledgers with what the scans saw.
func (t *Tally) Verdict(system string) *Verdict {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := &Verdict{System: system, Ledgers: t.exp.Files, OutRecords: t.OutRecords, Rereads: t.Rereads, Unparsed: t.Unparsed,
		Warm: t.Warm, PendRecords: t.PendRecords, FirstDup: t.firstDup}
	srcs := make([]int, 0, len(t.outCnt))
	for s := range t.outCnt {
		srcs = append(srcs, s)
	}
	sort.Ints(srcs)
	for _, src := range srcs {
		cnt, pend := t.outCnt[src], t.pending[src]
		st := t.exp.Srcs[src]
		for seq := range cnt {
			fate := uint8(idUnused)
			if seq < len(st) {
				fate = st[seq]
			}
			c := cnt[seq]
			if c > 0 {
				v.OutUnique++
			}
			if c > 1 {
				v.Duplicates++
				v.DupRecords += int64(c - 1)
			}
			switch fate {
			case IdConfirmed:
				v.Produced++
				switch {
				case c > 0 && pend[seq] == 1:
					v.InAndOut++
					if len(v.FirstBoth) < 5 {
						v.FirstBoth = append(v.FirstBoth, fmt.Sprintf("src=%d seq=%d", src, seq))
					}
				case c == 0 && pend[seq] == 1:
					v.PendingIn++
				case c == 0:
					v.Missing++
					if len(v.FirstMiss) < 5 {
						v.FirstMiss = append(v.FirstMiss, fmt.Sprintf("src=%d seq=%d", src, seq))
					}
				}
			case IdFailed, IdLaunched:
				v.Ambiguous++
				if c > 0 {
					v.AmbigInOut++
				}
			default:
				if c > 0 {
					v.Extra++
				}
			}
		}
	}
	v.FirstExtra = t.firstExtra
	v.Pass = v.Duplicates == 0 && v.Missing == 0 && v.InAndOut == 0 && v.Extra == 0 && v.Unparsed == 0
	return v
}

// Print writes the [verify] line (and the first examples) and, with out, the JSON.
func (v *Verdict) Print(out string) {
	verdict := "FAIL"
	if v.Pass {
		verdict = "PASS"
	}
	fmt.Printf("[verify] system=%s ledgers=%d produced=%d ambiguous=%d out_records=%d out_unique=%d duplicates=%d dup_records=%d missing=%d pending_in=%d in_and_out=%d extra=%d ambiguous_in_out=%d rereads=%d unparsed=%d warm=%d scan=%.1fs | VERDICT %s\n",
		v.System, v.Ledgers, v.Produced, v.Ambiguous, v.OutRecords, v.OutUnique, v.Duplicates, v.DupRecords, v.Missing,
		v.PendingIn, v.InAndOut, v.Extra, v.AmbigInOut, v.Rereads, v.Unparsed, v.Warm, v.ScanS, verdict)
	for _, s := range []struct {
		k  string
		xs []string
	}{{"duplicate", v.FirstDup}, {"missing", v.FirstMiss}, {"extra", v.FirstExtra}, {"in_and_out", v.FirstBoth}} {
		if len(s.xs) > 0 {
			fmt.Printf("[verify] first %s: %s\n", s.k, strings.Join(s.xs, ", "))
		}
	}
	if out != "" {
		if js, err := json.MarshalIndent(v, "", "  "); err == nil {
			_ = os.WriteFile(out, js, 0o644)
		}
	}
}

// IdleScan runs a scan until it has been idle (no new record) for idle, or max elapsed; progress() returns the
// records seen so far. It prints a progress line every 10 s.
func IdleScan(what string, idle, maxDur time.Duration, progress func() int64, done <-chan struct{}) {
	t0 := time.Now()
	last, lastChange, lastLog := int64(-1), time.Now(), time.Now()
	for {
		n := progress()
		if n != last {
			last, lastChange = n, time.Now()
		}
		if time.Since(lastChange) >= idle {
			fmt.Printf("[verify] %s: %d records, idle %v: scan complete after %.1fs\n", what, n, idle, time.Since(t0).Seconds())
			return
		}
		if time.Since(t0) >= maxDur {
			fmt.Printf("[verify] WARN %s: %d records, still moving after -verify-max %v: scan stopped\n", what, n, maxDur)
			return
		}
		if time.Since(lastLog) >= 10*time.Second {
			lastLog = time.Now()
			fmt.Printf("[verify] %s: %d records after %.0fs\n", what, n, time.Since(t0).Seconds())
		}
		select {
		case <-done:
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}
