package core

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(t *testing.T, args ...string) *Config {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := &Config{}
	c.Register(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if err := c.Finalize(); err != nil {
		t.Fatal(err)
	}
	return c
}

// ---------------------------------------------------------------- pacer

// The pacer offers exactly rate x (duration - ramp/2) units (to within the
// W random phases), whatever the sender does.
func TestPacerOfferedRate(t *testing.T) {
	for _, tc := range []struct {
		ups  float64
		ramp time.Duration
		dur  time.Duration
	}{
		{20000, 0, 2 * time.Second},
		{20000, time.Second, 2 * time.Second},
		{300, 0, 2 * time.Second}, // 7 workers, sub-ms ticks not needed
		{50000, 0, 1500 * time.Millisecond},
	} {
		var launched, bulk atomic.Int64
		t0 := time.Now().Add(20 * time.Millisecond)
		p := &Pacer{UnitsPerSec: tc.ups, Ramp: tc.ramp, End: t0.Add(tc.dur), Seed: 1,
			Launch: func(int, time.Time) { launched.Add(1) },
			Bulk:   func(_ int, n int64) { bulk.Add(n) }}
		p.Run(context.Background(), t0)
		got := float64(launched.Load() + bulk.Load())
		want := tc.ups * (tc.dur.Seconds() - tc.ramp.Seconds()/2)
		W := float64(PacerWorkers(tc.ups))
		t.Logf("units/s=%.0f ramp=%v duration=%v: offered %.0f, schedule %.0f (%+.3f%%), %d bulk-shed, ended %v after the end",
			tc.ups, tc.ramp, tc.dur, got, want, 100*(got-want)/want, bulk.Load(), (time.Since(t0) - tc.dur).Round(time.Millisecond))
		// each worker's phase shifts its schedule by < one step: at most 1 unit each
		if math.Abs(got-want) > W+1 && math.Abs(got-want)/want > 0.002 {
			t.Errorf("ups=%.0f ramp=%v: offered %.0f, want %.0f (±%.0f)", tc.ups, tc.ramp, got, want, W+1)
		}
		if time.Since(t0) > tc.dur+300*time.Millisecond {
			t.Errorf("pacer ran %v past its end", time.Since(t0)-tc.dur)
		}
	}
}

// Units are scheduled on the F^-1 instants: during the ramp the offered
// count follows rps*t^2/(2R).
func TestPacerRampShape(t *testing.T) {
	var early, late atomic.Int64
	t0 := time.Now().Add(20 * time.Millisecond)
	ramp := time.Second
	p := &Pacer{UnitsPerSec: 10000, Ramp: ramp, End: t0.Add(ramp), Seed: 3,
		Launch: func(_ int, s time.Time) {
			if s.Sub(t0) < ramp/2 {
				early.Add(1)
			} else {
				late.Add(1)
			}
		},
		Bulk: func(int, int64) {}}
	p.Run(context.Background(), t0)
	// first half of a linear ramp holds 1/4 of its units
	frac := float64(early.Load()) / float64(early.Load()+late.Load())
	t.Logf("ramp 1 s at 10000 units/s: first half %d, second half %d units (%.4f of the ramp in the first half, F=t^2 wants 0.25)", early.Load(), late.Load(), frac)
	if math.Abs(frac-0.25) > 0.02 {
		t.Errorf("first half of the ramp holds %.3f of the units, want 0.25", frac)
	}
}

// With the in-flight cap full and a sender that never completes, every unit
// after the first is shed, the offered count still matches the rate, and the
// pacer never blocks (it ends on time).
func TestShedWhenCapFullAndPacerNeverBlocks(t *testing.T) {
	c := testConfig(t, "-rate", "20000", "-batch", "10", "-max-inflight", "3", "-ramp", "0", "-duration", "1s", "-report", "1h", "-topics", "2", "-partitions", "8")
	r, err := NewRun(c, "test")
	if err != nil {
		t.Fatal(err)
	}
	var sent atomic.Int64
	start := time.Now()
	t0 := start.Add(10 * time.Millisecond)
	r.Produce(context.Background(), t0, func(u *Unit) { sent.Add(1) }) // never completes a unit
	took := time.Since(t0)
	r.StopReporter(time.Now())
	if took > 1300*time.Millisecond {
		t.Fatalf("pacer blocked: producing took %v for a 1 s run", took)
	}
	offU, shedU := r.offeredUnits.Load(), r.shedUnits.Load()
	offM, shedM := r.offeredMsgs.Load(), r.shedMsgs.Load()
	t.Logf("cap 3 units, sender never completes: offered %d units (%d msgs) in %v, shed %d, sent %d, producing took %v", offU, offM, time.Second, shedU, sent.Load(), took.Round(time.Millisecond))
	wantU := 20000.0 / 10 * 1.0
	if math.Abs(float64(offU)-wantU)/wantU > 0.02 {
		t.Errorf("offered %d units, want ~%.0f", offU, wantU)
	}
	if sent.Load() != 3 || offU-shedU != 3 || r.Inflight() != 3 {
		t.Errorf("sent=%d offered-shed=%d inflight=%d, want 3/3/3", sent.Load(), offU-shedU, r.Inflight())
	}
	if offM != offU*10 || shedM != shedU*10 {
		t.Errorf("msgs offered=%d shed=%d, want units x 10", offM, shedM)
	}
}

// A completed unit releases its slot and records one latency sample.
func TestUnitCompletion(t *testing.T) {
	c := testConfig(t, "-rate", "2000", "-batch", "5", "-max-inflight", "1000", "-ramp", "0", "-duration", "500ms", "-report", "1h")
	r, _ := NewRun(c, "test")
	r.Produce(context.Background(), time.Now(), func(u *Unit) {
		if len(u.Payloads) != u.N {
			t.Errorf("unit has %d payloads for %d msgs", len(u.Payloads), u.N)
		}
		for i := 0; i < u.N; i++ {
			var err error
			if u.Seq == 3 && i == 2 {
				err = context.DeadlineExceeded // one failed message fails its unit
			}
			go u.MsgDone(err)
		}
	})
	r.WaitInflight(2 * time.Second)
	r.StopReporter(time.Now())
	off, ach, errU := r.offeredUnits.Load(), r.achievedUnits.Load(), r.pushErrUnits.Load()
	if ach+errU != off || errU != 1 || r.pushed.Load() != off*5-1 {
		t.Errorf("offered=%d achieved=%d errUnits=%d pushed=%d", off, ach, errU, r.pushed.Load())
	}
	h := make([]int64, olNumBuckets)
	r.prodLat.snapshot(h)
	if countOf(h) != ach {
		t.Errorf("%d latency samples for %d achieved units", countOf(h), ach)
	}
}

// ---------------------------------------------------------------- pickers

func TestPickerRRCoversSpaceFromLoaderOffset(t *testing.T) {
	c := testConfig(t, "-topics", "3", "-partitions", "7", "-loader-index", "1", "-loaders", "2")
	p, _ := NewPicker(c)
	w := p.Worker(1)
	seen := map[[2]uint64]bool{}
	for i := 0; i < 21; i++ {
		tp, e := w.Pick(0)
		if i == 0 && (tp != 0 || e != 3) {
			t.Fatalf("first pick = (%d,%d), want (0,3): start = loader_index*space/loaders", tp, e)
		}
		seen[[2]uint64{uint64(tp), e}] = true
	}
	if len(seen) != 21 {
		t.Fatalf("rr covered %d of 21 (topic, entity) pairs in 21 picks", len(seen))
	}
}

func TestPickerActiveWindowExactlyActivePerSecond(t *testing.T) {
	for _, dist := range []string{"rotate", "scatter"} {
		c := testConfig(t, "-topics", "2", "-entities", "100000", "-dist", dist, "-active", "500", "-loader-index", "4", "-loaders", "9")
		p, _ := NewPicker(c)
		p.SetStart(0)
		w := p.Worker(1)
		covered := map[uint64]bool{}
		for sec := int64(0); sec < 10; sec++ {
			seen := [2]map[uint64]bool{{}, {}}
			for i := 0; i < 2*500*4; i++ { // 4 units per active entity and topic
				tp, e := w.Pick(sec*1_000_000 + int64(i)*100)
				seen[tp][e] = true
				covered[e] = true
			}
			for tp := range seen {
				if len(seen[tp]) != 500 {
					t.Fatalf("%s sec=%d topic=%d: %d distinct entities, want 500", dist, sec, tp, len(seen[tp]))
				}
			}
		}
		if len(covered) != 5000 {
			t.Fatalf("%s: %d entities in 10 s, want 5000 (window advances by -active per second)", dist, len(covered))
		}
	}
	// goload's rr with -active N and -active-policy
	c := testConfig(t, "-partitions", "1000", "-active", "10", "-active-policy", "scatter")
	p, _ := NewPicker(c)
	if p.mode != pickWindow || p.mult == 0 {
		t.Fatal("-dist rr -active N -active-policy scatter must be the scatter window")
	}
}

func TestZipfSkew(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const n, draws = 10000, 2_000_000
	for _, s := range []float64{1.1, 1.0, 0.8} {
		z := NewZipf(rng, n, s)
		cnt := make([]int, n+1)
		for i := 0; i < draws; i++ {
			k := z.Next()
			if k < 1 || k > n {
				t.Fatalf("rank %d out of [1,%d]", k, n)
			}
			cnt[k]++
		}
		var H float64
		for k := 1; k <= n; k++ {
			H += math.Pow(float64(k), -s)
		}
		for _, k := range []int{1, 2, 10, 100} {
			want := math.Pow(float64(k), -s) / H
			got := float64(cnt[k]) / draws
			if math.Abs(got-want)/want > 0.05 {
				t.Errorf("s=%.1f P(%d)=%.5f, want %.5f", s, k, got, want)
			}
		}
	}
	// topic weights: s = 1.0 over 10 topics (rand.Zipf cannot do s <= 1)
	z := NewZipf(rng, 10, 1.0)
	cnt := make([]int, 11)
	for i := 0; i < 1_000_000; i++ {
		cnt[z.Next()]++
	}
	H := 0.0
	for k := 1; k <= 10; k++ {
		H += 1 / float64(k)
	}
	for k := 1; k <= 10; k++ {
		want := 1 / float64(k) / H
		if got := float64(cnt[k]) / 1e6; math.Abs(got-want) > 0.005 {
			t.Errorf("topic zipf s=1: P(%d)=%.4f want %.4f", k, got, want)
		}
	}
}

// The zipf picker permutes ranks over the space (a bijection) so hot keys are
// spread over the partitions instead of sitting on entities 0,1,2...
func TestPickerZipfPermutation(t *testing.T) {
	c := testConfig(t, "-entities", "1000", "-dist", "zipf")
	p, _ := NewPicker(c)
	seen := map[uint64]bool{}
	for r := uint64(0); r < 1000; r++ {
		seen[(r*p.perm)%p.space] = true
	}
	if len(seen) != 1000 {
		t.Fatalf("rank permutation is not a bijection: %d images", len(seen))
	}
	w := p.Worker(5)
	hot := map[uint64]int{}
	for i := 0; i < 100000; i++ {
		_, e := w.Pick(0)
		hot[e]++
	}
	if hot[0] < hot[p.perm%p.space] { // rank 1 -> entity 0, rank 2 -> entity perm
		t.Errorf("rank 1 (entity 0) drew %d, rank 2 (entity %d) drew %d", hot[0], p.perm%p.space, hot[p.perm%p.space])
	}
	if frac := float64(hot[0]) / 1e5; frac < 0.1 {
		t.Errorf("hottest entity share %.3f; zipf(1.1) over 1000 wants ~0.14", frac)
	}
}

func TestTopicZipfPicker(t *testing.T) {
	c := testConfig(t, "-topics", "10", "-topic-dist", "zipf", "-partitions", "4")
	p, _ := NewPicker(c)
	w := p.Worker(9)
	cnt := make([]int, 10)
	for i := 0; i < 200000; i++ {
		tp, e := w.Pick(0)
		if e >= 4 {
			t.Fatalf("entity %d out of 4 partitions", e)
		}
		cnt[tp]++
	}
	if !(cnt[0] > cnt[1] && cnt[1] > cnt[4] && cnt[4] > cnt[9]) {
		t.Errorf("topic counts not zipf-ordered: %v", cnt)
	}
}

// ---------------------------------------------------------------- payload

func TestStampParseRoundTrip(t *testing.T) {
	pool := NewPayloadPool(1, 256, 100, 7)
	// goload's jsonEvent at 256: the fixed fields alone are ~280 B, so the
	// text stays empty and events are ~290 B (same bytes as Queen's runs).
	if pool.AvgRaw < 270 || pool.AvgRaw > 310 {
		t.Errorf("avg raw event %.1f B, want ~290 (goload's jsonEvent at -payload 256)", pool.AvgRaw)
	}
	if big := NewPayloadPool(1, 1024, 4, 7); big.AvgRaw < 1000 || big.AvgRaw > 1040 {
		t.Errorf("avg raw event at 1024: %.1f B", big.AvgRaw)
	}
	ts := time.Now().UnixMicro()
	msgs := pool.Stamp(ts, 100)
	if len(msgs) != 100 {
		t.Fatalf("%d msgs", len(msgs))
	}
	for i, m := range msgs {
		var ev map[string]any
		if err := json.Unmarshal(m, &ev); err != nil {
			t.Fatalf("msg %d not JSON: %v: %s", i, err, m)
		}
		if int64(ev["ts"].(float64)) != ts || int(ev["src"].(float64)) != 7 || ev["text"] == nil || ev["id"] == nil {
			t.Fatalf("msg %d fields: %v", i, ev)
		}
		gts, src, ok := ParseStamp(m)
		if !ok || gts != ts || src != 7 {
			t.Fatalf("ParseStamp = %d,%d,%v want %d,7,true", gts, src, ok, ts)
		}
		if cap(m) != len(m) {
			t.Fatalf("payload %d can grow into its neighbour (cap %d len %d)", i, cap(m), len(m))
		}
	}
	if _, _, ok := ParseStamp(pool.Warm()); ok {
		t.Fatal("a warm-up message parsed as a load message")
	}
	for _, bad := range []string{"", "{", `{"ts":}`, `{"ts":12`, `{"tx":1,"src":2,`, `x"ts":1,`} {
		if _, _, ok := ParseStamp([]byte(bad)); ok {
			t.Errorf("ParseStamp(%q) ok", bad)
		}
	}
	if ts, src, ok := ParseStamp([]byte(`{"ts":5,"a":1}`)); !ok || ts != 5 || src != -1 {
		t.Errorf("ts without src: %d %d %v", ts, src, ok)
	}
	// units rotate through the pool: two consecutive units differ
	a, b := pool.Stamp(1, 1), pool.Stamp(1, 1)
	if string(a[0]) == string(b[0]) {
		t.Error("consecutive units reuse the same pool batch")
	}
}

// ---------------------------------------------------------------- histograms

// refOLBucketIndex / refOLBucketMid / refOLPercentile: an independent copy of
// goload's functions (ref/goload-pe-build/main.go) to pin hist.go to them.
func refOLBucketIndex(v int64) int {
	const (
		olLinearMax  = 1024
		olSubBits    = 6
		olSubCount   = 1 << olSubBits
		olBaseOctave = 10
		olMaxOctave  = 26
		olNumBuckets = olLinearMax + (olMaxOctave-olBaseOctave+1)*olSubCount
	)
	if v <= 0 {
		return 0
	}
	if v < olLinearMax {
		return int(v)
	}
	octave := bits.Len64(uint64(v)) - 1
	if octave > olMaxOctave {
		return olNumBuckets - 1
	}
	shift := uint(octave - olSubBits)
	sub := int((v - (int64(1) << uint(octave))) >> shift)
	return olLinearMax + (octave-olBaseOctave)*olSubCount + sub
}

func refOLBucketMid(idx int) float64 {
	if idx < 1024 {
		return float64(idx) + 0.5
	}
	j := idx - 1024
	octave := 10 + j/64
	sub := j % 64
	width := int64(1) << uint(octave-6)
	lo := (int64(1) << uint(octave)) + int64(sub)*width
	return float64(lo) + float64(width)/2
}

func refOLPercentile(counts []int64, p float64) float64 {
	var total int64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range counts {
		cum += c
		if cum >= target {
			return refOLBucketMid(i) / 1000.0
		}
	}
	return refOLBucketMid(len(counts)-1) / 1000.0
}

func TestPercentileParityWithGoload(t *testing.T) {
	if olNumBuckets != 2112 {
		t.Fatalf("olNumBuckets = %d, goload's layout has 2112", olNumBuckets)
	}
	rng := rand.New(rand.NewSource(11))
	for trial := 0; trial < 20; trial++ {
		h := newOLHist()
		ref := make([]int64, olNumBuckets)
		n := 1 + rng.Intn(100000)
		for i := 0; i < n; i++ {
			// log-uniform latencies 1 µs .. 100 s, beyond the 67 s ceiling too
			v := int64(math.Exp(rng.Float64() * math.Log(1e8)))
			if bi := olBucketIndex(v); bi != refOLBucketIndex(v) {
				t.Fatalf("bucket(%d) = %d, goload %d", v, bi, refOLBucketIndex(v))
			}
			h.record(v)
			ref[refOLBucketIndex(v)]++
		}
		got := make([]int64, olNumBuckets)
		h.snapshot(got)
		for _, p := range []float64{0.5, 0.9, 0.99, 0.999, 1} {
			if a, b := olPercentile(got, p), refOLPercentile(ref, p); a != b {
				t.Fatalf("p%.3f = %v, goload %v", p, a, b)
			}
		}
	}
	// hand-checked values
	h := make([]int64, olNumBuckets)
	h[olBucketIndex(1500)]++ // octave 10, width 16, sub 29 -> [1488,1504) mid 1496
	if got := olPercentile(h, 0.99); got != 1.496 {
		t.Errorf("1500 µs -> %v ms, want 1.496", got)
	}
}

// ---------------------------------------------------------------- consumers

func TestConsumerTopicAssignment(t *testing.T) {
	// cons-total >= T: consumer ci reads topic ci % T; topics shared by several
	const T, total = 10, 297
	members := make([]int, T)
	for ci := 0; ci < total; ci++ {
		ts := ConsumerTopics(ci, total, T)
		if len(ts) != 1 || ts[0] != ci%T {
			t.Fatalf("ci=%d: %v", ci, ts)
		}
		members[ts[0]]++
	}
	c := testConfig(t, "-topics", "10", "-cons-total", "297", "-consumers", "33", "-cons-offset", "264")
	for tp := 0; tp < T; tp++ {
		if c.MembersOfTopic(tp) != members[tp] {
			t.Errorf("MembersOfTopic(%d)=%d, counted %d", tp, c.MembersOfTopic(tp), members[tp])
		}
	}
	// cons-total < T: consumer ci reads {t : t % cons-total == ci}; every topic exactly once
	const T2 = 1000
	owner := make([]int, T2)
	for i := range owner {
		owner[i] = -1
	}
	for ci := 0; ci < total; ci++ {
		for _, tp := range ConsumerTopics(ci, total, T2) {
			if tp%total != ci || owner[tp] != -1 {
				t.Fatalf("topic %d given to ci=%d (owner %d)", tp, ci, owner[tp])
			}
			owner[tp] = ci
		}
	}
	for tp, o := range owner {
		if o == -1 {
			t.Fatalf("topic %d has no consumer", tp)
		}
	}
}

func TestLocalSrcAndDurations(t *testing.T) {
	c := testConfig(t, "-loader-index", "4", "-loaders", "9", "-local-src", "3-5,8", "-duration", "65", "-ramp", "2.5")
	for s, want := range map[int]bool{3: true, 4: true, 5: true, 8: true, 0: false, 6: false} {
		if c.IsLocalSrc(s) != want {
			t.Errorf("IsLocalSrc(%d) = %v", s, !want)
		}
	}
	if c.Duration != 65*time.Second || c.Ramp != 2500*time.Millisecond {
		t.Errorf("duration %v ramp %v", c.Duration, c.Ramp)
	}
	k := testConfig(t, "-mode", "keyed")
	if k.Batch != 1 {
		t.Errorf("keyed default batch %d, want 1", k.Batch)
	}
	b := testConfig(t)
	if b.Batch != 100 {
		t.Errorf("batch default %d, want 100", b.Batch)
	}
}

func TestProcSim(t *testing.T) {
	p := NewProcSim(200)
	start := time.Now()
	for i := 0; i < 20; i++ {
		p.Add(10) // 2 ms per call
	}
	if took := time.Since(start); took < 38*time.Millisecond || took > 80*time.Millisecond {
		t.Errorf("20 x 10 msgs x 200 µs slept %v, want ~40 ms", took)
	}
}

// ---------------------------------------------------------------- partition ownership

// Pulsar failover/exclusive consumers split partitions like a Kafka group:
// every partition of every topic is owned by exactly one consumer, for both
// topic rules (cons-total >= T and < T), and the lists are deterministic.
func TestConsumerPartitionsOwnedExactlyOnce(t *testing.T) {
	for _, tc := range []struct{ topics, parts, total int }{
		{1, 200, 198}, {1, 200, 297}, {1, 100000, 297}, {1, 12, 6}, {1, 5, 9}, // T=1: p % cons-total == ci
		{10, 1000, 297}, {3, 7, 10}, {10, 4, 297}, // cons-total >= T
		{1000, 100, 297}, {5, 4, 2}, {7, 3, 7}, // cons-total < T (and == T)
	} {
		c := testConfig(t, "-topics", fmt.Sprint(tc.topics), "-partitions", fmt.Sprint(tc.parts), "-cons-total", fmt.Sprint(tc.total), "-consumers", "1")
		owner := make([][]int, tc.topics)
		for tp := range owner {
			owner[tp] = make([]int, tc.parts)
			for p := range owner[tp] {
				owner[tp][p] = -1
			}
		}
		idle := 0
		for ci := 0; ci < tc.total; ci++ {
			n := 0
			for tp := 0; tp < tc.topics; tp++ {
				ps := c.ConsumerPartitions(ci, tp)
				if len(ps) > 0 && !slices.Contains(c.ConsumerTopics(ci), tp) {
					t.Fatalf("%+v: ci=%d owns partitions of topic %d it does not read", tc, ci, tp)
				}
				if !slices.Equal(ps, c.ConsumerPartitions(ci, tp)) || !slices.IsSorted(ps) {
					t.Fatalf("%+v: ci=%d topic %d: not deterministic/sorted", tc, ci, tp)
				}
				for _, p := range ps {
					if owner[tp][p] != -1 {
						t.Fatalf("%+v: topic %d partition %d owned by %d and %d", tc, tp, p, owner[tp][p], ci)
					}
					owner[tp][p] = ci
				}
				n += len(ps)
			}
			if n == 0 {
				idle++
			}
		}
		for tp := range owner {
			for p, o := range owner[tp] {
				if o == -1 {
					t.Fatalf("%+v: topic %d partition %d has no consumer", tc, tp, p)
				}
			}
		}
		if tc.topics == 1 {
			for ci := 0; ci < tc.total; ci++ { // the literal T=1 rule
				for _, p := range c.ConsumerPartitions(ci, 0) {
					if p%tc.total != ci {
						t.Fatalf("T=1: ci=%d got partition %d, want p %% %d == ci", ci, p, tc.total)
					}
				}
			}
			if want := max(0, tc.total-tc.parts); idle != want {
				t.Errorf("%+v: %d idle consumers, want %d", tc, idle, want)
			}
		}
	}
}

// -producer-shard: process li of n owns {p : p % n == li}; the shards cover
// every partition exactly once, and the picker only ever returns partitions of
// its own shard (rr walks the whole shard; rotate/scatter keep -active N
// distinct partitions per second across all the processes).
func TestProducerShard(t *testing.T) {
	for _, tc := range []struct{ parts, loaders int }{{12, 9}, {200, 9}, {1000, 9}, {100000, 9}, {7, 2}, {9, 9}} {
		owned := make([]int, tc.parts)
		for li := 0; li < tc.loaders; li++ {
			for _, p := range ShardPartitions(li, tc.loaders, tc.parts) {
				owned[p]++
			}
		}
		for p, n := range owned {
			if n != 1 {
				t.Fatalf("%+v: partition %d in %d shards", tc, p, n)
			}
		}
	}
	for _, dist := range []string{"rr", "rotate", "scatter", "zipf"} {
		covered := map[uint64]int{}
		for li := 0; li < 9; li++ {
			args := []string{"-partitions", "1000", "-loader-index", fmt.Sprint(li), "-loaders", "9", "-dist", dist}
			if dist == "rotate" || dist == "scatter" {
				args = append(args, "-active", "90")
			}
			c := testConfig(t, args...)
			c.ShardIndex, c.ShardCount = li, 9
			p, err := NewPicker(c)
			if err != nil {
				t.Fatal(err)
			}
			p.SetStart(0)
			w := p.Worker(int64(li))
			shard := ShardPartitions(li, 9, 1000)
			perSec := map[uint64]bool{}
			for i := 0; i < 5000; i++ {
				_, e := w.Pick(int64(i) * 100) // all inside second 0
				if int(e)%9 != li || e >= 1000 {
					t.Fatalf("%s li=%d picked partition %d outside its shard", dist, li, e)
				}
				covered[e]++
				perSec[e] = true
			}
			switch dist {
			case "rr":
				if len(perSec) != len(shard) {
					t.Fatalf("rr li=%d covered %d of its %d partitions", li, len(perSec), len(shard))
				}
			case "rotate", "scatter":
				if len(perSec) != 10 { // ceil(90/9)
					t.Fatalf("%s li=%d: %d distinct partitions in a second, want 10", dist, li, len(perSec))
				}
			}
		}
		if dist == "rr" && len(covered) != 1000 {
			t.Fatalf("rr over 9 shards covered %d of 1000 partitions", len(covered))
		}
	}
	c := testConfig(t, "-partitions", "5", "-loader-index", "7", "-loaders", "9")
	c.ShardIndex, c.ShardCount = 7, 9
	if _, err := NewPicker(c); err == nil {
		t.Fatal("a shard without partitions must be an error")
	}
}
