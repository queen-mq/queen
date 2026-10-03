package core

import (
	"fmt"
	"math/rand"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// scatterMultiplier, gcd, partitionIndex: copied VERBATIM from goload
// (ref/goload-pe-build/main.go), so -dist rotate|scatter with -active N is
// goload's -active-partitions N -active-policy rotate|scatter.

// scatterMultiplier picks the multiplier that spreads a small active window
// across a large partition space.
//
// It has to be coprime with the space, or the mapping stops being a bijection
// and the run silently writes to a subset. It also has to be far from a small
// residue: a hardcoded 1_000_003 against a space of 1_000_000 is 3 modulo the
// space, so consecutive slots would land three apart and the window would stay
// contiguous in all but name. Fibonacci hashing gives both: space/phi, nudged
// odd and then up until it is coprime.
func scatterMultiplier(space uint64) uint64 {
	a := uint64(float64(space) * 0.6180339887498949)
	if a%2 == 0 {
		a++
	}
	for gcd(a, space) != 1 {
		a += 2
	}
	return a
}

func gcd(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// partitionIndex places request n of second sec inside the active window.
//
//	mult == 0  rotate: the window is contiguous and advances one window per
//	           second, so the count of partitions ever written grows by active
//	           per second until the space is covered. Best case for B-tree
//	           locality.
//	mult != 0  scatter: the same window, permuted across the whole space by a
//	           coprime multiplier. Worst case: consecutive writes land in
//	           unrelated pages. Still a bijection, so coverage is unchanged.
//
// Pure and package level so both properties can be tested: exactly `active`
// distinct partitions per second, and full coverage after space/active seconds.
func partitionIndex(mult, sec, n, active, space uint64) uint64 {
	slot := (sec*active + n%active) % space
	if mult != 0 {
		return (slot * mult) % space
	}
	return slot
}

// ---------------------------------------------------------------------------

type pickMode int

const (
	pickRR     pickMode = iota // round robin over the space (goload's default)
	pickWindow                 // -active N per second: rotate (mult 0) or scatter
	pickZipf                   // Zipf(-zipf-s) over the space, ranks permuted
)

// Picker chooses (topic, entity) for every launched unit. The entity space per
// topic is -entities, or -partitions when -entities is 0 (entity e IS
// partition e). Shared by all pacer workers; per-worker state (RNG, Zipf
// samplers) lives in WorkerPicker.
type Picker struct {
	topics    uint64
	space     uint64
	mode      pickMode
	topicZipf bool
	active    uint64
	mult      uint64 // window: 0 = rotate, else the scatter multiplier
	perm      uint64 // zipf: rank r -> entity (r*perm) % space (a bijection)
	start     uint64 // rr start offset in entity units: loader_index*space/loaders
	shardI    uint64 // producer sharding: pick j in the shard, entity = shardI + j*shardN
	shardN    uint64
	zipfS     float64
	topicS    float64
	t0Micros  atomic.Int64 // start instant; "second" of a unit = (sched-t0)/1s

	counter  atomic.Uint64   // topic-rr: unit n -> topic n%T, entity step n/T
	perTopic []atomic.Uint64 // topic-zipf: per-topic entity steps
}

// NewPicker builds the picker from the workload config.
func NewPicker(c *Config) (*Picker, error) {
	p := &Picker{
		topics: uint64(c.Topics),
		space:  c.PickSpace(),
		zipfS:  c.ZipfS,
		topicS: c.TopicZipfS,
	}
	if c.Sharded() {
		if p.space == 0 {
			return nil, fmt.Errorf("producer shard %d of %d owns none of the %d partitions", c.ShardIndex, c.ShardCount, c.Partitions)
		}
		p.shardI, p.shardN = uint64(c.ShardIndex), uint64(c.ShardCount)
	} else if c.Loaders > 0 {
		p.start = uint64(c.LoaderIndex) * p.space / uint64(c.Loaders)
	}
	switch c.TopicDist {
	case "rr", "":
	case "zipf":
		p.topicZipf = c.Topics > 1
		p.perTopic = make([]atomic.Uint64, c.Topics)
	default:
		return nil, fmt.Errorf("-topic-dist %q: want rr|zipf", c.TopicDist)
	}
	policy := ""
	switch c.Dist {
	case "rr", "":
		p.mode = pickRR
		if c.Active > 0 {
			policy = c.ActivePolicy // goload: -active-partitions with -active-policy
		}
	case "rotate", "scatter":
		policy = c.Dist
	case "zipf":
		p.mode = pickZipf
		p.perm = scatterMultiplier(p.space)
	default:
		return nil, fmt.Errorf("-dist %q: want rr|rotate|scatter|zipf", c.Dist)
	}
	if policy != "" {
		active := uint64(c.Active)
		if p.shardN > 1 && active > 0 {
			// the processes' windows are disjoint: -active N stays N distinct
			// partitions per second across all of them
			active = (active + p.shardN - 1) / p.shardN
		}
		if active == 0 || active > p.space {
			active = p.space
		}
		switch policy {
		case "rotate":
		case "scatter":
			p.mult = scatterMultiplier(p.space)
		default:
			return nil, fmt.Errorf("-active-policy %q: want rotate|scatter", policy)
		}
		p.active = active
		if active == p.space {
			p.mode = pickRR // goload: an active window as wide as the space is the plain round robin
		} else {
			p.mode = pickWindow
		}
	}
	return p, nil
}

// SetStart anchors the rotate/scatter seconds at the producers' start instant.
func (p *Picker) SetStart(t0Micros int64) { p.t0Micros.Store(t0Micros) }

// Describe returns a one-line description for the header.
func (p *Picker) Describe() string {
	td := "rr"
	if p.topicZipf {
		td = fmt.Sprintf("zipf(s=%.2f)", p.topicS)
	}
	shard := ""
	if p.shardN > 1 {
		shard = fmt.Sprintf(" [producer shard %d/%d: partitions p %% %d == %d, entity j -> partition %d + j*%d]", p.shardI, p.shardN, p.shardN, p.shardI, p.shardI, p.shardN)
	}
	return p.describe(td) + shard
}

func (p *Picker) describe(td string) string {
	switch p.mode {
	case pickWindow:
		pol := "rotate"
		if p.mult != 0 {
			pol = "scatter"
		}
		return fmt.Sprintf("topics=%s entities: %s active=%d/s of space=%d (space covered in %.0f s)", td, pol, p.active, p.space, float64(p.space)/float64(p.active))
	case pickZipf:
		return fmt.Sprintf("topics=%s entities: zipf(s=%.2f) over space=%d, rank r -> entity r*%d mod space", td, p.zipfS, p.space, p.perm)
	default:
		return fmt.Sprintf("topics=%s entities: rr over space=%d starting at %d", td, p.space, p.start)
	}
}

// WorkerPicker is one pacer worker's view of the picker (own RNG and Zipf
// samplers; the rr counters are shared atomics).
type WorkerPicker struct {
	p    *Picker
	zEnt *Zipf
	zTop *Zipf
}

// Worker returns a per-worker picker seeded with seed.
func (p *Picker) Worker(seed int64) *WorkerPicker {
	rng := rand.New(rand.NewSource(seed))
	w := &WorkerPicker{p: p}
	if p.mode == pickZipf {
		w.zEnt = NewZipf(rng, p.space, p.zipfS)
	}
	if p.topicZipf {
		w.zTop = NewZipf(rng, p.topics, p.topicS)
	}
	return w
}

// Pick returns the topic index and entity for a unit scheduled at schedMicros
// (with producer sharding, the entity is the partition of the shard).
func (w *WorkerPicker) Pick(schedMicros int64) (topic int, entity uint64) {
	t, e := w.pick(schedMicros)
	if p := w.p; p.shardN > 1 {
		e = p.shardI + e*p.shardN
	}
	return t, e
}

func (w *WorkerPicker) pick(schedMicros int64) (topic int, entity uint64) {
	p := w.p
	var t, c uint64
	if p.topicZipf {
		t = w.zTop.Next() - 1
		c = p.perTopic[t].Add(1) - 1
	} else {
		n := p.counter.Add(1) - 1
		t = n % p.topics
		c = n / p.topics
	}
	switch p.mode {
	case pickZipf:
		r := w.zEnt.Next() - 1
		return int(t), (r * p.perm) % p.space
	case pickWindow:
		var sec uint64
		if d := schedMicros - p.t0Micros.Load(); d > 0 {
			sec = uint64(d / 1_000_000)
		}
		return int(t), partitionIndex(p.mult, sec, p.start+c, p.active, p.space)
	default:
		return int(t), (p.start + c) % p.space
	}
}
