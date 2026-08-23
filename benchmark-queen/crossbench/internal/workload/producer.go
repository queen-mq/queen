package workload

import (
	"context"
	"math"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// open-loop pacer
// ---------------------------------------------------------------------------

// maxCatchUp bounds how much backlog a single late wake-up may flush. Beyond it
// the surplus is SHED and accounted, never silently absorbed: an open-loop rig
// that quietly slows its own offered rate turns a broker's failure into a clean
// graph, which is the classic way load tests lie.
const maxCatchUp = 8192

// Pacer offers `rate` events/s on a WALL-CLOCK schedule: a late wake still owes
// everything accumulated since t0, so the offered rate cannot sag just because
// the system under test got slow. With rampSec>0 the density ramps 0->full over
// the first rampSec seconds.
//
// emit is called once per owed event with its coordinated-omission-correct
// SCHEDULED instant — not the instant it was actually handed over. Latency is
// measured from that, so queueing delay inside the rig is counted, not hidden.
func Pacer(ctx context.Context, rate, rampSec int, emit func(sched time.Time), onShed func(n int64)) {
	if rate <= 0 {
		<-ctx.Done()
		return
	}
	rps := float64(rate)
	minTick, maxTick := 250*time.Microsecond, 1*time.Millisecond
	step := time.Duration(float64(time.Second) / rps)
	tickEvery := step
	if tickEvery > maxTick {
		tickEvery = maxTick
	}
	if tickEvery < minTick {
		tickEvery = minTick
	}
	ramp := float64(rampSec)
	base := time.Now()
	tk := time.NewTicker(tickEvery)
	defer tk.Stop()

	var k int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		el := time.Since(base).Seconds()
		var cum float64
		if ramp <= 0 || el >= ramp {
			cum = rps * (el - maxf(ramp, 0)/2)
		} else {
			cum = rps * el * el / (2 * ramp)
		}
		owed := int64(cum) + 1 - k
		if owed <= 0 {
			continue
		}
		if owed > maxCatchUp {
			bulk := owed - maxCatchUp
			owed = maxCatchUp
			k += bulk
			onShed(bulk)
		}
		for n := int64(0); n < owed; n++ {
			var schedSec float64
			kf := float64(k)
			if ramp > 0 && kf < rps*ramp/2 {
				schedSec = math.Sqrt(2 * kf * ramp / rps)
			} else {
				schedSec = kf/rps + maxf(ramp, 0)/2
			}
			k++
			emit(base.Add(time.Duration(schedSec * float64(time.Second))))
		}
	}
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// sharded ordered producer
// ---------------------------------------------------------------------------

// PublishFunc is one keyed single publish. The adapter must preserve order for
// a given key across successive calls made from the same goroutine.
type PublishFunc func(ctx context.Context, topic, key string, payload []byte) error

type pushJob struct {
	prop int
	seq  int64
	ts   int64
}

// ProducerConfig parameterises one flow's ingress producer.
type ProducerConfig struct {
	Topic      string
	Flow       Flow
	Rate       int
	RampSec    int
	Properties int
	HotProps   int // entities receiving HotFactor x a cold entity's share
	HotFactor  int // 0 or 1 = uniform
	Shards     int // ordered pusher shards; property -> shard is fixed
	ChanCap    int // per-shard queue depth; full => shed
	RatesPad   []byte
}

// propSelector deals out the next property to publish.
//
// Uniform is a plain round robin, unchanged from the pre-skew harness. Skewed
// uses exact integer Bresenham: across every window of hotWeight+coldCount
// events, exactly hotWeight land on the hot cohort, spread as evenly as integer
// arithmetic allows. Even spreading is the point — a hot entity delivered in
// bursts is a buffering test, while a hot entity delivered steadily is a
// head-of-line test, and only the second one is what a noisy neighbour is.
//
// No randomness: the same flags produce the same sequence on every run and on
// every system under test, so a skew cell is as reproducible as a uniform one.
type propSelector struct {
	properties int
	hotProps   int
	hotWeight  int
	coldCount  int
	total      int
	acc        int
	hotNext    int
	coldNext   int
	rrNext     int
}

func newPropSelector(properties, hotProps, hotFactor int) *propSelector {
	s := &propSelector{properties: properties}
	if hotProps <= 0 || hotFactor <= 1 || hotProps >= properties {
		return s // uniform
	}
	s.hotProps = hotProps
	s.hotWeight = hotProps * hotFactor
	s.coldCount = properties - hotProps
	s.total = s.hotWeight + s.coldCount
	return s
}

func (s *propSelector) next() int {
	if s.hotProps == 0 {
		p := s.rrNext
		s.rrNext++
		if s.rrNext >= s.properties {
			s.rrNext = 0
		}
		return p
	}
	s.acc += s.hotWeight
	if s.acc >= s.total {
		s.acc -= s.total
		p := s.hotNext
		s.hotNext++
		if s.hotNext >= s.hotProps {
			s.hotNext = 0
		}
		return p
	}
	p := s.hotProps + s.coldNext
	s.coldNext++
	if s.coldNext >= s.coldCount {
		s.coldNext = 0
	}
	return p
}

// RunProducer paces one flow and publishes its events SINGLY (batch of 1 — the
// ingress side deliberately stresses request rate, as a real channel manager's
// upstream does).
//
// Per-property publish order is guaranteed structurally: the single pacer
// goroutine assigns seq under no contention, and property p is routed to shard
// p%shards where exactly ONE pusher drains the channel in FIFO order. So
// seq order == broker arrival order for every property, on every system.
//
// A shed (shard channel full) does NOT consume a seq, so back-pressure can
// never manufacture a gap the verifier would read as data loss.
func RunProducer(ctx context.Context, wg *sync.WaitGroup, publish PublishFunc,
	cfg ProducerConfig, seqCounter []int64, c *Counters, maxSeq []int64) {

	shards := cfg.Shards
	if shards > cfg.Properties {
		shards = cfg.Properties
	}
	if shards < 1 {
		shards = 1
	}

	chans := make([]chan pushJob, shards)
	for i := range chans {
		chans[i] = make(chan pushJob, cfg.ChanCap)
	}

	for s := 0; s < shards; s++ {
		wg.Add(1)
		go func(ch chan pushJob) {
			defer wg.Done()
			for job := range ch {
				if ctx.Err() != nil {
					continue // drain-drop after shutdown: tail seqs read as in-flight
				}
				st := Stamp{Prop: job.prop, Flow: cfg.Flow, Seq: job.seq, TS: job.ts}
				payload := EncodeIngress(st, cfg.RatesPad)
				key := PartitionKey(job.prop)
				for {
					err := publish(ctx, cfg.Topic, key, payload)
					if err == nil {
						c.Published.Add(1)
						break
					}
					if ctx.Err() != nil {
						c.PushErr.Add(1)
						break
					}
					c.PushRetry.Add(1)
					time.Sleep(3 * time.Millisecond)
				}
			}
		}(chans[s])
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			for _, ch := range chans {
				close(ch)
			}
		}()
		sel := newPropSelector(cfg.Properties, cfg.HotProps, cfg.HotFactor)
		Pacer(ctx, cfg.Rate, cfg.RampSec, func(sched time.Time) {
			prop := sel.next()
			c.Offered.Add(1)
			ch := chans[prop%shards]
			// Reserve the seq only if the shard can take the job: a full
			// channel must shed WITHOUT burning a sequence number.
			select {
			case ch <- pushJob{prop: prop, seq: seqCounter[prop] + 1, ts: sched.UnixMicro()}:
				seqCounter[prop]++
				if seqCounter[prop] > maxSeq[prop] {
					maxSeq[prop] = seqCounter[prop]
				}
			default:
				c.Shed.Add(1)
			}
		}, func(n int64) { c.Shed.Add(n) })
	}()
}

// PartitionKey is the ordering key for a property. Every adapter maps this
// string onto its own lane concept (Queen partition, Kafka record key, Rabbit
// routing key, pgmq group) — the string itself is identical everywhere.
func PartitionKey(prop int) string {
	return "p" + itoa(prop)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
