package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar"

	"mqload/internal/core"
)

// routedMsg carries the explicit partition of an E=0 message next to the
// ProducerMessage handed to the client. ProducerMessage is the FIRST field, so
// the *ProducerMessage the client passes to the MessageRouter (the caller's
// pointer, synchronously inside SendAsync) is also the *routedMsg: routing
// costs no lookup, lock or per-message property (properties would go on the
// wire). Used by the warm-up and by -producer-shard=false.
type routedMsg struct {
	pulsar.ProducerMessage
	magic uint32
	part  int
}

const routedMagic = 0x6d716c64

// routeExplicit is the MessageRouter for E=0: the unit's partition index.
func routeExplicit(m *pulsar.ProducerMessage, _ pulsar.TopicMetadata) int {
	rm := (*routedMsg)(unsafe.Pointer(m))
	if rm.magic != routedMagic {
		panic("pload: explicit router got a message that is not a routedMsg")
	}
	return rm.part
}

// shardMode: -producer-shard with explicit partitions (E=0, P>0).
func shardMode(cfg *core.Config, pc *pulsarFlags) bool {
	return pc.producerShard && !cfg.Keyed() && cfg.Partitions > 0
}

// producedPartitions: partitions this process produces to, over all topics.
func producedPartitions(cfg *core.Config, pc *pulsarFlags) int64 {
	if shardMode(cfg, pc) {
		return int64(cfg.Topics) * int64(len(core.ShardPartitions(cfg.LoaderIndex, cfg.Loaders, cfg.Partitions)))
	}
	return int64(cfg.Topics) * int64(max(cfg.Partitions, 1))
}

// pendingSizing returns MaxPendingMessages per partition producer and the
// client memory limit. The Go client's MaxPendingMessages is PER PARTITION
// producer and preallocates 24 B per slot (a channel slot + a ring slot), so
// the SPEC default (max-inflight x unit x 2: never block) is capped by
// -pending-budget-mb over all partitions this process produces to (floor: the
// client default 1000). SendAsync never blocks either way
// (DisableBlockIfQueueFull): a full partition queue fails that send (a push
// error, visible), it does not stall the pacer.
func pendingSizing(run *core.Run, pc *pulsarFlags) (int, int64) {
	cfg := run.Cfg
	want := pc.maxPending
	if want <= 0 {
		want = cfg.MaxInflight * cfg.MaxUnit() * 2
	}
	parts := max(producedPartitions(cfg, pc), 1)
	capPer := int((int64(pc.pendingBudgetMB) << 20) / (24 * parts))
	per := min(want, max(1000, capPer))
	if per < want && cfg.Rate > 0 {
		fmt.Printf("WARN max-pending %d per partition producer would preallocate %d MB over %d partitions (24 B/slot); using %d (-pending-budget-mb %d)\n",
			want, int64(want)*24*parts>>20, parts, per, pc.pendingBudgetMB)
	}
	// Memory limit: accounting only (nothing preallocated); above what the
	// in-flight cap can hold, so it never binds.
	mem := int64(cfg.MaxInflight) * int64(cfg.MaxUnit()) * int64(run.Pool.AvgStamped+64) * 2
	mem = max(mem, 64<<20)
	run.SetInfo("max_pending_per_partition", per)
	run.SetInfo("memory_limit_bytes", mem)
	return per, mem
}

// pslot is one lazily created per-partition producer (-producer-shard).
type pslot struct {
	prod     atomic.Pointer[pulsar.Producer] // set once ready (fast path)
	mu       sync.Mutex
	creating bool
	failedAt time.Time
	err      error
	waiters  []*core.Unit
}

type producers struct {
	run    *core.Run
	pc     *pulsarFlags
	client pulsar.Client
	shard  bool
	opts   pulsar.ProducerOptions // template for per-partition producers
	names  []string

	prods []pulsar.Producer // partitioned mode: one per topic
	slots [][]*pslot        // shard mode: [topic][partition], nil outside the shard

	createSem chan struct{}
	created   atomic.Int64
	createErr atomic.Int64
	createNs  atomic.Int64
	waiting   atomic.Int64
}

// newProducers: E=0 with -producer-shard: per-partition producers on this
// process's shard, created lazily on first use (or all before READY with
// -producer-eager). Otherwise one partitioned producer per topic, created
// before READY (E=0: explicit-partition router; E>0: key e<id>, default key
// hash routing).
func newProducers(ctx context.Context, run *core.Run, pc *pulsarFlags, client pulsar.Client, pending int, names []string) (*producers, error) {
	cfg := run.Cfg
	p := &producers{run: run, pc: pc, client: client, names: names, shard: shardMode(cfg, pc),
		createSem: make(chan struct{}, pc.adminConc)}
	if cfg.Rate <= 0 {
		fmt.Println("  [produce] -rate 0: no producers")
		return p, nil
	}
	p.opts = pulsar.ProducerOptions{
		CompressionType:         pc.compressionV,
		BatchingMaxPublishDelay: pc.batchDelay,
		BatchingMaxMessages:     uint(pc.batchMaxMsgs),
		BatchingMaxSize:         uint(pc.batchMaxBytes),
		MaxPendingMessages:      pending,
		DisableBlockIfQueueFull: true, // the pacer must never block
		SendTimeout:             pc.sendTimeout,
	}
	if pc.keyBatching {
		p.opts.BatcherBuilderType = pulsar.KeyBasedBatchBuilder
	}
	if p.shard {
		own := core.ShardPartitions(cfg.LoaderIndex, cfg.Loaders, cfg.Partitions)
		p.slots = make([][]*pslot, cfg.Topics)
		for t := range p.slots {
			p.slots[t] = make([]*pslot, cfg.Partitions)
			for _, part := range own {
				p.slots[t][part] = &pslot{}
			}
		}
		run.SetInfo("producer_mode", "shard")
		run.SetInfo("producer_shard_partitions", len(own)*cfg.Topics)
		fmt.Printf("  [produce] producer shard %d/%d: %d partitions per topic (p %% %d == %d) x %d topics, per-partition producers created %s\n",
			cfg.LoaderIndex, cfg.Loaders, len(own), cfg.Loaders, cfg.LoaderIndex, cfg.Topics,
			map[bool]string{true: "before READY (-producer-eager)", false: "lazily on first use"}[pc.producerEager])
		if pc.producerEager {
			t0 := time.Now()
			err := parallel(ctx, cfg.Topics*len(own), pc.adminConc, func(ctx context.Context, i int) error {
				t, part := i/len(own), own[i%len(own)]
				sl := p.slots[t][part]
				p.createNow(t, part, sl)
				if sl.prod.Load() == nil {
					return fmt.Errorf("producer %s-partition-%d: %v", p.names[t], part, sl.err)
				}
				return nil
			})
			if err != nil {
				p.close()
				return nil, err
			}
			fmt.Printf("  [produce] %d per-partition producers created in %.1fs\n", p.created.Load(), time.Since(t0).Seconds())
		}
		return p, nil
	}
	p.prods = make([]pulsar.Producer, cfg.Topics)
	t0 := time.Now()
	err := parallel(ctx, len(p.names), pc.adminConc, func(ctx context.Context, i int) error {
		opts := p.opts
		opts.Topic = pc.fullName(p.names[i])
		if !cfg.Keyed() {
			opts.MessageRouter = routeExplicit // E=0: entity e IS partition e
		}
		prod, err := client.CreateProducer(opts)
		if err != nil {
			return fmt.Errorf("producer %s: %v", p.names[i], err)
		}
		p.prods[i] = prod
		p.created.Add(1)
		return nil
	})
	if err != nil {
		p.close()
		return nil, err
	}
	run.SetInfo("producer_mode", "partitioned")
	fmt.Printf("  [produce] %d partitioned producers (one per topic, %d partition producers each) created in %.1fs\n",
		len(p.prods), max(cfg.Partitions, 1), time.Since(t0).Seconds())
	return p, nil
}

// send hands one unit to its producer. Never blocks (DisableBlockIfQueueFull;
// a unit whose shard producer is still being created waits in the slot, not
// in the pacer). The latency sample is taken at the callback of the unit's
// last message (core.Unit.MsgDone).
func (p *producers) send(u *core.Unit) {
	if p.shard {
		sl := p.slots[u.Topic][u.Entity]
		if pr := sl.prod.Load(); pr != nil {
			sendUnit(*pr, u)
			return
		}
		p.sendSlow(u, sl)
		return
	}
	prod := p.prods[u.Topic]
	msgs := make([]routedMsg, u.N) // one allocation per unit
	cb := func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) { u.MsgDone(err) }
	part := int(u.Entity)
	for j := range msgs {
		m := &msgs[j]
		m.magic, m.part = routedMagic, part
		m.Payload = u.Payloads[j]
		m.Key = u.Key // "" for E=0
		prod.SendAsync(context.Background(), &m.ProducerMessage, cb)
	}
}

// sendUnit sends a unit on a single-partition producer.
func sendUnit(prod pulsar.Producer, u *core.Unit) {
	msgs := make([]pulsar.ProducerMessage, u.N) // one allocation per unit
	cb := func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) { u.MsgDone(err) }
	for j := range msgs {
		msgs[j].Payload = u.Payloads[j]
		prod.SendAsync(context.Background(), &msgs[j], cb)
	}
}

func failUnit(u *core.Unit, err error) {
	for j := 0; j < u.N; j++ {
		u.MsgDone(err)
	}
}

// sendSlow: the slot's producer is not ready: queue the unit on the slot and
// start the creation (in the background) if nobody did.
func (p *producers) sendSlow(u *core.Unit, sl *pslot) {
	sl.mu.Lock()
	if pr := sl.prod.Load(); pr != nil {
		sl.mu.Unlock()
		sendUnit(*pr, u)
		return
	}
	if !sl.creating && sl.err != nil && time.Since(sl.failedAt) < 2*time.Second {
		err := sl.err
		sl.mu.Unlock()
		failUnit(u, err)
		return
	}
	sl.waiters = append(sl.waiters, u)
	start := !sl.creating
	sl.creating = true
	sl.mu.Unlock()
	p.waiting.Add(1)
	if start {
		go p.createNow(u.Topic, int(u.Entity), sl)
	}
}

// createNow creates the slot's producer (at most -admin-conc at a time) and
// flushes the units that waited for it.
func (p *producers) createNow(t, part int, sl *pslot) {
	sl.mu.Lock()
	if sl.prod.Load() != nil {
		sl.mu.Unlock()
		return
	}
	sl.creating = true
	sl.mu.Unlock()
	p.createSem <- struct{}{}
	opts := p.opts
	opts.Topic = p.pc.fullName(fmt.Sprintf("%s-partition-%d", p.names[t], part))
	t0 := time.Now()
	prod, err := p.client.CreateProducer(opts)
	<-p.createSem
	p.createNs.Add(time.Since(t0).Nanoseconds())
	sl.mu.Lock()
	waiters := sl.waiters
	sl.waiters = nil
	sl.creating = false
	if err != nil {
		sl.err, sl.failedAt = err, time.Now()
		p.createErr.Add(1)
		p.run.NoteErr("create-producer", fmt.Errorf("%s: %w", opts.Topic, err))
	} else {
		sl.err = nil
		sl.prod.Store(&prod)
		p.created.Add(1)
	}
	sl.mu.Unlock()
	p.waiting.Add(-int64(len(waiters)))
	for _, u := range waiters {
		if err != nil {
			failUnit(u, err)
		} else {
			sendUnit(prod, u)
		}
	}
}

// summary prints and records how many producers exist.
func (p *producers) summary() {
	n := p.created.Load()
	if n == 0 && !p.shard {
		return
	}
	avg := 0.0
	if n > 0 {
		avg = float64(p.createNs.Load()) / float64(n) / 1e6
	}
	mode := "partitioned producers (one per topic)"
	if p.shard {
		mode = "per-partition producers (shard)"
	}
	fmt.Printf("[info] producers created: %d %s, %d creation errors, avg creation %.1f ms\n", n, mode, p.createErr.Load(), avg)
	p.run.SetInfo("producers_created", n)
	p.run.SetInfo("producer_create_errors", p.createErr.Load())
}

func (p *producers) close() {
	var all []pulsar.Producer
	for _, prod := range p.prods {
		if prod != nil {
			all = append(all, prod)
		}
	}
	for _, row := range p.slots {
		for _, sl := range row {
			if sl != nil {
				if pr := sl.prod.Load(); pr != nil {
					all = append(all, *pr)
				}
			}
		}
	}
	sem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	for _, prod := range all {
		wg.Add(1)
		sem <- struct{}{}
		go func(prod pulsar.Producer) {
			defer func() { <-sem; wg.Done() }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = prod.FlushWithCtx(ctx)
			prod.Close()
		}(prod)
	}
	wg.Wait()
}
