package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"mqload/internal/core"
)

type admin struct {
	kc  *kafkaFlags
	cl  *kgo.Client
	adm *kadm.Client
}

func newAdmin(kc *kafkaFlags) *admin {
	cl, err := kgo.NewClient(kc.baseOpts("kload-admin")...)
	if err != nil {
		fatal("admin client: %v", err)
	}
	adm := kadm.NewClient(cl)
	adm.SetTimeoutMillis(60000) // broker-side timeout of CreateTopics/CreatePartitions
	return &admin{kc: kc, cl: cl, adm: adm}
}

func (a *admin) close() { a.cl.Close() }

// setupTopics: create (chunked, idempotent), wait for the readiness gate
// (leaders + full ISR, plus every replica log on disk when this process
// created or warms), optionally warm (one record without "ts" per partition,
// acks=all) and re-check readiness. Timings go to the JSON result.
func (a *admin) setupTopics(ctx context.Context, run *core.Run, names []string) error {
	cfg := run.Cfg
	if cfg.Partitions < 1 {
		return fmt.Errorf("kload needs -partitions >= 1")
	}
	P := cfg.Partitions
	tctx, cancel := context.WithTimeout(ctx, a.kc.topicTimeout)
	defer cancel()
	t := time.Now()
	full := cfg.Warm
	if cfg.Create {
		n, err := a.createTopics(tctx, names, P)
		if err != nil {
			return err
		}
		full = full || n > 0
		run.SetInfo("topic_create_s", time.Since(t).Seconds())
	}
	if err := a.waitReady(tctx, names, P, full, "topic"); err != nil {
		return err
	}
	readyS := time.Since(t).Seconds()
	run.SetInfo("topic_ready_s", readyS)
	fmt.Printf("  [topic] READY after %.1fs: %d topics x %d partitions, leaders + full ISR%s\n", readyS, len(names), P,
		map[bool]string{true: " + every replica log on disk", false: ""}[full])
	if cfg.Warm {
		tw := time.Now()
		if err := a.warm(tctx, run, names, P); err != nil {
			return err
		}
		ws := time.Since(tw).Seconds()
		if err := a.waitReady(tctx, names, P, true, "warm"); err != nil {
			return err
		}
		rs := time.Since(tw).Seconds() - ws
		run.SetInfo("warm_s", ws)
		run.SetInfo("rewarm_ready_s", rs)
		fmt.Printf("  [warm] one record (no \"ts\") into each of %d partitions, acks=all, in %.1fs; READY again after %.1fs\n", len(names)*P, ws, rs)
	}
	a.printConfigs(ctx, run, names[0])
	return nil
}

func (a *admin) topicConfigs() (map[string]*string, error) {
	cfgs := map[string]*string{}
	for _, kv := range strings.Split(a.kc.topicConfig, ",") {
		if kv = strings.TrimSpace(kv); kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("bad -topic-config entry %q", kv)
		}
		cfgs[k] = kadm.StringPtr(v)
	}
	if _, ok := cfgs["min.insync.replicas"]; !ok && a.kc.minISR > 0 {
		cfgs["min.insync.replicas"] = kadm.StringPtr(fmt.Sprint(a.kc.minISR))
	}
	return cfgs, nil
}

// createTopics creates the missing topics and grows short ones, at most
// -create-chunk partitions per request. Existing topics are left alone, so
// several processes may run it concurrently. Returns the requests issued.
func (a *admin) createTopics(ctx context.Context, names []string, P int) (int, error) {
	cfgs, err := a.topicConfigs()
	if err != nil {
		return 0, err
	}
	td, err := a.adm.ListTopics(ctx, names...)
	if err != nil {
		return 0, fmt.Errorf("list topics: %v", err)
	}
	var missing []string
	grow := map[string]int{}
	for _, n := range names {
		d, ok := td[n]
		switch {
		case !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition):
			missing = append(missing, n)
		case d.Err != nil:
			return 0, fmt.Errorf("topic %s: %v", n, d.Err)
		case len(d.Partitions) < P:
			grow[n] = len(d.Partitions)
		case len(d.Partitions) > P:
			return 0, fmt.Errorf("topic %s exists with %d partitions > -partitions %d", n, len(d.Partitions), P)
		}
	}
	if len(missing) == 0 && len(grow) == 0 {
		fmt.Printf("  [topic] all %d topics exist with %d partitions\n", len(names), P)
		return 0, nil
	}
	grownExisting := len(grow)
	chunk := a.kc.createChunk
	first := min(P, chunk)
	perReq := max(1, chunk/first)
	reqs := 0
	t := time.Now()
	for i := 0; i < len(missing); i += perReq {
		batch := missing[i:min(i+perReq, len(missing))]
		resp, err := a.adm.CreateTopics(ctx, int32(first), int16(a.kc.rf), cfgs, batch...)
		reqs++
		if err != nil {
			return reqs, fmt.Errorf("create topics: %v", err)
		}
		for _, r := range resp {
			switch {
			case r.Err == nil:
			case errors.Is(r.Err, kerr.TopicAlreadyExists):
				// another process created it; the readiness gate checks its size
			case errors.Is(r.Err, kerr.RequestTimedOut):
				fmt.Printf("  [topic] create %s: broker-side timeout, the readiness gate decides\n", r.Topic)
			default:
				return reqs, fmt.Errorf("create %s: %v %s", r.Topic, r.Err, r.ErrMessage)
			}
		}
		if first < P {
			for _, n := range batch {
				grow[n] = first
			}
		}
	}
	gnames := make([]string, 0, len(grow))
	for n := range grow {
		gnames = append(gnames, n)
	}
	sort.Strings(gnames)
	for _, n := range gnames {
		for have := grow[n]; have < P; {
			next := min(have+chunk, P)
			r, err := a.adm.UpdatePartitions(ctx, next, n)
			reqs++
			if err == nil {
				err = r.Error()
			}
			if err != nil {
				if errors.Is(err, kerr.InvalidPartitions) { // a concurrent creator grew it
					if cur, lerr := a.adm.ListTopics(ctx, n); lerr == nil && len(cur[n].Partitions) >= next {
						have = len(cur[n].Partitions)
						continue
					}
				}
				return reqs, fmt.Errorf("create partitions %s -> %d: %v", n, next, err)
			}
			have = next
		}
	}
	fmt.Printf("  [topic] %d topics created, %d grown, %d partitions each x RF %d accepted by the controller in %.1fs (%d requests of <= %d partitions)\n",
		len(missing), grownExisting, P, a.kc.rf, time.Since(t).Seconds(), reqs, chunk)
	return reqs, nil
}

// notReady returns "" when every partition of every topic has a leader and a
// full ISR (and, with full, every replica's log exists on its broker: a new
// partition is born with ISR = all replicas before any broker created it).
func (a *admin) notReady(ctx context.Context, names []string, P int, full bool) string {
	td, err := a.adm.ListTopics(ctx, names...)
	if err != nil {
		return "metadata: " + err.Error()
	}
	missing, wrong, noLeader, shortISR, replicas := 0, 0, 0, 0, 0
	for _, n := range names {
		d, ok := td[n]
		if !ok || d.Err != nil {
			missing++
			continue
		}
		if len(d.Partitions) != P {
			wrong++
			continue
		}
		for _, p := range d.Partitions {
			if p.Leader < 0 || p.Err != nil {
				noLeader++
			}
			if len(p.ISR) < len(p.Replicas) {
				shortISR++
			}
			replicas += len(p.Replicas)
		}
	}
	if missing+wrong+noLeader+shortISR > 0 {
		return fmt.Sprintf("%d topics missing, %d with != %d partitions, %d partitions without leader, %d with ISR < replicas", missing, wrong, P, noLeader, shortISR)
	}
	if !full {
		return ""
	}
	dirs, err := a.adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return "describe log dirs: " + err.Error()
	}
	onDisk := 0
	dirs.Each(func(ld kadm.DescribedLogDir) {
		for _, n := range names {
			onDisk += len(ld.Topics[n])
		}
	})
	if onDisk < replicas {
		return fmt.Sprintf("%d/%d replica logs on disk", onDisk, replicas)
	}
	return ""
}

func (a *admin) waitReady(ctx context.Context, names []string, P int, full bool, what string) error {
	poll := 200 * time.Millisecond
	if len(names)*P > 10000 {
		poll = time.Second
	}
	t := time.Now()
	var okSince, lastLog time.Time
	for {
		why := a.notReady(ctx, names, P, full)
		if why != "" {
			okSince = time.Time{}
		} else if okSince.IsZero() {
			okSince = time.Now()
		}
		if why == "" && time.Since(okSince) >= a.kc.topicSettle {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: topics not ready within -topic-timeout %v: %s", what, a.kc.topicTimeout, why)
		}
		if time.Since(lastLog) >= 10*time.Second {
			lastLog = time.Now()
			if why == "" {
				why = "ready, holding for -topic-settle"
			}
			fmt.Printf("  [%s] t=+%.0fs %s\n", what, time.Since(t).Seconds(), why)
		}
		select {
		case <-ctx.Done():
		case <-time.After(poll):
		}
	}
}

// warm writes one record without "ts" into every partition with acks=all, so
// the load meets partitions whose first append (leader epoch checkpoint,
// segment and index setup, producer state) already happened.
func (a *admin) warm(ctx context.Context, run *core.Run, names []string, P int) error {
	wcl, err := kgo.NewClient(append(a.kc.baseOpts("kload-warm"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(a.kc.codec),
		kgo.MaxBufferedRecords(len(names)*P+1),
		kgo.RecordDeliveryTimeout(a.kc.topicTimeout))...)
	if err != nil {
		return err
	}
	defer wcl.Close()
	val := run.Pool.Warm()
	var wg sync.WaitGroup
	var fail atomic.Int64
	var first atomic.Value
	for _, n := range names {
		for p := 0; p < P; p++ {
			wg.Add(1)
			wcl.Produce(ctx, &kgo.Record{Topic: n, Partition: int32(p), Value: val}, func(_ *kgo.Record, err error) {
				if err != nil {
					fail.Add(1)
					first.CompareAndSwap(nil, err.Error())
				}
				wg.Done()
			})
		}
	}
	wg.Wait()
	if fail.Load() > 0 {
		return fmt.Errorf("warm: %d of %d records failed, first: %v", fail.Load(), len(names)*P, first.Load())
	}
	return nil
}

// printConfigs records the topic and broker configs actually in force.
func (a *admin) printConfigs(ctx context.Context, run *core.Run, topic string) {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out := map[string]string{}
	keep := func(prefix string, rc kadm.ResourceConfigs, keys map[string]bool) {
		for _, r := range rc {
			for _, c := range r.Configs {
				if keys[c.Key] {
					v := "<nil>"
					if c.Value != nil {
						v = *c.Value
					}
					out[prefix+c.Key] = v + " (" + c.Source.String() + ")"
				}
			}
		}
	}
	if rc, err := a.adm.DescribeTopicConfigs(cctx, topic); err == nil {
		keep("topic.", rc, map[string]bool{"min.insync.replicas": true, "retention.ms": true, "segment.bytes": true, "compression.type": true,
			"flush.messages": true, "flush.ms": true, "max.message.bytes": true, "message.timestamp.type": true, "unclean.leader.election.enable": true})
	}
	if md, err := a.adm.BrokerMetadata(cctx); err == nil && len(md.Brokers) > 0 {
		run.SetInfo("brokers", len(md.Brokers))
		if rc, err := a.adm.DescribeBrokerConfigs(cctx, md.Brokers[0].NodeID); err == nil {
			keep("broker.", rc, map[string]bool{"num.network.threads": true, "num.io.threads": true, "num.replica.fetchers": true,
				"log.flush.interval.messages": true, "log.flush.interval.ms": true, "min.insync.replicas": true, "default.replication.factor": true,
				"group.coordinator.rebalance.protocols": true, "group.consumer.assignors": true, "group.initial.rebalance.delay.ms": true,
				"max.incremental.fetch.session.cache.slots": true, "compression.type": true})
		}
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  [config] %s = %s\n", k, out[k])
	}
	run.SetInfo("configs", out)
}
