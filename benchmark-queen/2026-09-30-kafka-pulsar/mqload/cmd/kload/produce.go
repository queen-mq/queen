package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mqload/internal/core"
)

type producers struct {
	run     *core.Run
	clients []*kgo.Client
	topics  []string
}

// newProducers creates -producers clients and opens their connections to
// every broker and loads their producer IDs before READY, so the first units
// do not pay for connection setup or InitProducerID.
func newProducers(ctx context.Context, run *core.Run, kc *kafkaFlags, topics []string) (*producers, error) {
	cfg := run.Cfg
	p := &producers{run: run, topics: topics}
	if cfg.Rate <= 0 {
		fmt.Println("  [produce] -rate 0: no producers")
		return p, nil
	}
	// Buffering above what -max-inflight can put in flight: every in-flight
	// unit could sit on one client, so TryProduce never hits ErrMaxBuffered
	// (the pacer's semaphore is the only thing that sheds).
	maxBuf := cfg.MaxInflight*cfg.MaxUnit() + 1024
	part := kgo.ManualPartitioner()
	if cfg.Keyed() {
		part = kgo.StickyKeyPartitioner(nil) // keyed records: murmur2(key) & 0x7fffffff % n, exactly Java's
	}
	for i := 0; i < kc.producers; i++ {
		opts := append(kc.baseOpts(fmt.Sprintf("kload-%d-p%d", cfg.LoaderIndex, i)),
			kgo.RequiredAcks(kgo.AllISRAcks()), // + idempotent (franz-go default)
			kgo.ProducerLinger(kc.linger),
			kgo.ProducerBatchCompression(kc.codec),
			kgo.ProducerBatchMaxBytes(int32(kc.batchMaxBytes)),
			kgo.MaxBufferedRecords(maxBuf),
			kgo.RecordPartitioner(part),
			kgo.RecordDeliveryTimeout(kc.deliveryTimeout),
		)
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, err
		}
		p.clients = append(p.clients, cl)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, len(p.clients))
	t := time.Now()
	for i, cl := range p.clients {
		wg.Add(1)
		go func(i int, cl *kgo.Client) {
			defer wg.Done()
			if err := cl.EnsureProduceConnectionIsOpen(wctx); err != nil {
				errs[i] = err
				return
			}
			_, _, errs[i] = cl.ProducerID(wctx)
		}(i, cl)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("producer warm-up: %v", err)
		}
	}
	fmt.Printf("  [produce] %d clients connected to every broker with producer IDs in %.2fs; max buffered %d records per client\n",
		len(p.clients), time.Since(t).Seconds(), maxBuf)
	run.SetInfo("producer_max_buffered_records", maxBuf)
	return p, nil
}

// send hands one unit to its client (unit u -> client u % producers). It
// never blocks: TryProduce fails instead of waiting for buffer space, and the
// buffer is sized so that it cannot fill.
func (p *producers) send(u *core.Unit) {
	cl := p.clients[u.Seq%uint64(len(p.clients))]
	topic := p.topics[u.Topic]
	recs := make([]kgo.Record, u.N) // one allocation per unit
	var key []byte
	if u.Key != "" {
		key = []byte(u.Key)
	}
	promise := func(_ *kgo.Record, err error) { u.MsgDone(err) }
	for j := range recs {
		r := &recs[j]
		r.Topic = topic
		r.Value = u.Payloads[j]
		r.Timestamp = u.Sched
		if key != nil {
			r.Key = key
		} else {
			r.Partition = int32(u.Entity)
		}
		cl.TryProduce(context.Background(), r, promise)
	}
}

func (p *producers) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, cl := range p.clients {
		wg.Add(1)
		go func(cl *kgo.Client) {
			defer wg.Done()
			_ = cl.Flush(ctx)
			cl.Close()
		}(cl)
	}
	wg.Wait()
}
