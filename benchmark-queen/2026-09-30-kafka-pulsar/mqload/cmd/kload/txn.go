package main

// -txn: Kafka's exactly-once consume-transform-produce, the way franz-go documents it (kgo.GroupTransactSession,
// KIP-447): every worker is a group member of <topic>-in-g-t0 AND a transactional producer with its own
// TransactionalID. One transaction = the transformed records produced to the SAME partition of <topic>-out + the
// polled input offsets committed through TxnOffsetCommit, ended with EndTxn; read_committed fetches, stable
// offsets (RequireStable) on every rebalance. A rebalance or a produce error aborts the transaction and the session
// rewinds to the committed offsets (the inputs are consumed again). Readers of <topic>-out are plain group consumers
// with read_committed (they never see an open or aborted transaction's records).

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"mqload/internal/core"
)

type workerCfg struct {
	tx      *core.TxnConfig
	stats   *core.TxnStats
	prefix  string // transactional ids: <prefix>-tx-<ci>
	out     string
	timeout time.Duration
	begins  atomic.Int64
}

// opts are the producer half of a worker's client: transactional (acks=all + idempotence, required), the matrix's
// linger/compression/batch size, explicit partitions (the input's partition), read_committed.
func (w *workerCfg) opts(kc *kafkaFlags, ci int) []kgo.Opt {
	return []kgo.Opt{
		kgo.TransactionalID(fmt.Sprintf("%s-tx-%d", w.prefix, ci)),
		kgo.TransactionTimeout(w.timeout),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(kc.linger),
		kgo.ProducerBatchCompression(kc.codec),
		kgo.ProducerBatchMaxBytes(int32(kc.batchMaxBytes)),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordDeliveryTimeout(kc.deliveryTimeout),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
		kgo.ConcurrentTransactionsBackoff(kc.txnBackoff),
		kgo.MetadataMinAge(kc.txnMetaMinAge),
	}
}

// fill polls until -txn-size records or -txn-linger after the first one (or the stop).
func (c *consumers) fill(k *kconsumer) []*kgo.Record {
	w := c.worker
	N := w.tx.Size
	var recs []*kgo.Record
	var deadline time.Time
	for len(recs) < N && !c.stopping.Load() {
		wait := 500 * time.Millisecond // re-check the stop flag at least this often
		if !deadline.IsZero() {
			rem := time.Until(deadline)
			if rem <= 0 {
				break
			}
			wait = min(wait, rem)
		}
		pctx, cancel := context.WithTimeout(c.ctx, wait)
		fs := k.sess.PollRecords(pctx, N-len(recs))
		cancel()
		if fs.IsClientClosed() {
			break
		}
		fs.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			c.run.NoteErr("fetch-in", fmt.Errorf("%s/%d: %w", t, p, err))
		})
		n0 := len(recs)
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) > n0 {
			w.stats.Received(len(recs) - n0)
			if deadline.IsZero() {
				deadline = time.Now().Add(w.tx.Linger)
			}
		}
	}
	return recs
}

// workerLoop: fill, Begin, produce the transformed records, flush, End (commit unless a produce failed; the
// session aborts on its own after a rebalance). Records polled are committed even when stopping.
func (c *consumers) workerLoop(k *kconsumer) {
	defer c.wg.Done()
	w := c.worker
	fails := 0
	for !c.stopping.Load() {
		recs := c.fill(k)
		if len(recs) == 0 {
			continue
		}
		t := w.stats.Begin()
		if err := k.sess.Begin(); err != nil {
			w.stats.Failed("txn-begin", err)
			if fails++; fails >= 10 {
				c.run.NoteErr("txn-worker", fmt.Errorf("worker c%d: 10 failed begins in a row, worker stopped: %w", k.ci, err))
				return
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		w.begins.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 2*w.timeout)
		e := kgo.AbortingFirstErrPromise(k.sess.Client())
		for _, r := range recs {
			k.sess.Produce(ctx, &kgo.Record{Topic: w.out, Partition: r.Partition, Key: r.Key, Value: core.Transform(r.Value, k.ci)}, e.Promise())
		}
		ferr := k.sess.Client().Flush(ctx) // the produce requests go now (no linger wait), the promises complete
		perr := e.Err()
		committed, err := k.sess.End(ctx, kgo.TransactionEndTry(ferr == nil && perr == nil))
		cancel()
		switch {
		case err != nil:
			w.stats.Failed("txn-end", err)
			fails++
			time.Sleep(50 * time.Millisecond)
		case committed:
			w.stats.Committed(t, len(recs))
			fails = 0
		case ferr != nil || perr != nil:
			w.stats.Aborted("txn-abort-produce", errors.Join(ferr, perr))
		default:
			w.stats.Aborted("txn-abort-rebalance", errors.New("aborted: the group rebalanced during the transaction (inputs consumed again)"))
		}
	}
}

// ---------------------------------------------------------------------------
// verifier

// runVerify (after every load process exited): <topic>-out in full from offset 0, read_committed (aborted records
// skipped, a record's partition+offset is its position), then <topic>-in from the workers' committed offsets to
// the end (inputs never processed). Exit 0 = PASS, 3 = FAIL, 1 = the scan failed.
func runVerify(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, kc *kafkaFlags, adm *admin) int {
	t0 := time.Now()
	exp, err := core.LoadIds(tx.IdsDir)
	if err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	in, out := core.InTopic(cfg), core.OutTopic(cfg)
	group := in + "-g-t0"
	fmt.Printf("[verify] kafka %s: %d ledgers from %s; reading %s from offset 0 (read_committed), then %s from group %s's committed offsets\n",
		kc.brokers, exp.Files, tx.IdsDir, out, in, group)
	ta := core.NewTally(exp)
	P := int32(cfg.Partitions)

	from := map[int32]kgo.Offset{}
	for p := int32(0); p < P; p++ {
		from[p] = kgo.NewOffset().AtStart()
	}
	if err := scanTopic(ctx, kc, tx, "out "+out, out, from, func(r *kgo.Record) {
		ta.Out(r.Value, core.PosNum(uint64(r.Partition), uint64(r.Offset)))
	}); err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}

	octx, cancel := context.WithTimeout(ctx, 60*time.Second)
	offs, err := adm.adm.FetchOffsets(octx, group)
	cancel()
	if err != nil {
		fmt.Printf("FATAL fetch offsets of %s: %v\n", group, err)
		return 1
	}
	from = map[int32]kgo.Offset{}
	committed := 0
	for p := int32(0); p < P; p++ {
		from[p] = kgo.NewOffset().AtStart()
		if o, ok := offs.Lookup(in, p); ok && o.Err == nil && o.At >= 0 {
			from[p] = kgo.NewOffset().At(o.At)
			committed++
		}
	}
	fmt.Printf("[verify] %s: committed offsets on %d of %d partitions (the rest from the start)\n", group, committed, P)
	if err := scanTopic(ctx, kc, tx, "rest of "+in, in, from, func(r *kgo.Record) { ta.Pending(r.Value) }); err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	v := ta.Verdict("kafka")
	v.ScanS = time.Since(t0).Seconds()
	v.Print(cfg.Out)
	if v.Pass {
		return 0
	}
	return 3
}

// scanTopic reads the given partitions of topic from the given offsets (read_committed, no group) until no record
// arrived for -verify-idle.
func scanTopic(ctx context.Context, kc *kafkaFlags, tx *core.TxnConfig, what, topic string, from map[int32]kgo.Offset, rec func(*kgo.Record)) error {
	cl, err := kgo.NewClient(append(kc.baseOpts("kload-verify"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: from}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.FetchMaxBytes(64<<20),
		kgo.FetchMaxPartitionBytes(int32(kc.fetchMaxPartBytes)),
	)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	var n atomic.Int64
	var errMu sync.Mutex
	errs := map[string]int{}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sctx.Err() == nil {
			fs := cl.PollRecords(sctx, 10000)
			if fs.IsClientClosed() {
				return
			}
			fs.EachError(func(t string, p int32, err error) {
				if errors.Is(err, context.Canceled) {
					return
				}
				errMu.Lock()
				errs[err.Error()]++
				errMu.Unlock()
			})
			fs.EachRecord(func(r *kgo.Record) {
				rec(r)
				n.Add(1)
			})
		}
	}()
	core.IdleScan(what, tx.VerifyIdle, tx.VerifyMax, n.Load, ctx.Done())
	cancel()
	<-done
	if len(errs) > 0 {
		ks := make([]string, 0, len(errs))
		for k := range errs {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		fmt.Printf("[verify] %s: fetch errors: %v\n", what, ks)
	}
	return nil
}

var _ = kadm.StringPtr // kadm is used through admin
