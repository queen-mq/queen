package main

// -txn: Pulsar's exactly-once consume-transform-produce with the transaction coordinator, the way
// pulsar-client-go v0.21 offers it: client.NewTransaction (a TC round trip), producer messages sent WITH the
// transaction (the TC registers each output partition), the inputs acknowledged WITH the transaction (AckWithTxn:
// pending acks until the commit), then txn.Commit (EndTxn: the TC commits the transaction buffer of every output
// partition and the pending acks of every input subscription). Every worker owns a failover slice of
// <topic>-in's partitions (consumer slices, as the plain runs) and one producer per owned partition of <topic>-out
// (output partition = input partition), all on its OWN client: the Go client serializes TC requests per client
// and coordinator, and the batch builder stamps a batch with the first message's transaction, so producers are
// never shared by two open transactions. A transaction's outputs are flushed before it commits (no batch is
// ever left half way). Readers of <topic>-out are the plain consumers (Pulsar dispatches committed messages only).
//
// Whole batch entries per transaction (measured 10-01): pulsar-client-go 0.21 acknowledges a batch message with
// the CUMULATIVE ack set of its batch and the batch's last message with a whole-entry ack, and the broker rejects a
// transactional ack of an entry that has a pending ack (TransactionConflictException "... in pending ack status",
// ~1 transaction in 10 at 10 per transaction over input batches of 100), aborting the transaction. So the workers
// run without batch-index acks (an entry is acked, with the transaction, by its last message: one ack request per
// entry) and a transaction always holds whole entries; the feeder caps the input batches at -txn-size messages
// (main.go), so a transaction is one entry of -txn-size messages. After an abort the worker re-subscribes (its
// receiver queue is dropped and the broker redelivers from the mark-delete position): a failover redelivery while
// the old copies sit in the queue would process an input twice.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"mqload/internal/core"
)

type pworker struct {
	ci      int
	client  pulsar.Client
	opts    pulsar.ConsumerOptions
	cons    pulsar.Consumer
	ch      <-chan pulsar.ConsumerMessage
	parts   map[string]int // input partition topic -> partition index
	prods   map[int]pulsar.Producer
	ready   [][]pulsar.ConsumerMessage          // complete input entries not in a transaction yet
	partial map[string][]pulsar.ConsumerMessage // per input partition: the entry being received
}

type pworkers struct {
	run      *core.Run
	pc       *pulsarFlags
	tx       *core.TxnConfig
	stats    *core.TxnStats
	in, out  string
	list     []*pworker
	stopping atomic.Bool
	wg       sync.WaitGroup
	idle     int
	dropped  atomic.Int64 // messages held outside a transaction at the stop (left unacked: still pending in the input)
	resubs   atomic.Int64
}

func newPWorkers(run *core.Run, pc *pulsarFlags, tx *core.TxnConfig, stats *core.TxnStats) *pworkers {
	return &pworkers{run: run, pc: pc, tx: tx, stats: stats, in: core.InTopic(run.Cfg), out: core.OutTopic(run.Cfg)}
}

func (w *pworkers) clientOpts() pulsar.ClientOptions {
	pc := w.pc
	return pulsar.ClientOptions{
		URL:                     pc.url,
		OperationTimeout:        pc.opTimeout,
		ConnectionTimeout:       pc.connTimeout,
		MaxConnectionsPerBroker: pc.connsPerBroker,
		MemoryLimitBytes:        64 << 20,
		EnableTransaction:       true,
		Logger:                  newLogger(pc.clientLog),
	}
}

// start creates every worker: its client (with the TC), its consumer on its partitions of <topic>-in and its
// producers on the same partitions of <topic>-out, before READY.
func (w *pworkers) start(ctx context.Context) error {
	cfg, pc := w.run.Cfg, w.pc
	w.list = make([]*pworker, cfg.Consumers)
	t := time.Now()
	var nprod atomic.Int64
	err := parallel(ctx, cfg.Consumers, pc.adminConc, func(ctx context.Context, i int) error {
		ci := cfg.ConsOffset + i
		parts := cfg.ConsumerPartitions(ci, 0)
		if len(parts) == 0 {
			return nil // more workers than partitions: this one owns none
		}
		cl, err := pulsar.NewClient(w.clientOpts())
		if err != nil {
			return fmt.Errorf("worker c%d client: %v", ci, err)
		}
		pw := &pworker{ci: ci, client: cl, parts: map[string]int{}, prods: map[int]pulsar.Producer{}, partial: map[string][]pulsar.ConsumerMessage{}}
		topics := make([]string, 0, len(parts))
		for _, p := range parts {
			name := pc.fullName(fmt.Sprintf("%s-partition-%d", w.in, p))
			topics = append(topics, name)
			pw.parts[name] = p
		}
		opts := pulsar.ConsumerOptions{
			SubscriptionName:               pc.sub,
			Type:                           pc.subTypeV,
			Name:                           fmt.Sprintf("c%05d", ci),
			ReceiverQueueSize:              pc.receiverQueue,
			EnableBatchIndexAcknowledgment: false, // whole entries per transaction (see the header)
			SubscriptionInitialPosition:    pulsar.SubscriptionPositionEarliest,
		}
		if len(topics) == 1 {
			opts.Topic = topics[0]
		} else {
			opts.Topics = topics
		}
		pw.opts = opts
		opts.MessageChannel = make(chan pulsar.ConsumerMessage, max(pc.receiverQueue, 10))
		cons, err := cl.Subscribe(opts)
		if err != nil {
			cl.Close()
			return fmt.Errorf("worker c%d subscribe %d partitions of %s: %v", ci, len(topics), w.in, err)
		}
		pw.cons, pw.ch = cons, cons.Chan()
		for _, p := range parts {
			prod, err := cl.CreateProducer(pulsar.ProducerOptions{
				Topic:           pc.fullName(fmt.Sprintf("%s-partition-%d", w.out, p)),
				CompressionType: pc.compressionV,
				// a transaction's messages are flushed explicitly before it commits; the delay only bounds the
				// flush ticker (pulsar-client-go runs one per partition producer: ~0.55 cores per 1000 at 5 ms)
				BatchingMaxPublishDelay: time.Second,
				BatchingMaxMessages:     uint(pc.batchMaxMsgs),
				BatchingMaxSize:         uint(pc.batchMaxBytes),
				MaxPendingMessages:      max(1000, 4*w.tx.Size),
				DisableBlockIfQueueFull: true,
				SendTimeout:             pc.sendTimeout,
			})
			if err != nil {
				cons.Close()
				cl.Close()
				return fmt.Errorf("worker c%d producer %s-partition-%d: %v", ci, w.out, p, err)
			}
			pw.prods[p] = prod
			nprod.Add(1)
		}
		w.list[i] = pw
		return nil
	})
	if err != nil {
		w.close()
		return err
	}
	n := 0
	for _, pw := range w.list {
		if pw == nil {
			w.idle++
			continue
		}
		n++
		w.wg.Add(1)
		go w.loop(pw)
	}
	fmt.Printf("  [txn] %d workers (ci %d..%d of %d, %d idle): each its own client + TC, a %s consumer on its partitions of %s, %d producers on %s in all; txn-size %d, linger %v, txn-timeout %v; ready in %.1fs\n",
		n, cfg.ConsOffset, cfg.ConsOffset+cfg.Consumers-1, cfg.ConsTotal, w.idle, pc.subType, w.in, nprod.Load(), w.out, w.tx.Size, w.tx.Linger, pc.txnTimeout, time.Since(t).Seconds())
	w.run.SetInfo("txn_workers", n)
	w.run.SetInfo("txn_producers", nprod.Load())
	return nil
}

// fill builds the next transaction out of WHOLE batch entries: messages are collected per input partition until
// their entry is complete (an entry is delivered whole, so its rest is already queued), complete entries are taken in
// arrival order while they fit in -txn-size (an entry that does not fit waits for the next transaction; an entry
// bigger than -txn-size alone is a transaction of its own), and a transaction commits when full or -txn-linger after
// its first entry. At the stop, what is complete is committed; the rest stays unacked (redelivered later).
func (w *pworkers) fill(pw *pworker) []pulsar.ConsumerMessage {
	N := w.tx.Size
	var txn []pulsar.ConsumerMessage
	var deadline time.Time
	tm := time.NewTimer(time.Hour)
	defer tm.Stop()
	take := func() bool { // true when the transaction is full
		for len(pw.ready) > 0 {
			e := pw.ready[0]
			if len(txn) > 0 && len(txn)+len(e) > N {
				return true
			}
			txn = append(txn, e...)
			pw.ready = pw.ready[1:]
			if deadline.IsZero() {
				deadline = time.Now().Add(w.tx.Linger)
			}
			if len(txn) >= N {
				return true
			}
		}
		return false
	}
	recv := func(cm pulsar.ConsumerMessage) {
		t := cm.Topic()
		pw.partial[t] = append(pw.partial[t], cm)
		id := cm.ID()
		if bs, bi := id.BatchSize(), id.BatchIdx(); bs <= 1 || bi < 0 || bi >= bs-1 {
			pw.ready = append(pw.ready, pw.partial[t])
			delete(pw.partial, t)
		}
	}
	for {
		if take() {
			break
		}
		if len(txn) > 0 && !time.Now().Before(deadline) {
			break
		}
		if w.stopping.Load() {
			break
		}
		wait := 200 * time.Millisecond
		if len(txn) > 0 {
			if rem := time.Until(deadline); rem < wait {
				wait = max(rem, time.Millisecond)
			}
		}
		tm.Reset(wait)
		select {
		case cm, ok := <-pw.ch:
			if !ok {
				return txn
			}
			recv(cm)
		more:
			for i := 0; i < 4*N+64; i++ {
				select {
				case cm, ok := <-pw.ch:
					if !ok {
						break more
					}
					recv(cm)
				default:
					break more
				}
			}
		case <-tm.C:
		}
	}
	return txn
}

// leftover counts the messages a worker holds outside a transaction (at the stop: they stay unacked).
func (pw *pworker) leftover() int {
	n := 0
	for _, e := range pw.ready {
		n += len(e)
	}
	for _, e := range pw.partial {
		n += len(e)
	}
	return n
}

func (w *pworkers) loop(pw *pworker) {
	defer w.wg.Done()
	for !w.stopping.Load() {
		ms := w.fill(pw)
		if len(ms) == 0 {
			continue
		}
		w.stats.Received(len(ms))
		w.transact(pw, ms)
	}
	w.dropped.Add(int64(pw.leftover()))
}

// transact runs one transaction over ms: sends and acks in parallel (acks per input partition in order: the
// client serializes them per partition anyway), flush, commit. Not committed -> the inputs are nacked (redelivered
// after 1 s; failover redelivers from the subscription's mark-delete position).
func (w *pworkers) transact(pw *pworker, ms []pulsar.ConsumerMessage) {
	t := w.stats.Begin()
	txn, err := pw.client.NewTransaction(w.pc.txnTimeout)
	if err != nil {
		w.stats.Failed("txn-new", err)
		w.nack(pw, ms)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*w.pc.txnTimeout)
	defer cancel()
	byPart := map[int][]pulsar.ConsumerMessage{}
	for _, cm := range ms {
		p, ok := pw.parts[cm.Topic()]
		if !ok {
			p = -1
		}
		byPart[p] = append(byPart[p], cm)
	}
	var firstErr atomic.Pointer[error]
	setErr := func(e error) {
		if e != nil {
			firstErr.CompareAndSwap(nil, &e)
		}
	}
	var wg sync.WaitGroup
	// outputs: same partition of <topic>-out, sent with the transaction, then flushed
	for p, part := range byPart {
		prod := pw.prods[p]
		if prod == nil {
			setErr(fmt.Errorf("no producer for input partition %d (%s)", p, part[0].Topic()))
			continue
		}
		wg.Add(1)
		go func(prod pulsar.Producer, part []pulsar.ConsumerMessage) {
			defer wg.Done()
			var swg sync.WaitGroup
			for _, cm := range part {
				swg.Add(1)
				prod.SendAsync(ctx, &pulsar.ProducerMessage{Payload: core.Transform(cm.Payload(), pw.ci), Transaction: txn},
					func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
						setErr(err)
						swg.Done()
					})
			}
			setErr(prod.FlushWithCtx(ctx))
			swg.Wait()
		}(prod, part)
	}
	// inputs: acknowledged with the transaction (pending until it commits)
	for _, part := range byPart {
		wg.Add(1)
		go func(part []pulsar.ConsumerMessage) {
			defer wg.Done()
			for _, cm := range part {
				if err := pw.cons.AckWithTxn(cm.Message, txn); err != nil {
					setErr(err)
					return
				}
			}
		}(part)
	}
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		if aerr := txn.Abort(ctx); aerr != nil {
			w.stats.Failed("txn-op", fmt.Errorf("%v (abort: %v)", *e, aerr))
		} else {
			w.stats.Aborted("txn-abort-op", *e)
		}
		w.nack(pw, ms)
		return
	}
	if err := txn.Commit(ctx); err != nil {
		w.stats.Failed("txn-commit", err)
		if txn.GetState() == pulsar.TxnOpen {
			_ = txn.Abort(ctx)
		}
		w.nack(pw, ms)
		return
	}
	w.stats.Committed(t, len(ms))
}

// nack: the transaction did not commit, its inputs must come back. The worker re-subscribes: the receiver queue
// is dropped and the broker redelivers everything unacked from the mark-delete position (a failover nack would
// redeliver the same, while the old copies of the later messages still sit in the queue).
func (w *pworkers) nack(pw *pworker, _ []pulsar.ConsumerMessage) {
	pw.cons.Close()
	w.resubs.Add(1)
	pw.ready, pw.partial = nil, map[string][]pulsar.ConsumerMessage{} // all of it comes back
	for attempt := 1; ; attempt++ {
		opts := pw.opts
		opts.MessageChannel = make(chan pulsar.ConsumerMessage, max(w.pc.receiverQueue, 10))
		cons, err := pw.client.Subscribe(opts)
		if err == nil {
			pw.cons, pw.ch = cons, cons.Chan()
			return
		}
		w.run.NoteErr("txn-resubscribe", fmt.Errorf("worker c%d attempt %d: %w", pw.ci, attempt, err))
		if w.stopping.Load() && attempt >= 3 {
			pw.ch = make(chan pulsar.ConsumerMessage) // nothing more: the stop ends the loop
			return
		}
		time.Sleep(time.Second)
	}
}

// stop: every worker finishes its transaction and exits; then consumers, producers and clients close.
func (w *pworkers) stop() {
	w.stopping.Store(true)
	defer func() {
		fmt.Printf("[info] txn workers: %d re-subscriptions after an abort, %d messages held outside a transaction at the stop (left unacked)\n", w.resubs.Load(), w.dropped.Load())
		w.run.SetInfo("txn_resubscribes", w.resubs.Load())
		w.run.SetInfo("txn_entry_dropped", w.dropped.Load())
	}()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3*w.pc.txnTimeout + w.tx.Linger):
		fmt.Println("WARN txn workers did not stop in time")
	}
	w.close()
}

func (w *pworkers) close() {
	var wg sync.WaitGroup
	for _, pw := range w.list {
		if pw == nil {
			continue
		}
		wg.Add(1)
		go func(pw *pworker) {
			defer wg.Done()
			for _, prod := range pw.prods {
				prod.Close()
			}
			if pw.cons != nil {
				pw.cons.Close()
			}
			pw.client.Close()
		}(pw)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// verifier

// runVerify (after every load process exited): <topic>-out in full through the subscription "verify" (created
// with the topics, before anything was produced, never consumed during the run; Pulsar dispatches committed
// messages only), a message's ledger:entry:batch-index:partition as its position; then, once every open
// transaction timed out, the unacknowledged rest of <topic>-in on the workers' subscription (never acked).
// Exit 0 = PASS, 3 = FAIL, 1 = the scan failed.
func runVerify(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, pc *pulsarFlags, client pulsar.Client) int {
	t0 := time.Now()
	exp, err := core.LoadIds(tx.IdsDir)
	if err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	in, out := core.InTopic(cfg), core.OutTopic(cfg)
	fmt.Printf("[verify] pulsar %s: %d ledgers from %s; reading %s on subscription verify, then the rest of %s on %s\n",
		pc.url, exp.Files, tx.IdsDir, out, in, pc.sub)
	ta := core.NewTally(exp)
	pos := func(m pulsar.Message) uint64 {
		id := m.ID()
		return core.PosNum(uint64(id.LedgerID())<<20^uint64(id.PartitionIdx()), uint64(id.EntryID())<<20^uint64(uint32(id.BatchIdx())))
	}
	if err := scanSub(ctx, cfg, tx, pc, client, "out "+out, out, "verify", func(m pulsar.Message) { ta.Out(m.Payload(), pos(m)) }); err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	if w := pc.txnTimeout + 2*time.Second - time.Since(t0); w > 0 {
		fmt.Printf("[verify] waiting %v until every open transaction (timeout %v) is over before reading the rest of %s\n", w.Round(time.Second), pc.txnTimeout, in)
		select {
		case <-ctx.Done():
		case <-time.After(w):
		}
	}
	if err := scanSub(ctx, cfg, tx, pc, client, "rest of "+in, in, pc.sub, func(m pulsar.Message) { ta.Pending(m.Payload()) }); err != nil {
		fmt.Printf("FATAL %v\n", err)
		return 1
	}
	v := ta.Verdict("pulsar")
	v.ScanS = time.Since(t0).Seconds()
	v.Print(cfg.Out)
	if v.Pass {
		return 0
	}
	return 3
}

// scanSub reads every partition of topic on subscription sub with -verify-consumers failover consumers (each a
// slice of the partitions), never acking, until nothing arrived for -verify-idle.
func scanSub(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, pc *pulsarFlags, client pulsar.Client, what, topic, sub string, rec func(pulsar.Message)) error {
	n := min(pc.verifyConsumers, cfg.Partitions)
	conss := make([]pulsar.Consumer, n)
	t := time.Now()
	err := parallel(ctx, n, pc.adminConc, func(ctx context.Context, k int) error {
		var topics []string
		for p := k; p < cfg.Partitions; p += n {
			topics = append(topics, pc.fullName(fmt.Sprintf("%s-partition-%d", topic, p)))
		}
		opts := pulsar.ConsumerOptions{SubscriptionName: sub, Type: pulsar.Failover, Name: fmt.Sprintf("verify-%03d", k),
			ReceiverQueueSize: 1000, EnableBatchIndexAcknowledgment: true, SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			MessageChannel: make(chan pulsar.ConsumerMessage, 1000)}
		if len(topics) == 1 {
			opts.Topic = topics[0]
		} else {
			opts.Topics = topics
		}
		c, err := client.Subscribe(opts)
		if err != nil {
			return fmt.Errorf("verify subscribe %s/%s slice %d: %v", topic, sub, k, err)
		}
		conss[k] = c
		return nil
	})
	if err != nil {
		for _, c := range conss {
			if c != nil {
				c.Close()
			}
		}
		return err
	}
	fmt.Printf("[verify] %s: %d consumers on %s subscribed in %.1fs\n", what, n, sub, time.Since(t).Seconds())
	var cnt atomic.Int64
	sctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, c := range conss {
		wg.Add(1)
		go func(c pulsar.Consumer) {
			defer wg.Done()
			ch := c.Chan()
			for {
				select {
				case <-sctx.Done():
					return
				case cm, ok := <-ch:
					if !ok {
						return
					}
					rec(cm.Message)
					cnt.Add(1)
				}
			}
		}(c)
	}
	core.IdleScan(what, tx.VerifyIdle, tx.VerifyMax, cnt.Load, ctx.Done())
	cancel()
	wg.Wait()
	for _, c := range conss {
		c.Close()
	}
	return nil
}

var _ = errors.New
