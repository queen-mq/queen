// qload — open-loop Queen load generator for the Kafka vs Redpanda vs Pulsar vs Queen benchmark (SPEC.md, and
// txn/run.sh for the transactional pipeline). Workload model, pacer, histograms, payloads, output lines, start
// barrier, transaction statistics, id ledger and verifier live in internal/core, shared with kload and pload; this
// file wires them to Queen 2.0's HTTP API.
//
// Producer: one POST /api/v1/push per unit (the unit's messages, one partition "p<entity>"), never retried, the
// pacer never blocks (each unit is its own goroutine under the -max-inflight cap), as goload's open loop.
//
// Plain mode: consumers pop the queue (queue mode, -pop-width partitions per pop, -pop-batch, long poll), take e2e,
// then ack the batch asynchronously under -ack-inflight (goload -manual-ack -ack-async).
//
// -txn: workers pop <topic>-in (leased, -pop-width partitions, up to -txn-size messages, filled for up to
// -txn-linger) and commit ONE POST /api/v1/transaction per batch: ack every input WITH its lease (the fence: a
// transaction whose lease ran out rolls back) + push the transformed messages to the same partition of
// <topic>-out. Readers pop <topic>-out like plain consumers (Queen only ever serves committed messages).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mqload/internal/core"
)

type queenFlags struct {
	urls       string
	conns      int
	timeout    time.Duration
	popWidth   int
	popBatch   int
	popTimeout time.Duration
	lease      int
	leaseTime  int
	complRet   int
	dedup      int
	retryLimit int
	configure  bool
	warmConc   int
	warmIdle   time.Duration
	leaseWait  time.Duration
	verifyConc int
	pushWire   string
	pushZstd   bool
	pushSpread int
	txnWire    string
	txnKV      bool
	groups     int
	groupWhere string
	groupSub   string
	stampK     int

	url    string
	list   []string
	wheres []string // per group: its where ("" = a plain group)
}

func (q *queenFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&q.urls, "urls", "http://127.0.0.1:6632", "broker base URLs, comma-separated: process i talks to url[i % n] (followers forward to the leader)")
	fs.StringVar(&tenantHeader, "tenant", "", "x-queen-tenant header on every request (a broker with QUEEN_TENANCY_HEADER=1): the tenant this process works as; empty = no header, the default tenant")
	fs.StringVar(&q.pushWire, "push-wire", "json", "push body: json | bin (the Go client's binary push body, answered with counts; needs a broker that accepts it)")
	fs.BoolVar(&popFrames, "pop-frames", false, "pops ask for the messages as the stored records (frames=true) and unpack them here")
	fs.BoolVar(&ackCounts, "ack-counts", false, "ack batches ask for the totals (\"answer\":\"counts\") instead of one object per message")
	fs.BoolVar(&q.pushZstd, "push-zstd", false, "with -push-wire bin: the messages framed and zstd-compressed by the client, kept by the broker as they are")
	fs.IntVar(&q.pushSpread, "push-spread", 1, "the messages of one push request in N consecutive partitions from the unit's own, in N runs (N > 1: one multi-partition request, which the broker plans as one multi-push)")
	fs.StringVar(&q.txnWire, "txn-wire", "json", "-txn: transaction body: json | bin (the Go client's binary transaction body: the acks as JSON, the pushes framed and zstd-compressed here, answered with counts; needs a broker that accepts it)")
	fs.BoolVar(&q.txnKV, "txn-kv", false, "-txn: every transaction also puts one KV key per input partition (ns wf, key <topic>/<partition>, ttl 1 h): a workflow step's entity state")
	fs.IntVar(&q.groups, "groups", 0, "plain mode: consumers in N consumer groups g0..g<N-1> instead of queue mode, global consumer i in group i % N (each group gets every message it takes: fan-out); 0 = queue mode")
	fs.StringVar(&q.groupWhere, "group-where", "", "plain mode: the groups' wheres, ';'-separated, group i takes the i-th (filtered groups; an empty one = a plain group); sets -groups when that is 0. E.g. \"data.k < 30;data.k >= 30 and data.k < 60\"")
	fs.StringVar(&q.groupSub, "group-sub", "", "subscriptionMode of the groups' first pop (\"all\"; empty = the broker's default, new)")
	fs.IntVar(&q.stampK, "stamp-k", 0, "every load message also carries \"k\":<j % N> (j its index in the push), so a where on data.k takes an exact share; 0 = off")
	fs.IntVar(&q.conns, "conns", 2048, "HTTP keep-alive connections kept per broker (MaxIdleConnsPerHost, goload's -idle-conns)")
	core.DurationVar(fs, &q.timeout, "timeout", 30*time.Second, "request timeout")
	fs.IntVar(&q.popWidth, "pop-width", 10, "partitions claimed per pop (workers and readers; the 09-30 grid's pop width)")
	fs.IntVar(&q.popBatch, "pop-batch", 1000, "messages per reader/consumer pop (goload -pop-batch)")
	core.DurationVar(fs, &q.popTimeout, "pop-timeout", 2*time.Second, "long-poll pop timeout (wait=true)")
	fs.IntVar(&q.lease, "lease", 30, "leaseSeconds of every pop (a transaction that outlives its lease rolls back and the messages are redelivered)")
	fs.IntVar(&q.leaseTime, "lease-time", 30, "configure: the queues' leaseTime (pops pass -lease anyway)")
	fs.IntVar(&q.complRet, "completed-retention", 3600, "configure: completedRetentionSeconds (the verifier re-reads <topic>-out after the run)")
	fs.IntVar(&q.dedup, "dedup-window", 0, "configure: dedupWindowSeconds (0 = off, as the 09-30 grid against Kafka/Pulsar)")
	fs.IntVar(&q.retryLimit, "retry-limit", 1000000, "configure: retryLimit (a redelivered input never goes to the DLQ)")
	fs.BoolVar(&q.configure, "configure", true, "with -create: configure the queues (POST /api/v1/configure)")
	fs.IntVar(&q.warmConc, "warm-conc", 16, "-warm: push/drain requests in flight")
	core.DurationVar(fs, &q.warmIdle, "warm-drain-idle", 10*time.Second, "-warm: the drain gives up after this long with no message out (ten million partitions: the first pops take over a minute, give it 300s)")
	core.DurationVar(fs, &q.leaseWait, "verify-lease-wait", 0, "-verify: wait until this long after the verifier started before reading the unprocessed rest of <topic>-in, so leases of stopped workers ran out (0 = -lease + 2s)")
	fs.IntVar(&q.verifyConc, "verify-conc", 16, "-verify: parallel poppers per scan")
}

func (q *queenFlags) finalize(cfg *core.Config) error {
	for _, u := range strings.Split(q.urls, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			q.list = append(q.list, u)
		}
	}
	if len(q.list) == 0 {
		return fmt.Errorf("-urls is empty")
	}
	q.url = q.list[cfg.LoaderIndex%len(q.list)]
	if q.popWidth < 1 {
		q.popWidth = 1
	}
	if q.leaseWait <= 0 {
		q.leaseWait = time.Duration(q.lease+2) * time.Second
	}
	if q.groupWhere != "" {
		for _, w := range strings.Split(q.groupWhere, ";") {
			q.wheres = append(q.wheres, strings.TrimSpace(w))
		}
		if q.groups == 0 {
			q.groups = len(q.wheres)
		}
		if len(q.wheres) > q.groups {
			return fmt.Errorf("-group-where has %d wheres for -groups %d", len(q.wheres), q.groups)
		}
	}
	if q.groups < 0 || q.stampK < 0 {
		return fmt.Errorf("-groups and -stamp-k must be >= 0")
	}
	for len(q.wheres) < q.groups {
		q.wheres = append(q.wheres, "")
	}
	return nil
}

// group is global consumer i's consumer group and where ("", "" = queue mode).
func (q *queenFlags) group(i int) (string, string) {
	if q.groups == 0 {
		return "", ""
	}
	g := i % q.groups
	return "g" + strconv.Itoa(g), q.wheres[g]
}

func fatal(format string, a ...any) {
	fmt.Printf("FATAL "+format+"\n", a...)
	os.Exit(1)
}

func main() {
	core.Init()
	fs := flag.NewFlagSet("qload", flag.ExitOnError)
	cfg := &core.Config{}
	cfg.Register(fs)
	tx := &core.TxnConfig{}
	tx.RegisterTxn(fs)
	qf := &queenFlags{}
	qf.register(fs)
	_ = fs.Parse(os.Args[1:])
	if err := cfg.Finalize(); err != nil {
		fatal("%v", err)
	}
	if err := tx.FinalizeTxn(cfg); err != nil {
		fatal("%v", err)
	}
	if err := qf.finalize(cfg); err != nil {
		fatal("%v", err)
	}
	if cfg.Partitions < 1 {
		fatal("qload needs -partitions >= 1 (partitions p0..p<P-1>)")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // after the first signal, a second one kills the process

	cli := newHTTP(qf.url, qf.conns, qf.timeout+qf.popTimeout)
	if tx.Verify {
		os.Exit(runVerify(ctx, cfg, tx, qf, cli))
	}

	run, err := core.NewRun(cfg, "qload")
	if err != nil {
		fatal("%v", err)
	}
	run.Pool.KeyMod = qf.stampK
	if tx.On && qf.groups > 0 {
		fatal("-groups and -group-where are for plain mode, not -txn")
	}
	mode := "plain: consumers pop+ack the queue"
	if qf.groups > 0 {
		mode = fmt.Sprintf("plain: consumers in %d groups pop+ack the queue (wheres %q, subscriptionMode %q, stamp-k %d)",
			qf.groups, qf.wheres, qf.groupSub, qf.stampK)
	}
	if tx.On {
		mode = fmt.Sprintf("txn: %s -> workers (txn-size %d, linger %v, one /transaction = acks with leases + pushes) -> %s, %d read-committed readers",
			core.InTopic(cfg), tx.Size, tx.Linger, core.OutTopic(cfg), tx.Readers)
	}
	run.Header(fmt.Sprintf("queen: url=%s (of %d) conns=%d timeout=%v pop-width=%d pop-batch=%d pop-timeout=%v lease=%ds | configure: leaseTime=%d completedRetention=%d dedup=%d retryLimit=%d | %s",
		qf.url, len(qf.list), qf.conns, qf.timeout, qf.popWidth, qf.popBatch, qf.popTimeout, qf.lease, qf.leaseTime, qf.complRet, qf.dedup, qf.retryLimit, mode))

	adm := &admin{cli: cli, qf: qf, cfg: cfg, run: run, txn: tx.On}
	if err := adm.setup(ctx); err != nil {
		fatal("%v", err)
	}
	if cfg.CreateOnly {
		fmt.Printf("[create] done: %s x %d partitions\n", strings.Join(adm.queues(), ", "), cfg.Partitions)
		return
	}

	var stats *core.TxnStats
	var workers *txnWorkers
	cons := &consumers{run: run, cli: cli, qf: qf}
	if tx.On {
		stats = run.EnableTxn(tx.IdsOut)
		workers = newWorkers(run, cli, qf, tx, stats)
		workers.start()
		cons.queue, cons.n, cons.stats = core.OutTopic(cfg), tx.Readers, stats
	} else {
		cons.queue, cons.n = cfg.Topic, cfg.Consumers
	}
	cons.start()
	prods := newProducers(run, cli, qf, tx.On)

	t0 := run.WaitStart(ctx)
	run.Produce(ctx, t0, prods.send)
	run.WaitInflight(qf.timeout + 5*time.Second)
	var end time.Time
	if tx.On {
		end = run.DrainTxn(ctx.Done(), tx.IdleExit)
	} else {
		end = run.Drain(ctx)
	}
	run.StopReporter(end)
	if workers != nil {
		workers.stop()
	}
	cons.stop()
	prods.close()
	run.Finish(end)
}
