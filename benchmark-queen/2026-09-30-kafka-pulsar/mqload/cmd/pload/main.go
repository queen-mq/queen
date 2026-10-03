// pload — open-loop Pulsar load generator for the Kafka vs Pulsar vs Queen
// benchmark (SPEC.md). Workload model, pacer, histograms, payloads, output
// lines and start barrier live in internal/core and are shared with kload, so
// both loaders have identical semantics by construction; this file wires them
// to Apache Pulsar through apache/pulsar-client-go.
//
// Setup (admin REST, -admin): tenant (allowedClusters = the cluster from
// GET /admin/v2/clusters), namespace with -bundles bundles (+ optional
// -persistence E,Qw,Qa), partitioned topics (PUT .../partitions; -partitions 0
// = non-partitioned), and the subscription created BEFORE any producer sends
// (PUT .../subscription/<sub>, then verified on every partition). -warm sends
// one message without "ts" to every partition and consumes it.
//
// Producer: one per (process, topic); LZ4, batching 5 ms / 1000 msgs / 128 KB,
// KeyBasedBatchBuilder for Key_Shared, DisableBlockIfQueueFull with
// MaxPendingMessages above what -max-inflight can put on a partition, so
// SendAsync never blocks the pacer. E=0 -> a MessageRouter returning the
// explicit partition; E>0 -> key e<id> and the default key-hash routing.
//
// Consumer: one per consumer index; a multi-topic consumer when it owns
// several topics; receiver queue 1000, ack grouping 100 ms, batch-index acks,
// initial position earliest, Ack per message.
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

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"

	"mqload/internal/core"
)

type pulsarFlags struct {
	url             string
	admin           string
	tenant          string
	namespace       string
	bundles         int
	persistence     string
	sub             string
	subType         string
	compression     string
	batchDelay      time.Duration
	batchMaxMsgs    int
	batchMaxBytes   int
	maxPending      int
	pendingBudgetMB int
	keyBatching     bool
	receiverQueue   int
	ackGroupTime    time.Duration
	connsPerBroker  int
	ioThreads       int
	opTimeout       time.Duration
	connTimeout     time.Duration
	sendTimeout     time.Duration
	topicTimeout    time.Duration
	adminConc       int
	clientLog       string
	settle          time.Duration
	consumerSlices  bool
	producerShard   bool
	producerEager   bool
	txnTimeout      time.Duration
	verifyConsumers int

	subTypeV     pulsar.SubscriptionType
	compressionV pulsar.CompressionType
	persist      [3]int
}

func (p *pulsarFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&p.url, "url", "pulsar://127.0.0.1:6650", "service URL with every broker: pulsar://ip1:6650,ip2:6650,ip3:6650")
	fs.StringVar(&p.admin, "admin", "http://127.0.0.1:8080", "admin REST base URL")
	fs.StringVar(&p.tenant, "tenant", "bench", "tenant")
	fs.StringVar(&p.namespace, "namespace", "ns", "namespace (under the tenant)")
	fs.IntVar(&p.bundles, "bundles", 48, "bundles of the namespace when this process creates it")
	fs.StringVar(&p.persistence, "persistence", "", "namespace persistence E,Qw,Qa, e.g. 3,3,2 (empty = broker default)")
	fs.StringVar(&p.sub, "sub", "sub", "subscription name (one per topic)")
	fs.StringVar(&p.subType, "sub-type", "failover", "failover | shared | key_shared | exclusive")
	fs.StringVar(&p.compression, "compression", "lz4", "none | lz4 | zlib | zstd")
	core.DurationVar(fs, &p.batchDelay, "batch-delay", 5*time.Millisecond, "BatchingMaxPublishDelay")
	fs.IntVar(&p.batchMaxMsgs, "batch-max-msgs", 1000, "BatchingMaxMessages")
	fs.IntVar(&p.batchMaxBytes, "batch-max-bytes", 131072, "BatchingMaxSize")
	fs.IntVar(&p.maxPending, "max-pending", 0, "MaxPendingMessages per partition producer (0 = max-inflight x max unit x 2, capped by -pending-budget-mb)")
	fs.IntVar(&p.pendingBudgetMB, "pending-budget-mb", 64, "cap on the pending queues the Go client preallocates (24 B x MaxPendingMessages per partition producer)")
	fs.BoolVar(&p.keyBatching, "key-batching", false, "KeyBasedBatchBuilder (default: on when -sub-type key_shared, REQUIRED there for per-key ordering with batching)")
	fs.IntVar(&p.receiverQueue, "receiver-queue", 1000, "ReceiverQueueSize")
	core.DurationVar(fs, &p.ackGroupTime, "ack-group-time", 100*time.Millisecond, "ack grouping MaxTime (MaxSize 1000)")
	fs.IntVar(&p.connsPerBroker, "conns-per-broker", 1, "client MaxConnectionsPerBroker")
	fs.IntVar(&p.ioThreads, "io-threads", 0, "accepted for parity with the Java client; the Go client has no IO thread pool (goroutines per connection): no effect")
	core.DurationVar(fs, &p.opTimeout, "op-timeout", 30*time.Second, "client OperationTimeout (create producer, subscribe)")
	core.DurationVar(fs, &p.connTimeout, "conn-timeout", 10*time.Second, "client ConnectionTimeout")
	core.DurationVar(fs, &p.sendTimeout, "send-timeout", 30*time.Second, "producer SendTimeout")
	core.DurationVar(fs, &p.topicTimeout, "topic-timeout", 300*time.Second, "give up if the admin setup (and warm) does not finish within this")
	fs.IntVar(&p.adminConc, "admin-conc", 32, "admin REST requests in flight during setup")
	fs.StringVar(&p.clientLog, "client-log", "warn", "pulsar client log level to stderr: error|warn|info|debug")
	fs.BoolVar(&p.consumerSlices, "consumer-slices", true, "failover/exclusive on partitioned topics: each consumer subscribes only to its partitions (explicit <topic>-partition-N topics, every partition exactly one consumer, like a Kafka group); false = every consumer on the whole partitioned topic")
	fs.BoolVar(&p.producerShard, "producer-shard", true, "-entities 0: process li produces only to the partitions p % loaders == li, through per-partition producers created lazily on first use; the picker walks that subset (-entities > 0: partitioned producer + key routing)")
	fs.BoolVar(&p.producerEager, "producer-eager", false, "with -producer-shard: create every per-partition producer of the shard before READY instead of on first use")
	core.DurationVar(fs, &p.settle, "settle", 2*time.Second, "after every consumer subscribed, wait this long before READY (failover switches the active consumer 1 s after a join; Key_Shared re-splits hash ranges)")
	core.DurationVar(fs, &p.txnTimeout, "txn-timeout", 10*time.Second, "-txn: NewTransaction timeout (the TC aborts a transaction left open this long)")
	fs.IntVar(&p.verifyConsumers, "verify-consumers", 16, "-verify: consumers per scan (each a failover slice of the partitions)")
}

func (p *pulsarFlags) finalize(cfg *core.Config) error {
	switch p.subType {
	case "failover":
		p.subTypeV = pulsar.Failover
	case "shared":
		p.subTypeV = pulsar.Shared
	case "key_shared":
		p.subTypeV = pulsar.KeyShared
	case "exclusive":
		p.subTypeV = pulsar.Exclusive
	default:
		return fmt.Errorf("-sub-type %q: want failover|shared|key_shared|exclusive", p.subType)
	}
	if !cfg.IsSet("key-batching") {
		p.keyBatching = p.subType == "key_shared"
	}
	switch p.compression {
	case "none":
		p.compressionV = pulsar.NoCompression
	case "lz4":
		p.compressionV = pulsar.LZ4
	case "zlib":
		p.compressionV = pulsar.ZLib
	case "zstd":
		p.compressionV = pulsar.ZSTD
	default:
		return fmt.Errorf("-compression %q", p.compression)
	}
	if p.persistence != "" {
		parts := strings.Split(p.persistence, ",")
		if len(parts) != 3 {
			return fmt.Errorf("-persistence %q: want E,Qw,Qa", p.persistence)
		}
		for i, s := range parts {
			v, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil || v < 1 {
				return fmt.Errorf("-persistence %q", p.persistence)
			}
			p.persist[i] = v
		}
	}
	if p.adminConc < 1 {
		p.adminConc = 1
	}
	p.admin = strings.TrimRight(p.admin, "/")
	return nil
}

// fullName is persistent://tenant/ns/topic.
func (p *pulsarFlags) fullName(topic string) string {
	return "persistent://" + p.tenant + "/" + p.namespace + "/" + topic
}

func fatal(format string, a ...any) {
	fmt.Printf("FATAL "+format+"\n", a...)
	os.Exit(1)
}

func newLogger(level string) plog.Logger {
	l := logrus.New()
	l.SetOutput(os.Stderr)
	lv, err := logrus.ParseLevel(level)
	if err != nil {
		lv = logrus.WarnLevel
	}
	l.SetLevel(lv)
	return plog.NewLoggerWithLogrus(l)
}

func main() {
	core.Init()
	fs := flag.NewFlagSet("pload", flag.ExitOnError)
	cfg := &core.Config{}
	cfg.Register(fs)
	tx := &core.TxnConfig{}
	tx.RegisterTxn(fs)
	pc := &pulsarFlags{}
	pc.register(fs)
	_ = fs.Parse(os.Args[1:])
	if err := cfg.Finalize(); err != nil {
		fatal("%v", err)
	}
	if err := tx.FinalizeTxn(cfg); err != nil {
		fatal("%v", err)
	}
	if err := pc.finalize(cfg); err != nil {
		fatal("%v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // after the first signal, a second one kills the process

	if tx.Verify {
		client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pc.url, OperationTimeout: pc.opTimeout,
			ConnectionTimeout: pc.connTimeout, Logger: newLogger(pc.clientLog)})
		if err != nil {
			fatal("client: %v", err)
		}
		rc := runVerify(ctx, cfg, tx, pc, client)
		client.Close()
		os.Exit(rc)
	}
	if tx.On {
		mainTxn(ctx, cfg, tx, pc)
		return
	}

	if shardMode(cfg, pc) && cfg.Rate > 0 {
		// the picker walks only this process's partitions (p % loaders == li)
		cfg.ShardIndex, cfg.ShardCount = cfg.LoaderIndex, cfg.Loaders
	}
	run, err := core.NewRun(cfg, "pload")
	if err != nil {
		fatal("%v", err)
	}
	pending, memLimit := pendingSizing(run, pc)
	run.Header(fmt.Sprintf("pulsar: url=%s admin=%s ns=%s/%s sub=%s type=%s compression=%s batch=%v/%d msgs/%d B key-batching=%v max-pending=%d/partition mem-limit=%d MB receiver-queue=%d ack-group=%v conns/broker=%d",
		pc.url, pc.admin, pc.tenant, pc.namespace, pc.sub, pc.subType, pc.compression, pc.batchDelay, pc.batchMaxMsgs, pc.batchMaxBytes,
		pc.keyBatching, pending, memLimit>>20, pc.receiverQueue, pc.ackGroupTime, pc.connsPerBroker))
	if pc.ioThreads > 0 {
		fmt.Println("  note: -io-threads has no effect in the Go client (no IO thread pool)")
	}
	if pc.subType == "key_shared" && !cfg.Keyed() {
		fmt.Println("WARN key_shared without -entities: messages carry no key")
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pc.url,
		OperationTimeout:        pc.opTimeout,
		ConnectionTimeout:       pc.connTimeout,
		MaxConnectionsPerBroker: pc.connsPerBroker,
		MemoryLimitBytes:        memLimit,
		Logger:                  newLogger(pc.clientLog),
	})
	if err != nil {
		fatal("client: %v", err)
	}
	defer client.Close()

	adm := newAdmin(pc)
	if err := adm.setupTopics(ctx, run, cfg.TopicNames(), nil); err != nil {
		fatal("%v", err)
	}
	if cfg.Warm {
		if err := warm(ctx, run, pc, client, cfg.TopicNames()); err != nil {
			fatal("%v", err)
		}
	}
	if cfg.CreateOnly {
		fmt.Printf("[create] done: %d topics x %d partitions, subscription %q on every partition\n", cfg.Topics, cfg.Partitions, pc.sub)
		return
	}

	cons := newConsumers(run, pc, client)
	if err := cons.start(ctx); err != nil {
		cons.stop()
		fatal("%v", err)
	}
	prods, err := newProducers(ctx, run, pc, client, pending, cfg.TopicNames())
	if err != nil {
		cons.stop()
		fatal("%v", err)
	}

	t0 := run.WaitStart(ctx)
	run.Produce(ctx, t0, prods.send)
	run.WaitInflight(pc.sendTimeout + 5*time.Second)
	end := run.Drain(ctx)
	run.StopReporter(end)
	cons.stop()
	prods.close()
	prods.summary()
	run.Finish(end)
}

// mainTxn is the -txn process: topics <topic>-in and <topic>-out (subscription sub on both, verify on
// <topic>-out), the feeder producing to <topic>-in, the transactional workers (txn.go) and the readers of
// <topic>-out (the plain consumers).
func mainTxn(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, pc *pulsarFlags) {
	in, out := core.InTopic(cfg), core.OutTopic(cfg)
	if !(pc.consumerSlices && (pc.subType == "failover" || pc.subType == "exclusive")) {
		fatal("-txn needs -sub-type failover|exclusive with -consumer-slices (every input partition one worker)")
	}
	if tx.Readers != 0 && tx.Readers != cfg.Consumers {
		fatal("pload -txn: -readers must be 0 or -consumers (readers take the workers' slices of %s)", out)
	}
	if shardMode(cfg, pc) && cfg.Rate > 0 {
		cfg.ShardIndex, cfg.ShardCount = cfg.LoaderIndex, cfg.Loaders
	}
	// the input's batch entries hold at most -txn-size messages, so a transaction of whole entries (txn.go) is one
	// entry of -txn-size messages
	entryCap := "feeder batches capped at -txn-size messages (whole entries per transaction)"
	if !cfg.IsSet("batch-max-msgs") {
		pc.batchMaxMsgs = min(pc.batchMaxMsgs, tx.Size)
	} else if pc.batchMaxMsgs > tx.Size {
		entryCap = fmt.Sprintf("WARN -batch-max-msgs %d > -txn-size %d: transactions hold whole entries, so they will be bigger than -txn-size", pc.batchMaxMsgs, tx.Size)
	}
	run, err := core.NewRun(cfg, "pload")
	if err != nil {
		fatal("%v", err)
	}
	pending, memLimit := pendingSizing(run, pc)
	run.SetInfo("feeder_batch_max_msgs", pc.batchMaxMsgs)
	run.Header(fmt.Sprintf("pulsar: url=%s admin=%s ns=%s/%s sub=%s type=%s compression=%s batch=%v/%d msgs/%d B max-pending=%d/partition mem-limit=%d MB receiver-queue=%d ack-group=%v conns/broker=%d | "+entryCap+" | txn: %s -> %d workers (own client + TC each, NewTransaction timeout %v, send+AckWithTxn on whole entries, txn-size %d, linger %v) -> %s, %d readers",
		pc.url, pc.admin, pc.tenant, pc.namespace, pc.sub, pc.subType, pc.compression, pc.batchDelay, pc.batchMaxMsgs, pc.batchMaxBytes,
		pending, memLimit>>20, pc.receiverQueue, pc.ackGroupTime, pc.connsPerBroker, in, cfg.Consumers, pc.txnTimeout, tx.Size, tx.Linger, out, tx.Readers))
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     pc.url,
		OperationTimeout:        pc.opTimeout,
		ConnectionTimeout:       pc.connTimeout,
		MaxConnectionsPerBroker: pc.connsPerBroker,
		MemoryLimitBytes:        memLimit,
		Logger:                  newLogger(pc.clientLog),
	})
	if err != nil {
		fatal("client: %v", err)
	}
	defer client.Close()
	names := []string{in, out}
	adm := newAdmin(pc)
	if err := adm.setupTopics(ctx, run, names, map[string][]string{out: {"verify"}}); err != nil {
		fatal("%v", err)
	}
	if cfg.Warm {
		if err := warm(ctx, run, pc, client, names); err != nil {
			fatal("%v", err)
		}
	}
	if cfg.CreateOnly {
		fmt.Printf("[create] done: %s, %s x %d partitions, subscription %q on both, verify on %s\n", in, out, cfg.Partitions, pc.sub, out)
		return
	}
	stats := run.EnableTxn(tx.IdsOut)
	workers := newPWorkers(run, pc, tx, stats)
	if err := workers.start(ctx); err != nil {
		fatal("%v", err)
	}
	readers := newConsumers(run, pc, client)
	readers.names = []string{out}
	readers.readerTxn = stats
	if tx.Readers > 0 {
		if err := readers.start(ctx); err != nil {
			readers.stop()
			workers.stop()
			fatal("%v", err)
		}
	}
	prods, err := newProducers(ctx, run, pc, client, pending, []string{in})
	if err != nil {
		readers.stop()
		workers.stop()
		fatal("%v", err)
	}
	t0 := run.WaitStart(ctx)
	run.Produce(ctx, t0, prods.send)
	run.WaitInflight(pc.sendTimeout + 5*time.Second)
	end := run.DrainTxn(ctx.Done(), tx.IdleExit)
	run.StopReporter(end)
	workers.stop()
	readers.stop()
	prods.close()
	prods.summary()
	run.Finish(end)
}
