// kload — open-loop Kafka load generator for the Kafka vs Pulsar vs Queen
// benchmark (SPEC.md). Workload model, pacer, histograms, payloads, output
// lines and start barrier live in internal/core and are shared with pload, so
// both loaders have identical semantics by construction; this file wires them
// to Apache Kafka through franz-go (kgo + kadm).
//
// Producer: -producers franz-go clients per process (unit u -> client
// u % producers); acks=all, idempotent (franz-go default: 5 produce requests
// in flight per broker), linger 5 ms, lz4, 1 MiB batches; E=0 -> explicit
// partition (ManualPartitioner); E>0 -> key e<id> with franz-go's
// StickyKeyPartitioner(nil) = Java's murmur2 keyed partitioning. TryProduce
// never blocks: MaxBufferedRecords is sized above what -max-inflight can put
// in flight.
//
// Consumer: one franz-go client per consumer, groups per SPEC §2, KIP-848
// ("consumer" protocol, server-side "uniform" assignor) or classic with the
// cooperative-sticky balancer; PollRecords(-poll), e2e per record, -proc-us,
// then the polled offsets committed async per poll under the process-wide
// -ack-inflight cap (full = block, never shed).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mqload/internal/core"
)

type kafkaFlags struct {
	brokers           string
	rf                int
	minISR            int
	topicConfig       string
	producers         int
	linger            time.Duration
	compression       string
	inflightPerBroker int
	batchMaxBytes     int
	fetchMaxWait      time.Duration
	fetchMinBytes     int
	fetchMaxPartBytes int
	groupProtocol     string
	sessionTimeout    time.Duration
	stableWait        time.Duration
	groupTimeout      time.Duration
	createChunk       int
	topicTimeout      time.Duration
	topicSettle       time.Duration
	metadataMaxAge    time.Duration
	deliveryTimeout   time.Duration
	clientLog         string
	txnTimeout        time.Duration
	txnBackoff        time.Duration
	txnMetaMinAge     time.Duration

	seeds []string
	codec kgo.CompressionCodec
}

func (k *kafkaFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&k.brokers, "brokers", "127.0.0.1:9092", "seed brokers, comma-separated (all 3 private IPs)")
	fs.IntVar(&k.rf, "rf", 3, "replication factor of created topics")
	fs.IntVar(&k.minISR, "min-isr", 2, "min.insync.replicas of created topics")
	fs.StringVar(&k.topicConfig, "topic-config", "retention.ms=600000,segment.bytes=268435456", "topic configs k=v,... for created topics")
	fs.IntVar(&k.producers, "producers", 4, "franz-go producer clients per process (unit u -> client u % producers)")
	core.DurationVar(fs, &k.linger, "linger", 5*time.Millisecond, "ProducerLinger (Kafka 4.x linger.ms default)")
	fs.StringVar(&k.compression, "compression", "lz4", "none|gzip|snappy|lz4|zstd")
	fs.IntVar(&k.inflightPerBroker, "inflight-per-broker", 5, "produce requests in flight per broker (idempotent producers: franz-go always uses 5, Kafka's max)")
	fs.IntVar(&k.batchMaxBytes, "batch-max-bytes", 1048576, "ProducerBatchMaxBytes")
	core.DurationVar(fs, &k.fetchMaxWait, "fetch-max-wait", 500*time.Millisecond, "FetchMaxWait")
	fs.IntVar(&k.fetchMinBytes, "fetch-min-bytes", 1, "FetchMinBytes")
	fs.IntVar(&k.fetchMaxPartBytes, "fetch-max-partition-bytes", 1048576, "FetchMaxPartitionBytes")
	fs.StringVar(&k.groupProtocol, "group-protocol", "consumer", "consumer (KIP-848, server-side uniform assignor) | classic (cooperative-sticky)")
	core.DurationVar(fs, &k.sessionTimeout, "session-timeout", 45*time.Second, "group session timeout (classic; KIP-848 uses the broker's group.consumer.session.timeout.ms)")
	core.DurationVar(fs, &k.stableWait, "stable-wait", 3*time.Second, "group stable = every partition assigned and no assignment change for this long")
	core.DurationVar(fs, &k.groupTimeout, "group-timeout", 180*time.Second, "give up if the groups are not stable within this")
	fs.IntVar(&k.createChunk, "create-chunk", 5000, "partitions per CreateTopics/CreatePartitions request (KRaft caps a request at 10000 records)")
	core.DurationVar(fs, &k.topicTimeout, "topic-timeout", 300*time.Second, "give up if the topics are not created and ready within this")
	core.DurationVar(fs, &k.topicSettle, "topic-settle", 0, "the topics must stay ready (leaders, full ISR, replica logs) this long")
	core.DurationVar(fs, &k.metadataMaxAge, "metadata-max-age", 5*time.Minute, "franz-go MetadataMaxAge (periodic full metadata refresh)")
	core.DurationVar(fs, &k.deliveryTimeout, "delivery-timeout", 120*time.Second, "RecordDeliveryTimeout (Java delivery.timeout.ms default)")
	fs.StringVar(&k.clientLog, "client-log", "none", "franz-go log level to stderr: none|error|warn|info|debug")
	core.DurationVar(fs, &k.txnTimeout, "txn-timeout", 10*time.Second, "-txn: TransactionTimeout of every worker (Kafka Streams' EOS default; an open transaction of a dead worker blocks read_committed readers this long)")
	core.DurationVar(fs, &k.txnBackoff, "txn-backoff", 20*time.Millisecond, "-txn: ConcurrentTransactionsBackoff (franz-go default 20ms): the retry delay of transactional requests that meet the previous transaction's markers still being written")
	core.DurationVar(fs, &k.txnMetaMinAge, "txn-metadata-min-age", 250*time.Millisecond, "-txn: MetadataMinAge of the workers (franz-go default 5s). With transactions v2 (KIP-890) a transaction's first produce can answer CONCURRENT_TRANSACTIONS while the previous one's markers are still written; franz-go retries that produce after a metadata refresh, which this rate-limits: at 5s it made the 10-01 smoke's ~5 s p999 commits")
}

func (k *kafkaFlags) finalize() error {
	for _, b := range strings.Split(k.brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			k.seeds = append(k.seeds, b)
		}
	}
	if len(k.seeds) == 0 {
		return fmt.Errorf("-brokers is empty")
	}
	switch k.compression {
	case "none":
		k.codec = kgo.NoCompression()
	case "gzip":
		k.codec = kgo.GzipCompression()
	case "snappy":
		k.codec = kgo.SnappyCompression()
	case "lz4":
		k.codec = kgo.Lz4Compression()
	case "zstd":
		k.codec = kgo.ZstdCompression()
	default:
		return fmt.Errorf("-compression %q", k.compression)
	}
	switch k.groupProtocol {
	case "consumer", "classic":
	default:
		return fmt.Errorf("-group-protocol %q: want consumer|classic", k.groupProtocol)
	}
	if k.producers < 1 {
		k.producers = 1
	}
	if k.createChunk < 1 {
		k.createChunk = 5000
	}
	return nil
}

// common client options (logger, metadata age)
func (k *kafkaFlags) baseOpts(id string) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(k.seeds...), kgo.ClientID(id), kgo.MetadataMaxAge(k.metadataMaxAge)}
	lvl := map[string]kgo.LogLevel{"error": kgo.LogLevelError, "warn": kgo.LogLevelWarn, "info": kgo.LogLevelInfo, "debug": kgo.LogLevelDebug}
	if l, ok := lvl[k.clientLog]; ok {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, l, func() string {
			return time.Now().UTC().Format("15:04:05.000 ") + id + " "
		})))
	}
	return opts
}

func fatal(format string, a ...any) {
	fmt.Printf("FATAL "+format+"\n", a...)
	os.Exit(1)
}

func main() {
	core.Init()
	fs := flag.NewFlagSet("kload", flag.ExitOnError)
	cfg := &core.Config{}
	cfg.Register(fs)
	tx := &core.TxnConfig{}
	tx.RegisterTxn(fs)
	kc := &kafkaFlags{}
	kc.register(fs)
	_ = fs.Parse(os.Args[1:])
	if err := cfg.Finalize(); err != nil {
		fatal("%v", err)
	}
	if err := tx.FinalizeTxn(cfg); err != nil {
		fatal("%v", err)
	}
	if err := kc.finalize(); err != nil {
		fatal("%v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // after the first signal, a second one kills the process

	if tx.Verify {
		adm := newAdmin(kc)
		rc := runVerify(ctx, cfg, tx, kc, adm)
		adm.close()
		os.Exit(rc)
	}
	if tx.On {
		mainTxn(ctx, cfg, tx, kc)
		return
	}

	run, err := core.NewRun(cfg, "kload")
	if err != nil {
		fatal("%v", err)
	}
	run.Header(fmt.Sprintf("kafka: brokers=%s rf=%d min-isr=%d producers=%d linger=%v compression=%s acks=all idempotent batch-max-bytes=%d | consumers: protocol=%s fetch-max-wait=%v fetch-min-bytes=%d fetch-max-partition-bytes=%d session=%v",
		kc.brokers, kc.rf, kc.minISR, kc.producers, kc.linger, kc.compression, kc.batchMaxBytes, kc.groupProtocol, kc.fetchMaxWait, kc.fetchMinBytes, kc.fetchMaxPartBytes, kc.sessionTimeout))
	if kc.inflightPerBroker != 5 {
		fmt.Printf("WARN -inflight-per-broker %d ignored: idempotent producers (acks=all, SPEC) run 5 in flight per broker, franz-go's fixed idempotent maximum\n", kc.inflightPerBroker)
	}

	adm := newAdmin(kc)
	defer adm.close()
	if err := adm.setupTopics(ctx, run, cfg.TopicNames()); err != nil {
		fatal("%v", err)
	}
	if cfg.CreateOnly {
		fmt.Printf("[create] done: %d topics x %d partitions ready\n", cfg.Topics, cfg.Partitions)
		return
	}

	cons := newConsumers(run, kc, adm)
	if err := cons.start(ctx); err != nil {
		fatal("%v", err)
	}
	if err := cons.waitStable(ctx); err != nil {
		cons.stop()
		fatal("%v", err)
	}

	prods, err := newProducers(ctx, run, kc, cfg.TopicNames())
	if err != nil {
		cons.stop()
		fatal("%v", err)
	}

	t0 := run.WaitStart(ctx)
	run.Produce(ctx, t0, prods.send)
	run.WaitInflight(kc.deliveryTimeout + 5*time.Second)
	end := run.Drain(ctx)
	run.StopReporter(end)
	cons.stop()
	prods.close()
	run.Finish(end)
}

// mainTxn is the -txn process: topics <topic>-in and <topic>-out, the feeder producing to <topic>-in, the
// transactional workers (txn.go) and the read_committed readers of <topic>-out.
func mainTxn(ctx context.Context, cfg *core.Config, tx *core.TxnConfig, kc *kafkaFlags) {
	in, out := core.InTopic(cfg), core.OutTopic(cfg)
	run, err := core.NewRun(cfg, "kload")
	if err != nil {
		fatal("%v", err)
	}
	run.Header(fmt.Sprintf("kafka: brokers=%s rf=%d min-isr=%d producers=%d linger=%v compression=%s acks=all idempotent batch-max-bytes=%d | consumers: protocol=%s fetch-max-wait=%v fetch-min-bytes=%d fetch-max-partition-bytes=%d session=%v | txn: %s -> %d workers (GroupTransactSession, transactional.id per worker, txn-timeout %v, concurrent-txn backoff %v, metadata-min-age %v, txn-size %d, linger %v, read_committed) -> %s, %d read_committed readers",
		kc.brokers, kc.rf, kc.minISR, kc.producers, kc.linger, kc.compression, kc.batchMaxBytes, kc.groupProtocol, kc.fetchMaxWait, kc.fetchMinBytes, kc.fetchMaxPartBytes, kc.sessionTimeout,
		in, cfg.Consumers, kc.txnTimeout, kc.txnBackoff, kc.txnMetaMinAge, tx.Size, tx.Linger, out, tx.Readers))
	adm := newAdmin(kc)
	defer adm.close()
	if err := adm.setupTopics(ctx, run, []string{in, out}); err != nil {
		fatal("%v", err)
	}
	if cfg.CreateOnly {
		fmt.Printf("[create] done: %s, %s x %d partitions ready\n", in, out, cfg.Partitions)
		return
	}
	stats := run.EnableTxn(tx.IdsOut)
	workers := newConsumers(run, kc, adm)
	workers.topicNames = []string{in}
	workers.worker = &workerCfg{tx: tx, stats: stats, prefix: cfg.Topic, out: out, timeout: kc.txnTimeout}
	readers := newConsumers(run, kc, adm)
	readers.topicNames = []string{out}
	readers.extraOpts = []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())}
	readers.readerTxn = stats
	readers.n = tx.Readers
	if tx.Readers > 0 {
		readers.offset, readers.total = tx.ReaderIndex(cfg, 0)
	}
	if err := workers.start(ctx); err != nil {
		fatal("%v", err)
	}
	if err := readers.start(ctx); err != nil {
		workers.stop()
		fatal("%v", err)
	}
	if err := workers.waitStable(ctx); err != nil {
		workers.stop()
		readers.stop()
		fatal("%v", err)
	}
	if err := readers.waitStable(ctx); err != nil {
		workers.stop()
		readers.stop()
		fatal("%v", err)
	}
	prods, err := newProducers(ctx, run, kc, []string{in})
	if err != nil {
		workers.stop()
		readers.stop()
		fatal("%v", err)
	}
	t0 := run.WaitStart(ctx)
	run.Produce(ctx, t0, prods.send)
	run.WaitInflight(kc.deliveryTimeout + 5*time.Second)
	end := run.DrainTxn(ctx.Done(), tx.IdleExit)
	run.StopReporter(end)
	workers.stop()
	readers.stop()
	prods.close()
	fmt.Printf("[info] txn workers began %d transactions\n", workers.worker.begins.Load())
	run.Finish(end)
}
