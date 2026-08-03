// Command cmbench runs the CM-BENCH channel-manager workload (SPEC.md) against
// one system under test and verifies the result.
//
//	cmbench -system pgmq   -pgmq-dsn 'postgres://...'      -rate 5000 -duration 1200
//	cmbench -system kafka  -kafka-seeds localhost:9092     -rate 5000 -duration 1200
//	cmbench -system rabbit -rabbit-url amqp://...          -rate 5000 -duration 1200 -lanes 100
//	cmbench -system mem                                    -rate 500  -duration 30
//
// Always run -system mem first on a new machine: it is the control (SPEC.md),
// and a PASS there is what licenses you to attribute later defects to a system
// rather than to the rig.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"crossbench/internal/broker"
	"crossbench/internal/broker/kafka"
	"crossbench/internal/broker/mem"
	"crossbench/internal/broker/pgmq"
	"crossbench/internal/broker/queen"
	"crossbench/internal/broker/rabbit"
	"crossbench/internal/runner"
	"crossbench/internal/verify"
	"crossbench/internal/workload"
)

func main() {
	fs := flag.NewFlagSet("cmbench", flag.ExitOnError)

	system := fs.String("system", "", "system under test: mem | queen | kafka | rabbit | pgmq")
	rate := fs.Int("rate", 5000, "R: total offered events/s, split 50/50 across the two flows")
	properties := fs.Int("properties", 1000, "P: ordering-key cardinality (properties)")
	duration := fs.Int("duration", 1200, "rated window, seconds")
	ramp := fs.Int("ramp", 30, "linear ramp to full rate, seconds")
	drain := fs.Int("drain", 60, "grace after the producers stop, seconds")
	report := fs.Int("report", 1, "report interval, seconds")
	logDir := fs.String("logdir", "./cmlogs", "stage logs + produced.meta + result.json")
	payloadB := fs.Int("payload-b", 2048, "flow-B rates payload target bytes")
	prefix := fs.String("prefix", "", "prefix on all four topic names. Belt-and-braces against cross-run contamination: -reset should already give a clean slate, but a run that inherits a previous run's backlog and cursors reports the old tail before the new seq 0, which reads as order violations that belong to nobody")

	lanes := fs.Int("lanes", 0, "physical ordered lanes per topic (0 = one per property). Lower it to measure head-of-line blocking between properties")
	durability := fs.String("durability", "default", "durability tier: default | fsync (SPEC.md §5.3 — never compare across tiers silently)")
	dedup := fs.Int("dedup-window", 0, "broker-side dedup window seconds where the system has it (headline runs use 0 everywhere, SPEC.md §5.4)")
	batch := fs.Int("batch", 100, "max messages per key-batch handed to a stage")
	prefetch := fs.Int("prefetch", 0, "per-lane prefetch hint (0 = adapter default)")
	laneHeadroom := fs.Float64("lane-headroom", 1.5, "multiplier on the Little's law lane budget")
	pushShards := fs.Int("push-shards", 64, "ordered publisher shards per flow")
	noReset := fs.Bool("no-reset", false, "keep the existing topology instead of recreating it")
	noWarmup := fs.Bool("no-warmup", false, "skip pre-hydrating the properties before the rated window")

	verifyOnly := fs.String("verify-only", "", "skip the run: verify an existing log directory and exit")

	kafkaSeeds := fs.String("kafka-seeds", "localhost:9092", "kafka bootstrap servers, comma separated")
	kafkaMembers := fs.Int("kafka-members", 4, "consumer-group members per stage")
	kafkaLinger := fs.Duration("kafka-linger", 5*time.Millisecond, "producer linger")

	rabbitURL := fs.String("rabbit-url", "amqp://guest:guest@localhost:5672/", "rabbitmq URL")
	rabbitQType := fs.String("rabbit-queue-type", "classic", "classic | quorum")
	rabbitPubChans := fs.Int("rabbit-publish-channels", 64, "publish channel pool size")

	pgmqDSN := fs.String("pgmq-dsn", "postgres://postgres:postgres@localhost:5432/postgres", "pgmq DSN")
	pgmqReadFn := fs.String("pgmq-read-fn", "read_grouped_rr", "grouped read function (see the package comment before changing)")
	pgmqReaders := fs.Int("pgmq-readers", 8, "concurrent read loops per stage")
	pgmqConns := fs.Int("pgmq-max-conns", 160, "connection pool size")
	pgmqVT := fs.Int("pgmq-vt", 60, "visibility timeout seconds (the lease)")

	queenURL := fs.String("queen-url", "http://127.0.0.1:6632", "queen broker base URL")
	queenPopMode := fs.String("queen-pop-mode", "wildcard", "consumption mode, a REPORTED AXIS: wildcard | targeted. wildcard = broker assigns lanes dynamically (the architectural claim; pays the candidate scan). targeted = application owns a static partition map (faster; gives the claim up, Queen's counterpart of Kafka static assignment). Run BOTH and publish the pair")
	queenPopParts := fs.Int("queen-pop-partitions", 10, "multi-partition cap for wildcard pops")
	queenLease := fs.Int("queen-lease", 60, "leaseTime seconds on all four queues")
	queenToken := fs.String("queen-token", "", "tenant API key: aims the run at queen_proxy instead of straight at the broker")
	queenAutoAck := fs.Bool("queen-autoack", false, "pop with autoAck=true (no lease, no client ack) — A/B lever to measure the ack path's latency contribution")
	queenPopWorkers := fs.Int("queen-pop-workers", 0, "override pop workers per stage (0 = derive from the lane budget). At low rates the lane budget is small but the PARTITION count is not, and latency is set by how often a partition gets revisited — so this may need to be far larger than the lanes suggest")
	queenPopWait := fs.Bool("queen-pop-wait", true, "long-poll: empty pops park server-side instead of spinning")
	queenPopTimeout := fs.Int("queen-pop-timeout", 2000, "long-poll timeout ms")
	queenSweepFloor := fs.Duration("queen-sweep-floor", 250*time.Millisecond, "targeted mode: minimum wall time per full partition sweep")

	_ = fs.Parse(os.Args[1:])

	if *verifyOnly != "" {
		os.Exit(runVerifyOnly(*verifyOnly, *properties, *rate, *prefix))
	}
	if *system == "" {
		fmt.Fprintln(os.Stderr, "cmbench: -system is required (mem | queen | kafka | rabbit | pgmq)")
		fs.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := runner.DefaultConfig()
	cfg.Topology.Properties = *properties
	cfg.Topology.RateEvents = *rate
	cfg.Topology.PayloadB = *payloadB
	cfg.Topology.Prefix = *prefix
	cfg.LogDir = *logDir
	cfg.DurationSec = *duration
	cfg.RampSec = *ramp
	cfg.DrainSec = *drain
	cfg.ReportSec = *report
	cfg.LaneHeadroom = *laneHeadroom
	cfg.BatchSize = *batch
	cfg.Prefetch = *prefetch
	cfg.PushShards = *pushShards
	cfg.Warmup = !*noWarmup
	cfg.Setup = broker.SetupOpts{
		PhysicalLanes:  *lanes,
		DedupWindowSec: *dedup,
		Durability:     *durability,
		Reset:          !*noReset,
	}

	var b broker.Broker
	var err error
	switch strings.ToLower(*system) {
	case "mem":
		b = mem.New()
	case "kafka":
		kc := kafka.DefaultConfig(strings.Split(*kafkaSeeds, ","))
		kc.MembersPerGroup = *kafkaMembers
		kc.Linger = *kafkaLinger
		kc.Durability = *durability
		b, err = kafka.New(ctx, kc)
	case "rabbit":
		rc := rabbit.DefaultConfig(*rabbitURL)
		rc.QueueType = *rabbitQType
		rc.PublishChannels = *rabbitPubChans
		b, err = rabbit.New(ctx, rc)
	case "pgmq":
		pc := pgmq.DefaultConfig(*pgmqDSN)
		pc.ReadFn = *pgmqReadFn
		pc.Readers = *pgmqReaders
		pc.MaxConns = int32(*pgmqConns)
		pc.VisibilityTimeoutSec = *pgmqVT
		b, err = pgmq.New(ctx, pc)
	case "queen":
		switch *queenPopMode {
		case "wildcard", "targeted":
		default:
			fmt.Fprintf(os.Stderr, "cmbench: -queen-pop-mode must be wildcard or targeted, got %q\n", *queenPopMode)
			os.Exit(2)
		}
		qcfg := queen.DefaultConfig(*queenURL)
		qcfg.Targeted = *queenPopMode == "targeted"
		qcfg.PopPartitions = *queenPopParts
		qcfg.LeaseTimeSec = *queenLease
		qcfg.Token = *queenToken
		qcfg.AutoAck = *queenAutoAck
		qcfg.PopWorkers = *queenPopWorkers
		qcfg.PopWait = *queenPopWait
		qcfg.PopTimeout = *queenPopTimeout
		qcfg.SweepFloor = *queenSweepFloor
		b, err = queen.New(ctx, qcfg)
	default:
		fmt.Fprintf(os.Stderr, "cmbench: unknown system %q\n", *system)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmbench: connecting to %s: %v\n", *system, err)
		os.Exit(1)
	}
	defer b.Close()

	res, err := runner.Run(ctx, b, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmbench: run failed: %v\n", err)
		os.Exit(1)
	}
	if err := writeResult(cfg.LogDir, res, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "cmbench: writing result.json: %v\n", err)
	}
	if !res.Verify.Pass {
		// A FAIL is a legitimate outcome that must be published (SPEC.md §5.6),
		// but it must never be mistaken for a clean run by a script.
		os.Exit(3)
	}
}

func runVerifyOnly(dir string, properties, rate int, prefix string) int {
	t := workload.DefaultTopology()
	t.Properties = properties
	t.RateEvents = rate
	t.Prefix = prefix
	stages := make([]verify.Stage, 0, 12)
	for _, s := range t.Stages() {
		stages = append(stages, verify.Stage{Topic: s.Topic, Group: s.Group, Flow: string(s.Flow)})
	}
	rep, err := verify.Run(dir, stages, 0, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	rep.Print(os.Stdout, dir)
	if !rep.Pass {
		return 3
	}
	return 0
}

// resultFile is the machine-readable run record. The cost table of SPEC.md §6.1
// is assembled by joining these across systems with the VM sampler output.
type resultFile struct {
	System      string         `json:"system"`
	StartedUnix int64          `json:"started_unix"`
	ElapsedSec  float64        `json:"elapsed_sec"`
	Rate        int            `json:"rate_events_per_sec"`
	Properties  int            `json:"properties"`
	Durability  string         `json:"durability"`
	DedupWindow int            `json:"dedup_window_sec"`
	Invariants  map[string]int `json:"invariants"`
	Provisioned map[string]any `json:"provisioned"`
	Flow        map[string]any `json:"flow"`
	Latency     map[string]any `json:"latency_ms"`
	Correctness map[string]any `json:"correctness"`
	BrokerStats map[string]any `json:"broker_stats"`
	Streams     []streamRecord `json:"streams"`
}

type streamRecord struct {
	Stream   string `json:"stream"`
	Msgs     int64  `json:"msgs"`
	Unique   int64  `json:"unique"`
	Dups     int64  `json:"dups"`
	Gaps     int64  `json:"gaps"`
	Viols    int64  `json:"order_violations"`
	Inflight int64  `json:"inflight_at_cutoff"`
	Pass     bool   `json:"pass"`
}

func writeResult(dir string, r *runner.Result, cfg runner.Config) error {
	c := r.Counters
	p50a, p95a, p99a := c.E2E(workload.FlowA)
	p50b, p95b, p99b := c.E2E(workload.FlowB)

	var acked, ackErr int64
	for _, sc := range r.Stages {
		acked += sc.Acked.Load()
		ackErr += sc.AckErr.Load()
	}

	streams := make([]streamRecord, 0, len(r.Verify.Stages))
	for _, s := range r.Verify.Stages {
		streams = append(streams, streamRecord{
			Stream: strings.TrimSuffix(s.File, ".log"), Msgs: s.Msgs, Unique: s.Unique,
			Dups: s.Dups, Gaps: s.Gaps, Viols: s.Viols, Inflight: s.Inflight, Pass: s.Pass,
		})
	}

	out := resultFile{
		System:      r.System,
		StartedUnix: time.Now().Add(-r.Elapsed).Unix(),
		ElapsedSec:  r.Elapsed.Seconds(),
		Rate:        r.Topology.RateEvents,
		Properties:  r.Topology.Properties,
		Durability:  cfg.Setup.Durability,
		DedupWindow: cfg.Setup.DedupWindowSec,
		Invariants: map[string]int{
			"deliveries_per_sec":       r.Invariants.DeliveriesPerSec,
			"ordered_lanes":            r.Invariants.OrderedLanes,
			"publishes_per_sec_native": r.Invariants.PublishNativeFan,
			"publishes_per_sec_copied": r.Invariants.PublishCopiedFan,
		},
		Provisioned: map[string]any{
			"ordered_lanes":               r.Provisioned.OrderedLanes,
			"physical_queues":             r.Provisioned.PhysicalQueues,
			"consumer_members":            r.Provisioned.ConsumerMembers,
			"connections":                 r.Provisioned.Connections,
			"publishes_per_ingress_event": r.Provisioned.PublishesPerIngressEvent,
			"semantics_built_in_app":      r.Provisioned.BuiltSemantics,
		},
		Flow: map[string]any{
			"offered": c.Offered.Load(), "published": c.Published.Load(),
			"derived": c.Derived.Load(), "delivered": c.Delivered.Load(),
			"processed": c.Processed.Load(), "shed": c.Shed.Load(),
			"push_err": c.PushErr.Load(), "push_retry": c.PushRetry.Load(),
			"acked": acked, "ack_err": ackErr,
			"slow_decodes": r.SlowDecodes,
		},
		Latency: map[string]any{
			"flow_a_p50": p50a, "flow_a_p95": p95a, "flow_a_p99": p99a,
			"flow_b_p50": p50b, "flow_b_p95": p95b, "flow_b_p99": p99b,
		},
		Correctness: map[string]any{
			"pass": r.Verify.Pass, "gaps": r.Verify.Gaps,
			"order_violations": r.Verify.Viols, "dups": r.Verify.Dups,
			"inflight_at_cutoff": r.Verify.Inflight,
		},
		BrokerStats: r.BrokerStats,
		Streams:     streams,
	}

	blob, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "result.json"), append(blob, '\n'), 0o644)
}
