// The IBM/sarama half of the M6 client matrix: the most-deployed pure-Go Kafka
// client, and the one whose legacy installed base makes it the interesting row.
// It runs against the same stack the franz-go suite uses (compat/rig.sh), with
// nothing faked in between.
//
//	go run . [bootstrap] [runId]          # or: ./run.sh
//	SCENARIO=group go run . 127.0.0.1:19092
//
// # WHAT THIS SUITE PROVES, and the one thing it exists to pin
//
// sarama does not negotiate the way the other four clients in the matrix do.
// franz-go, librdkafka, kafkajs and the Java client each pick a request version
// per API from what the broker advertised. sarama picks it from ONE knob,
// `Config.Version`, a Kafka RELEASE number: `Config.Version = V3_6_0_0` means
// "send every request at the version Kafka 3.6 speaks", which for Fetch is v12
// and for Produce is v10 — both outside this facade's advertised windows
// (Fetch 4..=6, Produce 3..=9; see protocols/queen-kafka/src/versions.rs), and an
// out-of-window version on an advertised key is answered by CLOSING the
// connection, not by an error code.
//
// What saves sarama is `restrictApiVersion` (sarama/api_versions.go), called
// from `Broker.sendInternal`: since the ApiVersions handshake was made
// unconditional (`Config.ApiVersionsRequest`, default true), sarama clamps every
// outgoing request down to the broker's advertised maximum. So the version that
// reaches the wire is min(Config.Version's version, our advertised max) — and
// the facade sees v6 Fetch and v9 Produce no matter how high `Config.Version`
// is set.
//
// THE CLAMP IS NEW. `restrictApiVersion` does not exist before sarama
// **v1.46.0** — v1.45.2 and every release below it has no such function, and
// sends whatever `Config.Version` names. That is the single fact a sarama
// deployment has to check before pointing at this facade, and the two failure
// modes below v1.46.0 are not symmetric:
//
//   - At v1.45.2's own DefaultVersion (2.1.0), Produce is v7 and lands, and
//     Fetch is v10. The facade closes the connection on every fetch
//     ("Fetch v10 is outside the advertised window 4..=6"), and sarama's
//     consumer retries it forever: "consumer/broker/0 disconnecting due to
//     error processing FetchRequest: EOF", over and over, no records, no
//     terminal error. Producing works. That is the dangerous shape.
//   - At Config.Version = 3.6, even Metadata is out of window (v10), so the
//     client cannot bootstrap at all and says so in seconds.
//
// The fix for an old sarama is `Config.Version = sarama.V1_0_0_0`, verified:
// Produce v5, Fetch v6, Metadata v5, all inside the windows.
//
// The clamp also makes ONE client-side setting load-bearing on the CURRENT
// line, and the `noapiversions` scenario is the experiment that shows it: with
// `Config.ApiVersionsRequest = false` there is no advertised table to clamp
// against, sarama sends what `Config.Version` says, and the client dies the same
// way. Two ordinary configurations reach that state without anyone choosing it
// — turning the handshake off, and leaving `Net.SASL.Version` at its default
// `SASLHandshakeV0`, which sarama's own config validation refuses to combine
// with the handshake. Both are the CLIENT'S doing, not the facade's, and both
// are recorded here rather than argued about.
//
// The rest is the ordinary bar: 512 keyed records with headers over 8
// partitions, every codec sarama can encode, a consumer group that reads them
// back byte-exact and in order, a commit that a second group member resumes
// from, an auto-created topic, the offset bounds, and (with a TLS listener) one
// produce+consume over SASL/PLAIN.
//
// WHAT IS THE CLIENT'S FAULT AND NOT THE FACADE'S
//
//   - `Config.Producer.Idempotent = true` WORKS since M7 F3: InitProducerId
//     (key 22) is advertised 0..=4 and the per-partition sequence window is
//     enforced (protocols/queen-kafka/src/idempotent.rs). Before F3 the connection closed
//     on the unadvertised key and sarama retried it Producer.Transaction.Retry.Max
//     (50) times, one fresh connection each. Measured by `edges`.
//   - sarama's `ClusterAdmin` WORKS since M7 F1/F2: it is built on ListGroups
//     (16), CreateTopics (19), DeleteTopics (20) and DescribeConfigs (32), and
//     all four are advertised. Before them even `ListTopics` was refused, on
//     the DescribeConfigs it issues after its Metadata rather than on the
//     Metadata itself. Measured by `edges`.
//   - `ConsumePartition` at an out-of-range offset is refused by sarama itself,
//     from the bounds our ListOffsets gave it, before any Fetch goes out. That
//     is a ListOffsets test wearing an error's clothes; `offsets` says so.
//   - Every consumer group formation costs the facade's
//     QUEEN_KAFKA_GROUP_JOIN_DELAY_MS (3s by default, Kafka's
//     group.initial.rebalance.delay.ms). Slow is not broken.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------- environment

type env struct {
	bootstrap string // plaintext listener, e.g. 127.0.0.1:19092
	runID     string // stamped into every topic and group name
	queenURL  string // the broker's HTTP API, for cross-checks (optional)
	partsWant int    // QUEEN_KAFKA_DEFAULT_PARTITIONS on the facade
	tlsBoot   string // the --m5 listener, empty to skip the SASL lane
	saslToken string // the Queen bearer token = the SASL password
	tlsCert   string // PEM of the listener's self-signed certificate
}

func readEnv(args []string) env {
	e := env{
		bootstrap: getenv("QUEEN_KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
		runID:     getenv("RUN_ID", strconv.FormatInt(time.Now().Unix(), 10)),
		queenURL:  os.Getenv("QUEEN_URL"),
		tlsBoot:   os.Getenv("QUEEN_KAFKA_TLS_BOOTSTRAP"),
		saslToken: os.Getenv("QUEEN_KAFKA_SASL_TOKEN"),
		tlsCert:   os.Getenv("QUEEN_KAFKA_TLS_CERT"),
	}
	e.partsWant, _ = strconv.Atoi(getenv("QUEEN_KAFKA_PARTITIONS", "8"))
	if e.partsWant <= 0 {
		e.partsWant = 8
	}
	// Positional argv wins, mirroring compat/librdkafka and compat/java.
	if len(args) > 0 && args[0] != "" {
		e.bootstrap = args[0]
	}
	if len(args) > 1 && args[1] != "" {
		e.runID = args[1]
	}
	return e
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ------------------------------------------------------------------ scenarios

type scenario struct {
	name string
	run  func(*runner, *state)
}

// Order is dependency order: `produce` fills the state the later scenarios read.
var scenarios = []scenario{
	{"versions", scenarioVersions},
	{"produce", scenarioProduce},
	{"compression", scenarioCompression},
	{"group", scenarioGroup},
	{"resume", scenarioResume},
	{"autocreate", scenarioAutocreate},
	{"offsets", scenarioOffsets},
	{"defaults", scenarioDefaults},
	{"versionsweep", scenarioVersionSweep},
	{"noapiversions", scenarioNoApiVersions},
	{"edges", scenarioEdges},
	{"sasl", scenarioSasl},
}

func main() {
	e := readEnv(os.Args[1:])
	want := getenv("SCENARIO", "all")
	if len(os.Args) > 3 {
		want = os.Args[3]
	}

	fmt.Printf("queen-kafka compat: IBM/sarama %s\n", saramaModuleVersion())
	fmt.Printf("bootstrap=%s runId=%s partitions=%d tlsBootstrap=%q\n",
		e.bootstrap, e.runID, e.partsWant, e.tlsBoot)
	fmt.Printf("scenario=%s\n", want)

	r := &runner{}
	st := &state{env: e}

	ran := 0
	for _, s := range scenarios {
		if want != "all" && want != s.name {
			continue
		}
		ran++
		r.section(s.name)
		func() {
			defer func() {
				if p := recover(); p != nil {
					r.fail("scenario %s panicked: %v", s.name, p)
				}
			}()
			s.run(r, st)
		}()
	}
	if ran == 0 {
		fmt.Printf("no scenario matched %q; known: %s\n", want, strings.Join(scenarioNames(), " "))
		os.Exit(2)
	}

	r.section("wire versions sarama actually sent")
	globalTap.report()

	if r.fails == 0 {
		fmt.Printf("\nRESULT: PASS (%d checks)\n", r.checks)
		return
	}
	fmt.Printf("\nRESULT: FAIL (%d) of %d checks\n", r.fails, r.checks)
	os.Exit(1)
}

func scenarioNames() []string {
	out := make([]string, 0, len(scenarios))
	for _, s := range scenarios {
		out = append(out, s.name)
	}
	return out
}
