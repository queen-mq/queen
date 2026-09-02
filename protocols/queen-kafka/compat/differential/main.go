// Command differential runs the same wire-level scenarios against the
// queen-kafka facade and against a real single-node Apache Kafka, and diffs
// what a client can OBSERVE: error codes, offsets, watermarks, response shapes.
//
// It is the M6 oracle from PLAN_QUEEN_KAFKA.md. What it deliberately does not
// diff: timings, node ids, hosts and ports, throttle values, cluster ids and
// anything the broker generates freely (member ids, coordinator identity). A
// facade that answered those identically would be lying about being itself.
//
//	protocols/queen-kafka/compat/differential/rig-diff.sh run          # stack + runner
//	QK_FACADE=host:port QK_KAFKA=host:port go run .          # against a live pair
//	... -run group                                           # one scenario
//
// Exit status is 1 when a divergence appears that is not in the deliberate
// deviation table at the bottom of this file, so the runner can become a gate
// later without being one today.
package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ------------------------------------------------------------------ recording

type observation struct {
	key  string
	val  string
	info bool
}

// recorder collects one target's answers for one scenario. Keys are stable
// strings; every key a scenario records against one broker it must also record
// against the other, or the diff reports the absence — which is itself a
// finding, and the reason nothing here is recorded conditionally on success.
type recorder struct {
	obs   []observation
	index map[string]int
}

func newRecorder() *recorder { return &recorder{index: map[string]int{}} }

func (r *recorder) put(key, val string, info bool) {
	if i, ok := r.index[key]; ok {
		r.obs[i].val = val
		return
	}
	r.index[key] = len(r.obs)
	r.obs = append(r.obs, observation{key: key, val: val, info: info})
}

// add records an observation that is diffed.
func (r *recorder) add(key, format string, args ...any) {
	r.put(key, fmt.Sprintf(format, args...), false)
}

// info records an observation that is printed side by side but never diffed:
// something legitimately broker-specific (a node id, a count of supported
// APIs) that is still worth having in the report.
func (r *recorder) info(key, format string, args ...any) {
	r.put(key, fmt.Sprintf(format, args...), true)
}

// bad records a failure. It is diffed like anything else: an error on one side
// and an answer on the other is the loudest divergence there is.
func (r *recorder) bad(key string, err error) {
	r.put(key, "ERROR: "+err.Error(), false)
}

// ------------------------------------------------------------------- scenarios

type target struct {
	label string // "facade" or "kafka"
	addr  string
}

func (t *target) dial() (*conn, error) { return dial(t.label, t.addr) }

type runctx struct {
	target *target
	rec    *recorder
	runID  string
	baseTS int64
	parts  int32
}

// topic returns a per-run topic name, identical on both brokers so the two
// recordings line up, distinct between runs so a second run against a stack
// that is still up starts from empty partitions.
func (c *runctx) topic(suffix string) string {
	return fmt.Sprintf("dt-%s-%s", c.runID, suffix)
}

func (c *runctx) group(suffix string) string {
	return fmt.Sprintf("dg-%s-%s", c.runID, suffix)
}

type scenario struct {
	name string
	desc string
	run  func(c *runctx)
}

var scenarios []scenario

// ---------------------------------------------------------------------- diff

type divergence struct {
	scenario string
	key      string
	facade   string
	kafka    string
	verdict  string
	why      string
}

func main() {
	facadeAddr := envOr("QK_FACADE", "127.0.0.1:29192")
	kafkaAddr := envOr("QK_KAFKA", "127.0.0.1:29092")
	only := flag.String("run", "", "regexp: only scenarios whose name matches")
	quiet := flag.Bool("quiet", false, "print divergences only, not every observation")
	flag.Parse()

	runID := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond)%100000000)
	// One base timestamp for both targets: record timestamps are then
	// directly comparable between the two brokers instead of being noise.
	baseTS := time.Now().UnixMilli() - 60_000

	facade := &target{label: "facade", addr: facadeAddr}
	kafka := &target{label: "kafka", addr: kafkaAddr}

	fmt.Printf("queen-kafka M6 differential\n")
	fmt.Printf("  facade  %s\n  kafka   %s\n  run id  %s\n  base ts %d\n\n",
		facadeAddr, kafkaAddr, runID, baseTS)

	var filter *regexp.Regexp
	if *only != "" {
		var err error
		filter, err = regexp.Compile(*only)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad -run regexp: %v\n", err)
			os.Exit(2)
		}
	}

	// A fixed order rather than the order the files happened to register in.
	// It is load-bearing in one place: the metadata scenario asks about
	// __consumer_offsets, which on Kafka does not exist until a group has
	// committed, so the group scenario has to have run.
	order := map[string]int{
		"produce-consume": 0, "listoffsets": 1, "fetch-bounds": 2,
		"group": 3, "metadata": 4, "apiversions": 5, "extras": 6,
	}
	sort.SliceStable(scenarios, func(i, j int) bool {
		oi, oki := order[scenarios[i].name]
		oj, okj := order[scenarios[j].name]
		if !oki || !okj {
			return oki && !okj
		}
		return oi < oj
	})

	var divs []divergence
	ran := 0
	for _, s := range scenarios {
		if filter != nil && !filter.MatchString(s.name) {
			continue
		}
		ran++
		fmt.Printf("=========================================================== %s\n", s.name)
		if s.desc != "" {
			fmt.Printf("            %s\n", s.desc)
		}
		recs := map[string]*recorder{}
		for _, t := range []*target{facade, kafka} {
			r := newRecorder()
			recs[t.label] = r
			c := &runctx{target: t, rec: r, runID: runID, baseTS: baseTS, parts: 8}
			func() {
				defer func() {
					if p := recover(); p != nil {
						r.bad("panic", fmt.Errorf("%v", p))
					}
				}()
				s.run(c)
			}()
		}
		divs = append(divs, report(s.name, recs["facade"], recs["kafka"], *quiet)...)
		fmt.Println()
	}

	if ran == 0 {
		fmt.Fprintln(os.Stderr, "no scenario matched")
		os.Exit(2)
	}

	fmt.Printf("=========================================================== SUMMARY\n")
	counts := map[string]int{}
	for _, d := range divs {
		counts[d.verdict]++
	}
	nReview := counts["REVIEW"]
	fmt.Printf("%d divergence(s): %d deliberate, %d accepted as harmless, %d to classify by hand\n\n",
		len(divs), counts["DELIBERATE"], counts["ACCEPTED"], nReview)
	for _, want := range []string{"REVIEW", "DELIBERATE", "ACCEPTED"} {
		for _, d := range divs {
			if d.verdict != want {
				continue
			}
			fmt.Printf("[%s] %s / %s\n", d.verdict, d.scenario, d.key)
			fmt.Printf("        facade = %s\n", d.facade)
			fmt.Printf("        kafka  = %s\n", d.kafka)
			if d.why != "" {
				fmt.Printf("        why    = %s\n", d.why)
			}
		}
	}
	if nReview > 0 {
		os.Exit(1)
	}
}

func report(name string, f, k *recorder, quiet bool) []divergence {
	if f == nil || k == nil {
		panic("a scenario recorded nothing for one of the targets")
	}
	// The union of the keys, in the order the facade recorded them, then any
	// the facade never produced.
	var keys []string
	seen := map[string]bool{}
	for _, o := range f.obs {
		keys = append(keys, o.key)
		seen[o.key] = true
	}
	var extra []string
	for _, o := range k.obs {
		if !seen[o.key] {
			extra = append(extra, o.key)
		}
	}
	sort.Strings(extra)
	keys = append(keys, extra...)

	var divs []divergence
	for _, key := range keys {
		fo, fok := get(f, key)
		ko, kok := get(k, key)
		switch {
		case fok && kok && fo.info:
			if !quiet {
				fmt.Printf("  i %-52s facade=%s kafka=%s\n", key, fo.val, ko.val)
			}
		case fok && kok && fo.val == ko.val:
			if !quiet {
				fmt.Printf("  = %-52s %s\n", key, fo.val)
			}
		default:
			fv, kv := "<not recorded>", "<not recorded>"
			if fok {
				fv = fo.val
			}
			if kok {
				kv = ko.val
			}
			verdict, why := classify(name, key)
			fmt.Printf("  %s %-52s\n        facade = %s\n        kafka  = %s\n",
				map[string]string{"DELIBERATE": "d", "ACCEPTED": "a", "REVIEW": "!"}[verdict], key, fv, kv)
			divs = append(divs, divergence{
				scenario: name, key: key, facade: fv, kafka: kv, verdict: verdict, why: why,
			})
		}
	}
	return divs
}

func get(r *recorder, key string) (observation, bool) {
	i, ok := r.index[key]
	if !ok {
		return observation{}, false
	}
	return r.obs[i], true
}

// ------------------------------------------------------- deliberate deviations

// The list from the queen-kafka README and the crate's own comments, as
// patterns over "scenario/key". A divergence that matches one of these is
// reported as DELIBERATE and does not fail the run; everything else lands in
// REVIEW and does.
var deliberate = []classification{
	{regexp.MustCompile(`^listoffsets/ts\.concrete\.`),
		"documented: ListOffsets with a concrete timestamp answers -1 with no error"},
	{regexp.MustCompile(`^listoffsets/nonexistent\.`),
		"documented minor: a concrete-timestamp probe on a topic that does not exist is answered as if it did"},
	{regexp.MustCompile(`^metadata/internal(_autocreate)?\.`),
		"documented: __-prefixed topics are hidden and refused everywhere"},
	{regexp.MustCompile(`^metadata/alltopics\.has_consumer_offsets$`),
		"documented: __-prefixed topics are hidden; the facade has no __consumer_offsets to hide either"},

	// ------------------------------------------ M7 F3: the idempotent producer
	{regexp.MustCompile(`^initproducerid/initproducerid\.bump\.(keeps_the_id|epoch)$`),
		"deliberate, and the ONE place F3 does not copy Apache Kafka. Kafka's non-transactional path answers a " +
			"KIP-360 bump by blindly ALLOCATING a fresh producer id at epoch 0 " +
			"(TransactionCoordinator.handleInitProducerId: 'if the transactional id is null, always blindly " +
			"accept'); the facade answers the SAME id one epoch higher. The epoch is what discriminates one " +
			"producer session from the next inside idempotent.rs's key, so bumping it invalidates everything " +
			"remembered under the old epoch while keeping the producer's identity stable across a recovery, and " +
			"it costs no entropy per recovery. Either answer satisfies every client: the Java client takes " +
			"whatever the response says (TransactionManager.setProducerIdAndEpoch) and resets its own sequences " +
			"because it asked for a bump. Measured on both sides above: bump.error_code is NONE on both"},
	{regexp.MustCompile(`^initproducerid/initproducerid\.transactional\.(error_code|granted_an_id)$`),
		"REWRITTEN BY M9, and it no longer says what it said: the facade GRANTS a transactional id now (single " +
			"mode; crate::txn), so this scenario's `granted_an_id` is true here and false on the oracle. The " +
			"oracle's false is a WARM-UP and not a refusal — Kafka answers NOT_COORDINATOR while its " +
			"__transaction_state partitions are still loading, the same first-minute transient the " +
			"extras/initproducerid entry below already names, and this scenario is the second of eight to run. " +
			"The settled comparison is in the `transactions` scenario further down the same report, which " +
			"retries those three coordinator codes before recording: there init.error_code is NONE and " +
			"init.granted_an_id is true on BOTH brokers. Two facts do stay genuinely different and neither is " +
			"here: the producer id's value (info, never diffed) and cluster mode, where this facade answers " +
			"TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) — fatal, so initTransactions() returns in milliseconds " +
			"instead of looping to max.block.ms — because transactions are single-node by configuration"},

	// --------------------------------------------------------- M9: transactions
	{regexp.MustCompile(`^transactions/\w+\.produce\d+\.base_offset$`),
		"THE M9 divergence, and the one the design asks to be ratified rather than assumed harmless. Kafka " +
			"appends a transactional batch to the log as it arrives and answers the real offset; this facade " +
			"STAGES it and allocates no offset until EndTxn(commit), so the produce answer has no offset to " +
			"carry and says -1. The Java client's RecordMetadata keeps -1 unchanged rather than adding the " +
			"batch index (bytecode-verified), so no fabricated offset ever reaches an application: what an " +
			"application loses is RecordMetadata.offset() inside a transaction, and what it gains is that " +
			"nothing partial is ever in the log. Every non-transactional produce still answers a real offset, " +
			"which is produce-consume's own assertion"},
	{regexp.MustCompile(`^transactions/commit\.log_end_offset$`),
		"Kafka advances the partition by N+1 — the records plus the COMMIT MARKER, a control batch it writes " +
			"into the data partition — and this facade by N, because it writes no markers at all: a committed " +
			"transaction here is one Postgres transaction of ordinary records. The consequence a client can see " +
			"is measured beside this and is nothing: commit.read_committed_records is 10 on both. The oracle's " +
			"marker is written after EndTxn returns, so the runner settles on lso == hw before asking, or this " +
			"row would be the runner's timing rather than a difference"},
	{regexp.MustCompile(`^transactions/abort\.log_end_offset$`),
		"the same marker rule with the aborted records added: Kafka advances by N+1 and LEAVES the aborted " +
			"records in the log for the client to filter, this facade advances by 0 because an aborted " +
			"transaction is a stage that is dropped and never reached the log. This is the direction that " +
			"cannot lose data — there is nothing to filter because there is nothing there"},
	{regexp.MustCompile(`^transactions/read_uncommitted\.visible_before_commit$`),
		"the ONE place a consumer of this facade sees LESS than it would see of Kafka, and the second " +
			"divergence M9 asks to be ratified. A read_uncommitted consumer of Kafka sees an open " +
			"transaction's records immediately; here it sees them at commit. Nothing a real client does " +
			"depends on the difference: read_uncommitted is the DEFAULT, so the records an ordinary consumer " +
			"sees are the same records in the same order, later by the producer's own commit cadence, and no " +
			"client library exposes 'records that may yet be rolled back' as a state an application can act " +
			"on. It also means read_committed and read_uncommitted return the same records here, which is why " +
			"the read_committed lag on this facade reaches 0 where Kafka's stops at 1 per partition",
	},
	{regexp.MustCompile(`^transactions/fetch\.(aborted_transactions|last_stable_offset)$`),
		"the two fields a read_committed client uses to filter, measured after an ABORT. Kafka's " +
			"aborted_transactions names the producer whose records the client must now skip and its last " +
			"stable offset trails the high watermark; the facade's list is always empty and its LSO is always " +
			"the high watermark, because no uncommitted record ever entered the log and there is nothing for a " +
			"client to skip. A client that does the filtering correctly against Kafka does nothing at all here, " +
			"which is the only behaviour this pair drives"},
	{regexp.MustCompile(`^transactions/endtxn\.unknown\.`),
		"an EndTxn for a transactional id neither broker was asked to open, and both refuse it fatally with a " +
			"NON-retriable code — which is the property a client acts on. The codes differ because the two " +
			"brokers know different things: Kafka has durable coordinator state, finds a mapping for the id " +
			"and refuses the producer id in it (INVALID_PRODUCER_ID_MAPPING, 49); the facade holds no stage, " +
			"so what it is refusing is the whole transaction (INVALID_TXN_STATE, 48). 48 is also what it " +
			"answers a transaction it LOST — a restart, a moved connection — and that is the answer that " +
			"matters: it is the only one that cannot let an application believe an uncommitted commit",
	},
	{regexp.MustCompile(`^transactions/addpartitions\.unknown_topic\.error_code$`),
		"documented in compat/ERRORS.md under AddPartitionsToTxn: a topic that merely does not exist yet is " +
			"NOT refused here, because the produce path auto-creates and the enrolment is not the surface that " +
			"decides whether a topic may exist. Kafka refuses it UNKNOWN_TOPIC_OR_PARTITION, which is " +
			"retriable, so a client meeting either answer refreshes metadata and produces. Neither broker " +
			"CREATES anything from the enrolment: addpartitions.unknown_topic.exists_after is false on both, " +
			"which is the part that would have been a defect"},
	{regexp.MustCompile(`^initproducerid/initproducerid\.empty_transactional\.`),
		"MEASURED, and the divergence is the fix: Apache Kafka refuses a ZERO-LENGTH transactional id " +
			"INVALID_REQUEST, because \"\" is not null and fails its own validation. The facade reads it as the " +
			"absent id it was meant to be (idempotent::transactional_id). Erlang's kafka_protocol 4.3.6 — brod, " +
			"and with it broadway_kafka and kaffe, which is most of the Elixir in production — hand-rolls its " +
			"encoders and writes a null nullable_string as \"\" (kpro_lib.erl:140). Copying Kafka here would " +
			"refuse every Elixir producer a producer id. Kafka never meets the bug because brod does not send " +
			"InitProducerId to it either; the facade does, and this is the same filter produce.rs already " +
			"applies to the same field (compat/brod/README.md)"},
	{regexp.MustCompile(`^idempotent/idempotent\.unknown_producer\.error_code$`),
		"MEASURED and DELIBERATE, and it is the one row worth arguing with. Apache Kafka 3.9.1 ACCEPTS a batch " +
			"at sequence 42 from a producer id it holds no state for (error NONE): it persists producer state in " +
			"the log, so an absent entry means the state was aged out, which is rare and benign. This facade " +
			"holds no durable producer state by design, so an absent entry is COMMON — every restart produces " +
			"one — and accepting would mean the sequence window is silently unenforced for exactly as long as a " +
			"producer keeps running after a restart. It answers OUT_OF_ORDER_SEQUENCE_NUMBER (45) instead, and " +
			"the cost of that choice was measured rather than assumed: " +
			"compat/go/idempotent_test.go TestAnIdempotentProducerSurvivesAFacadeRestart SIGKILLs the facade " +
			"under a live default-idempotence producer and the producer keeps running, every record accounted " +
			"for. UNKNOWN_PRODUCER_ID (59) is not used for the reason Kafka retired it: some clients answer it " +
			"by reasoning about log_start_offset, which this facade answers as -1"},

	// ------------------------------------------------ M7 F1: topics admin
	{regexp.MustCompile(`^createtopics/create\.\w+\.num_partitions$`),
		"since M7 this is NOT a deviation: Queen still declares no width per queue (configure_queue_v1 creates " +
			"the queue row and nothing else; a log_partitions row materialises on the first push), but the facade " +
			"stores a declared num_partitions as the topic's own width FLOOR and reports max(live lanes, that " +
			"floor). A brand-new topic has no lanes, so both brokers answer the count that was asked for. The " +
			"property that matters is asserted beside this one: create.new.metadata_agrees is true on both " +
			"brokers, so the number the create reports is the number the client's next Metadata hashes modulo"},
	{regexp.MustCompile(`^createtopics/create\.internal_name\.`),
		"documented: __-prefixed topics are hidden and refused everywhere (metadata::reserved_or_invalid). Apache " +
			"Kafka treats them as ordinary names; creating one here would make a queue the facade then refuses to " +
			"show, so CreateTopics — the surface where a NAME is validated — answers INVALID_TOPIC_EXCEPTION"},
	{regexp.MustCompile(`^createtopics/create\.compact\.`),
		"deliberate, and the loudest refusal in the stage: log compaction is a stated non-goal, so " +
			"cleanup.policy=compact is answered INVALID_CONFIG instead of being accepted and not performed. This " +
			"is what makes Kafka Connect fail at STARTUP rather than lose its connector configuration on a later " +
			"restart, and it is why CreateTopics does not unlock Connect"},
	{regexp.MustCompile(`^createtopics/create\.kafka_only_config\.`),
		"deliberate: a topic config Kafka has and Queen has no mechanism for is refused INVALID_CONFIG rather " +
			"than dropped. min.insync.replicas=1 happens to be the value the facade would report anyway, but " +
			"accepting the KEY would mean accepting min.insync.replicas=2 too — telling a client it got a " +
			"durability setting it did not get. The mapping is topic_config.rs and it is deliberately short",
	},
	{regexp.MustCompile(`^describeconfigs/topic\.retention_ms$`),
		"deliberate, and it is a DEFAULT difference rather than a missing key since M7 F4: retention now round-trips " +
			"for a topic this facade created, read from the record of the options bag the facade itself posted " +
			"(topic_record.rs), because Queen exposes NO HTTP read of those columns (get_queue_v2 answers no config " +
			"at all; get_queue_detail_v2's config object has leaseTime, retryLimit, retryDelay, ttl, maxQueueSize and " +
			"deadLetterQueue and NOT retentionEnabled/retentionSeconds). What still differs is the VALUE on a fresh " +
			"topic: Queen's default is retention OFF, which is Kafka's -1, where Kafka's own default is 604800000. A " +
			"topic this facade did not create still omits the key, since reporting a plausible default for a queue " +
			"whose columns cannot be read would be a guess",
	},
	{regexp.MustCompile(`^describeconfigs/topic\.(cleanup_policy|min_insync_replicas)_read_only$`),
		"deliberate: read_only is PER ROW since M7 F4, and these two are the rows that are genuinely fixed — the " +
			"only value either of them accepts is the one already reported (delete; one logical broker, so 1), so " +
			"nothing about them can be changed through this facade. Kafka reports them writable because its " +
			"AlterConfigs can really change them. retention.ms is NOT in this pattern: it is reported writable here " +
			"too, because AlterConfigs and IncrementalAlterConfigs land on it. A UI that greys out its edit button " +
			"on this flag is being told the truth by both"},
	{regexp.MustCompile(`^describeconfigs/broker_logger\.`),
		"deliberate: BROKER_LOGGER (resource type 8) describes a log4j hierarchy. The facade runs none, so it " +
			"answers INVALID_REQUEST rather than an empty config set, which would read as 'this resource exists " +
			"and is empty'"},

	// ------------------------------------------------ M7 F2: groups admin
	{regexp.MustCompile(`^groups-admin/.*\.authorized_operations$`),
		"deliberate: Kafka 3.9.1 with no authorizer computes READ|DELETE|DESCRIBE (328). The facade answers " +
			"i32::MIN, which is Kafka's OWN AUTHORIZED_OPERATIONS_OMITTED — the Java client turns it into null " +
			"and tools render 'unknown'. It has no ACL model: what a credential may do is Queen's to say, per " +
			"call, and it says so by answering 401 or 403 to that call. A bitfield computed here would be a " +
			"permission set this process invented, and a UI that greyed out a button on it would be acting on a " +
			"guess (handlers::describe_groups)"},
	{regexp.MustCompile(`^groups-admin/badname\.`),
		"deliberate, and it is the facade's own bound rather than a protocol one: a group id is refused " +
			"INVALID_GROUP_ID when it is empty or longer than 255 characters (coordinator::invalid_group_id), " +
			"because every copy of a group id — the registry key, the composed KV key, every log line about it — " +
			"is this facade's, and the protocol gives the field no bound at all (a client may send ~32 KB of one " +
			"at the non-flexible versions). Kafka has no such bound and answers `Dead` / GROUP_ID_NOT_FOUND for " +
			"these names. The rule is applied by all six group-addressed APIs through ONE function, so a name " +
			"JoinGroup refuses and DescribeGroups describes cannot exist, and 24 is on the closed set every " +
			"client accepts on both APIs"},

	// ------------------------------ M7 F4: the two remaining admin writes
	{regexp.MustCompile(`^createpartitions/increase\.`),
		"deliberate, and the ONE case of this API that is a capability gap rather than a copy of Kafka's own " +
			"answer: the oracle widens the topic and answers 0, the facade refuses INVALID_PARTITIONS. Queen " +
			"declares no width per queue at all — /configure has no `partitions` option and a lane exists once " +
			"something has been pushed to it — so the number a client sees is max(live lanes, " +
			"QUEEN_KAFKA_DEFAULT_PARTITIONS), whose second half is a broker START-UP setting rather than a " +
			"per-topic one. There is no write that widens one topic, so the refusal names the knob that does " +
			"(handlers::create_partitions). The decrease and equal cases are NOT in this pattern: those two are " +
			"the oracle's own sentences, byte for byte, and must stay identical"},
	{regexp.MustCompile(`^createpartitions/assignments\.message$`),
		"deliberate: both brokers answer INVALID_REPLICA_ASSIGNMENT (39) — that key is identical and is the one " +
			"a client branches on — but they are complaining about different things. The oracle is counting: two " +
			"new partitions asked for, one assignment given. The facade refuses manual placement outright, " +
			"because it is one logical broker and places no partition on any node, so an assignment is an " +
			"operator instruction it would otherwise discard in silence. Same sentence CreateTopics gives the " +
			"same field"},
	{regexp.MustCompile(`^offsetdelete/after\.list_groups\.contains$`),
		"deliberate, and measured on apache/kafka:3.9.1 rather than assumed: the oracle DROPS a group once the " +
			"last of its offsets is deleted, so it stops listing and answers GROUP_ID_NOT_FOUND to the next " +
			"request, while a PARTIAL delete leaves it listed. The facade leaves it listed either way, because " +
			"OffsetDelete removes offsets and DeleteGroups removes groups — that split is what keeps one API the " +
			"only thing that makes a group stop existing, and matching the oracle would mean a prefix walk on " +
			"every OffsetDelete to find out whether anything was left. Every other key in this scenario, the " +
			"subscription rule included, must stay identical (compat/ERRORS.md)"},
}

type classification struct {
	pat *regexp.Regexp
	why string
}

// Divergences the M6 pass looked at and judged harmless: real differences in
// what a client sees, none of which changes what a client can DO. They are
// listed here rather than left in REVIEW so that a later run fails only on
// something new — and each one carries the reason it was let through, so the
// judgement can be argued with instead of being invisible.
var accepted = []classification{
	{regexp.MustCompile(`^produce-consume/\w+\.fetch\.batch0\.(codec|last_offset_delta|max_timestamp|num_records_field)$`),
		"the facade re-batches on the fetch path (two produced batches come back as one) and does not re-compress; " +
			"batch boundaries and the wire codec are the broker's choice in Kafka too, and every record, key, header, " +
			"timestamp and offset is identical"},
	{regexp.MustCompile(`^(listoffsets/v5\.latest|metadata/known_v9\.p0)\.leader_epoch$`),
		"the facade has no leader epochs to report; -1 is Kafka's own 'unknown epoch' sentinel and clients skip " +
			"epoch validation on it rather than misbehaving"},
	{regexp.MustCompile(`^extras/initproducerid\.error_code$`),
		"a startup transient on the ORACLE's side, not a facade difference: `extras` is the first scenario to " +
			"run and Kafka's __transaction_state log may still be loading, which it answers " +
			"COORDINATOR_LOAD_IN_PROGRESS. The same question asked later, by the initproducerid scenario, is " +
			"answered NONE by both (initproducerid.fresh.error_code). Since M7 F3 the facade grants the id here " +
			"rather than refusing the key"},
	{regexp.MustCompile(`^extras/produce_v2\.`),
		"Produce v2 is below the advertised floor (versions.rs offers 3-9) and the facade closes the connection, " +
			"which is what Kafka itself does with a version it does not know — metadata_v99 closes identically on " +
			"both sides. Kafka 3.9 merely still supports v2"},
	{regexp.MustCompile(`^produce-consume/\w+\.produce1\.log_start_offset$`),
		"the produce answer carries no log start offset (-1, the field's own default in Kafka's schema and the " +
			"Java client's) because a push reports offsets and statuses and no lower bound: filling it in would " +
			"cost a bounds probe per produce, on the write path. The REASON was rewritten by M9, because the " +
			"old one — 'a field only the idempotent producer reads, and that producer is refused outright' — " +
			"stopped being true when F3 implemented the idempotent producer and stopped being defensible when " +
			"M9 implemented transactions. The true reason is narrower and survives both: the client's only use " +
			"for this field is UNKNOWN_PRODUCER_ID (59) recovery, which reasons about how far the log start has " +
			"moved, and this facade never answers 59. It answers OUT_OF_ORDER_SEQUENCE_NUMBER for a lost window " +
			"instead, whose recovery is KIP-360's epoch bump and reads nothing here (crate::idempotent). The " +
			"fetch path reports the real log start on both sides, which is where a client looks for it"},
	{regexp.MustCompile(`^transactions/(addpartitions|addoffsets|endtxn|txnoffsetcommit)\.v4\.unsupported$`),
		"one version above the facade's advertised ceiling of 3, where the facade closes the connection — what " +
			"it does for every version outside its window, and what Apache Kafka does for a version it does not " +
			"know (extras/metadata_v99 closes identically on both). Kafka 3.9.1 simply knows these four " +
			"versions. Each ceiling has its own argument in versions.rs and AddPartitionsToTxn's is the " +
			"strongest: its v4 is a DIFFERENT REQUEST — KIP-890's coordinator-to-leader verification, carrying " +
			"a transactions[] array and a verify_only flag — that only another broker sends, and the oracle's " +
			"own answer to a CLIENT that sends it is the 'no response, connection left open' recorded here"},

	// ------------------------------------------------ M7 F1: topics admin
	{regexp.MustCompile(`^createtopics/create\.duplicate\.error_codes$`),
		"the facade answers ONE result per request entry, so a name sent twice comes back twice; Kafka collapses " +
			"the pair into one result. Both answer INVALID_REQUEST and both create nothing " +
			"(create.duplicate.exists_after is false on both), which is the part a client acts on. No client " +
			"indexes this API positionally — the Java AdminClient and franz-go's kadm both key their futures by " +
			"topic name — and one-result-per-entry is what the facade does for DeleteTopics and DescribeConfigs " +
			"too, so matching Kafka here would make CreateTopics the odd one out"},
	{regexp.MustCompile(`^deletetopics/delete\.results_line_up$`),
		"the facade answers in REQUEST order, name for name; Kafka's controller answers in its own order (see the " +
			"delete.result_names info line beside this). The facade is the stricter of the two and every per-name " +
			"error code matches, so nothing a client reads differs — it is only the order it would have to sort " +
			"itself if it indexed positionally, which no client does"},
}

func classify(scenarioName, key string) (string, string) {
	full := scenarioName + "/" + key
	for _, d := range deliberate {
		if d.pat.MatchString(full) {
			return "DELIBERATE", d.why
		}
	}
	for _, a := range accepted {
		if a.pat.MatchString(full) {
			return "ACCEPTED", a.why
		}
	}
	return "REVIEW", ""
}

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
