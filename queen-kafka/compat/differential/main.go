// Command differential runs the same wire-level scenarios against the
// queen-kafka facade and against a real single-node Apache Kafka, and diffs
// what a client can OBSERVE: error codes, offsets, watermarks, response shapes.
//
// It is the M6 oracle from PLAN_QUEEN_KAFKA.md. What it deliberately does not
// diff: timings, node ids, hosts and ports, throttle values, cluster ids and
// anything the broker generates freely (member ids, coordinator identity). A
// facade that answered those identically would be lying about being itself.
//
//	queen-kafka/compat/differential/rig-diff.sh run          # stack + runner
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
	{regexp.MustCompile(`^initproducerid/initproducerid\.transactional\.error_code$`),
		"both brokers refuse a transactional id and neither grants one (transactional.granted_an_id is false on " +
			"both, which is the part a client acts on). The CODES differ because the refusals are different " +
			"facts: Kafka 3.9.1 answers NOT_COORDINATOR because this broker is not the coordinator for that " +
			"transactional id, which is retriable and tells the client to go and find the right node. The facade " +
			"answers TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) — fatal and final — because there is no right " +
			"node: transactions are excluded by plan and a retriable code would send the client round a loop " +
			"that cannot end. Same code and same sentence handlers::produce gives a transactional id, so a user " +
			"meets one message about transactions and not two"},
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
		"documented deviation: Queen declares no width per queue (configure_queue_v1 creates the queue row and " +
			"nothing else; a log_partitions row materialises on the first push), so the facade cannot honour a " +
			"per-topic partition count. It accepts the number, does nothing with it, and reports the width the " +
			"topic will actually have — max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS). The property that matters " +
			"is kept and is asserted beside this one: create.new.metadata_agrees is true on both brokers, so the " +
			"number the create reports is the number the client's next Metadata hashes modulo"},
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
		"documented gap, recorded rather than papered over: Queen exposes NO HTTP read of a queue's configuration " +
			"(get_queue_v2 answers no config at all; get_queue_detail_v2's config object has leaseTime, retryLimit, " +
			"retryDelay, ttl, maxQueueSize and deadLetterQueue and NOT retentionEnabled/retentionSeconds). So " +
			"retention is writable and not readable here: CreateTopics sets it, and the create's own v5+ echo is " +
			"where a client reads it back — createtopics/create.retention.echo matches Kafka exactly. Reporting a " +
			"plausible default instead of omitting the key would be a guess"},
	{regexp.MustCompile(`^describeconfigs/topic\.\w+_read_only$`),
		"deliberate: every config this facade reports is read_only=true because AlterConfigs is NOT advertised, so " +
			"nothing here can be changed through it. Kafka reports its topic configs writable because it has " +
			"AlterConfigs. A UI that greys out its edit button on this flag is being told the truth by both"},
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
			"cost a bounds probe per produce, on the write path, for a field only the idempotent producer reads " +
			"— and that producer, implemented since M7 F3, does not need it: the facade never answers " +
			"UNKNOWN_PRODUCER_ID (59), which is the one code whose client-side recovery consults this field. It " +
			"answers OUT_OF_ORDER_SEQUENCE_NUMBER for a lost window instead, whose recovery is KIP-360's epoch " +
			"bump and reads nothing here (crate::idempotent). The fetch path reports the real log start on both " +
			"sides, which is where a client looks for it"},

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
