# queen-kafka compat

The client-side half of PLAN_QUEEN_KAFKA.md's testing section: real Kafka
clients against a real broker, with nothing faked in between. `go/` is the
franz-go suite; `rig.sh` is the stack it needs; `authgate/` is the one piece the
rig has to fake, and only under `--m5` (see below). `js/`, `librdkafka/` and
`java/` are M6's other client rows and run against the same stack — see
[The rest of the M6 client matrix](#the-rest-of-the-m6-client-matrix).

Four suites stand up their own stack instead, because the shape they measure is
not one broker and one facade:

| Suite | Shape | Ports and containers |
| --- | --- | --- |
| [`cloud/`](cloud) | **Queen Cloud**: a whole cell — proxy control-plane Postgres, cell Postgres, broker, proxy, facade — with the facade's `QUEEN_URL` pointing at the PROXY rather than at the broker, so every Kafka request crosses authentication, tenant scoping, quotas and metering. 16 scenarios over franz-go, run by `cloud/rig-cloud.sh` | 33040-33044, containers `qkc-t2-pxdb` and `qkc-t2-cellpg` |
| [`cluster/`](cluster) | **cluster mode**: one Postgres, two mesh-wired Queen brokers on it, three clustered facades, one facade with the cluster config absent, and two unclustered facades. 11 scenarios, run by `cluster/rig-cluster.sh` | 32400-32419, container `qkx-c2-pg` |
| [`transactions/`](transactions) | **M9 transactions**: one Postgres, one broker and THREE facades — the one under test, one with `QUEEN_KAFKA_NODE_ID` set so the cluster-mode refusal can be measured, and one with the transaction caps at their floor. Nine scenarios over Java kafka-clients 4.3.1 and franz-go, run by `transactions/run.sh` | 32910-32914, container `qkt-acc-pg` |
| [`embedded/`](embedded) | **embedded mode** (`QUEEN_KAFKA_EMBEDDED=true` on the BROKER, which then supervises the facade as a child process), run by `embedded/rig-embedded.sh` | see its own README |

`embedded/` is on disk with its own README and rig and is **not claimed by any
report of the 2026-08-29 campaign**, so nothing in this file vouches for its
results. Read it as unratified until somebody re-runs it and says so here.

```sh
protocols/queen-kafka/compat/rig.sh                    # stand up, run everything, tear down
protocols/queen-kafka/compat/rig.sh -run TestLongPoll -v
protocols/queen-kafka/compat/rig.sh --keep             # leave the stack up to poke at
protocols/queen-kafka/compat/rig.sh --m5               # ...plus a TLS + SASL/PLAIN listener
```

`rig.sh` starts a throwaway `postgres:16` on **55432** with a tmpfs datadir
(never 5432 — that port is a live stack on a developer machine), builds and runs
the debug broker on **6699** with `QUEEN_APPLY_SCHEMA=true`, runs the debug
facade on **19092** advertising itself with `QUEEN_KAFKA_DEFAULT_PARTITIONS=8`,
runs the suite, and tears all of it down on every exit path. Arguments other
than `--keep` and `--m5` are passed to `go test`.

Against a stack that is already up, the suite is an ordinary `go test`:

```sh
cd protocols/queen-kafka/compat/go
GOWORK=off \
QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:19092 \
QUEEN_URL=http://127.0.0.1:6699 \
QUEEN_KAFKA_PARTITIONS=8 \
  go test -v .
```

`GOWORK=off` is required inside this repository: the root `go.work` lists the
two client modules and not this one, and a Go workspace refuses to build a
module outside itself.

## What the suite covers

| Test | What it pins |
| --- | --- |
| `TestMetadataAdvertisesOneBrokerAndTheAutoCreatedTopic` | one broker at the advertised address, leader/replicas/ISR of every partition, auto-create at the configured width |
| `TestProduceUncompressedReturnsContiguousOffsets` | acks=all, 100 keyed records across every partition, offsets contiguous from 0 per partition |
| `TestProduceEveryCompressionCodec` | gzip, snappy, lz4, zstd — each verified to have actually compressed (bytes written vs bytes of records) and to decode byte-exact |
| `TestConsumeRoundTripsEveryFieldByteExact` | key, value, headers (order included), timestamp; null vs empty for keys, values and header values; non-UTF-8 bytes; a 64 KiB value |
| `TestLongPollWakesTheParkedConsumer` | a consumer parked at the high watermark is woken by the write, not by its max-wait expiring; prints the wake latency |
| `TestListOffsetsBounds` | earliest/latest on a written partition, on an unwritten partition of a written topic, and on a never-written topic |
| `TestFetchBeyondTheEndIsOffsetOutOfRange` | the Kafka error reaches a client configured not to reset |
| `TestNativePayloadIsReadableThroughTheFacade` | a payload pushed over `POST /api/v1/push` reads back through Kafka as its own JSON, no key (the envelope fallback) |
| `TestKafkaProducedRecordIsAnEnvelopeOverTheNativeAPI` | a Kafka-produced record is stored as the `k`/`v`/`h`/`t` envelope and reads back over `POST /api/v1/fetch` |
| `TestProduceAcksZeroDoesNotHangAndLands` | fire-and-forget writes nothing back on the wire, does not stall the connection, and the records are there |
| `TestConcurrentProducersAndConsumer` | two producers and a consumer for ten seconds: no panic, no protocol error, no duplicate, counts and per-partition offsets add up |
| `TestConsumerGroupTwoMembersSplitThePartitions` | two members of one group: every record delivered exactly once, no partition read by both, both members assigned something |
| `TestConsumerGroupResumesFromCommittedOffsets` | a committed offset overrides `auto.offset.reset`: a second consumer told to start at the beginning resumes where the group stopped |
| `TestConsumerGroupRebalancesWhenAMemberLeaves` | LeaveGroup triggers a rebalance and the survivor reaches every record, with no gap and no duplicate across the two generations |
| `TestFindCoordinatorIsTheFacade` | the group coordinator is the broker Metadata advertises, and since M9 so is the TRANSACTION coordinator (`key_type` 1) in single-node mode |
| `TestSimpleConsumerCommitsAndReadsBackEveryTopic` | a client with no membership commits at generation -1, and reads its offsets back both by name and through the all-topics (null) form |
| `TestGroupSingleMemberConsumesCommitsAndExits` | one member reads 200 keyed records over every partition, commits, and Close() returns: the committed offsets are read back through OffsetFetch, not believed from the client |
| `TestGroupTwoMembersSplitThePartitionsEagerly` | the same split under range/round-robin rather than cooperative-sticky, asserted on the assignment the members were GIVEN (4 and 4 over 8 partitions) as well as on what arrived |
| `TestGroupRebalancesWhenAMemberDiesWithoutLeaving` | a member whose sockets stop with no LeaveGroup is evicted by its session timeout, the survivor takes the whole topic, and the duplicates around the rebalance are counted and reported rather than forbidden |
| `TestGroupOffsetsSurviveAFacadeRestart` | the facade is SIGKILLed and restarted under a stopped group; a fresh member told to start at the beginning gets only the records produced while it was down |
| `TestGroupSurvivesARejoinStorm` | a member joining and leaving three times running leaves no wedged generation, and every heartbeat error franz-go reported was REBALANCE_IN_PROGRESS-class |
| `TestFreshGroupOnAnExistingTopicStartsAtEarliest` | a group that has committed nothing gets -1 (not 0) per partition, and its consumer then applies its reset policy |
| `TestTLSAndSaslPlainOnboarding` (`--m5`) | franz-go's own TLS dialler and its own SASL/PLAIN, producing and consuming: the onboarding claim, end to end |
| `TestSaslListenerRefusesAnUnauthenticatedClient` (`--m5`) | a client with no credentials reads nothing from a SASL listener |
| `TestSaslRefusesAWrongPassword` (`--m5`) | a wrong password gets SASL_AUTHENTICATION_FAILED — the fatal code, not a retriable disconnect — and the listener still serves the right one |
| `TestPlaintextClientAgainstTheTLSListener` (`--m5`) | a plaintext frame on the TLS port gets a TLS alert and a close, the facade survives it, and a TLS client works immediately after |

### M7, added 2026-08-29 and 2026-08-30

Six files, 65 further top-level tests, on the same stack and in the same run,
which is what takes a full `rig.sh --m5` from 26 top-level tests to **91**. They
are grouped here rather than listed one by one; each file's header says what
every test pins. The last three are F4's.

| File | Tests | What it pins |
| --- | --- | --- |
| `go/admin_topics_test.go` | 21 | CreateTopics, DeleteTopics and DescribeConfigs at every advertised version: a created queue a second client can see, the refusals (an existing topic, `cleanup.policy=compact`, an unknown config, a reserved name, a manual replica assignment, a name repeated inside one request), `validate_only` writing nothing, `retention.ms` applied and echoed, the broker resource, and answers lining up index-for-index with the request |
| `go/admin_groups_test.go` | 11 | ListGroups showing a STOPPED group beside a live one and honouring the state filter, DescribeGroups with host and assignment and telling a stopped group from an unknown one, DeleteGroups refusing a group with members and removing a stopped group's offsets, the advertised windows, and a listing that survives a facade restart |
| `go/idempotent_test.go` | 11 | InitProducerId at every advertised version, an epoch bump, a transactional id refused immediately and an EMPTY one treated as absent, a default franz-go producer round-tripping, a duplicate batch answered with the ORIGINAL offsets, a sequence gap refused with nothing written, an unknown producer, a stale epoch fenced, and a default idempotent producer surviving a facade SIGKILL |
| `go/admin_configs_test.go` | 7 | the whole `retention.ms` round trip over the wire (describe `-1`/`DEFAULT`/writable, IncrementalAlterConfigs SET, describe `604800000`/`DYNAMIC_TOPIC_CONFIG`/writable, DELETE, describe back to `-1`), plus the deprecated full-replacement key 33, `validate_only` writing nothing, an untracked topic refused by name, and the broker resource |
| `go/admin_offsets_test.go` | 9 | OffsetDelete on an empty group and on a live one (subscribed topics refused 86, unsubscribed topics deleted), an offset never committed answered 0, an unknown group, the group still listed afterwards; and CreatePartitions refusing a decrease, an equal count and an increase |
| `go/admin_acls_test.go` | 6 | DescribeAcls, CreateAcls and DeleteAcls at v1, v2 and v3: the code, both message literals, an empty `resources`, one result per creation and per filter, and an empty request list answering an empty result list with no error |

The differential (`differential/rig-diff.sh run`, which diffs every answer
against `apache/kafka:3.9.1` in KRaft) grew with it, and again with M9's
`transactions` scenario: **17 scenarios**, 100 divergences from a cold stack (74
`deliberate`, 26 `accepted`) or 97 from a warm one (72, 25), and **0 left to
classify** either way. The three-row difference is the oracle's own transaction
coordinator warming up, which its accepted rows already name.
F4 added three: `acls` (`admin_acls.go`), `createpartitions` and `offsetdelete`
(`admin_partitions.go`, `admin_offsets.go`). `acls` and `offsetdelete` are held
to a **zero-divergence** bar rather than a classified one, because both APIs'
rules are Kafka's own and the facade has the material for all of them; any
divergence there is a semantics bug, not a deviation. `acls` earned that bar the
hard way, catching a wrong message literal that seven observation keys had been
reporting.

There is no `admin_configs.go`: the config write half is covered over the wire
by `go/admin_configs_test.go` and by `kafka-configs.sh` diffed against the
oracle, and a differential scenario for it is the one piece of F4's design that
was not built. It is worth adding, and it is the place a future retention or
`read_only` drift would be caught automatically.

## The Cloud acceptance

`cloud/` is the only suite in this directory that puts the **proxy** on the
critical path of every Kafka request. `rig-cloud.sh` stands a whole cell up
(control-plane Postgres, cell Postgres, broker, proxy, facade), issues the
credentials it needs through the proxy's own control plane, and runs the
franz-go suite in `cloud/` against it. `cloud/run.sh` is the same suite pointed
at a cell that is already up, so it can be aimed at a real staging cell without
editing a line of Go; every address and credential comes from the environment.

**16 of 16 green from a clean machine, 2026-08-30** (`go test` 127 s, on top of
the cell boot). What each group proves:

| Group | Scenarios | What it pins |
| --- | --- | --- |
| Tenant isolation | 4 | two tenants on one shared-host listener do not see each other's topics or records, do not share a consumer group, and do not share committed offsets |
| Scopes | 3 | a `consume` key reads but cannot create topics, a `produce` key cannot consume, and a key without `read` cannot even authenticate (Metadata **is** the queue listing, so the connection never opens) |
| The KV gates | 3 | offsets commit for a tenant whose plan carries no `kv` feature, a plain KV batch is still gated, and the transaction route is still `Produce`-classified |
| The proxy's edges | 3 | a 30 s long-poll Fetch is not cut by the proxy's 35 s upstream timeout, a rate-capped tenant is throttled rather than failed, and metering rows land in the control-plane database for Kafka traffic |
| Identity, mirror, freeze | 3 | two credentials of one tenant resolve to ONE group through `/auth/me`, a Kafka group shows up in Queen's own consumer-group views with `kind=kafka`, and a blocked tenant can still commit and read offsets while its produce is refused |

Two things the suite **measures and prints without asserting**, because both are
decisions rather than defects and an operator should meet them here rather than
in an invoice or a dashboard:

- **A Kafka Fetch books a request and zero messages.** A tenant consuming a
  million records through the Kafka wire is billed for the requests that carried
  them, not for the records.
- **A long-polling Fetch takes no parked slot at the proxy**, so the parked-pop
  gauge an operator watches for consumer pressure reads zero however many Kafka
  consumers are waiting. It cannot see them at all.

**Routing is by credential, not by SNI, and that is deliberate.**
`src/lib.rs` keeps `advertised_host` per PROCESS rather than per SNI lane, so one
facade hands every client the same bootstrap address whatever name it dialled: a
second tenant's connections would come back carrying the first tenant's SNI. Per-
cluster SNI therefore needs one facade process per cluster, and one facade
fronting many tenants needs the credential to be the authority. The rig runs the
proxy's shared-host arm (`QUEEN_PROXY_SHARED_HOSTS`) for that reason. The
consequence is the point: **the tenant of a Kafka connection is the tenant of the
SASL password, and nothing else.**

## The M5 surface: TLS, SASL and the throttle

By default the rig runs a **plaintext listener with no SASL**, which is the
facade's own default and the whole of an OSS deployment: `QUEEN_TOKEN` is the
one credential, and every connection reaches Queen with it. The Cloud fit is
four environment variables on top, all off unless set, and all validated at boot
rather than on the first connection:

| Variable | What it does |
| --- | --- |
| `QUEEN_KAFKA_TLS_CERT` / `QUEEN_KAFKA_TLS_KEY` | PEM paths. Both or neither — half of the pair is a boot failure, never a listener that quietly stays in cleartext. Switches the port to TLS and makes each connection's SNI available. |
| `QUEEN_KAFKA_SASL=plain` | Requires every connection to authenticate before it may send anything but ApiVersions and the SASL handshake. `sasl.password` **is the Queen bearer token**; `sasl.username` is a label that identifies the connection in the log. Validated on connect with one authenticated call to Queen. |
| `QUEEN_KAFKA_FORWARD_SNI_HOST=true` | Sends the TLS server name a connection dialled as the HTTP `Host` header of that connection's calls to Queen — which is what the proxy routes on. Needs TLS (a plaintext connection carries no SNI), and says so at boot. |

Two consequences worth stating before an operator meets them:

- **PLAIN on a plaintext listener puts a token on the wire in the clear.** It is
  legal — a loopback rig, a mesh that encrypts underneath — and it logs a warning
  at boot every time.
- **A wrong password is fatal and a broker outage is not.** Queen answering
  401/403 gets SASL_AUTHENTICATION_FAILED and the connection closes, which every
  client treats as final. Queen answering nothing usable (unreachable, 5xx, a
  429) gets a bare disconnect instead, which every client retries — see
  `src/handlers/sasl_authenticate.rs`.

`--m5` stands up a SECOND facade beside the first, on 19093, with the
self-signed certificate from `src/tls.rs` written into the rig's temp directory
and `QUEEN_KAFKA_SASL=plain` + `QUEEN_KAFKA_FORWARD_SNI_HOST=true`. It advertises
itself as `localhost:19093` and not as an address, because no TLS client sends an
SNI for an IP literal — the rig then asserts that the name reached the facade by
reading it back out of the log. The default suite is unchanged: the four M5
tests skip when the listener is not there.

The credential CHECK needs one piece the rig cannot get from the broker: the
rig's broker runs with `JWT_ENABLED` unset, so `GET /api/v1/resources/queues`
answers 200 to any bearer and every password would be accepted. So `--m5` also
starts **`authgate/`** on 6698 — a stdlib reverse proxy whose only behaviour is
one exact-match check on `Authorization: Bearer` and a 401 when it fails — and
points the TLS facade at that instead of at the broker. What that proves is the
facade's half: the password is forwarded as the bearer, a 401 becomes
SASL_AUTHENTICATION_FAILED, and the client treats it as fatal. It is not
evidence about any real auth layer's verdict; that is unit-tested
(`handlers::sasl_authenticate`, `conn`) and, for the broker, in `server/`.

Authentication also asks Queen **who this credential is**, once per credential,
with `GET /auth/me` (`src/identity.rs`): the answer is what a consumer group and
a queue-list cache are filed under, so that one tenant's two credentials — a key
rotation, a per-service key — are ONE group rather than two sharing one set of
committed offsets. In the rig the answer is `local` for every credential,
because a broker with `JWT_ENABLED` unset serves the standalone identity to any
caller and genuinely has one tenant; the facade logs it at debug
(`tenant was resolved from /auth/me`). Under `--m5` that call goes through the
authgate like any other, so the wrong-password test refuses it too and the
credential is filed under itself — which is the fallback every deployment whose
`/auth/me` does not identify bearers gets.

Queen's 429 (a frozen or rate-capped tenant) maps to Kafka's `throttle_time_ms`
on Produce, Fetch and Metadata, carrying the proxy's own `Retry-After`. Clients
back off on it natively, which is the point; the whole mapping and the reason
for each code beside it is `src/throttle.rs`.

## Two client facts worth knowing before reading a failure

- **`kgo.DisableIdempotentWrite()` is no longer needed** (2026-08-29, M7 F3).
  franz-go is an idempotent producer by default and `InitProducerId` is now
  advertised, so a stock `kgo.NewClient` produces. Tests written before F3 that
  still pass the option are unaffected, since disabling it is legal, and the
  ones in `go/` keep it on purpose: they assert the offsets a plain produce
  returns and drive hand-built batches with fixed sequences, and an idempotent
  client would put its own producer id and sequence on every one of them.
- **Transactions landed in M9** (2026-08-30) and are NOT exercised from `go/`.
  `kgo.TransactionalID` and `GroupTransactSession` work; the whole
  client-visible transaction path — Java and franz-go, fencing, the caps, a
  crash mid-transaction, and an exactly-once loop with an induced SIGKILL —
  lives in [`transactions/`](transactions), which stands up its own stack on
  32910-32914 and is run with `transactions/run.sh`.
- **acks=0 offsets are invented by the client.** There is no response to read
  them from, so franz-go fills them from its own per-partition counter. Nothing
  about them is an assertion on the facade.
- **The facade is restartable from inside the suite.** `rig.sh` writes a
  `start-facade.sh` into its temp directory and passes it as
  `QUEEN_KAFKA_RESTART_CMD`; it SIGKILLs the running facade, starts a fresh one
  on the same port, and prints `old=<pid> new=<pid>` so a test can prove the
  restart happened rather than assume it. Without the variable the restart test
  skips.
- **The group tests are slow on purpose.** A group forms only after the facade's
  join window (`QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`, 3 seconds by default, Kafka's
  `group.initial.rebalance.delay.ms`), and the rig runs the default rather than a
  shortened one — collapsing a fleet's simultaneous joins into one rebalance is
  part of what is being tested. Expect three seconds per group formation.

## The rest of the M6 client matrix

franz-go is the suite the rig runs by itself. The other three rows are scripts
you point at a stack that is already up — `rig.sh --keep` (and, so the franz-go
suite does not run first, `-run TestNothing`) leaves exactly that stack behind:

```sh
protocols/queen-kafka/compat/rig.sh --keep -run TestNothing          # stack on 19092, no suite

(cd protocols/queen-kafka/compat/js && npm install && node run.mjs all)   # kafkajs
protocols/queen-kafka/compat/librdkafka/kcat.sh                           # kcat (brew install kcat)
python3 -m venv .venv && .venv/bin/pip install confluent-kafka
.venv/bin/python protocols/queen-kafka/compat/librdkafka/confluent_group.py

java -cp "<jars>/*" \
  -Dorg.slf4j.simpleLogger.log.org.apache.kafka.clients.NetworkClient=debug \
  protocols/queen-kafka/compat/java/QueenKafkaCompat.java 127.0.0.1:19092 run1
java -cp "<jars>/*" protocols/queen-kafka/compat/java/QueenKafkaEdges.java 127.0.0.1:19092 run1
```

`<jars>` is a directory holding `kafka-clients`, `slf4j-api` and `slf4j-simple`
from Maven Central; nothing is built or published. Every script takes
`[bootstrap] [runId]`, prints one `ok`/`FAIL` line per assertion and ends with
`RESULT:`, and every one of them prints the API versions the client NEGOTIATED,
read out of that client's own debug stream rather than assumed.

| Row | Script | What it covers |
| --- | --- | --- |
| kafkajs 2.2.4 | `js/run.mjs [scenario]` | acks=all offsets, acks=0, gzip, byte-exact key/value/header round-trip, a group end to end, two members splitting 8 partitions and the survivor taking all of them, explicit commit + resume across a restart, and the auto-create dance on a never-seen topic |
| librdkafka 2.15 (kcat 1.7) | `librdkafka/kcat.sh` | `-L` metadata and auto-create, `-P`/`-C` round-trip with keys and headers, gzip/snappy/lz4/zstd, an explicit start offset, `-Q`, and `-G` group consume + commit + resume |
| librdkafka 2.15 (confluent-kafka 2.15) | `librdkafka/confluent_group.py` | delivery-report offsets, a group that commits synchronously, the commit read back over OffsetFetch, watermarks, a second consumer resuming, a fresh group applying `auto.offset.reset`, and two cooperative-sticky members |
| Java kafka-clients 3.9 | `java/QueenKafkaCompat.java` | acks=all and gzip producers, a group consumer over every partition, headers in order with empty and null kept apart, `commitSync` + `committed`, `beginningOffsets`/`endOffsets`, and commit-and-resume |
| Java, the unsupported shapes | `java/QueenKafkaEdges.java` | the DEFAULT (idempotent) producer and `group.protocol=consumer`. It narrates rather than asserts, and **half of it is out of date since M7 F3**: the default producer now sends, and its success line still calls that a fallback |
| **2026-08-29** Java kafka-clients 4.3.1 + 3.6.2 | `java-matrix/run.sh` | the KIP-896 question settled: 4.x negotiates every advertised API to the top of its window and no raised client floor lands above a facade cap; both versions plaintext and over SASL_SSL; `initTransactions()` hanging for `max.block.ms`, which M7 F3 did NOT fix and which is the transaction coordinator's retriable answer rather than InitProducerId. The 5 stale FAILs are inverted and the suite is green: 4.3.1 matrix 81 checks, 3.6.2 matrix 80, `edges` rc=0 on both. Its SASL lane runs on 4.3.1 only, because kafka-clients below 3.9 cannot do SASL on JDK 24 (JEP 486) |
| **2026-08-29** Spring Boot 3.5.16 + spring-kafka 3.3.16 | `spring-kafka/run.sh` | the stock Boot bean set, `@KafkaListener` at `concurrency=4` splitting 8 partitions, cooperative-sticky as its own protocol, commit and resume across listener ids, and how much of `KafkaAdmin` works (`describeTopics` and `clusterId` always did; since M7 F1 `NewTopic` beans do too, unless one asks for `cleanup.policy=compact`) |
| **2026-08-29** sarama 1.60.2 (+ 1.45.2) | `sarama/run.sh` | sarama's one-knob `Config.Version` against the advertised windows, a sweep from 0.10.2.0 to 4.3.1, and the version cliff below sarama v1.46.0 where the producer works while the consumer loops on EOF |
| **2026-08-29** segmentio/kafka-go 0.4.51 | `kafka-go/run.sh` | its two protocol stacks at once: the negotiating `Transport` and the `Conn` path that writes OffsetCommit v2 and OffsetFetch v1 with no negotiation, on the facade's exact floors; plus auto-create decided by the Metadata wire flag rather than by naming the topic |
| **2026-08-29** kafka-python 2.0.2 / 2.3.2 / 3.0.11 (+ ng) | `kafka-python/run.sh` | the default `api_version` probe proven safe on four releases, the hint above `(0,11,0)` proven to be the footgun, and SASL/PLAIN with the wire bytes: the authzid refusal this suite found was fixed on 2026-08-29 and the lane now passes stock |
| **2026-08-29** aiokafka 0.14.0 (+ 0.12.0) | `aiokafka/run.sh` | the asyncio client end to end, and the hard floor at 0.13.0: below it `JoinGroupRequest_v5` is closed and the client hangs for ever with produce still working |
| **2026-08-29** Confluent.Kafka 2.15.0 (.NET) | `confluent-dotnet/run.sh` | the easiest row in the matrix (idempotence is already off), negotiated exactly to the `versions.rs` ceilings, plus `ListConsumerGroupsAsync`, which used to abort the PROCESS with a glibc double free before writing a byte and which M7 F2 fixed by advertising key 16 at all |
| **2026-08-29** node-rdkafka 3.6.1 + @confluentinc/kafka-javascript 1.10.0 | `node-librdkafka/run.sh` | both Node librdkafka bindings, and a 2x2 cross-binding header probe showing that node-rdkafka loses binary header bytes on its WRITE path while the facade returns whatever it was given |
| **2026-08-29** @platformatic/kafka 2.11.0 | `platformatic-kafka/run.sh` | the new pure-TypeScript client, which walks DOWN from each advertised maximum and therefore sends exactly the facade's ceiling on every API; the same suite passes against apache/kafka 3.9.1, where it sends Fetch v17 |
| **2026-08-29** rdkafka 0.39.0 (Rust) | `rust-rdkafka/run.sh` | delivery reports carrying the assigned partition and offset (a direct check on `PushResult::offset`), a full commit-and-resume across two consumers, and the librdkafka consumer default `allow.auto.create.topics=false` |
| **2026-08-29** rdkafka 0.29.0 (Ruby) + php-rdkafka 6.0.5 | `rdkafka-ruby-php/run.sh` | two containerised bindings on either side of a librdkafka boundary: 2.14.2 compresses gzip, snappy and lz4, 2.6.1 silently sends all four codecs uncompressed against the advertised Produce floor of v3 |
| **2026-08-29** brod 4.6.3 (Erlang/Elixir) | `brod/run.sh` | was the only FAIL in the matrix, settled against an `apache/kafka:3.9.1` oracle in the same container: kafka_protocol encodes a null `transactional_id` as `""` and the facade refused any present one. Fixed the same day in `src/handlers/produce.rs`, and stock brod now passes 58 of 58. Also the only client that exercises the BOTTOM of the advertised windows (Metadata v2, ListOffsets v2, FindCoordinator v0, OffsetFetch v2) |

The full matrix, including the mandatory config and the caveat for each client,
the evidence behind every non-PASS row, and the usage-ranked landscape of what
was deliberately not tested and why, is [CLIENT_MATRIX.md](CLIENT_MATRIX.md).

Three client behaviours to know before reading a failure in these:

- **kcat's `-G` swallows every argument after it.** In group mode the remaining
  argv is the topic list, so `-e` or `-f` written after `-G group topic` is
  subscribed to instead of parsed and kcat then waits forever for a topic that
  does not exist. Put every option BEFORE `-G`.
- **kcat's `-o beginning` is a seek, not a reset.** Given it, kcat starts at
  offset 0 whatever the group has committed — against any broker. The resume
  test therefore asks for the reset policy (`-X topic.auto.offset.reset`) and
  leaves `-o` alone.
- **The Java producer no longer needs `enable.idempotence=false`** (2026-08-29,
  M7 F3). Left at its 3.x default it used to die on the first send with
  `UnsupportedVersionException: The node does not support INIT_PRODUCER_ID`; it
  now negotiates InitProducerId to v4 and sends. librdkafka and kafkajs never
  needed anything: librdkafka defaults the flag off and disables the feature by
  itself when the broker lacks it.
- **The stale assertions are inverted (2026-08-29).** `java-matrix` (5 FAILs)
  and `sarama`'s `edges` scenario (3 FAILs, 4 in a full run) used to assert that
  CreateTopics, DeleteTopics, DescribeConfigs, ListGroups, DescribeGroups and
  `ClusterAdmin.ListTopics` are REFUSED. All six work since M7 F1 and F2, so the
  assertions were what was wrong. All 34 of them across seven suites were
  inverted into positive checks of the 21-key surface rather than deleted, and
  every one of those suites was re-run to green. Two things the inversion
  taught, both worth keeping in mind before writing the next one:
  **CreateTopics does not honour `num_partitions`** (`create_topics.rs` takes
  the wider of the live lane count and `QUEEN_KAFKA_DEFAULT_PARTITIONS`), so the
  obvious inversion "created with the 4 partitions asked for" is itself wrong
  and the suites now assert the true width and name the deviation on the line;
  and the ruby/php **zstd detector was blind rather than merely fail-open**,
  because librdkafka's "Broker does not support compression type" notice rides
  the `msg` debug facility while both suites set `debug=protocol`. It is now
  `debug=protocol,msg` and fails closed.
- **`java/QueenKafkaEdges.java` is still out of date, and not by an assertion.**
  It narrates rather than asserts, so it does not FAIL, but its success line
  still reads "the client fell back to a non-idempotent producer", which is no
  longer what happens. `aiokafka` and `node-librdkafka` likewise still carry
  stale text (a "14 keys" reference and a fail-open idempotence section). None
  of the three was touched by the sweep.
