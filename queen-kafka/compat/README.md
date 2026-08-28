# queen-kafka compat

The client-side half of PLAN_QUEEN_KAFKA.md's testing section: real Kafka
clients against a real broker, with nothing faked in between. `go/` is the
franz-go suite; `rig.sh` is the stack it needs; `authgate/` is the one piece the
rig has to fake, and only under `--m5` (see below). `js/`, `librdkafka/` and
`java/` are M6's other client rows and run against the same stack — see
[The rest of the M6 client matrix](#the-rest-of-the-m6-client-matrix).

```sh
queen-kafka/compat/rig.sh                    # stand up, run everything, tear down
queen-kafka/compat/rig.sh -run TestLongPoll -v
queen-kafka/compat/rig.sh --keep             # leave the stack up to poke at
queen-kafka/compat/rig.sh --m5               # ...plus a TLS + SASL/PLAIN listener
```

`rig.sh` starts a throwaway `postgres:16` on **55432** with a tmpfs datadir
(never 5432 — that port is a live stack on a developer machine), builds and runs
the debug broker on **6699** with `QUEEN_APPLY_SCHEMA=true`, runs the debug
facade on **19092** advertising itself with `QUEEN_KAFKA_DEFAULT_PARTITIONS=8`,
runs the suite, and tears all of it down on every exit path. Arguments other
than `--keep` and `--m5` are passed to `go test`.

Against a stack that is already up, the suite is an ordinary `go test`:

```sh
cd queen-kafka/compat/go
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
| `TestFindCoordinatorIsTheFacade` | the group coordinator is the broker Metadata advertises; there is no transaction coordinator |
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

- **`kgo.DisableIdempotentWrite()` is mandatory.** franz-go is an idempotent
  producer by default, and `InitProducerId` is deliberately unimplemented (no
  transactions, no EOS). Without the option the first produce never happens.
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
queen-kafka/compat/rig.sh --keep -run TestNothing          # stack on 19092, no suite

(cd queen-kafka/compat/js && npm install && node run.mjs all)   # kafkajs
queen-kafka/compat/librdkafka/kcat.sh                           # kcat (brew install kcat)
python3 -m venv .venv && .venv/bin/pip install confluent-kafka
.venv/bin/python queen-kafka/compat/librdkafka/confluent_group.py

java -cp "<jars>/*" \
  -Dorg.slf4j.simpleLogger.log.org.apache.kafka.clients.NetworkClient=debug \
  queen-kafka/compat/java/QueenKafkaCompat.java 127.0.0.1:19092 run1
java -cp "<jars>/*" queen-kafka/compat/java/QueenKafkaEdges.java 127.0.0.1:19092 run1
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
| Java, the unsupported shapes | `java/QueenKafkaEdges.java` | the DEFAULT (idempotent) producer and `group.protocol=consumer`: both must fail fast and legibly, and both do |

Three client behaviours to know before reading a failure in these:

- **kcat's `-G` swallows every argument after it.** In group mode the remaining
  argv is the topic list, so `-e` or `-f` written after `-G group topic` is
  subscribed to instead of parsed and kcat then waits forever for a topic that
  does not exist. Put every option BEFORE `-G`.
- **kcat's `-o beginning` is a seek, not a reset.** Given it, kcat starts at
  offset 0 whatever the group has committed — against any broker. The resume
  test therefore asks for the reset policy (`-X topic.auto.offset.reset`) and
  leaves `-o` alone.
- **The Java producer must be told `enable.idempotence=false`**, exactly like
  franz-go's `DisableIdempotentWrite()`. Left at its 3.x default it dies on the
  first send with `UnsupportedVersionException: The node does not support
  INIT_PRODUCER_ID`. librdkafka and kafkajs need nothing: librdkafka defaults
  the flag off and disables the feature by itself when the broker lacks it.
