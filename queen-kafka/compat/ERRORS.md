# Error codes queen-kafka emits

The M6 audit (PLAN_QUEEN_KAFKA.md: "Error-code discipline audit"), written down.
One table per API: every non-zero code the facade can put on the wire, where it
comes from, whether a client retries it, and why that is the right code and not
a truer-sounding one.

## Why the code matters more than it looks

A Kafka error code is not a message. It is an instruction to a state machine
the facade does not own, and the two ways to get it wrong are not symmetric:

* **A code the client does not expect on that API is fatal.** The Java
  consumer's fetch path (`FetchCollector.initializeCompletedFetch`) walks a
  CLOSED set of per-partition codes and throws `IllegalStateException` on
  anything else — out of `poll()`, killing the consumer. Its commit and
  offset-fetch paths do the same with a bare `KafkaException`. So a code that
  reads as more precise, but is not on that API's list, ends the application
  where a vaguer one would have made it recover.
* **A retriable code where the truth is permanent is an infinite loop**, and a
  permanent code where the truth is transient is a delivery failure raised to
  an application that should have waited.

So each table below has a *retriable* column, and it means what Kafka means:
`Errors.<CODE>.exception() instanceof RetriableException`, plus the
`InvalidMetadataException` subset that also makes a client refresh metadata
first.

**The rule this audit produced:** INVALID_TOPIC_EXCEPTION is a METADATA answer.
Apache Kafka raises it where a topic NAME is validated — the metadata path and
CreateTopics — and nowhere else; a broker asked to fetch, list the offsets of,
or commit against a name it does not have simply does not have it. So
`handlers::metadata::reserved_or_invalid` keeps the precise code for Metadata,
and every other API applies the same name rule through
`handlers::metadata::not_a_topic_here`, which answers UNKNOWN_TOPIC_OR_PARTITION.

## ApiVersions (v0–v3)

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNSUPPORTED_VERSION` (35) | — | the requested version is outside the advertised window | The one API that must ANSWER an unsupported version rather than close, and it answers with a **v0-encoded body** so a client that guessed high can still read it. The body carries exactly one entry — ApiVersions' own window — which is what `NetworkClient.handleApiVersionsResponse` reads to choose the version to retry at; an empty array only makes a client fall back to 0. Apache Kafka sends the same twelve bytes. Every other API closes the connection instead, which is what Apache Kafka does. |

## Metadata (v0–v9) — per topic

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | the queue does not exist and the client asked us not to create it; or the name begins with `__` | `__` is answered UNKNOWN and not INVALID on purpose: to Kafka those are valid names that happen to be a broker's own bookkeeping topics, and INVALID would surface as a crash in tooling that lists them. |
| `INVALID_TOPIC_EXCEPTION` (17) | no | the name is not a legal Kafka topic name, or a null name in the request | One of **the only two APIs that emit this** — the other is CreateTopics (M7), the other surface where a client names a topic and can act on the answer. Everywhere else the same rule is narrowed to UNKNOWN by `not_a_topic_here`. |
| `LEADER_NOT_AVAILABLE` (5) | yes (+metadata) | the queue list could not be read; auto-create failed or was still in flight | Also the code beside `throttle_time_ms` when the tenant is capped — retriable, so the client backs off and comes back. |

## Produce (v3–v9) — per partition

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_REQUIRED_ACKS` (21) | no | `acks` is not 0, 1 or -1 | |
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | a `transactional_id` on the request, or the transactional flag on a batch | Fatal and final for the producer, and unmistakably about transactions. Kafka Streams stops here with a sentence instead of appearing to work. |
| `INVALID_RECORD` (87) | no | a CONTROL batch | A control batch is written by a transaction coordinator; this facade is nobody's. |
| `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43) | no | the records are a pre-v2 message set | Until M7 F3 this was also the answer to any batch carrying a producer id. It is not any more: the idempotent producer is implemented and the four codes below are what a producer id can now be answered with. |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) | no (KIP-360) | an idempotent batch would leave a GAP, or this facade holds no sequence window for that producer | Refusing the gap is what makes "idempotent" a claim about ORDER and not only about duplicates: **nothing is written**. An absent window is the same code because the recovery is the same — the producer bumps its epoch through InitProducerId v3 and resets. Apache Kafka 3.9.1 *accepts* the absent-window case (measured); the facade does not, because it has no durable producer state and an absent window is common rather than rare here. |
| `DUPLICATE_SEQUENCE_NUMBER` (46) | no | an idempotent batch is at or below the last appended sequence but is not a batch this facade appended | The re-batched retry. An EXACT resend is not this — it is answered `error_code = 0` with the offsets the original got, and nothing is written, which is Kafka's own duplicate semantics and the whole point of the window. |
| `INVALID_PRODUCER_EPOCH` (47) | no | an idempotent batch carries an epoch below the highest this facade has seen for that producer | A producer retrying a batch it had queued at an epoch it has since left. Not the transactional fencing, which this facade refuses outright. |
| `INVALID_RECORD` (87) | no | one partition entry mixes idempotent and non-idempotent batches, or batches from two producer sessions | No client does this. It is refused rather than half-answered because the response carries ONE error code and ONE base offset per partition, and a run that is half remembered and half new is describable by neither. |
| `CORRUPT_MESSAGE` (2) | **yes** | the batch headers or the records did not decode | Retriable in Kafka (a CRC failure can be the wire), and this is the code a real broker gives for the same thing. A producer therefore retries the same undecodable batch until `delivery.timeout.ms` and then fails — matching Apache Kafka, deliberately. |
| `MESSAGE_TOO_LARGE` (10) | no | the request decompresses past the frame ceiling, declares more records than one request may decode, or Queen answered 413 | The producer's own answer is to split the batch or raise `max.request.size`. |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | a `__` or unnameable topic; a partition outside the advertised width; Queen answered 404 | Past-the-width is UNKNOWN and not an invalid-request code because it is usually a stale metadata view, and UNKNOWN is what makes the client refresh. |
| `INVALID_TOPIC_EXCEPTION` (17) | no | the name is not a legal Kafka topic name | Kept here, unlike on the read paths: the Java **producer** fails the batch with the named exception rather than throwing on an unexpected code, and the producer's topic name really is illegal. |
| `LEADER_NOT_AVAILABLE` (5) | yes (+metadata) | the queue list was unreadable, auto-create did not resolve, or Queen answered 502–504 | |
| `REQUEST_TIMED_OUT` (7) | yes | no answer from Queen at all (connect, DNS, TLS, reset, our own budget); Queen answered 408; **or 429** | The one produce code whose meaning is "we do not know whether it landed". For 429 it is the code beside `throttle_time_ms`: THROTTLING_QUOTA_EXCEEDED is not on librdkafka's produce-retry list, which made a rate cap a permanent delivery failure on every Confluent client. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | Queen answered something else (a 400 is our bug), an unreadable 2xx, or a push whose offsets do not line up | Loud in the log by construction. |

`acks=0` writes no response frame at all, so none of these reaches that
producer; they are logged instead (`log_silent_failures`). The sequence window
is still updated on that path — a duplicate or an out-of-order simply cannot be
reported. Java and librdkafka both refuse `acks != all` under idempotence, so
the only way to reach it is a hand-rolled client.

## Fetch (v4–v6) — per partition

The closed set the Java consumer accepts here is
`{NONE, OFFSET_OUT_OF_RANGE, UNKNOWN_TOPIC_OR_PARTITION, UNKNOWN_TOPIC_ID,
INCONSISTENT_TOPIC_ID, TOPIC_AUTHORIZATION_FAILED, NOT_LEADER_OR_FOLLOWER,
REPLICA_NOT_AVAILABLE, KAFKA_STORAGE_ERROR, FENCED_LEADER_EPOCH,
UNKNOWN_LEADER_EPOCH, OFFSET_NOT_AVAILABLE, CORRUPT_MESSAGE,
UNKNOWN_SERVER_ERROR}`; anything else is an `IllegalStateException` out of
`poll()`. `handlers::fetch::tests::every_code_this_handler_emits_is_one_a_consumer_accepts`
pins every emission point against it.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `OFFSET_OUT_OF_RANGE` (1) | no (by design) | the offset is below the log start or above the high watermark; or the client sent a NEGATIVE fetch offset | Not retriable on purpose: it is what makes a consumer run `auto.offset.reset`, which is the only way out. A negative offset gets this rather than an invalid-request code for the same reason — a corrupted position must reset, not loop. |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | a `__` or unnameable topic; a partition index outside `0..width`, the lanes Metadata advertises; the log answered UNKNOWN; Queen answered 404 | **Changed in M6**: an unnameable name used to answer INVALID_TOPIC_EXCEPTION, which is outside the set above and would have killed a Java consumer. The WIDTH check is newer still: a lane past the advertised width used to be served as an error-free empty read at high watermark 0, so a consumer holding a stale assignment polled a partition that would never fill instead of refreshing its metadata. Produce has always refused the same lane. A queue list that cannot be read costs the check, not the fetch. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `NOT_LEADER_OR_FOLLOWER` (6) | yes (+metadata) | no answer from Queen; 429; 502–504 | The retriable answer here, where Produce uses LEADER_NOT_AVAILABLE: the two mean the same thing to this facade, and only this one is on the consumer's list. |
| `UNKNOWN_SERVER_ERROR` (-1) | no | a per-entry marker this build has no mapping for; an unreadable body; a misaligned answer | The unmapped-marker case raises the whole topic's log line to ERROR: it means the broker grew an answer the facade has to learn. |

A **429 is not an error on this path at all**: the partition is answered
error-free and empty, with `throttle_time_ms` beside it, so the consumer sleeps
and polls again rather than also paying for a metadata refresh the capped
tenant cannot afford.

## ListOffsets (v1–v5) — per partition

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes | a `__` or unnameable topic; a partition index outside `0..width`; the bounds probe said UNKNOWN; Queen answered 404 | **Changed in M6** for the unnameable case, same reason as Fetch: this request is on a consumer's recovery path from OFFSET_OUT_OF_RANGE. The width check is the same one Fetch applies, and it matters here because this is the call `seekToEnd` and every lag tool makes: a lane that does not exist answered `0` reads as an empty one. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `NOT_LEADER_OR_FOLLOWER` (6) | yes (+metadata) | no answer; 429; 502–504 | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | an unmapped probe marker, an unreadable body, a misaligned answer | |

Two non-error answers are load-bearing here. A **concrete timestamp** answers
offset `-1` with error 0 — Queen has no time index — and, **changed in M6**, it
is still probed first, so a concrete timestamp against a topic that does not
exist answers UNKNOWN_TOPIC_OR_PARTITION instead of claiming the topic is there
with no record at that time.

## FindCoordinator (v0–v3) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_GROUP_ID` (24) | no | an empty group id, or one past 255 characters | Apache Kafka answers this here too. The Java client raises `KafkaException` naming it, which is the right end for a misconfiguration. |
| `INVALID_REQUEST` (42) | no | a `key_type` this facade does not serve (the transaction coordinator) | |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the group registry is at its cap, or the actor could not be reached | Every client retries this after re-discovering the coordinator. |

## JoinGroup (v0–v4) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `MEMBER_ID_REQUIRED` (79) | — | KIP-394: a v4 join with an empty member id | Not an error to a client that speaks v4 — it is the round trip. The minted id is in the response. |
| `UNKNOWN_MEMBER_ID` (25) | — | a member id this coordinator never issued, issued before a restart, or evicted | The client forgets its id and joins again with an empty one. |
| `INVALID_SESSION_TIMEOUT` (26) | no | outside `QUEEN_KAFKA_GROUP_MIN/MAX_SESSION_MS` | |
| `INCONSISTENT_GROUP_PROTOCOL` (23) | no | no protocol type or list; a different protocol type from the group's; no assignment protocol in common | Checked on the way IN, like Apache Kafka's `doJoinGroup`, so one mis-set `partition.assignment.strategy` costs that client alone instead of the whole group. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | group cap reached, or the actor could not be reached | |
| `INVALID_GROUP_ID` (24) | no | empty, or past 255 characters | |

The **rebalance timeout** a member sends is clamped to 30 minutes
(`MAX_REBALANCE_TIMEOUT`) rather than refused: there is no error code a consumer
reads as "that field is wrong", and an unclamped `i32::MAX` is one member
holding the whole group's join window open for twenty-five days. The clamp is
logged.

## SyncGroup, Heartbeat, LeaveGroup (v0–v2) — top level

| Code | Retriable | When |
|---|---|---|
| `UNKNOWN_MEMBER_ID` (25) | — | not a member of this group (Sync, Heartbeat, Leave); the group is empty or gone |
| `ILLEGAL_GENERATION` (22) | — | the generation is not the current one (Sync, Heartbeat) |
| `REBALANCE_IN_PROGRESS` (27) | — | a join window is open (Sync, Heartbeat) |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the actor could not be reached |
| `INVALID_GROUP_ID` (24) | no | empty, or past 255 characters |

Two deliberate deviations, both documented in the code: a Heartbeat during
**CompletingRebalance** answers NONE rather than REBALANCE_IN_PROGRESS (telling
it to rebalance would reopen the window it is on the far side of, and the group
would chase its own tail), and INVALID_GROUP_ID on Sync/Heartbeat is a code the
Java client raises `KafkaException` for — unreachable from a real client, which
would have been refused at JoinGroup first.

## OffsetCommit (v2–v6) — per partition

OffsetCommit has no top-level error field, so a group-wide refusal is written
into every partition of the response. That is what Apache Kafka does too.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_GROUP_ID` (24) | no | empty, or past 255 characters | |
| `UNKNOWN_MEMBER_ID` (25) | — | not a member; or a SIMPLE consumer (generation -1, empty member id) committing under a group that has live members | The second is how two consumers silently overwrite each other's progress, so it is refused. |
| `ILLEGAL_GENERATION` (22) | — | a generation that has ended, or any non-simple commit into a group this coordinator does not hold | Both are answered by rejoining. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the actor could not be reached; the store answered 408/429/502–504, or not at all | |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes | a `__` or unnameable topic; a negative partition index | **Changed in M6** for the unnameable case: INVALID_TOPIC_EXCEPTION is outside the set the Java commit path knows and becomes a bare `KafkaException`. |
| `INVALID_COMMIT_OFFSET_SIZE` (28) | no | an offset below Kafka's own -1; or a `(group, topic)` whose composed key is longer than the store's key column | The second must fail LOUDLY: a commit this facade cannot store would otherwise read back later as "never committed". |
| `OFFSET_METADATA_TOO_LARGE` (12) | no | metadata past `offset.metadata.max.bytes` | |
| `GROUP_AUTHORIZATION_FAILED` (30) | no | the store answered 401 or 403 | GROUP and not TOPIC: the credential that failed is the one reading the group's offsets, and a client that reports the wrong noun sends its operator to the wrong grant. |
| `UNKNOWN_SERVER_ERROR` (-1) | no | the store answered something else, or unreadably | |

`retention_time_ms` (v2–v4) is accepted and not acted on: offsets here never
expire, so ignoring it can only keep them LONGER than asked, which is the safe
direction.

## OffsetFetch (v1–v7)

**No partition of this response ever carries an error code.** That is Apache
Kafka's own shape — the broker answers `-1` with error 0 for every
`(topic, partition)` the group has not committed, whatever the name looks like —
and the Java consumer raises `KafkaException` out of `poll()` for ANY
per-partition code here. **Changed in M6**: a `__` topic, an unnameable name, a
negative partition index and an over-long key are all answered `-1`/0 now, and
the "store answered short" case is refused group-wide instead.

Group-level codes (`error_code`, v2+; mirrored into every partition for v1,
which has no top-level field):

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_GROUP_ID` (24) | no | empty, or past 255 characters | |
| `COORDINATOR_LOAD_IN_PROGRESS` (14) | yes | the store did not cover every key asked for | The one thing worse than an error here is a wrong `-1`: reporting an unread key as "never committed" resets a consumer that had committed perfectly well. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the store answered 408/429/502–504, or not at all | |
| `GROUP_AUTHORIZATION_FAILED` (30) | no | the store answered 401 or 403 | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | the store answered something else, or unreadably | |

A partition with no committed offset is `-1` with error 0, and that is the whole
contract: `-1` is what makes a client apply `auto.offset.reset`, which is the
only correct behaviour for a group that has never committed.

## CreateTopics (v2–v6) — per topic

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_TOPIC_EXCEPTION` (17) | no | the name is not a legal Kafka topic name, or begins with `__` | The second API that emits it, and for the reason the audit's rule names: this is a surface where a NAME is validated, so the client can act on it. `__` is INVALID here and UNKNOWN in Metadata on purpose — creating one would make a queue the facade then refuses to show anywhere. |
| `TOPIC_ALREADY_EXISTS` (36) | no | the catalog already has a queue of that name | And `POST /api/v1/configure` is **not** called for it. The stored procedure is an upsert that rewrites every config column to its defaults, so a create over a live queue would silently reset its leaseTime, retention, retry policy and dedup window. |
| `INVALID_REPLICA_ASSIGNMENT` (39) | no | a non-empty `assignments` | A manual assignment names broker ids to place partitions on; this facade places nothing anywhere. Accepting it would be silently discarding an explicit operator instruction. |
| `INVALID_CONFIG` (40) | no | `cleanup.policy=compact` (or `compact,delete`); `retention.ms` under 1000 ms or below -1; any config name outside the mapping | The mapping is `src/topic_config.rs`. Compaction is refused because it is a stated non-goal and nothing compacts — which is what makes Kafka Connect fail at startup instead of losing its connector configuration on a later restart. A sub-second retention is refused rather than rounded to zero, which would mean "delete everything". |
| `INVALID_REQUEST` (42) | no | the same topic name appears more than once in one request | Apache Kafka's own answer, and none of the entries is created. It is also what stops the second configure for one name being an upsert over the queue the first one just made. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | The connection's credential may not create queues. |
| `THROTTLING_QUOTA_EXCEEDED` (89) | yes | **v6 only**: Queen answered 429, or the request asked for more than 100 topics | KIP-599, and the whole reason v6 is in the advertised window: it is the version at which a client understands the code. The wait rides `throttle_time_ms` beside it. |
| `REQUEST_TIMED_OUT` (7) | yes | below v6, everything the row above covers; at any version, Queen unreachable, a 408, or a 5xx; and the queue list being unreadable | A `RetriableException` in the Java AdminClient, so the call is retried inside the request timeout rather than surfaced. An unreadable catalog creates NOTHING: "absent" cannot be assumed from a read that failed, or the create becomes the upsert described under code 36. |
| `UNKNOWN_SERVER_ERROR` (-1) | no | Queen answered 2xx with a body this facade could not read, or a `configure` that did not confirm | Loud by construction: it is our bug or the broker's, and dressing it up as a timeout would leave a client retrying for ever. |

`validate_only` runs every check above and writes nothing; the answer is what
*would* have happened, including the width.

## DeleteTopics (v1–v5) — per topic

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | the route answered `{"deleted": false}`; or the name begins with `__` or is not a legal topic name | Kafka's own answer for deleting a topic that is not there. The name rule goes through `not_a_topic_here` and **not** through the INVALID code: this is not a name-validation surface, it is a "do you have this" surface. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `REQUEST_TIMED_OUT` (7) | yes | Queen unreachable, a 408, a 429 or a 5xx; or the request asked to delete more than 100 topics | DeleteTopics has no `THROTTLING_QUOTA_EXCEEDED` version inside the advertised window — v6 would, and v6 is out because it names topics by id — so the retriable code available here carries the throttle. |
| `UNKNOWN_SERVER_ERROR` (-1) | no | a body this facade could not read, or a 404 | The route answers 200 for a queue that is not there, so a 404 means the route itself is absent: a facade pointed at something that is not a Queen broker. |

**Two things this API leaves behind, stated rather than half-fixed.** A queue
that native Queen producers share with Kafka clients can be deleted by the Kafka
half — no new privilege (the same bearer can issue the same HTTP DELETE) but a
new blast radius. And committed offsets under `qk:group:*:<topic>:*` are not
removed with the topic and become orphans; Kafka has the same shape, and
DeleteGroups (below) is the tool for them.

## DescribeConfigs (v1–v4) — per resource

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | a TOPIC resource the catalog does not have, or a `__`/illegal name | Same `not_a_topic_here` rule as the read paths. |
| `INVALID_REQUEST` (42) | no | a BROKER resource named anything but `` or this node's id; any resource type other than topic (2) and broker (4) | BROKER_LOGGER (8) describes a log4j hierarchy this facade does not run. Answering an empty config set instead would read as "this resource exists and is empty", which is a different and wrong thing. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | the queue list could not be read because Queen answered 401 or 403 | |
| `REQUEST_TIMED_OUT` (7) | yes | the queue list could not be read for any other reason, including a 429 | A read, so it answers what the read paths answer, with the wait on `throttle_time_ms`. A BROKER resource is unaffected: it is answered out of this process and makes no call at all. |

**What it does NOT report, and why that is the honest answer.** A key is
reported only where the facade can name the thing that enforces its value. For a
TOPIC that is two keys — `cleanup.policy=delete` and `min.insync.replicas=1` —
because Queen exposes **no HTTP read of a queue's configuration**:
`GET /api/v1/resources/queues/:queue` answers no config at all, and
`GET /api/v1/status/queues/:queue` answers leaseTime, retryLimit, retryDelay,
ttl, maxQueueSize and deadLetterQueue and not `retentionEnabled`/
`retentionSeconds`. So `retention.ms` is **writable and not readable** here:
CreateTopics sets it, and the create's own v5+ config echo is where a client
reads it back. Omitting a key is protocol-legal; reporting a plausible default
for a knob nothing honours is not.

Every key is `read_only = true` and `is_sensitive = false`, and the synonym list
is empty. None of that is laziness: AlterConfigs is not advertised, so nothing
here can be changed through this facade, and nothing here inherits its value
from anything else.

## ListGroups (v0–v4) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `GROUP_AUTHORIZATION_FAILED` (30) | no | the durable group index could not be read because Queen answered 401 or 403 | The one KV failure that is reported rather than absorbed. It is not a moment in time, and a client handed a partial list with error 0 would take it for the whole one. GROUP and not TOPIC authorization, the same noun choice `offsets.rs` already argues for: the credential that failed is the one reading a group's data. |

**Every other failure answers error 0 with a SHORTER list**, and that is a
decision rather than an oversight. The answer has two halves — the groups this
process holds members for, and the groups the durable index in Queen knows about
([`src/offsets.rs`](../src/offsets.rs)) — and a tool rendering a page is better
served by the live half plus a log line than by an error. A failed index read is
logged at warn, once per window, naming what is missing: the groups whose
consumers are stopped.

The other bound has no code at all: past 10 000 groups the list is truncated,
because this API has no truncation flag on the wire. That is why the bound is the
same number as the group cap — past it this facade refuses to coordinate a new
group anyway — and why reaching it is a log line.

`states_filter` (v4, KIP-518) is applied, not ignored. A state string nothing is
in answers an empty list and no error, which is what Apache Kafka 3.9.1 does with
an unknown state too (measured).

## DescribeGroups (v0–v3) — per group

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_GROUP_ID` (24) | no | the group id is empty or longer than 255 characters | The facade's own bound, through the one `coordinator::invalid_group_id` all six group-addressed APIs share, so a name JoinGroup refuses and this one describes cannot exist. Apache Kafka has no such bound and answers `Dead` for these names; the protocol gives the field none either, so a client may send ~32 KB of one at the non-flexible versions and every copy of it would be this facade's. |
| `NOT_COORDINATOR` (16) | yes | **cluster mode only**: this node is not the rendezvous owner of the group | The redirect: every client answers it by re-running FindCoordinator. This node cannot see the members of a group another facade coordinates, and `Empty` would be a plausible wrong answer. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | **cluster mode only**: the live-node view is too old to say who owns the group; or the request asked about more than 1000 groups | |
| `COORDINATOR_NOT_AVAILABLE` (15) / `UNKNOWN_SERVER_ERROR` (-1) | yes / no | the durable group index could not be read | Answered rather than guessed: without the index this facade cannot tell a group that never existed from one whose consumers are stopped, and `Dead` for the second reads as "somebody deleted my group". The mapping is `offsets::kafka_error`. |

**A group nobody has ever heard of is error 0 with state `Dead`**, not an error.
Measured against `apache/kafka:3.9.1` at v0, v3 and v5 before the handler was
written, because it is counter-intuitive enough that writing it from the name of
the code would have got it wrong — and `kafka-consumer-groups.sh` turns exactly
that answer into `Consumer group 'g' does not exist.`

The three answers, and the two halves they come from:

| The group is | `group_state` | `protocol_type` | members |
|---|---|---|---|
| live on this facade | the FSM's | the members' | every member |
| only in the durable index | `Empty` | the index's | none |
| in neither | `Dead` | `""` | none |

`authorized_operations` (v3) is **always** `i32::MIN` — Kafka's own
`AUTHORIZED_OPERATIONS_OMITTED`, which the Java client turns into `null` and
tools render as "unknown" — whether or not `include_authorized_operations` was
set. Kafka 3.9.1 with no authorizer answers 328 (READ|DELETE|DESCRIBE) instead.
The facade has no ACL model: what a credential may do is Queen's to say, per
call, and it says so by answering 401 or 403 to that call. A bitfield computed
here would be a permission set this process invented.

`group_instance_id` is always null and the window stops one version below where
it is encoded: static membership is out of scope, the same rule that caps
JoinGroup at v4.

## DeleteGroups (v0–v2) — per group

| Code | Retriable | When | Notes |
|---|---|---|---|
| `NON_EMPTY_GROUP` (68) | no | the group has members | Kafka's own rule, kept exactly, and **nothing is touched**. It is the whole guard: without it one `--delete` typed against a running fleet would silently reset every consumer in it to `auto.offset.reset`. |
| `GROUP_ID_NOT_FOUND` (69) | no | there is no actor for the group and the store held nothing under its prefix | Kafka's own answer, measured. It is also what a second delete of the same group answers, which is what makes a partially failed delete safe to re-run. |
| `INVALID_GROUP_ID` (24) | no | the group id is empty or longer than 255 characters | Same rule and same reason as DescribeGroups above. |
| `NOT_COORDINATOR` (16) | yes | **cluster mode only**: this node is not the rendezvous owner | Applied before anything is read or written: a node that cannot see whether the group has members cannot apply the emptiness rule, and deleting the offsets of a group another node is running is exactly what that rule exists to stop. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | **cluster mode only**: the view is too old to say; or the request asked to delete more than 100 groups | |
| `GROUP_AUTHORIZATION_FAILED` (30) | no | Queen answered 401 or 403 | |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | Queen unreachable, a 408, a 429 or a 5xx | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | a body this facade could not read | |

**A failure part-way through leaves a partially deleted group, and that is said
rather than papered over.** The delete is offsets first (a paged prefix walk,
deleted a page at a time) and the index row last, so a failure between them
leaves a group that lists as `Empty` with nothing committed — a state Kafka has
too. There is no transaction across a KV batch boundary; every step is
idempotent, the answer is the retriable code the failure maps to, and re-running
the delete finishes the job.

**What this API is, and is not.** It is the only thing in the facade that removes
a committed offset, and it is a TOOL rather than a policy: offsets still never
expire on their own, and nothing here adds `offsets.retention.minutes`. What it
deletes is `qk:group:<group>:*` and `qk:groups:<group>` under the connection's
own credential. A group's fence key (`qk:fence:<group>`, cluster mode only) is
left alone — it belongs to `src/cluster/fence.rs`, and a stale fence on a
recreated group is resolved by that module's own discovery.

## InitProducerId (v0–v4) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | a non-empty `transactional_id` | The same code and the same sentence `Produce` gives a transactional id, so a user meets ONE message about transactions and not two. Fatal in the Java client, out of `InitProducerIdHandler`. An EMPTY id is **not** a transactional id and is granted normally: brod's hand-rolled encoder writes a null one as `""` (`idempotent::transactional_id`). |

That is the only error this API can answer. The handler makes **no call to
Queen at all** — the connection is already authenticated, so there is no
catalog to read, no push to make and nothing to be unavailable — which is
exactly the property the biggest onboarding papercut wanted: the answer is a
number and a zero, on the same turn of the connection loop.

**A transactional client does not reach this handler, and the refusal it does
meet is not fast.** Measured 2026-08-29 with kafka-clients 4.3.1: a producer
with `transactional.id` set asks `FindCoordinator` for a TRANSACTION coordinator
first, the facade answers that `COORDINATOR_NOT_AVAILABLE` (15) — which is
**retriable** — and the client loops there for the whole of `max.block.ms`
(~190 requests over 20 s) without ever sending an InitProducerId.
`initTransactions()` therefore still costs 20 s, exactly as it did before key 22
was advertised. Advertising the key did not change that and was never going to;
the fix is a fatal code on the FindCoordinator transaction path, which is
outside M7 F3's scope. Sent straight at node 0, this handler refuses in ~10 ms.

## SaslHandshake (v0–v1) / SaslAuthenticate (v0–v1)

| Code | API | When | Notes |
|---|---|---|---|
| `UNSUPPORTED_SASL_MECHANISM` (33) | Handshake | anything but PLAIN | The state is NOT advanced, so a client with a mechanism list may ask again for PLAIN on the same connection. |
| `ILLEGAL_SASL_STATE` (34) | both | already authenticated, or the listener has no SASL | The single most useful error this facade can give a client whose `security.protocol` says SASL while the listener is plaintext — otherwise a hang or a parse error. |
| `SASL_AUTHENTICATION_FAILED` (58) | Authenticate | Queen refused the credential, the PLAIN response is malformed, or Queen could not be reached | Answered, then the connection closes — the code is what stops a client retrying for ever. Nothing about the credential reaches the log (`conn::tests::no_credential_reaches_the_log_at_any_level`). |

## What is answered by CLOSING the connection, not by a code

Apache Kafka closes here too, and the client reconnects and renegotiates:

* a request at a version outside the advertised window (except ApiVersions);
* an api key that is not in the advertised table;
* a header or body that does not decode;
* any request other than ApiVersions/SaslHandshake/SaslAuthenticate before the
  connection has authenticated, on a listener with `QUEEN_KAFKA_SASL=plain`
  (answering an error code would be answering an unauthenticated request);
* a frame past the size ceiling.
