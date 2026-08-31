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
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | the transactional flag on a batch whose REQUEST carries no `transactional.id` | **Narrowed in M9.** A `transactional_id` on the request is no longer refused — it is a STAGE (`crate::txn`), and the records are written by `EndTxn(commit)`. What is left here is a transactional BATCH with no id on the request: there is no transaction to stage it in and no producer identity to fence it against. |
| `INVALID_TXN_STATE` (48) | no | a transactional produce for a `transactional.id` this facade holds no binding for, or for a partition the transaction never added | **M9.** The first is the crash path: a restart, or a connection that moved. Fatal in the Java transactional producer, and it has to be — the transaction genuinely cannot continue, and no other answer keeps an application from believing an uncommitted commit. The second is Kafka's own rule: a producer must send `AddPartitionsToTxn` for a partition before producing to it. |
| `MESSAGE_TOO_LARGE` (10) | no | a transactional produce past `QUEEN_KAFKA_TXN_MAX_BYTES` or `QUEEN_KAFKA_TXN_MAX_RECORDS` | **M9, and a deviation with no Kafka analogue** — a Kafka transaction has no size, because its records are appended as they arrive. Not retriable, deliberately: waiting will not make a 12 MiB transaction fit an 8 MiB stage. The transaction becomes abortable and the producer must abort it. |
| `PRODUCER_FENCED` (90) | no | a transactional produce whose epoch is below the one this facade holds | **M9.** A second producer took the `transactional.id`. |
| `INVALID_PRODUCER_EPOCH` (47) | no | a transactional produce whose epoch is ABOVE the one this facade holds | **M9.** An epoch this facade never granted. |
| `INVALID_PRODUCER_ID_MAPPING` (49) | no | a transactional produce whose `producer_id` is not the one this `transactional.id` holds | **M9.** |
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
| `INVALID_REQUEST` (42) | no | a `key_type` that is neither 0 (group) nor 1 (transaction) | **Corrected in M9.** This row used to say "the transaction coordinator", which the handler never answered: `key_type == 1` was answered `COORDINATOR_NOT_AVAILABLE` (15), and that mismatch is what hid the 20-second hang below. |
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | `key_type == 1` (a transaction coordinator) **in cluster mode** | **M9.** In SINGLE-NODE mode there is no error at all: this process is the transaction coordinator and answers its own address, exactly as it answers a group key. In cluster mode there is none, and the code has to be FATAL — a retriable one costs the client the whole of `max.block.ms` (see below). |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the group registry is at its cap, or the actor could not be reached, or a clustered node's view of the live set is stale | Every client retries this after re-discovering the coordinator. |

**The 20 seconds, and why the code here is the whole of it.** Measured
2026-08-29 with kafka-clients 4.3.1: a producer with `transactional.id` set asks
FindCoordinator for a TRANSACTION coordinator *first*, and while this handler
answered `COORDINATOR_NOT_AVAILABLE` — retriable — the Java
`FindCoordinatorHandler` re-enqueued the lookup and looped for the whole of
`max.block.ms` (~190 requests over 20 s) without ever sending an InitProducerId.
Advertising key 22 did not change that and was never going to, because the
client never got far enough to send key 22. The fix is the code, and it is in
this table.

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

## DescribeAcls, CreateAcls, DeleteAcls (v1–v3)

Three keys, one answer, and the answer is Apache Kafka's own. A broker with no
`authorizer.class.name` refuses all three `SECURITY_DISABLED (54)`, and so does
this facade, at every version and for every filter.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `SECURITY_DISABLED` (54) | no | always | DescribeAcls carries it at the **top level** with an empty `resources`, message `No Authorizer is configured on the broker`. CreateAcls and DeleteAcls carry it **per element** (one result per creation, one per filter, `matching_acls` empty), message `No Authorizer is configured.` |

**Two different sentences, and that is not a typo.** Apache Kafka's
`AclApis.handleDescribeAcls` builds its response by hand and sets *"No
Authorizer is configured on the broker"* with no full stop, while create and
delete go through `SecurityDisabledException("No Authorizer is configured.")`.
Both were recorded off `apache/kafka:3.9.1` and both are pinned, because the
acceptance bar for this family is a byte-for-byte match and the first attempt at
it used one string for all three.

**Per element, not top level, on the two writes.** Kafka's `getErrorResponse`
maps over the request, so an empty `creations` or `filters` list answers an
empty result list and **no error at all**. A top-level-only error on those two
would decode in a Java client as "the call succeeded and returned nothing",
which is the opposite of the answer.

**What this does not claim.** The facade does authenticate (SASL/PLAIN carrying
a Queen bearer) and Queen does authorize (401/403 arrive here as
TOPIC_AUTHORIZATION_FAILED and GROUP_AUTHORIZATION_FAILED where they arise).
"No Authorizer is configured." is true in Kafka's narrow sense, because there is
no principal/resource/operation table for a client to read or write, and saying
more on the wire would cost the byte-for-byte match. The fuller explanation is
here and in `CLIENT_MATRIX.md`, not in the response.

Nothing is read, nothing is written, and no Queen call is made: the handlers do
not take a `Facade` at all. In cluster mode every node answers identically,
because there is nothing to own. `kafka-acls.sh --list`, `--add` and `--remove`
all print `Error while executing ACL command: <the message>` and exit 1, which
is what they print against a real Kafka with security off.

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
`retentionSeconds`. Omitting a key is protocol-legal; reporting a plausible
default for a knob nothing honours is not.

**`retention.ms` is the third key, and only for a topic this facade created.**
Since M7 F4 the facade keeps its own record of the options bag it posted to
`POST /api/v1/configure` for each topic
([`src/topic_record.rs`](../src/topic_record.rs)), and retention is reported out
of that record — so a topic created through CreateTopics or auto-created through
Metadata round-trips its retention, and a topic the facade did not create still
omits the key. A record with retention enabled reports the window at the
resolution Queen stored it (whole seconds), sourced `TOPIC`; a record with no
retention reports `-1` sourced `DEFAULT`, because Queen's default is retention
off and that IS Kafka's `-1`.

The record is pinned to the queue's `id`, so a queue dropped and recreated under
the same name is caught and the key is omitted rather than answered from the
dead record. **The one window that remains, stated plainly:** a retention
changed OUTSIDE this facade — the Queen console, another SDK — between two
facade writes is invisible here, and the value reported is the one the facade
last applied. It is the same last-writer-wins two admins have against a real
Kafka, and it is said on the wire too, in the row's `documentation`.

`is_sensitive = false` on every key and the synonym list is empty: nothing here
is a credential and nothing here inherits its value from anything else.
`read_only` is **per row**. `cleanup.policy` and `min.insync.replicas` are
`true`, because the only value either accepts is the one already reported;
`retention.ms` is `false` on a tracked topic, because AlterConfigs and
IncrementalAlterConfigs really do land on it. Every BROKER row is `true`. A UI
that greys out its edit button on this flag is being told the truth.

## AlterConfigs (v0–v2) — per resource

The deprecated FULL-REPLACEMENT form. **Prefer IncrementalAlterConfigs**: this
key means "the resource's configuration becomes exactly what this request
names", so an AlterConfigs naming only `cleanup.policy=delete` turns retention
off, because retention is a key it did not name. That is what a real broker does
with key 33 and it is why Kafka deprecated it.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | a TOPIC resource the catalog does not have, or a `__`/illegal name | The same `not_a_topic_here` rule as every read path, so a client can tell "there is nothing to change" from "you cannot change this". |
| `INVALID_CONFIG` (40) | no | a topic this facade did not create (or whose queue has been replaced since); an unknown key; a value the facade cannot honour; `cleanup.policy=compact`; any BROKER config | The Java AdminClient turns 40 into a non-retriable `InvalidConfigurationException` whose message `kafka-configs.sh` prints verbatim, which is where each of these sentences has to land to be read at all. |
| `INVALID_REQUEST` (42) | no | a BROKER resource named anything but `` or this node's id; any resource type other than topic (2) and broker (4) | Identical to DescribeConfigs' rule, because it is the same fact. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `REQUEST_TIMED_OUT` (7) | yes | Queen was unreachable, answered 429 (with the wait on `throttle_time_ms`) or 5xx; the queue list or the config record could not be read; the record could not be written after a successful configure | KIP-599's `THROTTLING_QUOTA_EXCEEDED` is deliberately NOT used: neither of these APIs has a version at which a client is required to understand it, and a code outside the closed set the client accepts ends the application instead of making it retry. |

**Why an untracked topic is refused rather than altered.**
`POST /api/v1/configure` is a whole-row upsert over nineteen columns
(`server/sql/procedures/012_configure.sql`) and **thirteen of them cannot be read
back through any Queen route**. So a "set just this one key" write would reset a
tenant's dedup window, lease time, retry limit, TTL and DLQ flag to the stored
procedure's defaults. What makes an alter possible at all is the record above:
for a topic the facade created, the complete bag is known by construction, and
the write is that bag merged with the request. For every other topic there is
nothing to merge onto and the only alternative would be to guess at thirteen
columns, so the answer is `INVALID_CONFIG` with a sentence that says exactly
that. On a deployment that predates M7 F4 this is every topic; recreating a
topic through this facade, or setting the value in the Queen console, are the
two ways out.

**A failed record write is a retriable refusal, not a silent success.** The order
is: configure, then record. If the record write fails the record is DELETED and
the call answers `REQUEST_TIMED_OUT` — absence is the one honest state, because a
describe then omits `retention.ms` rather than reporting the value from before
the alter, and the client's retry re-applies a write that is idempotent.

**Cluster mode has no ownership gate.** These are topic-addressed, `/configure`
is a write any node may make and the record is in shared KV, so two nodes
altering one topic is last-writer-wins — which is what Apache Kafka's
AlterConfigs is, having no optimistic concurrency of its own.

## IncrementalAlterConfigs (v0–v1) — per resource

The DELTA form, and **the one `kafka-configs.sh --alter` actually sends** —
`ConfigCommand` has used it since Kafka 2.3 and 3.9's has no fallback to key 33.
Everything the request does not name is left exactly as it is: the write is the
stored bag merged with the delta, posted whole.

Every code and every rule above applies unchanged. The one addition is the
operation:

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_CONFIG` (40) | no | `APPEND` or `SUBTRACT` on `retention.ms` or `min.insync.replicas` | Those operations are legal only for LIST-typed configs and neither of those is one. |
| `INVALID_CONFIG` (40) | no | `SUBTRACT delete` from `cleanup.policy` | It computes an empty policy, and a topic with no cleanup policy is not a thing this facade or Kafka will have. `APPEND compact` computes `[delete,compact]` and meets the ordinary compaction refusal, which is the message an operator needs. |
| `INVALID_REQUEST` (42) | no | a `config_operation` that is not SET (0), DELETE (1), APPEND (2) or SUBTRACT (3) | Named rather than silently treated as a SET. |

`DELETE` resets a key to its default by dropping it out of the bag, which leaves
`configure_queue_v1`'s own default in force — for `retention.ms` that is
retention off, which is Kafka's `-1`. The request's `value` is ignored for a
DELETE, which is what Kafka does.

`validate_only` is honoured on both APIs: everything is computed, the response is
built the same way, and nothing is written. A delta that computes to the bag
already stored writes nothing either, and answers 0.

## CreatePartitions (v0–v3) — per topic

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | no | no such queue in the catalog, a `__`-prefixed name, or an illegal one | One rule and one code, the same as every other non-Metadata API. The `error_message` is null here, because the oracle sends none for this code either. |
| `INVALID_PARTITIONS` (37) | no | the count equals the advertised width, is below it, or is above it | Three different sentences; see below. Every one of them is a refusal, and nothing is ever written. |
| `INVALID_REPLICA_ASSIGNMENT` (39) | no | an INCREASE that carries a non-empty `assignments` | The same sentence `CreateTopics` gives the same field: one logical broker, no partition placed on any node, so an explicit placement cannot be honoured. A DECREASE that carries an assignment is still `INVALID_PARTITIONS`, because that is the order the oracle applies the two checks in. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 on the catalog read | |
| `REQUEST_TIMED_OUT` (7) | yes | the catalog could not be read: unreachable, 429 or 5xx | A 429's `Retry-After` rides out as `throttle_time_ms`. |

**This whole API is a refusal, and two thirds of it is Apache Kafka's own.**
Queen declares no width per queue: `POST /api/v1/configure` has no `partitions`
option, `queen.queues` has no such column, and a lane exists once something has
been pushed to it. The width advertised is `max(live lanes,
QUEEN_KAFKA_DEFAULT_PARTITIONS)`, and the second half of that is a broker
start-up setting rather than a per-topic one, so no write widens one topic.

The three messages, the first two recorded off `apache/kafka:3.9.1` in KRaft
mode rather than copied from a document:

* count equal to the current width: `Topic already has N partition(s).`
* count below it (a DECREASE, which a real broker refuses too):
  `The topic X currently has N partition(s); M would not be an increase.`
* count above it: the facade's own sentence, which names
  `QUEEN_KAFKA_DEFAULT_PARTITIONS` and the alternative of producing to the
  higher lanes directly.

Note there is no separate "below 1" answer, and that is measured too:
`--partitions 0` takes the DECREASE branch on the oracle, because the width is
never negative and the comparison catches every non-positive count first.

The deviation is the third case only. A provisioner declaring 12 partitions
against a facade whose default is 1024 is a decrease, where this answer is
indistinguishable from a real broker's. `validate_only` changes nothing, because
nothing is written on any path.

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

## OffsetDelete (v0) — top level and per partition

Top level, and when one of these is set the `topics` list is EMPTY, which is the
shape `OffsetDeleteRequest.getErrorResponse` builds and the one the Java
AdminClient reads before it looks at any partition:

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_GROUP_ID` (24) | no | the group id is empty or longer than 255 characters | The same rule as DescribeGroups and DeleteGroups. |
| `GROUP_ID_NOT_FOUND` (69) | no | no live actor for the group and no row under `qk:groups:` | Kafka's own answer, measured. |
| `INVALID_REQUEST` (42) | no | the request named more than 4096 partitions | A ceiling rather than a limit anyone meets. Without it one frame buys an unbounded run of admin calls on a muted connection. Split the request. |
| `NOT_COORDINATOR` (16) | yes | **cluster mode only**: this node is not the rendezvous owner | Applied before anything is read or written. A non-owner must not delete a group another node is running, and must not read that group's membership to decide the subscription rule either. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | **cluster mode only**: the view is too old to say; or the existence read failed | |
| `GROUP_AUTHORIZATION_FAILED` (30) | no | Queen answered 401 or 403 on the existence read | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | a body this facade could not read | |

Per partition:

| Code | Retriable | When | Notes |
|---|---|---|---|
| `GROUP_SUBSCRIBED_TO_TOPIC` (86) | no | a live consumer group is subscribed to the topic | Kafka's guard for this API, kept exactly. Also what an undecodable member subscription answers; see below. |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | no | a `__`-prefixed name, an illegal name, or a negative partition index | |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the delete call failed: Queen unreachable, a 408, a 429 or a 5xx | |
| `GROUP_AUTHORIZATION_FAILED` (30) | no | Queen answered 401 or 403 on the delete | |
| `NOT_COORDINATOR` (16) | yes | **cluster mode only**: the fence was lost inside the write | Nothing was removed. The client re-runs FindCoordinator and deletes where it should have. |

**Kafka's rule here is SUBSCRIPTION, not membership**, and it is kept exactly
(measured against `apache/kafka:3.9.1`): an empty group has every named
partition deletable; a live group running the `consumer` protocol has a
partition deletable only if the group is not subscribed to its topic, so a live
group's offsets for an UNSUBSCRIBED topic are deleted; and a live group of any
other protocol type has everything deletable. The subscription is decoded from
each member's JoinGroup metadata, which is a two-byte version followed by a
`ConsumerProtocolSubscription`. **If any member's bytes cannot be read, the
group counts as subscribed to everything the request named**, so the failure
mode is a refused delete rather than a wrong one.

**Deleting an offset that was never committed is error 0**, not a failure. The
store answers `applied:false` for a key that is not there, and reading that as a
verdict would turn `--delete-offsets` on a fresh group into a run of spurious
errors. Kafka answers 0 for the same thing.

**The group's existence row is NOT touched, and that is a deliberate
deviation.** This API removes offsets; DeleteGroups removes the group, and stays
the only thing that does. On `apache/kafka:3.9.1`, deleting the LAST offsets of
an already-empty group makes the group vanish from `--list` and answer
GROUP_ID_NOT_FOUND to the next request, while a partial delete leaves it listed
(both measured). Here it stays listed either way. Matching the oracle would mean
a prefix walk on every OffsetDelete to find out whether anything is left, and
would make this API a second way to delete a group.

**A failure part-way through leaves some offsets deleted, and that is said
rather than papered over.** There is no transaction across a KV batch boundary.
Every delete is idempotent, the affected partitions answer the retriable code,
and re-running finishes the job. In cluster mode the batch carries the group's
fence at operation 0 with `required:true`, exactly as an offset COMMIT does, so
a node stale about the live set removes nothing at all rather than removing the
real owner's offsets.

## InitProducerId (v0–v4) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | a non-empty `transactional_id` **in cluster mode** | The same code and the same sentence `handlers::find_coordinator` gives, so a user meets ONE message about transactions and not two. Fatal in the Java client, out of `InitProducerIdHandler`. An EMPTY id is **not** a transactional id and is granted normally: brod's hand-rolled encoder writes a null one as `""` (`idempotent::transactional_id`). |
| `INVALID_REQUEST` (42) | no | a `transactional_id` longer than the key column it is stored in | **M9.** Refused before anything is minted, so an id this facade could not store leaves no state behind. |
| `INVALID_TRANSACTION_TIMEOUT` (50) | no | `transaction.timeout.ms` above `QUEEN_KAFKA_TXN_MAX_TIMEOUT_MS` (default 900 000) or not positive | **M9.** Kafka's own answer for exactly this, with Kafka's own default, so a producer that meets it on a real broker meets it here. |
| `CONCURRENT_TRANSACTIONS` (51) | yes | a third producer moved the key between this facade's claim and its epoch bump, or the process is at `QUEEN_KAFKA_TXN_MAX_OPEN` | **M9.** Retriable and literally true. ONE retry happens inside the facade and then the backoff is the client's — a CAS loop in a request handler is what 024_kv.sql:585-587 forbids. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the transaction store could not be reached for the claim | **M9.** A 429 becomes `CONCURRENT_TRANSACTIONS` instead, and NOT a throttle: the throttle belongs on calls whose volume is what a cap is about, and `initTransactions()` happens once per producer lifetime. |

**The idempotent half still makes no call to Queen at all** — the connection is
already authenticated, so there is no catalog to read, no push to make and
nothing to be unavailable, which is exactly the property the biggest onboarding
papercut wanted. **The transactional half does**: one `putIfAbsent` on a fresh
id, two writes when a second producer takes an id somebody already holds. That
second write is the fencing.

## AddPartitionsToTxn (v0–v3) — per partition

v0–v3 has per-partition error codes and **no top-level one**, so a request-wide
refusal is replicated across every partition — the same shape OffsetCommit uses,
and the same thing Apache Kafka does on an API with no top-level code.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_TXN_STATE` (48) | no | no binding for this `transactional.id` on this facade; the transaction is past its `transaction.timeout.ms`; a partition past `MAX_TXN_PARTITIONS` (200); a transaction already poisoned by a cap | The cap has no `error_message` field to name itself in below v4, so it names itself in a sampled log line instead. |
| `PRODUCER_FENCED` (90) | no | the request epoch is BELOW the bound one | A second producer took the id. |
| `INVALID_PRODUCER_EPOCH` (47) | no | the request epoch is ABOVE the bound one | An epoch this facade never granted. |
| `INVALID_PRODUCER_ID_MAPPING` (49) | no | the `producer_id` is not the one this `transactional.id` holds | |
| `CONCURRENT_TRANSACTIONS` (51) | yes | a commit for this transaction is in flight | |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes | a `__`-prefixed topic, or a negative partition index | The same name rule, from the same helper, as every other path. A topic that merely does not EXIST yet is **not** refused: the produce path auto-creates. |
| `INVALID_TOPIC_EXCEPTION` (17) | no | a name that is not a legal Kafka topic name | |

## AddOffsetsToTxn (v0–v3) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_TXN_STATE` (48) | no | no binding, an expired transaction, or a SECOND, different `group_id` in one transaction | The second group is a stated deviation from Apache Kafka, which allows several. The offset budget is `WIRE_KV_MAX_OPS − 1 fence − 1 group index`, so a silent second group would silently shrink how many partitions a transaction can commit. |
| `PRODUCER_FENCED` (90) / `INVALID_PRODUCER_EPOCH` (47) / `INVALID_PRODUCER_ID_MAPPING` (49) | no | as AddPartitionsToTxn | |
| `INVALID_GROUP_ID` (24) | no | an empty group id, or one past 255 characters | The same rule the six group-addressed APIs apply, from the same place. |

## TxnOffsetCommit (v0–v3) — per partition

This API has no top-level error code, exactly as OffsetCommit has none.

| Code | Retriable | When | Notes |
|---|---|---|---|
| `UNKNOWN_MEMBER_ID` (25) | no | a member id this coordinator never issued, **or a non-null `group_instance_id`** | The second is the honest answer rather than a refusal of the version: `group.instance.id` is only expressible at JoinGroup v5, which is outside the advertised window, so a consumer of this facade can never have one and the field can only ever arrive null. |
| `ILLEGAL_GENERATION` (22) | no | a generation that has ended | The same `coordinator::check_commit` an ordinary OffsetCommit passes, so the group APIs cannot grow two opinions about a valid committer. |
| `INVALID_COMMIT_OFFSET_SIZE` (28) | no | a negative offset other than -1; a composed key past the store's key column; more partitions than one bundle's KV rider holds (`MAX_TXN_OFFSETS`, 62) | All three for one reason: **a commit this facade cannot store must not read back later as "never committed"**. |
| `OFFSET_METADATA_TOO_LARGE` (12) | no | metadata past `offset.metadata.max.bytes` (4096) | Kafka's own number and Kafka's own code. |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes | a `__` or unnameable topic, or a negative partition index | |
| `INVALID_TXN_STATE` (48) / `PRODUCER_FENCED` (90) / `INVALID_PRODUCER_EPOCH` (47) / `INVALID_PRODUCER_ID_MAPPING` (49) | no | as AddPartitionsToTxn | |

Nothing is WRITTEN by this request. The offsets are staged and `EndTxn(commit)`
writes them, in the same Postgres transaction as the records — which is the
whole of exactly-once processing here.

## EndTxn (v0–v3) — top level

| Code | Retriable | When | Notes |
|---|---|---|---|
| `PRODUCER_FENCED` (90) | no | the commit's `required` precondition lost: another producer holds this `transactional.id` | **Zero records and zero offsets were written** — a lost `required` precondition raises 23514 out of `kv_apply_v1` and rolls the whole bundle back (005_log_ack.sql). Asserted by reading the log, not by trusting the code. |
| `INVALID_TXN_STATE` (48) | no | no binding for this `transactional.id`; the transaction expired; a cap poisoned it | **The crash path.** Fatal, and it has to be: a facade that died mid-transaction lost the stage, and this is the only answer that cannot let an application believe an uncommitted commit. A commit that landed and whose response was lost also answers this — a FALSE NEGATIVE, which is the safe direction, because the offsets landed atomically with the records and a restarted application reprocesses nothing. |
| `COORDINATOR_NOT_AVAILABLE` (15) | yes | the bundle could not be sent: a transport failure, a 5xx, a 429 (with `throttle_time_ms`) | **The stage is KEPT**, so the client's retry commits the same records. Dropping it here would turn a retry into a silent empty commit. |
| `CONCURRENT_TRANSACTIONS` (51) | yes | a bundle for this transaction is already in flight | |
| `INVALID_PRODUCER_EPOCH` (47) / `INVALID_PRODUCER_ID_MAPPING` (49) | no | as AddPartitionsToTxn | |

`committed = false` answers **0** for an unheld or expired transaction as well
as for a held one: a lost stage IS an aborted transaction, because nothing of it
ever reached the log. A FENCED producer is still told it is fenced, because that
is a fact about the producer rather than about the transaction — one that
believes it still owns its id would open another transaction it cannot commit.

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
