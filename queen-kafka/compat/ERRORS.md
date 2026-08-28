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
| `INVALID_TOPIC_EXCEPTION` (17) | no | the name is not a legal Kafka topic name, or a null name in the request | **The only API that emits this**, and the API a client can act on it from. |
| `LEADER_NOT_AVAILABLE` (5) | yes (+metadata) | the queue list could not be read; auto-create failed or was still in flight | Also the code beside `throttle_time_ms` when the tenant is capped — retriable, so the client backs off and comes back. |

## Produce (v3–v9) — per partition

| Code | Retriable | When | Notes |
|---|---|---|---|
| `INVALID_REQUIRED_ACKS` (21) | no | `acks` is not 0, 1 or -1 | |
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) | no | a `transactional_id` on the request, or the transactional flag on a batch | Fatal and final for the producer, and unmistakably about transactions. Kafka Streams stops here with a sentence instead of appearing to work. |
| `INVALID_RECORD` (87) | no | a CONTROL batch | A control batch is written by a transaction coordinator; this facade is nobody's. |
| `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43) | no | a producer id is set (the idempotent producer), or the records are a pre-v2 message set | Not retriable, which is the point: accepting an idempotent producer would store sequence numbers nothing enforces and every retry would duplicate silently. |
| `CORRUPT_MESSAGE` (2) | **yes** | the batch headers or the records did not decode | Retriable in Kafka (a CRC failure can be the wire), and this is the code a real broker gives for the same thing. A producer therefore retries the same undecodable batch until `delivery.timeout.ms` and then fails — matching Apache Kafka, deliberately. |
| `MESSAGE_TOO_LARGE` (10) | no | the request decompresses past the frame ceiling, declares more records than one request may decode, or Queen answered 413 | The producer's own answer is to split the batch or raise `max.request.size`. |
| `UNKNOWN_TOPIC_OR_PARTITION` (3) | yes (+metadata) | a `__` or unnameable topic; a partition outside the advertised width; Queen answered 404 | Past-the-width is UNKNOWN and not an invalid-request code because it is usually a stale metadata view, and UNKNOWN is what makes the client refresh. |
| `INVALID_TOPIC_EXCEPTION` (17) | no | the name is not a legal Kafka topic name | Kept here, unlike on the read paths: the Java **producer** fails the batch with the named exception rather than throwing on an unexpected code, and the producer's topic name really is illegal. |
| `LEADER_NOT_AVAILABLE` (5) | yes (+metadata) | the queue list was unreadable, auto-create did not resolve, or Queen answered 502–504 | |
| `REQUEST_TIMED_OUT` (7) | yes | no answer from Queen at all (connect, DNS, TLS, reset, our own budget); Queen answered 408; **or 429** | The one produce code whose meaning is "we do not know whether it landed". For 429 it is the code beside `throttle_time_ms`: THROTTLING_QUOTA_EXCEEDED is not on librdkafka's produce-retry list, which made a rate cap a permanent delivery failure on every Confluent client. |
| `TOPIC_AUTHORIZATION_FAILED` (29) | no | Queen answered 401 or 403 | |
| `UNKNOWN_SERVER_ERROR` (-1) | no | Queen answered something else (a 400 is our bug), an unreadable 2xx, or a push whose offsets do not line up | Loud in the log by construction. |

`acks=0` writes no response frame at all, so none of these reaches that
producer; they are logged instead (`log_silent_failures`).

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
