# PLAN queen-sqs: SQS/SNS wire front for Queen

Goal: unmodified AWS SDKs and the drivers built on them (boto3/Celery, aws-sdk-php/Laravel
`sqs`, async-aws/Symfony, aws-sdk-java/Spring Cloud AWS, aws-sdk-net/MassTransit,
@aws-sdk/client-sqs/sqs-consumer, aws CLI, Terraform, KEDA) run against Queen by changing
`endpoint_url` only — the same workflow ElasticMQ and LocalStack already normalized.
`queen-sqs` is a separate binary deployed beside the broker (OSS) or beside the proxy
(Cloud), stateless: any instance answers any request, a plain Service/LB in front is
supported (the opposite of queen-kafka, and it must stay that way — every design choice
below that says "in KV, not in memory" exists to protect this sentence).

Definition of done — the only rigorous form of "100% compatible" (the queen-kafka bar):
every client in the matrix green, a differential run against REAL AWS SQS/SNS, and zero
unexplained divergences. Divergences are classified `deliberate` or `accepted`, each with
the sentence that must travel with it.

Non-goals, stated loudly in docs:
- **IAM.** `AddPermission`/`RemovePermission` and the `Policy` attribute are accepted and
  stored, never enforced. Authorization is Queen's, over the SigV4 keypair. Same honesty
  as the kafka ACL answer (SECURITY_DISABLED with Apache's own message).
- **The AWS platform.** Lambda triggers, EventBridge pipes, CloudWatch metrics, KMS.
  These are AWS the service, not SQS the API; no facade can provide them.
- **Strict SNS signature validators.** AWS's own validator libs pin `SigningCertURL` to
  `sns.*.amazonaws.com`. HTTP/S subscribers that refuse to configure the cert host cannot
  be satisfied by any self-hosted endpoint. Queue subscribers never see this.
- **Quota theater.** We do not emulate the 120k/20k in-flight caps, FIFO per-group
  throughput quotas, or 64KB billing chunks. Where Queen is a superset (dedup window,
  retention, payload size), the superset is the point and the docs say so.
- **Per-message `ApproximateReceiveCount` inside FIFO batches** — per-claim, not
  per-message (`log_consumers.attempt_count`). Exact on standard queues (claim size 1).
  Classified `accepted`; the field's own name buys the slack.

Analysis of record: scoping artifact 2026-08-30 (claude.ai/code/artifact/77908bdc-136f-4060-9fec-c31d87a02120).

## STATUS M0-M5 (2026-08-31)

M0 through M4 are implemented and driven by real clients against a real broker,
a real facade and a real Postgres with nothing faked in between. M5 is PARTIAL:
the client matrix and the divergence register are published and the embedded
mode is proven, the differential run against real AWS is not run and the
`crates/queen-facade` extraction is not started. Nothing is committed; the crate
has never run in CI. `queen-sqs/src` is 43,385 lines over 29 files,
`queen-sqs/compat` 15,217 (11,579 of suite code, 2,410 of markdown), and
`server/src/sqs_facade.rs` 1,010.

**The one sentence the whole design protects held, and it was not asserted into
existence.** Nothing durable is in the process: the queue and topic registry,
the subscriptions, the out-of-order delete-sets, the receive-attempt records,
the move-task progress and the purge cooldowns are all Queen KV under the
reserved `qs:` prefix (`qs:q:`, `qs:t:`, `qs:s:`, `qs:ds:`, `qs:rra:`,
`qs:mv:`/`qs:mvf:`, `qs:purge:`, `qs:qdel:`), and a receipt handle is a
self-contained HMAC-tagged blob rather than a row somebody has to look up. No
suite in the live run needed a sticky client, and the embedded row proves the
same binary answers with nobody having started it.

**Forty actions, and the set is CLOSED** — 23 SQS, 17 SNS, resolved to an enum
in exactly one place, so a name outside the set is `InvalidAction` and never
something plausible: "plausible" for a client that asked for `AddPermission`
means it believes its policy was applied. The error catalog is 36 codes rendered
in both protocols with AWS's own status and BOTH of AWS's spellings, and the
second spelling is not decoration — see the go row below.

### What shipped, per milestone

**M0 — the listener, both codecs, SigV4, queue CRUD, the message path.** One
port sniffs the protocol per request; SigV4 is verified in-house against vectors
produced by botocore itself rather than inferred from prose, header and
presigned variants both, and there is no AWS crate anywhere in the build. The
MD5 trio is computed from AWS's own length-prefixed binary encoding, which is
correctness and not decoration: aws-sdk-go-v2 recomputes `MD5OfMessageBody` on
send and `MD5OfBody` on every receive and fails the CALL on a mismatch. Exit
criterion met — aws CLI 12/0, boto3 109 passed and 1 failed, that one being
QS-01 below.

The M0 live run found the defect worth naming, and it was the highest-traffic
call shape in the whole surface: **`CreateQueue` on an existing queue naming NO
attributes was refused `QueueAlreadyExists`**, because the comparison reported a
difference in both directions, so every attribute the first create set became a
conflict for every later create that did not repeat it. That is the idempotent
create Celery, sqs-consumer, ActiveJob and Spring Cloud AWS all perform at
worker startup against a queue Terraform made with non-default attributes; under
that behaviour none of them boot. The comparison is now one-directional — only
the attributes the REQUEST supplies, against the queue's EFFECTIVE attributes,
which is what `GetQueueAttributes` answers — and the invariant "what the read
answers, the create accepts back" is a test rather than a hope
(`compat/M0_SMOKE.md` D1).

**M1 — lifecycle.** ChangeMessageVisibility and its batch, long poll,
per-message `DelaySeconds` on the timers API, tags, the full attribute set
including the depth counters KEDA and every autoscaler read. `PurgeQueue`
shipped as D3 says (delete-and-recreate, the 60-second cooldown emulated) and it
is SYNCHRONOUS where AWS answers at once and empties behind you — deliberately,
because a purge that returned early would leave the queue serving receives for
messages it has told the client are gone, with no task record for anyone to poll
(QS-17). C-SQS-1 was not taken, so `QUEEN_SQS_RECEIVE_MODE=amortized` **is
refused at boot** — a FATAL naming C-SQS-1 and exit code 1, pinned by two tests
— rather than accepted and served as `exact` under another mode's name. Exit
criterion met — sqs-consumer 21/0, Celery over kombu 37/0.

**M2 — FIFO, natively.** `MessageGroupId` is a Queen partition, so ordering
within a group, group-blocked-while-in-flight and deduplication are the broker's
own properties and not an emulation; `MessageDeduplicationId` (or the SHA-256 of
the body under `ContentBasedDeduplication`) is the push's `transactionId`, the
window is AWS's five minutes and `queen.dedupWindowSeconds` widens it.
`SequenceNumber` on the send is the C1 absolute offset; on the RECEIVE it needed
C-SQS-3 (below) and without that broker it is absent rather than wrong (QS-10).
Out-of-order deletes are a per-(partition, lease) delete-set in `qs:ds:` with
the contiguous prefix acked, `ReceiveRequestAttemptId` records in `qs:rra:`.

**M3 — DLQ.** A message past `maxReceiveCount` is not returned, it is MOVED:
push-to-DLQ and ack-original in ONE `POST /api/v1/transaction`, so a redrive can
neither duplicate nor lose. `ListDeadLetterSourceQueues` and the
`StartMessageMoveTask` / `Cancel` / `List` trio work, rate cap included. Two
facts both the go and js lanes pin on two independent redrives: the copy carries
a NEW `MessageId` (QS-23, accepted, with the original in the
`queen.originalMessageId` attribute and the source in `queen.sourceQueue`), and
**the receive count CONTINUES rather than restarting** — `maxReceiveCount=2`
puts a copy on the DLQ reporting `ApproximateReceiveCount=3`, which is AWS's
behaviour and what a redrive dashboard reads.

**M4 — SNS core.** Topics, subscriptions, filter policies in both scopes, raw
delivery, FIFO topics, `Publish` and `PublishBatch`. One publish is one
`POST /api/v1/transaction` bundling a push per matched subscription — atomic
fan-out, which is stronger than SNS itself promises; a `PublishBatch` is one
transaction PER ENTRY, because SNS's own contract is per-entry results and
bundling ten would let one entry's refusal roll back nine messages the client
was told nothing about. **Its exit criterion was not run.** MassTransit needs a
.NET toolchain and this host has none (`dotnet not found`), so the framework
that auto-creates SNS+SQS topologies is unproven. What stood in for it is the
boto3 SNS+SQS suite at 92/0 plus the SNS halves of the go (33 Query requests)
and js (66) rows — three clients over SNS's Query/XML codec, none of them the
framework the plan asked for.

**M5 — partial.** Published: `compat/MATRIX.md` (the matrix and the run of
record), `compat/DIVERGENCES.md` (45 rows — 21 deliberate, 23 accepted, 1 OPEN,
counted off the register itself — each with its classification, the sentence
that must travel with it, and the test or live assertion that fails if it
moves), `queen-sqs/README.md`, and the
two live write-ups `compat/M0_SMOKE.md` and `compat/M4_SMOKE.md`. Not done: the
differential run against real AWS, the `crates/queen-facade` extraction, the
webdoc pages and `gen-sqs-actions.mjs`, the compare.mdx reframe.

**Two server-side changes, and one of them this plan did not name.**

- **C-SQS-3 (new): the broker's pop render emits `offset` per message.** It is
  what makes `SequenceNumber` answerable on a RECEIVE and not only on a send;
  the go and js lanes assert it on both. `server/target/debug/queen` carries it
  and `rig.sh` deliberately does not rebuild `server/`, because it does not own
  it.
- **Embedded mode is `server/src/sqs_facade.rs`, a deliberate TWIN of
  `kafka_facade.rs` and not the generalization proposed under "Core changes"
  below.** The recorded reason: the two supervise different binaries with
  different boot-time preconditions, and a change to one must be a decision
  about that one. So the facade-supervisor generalization is a decision reversed,
  not a debt outstanding. Proven live: the broker named the child and the address
  it derived (`queen_url_from="loopback (bound listener)"` — the listener the
  parent actually bound, not a configured guess), `lsof` confirmed the child held
  `:9324` and its parent was the broker, boto3 ran the full CRUD+message path
  through it 9/0, and one SIGTERM stopped both and left no orphan.

### The gate (2026-08-31)

| Gate | Command | Verdict |
| --- | --- | --- |
| crate tests | `cargo test --manifest-path queen-sqs/Cargo.toml` | **715 passed, 0 failed** (706 lib + 9 bin; 1 doc-test ignored) |
| clippy | `--all-targets -- -D warnings` | clean, 0 warnings (forced fresh, not a cache hit) |
| rustfmt | `--check` | clean, no diff |
| server lib | `cargo test --manifest-path server/Cargo.toml --lib` | 588 tests: 581 passed, 0 failed, 7 ignored — no failures at all, so nothing to report as pre-existing |

### The live matrix

One rig instance for every row: `compat/rig.sh up`, all suites in sequence
against it, `compat/rig.sh down`. Throwaway Postgres 16 on `127.0.0.1:55440`,
the debug broker on `:26632`, the debug facade on `:19324`, SigV4 on, region
`queen-1`, account `000000000000`, `QUEEN_SQS_DEFAULT_PARTITIONS=8`,
`QUEEN_SQS_RECEIVE_MODE=exact`.

| Client | Protocol spoken (measured) | Result | Assertions |
| --- | --- | --- | --- |
| boto3 1.43.83 (SQS) | AWS JSON 1.0, 161 requests | **PARTIAL** | 109 passed, 1 failed |
| boto3 (SNS + SQS) | SNS Query/XML 86 req; SQS JSON 1.0 63 | PASS | 92 / 0 |
| aws CLI 1.46.1 | AWS JSON 1.0 | PASS | 12 / 0 |
| hand-rolled Query/XML over `urllib`, SigV4 in-file | Query/XML, 97 req, 97 `text/xml`, `X-Amz-Target` on 0 of 97 | PASS | 127 / 0 |
| Celery 5.6.3 over kombu 5.6.2 | AWS JSON 1.0, 49 requests | PASS | 37 / 0 |
| aws-sdk-go-v2 (sqs v1.48.1, sns v1.44.1) | SQS JSON 1.0 219 req; SNS Query/XML 33 | PASS | 239 / 0 |
| @aws-sdk/client-sqs + client-sns 3.1121.0 | SQS JSON 1.0 359 req; SNS Query/XML 66 | PASS | 302 / 0 |
| sqs-consumer 11.6.0 | AWS JSON 1.0, 98 requests | PASS | 21 / 0 |
| boto3 vs a facade the BROKER spawned | AWS JSON 1.0, 6 requests | PASS | 9 / 0 |

**928 assertions, 1 failure**, no panic, no 5xx and no `InternalError` in either
log for the whole run. No row is FAIL. The sqs-consumer row's 21 are inside the
302 above it (it was re-run alone so the worker loop has a result of its own)
and are not counted twice.

**The PARTIAL is not a boto3 weakness.** QS-01 is met equally by every client in
the table; what differs is that `smoke_m0.py` asserts it head-on while the other
suites drain through receive-delete-repeat, which is what a real consumer does.
It is written that way on purpose: the register's one open question is wired to
a failing suite line, so closing it turns a suite green instead of quietly
changing a count.

**Every protocol line is a measurement, per the suite contract** — a botocore
`before-send` handler for the python rows (per service, since two clients share
one listener), the `botocore.endpoint` logger in both the suite and the worker
subprocess for Celery, smithy-go's request dump plus the SigV4 credential SCOPE
(the only way to tell an SNS request from an SQS one on one port) for go, a
`finalizeRequest` middleware for js. `query_conformance.py` additionally asserts
`X-Amz-Target` was sent on 0 of 97 requests, which is the guard that stops that
lane drifting onto the JSON codec: botocore's SQS model no longer carries
`query` in its `protocols`, so it is the only live SQS client of ~1,700 lines of
`src/proto/query.rs` and `src/proto/xml.rs` in this whole run.

One client fact worth carrying, found by fourteen red go assertions that were
the SUITE being wrong: every generated `awsAwsjson10_deserializeError*` in
`service/sqs` overrides `ErrorCode()` with the `x-amzn-query-error` header, so a
Go program switching on the string sees
`AWS.SimpleQueueService.NonExistentQueue` where `errors.As` sees
`QueueDoesNotExist` — against real AWS exactly as much as here. `error.rs` sends
both spellings and sends them the right way round; the suite now pins both.

### Known open, in the order they matter

- **QS-01, the only OPEN row on the register: a standard queue's concurrent
  in-flight ceiling is its partition width, not SQS's 120,000.** A receive is N
  parallel `batch=1` pops and a lane with a live claim serves no second pop.
  Measured, 10 sent and read without deleting: `queen.partitions` 1 → 1 in
  flight, 8 → 7, 64 → 10, 256 → 10. Nothing is lost or duplicated, every message
  is eventually receivable, and the depth attributes account for all of them, so
  KEDA still sees the blocked messages as work — what is missing is CONCURRENCY,
  not messages. The guidance that must travel with it: *size `queen.partitions`
  at or above the number of messages you intend to have in flight at once; the
  default 64 is invisible at ten messages and bites at a few hundred, and a
  consumer holding a message blocks the other messages in that lane for a full
  visibility timeout.* It interacts with M3: a message stuck behind a slow
  neighbour ages toward `maxReceiveCount` without ever being delivered. Three
  candidate answers are recorded (raise the default and document; take C-SQS-1;
  leave it and say so) and **none is taken**.
- **C-SQS-1 is not taken, and `amortized` refuses to boot** rather than
  pretending. When it lands and the refusal is deleted, that mode adds exactly
  two register rows, both inside SQS's own at-least-once envelope: extending one
  message's visibility extends its pop-mates, and terminating one returns the
  others as duplicates. It is also the lever that changes QS-01 and QS-02, which
  is the point of taking it.
- **The differential run against REAL AWS SQS/SNS has not been run**, and that
  is the release gate for "zero unexplained divergences". It needs a dedicated
  AWS account and stays a manual, gated lane by design (the proxyimpr lesson);
  ElasticMQ is allowed as a fast oracle and never as ground truth. Six questions
  wait on it (`DIVERGENCES.md` Q1-Q6), each settled in one or two calls: whether
  a deduplicated SNS `Publish` answers the original id (this run measured BOTH
  sides of that asymmetry — SQS `SendMessage` answers the original, SNS
  `Publish` mints a fresh one, so the question is now narrowed to the SNS half
  alone); whether real `CreateQueue` compares only supplied attributes and
  whether it compares TAGS; what a repeat `Subscribe` with different attributes
  does; whether a bare `.fifo` re-create is accepted on the topic side; whether
  a standard topic really accepts a FIFO queue subscription and which group id
  it picks; and whether FIFO `SequenceNumber` is queue-unique in practice.
- **M6, SNS over HTTP/S via queen-relay, is not started**, and D4 (SNS-compatible
  RSA with a hosted cert vs Standard Webhooks) is therefore still undecided.
  v0 sidesteps it: `sqs` is the only subscription protocol, everything else is
  refused (SN-16), and `ConfirmSubscription` can never succeed and says why
  (SN-09).
- **Nine client lanes the plan names were not run.** aws-sdk-net/MassTransit is
  absent because there is no .NET toolchain on this host, which is also M4's
  unmet exit criterion; also missing are aws-sdk-java v2/Spring Cloud AWS,
  aws-sdk-php/Laravel, async-aws/Symfony Messenger, aws-sdk-rust, Shoryuken,
  Terraform and the KEDA scaler read. **And no OLD SDK major in any language**:
  every row above is a current major and every current major speaks JSON for
  SQS, so the Query codec's only live SQS client in this run is one this
  repository wrote. async-aws is the sharpest of the absences, for the reason in
  the next bullet.
- **A Query message action addressed by PATH alone is refused
  `MissingParameter`, and it is not on the register.** AWS's own Query
  documentation posts to the queue URL with `Action=…` and no `QueueUrl`
  parameter; `queue_of` reads the parameter only. Dormant today — no client in
  the matrix builds that request — and it wakes for a client pinned to an old SDK
  major, for async-aws if it serializes that way, and for anyone following AWS's
  own examples with curl. The fix is small (the path segment is already parsed
  for the account check, so fall back to it when the parameter is absent) and it
  is a decision rather than a defect. Pinned as
  `Errors.queue_addressed_by_path_only_is_refused_today`, which records today's
  behaviour so a change is loud either way.
- **The Cloud shape has not been run.** Every row above points the facade at a
  bare broker. Pointing `QUEEN_URL` at a cell's proxy is where authentication,
  tenant scoping, quotas and metering enter, and the structural reason to expect
  it to work — SQS consumes through pop/ack, so there is no new read arm and no
  kafka-shaped fetch-billing gap — is an argument, not a measurement.
  queen-kafka has that row; this one does not.
- **`crates/queen-facade` is not extracted.** D5 said extract on the third
  facade and the third facade now exists, so `queen.rs` (6,215 lines here) and
  queen-kafka's are two implementations of one client with no test proving
  parity between them.
- **No webdoc, no CI, no image, nothing committed.** The reference pages and
  `gen-sqs-actions.mjs` are unwritten, the crate is out of release CI on purpose
  until the compat lanes are boring, and the repository `Dockerfile` builds the
  binary beside the broker but no published image carries it.

## Semantics (decided)

**The one real mismatch and its resolution.** Queen's durable lease is a per-(partition,
consumer-group) claim over a contiguous offset span with a monotonic ack cursor
(`001_log_schema.sql:217`, `005_log_ack.sql:16`); SQS visibility is per message. The
models coincide at claim width 1, so:

- **A standard queue is M synthesized partitions**, decimal-named `"0".."M-1"` (the
  kafka precedent: broker's `PartitionName` already accepts string-or-number). M default
  64, set at CreateQueue via attribute `queen.partitions`, immutable after (partition
  counts never shrink; document). Sends without a group hash `MessageId` across lanes.
  Consumer group: the queue-mode default (`__QUEUE_MODE__`) — SQS has no groups.
- **ReceiveMessage (standard, v0) = up to N parallel `batch=1` pops** (N =
  `MaxNumberOfMessages` ≤ 10, `leaseSeconds` = effective visibility, `wait/timeout` =
  long poll; SQS caps wait at 20s, pop allows it). Claim width 1 makes every SQS verb
  exact: `ChangeMessageVisibility` = `POST /lease/:id/extend` (that lease holds one
  message), terminate (=0) = ack status `retry` (releases one message, charges nothing),
  `DeleteMessage` = ack `completed` with no gap to swallow. Cost: one pop write-tx per
  message. Honest — SQS is a chatty ≤10-batch protocol and its own clients poll.
- **The batching dial (M1, gated on C-SQS-1):** one pop claiming k partitions capped at
  1 message each restores single-write-tx amortization. Bounded divergences appear —
  extending one message extends its pop-mates (invisible to others), terminating one
  returns the others as duplicates — both inside SQS's own at-least-once envelope.
  Config `QUEEN_SQS_RECEIVE_MODE=exact|amortized`, default `exact`. Never silently.
- **FIFO queues are native.** `.fifo` suffix → `MessageGroupId` = partition name,
  group-blocked-while-in-flight = the partition claim, delivery in order, batch pops per
  group are the natural shape. `MessageDeduplicationId` (or SHA-256 of body when
  `ContentBasedDeduplication`) = `transactionId`; queue `dedupWindowSeconds=300` at
  create for parity, attribute `queen.dedupWindowSeconds` to widen (the superset we
  sell). `SequenceNumber` = the C1 absolute offset already on push results
  (`fusion.rs:60`). Per-message `DelaySeconds` on FIFO errors exactly as AWS errors.
- **Out-of-order deletes.** Standard/exact: impossible by construction. FIFO batch (and
  amortized standard): facade keeps a per-(partition, leaseId) delete-set and acks the
  contiguous prefix; set lives in KV (`qs:ds:` keys, TTL = lease + slack) so any facade
  instance can serve the delete — never in memory. If an earlier message expires
  undeleted, the suffix redelivers and the facade auto-acks members of the recorded
  delete-set on the way through. Residual duplicates classified `accepted`.
- **Redrive is facade-driven, and the DLQ is a real queue.** SQS-created queues get
  `deadLetterQueue=false` (native log_dlq stays out of the SQS path). On pop, if
  `deliveryAttempt` > `maxReceiveCount` from the queue's `RedrivePolicy`, the facade
  does not return the message: it moves it — push-to-DLQ-queue + ack-original in ONE
  `POST /api/v1/transaction` (atomic, exactly-once, fresh `transactionId` so dedup
  cannot swallow the move; original MessageId rides in the envelope).
  `ListDeadLetterSourceQueues` reads the registry. `StartMessageMoveTask` /
  `Cancel` / `List` = the same move in reverse, a facade loop with progress under
  `qs:mv:`, rate-capped by `MaxNumberOfMessagesPerSecond`.
- **Per-message `DelaySeconds` (standard, 0..900)** = the timers API: `timerKey` =
  MessageId, payload = the envelope base64'd, 90-day horizon dwarfs 15 min. Queue
  default `DelaySeconds` = native `delayedProcessing`. `ApproximateNumberOfMessagesDelayed`
  = `GET /api/v1/timers/:queue` count.
- **PurgeQueue** = facade delete-and-recreate (re-applying registry attributes),
  answered async like AWS (60s cooldown emulated). Escalate to C-SQS-2 only if the
  race window bites in practice.
- **Depth attributes**: `ApproximateNumberOfMessages` = `/resources/queues/:q/depth`
  for the queue-mode group; `NotVisible` = in-flight from queue detail/stats. KEDA and
  autoscalers read these; they are load-bearing, not decoration.

**Envelope** (the records.rs idea, new shape — bodies are strings, so no mandatory
base64):

```
{"b": "<body string>",                    // always present
 "a": {"name": {"t": "String|Number|Binary[.custom]", "v": "<string or b64>"}},  // omitted when empty
 "s": {...},                              // system attributes (AWSTraceHeader), omitted when empty
 "m": "<original MessageId>"}             // only on moved (redriven) copies
```

Strict-shape recognition with fallback: a payload that is not this envelope (a native
Queen producer's JSON) is served as body = the payload's own text, no attributes — mixed
native/SQS consumption works both ways. The one acknowledged collision: a native payload
that happens to be `{"b": ...}` shaped.

**Receipt handle** = base64url of `{q, partition, transactionId, leaseId,
deliveryAttempt, exp}` + HMAC tag (key = facade secret, `hmac`/`sha2` already in deps).
Self-contained → any instance serves the delete → the stateless sentence holds. A handle
from a previous delivery of the same message fails on `leaseId` mismatch →
`ReceiptHandleIsInvalid`/`MessageNotInflight`, matching AWS's contract.

**MessageIds** are the broker's message uuid. MD5OfMessageBody / MD5OfMessageAttributes /
MD5OfMessageSystemAttributes computed exactly per the AWS algorithm (attributes MD5 has
its own binary encoding: name-len+name, type-len+type, transport byte, value-len+value).
boto3 validates these client-side; they are correctness, not decoration. Payload ceiling:
per-queue `MaximumMessageSize`, default 262144, max 1048576 (AWS raised to 1 MiB
2025-08); body charset = the documented XML-char restriction, enforced.

## Wire protocols (decided)

Both, sniffed per request on one listener (default **:9324**, the de-facto self-hosted
SQS port):

- **AWS JSON 1.0**: `Content-Type: application/x-amz-json-1.0`, `X-Amz-Target:
  AmazonSQS.<Action>`. What every SDK major since late 2023 speaks.
- **Query/XML**: form-encoded `Action=...&Version=2012-11-05`, XML responses. Older SDK
  majors, async-aws — and ALL of SNS (`Version=2010-03-31`), which never moved to JSON.

One internal action layer, two codecs. Error catalog in both renderings (JSON `__type` +
`x-amzn-query-error` header for compat; XML `<ErrorResponse>`): `QueueDoesNotExist`,
`ReceiptHandleIsInvalid`, `MessageNotInflight`, `PurgeQueueInProgress`,
`BatchEntryIdsNotDistinct`, `TooManyEntriesInBatchRequest`, `InvalidAttributeName`,
`QueueDeletedRecently` (emulate the 60s window — SDK retry behavior depends on it),
`QueueAlreadyExists` (only on attribute mismatch, per AWS). ERRORS.md-style closed set,
same discipline as kafka: the file is the contract, a new code is a reviewed event.

Batch actions (10-entry cap, `BatchResultErrorEntry` per-entry partial failure) map to
the broker's per-entry result arrays directly.

## Auth (decided)

SigV4 verification, in-house (~1–1.2k lines + AWS test vectors). The S3-archive plan's
signer was never written; this is new code in the harder direction: reconstruct the
canonical request from bytes the CLIENT chose. Header variant and presigned-query
variant, clock-skew window, constant-time compare, `UNSIGNED-PAYLOAD` accepted, standard
double URI-encoding (S3's single-encoding quirk does not apply). No AWS crates — the
ruling is inherited (`hmac`, `sha2`, `hex`, `base64` in-tree).

Credential model — the sasl.rs "no credential store" spirit, adapted because SigV4 is
SCRAM-shaped (the secret never crosses the wire, the verifier must hold it):

- OSS: static triples from env, `QUEEN_SQS_CREDENTIALS=akid:secret:queen_token[,...]`.
  Operator config, not a directory — the MinIO model. The queen_token is what the facade
  presents upstream for that principal.
- Dev: `QUEEN_SQS_AUTH=off` accepts anything (ElasticMQ parity; boto3 still wants dummy
  values configured, which is its normal fake-endpoint workflow).
- Cloud (later, not in this plan's milestones): per-tenant keypairs live in the PROXY,
  facade asks an introspection endpoint by access-key-id and caches. The facade itself
  never grows a directory.

Tenancy composes as kafka's: facade strips inbound `x-queen-tenant`, stamps its own when
configured, `KV_TRUSTED_PROXY` interlock unchanged. The account-id segment of queue URLs
(`http://host:9324/<account>/<name>`) and ARNs
(`arn:aws:sqs:<region>:<account>:<name>`) is config (`QUEEN_SQS_REGION`,
`QUEEN_SQS_ACCOUNT`, defaults `queen-1`/`000000000000`) and doubles as the tenant
carrier in Cloud.

## SNS (decided)

A facade-level construct; the broker needs nothing.

- Registry in KV under `qs:` (CAS on admin mutations, `forever` like offsets, kafka's
  escaping): `qs:t:<topic>` (attributes, FIFO flag), `qs:s:<topic>:<sub-id>` (protocol,
  endpoint/queue, RawMessageDelivery, FilterPolicy + scope), `qs:q:<queue>` is the SQS
  queue registry itself (attributes, tags, RedrivePolicy, created).
- **Publish / PublishBatch = one `POST /api/v1/transaction`** bundling one push per
  matched subscription — atomic fanout, stronger than SNS promises. FilterPolicy
  (MessageAttributes and MessageBody scope) evaluated in the facade at publish. Raw vs
  enveloped (`{"Type":"Notification","MessageId",...}`) applied per subscription when
  building each push payload. FIFO topics = FIFO queue fanout, same group ids.
- v0 scope: **SQS-queue subscriptions only.** That is MassTransit and JustSaying, the
  two frameworks that auto-create SNS+SQS topologies and therefore the two best
  end-to-end tests. HTTP/S subscriptions are M6, delegated to queen-relay (retry
  ladder, circuit breaker, SSRF guard already built there); what M6 adds on top and
  never inside relay's core: the SubscriptionConfirmation handshake as destination
  lifecycle state, the envelope applied at publish, and the D4 signature decision.

## Architecture (decided)

Separate crate + binary `queen-sqs/` (no workspace root exists; own manifest,
`publish = false`, kafka pattern). axum listener — the facade is plain HTTP, no framing
loop. Talks to the broker as an ordinary HTTP client. In-memory state: none that anyone
would miss — caches only (credential→identity, registry snapshot with TTL + CAS-checked
writes). Everything durable is broker KV. Restart is free; horizontal scale is a
Deployment.

Reuse lifts from queen-kafka (D5 resolved: **extract**, a third facade is plausible):
`crates/queen-facade` gets `QueenApi` + `HttpQueen` + `Catalog` (queen.rs, 4.1k — add
pop/ack/lease/timers/transaction verbs), `FakeQueen` (~1k), identity.rs + secret.rs
(~870). queen-kafka is repointed at the crate in the same change, tests prove parity.
`server/src/kafka_facade.rs` generalizes to a named-facade supervisor (it is ~90%
agnostic already); status arm becomes a map. Embedded: `QUEEN_SQS_EMBEDDED=true`, same
Dockerfile adjacency contract, same STRIPPED_ENV discipline.

## Core changes (server/, additive, only these)

- **C-SQS-1 (optional, unlocks `amortized` receive): `maxPerPartition` on pop** — claim
  up to `partitions` lanes, cap frames per lane. One write-tx, per-message claims
  preserved. Additive param, absent = today's behavior, byte-identical output otherwise.
- **C-SQS-2 (only if delete-and-recreate races bite): queue truncate endpoint.** Not
  scheduled; a decision record, not a milestone.
- Facade supervisor generalization (kafka_facade.rs → facades keyed by name) — refactor,
  no behavior change for kafka.

Nothing else. No new read arm (SQS consumes through pop/ack — which is also why proxy
metering works day one, unlike the kafka fetch-billing gap at `proxy/src/routes.rs:199`).

## Milestones

- **M0 — skeleton + standard queues.** Listener, both codecs, error catalog, SigV4
  verify + env credentials + auth-off, queue CRUD (Create/Delete/GetQueueUrl/List/
  Get-SetQueueAttributes, registry in KV), Send/Receive/Delete (+Batch) via exact-mode
  pops, MD5 fields, receipt handles. Exit: aws CLI and boto3 green end-to-end.
- **M1 — lifecycle fidelity.** ChangeMessageVisibility (+Batch), long poll, per-message
  DelaySeconds via timers, PurgeQueue (+cooldown), tags, full attribute set, depth
  attributes. C-SQS-1 lands here if taken; `amortized` mode behind config. Exit:
  sqs-consumer and Celery green.
- **M2 — FIFO.** `.fifo` queues, group ids, dedup (id + content-based), SequenceNumber,
  delete-set prefix buffering, ReceiveRequestAttemptId (KV, TTL = visibility). Exit:
  FIFO conformance corpus green.
- **M3 — DLQ.** RedrivePolicy parsing, facade redrive via atomic moves,
  ListDeadLetterSourceQueues, MessageMoveTask trio. Exit: redrive corpus green,
  including move-back-under-load.
- **M4 — SNS core.** Topic/subscription registry, Publish/PublishBatch transaction
  fanout, FilterPolicy both scopes, raw delivery, FIFO topics. Exit: MassTransit
  topology roundtrip green (auto-create topics, subscribe queues, pub/sub, fault path).
- **M5 — hardening + ship surface.** Client matrix (pinned old+new SDK majors per
  language so BOTH protocols stay exercised), differential vs real AWS, embedded mode,
  `crates/queen-facade` extraction, webdoc pages + `gen-sqs-actions.mjs` (fingerprint
  harness), compare.mdx reframe (SQS: from "the other choice" to "an interface Queen
  speaks"). Exit: matrix report published, zero unexplained divergences.
- **M6 (optional, demand-gated) — SNS over HTTP/S via queen-relay.** Handshake,
  envelope, D4 signature decision, `x-amz-sns-*` headers.

## Testing (how to run it)

Same skeleton as kafka's `compat/`:

- `queen-sqs/compat/rig.sh` — throwaway PG + debug broker + facade; authgate reused
  as-is for credential-refusal proof.
- `compat/differential/` — same scenarios against the facade and against REAL AWS
  (dedicated account, gated manual lane, never default CI — the proxyimpr lesson).
  ElasticMQ allowed only as a fast smoke oracle, never as ground truth. Every
  divergence classified in `DIVERGENCES.md`; zero unexplained is a release gate.
- Conformance corpus (facade unit level, runs in CI): SigV4 vectors (header, presigned,
  skew, tampered), MD5 vectors (body, attributes incl. Binary and custom types),
  receipt-handle lifecycle (stale handle, re-receive, forged tag), visibility races,
  batch partial failures, FIFO group blocking + dedup windows + attempt-id, redrive
  loops, both codec renderings of every error.
- Client matrix rigs: boto3, aws-sdk-go-v2, @aws-sdk/client-sqs+sns, aws-sdk-java v2,
  aws-sdk-php, aws-sdk-net, aws-sdk-rust, aws CLI; framework lanes: Laravel
  `queue:work` (stock sqs driver), Celery worker, Symfony Messenger (async-aws =
  Query-protocol coverage), Spring Cloud AWS listener, MassTransit, sqs-consumer,
  Shoryuken, Terraform plan/apply, KEDA scaler read. Suite contract copied verbatim
  from CLIENT_MATRIX.md: stack from env, one ok/FAIL line per assertion, RESULT line,
  nonzero exit, and each rig reports which protocol its client actually spoke, read
  from the client's own debug stream, never assumed.
- CI: unit + conformance in a `sqs-facade` job (own cell, kafka precedent); compat and
  differential lanes stay out until they are boring.

## Config surface (proposed)

```
QUEEN_SQS_LISTEN=0.0.0.0:9324        QUEEN_SQS_AUTH=sigv4|off
QUEEN_SQS_CREDENTIALS=akid:secret:token[,...]
QUEEN_SQS_REGION=queen-1             QUEEN_SQS_ACCOUNT=000000000000
QUEEN_SQS_RECEIVE_MODE=exact|amortized   (amortized requires broker with C-SQS-1)
QUEEN_SQS_DEFAULT_PARTITIONS=64      QUEEN_SQS_HANDLE_SECRET=<hmac key, else random>
QUEEN_URL / QUEEN_TOKEN              (as kafka)
QUEEN_SQS_EMBEDDED=true|false        QUEEN_SQS_BIN / QUEEN_SQS_SHUTDOWN_GRACE_MS
QUEEN_SQS_TLS_CERT / _KEY            (SDKs are happy on plain HTTP for custom endpoints)
```

## Open decisions (the rest are resolved above)

- **D3** PurgeQueue: shipped as delete-and-recreate; C-SQS-2 stays a recorded option.
- **D4** SNS HTTP signature format (M6 only): SNS-compatible RSA + hosted cert vs
  Standard Webhooks via relay. Decide when M6 is real; v0 sidesteps it.

## Later, deliberately out of this plan

Fair queues (MessageGroupId on standard queues, AWS 2025), extended-client S3 payload
pointers, KMS/SSE attribute semantics beyond accept-and-report, JMS bridge,
cross-account anything, CloudWatch-shaped metrics export (the console and /status
already tell the truth), and Cloud per-tenant key directory (proxy work, its own plan).
