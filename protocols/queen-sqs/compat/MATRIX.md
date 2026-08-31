# queen-sqs client matrix

Which SQS/SNS clients have been run against the facade, what each one spoke, and
what each one found. Every row is a real client against a real facade, a real
broker and a real Postgres, with nothing faked in between, and every row has a
suite in this directory that can be re-run.

Read it with [DIVERGENCES.md](DIVERGENCES.md) (the classified register of every
place this facade is not AWS) and with `PLAN_QUEEN_SQS.md`. A divergence on that
register is a decision, not a defect, and this file says so where a row meets
one.

## The run of record

**2026-08-31**, one rig instance for every row: `compat/rig.sh up`, all suites in
sequence against it, `compat/rig.sh down`. Throwaway Postgres 16 on
`127.0.0.1:55440`, `server/target/debug/queen` on `:26632`,
`protocols/queen-sqs/target/debug/queen-sqs` on `:19324`, SigV4 on, region `queen-1`,
account `000000000000`, `QUEEN_SQS_DEFAULT_PARTITIONS=8`,
`QUEEN_SQS_RECEIVE_MODE=exact`. `queen-sqs` was rebuilt from the working tree
immediately before the run (`cargo build --manifest-path protocols/queen-sqs/Cargo.toml`,
which `rig.sh up` then does again unconditionally); the broker binary is the one
already carrying **C-SQS-3** — `offset` on the pop render — and `rig.sh`
deliberately does not rebuild `server/`, because it does not own it. C-SQS-3 is
what the `SequenceNumber`-on-RECEIVE assertions in the go and js rows exercise;
without it they would be asserting an absence ([QS-10](DIVERGENCES.md#qs-10)).

**928 assertions, 1 failure**, and the failure is [QS-01](DIVERGENCES.md#qs-01) —
the one **OPEN** row on the register — asserted deliberately so that closing it
would turn a suite green rather than quietly change a number.

Neither log recorded a panic, a 5xx or an `InternalError` for the whole run.

## The matrix

| Client | Versions tested | Protocol spoken (measured) | Result | Assertions | Mandatory config | Notes | Suite |
| --- | --- | --- | --- | --- | --- | --- | --- |
| boto3 (SQS) | boto3 1.43.83, botocore 1.43.83, CPython 3.12.8 | AWS JSON 1.0 — 161 requests | **PARTIAL** | 109 passed, 1 failed | `endpoint_url` only | the one red assertion in the whole run: [QS-01](DIVERGENCES.md#qs-01), asserted here on purpose. botocore validates NO MD5 for SQS, so this suite computes all three itself | [`smoke_m0.py`](smoke_m0.py) |
| boto3 (SNS + SQS) | boto3 1.43.83, botocore 1.43.83 | SNS Query/XML — 86 requests; SQS AWS JSON 1.0 — 63 | **PASS** | 92 passed, 0 failed | `endpoint_url` only | two clients, one listener, two codecs, two SigV4 scopes; a deduplicated FIFO publish answers a FRESH MessageId ([SN-01](DIVERGENCES.md#sn-01)) | [`smoke_m4_sns.py`](smoke_m4_sns.py) |
| aws CLI | aws-cli 1.46.1, botocore 1.43.62 | AWS JSON 1.0 | **PASS** | 12 passed, 0 failed | `--endpoint-url` only | the CLI's own attribute shorthand is a second encoder, and it survives | [`smoke_m0_cli.sh`](smoke_m0_cli.sh) |
| none — hand-rolled Query/XML over `urllib`, SigV4 signed in-file | CPython 3.12.8, standard library only | Query/XML — 97 requests, 97 `text/xml` answers, `X-Amz-Target` on 0 of 97 | **PASS** | 127 passed, 0 failed | n/a | the ONLY live SQS client of the Query codec in this run; found the path-only addressing refusal (discrepancy 2 below) | [`python/query_conformance.py`](python/query_conformance.py) |
| Celery over kombu's SQS transport | celery 5.6.3, kombu 5.6.2, boto3 1.43.83, pycurl 7.47.0 | AWS JSON 1.0 — 49 requests | **PASS** | 37 passed, 0 failed | a `sqs://akid:secret@host:port` broker URL, `is_secure: false` on a plain rig | both kombu queue-resolution modes; the failing task's redelivery is a real lease expiry, not a `retry()` | [`python/celery_suite.py`](python/celery_suite.py) |
| aws-sdk-go-v2 | `service/sqs` v1.48.1, `service/sns` v1.44.1, `smithy-go` v1.28.1, go 1.25.7 | SQS AWS JSON 1.0 — 219 requests; SNS Query/XML — 33 | **PASS** | 239 passed, 0 failed | a hand-built `aws.Config` (so no developer AWS profile can leak in) | validates `MD5OfMessageBody` ITSELF and fails the call on a mismatch; `ErrorCode()` carries the LEGACY Query code (see below); `WaitTimeSeconds=0` cannot be sent from Go | [`go-sdk/`](go-sdk) |
| @aws-sdk/client-sqs + client-sns | 3.1121.0 / 3.1121.0, node v22.17.0 | SQS AWS JSON 1.0 — 359 requests; SNS Query/XML — 66; 1 more signed with a deliberately bad credential | **PASS** | 302 passed, 0 failed (all five lanes) | `endpoint` only | validates the BODY digest and provably not the two attribute digests; errors arrive as classes, so both of SQS's spellings have to be the right way round | [`js/`](js) |
| sqs-consumer | 11.6.0 (on @aws-sdk/client-sqs 3.1121.0) | AWS JSON 1.0 — 98 requests | **PASS** | 21 passed, 0 failed (lane run on its own) | none beyond the client's | a real worker loop: nacks by `ChangeMessageVisibility`, deletes in MessageId-keyed batches, long-polls back to back | [`js/`](js) `node run.mjs consumer` |
| boto3, against a facade the BROKER spawned | boto3 1.43.83; `QUEEN_SQS_EMBEDDED=true` | AWS JSON 1.0 — 6 requests | **PASS** | 9 passed, 0 failed | `QUEEN_SQS_BIN` + `QUEEN_SQS_CREDENTIALS` (SigV4 has no default keypair, by design) | nobody started the facade; one SIGTERM stopped both and left no orphan | the embedded one-shot, below |

Result meanings, copied from `protocols/queen-kafka/compat/CLIENT_MATRIX.md`: **PASS**, the
client works and every divergence it meets is on the register. **PARTIAL**, the
client works but a real user meets something sharp. **FAIL**, the client cannot
complete the basic path. No row is FAIL.

**Read the PARTIAL honestly.** It is not a boto3 weakness. [QS-01](DIVERGENCES.md#qs-01)
— a standard queue hands out at most one message per lane at a time — is met
equally by every client in the table; what differs is that `smoke_m0.py` asserts
it head-on while the other suites drain through receive-delete-repeat loops (what
a real consumer does) and so never trip over it. If the assertion were moved into
any other suite, that row would be PARTIAL too. It is written this way on purpose:
one suite states the open question as a failing line so that closing it turns a
suite green.

The sqs-consumer row's 21 assertions are also inside the 302 of the row above it
(`node run.mjs all` runs all five lanes); it was re-run alone so that the worker
loop has a result of its own rather than a share of somebody else's. Those 21 are
not counted twice in the 928.

### Every protocol line is a measurement

No row above infers a protocol from a version number. Each suite reads the shape
off the request its own client built, and says how:

* **boto3 and the aws CLI** — a `before-send` handler on botocore's event system
  (`smoke_m0.py::_record_protocol`, and the same recorder added to
  `smoke_m4_sns.py` for this run, keyed per service because that file drives two
  clients at one listener).
* **Celery** — a handler on botocore's own `botocore.endpoint` logger, in the
  suite process AND in the worker subprocess, parsed back out of the file.
* **aws-sdk-go-v2** — `ClientLogMode: aws.LogRequest` and a `logging.Logger` that
  reads smithy-go's request dump; the SERVICE comes from the SigV4 credential
  scope in the request's own `Authorization` header, which is the only way to
  tell an SNS request from an SQS one when both go to one port.
* **@aws-sdk/*** — a middleware at the end of `finalizeRequest`, so it sees the
  SIGNED request.
* **`query_conformance.py`** — it builds the bytes itself, and additionally
  asserts `X-Amz-Target` was sent on **0 of 97** requests, which is the guard
  that stops this lane from silently drifting onto the JSON codec.

The two codecs are exercised by clients that had no choice about it: botocore's
SQS model no longer carries `query` in its `protocols`, so every python and CLI
row speaks JSON whatever it is configured with, and `client-sns`/`service/sns`
resolve Query because SNS never moved. `query_conformance.py` exists because that
leaves ~1,700 lines of `src/proto/query.rs` and `src/proto/xml.rs` with no live
SQS client at all.

## The embedded one-shot

Not a rig: a throwaway Postgres and ONE process. The broker was started with

```
QUEEN_SQS_EMBEDDED=true
QUEEN_SQS_BIN=protocols/queen-sqs/target/debug/queen-sqs
QUEEN_SQS_LISTEN=127.0.0.1:9324   QUEEN_SQS_AUTH=sigv4
QUEEN_SQS_CREDENTIALS=QSQSTEST:qsqssecret:devtoken
QUEEN_SQS_REGION=queen-1   QUEEN_SQS_ACCOUNT=000000000000
QUEEN_SQS_DEFAULT_PARTITIONS=8   QUEEN_SQS_HANDLE_SECRET=…
```

and nothing else started `queen-sqs`. What was observed:

* the broker's own log named the child and the address it derived —
  `sqs: queen-sqs facade started (embedded) pid=51330 queen_url=http://127.0.0.1:26633
  queen_url_from="loopback (bound listener)"`, so the child was pointed at the
  listener the parent had actually bound, not at a configured guess;
* `lsof` confirmed pid 51330 held `127.0.0.1:9324` and that its parent was the
  broker;
* boto3 ran CreateQueue → SendMessage → ReceiveMessage → DeleteMessage →
  DeleteQueue against `:9324`: **9 assertions, 0 failures**, AWS JSON 1.0, and
  the queue URL came back as `http://127.0.0.1:9324/000000000000/emb-<run>`;
* one SIGTERM to the broker stopped both. The child logged its own graceful
  shutdown (`the listener is closed and in-flight requests are finishing`,
  `grace_ms=25000`), the parent logged `queen-sqs facade stopped with the broker
  … how="sigterm"`, **no orphan was left** and `:9324` was free immediately
  after.

## Facade discrepancies found in this run

Four, and only one of them is new.

### 1. A standard queue's in-flight ceiling is its partition count — the one red assertion

Registered: [QS-01](DIVERGENCES.md#qs-01), class **OPEN** (the only OPEN row on
the register). AWS holds 120,000 messages in flight per queue; here a receive is
N parallel `batch=1` pops and a lane with a live claim serves no second pop, so
the ceiling is the queue's partition count and a slow consumer blocks its lane.

Repro, verbatim from the run:

```
$ protocols/queen-sqs/compat/rig.sh up && source protocols/queen-sqs/compat/.rig/env.sh
$ python protocols/queen-sqs/compat/smoke_m0.py
#   partitions=1, 3 sent, 1 in flight at once
FAIL InFlight.three_messages_are_all_receivable_at_once: got 1, want 3
#   depth: visible=2 not-visible=1
ok InFlight.depth_attributes_account_for_every_message
ok InFlight.every_message_is_eventually_receivable
```

Create a queue with `queen.partitions=1`, send three messages, receive without
deleting: one message is in flight and the other two stay visible. The two lines
around the failure are the point — nothing is lost, the depth attributes account
for all three, and every message is delivered eventually; what is missing is
CONCURRENCY, not messages. Every other suite in this tree copes with it by
draining through receive-delete-repeat, or by creating a one-lane queue on
purpose where a test needs "the next receive gets that message".

This is the only assertion in 928 that is red, and it is red on purpose: it is
the register's one open question wired to a suite, so that changing the behaviour
turns a suite green instead of quietly changing a count.

### 2. NEW — a Query message action addressed by PATH alone is `MissingParameter`

**Not on the register.** AWS's own Query documentation addresses a message action
by posting to the queue URL (`POST /<account>/<queue>`) with `Action=…` in the
body and no `QueueUrl` parameter. `queen-sqs` resolves the queue from the
`QueueUrl` **parameter** only (`src/actions/queues.rs::queue_of` →
`require_text(params, "QueueUrl")`), so that request is refused.

Repro:

```
POST /000000000000/<queue> HTTP/1.1
Content-Type: application/x-www-form-urlencoded

Action=SendMessage&Version=2012-11-05&MessageBody=addressed%20by%20path
→ 400  <ErrorResponse><Error><Code>MissingParameter</Code>…
```

Pinned by `python/query_conformance.py` as
`Errors.queue_addressed_by_path_only_is_refused_today` — the assertion records
today's behaviour rather than demanding a fix, so a change to it is loud either
way.

**Dormant today, not harmless.** No client in this matrix builds that request:
botocore dropped `query` from the SQS model, so kombu takes its JSON branch,
which sends `QueueUrl`. It wakes up for a client pinned to an old SDK major, for
async-aws if it serializes the same way (untested — no PHP row yet), and for
anyone following AWS's own Query examples with curl. Worth a decision, and the
decision is cheap: the path segment is already parsed for the account check, so
falling back to it when the parameter is absent is a small change in one
function. Recorded here rather than fixed because this run does not touch `src/`.

### 3. FIFO deduplication answers the ORIGINAL id on the SQS path and a FRESH one on the SNS path

Registered on the SNS side: [SN-01](DIVERGENCES.md#sn-01), class deliberate, with
[Q1](DIVERGENCES.md#q1) as the differential question that would settle it. This
run measured **both** sides of the asymmetry in one afternoon, which the register
had not yet had:

```
# note SQS FIFO dedup MessageId: first=01a055f3-… deduplicated-send=01a055f3-… (same)      ← go-sdk
# note FifoTopic dedup MessageId: first=9dceb7a4-… deduplicated-publish=8e1247f7-… (DIFFERENT) ← smoke_m4_sns.py
```

So SQS `SendMessage` matches what AWS documents (a repeated
`MessageDeduplicationId` answers the original message's id) and SNS `Publish`
does not. Q1 asks whether real SNS also mints a fresh one; whichever way it
falls, the SQS half is now measured and correct, which narrows the question to
the SNS half alone.

### 4. A dead-lettered copy has a new MessageId, and its receive count continues

Registered: [QS-23](DIVERGENCES.md#qs-23), class accepted. The go suite pins the
divergence itself as an assertion rather than leaving it in a comment —
`Divergence.the_dead_letter_copy_has_a_new_message_id` — and both the go and js
lanes pin the compensation: the original id rides in the
`queen.originalMessageId` message attribute and the source queue in
`queen.sourceQueue` (`Redrive.the_copy_names_the_original_message_id`,
`Redrive.the_copy_names_its_source_queue`). The neighbouring fact is the one
worth reading: with `maxReceiveCount=2`, the copy on the DLQ reports
`ApproximateReceiveCount=3` — the count CONTINUES rather than restarting, which
is what AWS does and what a redrive dashboard reads. Both clients assert it, on
two independent redrives.

```
#   dead-letter copy: ApproximateReceiveCount=3
ok Redrive.receive_count_continues_rather_than_restarting
```

## Client behaviours found in this run (not facade discrepancies)

Three, all of which cost a live run to find and two of which were suite defects
this run fixed. They are here because the next person to read a red line in one
of these suites will meet them first.

### aws-sdk-go-v2 reports the LEGACY Query error code from `ErrorCode()`

Fourteen go assertions failed on the first run, all of one family:

```
FAIL Errors.get_queue_url_on_a_missing_queue:
  got [2]interface {}{"AWS.SimpleQueueService.NonExistentQueue", true},
  want [2]interface {}{"QueueDoesNotExist", true}
```

The `true` is the interesting half: `errors.As(err, new(*types.QueueDoesNotExist))`
SUCCEEDED every time, so the facade's `__type` was right and the modelled class
was built. Only the code string differed — and the SDK is where it comes from.
Every generated `awsAwsjson10_deserializeError<Shape>` in `service/sqs@v1.48.1`
ends with

```go
awsQueryErrorCode := getAwsQueryErrorCode(response)   // reads x-amzn-query-error
if awsQueryErrorCode != "" { output.ErrorCodeOverride = &awsQueryErrorCode }
```

so a Go program that switches on `apiErr.ErrorCode()` sees
`AWS.SimpleQueueService.NonExistentQueue` and one that uses `errors.As` sees the
shape — against real AWS exactly as much as against this facade, because the
header is AWS's own and the override exists to serve customers migrating off the
Query protocol. `src/error.rs`'s catalog sends both spellings and sends them the
right way round; the suite was asserting a string no Go caller ever observes.
**Suite fixed** (`go-sdk/helpers.go`: a `legacyCode` table mirroring
`smoke_m0.py`'s `ERROR_CODES`, and `expectAPIError` now compares
`wireCode(shape)`), suite re-run: 239 passed, 0 failed. Both spellings stay
pinned by one call — the shape through `errors.As`, the legacy code through the
comparison.

### `Celery.close()` does not drop the AMQP producer pool

`python/celery_suite.py` failed `Predefined.publishing_sent_no_ListQueues` on the
first run: the phase that configures kombu with `predefined_queues` — the shape
where a worker has no queue-admin permissions and must neither list nor create —
saw a `ListQueues(QueueNamePrefix=<the full queue name>)` on the wire.

It was not the facade. Instrumenting kombu's `Channel._update_queue_cache` showed
the call came from `_resolve_queue_url` on a channel whose `transport_options`
carried **no `predefined_queues` at all**, over the same `Connection` object the
previous phase had used. celery 5.6's `Celery.close()` does two things —
`self._pool = None` and `_deregister_app(self)` — and does not touch `app.amqp`,
which is where a publishing app actually keeps its connection
(`AMQP.producer_pool`, memoized on an object that is itself a `cached_property`
of the app). So the suite reconfigured the app and republished down a connection
built from the previous phase's options.

**Suite fixed** (`drop_connections()`: `app.close()`, then
`app.__dict__.pop("amqp")`, then `kombu.pools.reset()`), suite re-run: 37 passed,
0 failed. Worth knowing beyond this suite — any process that reconfigures a
Celery app's broker transport options in place has the same trap.

### `WaitTimeSeconds=0` cannot be sent from Go

aws-sdk-go-v2 models it as a non-pointer `int32` and omits it at zero, so the go
suite drives its short-poll assertion from the queue's own
`ReceiveMessageWaitTimeSeconds=0` instead. A client difference, not a facade one
— boto3 sends the explicit zero, and does. Already on the register's "Not
divergences" list; repeated here because it is the kind of line that looks like a
facade gap in a diff of two suites.

## What each suite added that no other one could

The matrix is only worth running if the rows disagree about something. They do:

* **`smoke_m0.py` / `smoke_m4_sns.py`** — the reference inventory, and the only
  rows where MD5s are checked by a suite rather than by an SDK. botocore
  validates no digest for SQS (there is no MD5 handler for it anywhere in the
  package), so a facade returning a constant would sail past a boto3-only run;
  both digests are computed in-file from AWS's length-prefixed binary encoding.
* **`query_conformance.py`** — the only live client of the Query/XML codec on the
  SQS side: the flattening (`MessageAttribute.N.Value.*`, sparse and out-of-order
  batch entries, the transparent `.member.` segment, the depth cap), SQS's
  flattened lists against SNS's `<member>` spelling, the LEGACY error codes in
  XML with the empty `<Detail/>`, and the negative space of SigV4 (wrong secret,
  unknown key id, unsigned, stale clock, mismatched scope date, foreign service —
  and a foreign REGION asserted to be ACCEPTED, because "change `endpoint_url`
  and nothing else" depends on it).
* **`celery_suite.py`** — the framework workflow, and the only row whose
  redelivery is driven by a real lease expiry: `acks_late=True` +
  `acks_on_failure_or_timeout=False` means a raising task is never acknowledged
  and never deleted, so the second delivery can only come from the visibility
  timeout lapsing. The assertion is on the GAP, not on the count.
* **`go-sdk`** — a digest the client checks itself
  (`service/sqs/cust_checksum_validation.go` recomputes `MD5OfMessageBody` on
  send and `MD5OfBody` on every received message and fails the CALL on a
  mismatch, so a wrong body digest here is not one red line but every send and
  every receive erroring), an exception picked from a different field
  (`__type` + the query-error header, where botocore reads `QueryErrorCode`), and
  `SequenceNumber` asserted on the RECEIVE as well as the send — the C-SQS-3
  path.
* **`js`** — two protocols in one process, errors as classes
  (`catch (e) { if (e instanceof QueueDoesNotExist) }`, which works only if the
  facade gets SQS's two spellings the right way round), and an SDK that validates
  the body digest but provably not the two attribute digests — which the `probe`
  lane proves against an in-process stub returning deliberately corrupt digests
  rather than trusting the documentation.
* **`sqs-consumer`** — the only row that is a real worker loop rather than a
  scripted sequence: it nacks by `ChangeMessageVisibility`, deletes in batches
  keyed by MessageId, and long-polls back to back. Twenty messages through
  `handleMessage` with three handlers throwing produced 23 deliveries and 29
  empty polls, every message processed exactly once, no SQS-API error emitted,
  queue drained; twelve through `handleMessageBatch` returning a SUBSET behaved
  the same way, in batches of `[6,3,2,1,1,1,1]` — which is [QS-01](DIVERGENCES.md#qs-01)
  visible as a shape rather than as a failure.
* **the embedded one-shot** — the only row where nobody started the facade.

## Re-running it

```sh
cargo build --manifest-path protocols/queen-sqs/Cargo.toml
protocols/queen-sqs/compat/rig.sh up
source protocols/queen-sqs/compat/.rig/env.sh

python  protocols/queen-sqs/compat/smoke_m0.py                    # 1 expected FAIL: QS-01
python  protocols/queen-sqs/compat/smoke_m4_sns.py
AWS_CLI=…/bin/aws protocols/queen-sqs/compat/smoke_m0_cli.sh
python  protocols/queen-sqs/compat/python/query_conformance.py
python  protocols/queen-sqs/compat/python/celery_suite.py         # needs celery, kombu
GOWORK=off go -C protocols/queen-sqs/compat/go-sdk run .          # GOWORK=off is not optional
( cd protocols/queen-sqs/compat/js && npm install && node run.mjs all )

protocols/queen-sqs/compat/rig.sh down
```

The python rows want a venv holding `boto3 celery kombu` (`pycurl` optional — it
decides which of kombu's two consumer paths phase 2 runs, and the suite detects
which and says so). Every suite defaults to the rig's own endpoint and
credentials, so a bare invocation after an `up` does the obvious thing; every one
of them takes the stack from the environment and none has a hardcoded address.

## What is NOT in this matrix yet

Stated so that "measured" and "expected to work" stay different claims. The plan
names these lanes and this run did not have them:

* **the differential run against real AWS SQS/SNS** — the release gate for "zero
  unexplained divergences", and the only thing that answers
  [Q1–Q6](DIVERGENCES.md#questions-for-the-differential-lane);
* **aws-sdk-java v2 / Spring Cloud AWS, aws-sdk-php / Laravel, async-aws /
  Symfony Messenger, aws-sdk-net / MassTransit, aws-sdk-rust, Shoryuken,
  Terraform, KEDA.** async-aws is the notable gap: it is the Query-protocol
  client an SDK row would exercise, and the path-only finding above is exactly
  the shape it would meet first;
* **an OLD SDK major per language.** Every row above is a current major, and
  every current major of every language speaks JSON for SQS. The Query codec's
  only live SQS client in this run is one this repository wrote;
* **`QUEEN_SQS_RECEIVE_MODE=amortized`** (C-SQS-1). The whole run is `exact`;
* **the Cloud shape**, where the facade's `QUEEN_URL` is a cell's proxy rather
  than a broker, so every request crosses authentication, tenant scoping, quotas
  and metering. queen-kafka has that row; this one does not yet.
