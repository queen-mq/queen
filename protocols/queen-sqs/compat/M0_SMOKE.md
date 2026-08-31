# queen-sqs M0 — first live run

The M0 surface driven by boto3 and by the aws CLI against a REAL broker and a
REAL Postgres, which is the half `protocols/queen-sqs/src/http_tests.rs` cannot do: those
tests drive `FakeQueen`, so a pop always answers, a lease never lapses and the
KV registry is a map in the same process. Everything below is what changed when
those three stopped being true.

Run: 2026-08-31, branch `queen-kafka`, `queen-sqs` at the working tree of that
morning, broker `server/target/debug/queen`, Postgres 16 in the rig's container.

## How it was run

```
protocols/queen-sqs/compat/rig.sh up
source protocols/queen-sqs/compat/.rig/env.sh
python protocols/queen-sqs/compat/smoke_m0.py
AWS_CLI=/path/to/venv/bin/aws protocols/queen-sqs/compat/smoke_m0_cli.sh
protocols/queen-sqs/compat/rig.sh down
```

Stack: throwaway Postgres on 55440 (container `qsqs-rig-pg`), debug broker on
26632 with `QUEEN_APPLY_SCHEMA=true` and JWT off, facade on 19324 with
`QUEEN_SQS_AUTH=sigv4`, one credential `QSQSTEST:qsqssecret:devtoken`, region
`queen-1`, account `000000000000`, `QUEEN_SQS_DEFAULT_PARTITIONS=8`.

## Results

| suite | client | protocol spoken | passed | failed |
|---|---|---|---|---|
| `smoke_m0.py` | boto3 1.43.83 / botocore 1.43.83 | AWS JSON 1.0 | 95 | 2 |
| `smoke_m0_cli.sh` | aws-cli 1.46.1 (botocore 1.43.62) | AWS JSON 1.0 | 12 | 0 |

Neither log recorded a panic; the only WARN lines the facade emitted were the
two `SignatureDoesNotMatch` the suite provokes on purpose. The broker's one WARN
is a spool-directory fallback that has nothing to do with the facade.

Both failures reproduce on a freshly torn-down and rebuilt stack, and both are
the facade's. The rig and the suites are green on everything else, including
every assertion whose first version was wrong (see "What the suites got wrong",
below — three of the first run's five failures were the suite's own fault and
are fixed).

Protocol note: BOTH clients in this run speak AWS JSON 1.0. The Query/XML codec
is therefore **not exercised by these two suites at all** — it needs an older
SDK major or async-aws, which is the M5 client matrix's job. Nothing here should
be read as evidence about it.

## Discrepancies — facade vs. real SQS

Line numbers are from the working tree of the run and `protocols/queen-sqs/src` is under
active edit, so trust the function names over the numbers.

### D1. `CreateQueue` on an existing queue, naming no attributes, is refused

**Request.** `CreateQueue{QueueName: "m0-crud-…"}` — no `Attributes` member at
all — against a queue created earlier in the same run with
`VisibilityTimeout=30, MessageRetentionPeriod=3600, MaximumMessageSize=262144,
DelaySeconds=0, ReceiveMessageWaitTimeSeconds=0`.

**Expected.** `200`, `{"QueueUrl": "http://…/000000000000/m0-crud-…"}`. AWS's
own documentation of the `QueueNameExists` error: it is returned "only if the
request includes attributes whose values differ from those of the existing
queue". A request that includes no attributes includes none that differ.
PLAN_QUEEN_SQS.md says the same thing in its error catalog —
"`QueueAlreadyExists` (only on attribute mismatch, per AWS)".

**Actual.** `400 QueueAlreadyExists` / `QueueNameExists`, message "A queue
already exists with the same name and a different value for attribute
DelaySeconds".

**Suspected.** `protocols/queen-sqs/src/registry.rs:687`, in `Registry::create`:

```rust
Err(winner) => match first_difference(&winner.attributes, &record.attributes) {
```

`record.attributes` is the request's attributes plus the stamped
`queen.partitions`, and `first_difference` (`registry.rs:1301`) reports a
difference in BOTH directions — its `.or_else` arm finds keys present in the
stored record and absent from the request. So every attribute the first create
set becomes a conflict for every later create that does not repeat it. The
comparison wants to be one-directional: only the attributes the REQUEST names,
compared against what is stored.

**Why it matters.** This is the idempotent create every framework performs at
worker startup — Celery, sqs-consumer, ActiveJob, Spring Cloud AWS — against a
queue that Terraform or an operator made with non-default attributes. Under
this behaviour every one of those workers fails to boot. It is the single
highest-traffic call shape in the whole M0 surface.

**Note for the differential lane.** The CreateQueue page carries a second,
looser sentence ("If you provide the name of an existing queue along with the
exact names and values of all the queue's attributes, CreateQueue returns the
queue URL for the existing queue"), which read alone would license today's
behaviour. The error's own page is the specific one and the ecosystem's
behaviour agrees with it, but a run against real AWS settles it in one call and
should be the thing that closes this item.

**Fixed** (not yet re-run live). The comparison is one-directional in
`registry.rs`: a create that names an existing queue wins its URL unless an
attribute it SUPPLIES differs from that queue's current value. Three parts, each
with its own regression test in `registry.rs` and `actions/queues.rs`:

  * the request's own attributes are the only ones read — the stamped
    `queen.partitions` is a default this facade implies and is not compared,
    though a client that supplies one is compared against it;
  * the existing side is the queue's EFFECTIVE attributes, which is what
    `GetQueueAttributes` answers, so supplying AWS's default against a queue
    created bare is not a difference (the invariant "what the read answers, the
    create accepts back" is pinned as a test);
  * `tags` are neither compared nor applied on an existing queue — they are not
    attributes, and `TagQueue` is the action that changes them. This is the half
    the differential lane should still settle; both halves are now asserted in
    `smoke_m0.py`, alongside the subset re-create.

One deliberate consequence, because the suffix DECLARES the type: an existing
`.fifo` queue re-created WITHOUT `FifoQueue=true` now succeeds, where the same
request for a queue that is not there is still the bad create it always was.

### D2. In-flight messages are capped by the queue's partition count

**Request.** A standard queue with `queen.partitions=1` and
`VisibilityTimeout=300`; `SendMessageBatch` of 3; then `ReceiveMessage` with
`MaxNumberOfMessages=10`, repeatedly, deleting nothing.

**Expected.** All 3 in flight at once. SQS's in-flight ceiling is 120,000 per
queue and has nothing to do with any internal lane; a standard queue has no
head-of-line blocking, which is most of what distinguishes it from a FIFO one.

**Actual.** 1. The other two stay invisible until the first is deleted or its
lease lapses. Measured across widths, 10 messages sent to a queue of each width,
read without deleting:

| `queen.partitions` | sent | in flight at once | `ApproximateNumberOfMessages` | `…NotVisible` |
|---|---|---|---|---|
| 1 | 3 | 1 | 2 | 1 |
| 1 | 10 | 1 | 9 | 1 |
| 8 | 10 | 7 | 3 | 7 |
| 64 | 10 | 10 | 0 | 10 |
| 256 | 10 | 10 | 0 | 10 |

The ceiling is the number of DISTINCT lanes the messages hashed into, so with
the shipped default of 64 lanes it is invisible at ten messages and bites at a
few hundred; at the rig's 8 it bites at ten. (The 8-lane row reads 7 rather than
the ~5.9 lanes ten random ids are expected to occupy — the count is a sample,
and the mechanism is the deterministic `partitions=1` rows.)

**Suspected.** `protocols/queen-sqs/src/actions/messages.rs:625`, `pop_exact`: N
concurrent `pop_queue` calls, each `batch=1`. Each pop takes a durable claim on
one (partition, group) for `lease_seconds`, and a lane with a live claim serves
no second pop, so N concurrent pops collect at most one message per free lane.
This is the plan's exact mode working as designed; what is new is the
consequence, which the plan does not state: a standard queue's concurrency
ceiling is its width, and a consumer holding a message blocks every message
behind it in that lane for a full visibility timeout.

**Not a correctness bug.** Nothing is lost or duplicated — the suite asserts
`InFlight.every_message_is_eventually_receivable` and it passes, and the depth
attributes account for every message (`InFlight.depth_attributes_account_for_
every_message` passes too, so KEDA and friends still see the blocked messages as
work). It is a throughput and latency property, and a semantic one: SQS
customers do not expect a slow consumer to stall its neighbours on a standard
queue.

**Worth deciding, not necessarily fixing here.** `QUEEN_SQS_DEFAULT_PARTITIONS`
is the dial that moves it, and the honest fix may be documentation plus a wider
default rather than code. It interacts directly with the `amortized` receive
mode (C-SQS-1) and with M3 redrive, since a message stuck behind a slow
neighbour ages toward `maxReceiveCount` without ever being delivered. Whatever
is decided, it belongs in the divergence register with its sentence, because
today it is written down nowhere.

## What was confirmed working against the real broker

Recorded because these are the things a FakeQueen suite proves least, and all of
them held:

- **SigV4 end to end.** Both clients signed normally against `--endpoint-url` /
  `endpoint_url` with credentials from the environment; a wrong secret is
  refused as `SignatureDoesNotMatch` and not as anything else.
- **The MD5s are right.** The suite computes AWS's two digests itself — the body
  one, and the attribute one with its length-prefixed name / type / transport
  byte / value encoding — over String, Number, Binary and a custom
  `String.email` type, on `SendMessage` and per entry on a 10-entry
  `SendMessageBatch`. All match. (This had to be done by hand: **botocore does
  not validate SQS MD5s client-side**, unlike the Java, JS and .NET SDKs. A
  suite that leaned on boto3 for this would have checked nothing.)
- **The error catalog matches AWS in both spellings.** SQS names every error
  twice and the two names are usually different words; the facade gets every
  pair right, which is what makes boto3 raise the modelled exception class:

  | shape (`QueryErrorCode`) | legacy code (`Code`) |
  |---|---|
  | `QueueDoesNotExist` | `AWS.SimpleQueueService.NonExistentQueue` |
  | `QueueNameExists` | `QueueAlreadyExists` |
  | `QueueDeletedRecently` | `AWS.SimpleQueueService.QueueDeletedRecently` |
  | `BatchEntryIdsNotDistinct` | `AWS.SimpleQueueService.BatchEntryIdsNotDistinct` |
  | `EmptyBatchRequest` | `AWS.SimpleQueueService.EmptyBatchRequest` |
  | `TooManyEntriesInBatchRequest` | `AWS.SimpleQueueService.TooManyEntriesInBatchRequest` |
  | `ReceiptHandleIsInvalid` | `ReceiptHandleIsInvalid` |
  | `InvalidAttributeName` | `InvalidAttributeName` |

- **Long poll really parks.** `WaitTimeSeconds=3` on an empty queue returned
  after 3.01s; `WaitTimeSeconds=0` after 0.02s; a message already waiting comes
  back at once instead of serving out the window.
- **The lease is the broker's, not a facade timer.** A message received and
  abandoned on a `VisibilityTimeout=2` queue came back on its own with
  `ApproximateReceiveCount=2` and a NEW receipt handle, and the old handle was
  refused. `ChangeMessageVisibility` extends (the message stayed hidden past the
  queue's own 2s) and terminates (`0` brought it straight back, count 2).
- **Receipt handles are capabilities.** A forged handle is
  `ReceiptHandleIsInvalid` on `DeleteMessage` and a per-entry `Failed` with
  `SenderFault=true` inside a `DeleteMessageBatch` that succeeds for its other
  entries. Double-delete of a real handle answers 200, as AWS does.
- **The 60-second tombstone is armed.** `CreateQueue` on a just-deleted name is
  `QueueDeletedRecently`.
- **`SetQueueAttributes` merges** rather than replacing, and a queue URL bearing
  another account's segment reads as `QueueDoesNotExist` rather than as a
  malformed request.

Two observations that are not defects and are recorded so nobody files them:

- `GetQueueAttributes` with `AttributeNames=All` includes `queen.partitions`,
  which is not an AWS attribute. It is the plan's own extension and both clients
  ignored it cleanly. Worth a line in the docs, not a change.
- `ApproximateFirstReceiveTimestamp` is absent under `All`; `messages.rs`
  documents the omission deliberately. Neither client minded.

## What the suites got wrong

Kept here so the next reader does not re-derive it. The first run reported five
failures; three were the suite's:

1. **Error codes asserted under the wrong name.** The suite expected
   `QueueAlreadyExists` in `QueryErrorCode`. It is the SHAPE name that goes
   there (`QueueNameExists`) and the legacy code that goes in `Code` — see
   `botocore/handlers.py:_handle_sqs_compatible_error`, which uses
   `QueryErrorCode` to pick the exception class. The facade was right; the
   suite now pins all three of shape, code and exception class through
   `expect_error`.
2. **Draining without deleting.** Two assertions read ten messages back with a
   loop that never deleted, and got six. That is D2 seen sideways, not a
   separate fault: exhaustive reads now use `drain_deleting` (receive, delete,
   repeat), which is what a real consumer does and the only shape that can empty
   a queue holding more messages than it has lanes. Where the suite genuinely
   needs several handles alive at once (`DeleteMessageBatch`), `hold` sends an
   extra message rather than waiting on a lane that will not open.
3. **`read` split a body on its spaces.** The CLI suite parsed
   `--output text`'s tab-separated projection with a default `IFS`, so a body
   with spaces in it became the body's first word and a receipt handle that was
   its second. `IFS=$'\t'` fixes it; the body deliberately still has spaces.

Also strengthened while there: "the deleted message does not come back" proved
nothing on its own, since an undeleted message is equally silent for its
visibility timeout. The CLI suite now asserts both depth counters at zero.
