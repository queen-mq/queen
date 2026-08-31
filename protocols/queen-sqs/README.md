# queen-sqs

An SQS and SNS wire front for Queen. Point an unmodified AWS SDK at it — change
`endpoint_url` and nothing else — and it translates to a Queen broker over plain
HTTP. Everything durable lives in Queen: the queue and topic registry, the
messages, the leases, the offsets. The facade itself holds nothing but caches,
so a restart is free and **any instance can answer any request**, which is the
one sentence the whole design protects (and the exact opposite of
[queen-kafka](../queen-kafka/README.md) — see [Deploying it](#deploying-it)).

The claim it makes is the same one ElasticMQ and LocalStack normalized: your
SDK, your framework, your driver, one changed endpoint.

## What it can do

- **Standard queues.** Send, receive, delete, and their batch forms; long poll;
  per-message `DelaySeconds`; the visibility timeout, extended and terminated;
  `PurgeQueue` with AWS's 60-second cooldown; tags; the full attribute set,
  including the depth counters KEDA and every autoscaler read.
- **FIFO queues, natively.** A `.fifo` name makes `MessageGroupId` a Queen
  partition, so ordering within a group, group-blocked-while-in-flight and
  deduplication are the broker's own properties and not an emulation.
  `MessageDeduplicationId` (or the SHA-256 of the body under
  `ContentBasedDeduplication`) is the push's transaction id; the window is AWS's
  five minutes by default and `queen.dedupWindowSeconds` widens it.
- **Dead-letter queues.** `RedrivePolicy` is honoured by the facade: a message
  past `maxReceiveCount` is not returned, it is MOVED — push-to-DLQ and
  ack-original in ONE `POST /api/v1/transaction`, so a redrive can neither
  duplicate nor lose. `ListDeadLetterSourceQueues` and the
  `StartMessageMoveTask` / `Cancel` / `List` trio work, rate cap included.
- **SNS, for SQS subscribers.** Topics, subscriptions, filter policies (both
  scopes), raw delivery, FIFO topics, `Publish` and `PublishBatch`. **One
  publish is one transaction across every matched subscriber**, which is
  stronger than SNS itself promises. See [SNS: what v0 is](#sns-what-v0-is).
- **Both wire protocols on one listener**, sniffed per request: AWS JSON 1.0
  (what every SDK major since late 2023 speaks for SQS) and Query/XML (older
  majors, async-aws — and ALL of SNS, which never moved to JSON).
- **SigV4**, verified in-house against static credentials, header and presigned
  variants both. No AWS crates anywhere in the build.

Forty actions, and the set is CLOSED: an action this facade does not implement
answers `InvalidAction` rather than something plausible, because "plausible" for
a client that asked for `AddPermission` means it believes its policy was
applied.

## Run it

```
cargo build --release
QUEEN_URL=http://localhost:6632 \
QUEEN_SQS_CREDENTIALS=AKIDEXAMPLE:a-long-secret:$QUEEN_TOKEN \
./target/release/queen-sqs
```

It listens on `0.0.0.0:9324` — the de-facto self-hosted SQS port — and answers
`GET /healthz`. Queue URLs are `http://<host>:9324/<account>/<name>` and ARNs are
`arn:aws:sqs:<region>:<account>:<name>`, with `QUEEN_SQS_REGION` and
`QUEEN_SQS_ACCOUNT` defaulting to `queen-1` and `000000000000`.

### boto3

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:9324",
    region_name="queen-1",              # any region: the scope is not enforced
    aws_access_key_id="AKIDEXAMPLE",
    aws_secret_access_key="a-long-secret",
)
url = sqs.create_queue(QueueName="orders")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="hello")
for m in sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10,
                             WaitTimeSeconds=20).get("Messages", []):
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
```

The `aws` CLI is the same move: `aws --endpoint-url http://localhost:9324 sqs
list-queues`.

### Laravel, stock `sqs` driver

Laravel hands the connection array to the SDK's `SqsClient` after pulling the
credentials out of it, so `endpoint` is forwarded as-is. `prefix` is what
actually addresses the queue, and it is the account segment of a queue URL:

```php
// config/queue.php
'sqs' => [
    'driver'   => 'sqs',
    'key'      => env('AWS_ACCESS_KEY_ID'),
    'secret'   => env('AWS_SECRET_ACCESS_KEY'),
    'endpoint' => env('AWS_ENDPOINT', 'http://queen-sqs:9324'),
    'prefix'   => env('SQS_PREFIX', 'http://queen-sqs:9324/000000000000'),
    'queue'    => env('SQS_QUEUE', 'default'),
    'region'   => env('AWS_DEFAULT_REGION', 'queen-1'),
],
```

`php artisan queue:work sqs` then runs against Queen. (This is not the same
thing as `clients/client-php`, which is a NATIVE Queen queue driver and the
faster of the two — this path exists so an application already written for SQS
does not have to change.)

### Celery

kombu takes the endpoint from the broker URL's host and port and the credentials
from its userinfo, so one string is the whole of it:

```python
app.conf.broker_url = "sqs://AKIDEXAMPLE:a-long-secret@localhost:9324"
app.conf.broker_transport_options = {
    "region": "queen-1",
    "is_secure": False,          # without this kombu addresses https
    "visibility_timeout": 300,
    "wait_time_seconds": 2,
    "polling_interval": 0.5,
}
```

Percent-quote both halves of the userinfo: a secret is allowed to contain `/`
and `@`. This is the configuration `compat/python/celery_suite.py` drives a real
Celery worker with.

## The two protocols

Both arrive on the one listener and are told apart per request, never by port
and never by configuration:

| protocol | how it is recognized | who speaks it |
|---|---|---|
| AWS JSON 1.0 | `X-Amz-Target: AmazonSQS.<Action>`, `Content-Type: application/x-amz-json-1.0` | every current SDK major, for SQS |
| Query / XML | form-encoded `Action=…&Version=…`, XML answers | older SDK majors, async-aws, and **all of SNS** (`Version=2010-03-31`) |

One internal action layer, two codecs: an action never learns which protocol
served it. The error catalog is rendered in both, and SQS's two spellings per
error — the shape name in `QueryErrorCode`, the legacy code in `Code` — are both
correct, which is what makes boto3 raise the modelled exception class rather
than a generic one.

A useful consequence for testing: boto3's SQS client speaks JSON and its SNS
client speaks Query, so one Python process crosses both codecs on one listener
and one SigV4 verifier.

## Credentials

SigV4 is verified against a static list, the MinIO model — operator config, not
a directory:

```
QUEEN_SQS_CREDENTIALS=akid:secret:queen_token[,akid:secret:queen_token…]
```

Each triple is an SQS access key, its secret (which never crosses the wire, so
the verifier has to hold it), and the **Queen token this facade presents upstream
for that principal**. That third field is how one listener serves several Queen
identities, and it is not an AWS session token: `AWS_SESSION_TOKEN` is never
read and signing with one fails verification.

`QUEEN_SQS_AUTH=off` accepts anything, ElasticMQ-style, for a laptop. A `sigv4`
listener with no credentials is a boot failure rather than an endpoint that can
only answer `InvalidClientTokenId`.

**The credential scope's region is not enforced**, deliberately: an SDK signs
with the region its user configured, and pointing boto3 at an `endpoint_url`
does not change that. The SERVICE is checked, to the two this listener answers.

**`QUEEN_SQS_HANDLE_SECRET` matters the day you add a second replica.** Receipt
handles are self-contained and HMAC-tagged, which is what lets any instance serve
a delete; unset, each process mints its own key and a handle taken from one
replica is `ReceiptHandleIsInvalid` at the next. The boot line says out loud
whether the secret was CONFIGURED or GENERATED, at every boot, for exactly this
reason.

Tenancy composes as kafka's: the facade strips an inbound `x-queen-tenant` and
stamps its own when configured, and the account segment of queue URLs doubles as
the tenant carrier in Cloud.

## Receive modes: `exact`, and the one that is refused

Queen's durable lease is a per-(partition, consumer-group) claim over a
contiguous span; SQS visibility is per message. **The two models coincide at
claim width 1**, so `exact` — the default and, today, the only mode — makes a
`ReceiveMessage` up to N parallel `batch=1` pops. Every SQS verb is then exact
rather than approximate: `ChangeMessageVisibility` extends a lease holding one
message, a terminate releases one message and charges nothing, a
`DeleteMessage` is an ack with no gap to swallow.

The cost is one pop write-transaction per message, which is honest — SQS is a
chatty ≤10-batch protocol and its own clients poll.

**`QUEEN_SQS_RECEIVE_MODE=amortized` is refused at boot**, with a FATAL naming
what it needs, rather than accepted and quietly served as `exact` under another
name. It requires `maxPerPartition` on the broker's pop (core change C-SQS-1),
which is not implemented. When it lands it will trade the per-message write for
two bounded divergences, both inside SQS's own at-least-once envelope; they are
pre-registered in [compat/DIVERGENCES.md](compat/DIVERGENCES.md).

## The one number you have to size: `queen.partitions`

A standard queue is M synthesized partitions, decimal-named, and **M is the
ceiling on how many messages that queue can have in flight at once.** A pop
claims a lane for the whole visibility timeout, so a consumer holding a message
blocks the rest of that lane behind it. AWS's own ceiling is 120,000 per queue
and has nothing to do with any internal lane, so this is a real divergence and it
is the register's [QS-01](compat/DIVERGENCES.md#qs-01).

Measured, ten messages sent to a queue of each width and read without deleting:

| `queen.partitions` | in flight at once |
|---|---|
| 1 | 1 |
| 8 | 7 |
| 64 | 10 |

Nothing is lost, nothing is duplicated, every message is eventually receivable
and the depth attributes account for all of them — so autoscalers still see the
blocked messages as work. What it costs is throughput and tail latency.

**Guidance.** The default is 64, which is invisible at ten messages and bites at
a few hundred. Set `queen.partitions` at or above the number of messages you
intend to have in flight concurrently on that queue, at CreateQueue:

```python
sqs.create_queue(QueueName="orders", Attributes={"queen.partitions": "512"})
```

It is immutable afterwards (partition counts never shrink), so it is worth a
minute at design time. `QUEEN_SQS_DEFAULT_PARTITIONS` moves the default for
queues that name none. Two consequences worth knowing: a short receive is normal
and legal, and a message stuck behind a slow neighbour ages toward
`maxReceiveCount` without ever being delivered.

On a FIFO queue the lane is the `MessageGroupId` and the width is not a dial:
one group is one message in flight per consumer, which is SQS FIFO's own
semantics.

## SNS: what v0 is

**SQS-queue subscriptions only.** That is MassTransit and JustSaying — the two
frameworks that auto-create SNS+SQS topologies and therefore the two best
end-to-end tests. `http`, `https`, `email`, `sms` and `lambda` are refused with a
message naming the milestone; HTTP/S delivery is M6, delegated to queen-relay,
and it is the milestone that has to answer the notification-signature question.

What is here works the way a subscriber sees it: the notification envelope with
its `Type`/`MessageId`/`TopicArn`/`Subject`/`Message`/`Timestamp`/
`MessageAttributes`, `RawMessageDelivery` that really is raw, filter policies
evaluated at publish over both scopes, FIFO topics that order identically for
every subscriber, and a `DeleteTopic` that cascades durably.

Two things to know before you meet them. **The notification is unsigned** — no
`Signature`, no `SigningCertURL`, no `UnsubscribeURL`, because a signature
nothing can verify is worse than none; queue subscribers read none of the three.
And **`Publish` answers no `SequenceNumber`**, because a transaction's push
echoes carry no offset and switching to `/push` would forfeit the atomic
fan-out.

## Deploying it

**Put it behind a plain Service, an Ingress or a load balancer. That is
supported, and it is the difference from queen-kafka.** A Kafka client re-dials
the address `Metadata` hands it, so kafka facades must each have their own
reachable address and a shared VIP is an anti-pattern that produces infinite
redirects. Nothing here works that way: SQS clients hold a queue URL, every
instance answers every request, and the three things that would otherwise be
per-process state are deliberately not:

- the queue, topic and subscription registry is in Queen's key/value store, with
  compare-and-set on every admin mutation;
- a receipt handle is self-contained and HMAC-tagged, so the instance that
  serves a `DeleteMessage` need not be the one that served the receive;
- the FIFO delete-set — which is what makes an out-of-order delete safe — lives
  in KV under `qs:ds:`, not in memory, for exactly the same reason.

Scale it as a Deployment. Set `QUEEN_SQS_HANDLE_SECRET` to the same value on
every replica, and give every replica of one endpoint credentials of ONE Queen
tenant: the registry lives in `queen.kv`, which is keyed by tenant, so two
tenants behind one address would be two disjoint sets of queues.

## Embedded mode

`QUEEN_SQS_EMBEDDED=true` makes the BROKER spawn and supervise this binary as a
child on loopback: one deployment, two processes, one image (the repository's
`Dockerfile` builds the facade beside the broker). It is the twin of the Kafka
supervisor, on purpose — an operator who has read one has read both — and the
mechanics are `server/src/sqs_facade.rs`: exponential backoff on a crash loop,
the child in its own process group, secrets stripped from the child's
environment after it has read them, `PR_SET_PDEATHSIG` on Linux so a killed
broker cannot leave an orphan holding the SQS port (on macOS it can, which is a
dev-machine caveat and is stated rather than papered over).

Set `QUEEN_SQS_SHUTDOWN_GRACE_MS` explicitly if you care about a 20-second long
poll surviving a rolling restart: the supervisor's default is 5s and the
facade's own is 25s, and only an explicit value makes the two agree.

## Configuration

```
QUEEN_SQS_LISTEN=0.0.0.0:9324          QUEEN_SQS_AUTH=sigv4|off
QUEEN_SQS_CREDENTIALS=akid:secret:token[,…]
QUEEN_SQS_REGION=queen-1               QUEEN_SQS_ACCOUNT=000000000000
QUEEN_SQS_RECEIVE_MODE=exact           (amortized is REFUSED at boot: needs C-SQS-1)
QUEEN_SQS_DEFAULT_PARTITIONS=64        QUEEN_SQS_HANDLE_SECRET=<≥16 bytes, else random>
QUEEN_SQS_TLS_CERT / _KEY              (SDKs are happy on plain HTTP for a custom endpoint)
QUEEN_SQS_EMBEDDED=true|false          QUEEN_SQS_BIN / QUEEN_SQS_SHUTDOWN_GRACE_MS
QUEEN_URL / QUEEN_TOKEN                (as kafka)
```

Every one of them is read in one place, at boot, and never again — which is what
lets embedded mode strip the secrets out of the child's environment afterwards.
A bad value is a boot failure with a sentence naming what to set instead, never a
default quietly substituted.

## Tests

`cargo test` — **715 green** (706 lib, 9 bin), with `clippy -D warnings` and
`fmt --check` clean. That includes the conformance corpus: SigV4 vectors (header,
presigned, skew, tampered), AWS's own published MD5 vectors over String, Number,
Binary and custom types, receipt-handle lifecycle including forged tags,
visibility races, batch partial failures, FIFO group blocking and dedup windows,
redrive loops, and both codec renderings of every error.

Live, against a real Postgres and a real broker:

```
protocols/queen-sqs/compat/rig.sh up            # throwaway PG + debug broker + facade
source protocols/queen-sqs/compat/.rig/env.sh   # QUEEN_SQS_ENDPOINT, AWS_*, …
python protocols/queen-sqs/compat/smoke_m0.py                     # the SQS surface, boto3
python protocols/queen-sqs/compat/smoke_m4_sns.py                 # SNS, boto3
AWS_CLI=…/bin/aws protocols/queen-sqs/compat/smoke_m0_cli.sh      # the aws CLI
protocols/queen-sqs/compat/rig.sh down
```

`rig.sh` is a stack MANAGER rather than a one-shot runner — `up`, `down`,
`status`, `logs` — because these suites are things a person also runs one at a
time. Its ports are deliberately not the defaults (Postgres 55440, broker 26632,
facade 19324) so an ElasticMQ or a LocalStack already running is neither
shadowed nor shadowing.

Four more lanes take the same stack from the same environment, each with the
compat suite contract (stack from env, one `ok NAME`/`FAIL NAME` per assertion, a
`RESULT:` line, nonzero exit, and the protocol the client actually spoke read
from its own debug output):

| lane | client | what it adds |
|---|---|---|
| [`compat/go-sdk`](compat/go-sdk) | aws-sdk-go-v2 sqs + sns | a second language; SDK-side checksum validation |
| [`compat/js`](compat/js) | @aws-sdk/client-sqs + client-sns, **sqs-consumer** | client-side MD5 validation, error CLASSES, a real worker loop |
| [`compat/python/celery_suite.py`](compat/python) | Celery 5.6 over kombu | the framework workflow: queue creation at start-up, long poll, redelivery by visibility timeout |
| [`compat/python/query_conformance.py`](compat/python) | hand-rolled urllib + its own signer | the Query/XML codec end to end — no Python client can reach it any more, botocore's SQS model is JSON-only |

Measured on 2026-08-31, on a torn-down and rebuilt stack: `smoke_m0.py`
**109 passed, 1 failed** and `smoke_m4_sns.py` **92/0**, with no panic in either
log. The one failure is [QS-01](compat/DIVERGENCES.md#qs-01), the partition-width
in-flight ceiling: a registered divergence that fails on purpose until it is
decided. `smoke_m0_cli.sh` was **12/0** on the same day's earlier stack. The four
lanes in the table have no published run yet.

## Divergences

**[compat/DIVERGENCES.md](compat/DIVERGENCES.md) is the register**: 45 rows, each
with its classification (`deliberate`, `accepted`, or `OPEN`), the sentence that
has to travel with it, and the test or live assertion that fails if it moves.
Zero unexplained is the release gate — which does not mean zero rows, it means
zero rows that are not written down and argued. One row is `OPEN` today (QS-01,
above) and six questions are waiting on a differential run against a real AWS
account.

Two live-run write-ups sit beside it and are the evidence behind many of the
rows: [compat/M0_SMOKE.md](compat/M0_SMOKE.md) (the SQS surface, including the
`CreateQueue` idempotency defect that run found and the fix) and
[compat/M4_SMOKE.md](compat/M4_SMOKE.md) (SNS, and the first time anything
crossed the Query/XML codec live).

## Status

Preview, and M0 through M4 of [PLAN_QUEEN_SQS.md](../PLAN_QUEEN_SQS.md).
Standard queues, FIFO queues, lifecycle, DLQ and redrive, and the SNS core are
implemented and driven by real clients against a real broker. What M5 still owes:
the full client matrix published, the differential run against real AWS, the
`crates/queen-facade` extraction shared with queen-kafka, and the webdoc pages.
It is not in release CI; the repository's `Dockerfile` builds the binary beside
the broker, but no published image carries it yet.
