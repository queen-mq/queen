# queen-sqs compat: the Python lane

Two suites, and they do not overlap at all.

| file | client | wire protocol | what it is for |
|---|---|---|---|
| `celery_suite.py` | Celery 5.6 over kombu's SQS transport (boto3 underneath) | AWS JSON 1.0 | the framework workflow: queue creation at start-up, long-poll consume, delete-per-message, and a failing task redelivered by the **visibility timeout** |
| `query_conformance.py` | none — hand-rolled, `urllib` + a SigV4 signer written in the file | Query/XML | the SQS Query codec end to end: the flattening, the XML answer shapes, the legacy error spellings, `x-amzn-RequestId` |

`query_conformance.py` exists because **no Python client can reach the Query
codec any more.** botocore's SQS model is JSON-only:

```
$ python -c "import boto3; print(boto3.client('sqs', endpoint_url='http://x', \
    region_name='r', aws_access_key_id='a', aws_secret_access_key='b') \
    .meta.service_model.protocols)"
['json']
```

so boto3, the aws CLI, Celery and everything built on them speak AWS JSON 1.0
whatever they are configured with — which M0_SMOKE.md already recorded ("BOTH
clients in this run speak AWS JSON 1.0. The Query/XML codec is therefore not
exercised by these two suites at all"). `src/proto/query.rs` and
`src/proto/xml.rs` are ~1,700 lines that had unit tests and no live client.
This lane is that client.

## Running them

Both follow the compat suite contract: the stack comes from the environment,
one `ok NAME` / `FAIL NAME: detail` line per assertion, a `RESULT:` line last, a
nonzero exit on failure, and a `# protocol spoken:` line read from the client's
own debug output.

```sh
# 1. a python with the clients in it (any venv; this is the whole list)
python3 -m venv /path/to/awsenv
/path/to/awsenv/bin/pip install boto3 celery kombu

# 2. the stack
protocols/queen-sqs/compat/rig.sh up
source protocols/queen-sqs/compat/.rig/env.sh

# 3. the suites, in either order, independently
/path/to/awsenv/bin/python protocols/queen-sqs/compat/python/query_conformance.py
/path/to/awsenv/bin/python protocols/queen-sqs/compat/python/celery_suite.py

# 4.
protocols/queen-sqs/compat/rig.sh down
```

`pycurl` is **optional**. With it installed, celery's default consumer runs
kombu's async hub in one of the two phases; without it, both phases run the
blocking consumer. The suite detects which and says so in its output — it does
not need to be told.

### Environment

Read by both suites, all with rig-matching defaults so they can be run without
sourcing anything:

| variable | default | |
|---|---|---|
| `QUEEN_SQS_ENDPOINT` | `http://127.0.0.1:19324` | the facade |
| `QUEEN_SQS_REGION` | `queen-1` | the SigV4 scope's region and the ARN's |
| `QUEEN_SQS_ACCOUNT` | `000000000000` | the queue URL's account segment |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `QSQSTEST` / `qsqssecret` | the rig's one credential |

`celery_suite.py` reads two more of its own:

| variable | default | |
|---|---|---|
| `QSQS_CELERY_VISIBILITY` | `12` | the queue's `VisibilityTimeout`, and the floor the redelivery is measured against |
| `QSQS_CELERY_KEEP` | unset | `1` keeps the worker logs, the botocore wire logs and the task evidence files in their temp directory instead of deleting them |

The rest (`QSQS_CELERY_QUEUE`, `_RESULTS`, `_PREDEFINED`, `_SYNC`, `_WIRELOG`)
are how the suite process talks to the worker process it spawns. They are not
for people; setting them by hand only confuses the run.

## `celery_suite.py`

One file that is both the suite and the Celery app, because a worker has to
import the module that defines its tasks. The suite spawns

```sh
python -m celery -A celery_suite worker --pool solo --concurrency 1 \
    --loglevel INFO -Q <queue> -n qsqs-<phase>-<run>@%h \
    --without-gossip --without-mingle --without-heartbeat
```

as a subprocess with the phase's settings in its environment; the tasks record
every invocation to a file, and every assertion is made from outside the worker
— against the file, or against the queue through boto3. boto3 is the
*instrument* here, not a second client under test: what it is asked is always a
fact about the queue (does it exist, what are its attributes, is it empty) that
only makes sense read from outside. About 38 assertions across the two phases.

Two phases, because kombu has two queue-resolution modes and they exercise
opposite halves of the facade.

**Phase 1, `discovered`** — no `predefined_queues`. kombu calls `ListQueues` at
channel open and `CreateQueue` for a queue it does not find, which is the
create-at-start-up shape. Three tasks, drained by a worker whose transport has
its async flag turned off, so that every call it makes — the `ReceiveMessage`
included — is a botocore call and lands in the wire log.

**Phase 2, `predefined`** — the queue is created out of band and its URL handed
to kombu, which then calls neither `ListQueues` nor `CreateQueue`
(kombu 5.6's `Channel._create_queue` returns `None` from its first statement
when `predefined_queues` is set). This is the deployment shape where a worker
has no queue-admin permissions, and it is the phase that carries the ten tasks
and the failing one. Celery runs in its default configuration here.

### The failing task, and why it is a broker test

`qsqs.flaky` carries `acks_late=True` and `acks_on_failure_or_timeout=False`.
Under those two settings a task body that raises is **not acknowledged**: celery
takes the last branch of celery 5.6's `Request.on_failure` and calls
`self.reject(requeue=False)`, which for kombu's SQS transport pops the message
from the worker's local delivered map and deletes *nothing*. The message is
still in flight at the broker, and the only thing that can bring it back is its
lease expiring.

So the assertion is not "the task ran twice" — it is that the second run came
**at least `VisibilityTimeout` seconds after the first**, carrying the same
celery task id. `self.retry()` would have republished within milliseconds with a
new message id and proved nothing about the facade; a `Reject(requeue=True)`
likewise (kombu's `_restore_at_beginning` re-`_put`s the message). Only a real
lease expiry produces the gap.

### D1, the idempotent create

M0_SMOKE.md's D1 is about the create every framework performs at worker
start-up against a queue somebody else made with non-default attributes. kombu
issues the **first** of those creates and, by itself, never the repeat: its
`_queue_cache` plus a targeted `ListQueues(QueueNamePrefix=<full name>)` mean it
does not create a queue it can already see, and `predefined_queues` suppresses
the create entirely. So the phase asserts both halves separately —

* kombu really created the queue, with its own `VisibilityTimeout` on it, and
  the facade stamped `queen.partitions` (read back through `GetQueueAttributes`,
  and the `CreateQueue` is read out of the wire log *before* any other client
  touches the queue, so it is unambiguously kombu's);
* the repeat, replayed in kombu's exact shape —
  `CreateQueue(QueueName, Attributes={'VisibilityTimeout': …})` against the
  queue kombu just made — returns the same URL, as does a bare repeat naming no
  attributes at all, while a repeat naming a *different* `VisibilityTimeout` is
  refused as `QueueNameExists`.

That replay is byte for byte the call a second worker fleet, or a redeployed one
whose cache is cold and whose `ListQueues` is filtered by IAM, would make.

### Where the protocol line comes from

The suite installs a handler on botocore's own `botocore.endpoint` logger, in
both its own process and the worker's, and keeps the one line botocore writes
before every request:

```
Making request for OperationModel(name=SendMessage) with params: {…
  'headers': {'X-Amz-Target': 'AmazonSQS.SendMessage',
              'Content-Type': 'application/x-amz-json-1.0', …}, …}
```

The `# protocol spoken:` line and the per-operation summary are parsed out of
that file. Nothing is inferred from an SDK version, and the line after it (the
one carrying the `Authorization` header) is filtered out rather than written.

**The one gap in that stream, stated rather than papered over:** on celery's
default consumer, kombu's async hub builds and sends the `ReceiveMessage`
itself, so botocore's endpoint is not involved and that call is invisible to the
log. Every other call — `CreateQueue`, `ListQueues`, `SendMessage`,
`DeleteMessage`, `GetQueueAttributes` — goes through botocore on both paths,
because `Channel.basic_ack` deletes with the synchronous client. That is why
phase 1 turns the async flag off: between the two phases every operation is
observed at least once, and both consumer paths run.

## `query_conformance.py`

No SDK, and nothing imported that is not in the standard library. It builds the
form bodies, signs them, posts them with `urllib`, and parses the XML with
`xml.etree`. About 120 assertions over:

* **the envelope** — `<{Action}Response>` / `<{Action}Result>` /
  `<ResponseMetadata><RequestId>`, the SQS namespace, `text/xml`, and the two
  actions that write no result element at all (`SetQueueAttributes`,
  `DeleteQueue`);
* **the flattening** — `MessageAttribute.N.Value.{DataType,StringValue,BinaryValue}`,
  `MessageSystemAttribute.N.*`, batch entries written **sparse and out of order**
  and one of them with the transparent `.member.` segment, `Attribute.N.Name/.Value`
  and the `Attributes.entry.N.key/.value` spelling, `AttributeName.N` and a bare
  unindexed `AttributeName=All`, and the 32-segment depth cap;
* **the answer shapes** — SQS's flattened lists (`<QueueUrl>` repeated, never a
  `<QueueUrls>` wrapper; `<Message>`, `<{Action}ResultEntry>`,
  `<BatchResultErrorEntry>`) and its maps (`<Attribute><Name/><Value/></Attribute>`,
  `<Tag><Key/><Value/></Tag>`) — never SNS's `<member>` and `<entry><key/>`;
* **the MD5s** — body, message attributes (String, Number, and a `Binary.gzip`
  custom type whose digest is over the RAW bytes), and system attributes,
  computed in the file because no Python client checks them;
* **the errors** — `<ErrorResponse><Error><Type>Sender</Type><Code>…`, the
  **legacy** code spellings (`AWS.SimpleQueueService.NonExistentQueue` and
  friends), the empty `<Detail/>` SQS writes and SNS does not, the top-level
  `<RequestId>`, and an explicit check that no JSON spelling
  (`com.amazonaws.sqs#…`, `QueueNameExists`, `MessageNotInflight`) leaks into an
  XML document — including inside a `BatchResultErrorEntry`, where the codec has
  to translate the code it was handed;
* **SigV4** — the correct signature is proved by every other request in the
  file; the negative space is a wrong secret, an unknown key id, an unsigned
  request, a stale clock, a scope date that disagrees with the timestamp, and a
  scope naming another service, each with its own code. A scope naming a foreign
  *region* is asserted to be **accepted**, because "change `endpoint_url` and
  nothing else" depends on it;
* **`x-amzn-RequestId`** on every answer, success and error, unique per request,
  and equal to the id inside the document.

### Reading more than one message, and D2

M0_SMOKE.md's D2 — a standard queue's in-flight ceiling is the number of
distinct LANES its messages hashed into, and the rig runs at eight — is not a
correctness bug but it is a trap for a suite. Three messages over eight lanes
collide about a third of the time, and a helper that collected them all before
deleting any would stall on the collision, then pick up the first message a
second time when its visibility lapsed, and report a lost message.

So there is deliberately no "receive N" helper here. There are three, and which
one a test wants is a decision it makes on purpose:

* `receive_one` — one message, looping past empty polls;
* `collect_deleting` — receive, delete, go round again, which is the only shape
  that can read a queue holding more messages than it has free lanes;
* `hold` — N messages live at once, topping the queue up with fillers rather
  than waiting on a lane that will not open (a fresh MessageId is a fresh lane).
  Used only where a batch delete needs two handles at the same time, and the
  queue is drained afterwards rather than asserted empty.

`celery_suite.py` needs none of this: its worker deletes as it goes, so lanes
free themselves.

### The signer

Written in the file from AWS's specification, and checked three ways before it
was ever pointed at the facade:

* it reproduces AWS's own SigV4 test-suite vector `get-vanilla` exactly;
* it produces a byte-identical `Authorization` header to botocore's `SigV4Auth`
  on the root path, a queue path, an already-percent-encoded path (the double
  encoding), and a path of unreserved characters;
* and a re-implementation of `src/sigv4.rs`'s *verifier* re-derives the
  signature from the bytes the client actually put on a socket — form body,
  `Host`, header casing and all — and agrees.

## What this lane found

Recorded here rather than left in a log, for whoever writes the M5 report.

**`QueueUrl` is required even when the path names the queue.** AWS's Query
documentation addresses a message action by posting to the queue URL
(`POST /<account>/<queue>`) with `Action=…` in the body and no `QueueUrl`
parameter, and kombu's async Query mode still builds exactly that request
(`kombu/asynchronous/aws/sqs/connection.py::_create_query_request` passes the
queue URL as the request URL and never as a parameter). `queen-sqs` resolves the
queue from the `QueueUrl` **parameter** only
(`actions/queues.rs::queue_of` → `require_text(params, "QueueUrl")`), so that
request is a `MissingParameter`. The suite pins today's behaviour rather than
asserting a fix, under
`Errors.queue_addressed_by_path_only_is_refused_today`.

It is **dormant today**: botocore dropped `query` from the SQS model's
`protocols`, so kombu takes its JSON branch, which does send `QueueUrl`. It
would wake up for a client pinned to an old SDK major, for async-aws if it
serializes the same way, and for anyone following AWS's own Query examples with
curl. Worth a decision in M5, not a fix from this lane.

**Both suites have now run against the rig**, on 2026-08-31, and both are green:
`query_conformance.py` 127 assertions and `celery_suite.py` 37, no failures
either side. The numbers and the protocol lines are in
[`../MATRIX.md`](../MATRIX.md), which is the run of record.

**What the first live run of `celery_suite.py` cost, and why it is written down.**
It failed `Predefined.publishing_sent_no_ListQueues` — the predefined phase,
which must neither list nor create, listed. It was not the facade. celery 5.6's
`Celery.close()` does two things (`self._pool = None` and `_deregister_app`) and
does NOT touch `app.amqp`, which is where a publishing app actually keeps its
connection (`AMQP.producer_pool`, memoized on an object that is itself a
`cached_property` of the app). So the second phase reconfigured the app and then
published down the FIRST phase's connection, whose `transport_options` had no
`predefined_queues` in them, and kombu's `_resolve_queue_url` did what it does
without a predefined map: `ListQueues(QueueNamePrefix=<the full queue name>)`.
`drop_connections()` is the fix — `close()`, then `app.__dict__.pop("amqp")`,
then `kombu.pools.reset()` — and it is worth knowing beyond this suite: any
process that reconfigures a Celery app's broker transport options in place has
the same trap.
