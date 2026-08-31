# queen-sqs — the Node matrix row

`@aws-sdk/client-sqs`, `@aws-sdk/client-sns` and **sqs-consumer**, unmodified,
against a live queen-sqs, a live broker and a live Postgres. Nothing here is
patched, shimmed or configured beyond `endpoint` — which is the whole claim
`PLAN_QUEEN_SQS.md` makes and therefore the only interesting way to test it.

This row is not a copy of the python smokes in another language. It exists
because a JavaScript client differs from boto3 in four ways that a facade can
get wrong independently:

1. **It validates MD5s.** botocore does not; the installed `@aws-sdk/client-sqs`
   does, on the body, and raises `InvalidChecksumError` before the caller sees
   the answer. What it does NOT check is the two attribute digests — see below.
2. **It speaks two protocols in one process.** `client-sqs` resolves AWS JSON
   1.0, `client-sns` resolves Query/XML. One `node run.mjs all` therefore crosses
   both codecs on one listener and one SigV4 verifier.
3. **Its errors are classes.** `catch (e) { if (e instanceof QueueDoesNotExist) }`
   is what applications write, and it works only if the facade gets SQS's TWO
   spellings the right way round.
4. **sqs-consumer is a real worker loop**, not a scripted sequence of calls: it
   nacks by `ChangeMessageVisibility`, deletes in batches keyed by MessageId,
   and long-polls back to back for as long as the process lives.

## Running it

The stack comes from the environment. `rig.sh up` writes the file that supplies
it; nothing here has a hardcoded address.

```sh
protocols/queen-sqs/compat/rig.sh up                 # throwaway PG + debug broker + facade
source protocols/queen-sqs/compat/.rig/env.sh        # QUEEN_SQS_ENDPOINT, AWS_*, ...

cd protocols/queen-sqs/compat/js
npm install                                # once; node 22+
node run.mjs all                           # every lane

protocols/queen-sqs/compat/rig.sh down
```

`npm test` is `node run.mjs all`. Lanes can be named individually and combined:

| lane | needs the rig | what it is |
|---|---|---|
| `vectors` | no | the MD5 algorithms against the goldens in `protocols/queen-sqs/src/md5.rs` |
| `probe` | no | what the installed SDK does about protocols, MD5s and errors, proved against an in-process stub |
| `sqs` | yes | the SQS inventory, mirroring `compat/smoke_m0.py` |
| `sns` | yes | the SNS inventory, mirroring `compat/smoke_m4_sns.py` — the Query/XML codec |
| `consumer` | yes | sqs-consumer: `handleMessage` with throws and redelivery, `handleMessageBatch` with partial acks |

```sh
node run.mjs vectors probe      # offline; ~1s; needs nothing running
node run.mjs consumer           # just the worker loops
npm run offline                 # the same two offline lanes
```

The environment it reads, all of it optional and all of it written by
`rig.sh up` into `.rig/env.sh`:

| variable | default | what it is |
|---|---|---|
| `QUEEN_SQS_ENDPOINT` | `http://127.0.0.1:19324` | the facade |
| `QUEEN_SQS_REGION` | `queen-1` | signing region, and the region in every ARN |
| `QUEEN_SQS_ACCOUNT` | `000000000000` | the account segment of URLs and ARNs |
| `QUEEN_SQS_PARTITIONS` | `8` | the rig's `QUEEN_SQS_DEFAULT_PARTITIONS`; asserted as `queen.partitions` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `QSQSTEST` / `qsqssecret` | the rig's one credential |

The defaults ARE the rig's, so a bare `node run.mjs all` after an `up` does the
obvious thing; the endpoint is printed on the first line of every run so no
reader has to guess which stack a number came from.

## The output contract

Copied from `protocols/queen-kafka/compat/CLIENT_MATRIX.md`, verbatim in what matters:

* one `ok NAME` or `FAIL NAME: detail` line per assertion;
* every non-assertion line is `#`-prefixed;
* `RESULT: PASS` / `RESULT: FAIL` last;
* a nonzero exit status when anything failed;
* **the protocol each client actually spoke**, read off the requests the SDK
  itself built (a middleware at the end of `finalizeRequest`, so it sees the
  SIGNED request) and counted per request — never inferred from a version.

A real run — this is the tail of the one recorded in [`../MATRIX.md`](../MATRIX.md)
on 2026-08-31, all five lanes, first attempt, nothing fixed to get it:

```
# endpoint http://127.0.0.1:19324  region queen-1  account 000000000000  partitions 8  run d025c4e4
# node v22.17.0  @aws-sdk/client-sqs 3.1121.0  @aws-sdk/client-sns 3.1121.0  sqs-consumer 11.6.0
# lanes: vectors, probe, sqs, sns, consumer
ok CreateQueue.url
...
# worker lane: 23 deliveries for 20 messages, 29 empty polls
# batch lane: 6 batches, sizes [7,2,2,2,1,1]
# protocol spoken (sqs): AWS JSON 1.0 (application/x-amz-json-1.0; X-Amz-Target: AmazonSQS.*) — 359 request(s), signed for sqs
# protocol spoken (sns): Query/XML (application/x-www-form-urlencoded; Version=2010-03-31) — 66 request(s), signed for sns
# protocol spoken (sqs (bad credential)): AWS JSON 1.0 (application/x-amz-json-1.0; X-Amz-Target: AmazonSQS.*) — 1 request(s), signed for sqs
# 302 passed, 0 failed
RESULT: PASS
```

Assertion names are the python smokes' names wherever the two suites assert the
same fact. That is what makes the matrix worth running: an assertion that fails
in this row and passes in the boto3 row is a CLIENT difference; one that fails in
both is the facade.

## What the installed SDK actually does about MD5

The received wisdom is that the JS v3 SDK "dropped MD5 validation". At the
version installed here that is **half true, and the half matters** — read out of
the package on disk by `lib/sdk-md5.mjs`, and proved at runtime by the `probe`
lane against a stub that returns deliberately corrupt digests:

| digest | who checks it | evidence |
|---|---|---|
| `MD5OfMessageBody` on `SendMessage` | the SDK, and this suite | `getSendMessagePlugin` is applied by `client-sqs`; a corrupt digest raises `InvalidChecksumError` |
| `MD5OfMessageBody` per entry on `SendMessageBatch` | the SDK, and this suite | `getSendMessageBatchPlugin` |
| `MD5OfBody` per message on `ReceiveMessage` | the SDK, and this suite | `getReceiveMessagePlugin` |
| `MD5OfMessageAttributes` | **nobody but this suite** | the checksum middleware never names it; a corrupt digest reaches the caller untouched |
| `MD5OfMessageSystemAttributes` | **nobody but this suite** | likewise |

So `lib/md5.mjs` implements AWS's attribute encoding — length-prefixed name,
length-prefixed type (the full label, custom suffix included), transport byte
(`Number` uses the STRING byte), length-prefixed value (`Binary`: the DECODED
bytes), fed in ascending name order — and the assertions are ours. The `vectors`
lane pins that implementation against the goldens in `protocols/queen-sqs/src/md5.rs`
BEFORE anything is compared to a live answer, so a red MD5 assertion in the `sqs`
lane is the facade and never the suite. Three implementations in three languages
agreeing is what those goldens are worth.

The `probe` lane also pins how a facade error becomes a JS exception, which is
the thing the four-spelling assertions rest on:

| facade renders | the SDK produces |
|---|---|
| JSON `__type: com.amazonaws.sqs#QueueDoesNotExist` + `x-amzn-query-error: AWS.SimpleQueueService.NonExistentQueue;Sender` | `QueueDoesNotExist` (a modelled class), `name` = `QueueDoesNotExist`, `Code` = `AWS.SimpleQueueService.NonExistentQueue`, `Type` = `Sender` |
| XML `<Error><Type>Sender</Type><Code>NotFound</Code>` (404) | `NotFoundException`, `name` = `NotFoundException`, `Code` = `NotFound`, `Type` = `Sender` |
| a code SQS's own model does not carry (`InvalidParameterValue`, `SignatureDoesNotMatch`) | a bare `SQSServiceException` whose `name` is the shape and whose `Code`/`Type` still arrive — three spellings, no class |

## The inventory

**`sqs`** — queue CRUD and the idempotent create in its three provisioner shapes;
`GetQueueUrl` / `ListQueues` with prefixes; the attribute catalog under `All`,
exact selection, unknown-name refusal, and `SetQueueAttributes` merging; tags;
`SendMessage` with String / Number / Binary / custom-label attributes and a
system attribute, and all three MD5s; the full round trip including `Binary` as
`Uint8Array`; `SendMessageBatch` with per-entry digests; the four batch refusals
and the two size refusals; long polling (short, full-window, early return);
`ChangeMessageVisibility` extend / terminate / stale handle; redelivery after a
lease lapses; delete, double-delete, forged handle, and a partial-failure
`DeleteMessageBatch`; per-message `DelaySeconds` through the timers path and the
delayed counter; `PurgeQueue` and its 60-second cooldown; FIFO ordering,
deduplication and the `SequenceNumber` round trip (C-SQS-3); DLQ redrive with the
carried receive count, `queen.originalMessageId`, `queen.sourceQueue` and
`ListDeadLetterSourceQueues`; the error catalog; `DeleteQueue` and its tombstone.

**`sns`** — topic create (idempotent and conflicting), attributes and counts,
`ListTopics` paging; subscribe, subscription attributes, idempotence,
`ListSubscriptionsByTopic`, the unknown-topic refusal and the two v0 subscriber
refusals; the notification envelope field by field, including the absent
signature fields; raw message delivery; filter policies both applied and
removed; `PublishBatch`; fan-out to two queues under one publish id; FIFO topics
with ordering and deduplication; unsubscribe and delete-topic cascade.

**`consumer`** — twenty messages through a `handleMessage` loop where three
handlers throw: the throw becomes a `ChangeMessageVisibility`, the message comes
back exactly once with `ApproximateReceiveCount` at 2, every message is processed
exactly once, no SQS-API error is emitted, and the queue drains. Then twelve
through `handleMessageBatch` returning a SUBSET: the acked messages never come
back, the three left out do, and the queue drains.

## Two things to know before adding an assertion

**D2 — a standard queue's in-flight ceiling is its partition count**
(`M0_SMOKE.md`). A receive is N parallel `batch=1` pops, and a lane with a live
claim serves no second pop, so a loop that receives ten times and expects ten
messages is wrong in the SUITE. Exhaustive reads go through `drainDeleting`
(receive, delete, repeat — what a real consumer does); assertions that need
several handles alive at once go through `hold`, which sends filler rather than
waiting on a lane that will not open. The consumer lane is the shape that copes
with D2 naturally, which is why its batch assertions say "at least one poll
returned more than one message" rather than assuming full batches.

**Names are unique per run.** `DeleteQueue` arms a 60-second
`QueueDeletedRecently` tombstone exactly as AWS does, so a suite with fixed names
could not be run twice inside a minute. Every queue and topic carries the run id,
and the runner deletes what it made on the way out.

## Files

```
run.mjs                 the runner: lanes, the reachability guard, teardown, the RESULT line
lib/report.mjs          ok / FAIL / RESULT, structural equality, the exit status
lib/stack.mjs           the stack from the environment, both clients, the protocol recorder
lib/md5.mjs             AWS's three digests, implemented here because the SDK checks one
lib/sdk-md5.mjs         what the installed major validates, read from its own source
lib/queue.mjs           makeQueue / drainDeleting / hold / depth / the four-spelling error assertions
lib/versions.mjs        which dependency versions this run exercised
scenarios/vectors.mjs   the MD5 goldens (offline)
scenarios/probe.mjs     the SDK's own behaviour, against a stub (offline)
scenarios/sqs.mjs       the SQS inventory
scenarios/sns.mjs       the SNS inventory
scenarios/consumer.mjs  the sqs-consumer worker loops
```
