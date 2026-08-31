# queen-sqs · the Go client matrix suite

`aws-sdk-go-v2` (`service/sqs` + `service/sns`) driven against a live facade, a
live broker and a live Postgres. Same contract as `compat/smoke_m0.py` and
`compat/smoke_m4_sns.py`, a different client — and the differences are the
reason this rig exists rather than a third copy of the same assertions.

## How the runner invokes it

One command, everything from the environment:

```sh
GOWORK=off go -C queen-sqs/compat/go-sdk run .
```

It works from any directory (`go -C` changes into the module first), needs no
build step of its own, and exits nonzero when anything failed. The stack must be
up and its environment exported:

```sh
queen-sqs/compat/rig.sh up
source queen-sqs/compat/.rig/env.sh
GOWORK=off go -C queen-sqs/compat/go-sdk run .
```

**`GOWORK=off` is not optional.** The repository root carries a `go.work` that
`use`s `clients/client-cli` and `clients/client-go`; this module is inside that
tree and deliberately NOT a member of the workspace (adding it would change a
file this suite does not own). In workspace mode the go command refuses to
operate on a module the workspace does not use, so every `go` invocation here —
`run`, `build`, `vet`, `test` — carries `GOWORK=off`.

### What it reads from the environment

| Variable | Default | What it is |
|---|---|---|
| `QUEEN_SQS_ENDPOINT` | `http://127.0.0.1:19324` | the facade's listener |
| `QUEEN_SQS_REGION` | `queen-1` | SigV4 scope and ARN region |
| `QUEEN_SQS_ACCOUNT` | `000000000000` | the account segment of queue URLs and ARNs |
| `QUEEN_SQS_PARTITIONS` | `8` | the facade's `QUEEN_SQS_DEFAULT_PARTITIONS`, asserted |
| `AWS_ACCESS_KEY_ID` | `QSQSTEST` | the rig's static credential |
| `AWS_SECRET_ACCESS_KEY` | `qsqssecret` | its secret |

The defaults are the rig's own, exactly as the python suites default: a person
with the stack up and nothing sourced gets the stack they have rather than a
usage error. `AWS_SESSION_TOKEN` is never read or sent — the facade's third
credential field is a Queen bearer, not an AWS session token, and signing with
one would fail verification.

The `aws.Config` is assembled by hand rather than through
`config.LoadDefaultConfig`, so a developer machine with a real AWS profile, an
SSO cache or an IMDS route cannot change what this suite signs with.

## What it prints

The suite contract, verbatim in what matters. Below is the shape with a failure
in it, because that is the shape worth reading; the run of record
([`../MATRIX.md`](../MATRIX.md), 2026-08-31) was 239 passed and 0 failed, with
the same two protocol lines at 219 and 33 requests.

```
# endpoint http://127.0.0.1:19324  region queen-1  account 000000000000  partitions 8  run 3f9a1c22
# client github.com/aws/aws-sdk-go-v2/service/sns v1.44.1, …/service/sqs v1.48.1, …/smithy-go v1.28.1, go1.25.7
ok CreateQueue.url
FAIL Redrive.receive_count_continues_rather_than_restarting: got "1", want "3"
# note SQS FIFO dedup MessageId: first=… deduplicated-send=… (same)
# protocol spoken: sqs: AWS JSON 1.0 (application/x-amz-json-1.0) — 214 request(s)
# protocol spoken: sns: Query/XML (application/x-www-form-urlencoded) — 31 request(s)
# 137 passed, 1 failed
#   failed: Redrive.receive_count_continues_rather_than_restarting
RESULT: FAIL
```

One `ok NAME` / `FAIL NAME: detail` per assertion, `#` lines that are
observations rather than verdicts, one `RESULT:` line last, nonzero exit on
failure.

**The protocol line is a measurement.** The clients are built with
`ClientLogMode: aws.LogRequest`, and the suite's own `logging.Logger`
(`protocol.go`) reads the shape off smithy-go's request dump: `X-Amz-Target` +
`application/x-amz-json-1.0` is AWS JSON 1.0, a form-encoded body with neither is
Query/XML. The SERVICE comes from the SigV4 credential scope in the request's own
`Authorization` header (`Credential=…/…/…/sqs/aws4_request`) — the only way to
tell an SNS request from an SQS one when both go to one port and the Query
protocol keeps its action in a body the dump does not include. The logger prints
nothing; it only counts. The `protocols` scenario then turns the tally into
three assertions, so "SQS spoke JSON and SNS spoke Query" is a verdict and not a
claim.

## What it covers

| Scenario | What it pins |
|---|---|
| `queue_crud` | CreateQueue idempotency (identical / bare / subset), `QueueNameExists` on a conflict, GetQueueUrl, ListQueues prefix |
| `queue_attributes` | `All` and selected reads, the depth counters, `queen.partitions` = the rig's width, SetQueueAttributes merges, `InvalidAttributeName` both ways |
| `queue_tags` | tags at create, TagQueue add + overwrite, UntagQueue |
| `send_receive_delete` | MessageAttributes (String, Number, Binary, custom label), `AWSTraceHeader`, all three MD5s, the system view, the DEPRECATED `AttributeNames` spelling, delete + double-delete + forged handle |
| `sdk_checksum_validation` | that the SDK's own body-digest check is armed, on send AND on receive |
| `batches` | SendMessageBatch of 10 with per-entry digests, the three batch refusals, DeleteMessageBatch PARTIAL failure (`SenderFault`) |
| `long_poll` | a `WaitTimeSeconds=3` poll really waits, a short poll does not, a waiting message returns early |
| `visibility` | extend hides, terminate (0) returns it, receive count becomes 2, the stale handle is refused |
| `fifo_group_ordering` | two interleaved groups, each delivered in publish order |
| `fifo_sequence_number` | `SequenceNumber` on the send AND on the receive (C-SQS-3), group id and dedup id read back |
| `fifo_deduplication` | a repeated `MessageDeduplicationId` succeeds and is never delivered, dated by a marker |
| `dlq_redrive` | `maxReceiveCount=2`, two nacked deliveries, the third never handed out, arrival on a REAL receivable DLQ with `queen.originalMessageId`, `queen.sourceQueue` and a receive count that CONTINUES at 3, source drained, `ListDeadLetterSourceQueues` |
| `sns_*` | topic + subscription lifecycle, the `Notification` envelope, raw delivery, filter policy match/no-match/removal |
| `errors` | `QueueDoesNotExist` in five shapes, a foreign account, a wrong secret |
| `delete_queue` | the delete, and the 60-second `QueueDeletedRecently` tombstone |
| `protocols` | which codec each client actually spoke |

## The two things this suite adds over the python ones

**1. A digest the client checks itself.** botocore validates no MD5 for SQS —
`smoke_m0.py`'s header says so and is right. `aws-sdk-go-v2` does:
`service/sqs/cust_checksum_validation.go` recomputes `MD5OfMessageBody` on
SendMessage and SendMessageBatch and `MD5OfBody` on every message ReceiveMessage
answers, and fails the CALL when one disagrees. So a wrong body digest here is
not an assertion failing, it is every send and every receive erroring.

Be exact about the half it does **not** check: `MD5OfMessageAttributes` and
`MD5OfMessageSystemAttributes` are validated by aws-sdk-go **v1** and by the
Java, JS and .NET SDKs, and **not** by v2. Those two are computed by this suite
(`awsmd5.go`, AWS's length-prefixed binary encoding) and asserted directly, the
same way the python suite does it.

`sdk_checksum_validation` proves the SDK's check is armed rather than trusting
the documentation: it reads `Options().DisableMessageChecksumValidation`, and
then corrupts one `SendMessage` answer and one `ReceiveMessage` answer on the
way back through the middleware stack and requires each call to fail. (The
corruption is client-side and after the round trip, so the messages really are
on the queue; the reads that follow account for them.)

**2. An exception picked from a different field, and a code taken from a third.**
botocore maps SQS's JSON errors from `QueryErrorCode`; smithy-go picks the TYPE
from `__type` alone — it strips the `com.amazonaws.sqs#` namespace and matches
the SHAPE name. So every `expectAPIError(…, new(*types.QueueDoesNotExist))` here
tests a byte no python client reads. SNS's half goes through the Query/XML
deserializer instead, which is a third path again.

`ErrorCode()` is then a fourth: every generated
`awsAwsjson10_deserializeError<Shape>` in `service/sqs@v1.48.1` ends with
`awsQueryErrorCode := getAwsQueryErrorCode(response); if awsQueryErrorCode != ""
{ output.ErrorCodeOverride = &awsQueryErrorCode }`, so the LEGACY Query code from
the `x-amzn-query-error` header wins over the shape name — for modelled errors
too, and against real AWS as much as against this facade, because the header is
AWS's own and the override exists to serve customers migrating off the Query
protocol. A Go program switching on `apiErr.ErrorCode()` therefore sees
`AWS.SimpleQueueService.NonExistentQueue`, never `QueueDoesNotExist`. Assertions
here still NAME the shape, the way the catalog and every other suite in this tree
names it; `expectAPIError` compares `wireCode(shape)`, and `legacyCode`
(`helpers.go`) is the pairing table — the same one `smoke_m0.py` carries as
`ERROR_CODES` and `src/error.rs` carries as its catalog. Both halves of a pair
are pinned by one call: the shape through `errors.As`, the legacy code through
the comparison.

## Offline checks

No rig needed, and all three are clean:

```sh
GOWORK=off go -C queen-sqs/compat/go-sdk build ./...
GOWORK=off go -C queen-sqs/compat/go-sdk vet ./...
GOWORK=off go -C queen-sqs/compat/go-sdk test ./...
```

`go test` runs three files that need no stack:

- `awsmd5_test.go` checks this suite's MD5 implementation against
  `queen-sqs/src/md5.rs`'s own golden vectors — two independent implementations
  against the same published values, where a vector invented here would only
  prove the file agrees with itself.
- `protocol_test.go` checks the request-dump parser against realistic
  `httputil.DumpRequestOut` renderings, including an unsigned request and an
  unknown content type.
- `clients_test.go` points two real clients at a throwaway HTTP server and reads
  the tally back, which is the only way to check the claim that matters and is
  not this repository's to make: that `service/sqs` still speaks AWS JSON 1.0
  and `service/sns` still speaks Query. A future SDK major could change either
  without a line here failing to compile.

## Things worth knowing before reading a failure

- **A standard queue hands out at most one message per lane at a time**
  (`M0_SMOKE.md`, divergence D2). Every exhaustive read is a
  receive-DELETE-repeat loop; the one place that needs several handles alive at
  once (`DeleteMessageBatch`) sends an extra message rather than waiting on a
  lane that will not open. Two scenarios that count deliveries of one specific
  message (`sdk_checksum_validation`, `dlq_redrive`) create their queues with
  `queen.partitions=1` so that "the next receive gets it" is not luck.
- **`WaitTimeSeconds=0` cannot be sent from Go.** aws-sdk-go-v2 models it as a
  non-pointer `int32` and omits it when zero, so the short-poll assertion is
  driven by the queue's own `ReceiveMessageWaitTimeSeconds=0` instead. This is a
  client difference, not a facade one; boto3 sends the explicit zero.
  `ChangeMessageVisibility`'s `VisibilityTimeout` is a REQUIRED member and IS
  serialized at zero, so terminate-the-lease is exercised exactly.
- **Names are unique per run.** `DeleteQueue` arms a 60-second
  `QueueDeletedRecently` tombstone, so a suite on fixed names could not be run
  twice inside a minute. Everything the run made is torn down at the end, on its
  own context, newest first.
- **Two divergences are pinned as assertions**, not left in comments, so a
  change to either is loud: a dead-lettered copy has a NEW MessageId (the copy is
  a new row in a different queue and the broker mints ids — the original rides in
  `queen.originalMessageId`), and the notification envelope carries no
  `Signature`, `SigningCertURL` or `UnsubscribeURL`.
- **One observation is recorded, not judged** (`# note`): whether a repeated
  `MessageDeduplicationId` answers the ORIGINAL message's id on the SQS path.
  AWS documents that it does; `smoke_m4_sns.py` records the SNS path minting a
  fresh one. Only a differential run against real AWS settles it, so this suite
  measures it and does not vote.
