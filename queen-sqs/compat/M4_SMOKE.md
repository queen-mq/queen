# queen-sqs M4 — SNS, first live run

The SNS half driven by boto3 against a REAL broker and a REAL Postgres, which is
the half `queen-sqs/src/sns/*` and `src/http_tests.rs` cannot do: those tests
drive `FakeQueen`, so a fan-out transaction always commits, a repeated
`MessageDeduplicationId` is a map lookup, and the subscription registry is in the
same process as the publisher. Everything below is what changed when those three
stopped being true — and, because SNS speaks the Query protocol, it is also the
first time anything in `compat/` has exercised the XML codec at all.

Run: 2026-08-31, branch `queen-kafka`, `queen-sqs` at the working tree of that
morning, broker `server/target/debug/queen`, Postgres 16 in the rig's container.

## How it was run

```
queen-sqs/compat/rig.sh down && queen-sqs/compat/rig.sh up   # a fresh stack
source queen-sqs/compat/.rig/env.sh
python queen-sqs/compat/smoke_m0.py        # the M0 regression
python queen-sqs/compat/smoke_m4_sns.py    # this milestone
queen-sqs/compat/rig.sh down
```

Stack: throwaway Postgres on 55440 (container `qsqs-rig-pg`), debug broker on
26632 with `QUEEN_APPLY_SCHEMA=true` and JWT off, facade on 19324 with
`QUEEN_SQS_AUTH=sigv4`, one credential `QSQSTEST:qsqssecret:devtoken`, region
`queen-1`, account `000000000000`, `QUEEN_SQS_DEFAULT_PARTITIONS=8`.

## Results

| suite | client | protocol spoken | passed | failed |
|---|---|---|---|---|
| `smoke_m0.py` (regression) | boto3 1.43.83 / botocore 1.43.83 | AWS JSON 1.0 | 99 | 1 |
| `smoke_m4_sns.py` | boto3 1.43.83 / botocore 1.43.83 | **Query / XML** (SNS) + AWS JSON 1.0 (SQS) | 92 | 0 |

**The M4 suite is green on its first live run and reproduces green on a
torn-down and rebuilt stack.** No test in it has ever failed, so — unlike M0 —
there is no "what the suites got wrong" section to write.

**The M0 regression moved from 95/2 to 99/1**: `M0_SMOKE.md` D1 (the idempotent
`CreateQueue` naming no attributes) is FIXED and now verified against the live
broker, along with the two tag assertions that landed with it. The single
remaining failure is D2 — `InFlight.three_messages_are_all_receivable_at_once`,
`got 1, want 3` — unchanged, and it has an SNS-side consequence recorded below.

Neither log recorded a panic. The facade emitted exactly one WARN, the
`SignatureDoesNotMatch` the M0 suite provokes on purpose; the broker's one WARN
is the same spool-directory fallback M0 recorded, which has nothing to do with
the facade. The one INFO the SNS path raised is the dedup line, once, which is
the proof that D1 below went through the broker's real refusal and not a
short-circuit:

```
INFO sqs: a publish repeated a MessageDeduplicationId inside the queue's window
          and wrote nothing; the original message stands suppressed=0
          topic=m4-fifo-dedup-…fifo
```

**Protocol note, and it closes a gap M0 left open.** `M0_SMOKE.md` recorded that
both its clients speak AWS JSON 1.0 and that "the Query/XML codec is therefore
not exercised by these two suites at all". boto3's SNS client speaks Query
(`apiVersion` 2010-03-31, form-encoded in, XML out) and its SQS client speaks
JSON, so every assertion in this file crosses BOTH codecs on one listener and one
SigV4 verifier — the SNS half signed for service `sns`, the SQS half for `sqs`.
The lift table in `proto/query.rs` (`Attributes.entry.N.key`,
`MessageAttributes.entry.N.Name`, `PublishBatchRequestEntries.member.N`) is
exercised end to end for the first time and needed nothing.

## Discrepancies — facade vs. real SNS

Line numbers are from the working tree of the run and `queen-sqs/src` is under
active edit, so trust the function names over the numbers.

### D1. A deduplicated FIFO publish answers a NEW MessageId, not the original's

**The one finding of this run that is not a decision somebody already wrote
down.**

**Request.** A FIFO topic subscribed by a FIFO queue.
`Publish{Message: "the original", MessageGroupId: "g-dup",
MessageDeduplicationId: D}`, then `Publish{Message: "the duplicate, which must
not be delivered", MessageGroupId: "g-dup", MessageDeduplicationId: D}` —
the SAME D, a DIFFERENT message.

**Expected.** Both calls succeed and only the first message is delivered (this
holds), and — per `sns/publish.rs`'s own module header — the second call is
answered *the original message's id*:

> **A repeated `MessageDeduplicationId` is a SUCCESS.** … SQS answers a repeated
> dedup id with a success and the original message's id, so this does too.

**Actual.** The two calls answer two different ids. Measured, twice, on two
stacks:

```
# note FifoTopic dedup MessageId: first=e38017cc-…  deduplicated-publish=3b39f44d-…  (DIFFERENT)
```

Delivery is right: the queue receives the original and then the marker, and the
duplicate's body never appears. It is only the ANSWER that is wrong.

**Suspected.** `sns/publish.rs`, `publish`:

```rust
let prepared = Prepared::of(&topic, params)?;   // mints a fresh uuid
guard_size(&[&prepared], Batched::No)?;
deliver(ctx, &topic, &prepared).await?;         // swallows queen::Error::Duplicate
Ok(json!({ "MessageId": prepared.message_id }))
```

`Prepared::of` mints `uuid::Uuid::new_v4()` BEFORE anything is written, and
`commit` turns the broker's `Duplicate` into `Ok(())` without ever learning which
id won. Answering the original's id would mean reading it back — the broker knows
it, since the dedup index is keyed by the same `transactionId` — which is a
second round trip on the duplicate path only.

**Why it matters.** A publisher that retries after a timeout gets an id that
names nothing: it cannot be correlated against the notification any subscriber
actually received (which carries the FIRST publish's id, and this suite asserts
that it does), so an end-to-end trace built on `MessageId` breaks exactly on the
retry it was built for. This is the whole reason SQS returns the original id.

**Two readings, and the differential lane settles it in one call.** SQS's
`SendMessage` documents returning the original message's id for a repeated dedup
id, and the module header above asserts SNS does the same. AWS's SNS `Publish`
page is not explicit, and it is entirely possible real SNS also mints a new id —
in which case today's behaviour is right and the module header is the thing to
fix. EITHER WAY one of the two is wrong today, which is why this is recorded as a
discrepancy rather than left in a comment. The suite therefore MEASURES it and
prints it as a `# note` instead of failing on it.

### D2. A FIFO topic's whole fan-out into one queue is a single lane

`M0_SMOKE.md` D2 — in-flight messages capped by the queue's partition count — is
unchanged, still failing in the regression, and has a shape on this side that is
worth writing down separately because it is not a tuning knob.

On a STANDARD topic the fan-out hashes a fresh key across the target queue's
width (`push_for`), so D2 applies exactly as it does to `SendMessage` and
`QUEEN_SQS_DEFAULT_PARTITIONS` is the dial.

On a FIFO topic the lane is the `MessageGroupId`, on EVERY subscriber's queue —
which is what makes one publish order identically for all of them, and is the
property `FifoTopic.group_order_is_the_publish_order` passes on. The consequence
is that a FIFO topic publishing into one group delivers to each subscriber
through ONE partition, so that subscriber's concurrency for the topic is one
message at a time and no queue attribute widens it. A consumer that holds a
message for its visibility timeout stalls every later message of that group, for
every publisher.

This is SQS FIFO's own semantics for a single group and not a defect —
`MessageGroupId` is where a publisher buys concurrency back. It belongs in the
divergence register and in the docs because the number of groups is now a
CAPACITY decision on the publisher's side, which nothing in the SNS API hints at.

### D3. A FIFO publish answers no `SequenceNumber`

**Request.** `Publish` and `PublishBatch` on a FIFO topic.

**Expected.** AWS answers `{MessageId, SequenceNumber}` for a FIFO topic — a
128-bit number that increases per message group — and per-entry
`{Id, MessageId, SequenceNumber}` inside `PublishBatch`.

**Actual.** Measured live:

```
Publish response keys: ['MessageId']
PublishBatch successful entry keys: ['Id', 'MessageId']
```

**Suspected.** Stated, deliberately, in `sns/publish.rs`'s module header: a
transaction's push echoes carry no offset by construction (the wire builds them
without the `baseOffset` the stored procedure returned), and `POST /api/v1/push`,
which does answer one, is not a transaction and would forfeit the atomic fan-out.
The number is omitted rather than invented.

**Why it matters, and how little.** boto3 does not mind — an absent member is
simply absent, and no assertion in this suite needed it. It matters for a client
that ORDERS or DEDUPLICATES on the sequence number rather than on delivery order.
The fix, if the differential lane finds such a client, is a `baseOffset` on the
transaction's echoes — a broker change — and never a switch to `/push`.

### D4. A repeat `Subscribe` ignores the attributes it carries

**Request.** `Subscribe{TopicArn, Protocol: sqs, Endpoint: Q,
Attributes: {FilterPolicy: {"event":["one"]}}}`, then the same call with
`{"event":["two"]}`.

**Expected.** Unknown. AWS's sentence is *"if the requester already owns a
subscription with the specified attributes, that subscription's ARN is
returned"*, and it is silent about the case where the attributes differ.

**Actual.** The existing ARN is returned and the stored policy stays
`{"event":["one"]}`. Both halves are now asserted
(`Divergence.repeat_subscribe_answers_the_existing_arn`,
`…does_not_apply_the_new_attributes`) so that a change to either is visible.

**Why it matters.** It is the reconcile loop of every provisioner that manages
subscriptions declaratively: Terraform's `aws_sns_topic_subscription`, MassTransit
and JustSaying all re-`Subscribe` with the attribute set they want. Under this
behaviour a filter policy edited in the provisioner's source never reaches the
facade, and nothing reports drift. The counter-argument is the one in the source:
a `Subscribe` that silently replaced a live filter policy is a change nobody
asked for. AWS settles it in one call.

### D5. Two refusals AWS does not make

Both are `deliberate` in the divergence register, both were previously only
comments in `sns/admin.rs`, and both are now pinned live so that a relaxation is
a test change and not a silent one.

* **A standard topic cannot subscribe a FIFO queue**
  (`Divergence.standard_topic_to_a_fifo_queue_is_refused`). AWS permits it and
  chooses a group id itself. This facade refuses at `Subscribe` — where a client
  can read the reason — rather than inventing a group id per message and putting
  a FIFO consumer's ordering guarantee in the facade's hands without saying so.
  The fix, if it is wanted, is that decision, not a relaxation.
* **`ConfirmSubscription` can never succeed**
  (`Divergence.confirm_subscription_has_nothing_to_confirm`), and answers
  `InvalidParameter` naming the token. Every subscription this facade can create
  is same-account SQS, which AWS itself confirms at `Subscribe`, so no token is
  ever minted. The HTTP/S subscriptions that need the handshake are M6.

### D6. The notification carries no `Signature`, `SigningCertURL` or `UnsubscribeURL`

Recorded rather than argued: AWS writes all three and this deployment writes
none, which the suite asserts
(`Notification.carries_no_unverifiable_signature_fields`). A signature nothing
can verify, a certificate URL whose host AWS's own validator libraries pin to
`sns.*.amazonaws.com`, and an unsubscribe URL that would need a SigV4 signature
to work are three fields that would be worse present than absent. `SignatureVersion`
stays because it names the version a signature WOULD carry and clients compare it
as a string. Queue subscribers read none of the three; an HTTP/S subscriber would,
which is M6's problem.

## What was confirmed working against the real broker

Recorded because these are the things a `FakeQueen` suite proves least, and all
of them held:

- **The Query/XML codec, end to end, for the first time.** Every SNS call in
  this file is form-encoded in and XML out, signed for service `sns`, on the same
  listener the JSON SQS client is using at the same moment. Nothing in the lift
  table needed a client-side workaround, and botocore's Query parser accepted
  every answer including the empty ones (`DeleteTopic`, `Unsubscribe`, the two
  setters) and the list-of-pairs ones.
- **One publish is one MessageId, across subscribers.** A publish to a topic with
  two queues delivers to both, both notifications carry the PUBLISH's id, and the
  two SQS `MessageId`s (the broker's, one per delivery) differ — which is the
  observable half of "one publish is one `POST /api/v1/transaction`", and is true
  at AWS too.
- **The notification's shape is right in the body a subscriber actually reads.**
  Captured verbatim from the live queue:

  ```json
  {"Message":"hello","MessageAttributes":{"event":{"Type":"String","Value":"order.created"}},
   "MessageId":"6b24cad8-…","SignatureVersion":"1","Subject":"subj",
   "Timestamp":"2026-08-31T02:21:22.665Z","TopicArn":"arn:aws:sns:queen-1:000000000000:m4-sample-…",
   "Type":"Notification"}
  ```

  `Timestamp` is ISO-8601 with exactly three fractional digits and a `Z`, which is
  asserted by regex rather than by parse — a parser accepts shapes AWS never
  sends. A `Binary` message attribute travels as base64 under `Type: "Binary"`,
  and the envelope's attributes are NOT also written as SQS message attributes,
  so the two copies cannot disagree.
- **`RawMessageDelivery` really is raw.** The body is the published message byte
  for byte (`MD5OfBody` checked against Python's own md5), it is not an envelope,
  and the publish's message attributes arrive as SQS message attributes with
  their types intact and an `MD5OfMessageAttributes` beside them.
- **Filter policies are applied at publish, against a registry another process
  also writes.** `SetSubscriptionAttributes` then `Publish` is exact on one
  facade — the `forget_subscriptions` cache invalidation holds — and the three
  cases are separated: a non-matching attribute, an ABSENT attribute (which
  matches nothing, so a policy is a whitelist), and the matching one. Every
  negative is dated by a marker publish that must arrive, so no absence in this
  file is asserted on its own. Setting an EMPTY `FilterPolicy` removes it and
  delivery resumes.
- **`PublishBatch` really is per-entry.** Two good entries and one carrying a
  `MessageGroupId` on a standard topic: the two are delivered, the third is a
  `Failed` entry with `SenderFault=true`, `Code=InvalidParameter` and a message
  naming the member, and the delivered notifications carry exactly the
  `MessageId`s the `Successful` entries answered.
- **FIFO ordering survives five separate transactions.** Five publishes to one
  group, five deliveries, in publish order, with the publish ids in the same
  order — through a real broker where each publish is its own commit and the lane
  is a Postgres row somebody else could have taken.
- **Deduplication is the broker's, not the facade's.** The INFO line quoted above
  proves the second publish reached `queen::Error::Duplicate` and was translated,
  rather than being filtered in front of the store.
- **`DeleteTopic` cascades, and the cascade is durable.** After the delete:
  `GetTopicAttributes`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`
  on the orphaned ARN, `Publish`, `PublishBatch` and `Subscribe` are all
  `NotFound`/404; the account-wide `ListSubscriptions`, walked to the end of its
  pagination, no longer carries the subscription; `DeleteTopic` again succeeds
  (AWS documents it idempotent); and the subscriber's queue receives nothing from
  any of the refused calls. The subscription is proved LIVE before the delete, so
  the cascade is a cascade and not an empty assertion.
- **SNS's error spelling is SNS's, not SQS's.** One code, not two:
  `<Code>NotFound</Code>` with HTTP 404 and `<Code>InvalidParameter</Code>` with
  400, and botocore raises `NotFoundException` / `InvalidParameterException` off
  the shape name. An unknown `TopicArn` on `ListSubscriptionsByTopic` is
  `NotFound` and not an empty list — a client reads an empty list as "nothing is
  subscribed" rather than "you asked about the wrong topic".
- **`CreateTopic` is idempotent in all three shapes a provisioner uses** — the
  identical request, the request naming NO attributes, and a conflicting one
  (refused) — which is `M0_SMOKE.md` D1's rule applied on the topic side and
  correct here from the start.

Three observations that are not defects, recorded so nobody files them:

- **The notification's keys are in sorted order**, not AWS's declaration order
  (`Message`, `MessageAttributes`, `MessageId`, `SignatureVersion`, `Subject`,
  `Timestamp`, `TopicArn`, `Type`). JSON object order carries no meaning, every
  SNS consumer parses by field name, and the signature validators that DO build a
  canonical string build it from the parsed fields in their own fixed order.
- **`SubscriptionsPending` is structurally `0`** on every topic, and
  `PendingConfirmation` is `false` with `ConfirmationWasAuthenticated` `true` on
  every subscription. There is no state here that is anything else; see D5.
- **A subscription with no filter policy reports no `FilterPolicyScope`.** That
  is AWS's behaviour and the right one: a scope reported for a subscription with
  nothing to scope is drift a provisioner would try to reconcile for ever.

## What this suite deliberately does not cover

So the next reader does not mistake silence for a green light:

- **`MessageStructure=json`**, the per-protocol message document. Implemented
  (`select_message`) and unit-tested against the double; untouched here.
- **The filter grammar past an exact-match OR-list.** `prefix`, `suffix`,
  `numeric`, `anything-but`, `exists`, `$or` and `MessageBody` scope all have
  unit tests; this file proves only that the ENGINE is wired into the publish
  path against the live registry.
- **Topic and subscription tags** (`TagResource` / `UntagResource` /
  `ListTagsForResource`), and `ListTopics` / `ListSubscriptions` pagination past
  one page — the account listing is walked here, but never past 100 entries.
- **The chunked fan-out.** `MAX_FANOUT_PER_TRANSACTION` is 256 and the widest
  topic in this file has two subscribers, so the one place where a publish stops
  being atomic is untested. It needs a topic with 257 queues on it, which is a
  campaign of its own.
- **The 256 KiB ceiling and the ~250-byte envelope overrun** — the documented
  `accepted` divergence where a publish at the ceiling becomes a delivery over
  the target queue's `MaximumMessageSize`.
