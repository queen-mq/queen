# queen-sqs — the divergence register

Every place this facade answers something real SQS or real SNS does not, with the
reason it does, the sentence that has to travel with it, and the thing that would
tell us if it changed.

**Zero unexplained is the release gate** (PLAN_QUEEN_SQS.md, "Definition of
done"). Unexplained means a row that is not here, or a row here with no
classification. It does not mean zero rows: a facade over a different storage
model has divergences by construction, and the honest form of "100% compatible"
is that every one of them is written down, argued, and pinned by something that
fails when it moves.

## How to read a row

| field | what it means |
|---|---|
| **id** | Stable. `QS-…` is the SQS surface, `SN-…` the SNS surface. Cite the id in code comments and suite assertion names, never the row number. |
| **classification** | `deliberate` — we chose this, and choosing AWS's answer would cost something we are not willing to pay. `accepted` — we did not choose it, the storage model produced it, and it is inside a contract SQS already makes (at-least-once, "approximate", "up to N"). `OPEN` — measured, explained, and **nobody has taken the decision yet**. An OPEN row is what the release gate is actually about. |
| **the sentence** | The one line that must appear beside this behaviour wherever an operator or a client author meets it — the README, the webdoc page, an error message, a support answer. Not a summary of the row: the thing somebody has to be told. |
| **evidence** | The test, suite assertion or live measurement that fails if the behaviour changes. A row whose evidence is only a doc comment is a row that can rot; those are called out. |

Three cross-cutting non-goals from PLAN_QUEEN_SQS.md are the ground under several
rows and are not repeated in each: **IAM is not enforced** (QS-19), **the AWS
platform is not emulated** — Lambda triggers, EventBridge pipes, CloudWatch
metrics, KMS, none of which any facade can provide — and **quota theater is not
emulated** (QS-27).

Line numbers are deliberately absent: `protocols/queen-sqs/src` is under active edit, so
every pointer is a function or test NAME.

---

## Index

### SQS

| id | surface | class | behaviour |
|---|---|---|---|
| [QS-01](#qs-01) | ReceiveMessage, standard | **OPEN** | AWS holds 120,000 messages in flight per queue; here the ceiling is the queue's partition count, and a slow consumer blocks its lane |
| [QS-02](#qs-02) | ReceiveMessage, FIFO | accepted | AWS fills a receive from as many groups as it can; here one receive is one group |
| [QS-03](#qs-03) | ReceiveMessage | deliberate | `VisibilityTimeout=0` on a receive means "immediately visible to others" at AWS; here it means the queue's default |
| [QS-04](#qs-04) | ChangeMessageVisibility, FIFO | accepted | AWS returns THAT message; here a zero on any member returns the whole remainder of the claim |
| [QS-05](#qs-05) | ChangeMessageVisibility, FIFO | accepted | extending one message of a FIFO batch extends its batch-mates |
| [QS-06](#qs-06) | ChangeMessageVisibility | accepted | a change to a SHORTER visibility does not shorten a live lease |
| [QS-07](#qs-07) | ApproximateReceiveCount | accepted | inside a FIFO batch the count is the CLAIM's, so every message of one claim reports the same number |
| [QS-08](#qs-08) | ReceiveMessage attributes | accepted | `SenderId` and `ApproximateFirstReceiveTimestamp` are absent, including under `All` |
| [QS-09](#qs-09) | SequenceNumber | accepted | it counts within its own message group, where AWS's is unique across the queue |
| [QS-10](#qs-10) | SequenceNumber | accepted | absent — never synthesized — against a broker without C-SQS-3 |
| [QS-11](#qs-11) | DelaySeconds | accepted | a per-message delay ADDS to a queue-level `DelaySeconds` instead of replacing it |
| [QS-12](#qs-12) | ReceiveRequestAttemptId | accepted | AWS replays only "if none of the messages have been modified"; here it replays for as long as the record lives |
| [QS-13](#qs-13) | ReceiveRequestAttemptId | accepted | a receive answer over 48 KiB is not remembered, so a retry of that id is a fresh receive |
| [QS-14](#qs-14) | DeleteMessage, FIFO | accepted | duplicate suppression across a redelivery is best-effort; past its bounds a deleted message comes back once |
| [QS-15](#qs-15) | ReceiveMessage | accepted | `MaxNumberOfMessages` outside 1..10 is `InvalidParameterValue`, not `AWS.SimpleQueueService.ReadCountOutOfRange` |
| [QS-16](#qs-16) | the listener | accepted | a body over 2 MiB is `InvalidParameterValue`, not `RequestEntityTooLarge` |
| [QS-17](#qs-17) | PurgeQueue | deliberate | AWS answers at once and empties in the background; here the call does the work, and every prior receipt handle stops addressing anything |
| [QS-18](#qs-18) | GetQueueAttributes | deliberate | `SqsManagedSseEnabled`, `DeduplicationScope` and `FifoThroughputLimit` are answered only when a client set them |
| [QS-19](#qs-19) | AddPermission, Policy, KMS | deliberate | accepted, stored, answered — never enforced |
| [QS-20](#qs-20) | CreateQueue | deliberate | a Queen queue this facade's registry does not know is refused `QueueAlreadyExists`, never adopted |
| [QS-21](#qs-21) | CreateQueue | deliberate | an existing `.fifo` queue re-created without `FifoQueue=true` succeeds |
| [QS-22](#qs-22) | CreateQueue / CreateTopic | deliberate | tags on an existing resource are neither compared nor applied |
| [QS-23](#qs-23) | redrive | accepted | a dead-lettered copy has a NEW `MessageId`; AWS keeps the original's |
| [QS-24](#qs-24) | RedrivePolicy | deliberate | a queue may not name itself as its dead-letter target; cycles longer than one hop are not detected |
| [QS-25](#qs-25) | RedrivePolicy | accepted | a refusal echoes a bounded 256-character prefix of the document, where AWS echoes all of it |
| [QS-26](#qs-26) | GetQueueAttributes | deliberate | `AttributeNames=All` includes `queen.partitions` and `queen.dedupWindowSeconds`, which AWS does not define |
| [QS-27](#qs-27) | limits | deliberate | no quota emulation, and three supersets where Queen is wider than SQS |
| [QS-28](#qs-28) | the payload envelope | accepted | a native Queen payload that happens to be `{"b": …}` shaped is read as an SQS envelope |
| [QS-29](#qs-29) | SigV4 | deliberate | the credential scope's REGION is not enforced (the service is) |

### SNS

| id | surface | class | behaviour |
|---|---|---|---|
| [SN-01](#sn-01) | Publish, FIFO | deliberate | a repeated `MessageDeduplicationId` is a success carrying a FRESH `MessageId` |
| [SN-02](#sn-02) | Publish / PublishBatch | deliberate | no `SequenceNumber`, on a FIFO topic or anywhere |
| [SN-03](#sn-03) | Publish fan-out | accepted | past 256 matched subscriptions or 8 MiB the fan-out is more than one transaction |
| [SN-04](#sn-04) | Publish, FIFO | accepted | a duplicate rolls back the whole bundle, so a subscription added between two publishes receives nothing |
| [SN-05](#sn-05) | Publish fan-out | accepted | a `DeleteQueue` between the target read and the commit still delivers, into a re-provisioned orphan |
| [SN-06](#sn-06) | the notification | deliberate | no `Signature`, no `SigningCertURL`, no `UnsubscribeURL` |
| [SN-07](#sn-07) | Publish size | accepted | a publish at the 256 KiB ceiling lands ~250 bytes over the target queue's `MaximumMessageSize`, which AWS would drop |
| [SN-08](#sn-08) | Subscribe | deliberate | a standard topic cannot subscribe a FIFO queue; AWS permits it |
| [SN-09](#sn-09) | ConfirmSubscription | deliberate | it can never succeed, and says why |
| [SN-10](#sn-10) | Subscribe | deliberate | a repeat `Subscribe` answers the existing ARN and ignores the attributes it carries |
| [SN-11](#sn-11) | Subscribe | deliberate | the endpoint must be a queue this deployment knows; AWS accepts endpoints it cannot see |
| [SN-12](#sn-12) | GetTopicAttributes | deliberate | `Policy` and `EffectiveDeliveryPolicy` are answered only when a client set them |
| [SN-13](#sn-13) | Publish fan-out | accepted | past 10,000 subscriptions on one topic every publish delivers to the same prefix of them |
| [SN-14](#sn-14) | Publish, FIFO topic | accepted | a FIFO topic's fan-out into one queue is one lane, so a subscriber's concurrency per group is one message |
| [SN-15](#sn-15) | CreateTopic | deliberate | the `.fifo`/`FifoTopic` pairing is NOT relaxed for a re-create, where the queue side relaxes it |
| [SN-16](#sn-16) | Subscribe | deliberate | `sqs` is the only protocol; HTTP/S, email, SMS and Lambda are refused |

---

## SQS

### QS-01

**Standard-queue in-flight is bounded by the queue's partition count.**
`OPEN` · ReceiveMessage, standard queues · `compat/M0_SMOKE.md` D2

**AWS.** A standard queue has no head-of-line blocking, which is most of what
distinguishes it from a FIFO one. The in-flight ceiling is 120,000 per queue and
has nothing to do with any internal lane.

**Here.** A receive collects at most one message per FREE lane, because each pop
takes a durable claim on one `(partition, group)` for the whole visibility
timeout and a lane with a live claim serves no second pop (`actions::messages::
pop_exact`). Measured, 10 messages sent and read without deleting:

| `queen.partitions` | sent | in flight at once |
|---|---|---|
| 1 | 3 | 1 |
| 1 | 10 | 1 |
| 8 | 10 | 7 |
| 64 | 10 | 10 |
| 256 | 10 | 10 |

Nothing is lost or duplicated, every message is eventually receivable, and the
depth attributes account for all of them — so KEDA and every autoscaler still see
the blocked messages as work. It is a throughput and latency property, and a
semantic one.

**Why it is OPEN.** The mechanism is the plan's `exact` mode working as designed;
the CONSEQUENCE is not in the plan, and no decision has been recorded. The three
candidate answers are: raise `QUEEN_SQS_DEFAULT_PARTITIONS` and document (today's
de facto answer, and the default is already 64); take C-SQS-1 and the `amortized`
receive, which converts this into two different divergences (see [Not reachable
in this build](#not-reachable-in-this-build)); or leave it and say so in the
product docs. It also interacts with M3 redrive: a message stuck behind a slow
neighbour ages toward `maxReceiveCount` without ever being delivered.

**The sentence.** *A standard queue's concurrent in-flight ceiling is its
partition width, not SQS's 120,000: a consumer holding a message blocks the other
messages in that lane for a full visibility timeout. Size `queen.partitions` at
or above the number of messages you intend to have in flight at once — the
default 64 is invisible at ten messages and bites at a few hundred.*

**Evidence.** `compat/smoke_m0.py` — `InFlight.three_messages_are_all_receivable_at_once`,
which **is the one failing assertion in the live M0 run (109 passed, 1 failed)**
and is failing on purpose until this row is decided; `InFlight.every_message_is_
eventually_receivable` and `InFlight.depth_attributes_account_for_every_message`
pin the two things that are NOT wrong. Every other live suite works around it
deliberately: `compat/go-sdk` drains with receive-delete-repeat and pins
`queen.partitions=1` where it counts deliveries of one message.

---

### QS-02

**A FIFO receive fills from ONE message group.**
`accepted` · ReceiveMessage, `.fifo`

**AWS.** Fills a receive from as many groups as it can, so ten one-message groups
answer ten messages.

**Here.** One receive is a single pop with `partitions = 1`, so ten one-message
groups answer one message and the client polls again. Ordering within a group
only means anything if the whole run is claimed by one consumer, and a pop that
spread its batch over k lanes would hand k groups to one caller and claim each
for the whole visibility window.

**The sentence.** *`MaxNumberOfMessages` is a ceiling and never a promise: a FIFO
receive returns messages from one group, so a queue with many small groups needs
more polls than AWS would ask for. Nothing is delayed by more than one round
trip, and no group is ever split across consumers.*

**Evidence.** `actions::fifo::tests::many_groups_do_not_widen_one_receive`
(twenty groups, receive of 10, answers one — and the next receive gets the next
group, so the queue drains).

---

### QS-03

**`VisibilityTimeout: 0` on a receive applies the queue's default.**
`deliberate` · ReceiveMessage

**AWS.** Zero means the message is immediately visible to other consumers.

**Here.** Zero is also the broker's "use the queue's lease time", so the two
meanings collide on one number and a receive that sent `0` is indistinguishable
from one that sent nothing. Honouring AWS's meaning would mean popping and then
releasing — a second write transaction per message — and the release would end
the lease the receipt handle names, so the `DeleteMessage` the client is entitled
to make against that handle would answer success and delete nothing. A message
held for the queue's visibility is inside SQS's at-least-once envelope; a delete
that silently stops deleting is not.

**The sentence.** *A `VisibilityTimeout` of 0 on `ReceiveMessage` gives the
message the queue's default visibility rather than releasing it. To release a
message you already hold, use `ChangeMessageVisibility(0)`, which does exactly
what AWS's does.*

**Evidence.** `actions::messages::tests::a_zero_visibility_receive_takes_the_
queues_default`, which also asserts the property the divergence buys: the handle
from that receive still deletes.

---

### QS-04

**A FIFO `ChangeMessageVisibility(0)` returns the whole remainder of the claim.**
`accepted` · ChangeMessageVisibility, `.fifo`

**AWS.** Returns that one message to the queue.

**Here.** The lease covers a contiguous run of one group, and there is no verb
that releases one message of one — so the release names the claim's HEAD and
gives back everything from there. Naming the caller's own message instead would
be data loss, not a divergence: a `retry` ack commits everything strictly below
the position it names, so releasing at the third member would silently complete
the first two.

**The sentence.** *On a FIFO queue, terminating one message's visibility returns
the whole rest of the batch you were handed. They are messages nobody else could
see, and a FIFO consumer that reads its group again in order is already written
for it.*

**Evidence.** `actions::fifo::tests::zero_visibility_returns_the_whole_claim`
(released at member 0 and at member 2; nothing is committed, and the whole batch
is visible again at once).

---

### QS-05

**Extending one member of a FIFO batch extends its batch-mates.**
`accepted` · ChangeMessageVisibility, `.fifo`

**AWS.** Extends that one message.

**Here.** One claim is one lease. They are messages the same consumer is holding
and nobody else can see, so the visible effect is that a consumer which extends
the message it is working on keeps the rest of its own batch — which is what a
FIFO consumer processing a group in order wants — and never a message returned
late to somebody else.

**The sentence.** *A `ChangeMessageVisibility` on a FIFO message extends the
whole batch that message was delivered in. Nothing is returned to another
consumer earlier or later than it would have been.*

**Evidence.** `actions::fifo::tests::an_extension_extends_the_whole_claim`, and
the delete-set's TTL is extended with it (the test beside it).

---

### QS-06

**A change to a SHORTER visibility does not shorten a live lease.**
`accepted` · ChangeMessageVisibility · *classified in this register*

**AWS.** `ChangeMessageVisibility` sets the timeout, in both directions: a
message you hold for 12 hours can be given back to the queue in 30 seconds.

**Here.** The broker takes the GREATEST of the two expiries, so a renewal never
shortens a lease and never resurrects an expired one — the same rule that makes a
visibility change on an already-redelivered message answer `MessageNotInflight`
instead of stealing it back. The divergence is bounded by the ORIGINAL window: a
shortening request is a no-op, never an extension.

**The sentence.** *`ChangeMessageVisibility` only ever extends. Asking for a
shorter timeout than the one already running leaves the original running; asking
for `0` releases the message, which is the only way to give one back early.*

**Evidence gap — this row has no dedicated regression test.** The rule is
modelled in `queen::testing::FakeQueen::lease_extend` (LIVE leases only, and
`max`) and stated on `actions::messages::extend_lease`, and several tests depend
on it indirectly, but nothing fails if a future broker starts honouring a
shortening. Worth one test in `actions::messages`.

---

### QS-07

**`ApproximateReceiveCount` inside a FIFO batch is the claim's, not the
message's.**
`accepted` · message attributes · PLAN_QUEEN_SQS.md non-goal

**AWS.** Counts deliveries per message.

**Here.** The number is `log_consumers.attempt_count`, which lives on the
consumer row and counts CLAIMS. At claim width 1 — every standard queue — that is
exact. Inside a FIFO batch every message of one claim reports the same number,
including one that joined the lane between two deliveries and has only ever been
delivered once. Nothing in the log counts deliveries per message, so answering
per-message would mean a second store; the field's own name buys the slack, and
the plan lists it among the non-goals by name.

**The sentence.** *`ApproximateReceiveCount` is exact on a standard queue and
per-batch on a FIFO one: every message of one FIFO delivery reports the same
count. Do not build a poison-message rule on the difference between two members
of one batch — the queue's `RedrivePolicy` is the mechanism for that, and it
reads the same number.*

**Evidence.** `actions::fifo::tests::every_message_of_one_fifo_claim_reports_the_
same_receive_count`; the sentence is also on `queen::Message::delivery_attempt`.

---

### QS-08

**`SenderId` and `ApproximateFirstReceiveTimestamp` are absent.**
`accepted` · message attributes

**AWS.** Returns both on every message, and both under `AttributeNames=All`.

**Here.** Neither exists to answer. The sender's principal is not stored — this
facade knows who is RECEIVING, and writing the sender's identity into the payload
would add a fifth key to an envelope the plan fixes at four. Nothing records a
message's FIRST delivery: `deliveryAttempt` counts them, no clock remembers them.
Every SDK models `Attributes` as an open string map, so an absence reads as an
absence and not as a failure.

**The sentence.** *Two message attributes AWS always returns are not answered
here, under `All` or by name: `SenderId` (no sender principal is stored) and
`ApproximateFirstReceiveTimestamp` (no clock records a first delivery). Both are
absent rather than invented.*

**Evidence.** `actions::messages::system_view` (the catalog itself);
`compat/smoke_m0.py` `t_queue_attributes` and the M0 live run, which recorded
`ApproximateFirstReceiveTimestamp` absent under `All` and both clients minding
neither.

---

### QS-09

**`SequenceNumber` counts within its own message group.**
`accepted` · SendMessage / ReceiveMessage, `.fifo`

**AWS.** A 128-bit number that is unique across the queue and increases per
group.

**Here.** It is the absolute offset the push allocated, and on a FIFO queue the
partition IS the `MessageGroupId` — so it starts at 0 in every group and the same
number appears in each. It orders a group's own messages exactly, which is what a
FIFO consumer reads it for; an application that keys ACROSS groups by it
collides, and no queue-wide counter exists to answer with instead.

**The sentence.** *A `SequenceNumber` here orders messages within their
`MessageGroupId` and nowhere else: every group starts at 0, so the same number
appears in every group. Use `(MessageGroupId, SequenceNumber)` as the key, never
`SequenceNumber` alone.*

**Evidence.** `actions::messages::tests::a_sequence_number_counts_within_its_own_
group`; live in `compat/smoke_m0.py` `SequenceNumber.*` (send side ascending
within the group, receive side equal to what the send answered, absent on
standard queues).

---

### QS-10

**`SequenceNumber` is absent against a broker without C-SQS-3.**
`accepted` · ReceiveMessage, `.fifo`

**AWS.** Always present on a FIFO message.

**Here.** The receive side reads the `"offset"` the broker puts on every popped
message (C-SQS-3, `render_pop_parts` in `server/src/handlers/data.rs`). Against
an older broker that field is absent, `queen::Message::offset` parses as `None`,
and the attribute is simply not written. It is NOT synthesized: a number derived
from the transaction id or from the delivery position would order two messages of
a group differently from the way the log does, and a wrong `SequenceNumber` is
worse than an absent one for the only thing a client reads it for. The SEND side
has always answered one, because the push wire has carried the offset since C1.

**The sentence.** *A FIFO `ReceiveMessage` answers `SequenceNumber` only against a
broker with C-SQS-3. Deployed beside an older one, `MessageGroupId` and
`MessageDeduplicationId` are exact and `SequenceNumber` is absent — never
guessed.*

**Evidence.** `queen::tests::a_pop_body_without_an_offset_still_parses` (absence
tolerated, `None` and not `0`); `actions::messages::tests::an_absent_offset_
leaves_the_sequence_number_absent`; `…::a_fifo_receive_carries_the_sequence_
number_the_send_answered` and `…::a_standard_receive_answers_no_sequence_number`
for the present case.

---

### QS-11

**A per-message `DelaySeconds` ADDS to a queue-level `DelaySeconds`.**
`accepted` · SendMessage

**AWS.** The message's delay replaces the queue's.

**Here.** The message's delay is the timers API; the queue's is the broker's
`delayed_processing`, which hides a segment until it is that many seconds old and
cannot tell a segment a timer wrote from any other. So a 60-second message on a
30-second queue is visible after 90. Closing it would need a per-push "this one is
already late" flag on the broker, a core change the plan does not take. The bound
is the queue's own default, which is 0 on every queue that never set one.

**The sentence.** *A per-message `DelaySeconds` is added to the queue's own
`DelaySeconds` rather than replacing it. On a queue with the default of 0 — every
queue that never set one — the two are the same thing.*

**Evidence.** `actions::messages::tests::a_per_message_delay_is_the_timers_and_
the_queues_own_is_the_brokers`.

---

### QS-12

**A `ReceiveRequestAttemptId` replays after its messages were modified.**
`accepted` · ReceiveMessage, `.fifo`

**AWS.** Replays the recorded answer only *"if none of the messages have been
modified"*.

**Here.** It replays for as long as the record lives (TTL = visibility + slack).
A client that received under an attempt id, deleted the messages and then retried
that id inside the window is handed handles for messages that are gone — and a
delete under one answers success and deletes nothing, which is exactly AWS's own
contract for a stale handle. Closing it would mean naming the attempt id in every
delete-set so a delete could invalidate it: a second key on the hot path, to make
a retry-after-delete — a client bug — answer differently.

**The sentence.** *A `ReceiveRequestAttemptId` replays its recorded answer for the
life of the record, even if you have since deleted those messages. Retry an
attempt id to recover from a lost RESPONSE, which is what it is for; do not reuse
one after the messages were handled.*

**Evidence.** `actions::fifo::tests::an_attempt_id_replays_even_after_its_
messages_were_deleted`, which also pins that the stale handle answers success and
commits nothing.

---

### QS-13

**A receive answer over 48 KiB is not remembered for its attempt id.**
`accepted` · ReceiveMessage, `.fifo`

**AWS.** Remembers the answer whatever its size.

**Here.** The record is a KV value and the store refuses one over its own
ceiling. A facade that discovered that at write time would have to choose between
failing a receive whose messages are already claimed and lying about what it
stored, so the ceiling is checked first and an answer past it is answered and not
recorded — a retry of that id is then a fresh receive. A sampled WARN says so.

**The sentence.** *A `ReceiveRequestAttemptId` cannot replay an answer larger than
48 KiB; a retry of that id behaves as a new receive. It is a property of the
answer's size, not of the queue.*

**Evidence.** `actions::fifo::tests::an_answer_too_large_to_remember_is_still_
answered`.

---

### QS-14

**Duplicate suppression across a FIFO redelivery is best-effort.**
`accepted` · DeleteMessage, `.fifo`

**AWS.** A deleted message is gone.

**Here.** It is gone too — the delete-set makes an out-of-order delete safe and
the contiguous deleted prefix is acked, so nothing a client deleted is ever lost
and nothing it did not delete is ever acked away. What is best-effort is
SUPPRESSION on a redelivery: if an earlier message is never deleted, its lease
lapses and the whole suffix redelivers, and the marks that say "the client
already deleted this one" are read back only while they are there to read. They
are dropped by their own TTL, by a KV write this facade could not land, and by a
`getPrefix` page a lane overflowed (`MAX_PRIOR_SETS`, 64). After any of those the
message comes back to the client as a duplicate. The same boundary covers the
one cost two messages under one dedup key still carry: the ack names a key, so it
commits as far as the earlier namesake and the tail of a finished batch can
redeliver once.

**The sentence.** *A FIFO consumer must be idempotent. Deleting a message never
loses its neighbours, but a message you deleted can be delivered again after the
batch around it expires — this is SQS's own at-least-once envelope, and the
delete-set narrows it rather than closing it.*

**Evidence.** the module header of `actions::fifo` (the argument), and the
delete-set suite in `actions::fifo::tests` — the roster, the redelivery, the
carry-forward into the new claim, and the record's own TTL.

---

### QS-15

**`MaxNumberOfMessages` outside 1..10 is `InvalidParameterValue`.**
`accepted` · ReceiveMessage · error catalog

**AWS.** `AWS.SimpleQueueService.ReadCountOutOfRange`.

**Here.** Both are 400 Sender faults naming the parameter, the value and the
range, and no SDK branches on the difference — the value is a constant in the
client's own code, so the answer is read by a developer and not by a retry
policy. The catalog is the contract and a new code is a reviewed event
(`error.rs`); this one is recorded rather than invented, because its JSON 1.0
`__type` cannot be derived from the Query spelling and guessing it would put a
wrong `__type` on the wire for the sake of a matching string.

**The sentence.** *An out-of-range `MaxNumberOfMessages` is refused as
`InvalidParameterValue` and not under AWS's `ReadCountOutOfRange` name. Same
status, same fault, same information.*

**Evidence.** `actions::messages::tests::a_read_count_out_of_range_is_an_invalid_
parameter_here`, which also asserts that the code is NOT in the catalog — so
adding it is a reviewed change and not a drift.

---

### QS-16

**A body over 2 MiB is `InvalidParameterValue`.**
`accepted` · the listener · error catalog · *classified in this register*

**AWS.** Answers `RequestEntityTooLarge`.

**Here.** The closed catalog has no such code and we may not invent one. Of the
twenty-five it does have, `InvalidParameterValue` is the one that says "something
you sent is too big" with a `Sender` fault and a 400 rather than a retry-forever
5xx, and the message names the cap. The cap is on BYTES READ, applied before the
body is in memory: without it any unauthenticated client can ask the process to
buffer whatever it likes, and the signature that would have refused it is
computed over the body it is still reading.

**The sentence.** *A request body over 2 MiB is refused `InvalidParameterValue`
with the limit in the message, not `RequestEntityTooLarge`. It is a 400 and a
Sender fault, so an SDK stops instead of retrying two megabytes for ever.*

**Evidence.** `lib::tests::the_size_refusal_names_the_cap_and_blames_the_sender`.

---

### QS-17

**`PurgeQueue` is a synchronous delete-and-recreate.**
`deliberate` · PurgeQueue · PLAN_QUEEN_SQS.md D3

**AWS.** Answers immediately and empties in the background (*"the message
deletion process takes up to 60 seconds"*).

**Here.** Queen has no truncate (C-SQS-2 is a recorded option, not a milestone),
so emptying a queue means removing it and putting it back with the same options
bag: the client's call is as slow as the work. It is synchronous because the
alternative is worse here and not because async is hard — a purge that returned
before deleting would leave the queue answering receives for messages it has told
the client are gone, with no task record for anybody to poll. **Every receipt
handle minted before the call stops addressing anything**, because the lanes are
new rows with new ids; AWS says the same thing in its own words for messages sent
before a purge. The 60-second cooldown is emulated, and a client that times out
mid-purge meets it.

**The sentence.** *`PurgeQueue` does its work inside the request rather than in
the background, so the call takes as long as the queue is big, and every receipt
handle taken before it becomes invalid. The 60-second `PurgeQueueInProgress`
window behaves exactly as AWS's.*

**Evidence.** `actions::queues::tests` — `purge_empties_the_queue_and_leaves_it_
standing`, `a_purged_queue_is_empty_on_every_lane_and_takes_sends_again`,
`a_second_purge_inside_the_window_is_refused_and_deletes_nothing`,
`a_receipt_handle_from_before_a_purge_addresses_nothing_afterwards`.

---

### QS-18

**Three attributes AWS always reports have no default here.**
`deliberate` · GetQueueAttributes

**AWS.** Reports `SqsManagedSseEnabled`, `DeduplicationScope` and
`FifoThroughputLimit` on every queue that has them, set or not.

**Here.** Each is answered only when a client set it. `SqsManagedSseEnabled`
would have to be answered `true`, which claims an encryption at rest this facade
does not perform (SSE beyond accept-and-report is out of the plan by name); the
other two describe a FIFO throughput model whose truthful value here is not AWS's
default, because Queen deduplicates per PARTITION, which is per message group.

**The sentence.** *Three attributes are absent unless you set them, because
answering AWS's default would be a claim this deployment cannot keep:
`SqsManagedSseEnabled` (no encryption at rest is performed here),
`DeduplicationScope` and `FifoThroughputLimit` (deduplication is per message
group). The cost is that supplying AWS's own default for one of them in a
`CreateQueue` against a queue created bare is a mismatch, not a no-op.*

**Evidence.** `actions::queues::effective_attributes` (the catalog and the
argument); `registry::tests::create_answers_queue_already_exists_on_a_mismatch`
for the cost.

---

### QS-19

**IAM is accepted, stored, answered — and never enforced.**
`deliberate` · AddPermission / RemovePermission / `Policy` / `RedriveAllowPolicy` / KMS · PLAN_QUEEN_SQS.md non-goal 1

**AWS.** Evaluates the queue policy on every call, and `AddPermission` changes
who can do what.

**Here.** Authorization is QUEEN's, over the SigV4 keypair. `AddPermission` and
`RemovePermission` are validated as far as SQS validates them (the queue must
exist, the label is required) and then do nothing. `Policy`, `RedriveAllowPolicy`
and the KMS pair are stored and answered back verbatim, because Terraform and
MassTransit set them unconditionally and an attribute the facade refused to store
would fail an apply over a document nothing was going to read. The one outcome
the plan forbids is emulating the model: a client told its policy is in force
when nothing reads it. Same honesty as the kafka `SECURITY_DISABLED` answer.

**The sentence.** *SQS/SNS access policies are stored and echoed, never
evaluated. Authorization on this endpoint is the SigV4 keypair and the Queen
token behind it — a policy that denies everything denies nothing, and a policy
that grants another account grants nothing.*

**Evidence.** `http_tests::add_permission_is_accepted_validated_and_enforced_by_
nothing`; `actions::permission`; `registry::MUTABLE`'s own doc comment.

---

### QS-20

**A Queen queue this facade's registry does not know is never adopted.**
`deliberate` · CreateQueue · *classified in this register*

**AWS.** Has no such case: every queue is an SQS queue.

**Here.** `CreateQueue` over a Queen queue with no registry record is refused
`QueueAlreadyExists` — AWS's own code for "a queue of this name exists and is not
the one you described" — because `/configure` is a whole-row upsert: adopting it
would rewrite a live native queue's `leaseTime`, its retry budget and, worst, turn
retention ON at four days, which deletes data nobody asked to delete.

**The sentence.** *An SQS name may not be pointed at a Queen queue that native
producers already use: `CreateQueue` refuses it rather than reconfiguring it,
because adopting it would rewrite that queue's retention and lease settings.
Mixed native/SQS traffic on ONE queue is supported — create it through the SQS
API first.*

**Evidence.** `actions::queues::tests::a_native_queen_queue_is_never_
reconfigured_into_an_sqs_one` (refused, nothing configured, no record written).

---

### QS-21

**An existing `.fifo` queue re-created without `FifoQueue=true` succeeds.**
`deliberate` · CreateQueue · `compat/M0_SMOKE.md` D1

**AWS.** Undocumented for this exact shape. The `CreateQueue` page's looser
sentence would refuse it; the `QueueNameExists` page's rule — the error is
returned "only if the request includes attributes whose values differ from those
of the existing queue" — licenses it.

**Here.** The comparison is one-directional over the attributes the REQUEST
supplies, so a create that names none of them wins the queue's URL. The suffix
already DECLARES the type, so a re-create that omits `FifoQueue=true` is not
ambiguous. The same request for a queue that is NOT there is still the bad create
it always was.

**The sentence.** *An idempotent `CreateQueue` is compared only against the
attributes it supplies, so a worker that re-creates a queue at start-up boots
against a queue Terraform made with non-default attributes. For a `.fifo` name
that includes `FifoQueue`: the suffix declares the type on a re-create, and is
still required on a first create.*

**Evidence.** `registry::tests::a_create_that_names_no_attribute_answers_the_
existing_queue`, `…::the_stamped_width_is_not_a_supplied_attribute`,
`…::a_fifo_queue_is_re_created_without_repeating_the_attribute`,
`…::a_fifo_re_create_still_validates_its_attributes`; live in `compat/smoke_m0.py`
(the fix moved the M0 run from 95/2 to 99/1).

---

### QS-22

**Tags on an existing resource are neither compared nor applied.**
`deliberate` · CreateQueue / CreateTopic · *classified in this register*

**AWS.** Undocumented for a re-create carrying different tags.

**Here.** Tags are not attributes: they are not part of the identity a create
compares, and they are not applied to a resource that already exists.
`TagQueue`/`TagResource` is the action that changes them.

**The sentence.** *A `CreateQueue` or `CreateTopic` on a resource that already
exists ignores the `tags` it carries — it neither refuses over them nor writes
them. Use `TagQueue`/`TagResource` to change tags on a live resource.*

**Evidence.** `registry::tests::tags_on_a_re_create_are_neither_compared_nor_
applied`, `actions::queues::tests::a_re_create_with_different_tags_succeeds_and_
does_not_retag`; both halves asserted live in `compat/smoke_m0.py`. **This is
half of a differential question** — see [Q2](#q2).

---

### QS-23

**A dead-lettered copy has a new `MessageId`.**
`accepted` · redrive

**AWS.** Keeps the message's id across a move to the dead-letter queue.

**Here.** It cannot: the copy is a new row in a different queue and the broker
mints ids. The original rides in the envelope and is surfaced as the system
attribute `queen.originalMessageId`, beside `queen.sourceQueue` — without it a DLQ
consumer would have no correlation back to the message it is holding the remains
of, which is the first thing anybody debugging a dead-letter queue asks for. Both
are `queen.`-prefixed so neither can ever collide with an attribute AWS defines.
The receive COUNT does continue rather than restarting, which is AWS's rule.

**The sentence.** *A dead-lettered message arrives with a new `MessageId`; the id
it had on the source queue is the `queen.originalMessageId` attribute, and the
queue it came from is `queen.sourceQueue`. Correlate on those, not on
`MessageId`.*

**Evidence.** `compat/go-sdk/dlq.go` — `Divergence.the_dead_letter_copy_has_a_new_
message_id`, pinned as an assertion beside `Redrive.the_copy_names_the_message_it_
was_made_from` and `Redrive.receive_count_continues_rather_than_restarting`.

---

### QS-24

**A queue may not name itself as its dead-letter target.**
`deliberate` · RedrivePolicy

**AWS.** Its own validation may accept it.

**Here.** It is refused at `CreateQueue` and at `SetQueueAttributes`, because the
consequence here is not a chain but a live-lock: the copy is a new message with a
fresh delivery budget, so it is received once, found to be over the threshold by
the count it carries, moved to itself again, and so on for as long as anybody
polls — an unbounded rewrite of one message, costing a transaction each time.
**Cycles longer than one hop are NOT detected**: they would need a walk of the
whole target graph on every `SetQueueAttributes`, and they are visible to an
operator as a queue whose depth never falls.

**The sentence.** *A `RedrivePolicy` whose dead-letter target is the source queue
itself is refused. Longer cycles (A → B → A) are accepted and will rewrite a
message for ever — check your redrive graph, nothing here does.*

**Evidence.** `actions::dlq::tests::a_queue_may_not_be_its_own_dead_letter_
target`; `actions::dlq::tests::set_queue_attributes_validates_the_policy_too`.

---

### QS-25

**A `RedrivePolicy` refusal echoes a bounded prefix of the document.**
`accepted` · RedrivePolicy · *classified in this register*

**AWS.** Echoes the whole offending value in the error message.

**Here.** The first 256 characters, then `…`. The document is unbounded client
input, and this sentence lands in this facade's own logs as well as in the
client's answer. The message SHAPE is AWS's, which is what an operator comparing
two logs matches on.

**The sentence.** *An invalid `RedrivePolicy` is echoed back truncated at 256
characters. The error's shape and reason are AWS's; only the echo is bounded.*

**Evidence.** `actions::dlq::tests::the_refusal_echoes_a_bounded_prefix_of_the_
document`.

---

### QS-26

**`AttributeNames=All` includes two `queen.` attributes.**
`deliberate` · GetQueueAttributes · *classified in this register*

**AWS.** Has neither name.

**Here.** `queen.partitions` (the queue's lane width, fixed at create) and
`queen.dedupWindowSeconds` (the dedup window widener, the superset the plan
sells) are stored on the record and the record's own keys are always readable —
which is also what stops an attribute a later version stores from becoming
unreadable under an older catalog. Both clients in the M0 live run ignored them
cleanly, because every SDK models `Attributes` as an open string map.

**The sentence.** *`GetQueueAttributes` with `All` answers two names AWS does not
define — `queen.partitions` and `queen.dedupWindowSeconds`. They are this
facade's own extensions; SDKs ignore them, and code that round-trips the whole
attribute map back into `CreateQueue` is compared against them like any other
attribute it supplies.*

**Evidence.** `actions::queues::tests::every_attribute_the_plan_names_comes_back_
with_its_aws_type`; recorded live in `compat/M0_SMOKE.md`.

---

### QS-27

**No quota emulation, and three supersets.**
`deliberate` · limits · PLAN_QUEEN_SQS.md non-goal 4 · *classified in this register*

**AWS.** 120,000 in-flight per standard queue (20,000 for FIFO), FIFO per-group
throughput quotas, and billing in 64 KB chunks.

**Here.** None of the three is emulated — an in-flight cap this facade enforced
would be a number invented to imitate a bill. Where Queen is WIDER, the width is
the point: `MaximumMessageSize` may be set up to 1 MiB, `MessageRetentionPeriod`
is the queue's own, and `queen.dedupWindowSeconds` widens AWS's fixed five-minute
deduplication window up to a year.

**The sentence.** *SQS's service quotas are not emulated: nothing here refuses a
message for an in-flight cap or a FIFO throughput limit. Three limits are
supersets instead of matches — payload size, retention and the deduplication
window — so an application that only ever obeyed AWS's numbers keeps working, and
one written against these numbers will not port back unchanged.*

**Evidence.** `registry::RANGES` (the documented ranges, and the three that
deliberately are not AWS's); PLAN_QUEEN_SQS.md, Non-goals.

---

### QS-28

**The payload envelope's one acknowledged collision.**
`accepted` · the payload envelope · *classified in this register*

**AWS.** Has no such case: an SQS message is an SQS message.

**Here.** A Queen queue can carry both SQS traffic and a native Queen producer's,
in both directions — that is a feature, and it is why `decode` never fails: a
payload that is not this facade's four-key shape is served as body = the
payload's own text, with no attributes. Recognition is strict (a subset of
`b`/`a`/`s`/`m`, `b` a string, every optional field of the right type, every
base64 value decodable) precisely to keep the collision surface at exactly one
documented shape: a native payload that happens to be `{"b": "…"}` shaped is read
as an SQS envelope and its body is served as the value of `b`.

**The sentence.** *An SQS consumer can read a native Queen producer's messages
and vice versa. The one collision: a native JSON payload whose top-level shape is
`{"b": "<string>"}` — optionally with `a`, `s` or `m` — is read as an SQS
envelope. Any other key, or a non-string `b`, falls out to the native path.*

**Evidence.** `envelope::tests::the_recognition_matrix_holds` (the shape table,
including the near-misses that must NOT be recognized),
`…::a_native_document_is_served_as_its_own_text`,
`…::other_native_payloads_are_served_as_their_json_text`.

---

### QS-29

**SigV4 does not enforce the credential scope's region.**
`deliberate` · SigV4

**AWS.** Refuses a signature whose scope names a region other than the endpoint's.

**Here.** The signing key is derived from the region the CLIENT put in its own
scope, and an SDK signs with the region its user configured — pointing boto3 at
an `endpoint_url` does not change its `region_name`. Refusing a scope that says
`us-east-1` would make the one promise this facade exists to keep ("change
`endpoint_url` and nothing else") false for every client on earth. The SERVICE
*is* pinned, to the two this listener answers, because a scope naming `s3` is a
request that arrived at the wrong door. `QUEEN_SQS_REGION` still decides the
region segment of every queue URL and ARN.

**The sentence.** *Sign with any region you like: the credential scope's region is
not checked, because no SDK will ever be configured with this deployment's own
label. The service is checked (`sqs` or `sns`), and `QUEEN_SQS_REGION` is what
appears in queue URLs and ARNs.*

**Evidence.** `sigv4::tests::an_sns_request_in_a_foreign_region_verifies`, and
the presigned-query vector beside it.

---

## SNS

### SN-01

**A deduplicated FIFO publish answers a FRESH `MessageId`.**
`deliberate` · Publish, FIFO topic · `compat/M4_SMOKE.md` D1 · **differential question [Q1](#q1)**

**AWS (SQS).** `SendMessage` documents returning the ORIGINAL message's id for a
repeated `MessageDeduplicationId`. This facade does exactly that on the SQS path
(`actions::messages::tests::a_fifo_duplicate_answers_the_original_message`), so
the two paths differ from each other, which is why this row exists.

**AWS (SNS).** The `Publish` page does not say. Real SNS may well mint a new id.

**Here.** The publisher is answered a success — nothing needed to be written, and
delivery is right: the first message stands, the duplicate's body never appears,
and every subscriber's notification carries the FIRST publish's id. Only the
ANSWER is a fresh uuid. It cannot be otherwise, twice over: a cross-request
duplicate makes the stored procedure RAISE, so the broker's body is
`{success:false, reason:"duplicate", results:[]}` with NO echoes at all
(`server/src/handlers/data.rs`, `txn_fail_json`); and even a broker that echoed
the winning message id would answer the wrong number, because an SNS `MessageId`
is this facade's uuid written INTO the notification payload, not the broker's
per-delivery message uuid — recovering it would mean reading the stored payload of
the winning message back, which no verb on this wire does.

**The sentence.** *A `Publish` that repeats a `MessageDeduplicationId` inside the
window succeeds, writes nothing, and answers a NEW `MessageId` — not the id of the
message that stands. A publisher retrying after a timeout cannot correlate that id
against what subscribers received; correlate on your own dedup id instead.*

**Evidence.** `sns::publish::tests::a_repeated_deduplication_id_is_a_success_that_
writes_nothing` (a DIFFERENT body under the same key: the first is delivered, the
subscriber sees the first publish's id, the second publisher's id differs from
it); measured live twice on two stacks and printed as a `# note` by
`compat/smoke_m4_sns.py`, recorded in `compat/M4_SMOKE.md` D1.

---

### SN-02

**No `SequenceNumber`, on a FIFO topic or anywhere.**
`deliberate` · Publish / PublishBatch · `compat/M4_SMOKE.md` D3 · *classified in this register*

**AWS.** Answers `{MessageId, SequenceNumber}` for a FIFO topic, and per-entry
`{Id, MessageId, SequenceNumber}` inside `PublishBatch`.

**Here.** The member is absent. A transaction's push echoes carry no offset by
construction — the wire builds them without the `baseOffset` the stored procedure
returned — and `POST /api/v1/push`, which does answer one, is not a transaction
and would forfeit the atomic fan-out this whole module exists for. The atomicity
is the promise the plan makes; the sequence number is not, so the number is
omitted rather than invented. boto3 does not mind: an absent member is simply
absent.

**The sentence.** *An SNS `Publish` answers `MessageId` and nothing else, on FIFO
topics too. If you order or deduplicate on `SequenceNumber` rather than on
delivery order, this endpoint cannot serve you today — the fix is a `baseOffset`
on the transaction's echoes, a broker change, and never a switch away from the
atomic fan-out.*

**Evidence.** `sns::publish::tests::a_fifo_publish_answers_no_sequence_number`;
measured live (`Publish response keys: ['MessageId']`, `PublishBatch successful
entry keys: ['Id', 'MessageId']`).

---

### SN-03

**A fan-out past 256 subscriptions or 8 MiB is more than one transaction.**
`accepted` · Publish fan-out

**AWS.** Fans out per subscriber, with per-subscriber retry and per-subscriber
failure — so it never promised atomicity at all.

**Here.** One publish is normally ONE `POST /api/v1/transaction` bundling one push
per matched subscription, which is STRONGER than SNS promises: no subscriber can
receive a message another subscriber did not. Past `MAX_FANOUT_PER_TRANSACTION`
(256) or `MAX_FANOUT_BYTES_PER_TRANSACTION` (8 MiB) the fan-out is chunked, so a
facade that dies mid-fan-out can leave a prefix of the subscribers delivered. The
chunks are built as they are committed, which is what makes those two numbers a
memory bound as well as a transaction bound. A topic that wide gets a log line
saying so.

**The sentence.** *A publish is atomic across subscribers up to 256 of them (and
8 MiB); a wider topic commits in chunks, so a facade crash mid-publish can leave
some subscribers delivered and the rest not. Below that width, all-or-nothing —
which is more than SNS itself promises.*

**Evidence.** `sns::publish::tests::a_fanout_past_the_bundle_cap_delivers_
everything_in_more_than_one_transaction`, `…::a_wide_fanout_commits_in_bundles_
and_delivers_to_every_subscriber`, `…::a_heavy_fanout_closes_a_bundle_on_bytes`.
**Not exercised live**: the widest topic in `compat/smoke_m4_sns.py` has two
subscribers.

---

### SN-04

**A duplicate rolls back the whole bundle.**
`accepted` · Publish, FIFO topic

**AWS.** Deduplicates per subscriber delivery; a new subscriber added between two
identical publishes gets the second one.

**Here.** The bundle is all-or-nothing, so the broker refuses the WHOLE thing at
the first duplicate and there is no per-item verdict inside a transaction to skip
past. A subscription created BETWEEN the two publishes therefore receives nothing.

**The sentence.** *On a FIFO topic, a republished `MessageDeduplicationId`
delivers to nobody — including a subscriber that did not exist when the message
was first published. Subscribe before you publish, or use a fresh dedup id.*

**Evidence.** the module header of `sns::publish` (the argument);
`sns::publish::tests::a_repeated_deduplication_id_is_a_success_that_writes_
nothing` proves the write half.

---

### SN-05

**A `DeleteQueue` between the target read and the commit still delivers.**
`accepted` · Publish fan-out

**AWS.** Has the same race, resolved by SNS's own per-subscriber delivery.

**Here.** `Subscribe` refuses an endpoint whose queue does not exist, and the
fan-out resolves its targets in ONE FRESH batched read (bypassing the registry's
three-second cache) so the window is the request's own. A `DeleteQueue` inside
that window is still a delivery, and the broker's transaction wire LAZILY
PROVISIONS a queue it does not have — so the message lands in a Queen queue no
registry record owns, where `CreateQueue` will refuse to adopt it (QS-20) and
`ReceiveMessage` answers `QueueDoesNotExist`. Unclosable without a broker that
refuses to provision on push; `SendMessage` has the identical race.

**The sentence.** *Deleting a queue that a live subscription points at can strand
one in-flight publish in an unreachable queue. Unsubscribe before you delete —
the facade cannot make the delete and the publish mutually exclusive.*

**Evidence.** `sns::publish::tests::a_queue_another_instance_deleted_is_not_
delivered_to` and `…::a_subscription_whose_queue_is_gone_does_not_stop_the_others`
pin the resolved half (the window before the read); the residual window is
argued in the module header.

---

### SN-06

**The notification carries no `Signature`, `SigningCertURL` or `UnsubscribeURL`.**
`deliberate` · the notification envelope · `compat/M4_SMOKE.md` D6 · PLAN_QUEEN_SQS.md non-goal 3 · *classified in this register*

**AWS.** Writes all three on every notification.

**Here.** Every field the envelope does carry is one this deployment can stand
behind — `Type`, `MessageId`, `TopicArn`, `Subject`, `Message`, `Timestamp`,
`SignatureVersion`, `MessageAttributes`. The three it does not are a `Signature`
nothing can verify, a `SigningCertURL` whose host AWS's own validator libraries
pin to `sns.*.amazonaws.com`, and an `UnsubscribeURL` that would need a SigV4
signature to work: three fields that would be worse present than absent.
`SignatureVersion` stays because it names the version a signature WOULD carry and
clients compare it as a string. Queue subscribers read none of the three.

**The sentence.** *Notifications from this endpoint are unsigned and carry no
`UnsubscribeURL`. Queue subscribers never look at those fields; an HTTP/S
subscriber and AWS's own signature-validator libraries would, and that is the
milestone (M6) this v0 does not have.*

**Evidence.** `compat/smoke_m4_sns.py` `Notification.carries_no_unverifiable_
signature_fields`; `sns::publish::tests::the_default_delivery_is_the_sns_
notification_envelope`; pinned again from a second client in `compat/go-sdk`.

---

### SN-07

**A publish at the ceiling overruns the target queue's `MaximumMessageSize`.**
`accepted` · Publish size

**AWS.** Drops that delivery to that subscriber.

**Here.** The notification envelope is ~250 bytes larger than the message inside
it, so a publish at `MAX_MESSAGE_BYTES` (256 KiB, SNS's own) becomes a delivery
over a target queue's default `MaximumMessageSize`. This facade lands it, because
the queue attribute is a SEND-path rule and the fan-out is not a send — it is a
push the broker sizes against its own body limit alone. The direction is
delivering a message AWS would have dropped, and the alternative is a subscriber
that silently receives nothing for messages near the ceiling.

**The sentence.** *A publish within SNS's 256 KiB limit is always delivered here,
even when the envelope around it pushes the delivery past the subscribing queue's
`MaximumMessageSize` — where AWS would drop it. Nothing is refused that AWS
accepts; something is delivered that AWS would not deliver.*

**Evidence gap.** The ceiling itself is pinned
(`sns::publish::tests::a_message_over_the_ceiling_is_refused_on_what_crossed_the_
wire`), the overrun is not: `compat/M4_SMOKE.md` lists it under what the suite
deliberately does not cover. One live assertion at the ceiling would close it.

---

### SN-08

**A standard topic cannot subscribe a FIFO queue.**
`deliberate` · Subscribe · `compat/M4_SMOKE.md` D5

**AWS.** Permits it and chooses a group id itself.

**Here.** A standard topic's fan-out picks the target queue's lane by hashing a
fresh key across its width, and a FIFO queue's lanes are its `MessageGroupId`s —
so delivering there needs a decision about which group id every message lands
under. A group id invented per message would put a FIFO consumer's ordering
guarantee in this facade's hands without saying so. It is refused at `Subscribe`,
where a client can read the reason, rather than silently at publish. (FIFO topic
to a standard queue is AWS's OWN refusal and is not a divergence.)

**The sentence.** *A FIFO queue can only be subscribed to a FIFO topic here. AWS
allows a standard topic to fan out into a FIFO queue by choosing group ids for
you; this endpoint refuses at `Subscribe` rather than deciding your ordering for
you.*

**Evidence.** `compat/smoke_m4_sns.py` `Divergence.standard_topic_to_a_fifo_
queue_is_refused`; `sns::admin::tests::the_subscribe_refusals_name_what_is_wrong`.

---

### SN-09

**`ConfirmSubscription` can never succeed.**
`deliberate` · ConfirmSubscription · `compat/M4_SMOKE.md` D5

**AWS.** Confirms a pending subscription against a token it mailed or posted.

**Here.** Every subscription this facade can create is a same-account SQS
subscription, which AWS itself confirms AT `Subscribe`. `PendingConfirmation` is
a state no record here ever occupies, so no token is ever minted and every token
presented is one this endpoint did not issue. The answer is `InvalidParameter`
naming the token — AWS's own code for a token it does not recognise — with a
message that says why there is nothing to confirm. The consequence, recorded so
nobody files it: `SubscriptionsPending` is structurally `0` on every topic, and
`PendingConfirmation` is `false` with `ConfirmationWasAuthenticated` `true` on
every subscription.

**The sentence.** *There is no subscription-confirmation handshake here, because
there is nothing that needs one: an SQS subscription is confirmed the moment it is
created, exactly as at AWS. `ConfirmSubscription` always fails, and HTTP/S
subscriptions — the ones that need the handshake — are M6.*

**Evidence.** `compat/smoke_m4_sns.py` `Divergence.confirm_subscription_has_
nothing_to_confirm`; `sns::admin::tests::confirm_subscription_cannot_succeed_and_
explains_itself`.

---

### SN-10

**A repeat `Subscribe` ignores the attributes it carries.**
`deliberate` · Subscribe · `compat/M4_SMOKE.md` D4 · **differential question [Q3](#q3)** · *classified in this register*

**AWS.** *"If the requester already owns a subscription with the specified
attributes, that subscription's ARN is returned"* — and silent about the case
where the attributes DIFFER.

**Here.** `Subscribe` is idempotent per `(topic, protocol, endpoint)` and returns
the existing ARN. It does not apply the request's attributes to the existing
subscription: `SetSubscriptionAttributes` is the action that edits a live
subscription, and a `Subscribe` that silently replaced a live filter policy would
be a change nobody asked for. The cost is real and worth stating: it is the
reconcile loop of every declarative provisioner (Terraform's
`aws_sns_topic_subscription`, MassTransit, JustSaying), so a filter policy edited
in the provisioner's source never reaches the facade and nothing reports drift.

**The sentence.** *Re-subscribing an endpoint that is already subscribed returns
the existing ARN and changes nothing — a `FilterPolicy` or `RawMessageDelivery`
in a repeat `Subscribe` is ignored. Edit a live subscription with
`SetSubscriptionAttributes`; a provisioner that only ever calls `Subscribe` will
not see its changes applied.*

**Evidence.** `compat/smoke_m4_sns.py` `Divergence.repeat_subscribe_answers_the_
existing_arn` and `Divergence.repeat_subscribe_does_not_apply_the_new_attributes`
— both halves, so a change to either is visible.

---

### SN-11

**A subscription's endpoint must be a queue this deployment knows.**
`deliberate` · Subscribe

**AWS.** Accepts an endpoint it cannot see, because cross-account subscriptions
are legal there.

**Here.** One deployment is one account, so an endpoint nobody can resolve is a
configuration mistake. Refusing it at `Subscribe` is the difference between an
error a client can read and a topic that silently drops every publish.

**The sentence.** *`Subscribe` refuses a queue this endpoint does not have.
Cross-account SNS→SQS topologies are not modelled: create the queue first.*

**Evidence.** `sns::admin::tests::the_subscribe_refusals_name_what_is_wrong`;
the module header of `sns::admin` carries the classification.

---

### SN-12

**`Policy` and `EffectiveDeliveryPolicy` are answered only when set.**
`deliberate` · GetTopicAttributes

**AWS.** Reports a generated `Policy` and an `EffectiveDeliveryPolicy` on every
topic.

**Here.** Neither is evaluated (QS-19 for `Policy`; a queue subscription's
delivery is one push inside the publish transaction, with no retry ladder to
describe), and seeding AWS's default documents would answer a client that its
policy is in force — the one outcome the plan forbids. A `Policy` a client SET is
stored and answered, because that is what it asked for. What is lost is a
provisioner reading `Policy` on a topic it never set one on: it sees the key
absent instead of AWS's generated default.

**The sentence.** *A topic reports no `Policy` and no `EffectiveDeliveryPolicy`
unless you set one, because neither is enforced here and a default document would
claim otherwise.*

**Evidence.** `sns::admin::tests::a_topic_reports_no_policy_it_does_not_enforce`
(absent by default; verbatim once set).

---

### SN-13

**Past 10,000 subscriptions a topic's fan-out delivers to a prefix.**
`accepted` · Publish fan-out

**AWS.** Fans out to every subscription, and its own cap is far higher.

**Here.** `subscriptions_of` is a bounded read (`MAX_SCANNED` = 10,000) because it
runs inside ONE client request. Past it the counts on `GetTopicAttributes` are
approximate, the duplicate check in `Subscribe` can miss, and — the one that
matters — the fan-out delivers to the same PREFIX of the key range on every
publish while the rest of the subscribers never receive anything. The publish path
cannot tell a truncated list from a complete one, so a sampled WARN naming the
topic is the whole of the signal.

**The sentence.** *A topic with more than 10,000 subscriptions delivers to the
same first 10,000 of them on every publish and to none of the rest, with a WARN
naming the topic. It is a hard ceiling, not a slow path.*

**Evidence.** `sns::registry::subscriptions_of` and the `TRUNCATED_SCAN` sampler.
**Not exercised**: no test stands up a topic at the ceiling.

---

### SN-14

**A FIFO topic's fan-out into one queue is one lane.**
`accepted` · Publish, FIFO topic · `compat/M4_SMOKE.md` D2 · *classified in this register*

**AWS.** Same semantics for a single group — but the number of groups is not
usually thought of as a capacity decision at publish time.

**Here.** On a STANDARD topic the fan-out hashes a fresh key across the target
queue's width, so [QS-01](#qs-01) applies exactly as it does to `SendMessage` and
`queen.partitions` is the dial. On a FIFO topic the lane is the `MessageGroupId`,
on EVERY subscriber's queue — which is what makes one publish order identically
for all of them. So a FIFO topic publishing into one group delivers to each
subscriber through ONE partition: that subscriber's concurrency for the topic is
one message at a time, and no queue attribute widens it.

**The sentence.** *On a FIFO topic the `MessageGroupId` is the subscriber's lane:
one group means one message in flight per subscriber, whatever the queue's width,
and a slow consumer stalls every later message of that group for every publisher.
The number of groups is a capacity decision on the PUBLISHER's side, which
nothing in the SNS API hints at.*

**Evidence.** `compat/smoke_m4_sns.py` `FifoTopic.group_order_is_the_publish_
order` (the property this buys); `compat/M4_SMOKE.md` D2 for the measurement.

---

### SN-15

**The `.fifo`/`FifoTopic` pairing is not relaxed for a re-create.**
`deliberate` · CreateTopic · **differential question [Q4](#q4)** · *classified in this register*

**AWS.** Undocumented, exactly as on the queue side.

**Here.** `.fifo` and `FifoTopic=true` must BOTH be present or BOTH absent, on
every `CreateTopic` including a repeat — where the QUEUE side deliberately
relaxes the same rule for a re-create ([QS-21](#qs-21)). The asymmetry is
deliberate: the frameworks that create topics (MassTransit, JustSaying) send the
attribute set they want on every call, so a relaxation nobody needs would be a
divergence nobody checked.

**The sentence.** *A `.fifo` topic must be created — and re-created — with
`FifoTopic=true`. Queues relax this on a re-create and topics do not; if that
asymmetry ever bites, the queue's shape is the one to copy.*

**Evidence.** `sns::validate_topic_name` and the create-idempotency tests in
`sns::admin::tests`; the asymmetry is argued in the header of `sns/mod.rs`.

---

### SN-16

**`sqs` is the only subscribable protocol.**
`deliberate` · Subscribe · v0 scope · *classified in this register*

**AWS.** `http`, `https`, `email`, `email-json`, `sms`, `sqs`, `application`,
`lambda`, `firehose`.

**Here.** Anything but `sqs` is `InvalidParameter`, with a message naming the
milestone. v0's scope is SQS-queue subscriptions because that is MassTransit and
JustSaying — the two frameworks that auto-create SNS+SQS topologies and therefore
the two best end-to-end tests. HTTP/S is M6, delegated to queen-relay, and it is
the milestone that has to answer the signature question ([SN-06](#sn-06)).

**The sentence.** *This endpoint fans out to SQS queues and nothing else.
HTTP/S subscriptions are a planned milestone; email, SMS, Lambda and Firehose are
AWS the platform, not SNS the API, and no facade can provide them.*

**Evidence.** `sns::admin::subscribe` (the refusal and its message);
`sns::publish::tests::a_subscription_of_another_protocol_is_skipped` for the
publish side.

---

## Questions for the differential lane

A run against a real AWS account (dedicated, manual, never default CI) settles
each of these in one or two calls. Until then each is a row above whose
classification could flip.

### Q1

**Does a deduplicated SNS `Publish` answer the original message's id?** —
[SN-01](#sn-01). SQS's `SendMessage` page documents that it does for a repeated
`MessageDeduplicationId`; SNS's `Publish` page is silent. If real SNS mints a NEW
id, today's behaviour is right and SN-01 becomes a note rather than a divergence.
If it answers the original's, SN-01 stays and its cost is the argument for
whether to close it at all — closing it needs the broker to echo the winning
message from a rolled-back transaction AND a way to read the stored payload back,
neither of which exists.
**The call:** publish twice under one dedup id on a FIFO topic, compare the two
`MessageId`s.

### Q2

**Does real `CreateQueue` compare only the attributes the request supplies, and
does it compare tags?** — [QS-21](#qs-21), [QS-22](#qs-22). The `QueueNameExists`
error page says the error comes "only if the request includes attributes whose
values differ from those of the existing queue"; the `CreateQueue` page carries a
looser sentence ("the exact names and values of ALL the queue's attributes")
which, read alone, would license the behaviour this facade shipped and then
removed. The ecosystem agrees with the error page — under the looser reading every
Celery, sqs-consumer, ActiveJob and Spring Cloud AWS worker would fail to boot
against a Terraform-made queue — but only a live call settles it. Tags are the
untested half: nothing documents whether a `CreateQueue` carrying different tags
on an existing queue is refused, ignored (this facade), or applied.
**The calls:** create a queue with non-default attributes; re-create it naming
nothing; re-create it naming a SUBSET that matches; re-create it with different
`tags` and read them back.

### Q3

**What does a repeat `Subscribe` with DIFFERENT attributes do?** —
[SN-10](#sn-10). AWS's sentence covers only the "already owns a subscription with
the specified attributes" case. If real SNS APPLIES the new attributes, SN-10 is a
defect for every declarative provisioner and should be fixed rather than
classified; if it ignores them, SN-10 is parity and the row becomes a note.
**The call:** subscribe with `FilterPolicy {"event":["one"]}`, subscribe the same
endpoint again with `{"event":["two"]}`, then `GetSubscriptionAttributes`.

### Q4

**Does real `CreateTopic` accept a bare `.fifo` re-create?** — [SN-15](#sn-15),
against [QS-21](#qs-21). If it does, the topic side should copy the queue side's
relaxation and the asymmetry goes away.
**The call:** create `t.fifo` with `FifoTopic=true`, then `CreateTopic{Name:
"t.fifo"}` with no attributes.

### Q5

**Does a standard SNS topic really accept a FIFO queue subscription, and what
group id does it choose?** — [SN-08](#sn-08). The refusal here is deliberate, but
the FIX, if one is wanted, is precisely AWS's answer to "which group id".
**The call:** subscribe a `.fifo` queue to a standard topic, publish, and read
`MessageGroupId` off the delivered message.

### Q6

**Is `SequenceNumber` on a FIFO `SendMessage`/`ReceiveMessage` unique across the
queue or only within the group, in practice?** — [QS-09](#qs-09). The
documentation says queue-unique; what matters for a client is whether anything
real depends on it. A live sample across two groups tells us whether the
divergence is theoretical.
**The calls:** send to two groups on one FIFO queue, compare the answered
sequence numbers.

---

## Not divergences

Recorded so nobody files them, and so a differential run does not spend a day on
them.

* **The notification's JSON keys are in sorted order**, not AWS's declaration
  order. JSON object order carries no meaning, every SNS consumer parses by field
  name, and the signature validators that DO build a canonical string build it
  from the parsed fields in their own fixed order.
* **`SubscriptionsPending` is structurally `0`** and every subscription reports
  `PendingConfirmation: false` — a consequence of [SN-09](#sn-09), not a separate
  row.
* **A subscription with no filter policy reports no `FilterPolicyScope`.** That
  is AWS's behaviour, and the right one: a scope reported for a subscription with
  nothing to scope is drift a provisioner would reconcile for ever.
* **A `PublishBatch` is one transaction PER ENTRY**, not one for the batch. SNS's
  own contract is per-entry results and no atomicity across entries, so this
  matches; bundling ten entries would make one entry's refusal roll back nine
  messages a client was told nothing about.
* **The two SQS `MessageId`s from one fan-out differ from each other and from the
  publish's `MessageId`.** True at AWS too: the notification carries the
  publish's id, and each queue's own message id is the broker's.
* **`WaitTimeSeconds=0` cannot be sent from aws-sdk-go-v2** (modelled as a
  non-pointer `int32`, omitted at zero). A client difference, not a facade one;
  boto3 sends the explicit zero.
* **Depth attributes on a queue a native Queen consumer also reads** answer the
  queue-level numbers, so another consumer's backlog is included. There is no AWS
  analogue to diverge from — it is the price of the mixed consumption this facade
  is built for, and it is worth knowing before pointing KEDA at such a queue.
* **`error.rs` invents no status codes.** Every code in the catalog is a real AWS
  code with AWS's own status and both of AWS's spellings; the two places the
  catalog is narrower than AWS's are [QS-15](#qs-15) and [QS-16](#qs-16) and they
  are rows above.

---

## Not reachable in this build

`QUEEN_SQS_RECEIVE_MODE=amortized` is **refused at boot** — a `FATAL` naming
C-SQS-1 and exit code 1 — rather than accepted and served as `exact` under
another mode's name. It needs `maxPerPartition` on the broker's pop, which is not
implemented.

When C-SQS-1 lands and the refusal is deleted, that mode adds exactly two rows to
this register, both inside SQS's own at-least-once envelope, and both of which
would then need their own sentence and evidence:

* extending one message's visibility extends its pop-mates (invisible to other
  consumers, like [QS-05](#qs-05) but on a standard queue);
* terminating one message's visibility returns the others as duplicates.

It would also change [QS-01](#qs-01) and [QS-02](#qs-02), which is the point of
taking it.

Refusal pinned by `config::tests::the_unserved_receive_mode_is_refused_at_boot`
(four spellings, and the message names C-SQS-1 and PLAN_QUEEN_SQS.md) and
`main::tests::an_unserved_receive_mode_never_boots`.
