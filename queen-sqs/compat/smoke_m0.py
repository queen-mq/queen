#!/usr/bin/env python3
"""The queen-sqs M0 smoke: boto3 against a live facade, a live broker and a live
Postgres.

WHAT THIS IS FOR. The crate's own suite (`queen-sqs/src/http_tests.rs`) drives the
same actions against `FakeQueen`, so everything here has already passed once
against a broker that always answers the way the facade expects. This file is the
other half: the same surface against the REAL broker, where a pop can return
nothing, a lease really expires, and the KV registry is a table someone else also
writes. A test that only ever passes here and not there — or the other way round
— is the interesting one.

THE CONTRACT (copied from queen-kafka's CLIENT_MATRIX.md, verbatim in what
matters):

  * the stack comes from the environment, never from a hardcoded address;
  * ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
  * `RESULT: PASS` or `RESULT: FAIL` as the last line;
  * a nonzero exit status when anything failed.

  $ queen-sqs/compat/rig.sh up
  $ source queen-sqs/compat/.rig/env.sh
  $ python queen-sqs/compat/smoke_m0.py

MD5s ARE CHECKED HERE, NOT BY THE SDK. It is widely believed that boto3 verifies
`MD5OfMessageBody` and `MD5OfMessageAttributes` client-side — the Java, JS and
.NET SDKs do. botocore does not, and has not for years: there is no MD5 handler
for SQS anywhere in the package (`grep -rn MD5OfMessageAttributes botocore/`
finds only the service model). So a facade that returned a constant would sail
past a suite that leaned on the SDK for this. `_md5_of_body` and
`_md5_of_attributes` below implement AWS's two algorithms — the attribute one
with its own binary encoding, length-prefixed name, length-prefixed type,
transport byte, length-prefixed value — and the assertions are ours.

NAMES ARE UNIQUE PER RUN. Not tidiness: `DeleteQueue` arms a 60-second
`QueueDeletedRecently` tombstone, exactly as AWS does, so a suite that reused
fixed names could not be run twice inside a minute.

ERROR CODES ARE ASSERTED IN BOTH SPELLINGS. SQS carries two names for every
error and they are usually different words: the JSON `Code` is the LEGACY QUERY
code (`AWS.SimpleQueueService.NonExistentQueue`) and `QueryErrorCode` is the
SHAPE name (`QueueDoesNotExist`), which is the one botocore maps its exception
classes from (`botocore/handlers.py:_handle_sqs_compatible_error`). A facade that
got the pair backwards would still raise a `ClientError` and would break every
`except sqs.exceptions.QueueDoesNotExist` in the world, so [`ERROR_CODES`] pins
both and the assertions check the exception class too.
"""

import base64
import hashlib
import os
import struct
import sys
import time
import traceback
import uuid

import boto3
import botocore.exceptions

# --------------------------------------------------------------------- stack

ENDPOINT = os.environ.get("QUEEN_SQS_ENDPOINT", "http://127.0.0.1:19324")
REGION = os.environ.get("QUEEN_SQS_REGION", "queen-1")
ACCOUNT = os.environ.get("QUEEN_SQS_ACCOUNT", "000000000000")
AKID = os.environ.get("AWS_ACCESS_KEY_ID", "QSQSTEST")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "qsqssecret")

RUN = uuid.uuid4().hex[:8]
CREATED = []

# ------------------------------------------------------------------ reporting

PASSES = 0
FAILURES = []


def ok(name):
    global PASSES
    PASSES += 1
    print(f"ok {name}", flush=True)


def fail(name, detail):
    FAILURES.append(name)
    print(f"FAIL {name}: {detail}", flush=True)


def check(name, condition, detail=""):
    if condition:
        ok(name)
    else:
        fail(name, detail or "condition was false")
    return bool(condition)


def check_eq(name, got, want):
    return check(name, got == want, f"got {got!r}, want {want!r}")


TESTS = []


def test(fn):
    TESTS.append(fn)
    return fn


# ------------------------------------------------------------ AWS's own MD5s


def _md5_of_body(body):
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def _md5_of_attributes(attributes):
    """AWS's MessageAttributes digest.

    Names in ascending byte order, and for each one: 4-byte big-endian length +
    UTF-8 name, then the same for the data type string, then a single transport
    byte (1 = the value is a String, 2 = the value is Binary), then 4-byte
    big-endian length + the value's bytes. MD5 of the whole thing, hex.
    """
    if not attributes:
        return None
    buf = bytearray()

    def field(raw):
        buf.extend(struct.pack(">I", len(raw)))
        buf.extend(raw)

    for name in sorted(attributes, key=lambda n: n.encode("utf-8")):
        value = attributes[name]
        field(name.encode("utf-8"))
        field(value["DataType"].encode("utf-8"))
        if "StringValue" in value:
            buf.append(1)
            field(value["StringValue"].encode("utf-8"))
        else:
            buf.append(2)
            field(value["BinaryValue"])
    return hashlib.md5(bytes(buf)).hexdigest()


# ------------------------------------------------------------------- helpers


def client():
    c = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=AKID,
        aws_secret_access_key=SECRET,
    )
    # WHICH PROTOCOL THIS CLIENT ACTUALLY SPOKE, read off the request botocore
    # built rather than assumed from the SDK's version. The suite contract asks
    # for it (queen-kafka's CLIENT_MATRIX.md: "each rig reports which protocol
    # its client actually spoke, read from the client's own debug stream, never
    # assumed"), and the two shapes are distinguishable by one header: AWS JSON
    # 1.0 sends `X-Amz-Target: AmazonSQS.<Action>` with an
    # `application/x-amz-json-1.0` body, and the Query protocol sends neither
    # and a form-encoded one. The handler only records — it returns None, which
    # is how botocore's event system says "carry on".
    c.meta.events.register("before-send.sqs.*", _record_protocol)
    return c


PROTOCOLS = {}


def _record_protocol(request, **_):
    target = request.headers.get("X-Amz-Target")
    content_type = request.headers.get("Content-Type", "")
    if isinstance(target, bytes):
        target = target.decode("utf-8", "replace")
    if isinstance(content_type, bytes):
        content_type = content_type.decode("utf-8", "replace")
    if target and "json" in content_type:
        spoken = f"AWS JSON 1.0 ({content_type})"
    elif "x-www-form-urlencoded" in content_type:
        spoken = f"Query/XML ({content_type})"
    else:
        spoken = f"unrecognized (Content-Type: {content_type!r}, X-Amz-Target: {target!r})"
    PROTOCOLS[spoken] = PROTOCOLS.get(spoken, 0) + 1
    return None


def make_queue(c, label, attributes=None, tags=None, fifo=False):
    """A queue named for this run, remembered so the teardown can remove it.

    `fifo=True` appends the `.fifo` suffix AFTER the run id, because the suffix
    is the whole of how a FIFO queue is declared and it has to be the last thing
    in the name.
    """
    name = f"m0-{label}-{RUN}" + (".fifo" if fifo else "")
    kwargs = {"QueueName": name}
    if attributes:
        kwargs["Attributes"] = attributes
    if tags:
        kwargs["tags"] = tags
    url = c.create_queue(**kwargs)["QueueUrl"]
    CREATED.append(url)
    return name, url


def drain(c, url, count, timeout=25.0, **kwargs):
    """Collect up to `count` messages WITHOUT deleting them, until `timeout`.

    A receive is up to N parallel `batch=1` pops across the queue's lanes, so
    ONE call legitimately returns fewer than it asked for even when the queue is
    full: every read of more than one message in this file therefore loops.

    This helper cannot collect more messages than the queue has PARTITIONS —
    see `t_inflight_is_capped_by_partitions`, which is about exactly that — so
    it is used only where one or two messages are expected, or where the point
    IS how few come back. Exhaustive reads use `drain_deleting`.
    """
    got = []
    deadline = time.monotonic() + timeout
    while len(got) < count and time.monotonic() < deadline:
        response = c.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=min(10, count - len(got)),
            WaitTimeSeconds=1,
            **kwargs,
        )
        got.extend(response.get("Messages", []))
    return got


def drain_deleting(c, url, count, timeout=60.0, **kwargs):
    """What a real consumer does: receive, delete, go round again.

    Returns the message dicts, all already deleted. This is the only shape that
    can drain a queue holding more messages than it has lanes, because a lane
    with a message in flight hands out nothing else until that message is gone.
    """
    got = []
    deadline = time.monotonic() + timeout
    while len(got) < count and time.monotonic() < deadline:
        response = c.receive_message(
            QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1, **kwargs
        )
        batch = response.get("Messages", [])
        for message in batch:
            c.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        got.extend(batch)
    return got


def hold(c, url, count, timeout=60.0):
    """`count` messages received and NOT deleted, all in flight at once.

    Naively receiving `count` messages does not get there: two messages that
    hashed into the same lane cannot be in flight together, so a queue with
    three messages can hand out one and then nothing. When a receive comes back
    empty and the hold is short, this SENDS another message — a fresh MessageId
    is a fresh lane — rather than waiting on a lane that will not open. It
    leaves stragglers behind, so it is used only where nothing later asserts
    the queue is empty.
    """
    held = []
    deadline = time.monotonic() + timeout
    while len(held) < count and time.monotonic() < deadline:
        response = c.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        batch = response.get("Messages", [])
        held.extend(batch)
        if not batch and len(held) < count:
            c.send_message(QueueUrl=url, MessageBody=f"filler-{uuid.uuid4().hex}")
    return held[:count]


# SQS spells every error twice, and the two spellings are different words.
# shape name (QueryErrorCode, and what botocore names the exception class after)
# -> the legacy Query code AWS puts in `Code`.
ERROR_CODES = {
    "QueueDoesNotExist": "AWS.SimpleQueueService.NonExistentQueue",
    "QueueNameExists": "QueueAlreadyExists",
    "QueueDeletedRecently": "AWS.SimpleQueueService.QueueDeletedRecently",
    "BatchEntryIdsNotDistinct": "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    "EmptyBatchRequest": "AWS.SimpleQueueService.EmptyBatchRequest",
    "TooManyEntriesInBatchRequest": "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    "ReceiptHandleIsInvalid": "ReceiptHandleIsInvalid",
    "InvalidAttributeName": "InvalidAttributeName",
}


def error_code(exc):
    return exc.response.get("Error", {}).get("Code")


def query_error_code(exc):
    return exc.response.get("Error", {}).get("QueryErrorCode")


def expect_error(name, shape, call):
    """Run `call`, require it to fail as SQS's `shape`, in all three spellings."""
    try:
        call()
    except botocore.exceptions.ClientError as e:
        got = (query_error_code(e), error_code(e), type(e).__name__)
        want = (shape, ERROR_CODES[shape], shape)
        check_eq(name, got, want)
        return e
    fail(name, f"the call succeeded; expected {shape}")
    return None


# ---------------------------------------------------------------- queue CRUD


@test
def t_create_queue(c):
    attributes = {
        "VisibilityTimeout": "30",
        "MessageRetentionPeriod": "3600",
        "MaximumMessageSize": "262144",
        "DelaySeconds": "0",
        "ReceiveMessageWaitTimeSeconds": "0",
    }
    tags = {"team": "billing", "env": "rig"}
    name, url = make_queue(c, "crud", attributes, tags)
    check_eq("CreateQueue.url", url, f"{ENDPOINT}/{ACCOUNT}/{name}")

    again = c.create_queue(QueueName=name, Attributes=attributes, tags=tags)["QueueUrl"]
    check_eq("CreateQueue.idempotent_identical_request", again, url)

    # AWS answers QueueNameExists only when the request's OWN attributes
    # disagree with the stored ones. This one does.
    expect_error("CreateQueue.conflicting_attribute_refused", "QueueNameExists",
                 lambda: c.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "45"}))

    # ...and a repeat that names NO attributes has nothing to disagree with, so
    # AWS returns the existing queue's URL: "Amazon SQS returns this error only
    # if the request includes attributes whose values differ from those of the
    # existing queue" (the QueueNameExists error's own documentation), and
    # PLAN_QUEEN_SQS.md's "QueueAlreadyExists (only on attribute mismatch, per
    # AWS)". This is the idempotent create every framework does at worker
    # startup — Celery, sqs-consumer, ActiveJob — against a queue Terraform
    # made with non-default attributes, which is why it is asserted and not
    # assumed.
    try:
        bare = c.create_queue(QueueName=name)["QueueUrl"]
        check_eq("CreateQueue.repeat_without_attributes_is_idempotent", bare, url)
    except botocore.exceptions.ClientError as e:
        fail(
            "CreateQueue.repeat_without_attributes_is_idempotent",
            f"{error_code(e)}: {e.response.get('Error', {}).get('Message')} "
            f"— a request naming no attributes names none that differ",
        )

    # The same rule one step in: a SUBSET agrees about what it names and says
    # nothing about the rest, so the omitted MessageRetentionPeriod=3600 is not
    # a disagreement. This is the Terraform-made queue seen by a worker that
    # only knows its own visibility timeout.
    try:
        subset = c.create_queue(
            QueueName=name, Attributes={"VisibilityTimeout": "30"}
        )["QueueUrl"]
        check_eq("CreateQueue.repeat_with_a_subset_is_idempotent", subset, url)
    except botocore.exceptions.ClientError as e:
        fail(
            "CreateQueue.repeat_with_a_subset_is_idempotent",
            f"{error_code(e)}: {e.response.get('Error', {}).get('Message')}",
        )

    # Tags are NOT attributes — their own request member, their own three
    # actions — and the error's sentence names attributes only. The differential
    # lane is what settles the second half of this (that AWS leaves the existing
    # tags alone rather than merging the request's), which is why both halves
    # are asserted here rather than only the status code.
    try:
        retagged = c.create_queue(QueueName=name, tags={"team": "platform"})["QueueUrl"]
        check_eq("CreateQueue.repeat_with_other_tags_succeeds", retagged, url)
        check_eq(
            "CreateQueue.repeat_with_other_tags_does_not_retag",
            c.list_queue_tags(QueueUrl=url).get("Tags", {}),
            tags,
        )
    except botocore.exceptions.ClientError as e:
        fail(
            "CreateQueue.repeat_with_other_tags_succeeds",
            f"{error_code(e)}: {e.response.get('Error', {}).get('Message')}",
        )


@test
def t_get_queue_url_and_list(c):
    name, url = make_queue(c, "list")
    check_eq("GetQueueUrl.round_trip", c.get_queue_url(QueueName=name)["QueueUrl"], url)

    listed = c.list_queues().get("QueueUrls", [])
    check("ListQueues.contains_the_queue", url in listed, f"{url} not in {len(listed)} urls")

    prefixed = c.list_queues(QueueNamePrefix=name).get("QueueUrls", [])
    check_eq("ListQueues.prefix_filters", prefixed, [url])

    missing = c.list_queues(QueueNamePrefix=f"no-such-prefix-{RUN}").get("QueueUrls", [])
    check_eq("ListQueues.prefix_that_matches_nothing", missing, [])


@test
def t_queue_attributes(c):
    name, url = make_queue(c, "attrs", {"VisibilityTimeout": "30", "MessageRetentionPeriod": "3600"})

    all_attrs = c.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    check_eq("GetQueueAttributes.all_has_arn", all_attrs.get("QueueArn"),
             f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}")
    check_eq("GetQueueAttributes.all_has_visibility", all_attrs.get("VisibilityTimeout"), "30")
    for expected in ("CreatedTimestamp", "LastModifiedTimestamp",
                     "ApproximateNumberOfMessages",
                     "ApproximateNumberOfMessagesNotVisible",
                     "ApproximateNumberOfMessagesDelayed"):
        check(f"GetQueueAttributes.all_has_{expected}", expected in all_attrs,
              f"absent; got {sorted(all_attrs)}")

    selected = c.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])["Attributes"]
    check_eq("GetQueueAttributes.selection_is_exact", sorted(selected), ["VisibilityTimeout"])

    c.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "45"})
    after = c.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    check_eq("SetQueueAttributes.applies", after.get("VisibilityTimeout"), "45")
    # SetQueueAttributes MERGES: AWS has no way to remove an attribute, so one
    # that was not named must survive untouched.
    check_eq("SetQueueAttributes.merges_rather_than_replaces",
             after.get("MessageRetentionPeriod"), "3600")


@test
def t_tags(c):
    name, url = make_queue(c, "tags", tags={"team": "billing"})
    check_eq("CreateQueue.tags_are_stored",
             c.list_queue_tags(QueueUrl=url).get("Tags", {}), {"team": "billing"})

    c.tag_queue(QueueUrl=url, Tags={"env": "rig", "owner": "compat"})
    check_eq("TagQueue.adds", c.list_queue_tags(QueueUrl=url).get("Tags", {}),
             {"team": "billing", "env": "rig", "owner": "compat"})

    c.tag_queue(QueueUrl=url, Tags={"env": "rig2"})
    check_eq("TagQueue.overwrites_one_key",
             c.list_queue_tags(QueueUrl=url).get("Tags", {}).get("env"), "rig2")

    c.untag_queue(QueueUrl=url, TagKeys=["owner"])
    check_eq("UntagQueue.removes", sorted(c.list_queue_tags(QueueUrl=url).get("Tags", {})),
             ["env", "team"])


# ------------------------------------------------------------------- sending


@test
def t_send_with_message_attributes(c):
    _, url = make_queue(c, "sendattrs")
    body = "hello, queen-sqs"
    attributes = {
        "plain": {"DataType": "String", "StringValue": "a string"},
        "count": {"DataType": "Number", "StringValue": "42"},
        "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01\x02\xff"},
        "custom": {"DataType": "String.email", "StringValue": "alice@example.com"},
    }
    sent = c.send_message(QueueUrl=url, MessageBody=body, MessageAttributes=attributes)

    check_eq("SendMessage.md5_of_body", sent.get("MD5OfMessageBody"), _md5_of_body(body))
    check_eq("SendMessage.md5_of_attributes", sent.get("MD5OfMessageAttributes"),
             _md5_of_attributes(attributes))
    check("SendMessage.message_id_is_a_uuid",
          _is_uuid(sent.get("MessageId", "")), f"got {sent.get('MessageId')!r}")

    got = drain(c, url, 1, MessageAttributeNames=["All"])
    if not check_eq("SendMessage.round_trips", len(got), 1):
        return
    message = got[0]
    check_eq("ReceiveMessage.body", message["Body"], body)
    check_eq("ReceiveMessage.md5_of_body", message.get("MD5OfBody"), _md5_of_body(body))
    check_eq("ReceiveMessage.md5_of_attributes", message.get("MD5OfMessageAttributes"),
             _md5_of_attributes(attributes))
    check_eq("ReceiveMessage.message_id_matches_send",
             message.get("MessageId"), sent.get("MessageId"))

    received = message.get("MessageAttributes", {})
    check_eq("ReceiveMessage.attribute_names", sorted(received), sorted(attributes))
    check_eq("ReceiveMessage.string_attribute",
             received.get("plain", {}).get("StringValue"), "a string")
    check_eq("ReceiveMessage.number_attribute_keeps_its_type",
             received.get("count", {}).get("DataType"), "Number")
    check_eq("ReceiveMessage.number_attribute", received.get("count", {}).get("StringValue"), "42")
    check_eq("ReceiveMessage.binary_attribute",
             _as_bytes(received.get("blob", {}).get("BinaryValue")), b"\x00\x01\x02\xff")
    check_eq("ReceiveMessage.custom_data_type_survives",
             received.get("custom", {}).get("DataType"), "String.email")

    c.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])


@test
def t_send_batch(c):
    _, url = make_queue(c, "sendbatch")
    entries = [
        {"Id": f"e{i}", "MessageBody": f"batch-body-{i}",
         "MessageAttributes": {"i": {"DataType": "Number", "StringValue": str(i)}}}
        for i in range(10)
    ]
    result = c.send_message_batch(QueueUrl=url, Entries=entries)

    successful = result.get("Successful", [])
    check_eq("SendMessageBatch.ten_succeeded", len(successful), 10)
    check_eq("SendMessageBatch.none_failed", result.get("Failed", []), [])
    check_eq("SendMessageBatch.ids_echo",
             sorted(s["Id"] for s in successful), sorted(e["Id"] for e in entries))
    check("SendMessageBatch.message_ids_are_distinct",
          len({s["MessageId"] for s in successful}) == 10,
          f"{len({s['MessageId'] for s in successful})} distinct ids for 10 entries")

    by_id = {s["Id"]: s for s in successful}
    bad_body = [e["Id"] for e in entries
                if by_id.get(e["Id"], {}).get("MD5OfMessageBody") != _md5_of_body(e["MessageBody"])]
    check_eq("SendMessageBatch.per_entry_body_md5", bad_body, [])
    bad_attrs = [
        e["Id"] for e in entries
        if by_id.get(e["Id"], {}).get("MD5OfMessageAttributes")
        != _md5_of_attributes(e["MessageAttributes"])
    ]
    check_eq("SendMessageBatch.per_entry_attribute_md5", bad_attrs, [])

    # Receive-and-delete, which is the only shape that drains a queue holding
    # more messages than it has lanes.
    got = drain_deleting(c, url, 10)
    check_eq("SendMessageBatch.all_ten_are_receivable", len(got), 10)
    check_eq("SendMessageBatch.bodies_round_trip",
             sorted(m["Body"] for m in got), sorted(e["MessageBody"] for e in entries))
    check_eq("SendMessageBatch.queue_is_empty_afterwards", len(drain(c, url, 1, timeout=4.0)), 0)


@test
def t_batch_limits(c):
    _, url = make_queue(c, "batchlimits")
    expect_error("SendMessageBatch.duplicate_ids_refused", "BatchEntryIdsNotDistinct",
                 lambda: c.send_message_batch(QueueUrl=url, Entries=[
                     {"Id": "same", "MessageBody": "one"},
                     {"Id": "same", "MessageBody": "two"}]))
    expect_error("SendMessageBatch.eleven_entries_refused", "TooManyEntriesInBatchRequest",
                 lambda: c.send_message_batch(QueueUrl=url, Entries=[
                     {"Id": f"e{i}", "MessageBody": "x"} for i in range(11)]))
    expect_error("SendMessageBatch.empty_batch_refused", "EmptyBatchRequest",
                 lambda: c.send_message_batch(QueueUrl=url, Entries=[]))
    expect_error("DeleteMessageBatch.empty_batch_refused", "EmptyBatchRequest",
                 lambda: c.delete_message_batch(QueueUrl=url, Entries=[]))
    expect_error("GetQueueAttributes.unknown_attribute_refused", "InvalidAttributeName",
                 lambda: c.get_queue_attributes(QueueUrl=url, AttributeNames=["NotAnAttribute"]))
    expect_error("SetQueueAttributes.unknown_attribute_refused", "InvalidAttributeName",
                 lambda: c.set_queue_attributes(QueueUrl=url, Attributes={"NotAnAttribute": "1"}))


@test
def t_inflight_is_capped_by_partitions(c):
    """How many messages a standard queue will hand out at once.

    On SQS the answer is "all of them" (its in-flight ceiling is 120,000 per
    queue and has nothing to do with any lane). Here a standard queue is M
    synthesized partitions and a receive is a `batch=1` pop per lane, so a lane
    holding a message in flight hands out nothing else until that message is
    deleted or its lease lapses — which makes the real ceiling M, and puts
    head-of-line blocking on a queue that is supposed not to have any.

    `queen.partitions=1` is the cleanest possible statement of it: no hashing,
    no luck, three messages and one lane. A generous visibility so the lease
    cannot lapse underneath the measurement.
    """
    _, url = make_queue(c, "inflight", {"queen.partitions": "1", "VisibilityTimeout": "300"})
    c.send_message_batch(QueueUrl=url, Entries=[
        {"Id": f"e{i}", "MessageBody": f"lane-{i}"} for i in range(3)])

    got = drain(c, url, 3, timeout=12.0)
    print(f"#   partitions=1, 3 sent, {len(got)} in flight at once", flush=True)
    check_eq("InFlight.three_messages_are_all_receivable_at_once", len(got), 3)

    depth = c.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    print(f"#   depth: visible={depth.get('ApproximateNumberOfMessages')} "
          f"not-visible={depth.get('ApproximateNumberOfMessagesNotVisible')}", flush=True)
    # Whatever the ceiling turns out to be, the depth attributes have to add up:
    # KEDA and every autoscaler read them, and they are the one place the
    # blocked messages are still visible as work.
    total = int(depth.get("ApproximateNumberOfMessages", 0)) + \
        int(depth.get("ApproximateNumberOfMessagesNotVisible", 0))
    check_eq("InFlight.depth_attributes_account_for_every_message", total, 3)

    for message in got:
        c.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
    # ...and nothing is lost either way: the rest are readable once the lane is
    # free, which is what makes the cap a throughput property and not a
    # correctness one.
    rest = drain_deleting(c, url, 3 - len(got), timeout=30.0) if len(got) < 3 else []
    check_eq("InFlight.every_message_is_eventually_receivable", len(got) + len(rest), 3)


# --------------------------------------------------------------- long polling


@test
def t_long_poll_actually_waits(c):
    _, url = make_queue(c, "longpoll")

    started = time.monotonic()
    response = c.receive_message(QueueUrl=url, WaitTimeSeconds=3)
    waited = time.monotonic() - started
    check_eq("ReceiveMessage.long_poll_returns_empty", len(response.get("Messages", [])), 0)
    # The whole point of a long poll is that it did NOT answer immediately: a
    # facade that ignored WaitTimeSeconds would return in milliseconds and look
    # identical in every other respect.
    check("ReceiveMessage.long_poll_waited", 2.5 <= waited <= 8.0, f"returned after {waited:.2f}s")

    started = time.monotonic()
    response = c.receive_message(QueueUrl=url, WaitTimeSeconds=0)
    short = time.monotonic() - started
    check_eq("ReceiveMessage.short_poll_returns_empty", len(response.get("Messages", [])), 0)
    check("ReceiveMessage.short_poll_does_not_wait", short < 2.0, f"returned after {short:.2f}s")

    # A long poll with a message already waiting answers at once rather than
    # sitting out its timeout.
    c.send_message(QueueUrl=url, MessageBody="waiting")
    started = time.monotonic()
    got = drain(c, url, 1, timeout=10.0)
    elapsed = time.monotonic() - started
    check_eq("ReceiveMessage.long_poll_finds_a_waiting_message", len(got), 1)
    check("ReceiveMessage.long_poll_returns_early_when_it_can",
          elapsed < 8.0, f"took {elapsed:.2f}s")
    if got:
        c.delete_message(QueueUrl=url, ReceiptHandle=got[0]["ReceiptHandle"])


# ------------------------------------------------------------ visibility


@test
def t_change_message_visibility(c):
    _, url = make_queue(c, "vis", {"VisibilityTimeout": "2"})
    c.send_message(QueueUrl=url, MessageBody="visible-again")

    got = drain(c, url, 1)
    if not check_eq("ChangeMessageVisibility.first_receive", len(got), 1):
        return
    handle = got[0]["ReceiptHandle"]

    # EXTEND. The queue's own visibility is 2s, so without this the message
    # would be back within the window below; with it, it must not be.
    c.change_message_visibility(QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=120)
    check_eq("ChangeMessageVisibility.extend_hides_the_message",
             len(drain(c, url, 1, timeout=6.0)), 0)

    # TERMINATE. Zero releases it immediately, which is how every consumer
    # library nacks.
    c.change_message_visibility(QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=0)
    back = drain(c, url, 1, timeout=15.0, AttributeNames=["All"])
    if not check_eq("ChangeMessageVisibility.zero_returns_the_message", len(back), 1):
        return
    check_eq("ChangeMessageVisibility.same_message_came_back",
             back[0]["MessageId"], got[0]["MessageId"])
    check_eq("ChangeMessageVisibility.body_survived_the_release",
             back[0]["Body"], "visible-again")
    # A redelivery is a second delivery, and SQS counts it.
    check_eq("ChangeMessageVisibility.receive_count_after_release",
             back[0].get("Attributes", {}).get("ApproximateReceiveCount"), "2")

    # The OLD handle belongs to a lease that no longer exists. AWS's contract is
    # that it fails — as ReceiptHandleIsInvalid or MessageNotInflight, both of
    # which are in the catalog — rather than silently moving the new delivery.
    try:
        c.change_message_visibility(QueueUrl=url, ReceiptHandle=handle, VisibilityTimeout=60)
        fail("ChangeMessageVisibility.stale_handle_refused", "the stale handle was accepted")
    except botocore.exceptions.ClientError as e:
        code = query_error_code(e) or error_code(e)
        check("ChangeMessageVisibility.stale_handle_refused",
              code in ("ReceiptHandleIsInvalid", "MessageNotInflight"), f"got {code}")

    c.delete_message(QueueUrl=url, ReceiptHandle=back[0]["ReceiptHandle"])


@test
def t_receive_count_increments_after_expiry(c):
    """The visibility timeout is a real lease on the broker, not a facade timer.

    Nothing here calls ChangeMessageVisibility: the message is received and
    ABANDONED, and the assertion is that the broker took it back on its own and
    that the facade reported the second delivery as the second delivery.
    """
    _, url = make_queue(c, "expiry", {"VisibilityTimeout": "2"})
    c.send_message(QueueUrl=url, MessageBody="abandoned")

    first = drain(c, url, 1, AttributeNames=["All"])
    if not check_eq("ReceiveCount.first_delivery", len(first), 1):
        return
    check_eq("ReceiveCount.first_delivery_is_one",
             first[0].get("Attributes", {}).get("ApproximateReceiveCount"), "1")
    check("ReceiveCount.sent_timestamp_is_epoch_millis",
          _looks_like_epoch_millis(first[0].get("Attributes", {}).get("SentTimestamp")),
          f"got {first[0].get('Attributes', {}).get('SentTimestamp')!r}")

    # Deliberately no delete. The 2s lease has to lapse on its own; the wait is
    # generous because a reclaim is a sweep and not an alarm clock.
    time.sleep(3.0)
    second = drain(c, url, 1, timeout=30.0, AttributeNames=["All"])
    if not check_eq("ReceiveCount.redelivered_after_the_lease_lapsed", len(second), 1):
        return
    check_eq("ReceiveCount.same_message", second[0]["MessageId"], first[0]["MessageId"])
    check_eq("ReceiveCount.second_delivery_is_two",
             second[0].get("Attributes", {}).get("ApproximateReceiveCount"), "2")
    check("ReceiveCount.a_redelivery_has_a_new_receipt_handle",
          second[0]["ReceiptHandle"] != first[0]["ReceiptHandle"],
          "the same handle came back")

    c.delete_message(QueueUrl=url, ReceiptHandle=second[0]["ReceiptHandle"])


@test
def t_fifo_sequence_number_survives_the_round_trip(c):
    """C-SQS-3, live: the `SequenceNumber` a send answered comes back on the
    RECEIVE, off a real broker.

    This is the one assertion in this file that cannot be made anywhere else.
    The number is the absolute offset the push allocated, and until C-SQS-3 the
    pop wire did not carry it at all — so a facade could only answer it on the
    way in. `render_pop_parts` now writes an `"offset"` per delivered message
    and the facade renders it as `SequenceNumber` on a FIFO queue; the crate's
    own suite proves that against a double that was TAUGHT to emit the field,
    which is exactly the thing a live run has to check independently.

    A FIFO queue is used because that is where SQS has the field at all: on a
    standard queue AWS answers none, and neither does this facade, which the
    last two assertions pin.
    """
    _, fifo_url = make_queue(
        c, "seqnum", {"FifoQueue": "true", "VisibilityTimeout": "300"}, fifo=True
    )

    sent = []
    for i in range(3):
        answer = c.send_message(
            QueueUrl=fifo_url,
            MessageBody=f"ordered-{i}",
            MessageGroupId="g-seq",
            MessageDeduplicationId=f"{RUN}-seq-{i}",
        )
        sent.append(answer.get("SequenceNumber"))
    if not check("SequenceNumber.send_answers_one_per_message",
                 all(s is not None for s in sent), f"got {sent!r}"):
        return
    check("SequenceNumber.send_side_is_ascending_within_the_group",
          [int(s) for s in sent] == sorted(int(s) for s in sent),
          f"got {sent!r}")

    # ONE receive: a FIFO claim is a run of one group, so all three arrive
    # together and in order.
    got = drain(c, fifo_url, 3, AttributeNames=["All"])
    if not check_eq("SequenceNumber.received_the_whole_group", len(got), 3):
        return
    check_eq("SequenceNumber.bodies_in_publish_order",
             [m["Body"] for m in got], [f"ordered-{i}" for i in range(3)])
    received = [m.get("Attributes", {}).get("SequenceNumber") for m in got]
    check_eq("SequenceNumber.receive_answers_what_the_send_answered", received, sent)
    check("SequenceNumber.the_rest_of_the_fifo_view_is_intact",
          all(m.get("Attributes", {}).get("MessageGroupId") == "g-seq" for m in got),
          f"got {[m.get('Attributes', {}).get('MessageGroupId') for m in got]!r}")
    check_eq("SequenceNumber.dedup_ids_come_back",
             [m.get("Attributes", {}).get("MessageDeduplicationId") for m in got],
             [f"{RUN}-seq-{i}" for i in range(3)])
    for message in got:
        c.delete_message(QueueUrl=fifo_url, ReceiptHandle=message["ReceiptHandle"])

    # A standard queue has the field on NEITHER side, which is AWS's own shape:
    # the offset is on the wire for every message, and answering a
    # SequenceNumber off a standard queue would be answering a field AWS does
    # not send.
    _, plain_url = make_queue(c, "seqnum-plain", {"VisibilityTimeout": "30"})
    plain_send = c.send_message(QueueUrl=plain_url, MessageBody="unsequenced")
    check("SequenceNumber.absent_on_a_standard_send",
          "SequenceNumber" not in plain_send,
          f"got {plain_send.get('SequenceNumber')!r}")
    plain = drain(c, plain_url, 1, AttributeNames=["All"])
    if check_eq("SequenceNumber.standard_message_received", len(plain), 1):
        check("SequenceNumber.absent_on_a_standard_receive",
              "SequenceNumber" not in plain[0].get("Attributes", {}),
              f"got {plain[0].get('Attributes', {}).get('SequenceNumber')!r}")
        c.delete_message(QueueUrl=plain_url, ReceiptHandle=plain[0]["ReceiptHandle"])


# ------------------------------------------------------------------ deleting


@test
def t_delete_message(c):
    _, url = make_queue(c, "delete", {"VisibilityTimeout": "2"})
    c.send_message(QueueUrl=url, MessageBody="delete-me")

    got = drain(c, url, 1)
    if not check_eq("DeleteMessage.received", len(got), 1):
        return
    handle = got[0]["ReceiptHandle"]

    response = c.delete_message(QueueUrl=url, ReceiptHandle=handle)
    check_eq("DeleteMessage.status", response["ResponseMetadata"]["HTTPStatusCode"], 200)

    # The visibility is 2s: a deleted message that was merely released would be
    # back inside this window.
    check_eq("DeleteMessage.does_not_come_back", len(drain(c, url, 1, timeout=8.0)), 0)

    # Deleting twice is a normal consumer's retry after a timed-out response,
    # and AWS answers it with a success.
    try:
        again = c.delete_message(QueueUrl=url, ReceiptHandle=handle)
        check_eq("DeleteMessage.double_delete_is_idempotent",
                 again["ResponseMetadata"]["HTTPStatusCode"], 200)
    except botocore.exceptions.ClientError as e:
        fail("DeleteMessage.double_delete_is_idempotent",
             f"{query_error_code(e) or error_code(e)}: "
             f"{e.response.get('Error', {}).get('Message')}")

    # A handle that was never minted by this facade is a forgery, not a stale
    # lease, and must be refused.
    expect_error("DeleteMessage.forged_handle_refused", "ReceiptHandleIsInvalid",
                 lambda: c.delete_message(
                     QueueUrl=url,
                     ReceiptHandle=base64.urlsafe_b64encode(b"not-a-handle").decode()))


@test
def t_delete_message_batch_partial(c):
    _, url = make_queue(c, "delbatch", {"VisibilityTimeout": "300"})
    c.send_message_batch(QueueUrl=url, Entries=[
        {"Id": f"e{i}", "MessageBody": f"partial-{i}"} for i in range(3)
    ])
    # `hold` and not `drain`: three messages can share a lane, and this test
    # needs three handles alive at the same moment.
    got = hold(c, url, 3)
    if not check_eq("DeleteMessageBatch.received_three", len(got), 3):
        return

    # Two real handles and one forgery in the same call: AWS reports per-entry
    # failure and does NOT fail the request.
    entries = [{"Id": "a", "ReceiptHandle": got[0]["ReceiptHandle"]},
               {"Id": "b", "ReceiptHandle": got[1]["ReceiptHandle"]},
               {"Id": "bad", "ReceiptHandle": base64.urlsafe_b64encode(b"forged").decode()}]
    result = c.delete_message_batch(QueueUrl=url, Entries=entries)
    check_eq("DeleteMessageBatch.partial_success_ids",
             sorted(s["Id"] for s in result.get("Successful", [])), ["a", "b"])
    failed = result.get("Failed", [])
    check_eq("DeleteMessageBatch.partial_failure_ids", [f["Id"] for f in failed], ["bad"])
    if failed:
        check("DeleteMessageBatch.failure_entry_has_a_code",
              bool(failed[0].get("Code")), f"got {failed[0]!r}")
        check_eq("DeleteMessageBatch.failure_is_the_senders_fault",
                 failed[0].get("SenderFault"), True)

    # The all-succeed shape, on the one handle the partial call left alive.
    clean = c.delete_message_batch(
        QueueUrl=url, Entries=[{"Id": "c", "ReceiptHandle": got[2]["ReceiptHandle"]}])
    check_eq("DeleteMessageBatch.all_succeeded", [s["Id"] for s in clean.get("Successful", [])], ["c"])
    check_eq("DeleteMessageBatch.none_failed", clean.get("Failed", []), [])


# -------------------------------------------------------------------- errors


@test
def t_errors(c):
    absent = f"{ENDPOINT}/{ACCOUNT}/absent-{RUN}"
    expect_error("Errors.get_queue_url_on_a_missing_queue", "QueueDoesNotExist",
                 lambda: c.get_queue_url(QueueName=f"absent-{RUN}"))
    expect_error("Errors.receive_on_a_missing_queue", "QueueDoesNotExist",
                 lambda: c.receive_message(QueueUrl=absent))
    expect_error("Errors.get_attributes_on_a_missing_queue", "QueueDoesNotExist",
                 lambda: c.get_queue_attributes(QueueUrl=absent, AttributeNames=["All"]))
    expect_error("Errors.send_to_a_missing_queue", "QueueDoesNotExist",
                 lambda: c.send_message(QueueUrl=absent, MessageBody="nowhere"))
    # A URL from another account is a queue this deployment does not serve, and
    # must read as absent rather than as a malformed request.
    expect_error("Errors.queue_url_for_another_account", "QueueDoesNotExist",
                 lambda: c.receive_message(QueueUrl=f"{ENDPOINT}/999999999999/anything"))

    # A wrong secret is a signature that does not verify. It must be refused as
    # a signature problem, not as a missing queue — the rig's whole auth story
    # is one static credential, so this is the only proof that it is checked.
    impostor = boto3.client("sqs", endpoint_url=ENDPOINT, region_name=REGION,
                            aws_access_key_id=AKID, aws_secret_access_key="wrong-secret")
    try:
        impostor.list_queues()
        fail("Errors.wrong_secret_is_refused", "the request was served")
    except botocore.exceptions.ClientError as e:
        code = query_error_code(e) or error_code(e)
        check("Errors.wrong_secret_is_refused",
              code in ("SignatureDoesNotMatch", "InvalidClientTokenId", "AccessDenied"),
              f"got {code}")


@test
def t_delete_queue(c):
    name, url = make_queue(c, "gone")
    c.send_message(QueueUrl=url, MessageBody="doomed")

    response = c.delete_queue(QueueUrl=url)
    check_eq("DeleteQueue.status", response["ResponseMetadata"]["HTTPStatusCode"], 200)
    CREATED.remove(url)

    expect_error("DeleteQueue.url_lookup_afterwards", "QueueDoesNotExist",
                 lambda: c.get_queue_url(QueueName=name))
    expect_error("DeleteQueue.receive_afterwards", "QueueDoesNotExist",
                 lambda: c.receive_message(QueueUrl=url))

    check("DeleteQueue.is_out_of_ListQueues",
          url not in c.list_queues().get("QueueUrls", []), "still listed")

    # AWS arms a 60-second window in which the name cannot be reused, and SDK
    # retry behaviour depends on the code.
    expect_error("DeleteQueue.name_is_reserved_for_sixty_seconds", "QueueDeletedRecently",
                 lambda: c.create_queue(QueueName=name))


# ------------------------------------------------------------------ small fry


def _is_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def _looks_like_epoch_millis(value):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return False
    # Any instant between 2001 and 2286 in MILLISECONDS. Seconds would land far
    # below the floor, microseconds far above the ceiling — which is the whole
    # thing this is here to catch.
    return 1_000_000_000_000 <= n <= 9_999_999_999_999


def _as_bytes(value):
    if value is None or isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if hasattr(value, "read"):
        return value.read()
    if isinstance(value, str):
        return base64.b64decode(value)
    return bytes(value)


# ---------------------------------------------------------------------- main


def teardown(c):
    for url in list(CREATED):
        try:
            c.delete_queue(QueueUrl=url)
        except Exception:
            pass


def main():
    print(f"# endpoint {ENDPOINT}  region {REGION}  account {ACCOUNT}  run {RUN}", flush=True)
    c = client()
    try:
        c.list_queues()
    except Exception as e:
        print(f"FAIL rig.reachable: {e}")
        print("RESULT: FAIL")
        return 1

    for fn in TESTS:
        try:
            fn(c)
        except Exception:
            # One test blowing up must not cost the run every assertion after
            # it: the trace is printed, the test is a failure, and the rest go on.
            fail(fn.__name__, "unexpected exception")
            traceback.print_exc()

    teardown(c)

    # The contract's protocol line: what boto3 ACTUALLY put on the wire, counted
    # per request by `_record_protocol`, not inferred from its version.
    for spoken, count in sorted(PROTOCOLS.items(), key=lambda kv: -kv[1]):
        print(f"# protocol spoken: {spoken} — {count} request(s)", flush=True)
    print(f"# {PASSES} passed, {len(FAILURES)} failed", flush=True)
    for name in FAILURES:
        print(f"#   failed: {name}", flush=True)
    print(f"RESULT: {'FAIL' if FAILURES else 'PASS'}", flush=True)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
