#!/usr/bin/env python3
"""The queen-sqs M4 smoke: SNS over boto3, against a live facade, a live broker
and a live Postgres.

WHAT THIS IS FOR. `queen-sqs/src/sns/*` and `src/http_tests.rs` drive the same
seventeen SNS actions against `FakeQueen`, where a transaction always commits, a
dedup key is a map entry and a fan-out is a list append. This file is the other
half: the same surface where a publish is a real `POST /api/v1/transaction`, a
repeated `MessageDeduplicationId` is refused by Postgres rather than by a double,
and a subscriber's queue is a queue somebody has to pop.

THE CONTRACT (the same one `smoke_m0.py` copied from queen-kafka's
CLIENT_MATRIX.md, verbatim in what matters):

  * the stack comes from the environment, never from a hardcoded address;
  * ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
  * `RESULT: PASS` or `RESULT: FAIL` as the last line;
  * a nonzero exit status when anything failed.

  $ queen-sqs/compat/rig.sh up
  $ source queen-sqs/compat/.rig/env.sh
  $ python queen-sqs/compat/smoke_m4_sns.py

TWO CLIENTS, ONE LISTENER, TWO PROTOCOLS. `boto3.client("sns")` speaks the Query
protocol (form-encoded in, XML out; `apiVersion` 2010-03-31) and
`boto3.client("sqs")` speaks AWS JSON 1.0 against the SAME endpoint — so this
file is also the first thing in the compat tree that exercises the Query/XML
codec at all, which `M0_SMOKE.md` records as untested by either M0 suite. Both
clients sign SigV4, for `sns` and for `sqs` respectively, and the facade accepts
both scopes on one port.

ABSENCE IS NEVER ASSERTED BY ITSELF. "The filtered-out message did not arrive"
and "the deduplicated publish delivered nothing" are unfalsifiable on their own —
an empty receive is also what a slow broker looks like. Every negative here is
followed by a MARKER publish that must arrive, and the assertion is on the whole
set that came back: the marker's presence is what dates the absence.

WHY EACH TEST BUILDS ITS OWN TOPIC AND QUEUES. A subscription is a filter, a raw
flag and a delivery target all at once, and the three interact; sharing one
subscription across tests would make a failure in the fourth of them a puzzle
about the second. Names carry the run id for `smoke_m0.py`'s reason too:
`DeleteQueue` arms a 60-second `QueueDeletedRecently` tombstone.
"""

import base64
import hashlib
import json
import os
import re
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
QUEUES = []
TOPICS = []

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


def note(text):
    """An observation that is not a verdict: it goes to M4_SMOKE.md, not to the
    pass/fail count. Used where this facade's behaviour is defensible and only a
    run against real AWS can settle whether it is right."""
    print(f"# note {text}", flush=True)


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


# ------------------------------------------------------------------- clients


# WHICH PROTOCOL EACH CLIENT ACTUALLY SPOKE, read off the request botocore
# built rather than assumed from the SDK's version — `smoke_m0.py`'s recorder,
# with the service in the key because this file drives TWO clients against ONE
# listener and the whole point is that they do not agree. AWS JSON 1.0 sends
# `X-Amz-Target: Amazon<Service>.<Action>` with an `application/x-amz-json-1.0`
# body; the Query protocol sends neither and a form-encoded one. The handler
# only records — it returns None, which is how botocore's event system says
# "carry on".
PROTOCOLS = {}


def _protocol_recorder(service):
    def record(request, **_):
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
            spoken = (
                f"unrecognized (Content-Type: {content_type!r}, "
                f"X-Amz-Target: {target!r})"
            )
        key = (service, spoken)
        PROTOCOLS[key] = PROTOCOLS.get(key, 0) + 1
        return None

    return record


def sns_client():
    c = boto3.client(
        "sns",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=AKID,
        aws_secret_access_key=SECRET,
    )
    c.meta.events.register("before-send.sns.*", _protocol_recorder("sns"))
    return c


def sqs_client():
    c = boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=AKID,
        aws_secret_access_key=SECRET,
    )
    c.meta.events.register("before-send.sqs.*", _protocol_recorder("sqs"))
    return c


# SNS spells its errors ONCE — unlike SQS, which carries a legacy Query code
# beside the shape name (`smoke_m0.py`'s ERROR_CODES). The Query protocol puts
# the shape's own `error.code` in `<Code>`, and botocore's error factory names
# the exception class after the SHAPE. Both are pinned, because a facade that
# answered `NotFoundException` in `<Code>` would still raise a ClientError and
# would break every `except sns.exceptions.NotFoundException` in the world.
SNS_EXCEPTIONS = {
    "NotFound": "NotFoundException",
    "InvalidParameter": "InvalidParameterException",
}


def error_code(exc):
    return exc.response.get("Error", {}).get("Code")


def error_message(exc):
    return exc.response.get("Error", {}).get("Message", "")


def http_status(exc):
    return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")


def expect_sns_error(name, code, call, status=None):
    """Run `call`, require it to fail as SNS's `code`, in both spellings."""
    try:
        call()
    except botocore.exceptions.ClientError as e:
        got = (error_code(e), type(e).__name__)
        want = (code, SNS_EXCEPTIONS[code])
        if status is not None:
            got, want = (got + (http_status(e),)), (want + (status,))
        check_eq(name, got, want)
        return e
    fail(name, f"the call succeeded; expected {code}")
    return None


# ------------------------------------------------------------------- fixtures


def make_topic(sns, label, attributes=None):
    """A topic named for this run, remembered so the teardown removes it."""
    name = f"m4-{label}-{RUN}"
    kwargs = {"Name": name}
    if attributes:
        kwargs["Attributes"] = attributes
    arn = sns.create_topic(**kwargs)["TopicArn"]
    TOPICS.append(arn)
    return name, arn


def make_fifo_topic(sns, label, attributes=None):
    name = f"m4-{label}-{RUN}.fifo"
    merged = {"FifoTopic": "true"}
    merged.update(attributes or {})
    arn = sns.create_topic(Name=name, Attributes=merged)["TopicArn"]
    TOPICS.append(arn)
    return name, arn


def make_queue(sqs, label, attributes=None):
    """Answers (name, url, arn) — the ARN because that is what `Subscribe`
    takes, and this facade mints it from the same (region, account) the URL
    carries."""
    name = f"m4-{label}-{RUN}"
    kwargs = {"QueueName": name}
    if attributes:
        kwargs["Attributes"] = attributes
    url = sqs.create_queue(**kwargs)["QueueUrl"]
    QUEUES.append(url)
    return name, url, f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}"


def make_fifo_queue(sqs, label, attributes=None):
    merged = {"FifoQueue": "true"}
    merged.update(attributes or {})
    name = f"m4-{label}-{RUN}.fifo"
    url = sqs.create_queue(QueueName=name, Attributes=merged)["QueueUrl"]
    QUEUES.append(url)
    return name, url, f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}"


def collect(sqs, url, count, timeout=20.0, **kwargs):
    """Receive up to `count` messages, DELETING each one, until the timeout.

    Deleting is not tidiness: a standard queue hands out at most one message per
    lane at a time (`M0_SMOKE.md` D2), so a loop that never deleted could not
    collect more messages than the queue has partitions. Order is the order they
    came back in, which on a standard queue means nothing and on a FIFO queue
    means everything.
    """
    got = []
    deadline = time.monotonic() + timeout
    while len(got) < count and time.monotonic() < deadline:
        response = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=min(10, count - len(got)),
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
            **kwargs,
        )
        for message in response.get("Messages", []):
            got.append(message)
            sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
    return got


def collect_exactly(sqs, url, count, settle=4.0, timeout=20.0):
    """`count` messages, and then a settling window that must stay empty.

    This is the shape every NEGATIVE assertion in this file is written on: the
    messages that should be there, followed by proof that nothing else was
    behind them.
    """
    got = collect(sqs, url, count, timeout=timeout)
    extra = collect(sqs, url, 10, timeout=settle)
    return got, extra


def bodies(messages):
    return [m["Body"] for m in messages]


def one_message(sqs, url, timeout=20.0):
    got = collect(sqs, url, 1, timeout=timeout)
    return got[0] if got else None


def notification(message):
    """The SNS envelope inside an SQS body, or None when it is not JSON."""
    try:
        parsed = json.loads(message["Body"])
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


ISO8601_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _is_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


# ----------------------------------------------------------------- the topic


@test
def t_create_topic(sns, sqs):
    name, arn = make_topic(sns, "topic", {"DisplayName": "M4 orders"})
    check_eq("CreateTopic.arn", arn, f"arn:aws:sns:{REGION}:{ACCOUNT}:{name}")

    # The idempotent create, in the three shapes a provisioner performs it: the
    # identical request, the request that names NOTHING, and the subset. AWS
    # answers the existing topic's ARN for all three — the refusal is reserved
    # for a request that CONTRADICTS the topic — and `first_conflict` in
    # `sns/admin.rs` is one-directional for exactly this reason (the same rule
    # `M0_SMOKE.md` D1 got wrong on the queue side).
    again = sns.create_topic(Name=name, Attributes={"DisplayName": "M4 orders"})["TopicArn"]
    check_eq("CreateTopic.idempotent_identical_request", again, arn)
    try:
        bare = sns.create_topic(Name=name)["TopicArn"]
        check_eq("CreateTopic.idempotent_without_attributes", bare, arn)
    except botocore.exceptions.ClientError as e:
        fail(
            "CreateTopic.idempotent_without_attributes",
            f"{error_code(e)}: {error_message(e)} — a request naming no attributes "
            f"names none that differ",
        )

    expect_sns_error(
        "CreateTopic.conflicting_attribute_refused",
        "InvalidParameter",
        lambda: sns.create_topic(Name=name, Attributes={"DisplayName": "something else"}),
    )

    attributes = sns.get_topic_attributes(TopicArn=arn)["Attributes"]
    check_eq("GetTopicAttributes.arn", attributes.get("TopicArn"), arn)
    check_eq("GetTopicAttributes.owner", attributes.get("Owner"), ACCOUNT)
    check_eq("GetTopicAttributes.display_name", attributes.get("DisplayName"), "M4 orders")
    check_eq(
        "GetTopicAttributes.subscription_counts",
        (
            attributes.get("SubscriptionsConfirmed"),
            attributes.get("SubscriptionsPending"),
            attributes.get("SubscriptionsDeleted"),
        ),
        ("0", "0", "0"),
    )

    listed = set()
    token = None
    for _ in range(20):
        page = sns.list_topics(**({"NextToken": token} if token else {}))
        listed.update(t["TopicArn"] for t in page.get("Topics", []))
        token = page.get("NextToken")
        if not token:
            break
    check("ListTopics.contains_the_new_topic", arn in listed, f"{arn} not in {len(listed)} topics")


# ---------------------------------------------------------- the subscription


@test
def t_subscribe(sns, sqs):
    _, topic = make_topic(sns, "sub-topic")
    qname, qurl, qarn = make_queue(sqs, "sub-queue")

    subscription = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)
    arn = subscription["SubscriptionArn"]

    # AUTO-CONFIRMED. AWS answers the literal string "pending confirmation" for
    # a subscription that needs a handshake; a same-account SQS subscription
    # never does, so the real ARN is the only answer this action has. A client
    # that got the placeholder would store it and then fail every later
    # `SetSubscriptionAttributes`.
    check(
        "Subscribe.arn_is_not_pending_confirmation",
        arn != "pending confirmation",
        f"got {arn!r}",
    )
    check(
        "Subscribe.arn_extends_the_topic_arn",
        arn.startswith(topic + ":") and len(arn.split(":")) == 7,
        f"got {arn!r}",
    )
    check("Subscribe.arn_id_is_a_uuid", _is_uuid(arn.split(":")[-1]), f"got {arn!r}")

    attributes = sns.get_subscription_attributes(SubscriptionArn=arn)["Attributes"]
    check_eq(
        "GetSubscriptionAttributes.identity",
        (
            attributes.get("SubscriptionArn"),
            attributes.get("TopicArn"),
            attributes.get("Protocol"),
            attributes.get("Endpoint"),
            attributes.get("Owner"),
        ),
        (arn, topic, "sqs", qarn, ACCOUNT),
    )
    check_eq(
        "GetSubscriptionAttributes.confirmed_at_creation",
        (
            attributes.get("PendingConfirmation"),
            attributes.get("ConfirmationWasAuthenticated"),
        ),
        ("false", "true"),
    )
    check_eq(
        "GetSubscriptionAttributes.raw_message_delivery_defaults_off",
        attributes.get("RawMessageDelivery"),
        "false",
    )
    # No policy, so no scope — AWS reports the scope only where there is a
    # policy for it to apply to, and a provisioner that read one here would
    # report drift on every reconcile.
    check(
        "GetSubscriptionAttributes.no_filter_policy_scope_without_a_policy",
        "FilterPolicyScope" not in attributes and "FilterPolicy" not in attributes,
        f"got {sorted(attributes)}",
    )

    repeat = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)["SubscriptionArn"]
    check_eq("Subscribe.idempotent_per_topic_protocol_endpoint", repeat, arn)

    listing = sns.list_subscriptions_by_topic(TopicArn=topic)
    subscriptions = listing.get("Subscriptions", [])
    check_eq("ListSubscriptionsByTopic.count", len(subscriptions), 1)
    if subscriptions:
        entry = subscriptions[0]
        check_eq(
            "ListSubscriptionsByTopic.entry",
            (
                entry.get("SubscriptionArn"),
                entry.get("TopicArn"),
                entry.get("Protocol"),
                entry.get("Endpoint"),
                entry.get("Owner"),
            ),
            (arn, topic, "sqs", qarn, ACCOUNT),
        )

    counts = sns.get_topic_attributes(TopicArn=topic)["Attributes"]
    check_eq(
        "GetTopicAttributes.confirmed_count_follows_subscribe",
        counts.get("SubscriptionsConfirmed"),
        "1",
    )

    # An unknown topic must NOT answer an empty list: a client reads that as
    # "nothing is subscribed" rather than "you asked about the wrong topic".
    ghost = f"arn:aws:sns:{REGION}:{ACCOUNT}:m4-ghost-{RUN}"
    expect_sns_error(
        "ListSubscriptionsByTopic.unknown_topic_is_not_an_empty_list",
        "NotFound",
        lambda: sns.list_subscriptions_by_topic(TopicArn=ghost),
        status=404,
    )

    # The two refusals a v0 subscriber meets, each naming its member.
    expect_sns_error(
        "Subscribe.non_sqs_protocol_refused",
        "InvalidParameter",
        lambda: sns.subscribe(
            TopicArn=topic, Protocol="https", Endpoint="https://example.invalid/hook"
        ),
    )
    expect_sns_error(
        "Subscribe.endpoint_queue_must_exist",
        "InvalidParameter",
        lambda: sns.subscribe(
            TopicArn=topic,
            Protocol="sqs",
            Endpoint=f"arn:aws:sqs:{REGION}:{ACCOUNT}:m4-absent-{RUN}",
        ),
    )
    del qname, qurl


# -------------------------------------------------------- the notification


@test
def t_publish_notification_envelope(sns, sqs):
    _, topic = make_topic(sns, "notify-topic")
    _, qurl, qarn = make_queue(sqs, "notify-queue")
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)

    body = "the payload, with spaces and a comma, and \"quotes\""
    attributes = {
        "event": {"DataType": "String", "StringValue": "order.created"},
        "count": {"DataType": "Number", "StringValue": "7"},
        "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01\x02rig"},
    }
    published = sns.publish(
        TopicArn=topic, Message=body, Subject="M4 subject", MessageAttributes=attributes
    )
    message_id = published["MessageId"]
    check("Publish.message_id_is_a_uuid", _is_uuid(message_id), f"got {message_id!r}")

    message = one_message(sqs, qurl)
    if not check("Publish.notification_arrives", message is not None, "no message in 20s"):
        return

    envelope = notification(message)
    if not check(
        "Publish.body_is_json", envelope is not None, f"body was {message['Body']!r}"
    ):
        return

    check_eq("Notification.type", envelope.get("Type"), "Notification")
    check_eq("Notification.topic_arn", envelope.get("TopicArn"), topic)
    check_eq("Notification.message", envelope.get("Message"), body)
    check_eq("Notification.subject", envelope.get("Subject"), "M4 subject")
    # THE PUBLISH'S id, not the delivery's. It is what lets one fan-out be
    # correlated end to end, and it is AWS's own behaviour.
    check_eq("Notification.message_id_is_the_publishers", envelope.get("MessageId"), message_id)
    check(
        "Notification.timestamp_is_iso8601_millis",
        isinstance(envelope.get("Timestamp"), str)
        and bool(ISO8601_MS.match(envelope["Timestamp"])),
        f"got {envelope.get('Timestamp')!r}",
    )
    check_eq("Notification.signature_version", envelope.get("SignatureVersion"), "1")
    check_eq(
        "Notification.message_attributes",
        envelope.get("MessageAttributes"),
        {
            "event": {"Type": "String", "Value": "order.created"},
            "count": {"Type": "Number", "Value": "7"},
            "blob": {
                "Type": "Binary",
                "Value": base64.b64encode(b"\x00\x01\x02rig").decode("ascii"),
            },
        },
    )
    # The three fields AWS writes and this deployment cannot stand behind. Their
    # absence is the honest half of an unsigned notification, and it is pinned
    # so that a later change to add a Signature nothing can verify is loud.
    check(
        "Notification.carries_no_unverifiable_signature_fields",
        not any(k in envelope for k in ("Signature", "SigningCertURL", "UnsubscribeURL")),
        f"got {sorted(envelope)}",
    )
    # ...and the envelope's attributes are NOT also written as SQS message
    # attributes: two copies of one truth can disagree.
    check(
        "Notification.no_sqs_message_attributes_in_envelope_mode",
        not message.get("MessageAttributes"),
        f"got {message.get('MessageAttributes')!r}",
    )
    check_eq(
        "Notification.body_md5",
        message.get("MD5OfBody"),
        hashlib.md5(message["Body"].encode("utf-8")).hexdigest(),
    )
    # The SQS MessageId is the BROKER's, one per delivery, and is a different id
    # from the publish's — both are true at AWS too.
    check(
        "Notification.sqs_message_id_is_not_the_publish_id",
        message.get("MessageId") != message_id,
        "the delivery reused the publish's id",
    )


# --------------------------------------------------------- raw delivery


@test
def t_raw_message_delivery(sns, sqs):
    _, topic = make_topic(sns, "raw-topic")
    _, qurl, qarn = make_queue(sqs, "raw-queue")
    arn = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)["SubscriptionArn"]

    sns.set_subscription_attributes(
        SubscriptionArn=arn, AttributeName="RawMessageDelivery", AttributeValue="true"
    )
    read = sns.get_subscription_attributes(SubscriptionArn=arn)["Attributes"]
    check_eq(
        "SetSubscriptionAttributes.raw_message_delivery_reads_back",
        read.get("RawMessageDelivery"),
        "true",
    )

    body = '{"order":42,"note":"raw, not enveloped"}'
    published = sns.publish(
        TopicArn=topic,
        Message=body,
        Subject="ignored in raw mode",
        MessageAttributes={
            "event": {"DataType": "String", "StringValue": "order.created"},
            "count": {"DataType": "Number", "StringValue": "7"},
        },
    )

    message = one_message(sqs, qurl)
    if not check("RawMessageDelivery.arrives", message is not None, "no message in 20s"):
        return

    # THE WHOLE MEANING OF "RAW": a consumer written against a queue reads the
    # body it was sent and never learns a topic was involved.
    check_eq("RawMessageDelivery.body_is_the_message_alone", message["Body"], body)
    check(
        "RawMessageDelivery.body_is_not_an_envelope",
        (notification(message) or {}).get("Type") != "Notification",
        "the body was still an SNS notification",
    )
    check_eq(
        "RawMessageDelivery.attributes_are_forwarded",
        {
            name: (value.get("DataType"), value.get("StringValue"))
            for name, value in (message.get("MessageAttributes") or {}).items()
        },
        {"event": ("String", "order.created"), "count": ("Number", "7")},
    )
    check(
        "RawMessageDelivery.attribute_md5_is_present",
        bool(message.get("MD5OfMessageAttributes")),
        "no MD5OfMessageAttributes on a delivery carrying attributes",
    )
    check_eq(
        "RawMessageDelivery.body_md5",
        message.get("MD5OfBody"),
        hashlib.md5(body.encode("utf-8")).hexdigest(),
    )
    del published


# ------------------------------------------------------------ filter policy


@test
def t_filter_policy(sns, sqs):
    _, topic = make_topic(sns, "filter-topic")
    _, qurl, qarn = make_queue(sqs, "filter-queue")
    arn = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)["SubscriptionArn"]

    policy = {"event": ["order.created"]}
    sns.set_subscription_attributes(
        SubscriptionArn=arn, AttributeName="FilterPolicy", AttributeValue=json.dumps(policy)
    )
    read = sns.get_subscription_attributes(SubscriptionArn=arn)["Attributes"]
    check_eq(
        "SetSubscriptionAttributes.filter_policy_reads_back",
        json.loads(read.get("FilterPolicy", "null")),
        policy,
    )
    check_eq(
        "SetSubscriptionAttributes.filter_scope_defaults_to_attributes",
        read.get("FilterPolicyScope"),
        "MessageAttributes",
    )

    def emit(text, event=None):
        kwargs = {"TopicArn": topic, "Message": text}
        if event is not None:
            kwargs["MessageAttributes"] = {
                "event": {"DataType": "String", "StringValue": event}
            }
        return sns.publish(**kwargs)["MessageId"]

    # Non-matching first, matching second: if the filter leaked, the leak is
    # already in the queue by the time the marker lands.
    emit("filtered-out", "order.deleted")
    matched = emit("kept", "order.created")
    # An ABSENT attribute matches nothing but {"exists": false} — which is what
    # makes a filter policy a whitelist rather than a blacklist.
    emit("no-attributes-at-all")
    marker = emit("marker", "order.created")

    got, extra = collect_exactly(sqs, qurl, 2)
    delivered = sorted(
        (notification(m) or {}).get("Message") for m in got
    )
    check_eq("FilterPolicy.only_matching_publishes_are_delivered", delivered, ["kept", "marker"])
    check_eq("FilterPolicy.nothing_else_was_behind_them", bodies(extra), [])
    check_eq(
        "FilterPolicy.matched_publish_ids_are_carried_through",
        sorted((notification(m) or {}).get("MessageId") for m in got),
        sorted([matched, marker]),
    )

    # An EMPTY value is SNS's spelling for taking a policy off. Storing "" would
    # leave a subscription whose policy matches nothing — a topic that silently
    # delivers to no one.
    sns.set_subscription_attributes(
        SubscriptionArn=arn, AttributeName="FilterPolicy", AttributeValue=""
    )
    cleared = sns.get_subscription_attributes(SubscriptionArn=arn)["Attributes"]
    check(
        "SetSubscriptionAttributes.empty_value_removes_the_policy",
        "FilterPolicy" not in cleared and "FilterPolicyScope" not in cleared,
        f"got {sorted(cleared)}",
    )
    emit("after-removal")
    after, _ = collect_exactly(sqs, qurl, 1, settle=2.0)
    check_eq(
        "FilterPolicy.removal_restores_delivery",
        [(notification(m) or {}).get("Message") for m in after],
        ["after-removal"],
    )


# ------------------------------------------------------------- publish batch


@test
def t_publish_batch(sns, sqs):
    _, topic = make_topic(sns, "batch-topic")
    _, qurl, qarn = make_queue(sqs, "batch-queue")
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)

    answer = sns.publish_batch(
        TopicArn=topic,
        PublishBatchRequestEntries=[
            {"Id": "one", "Message": "batch-one"},
            # A `MessageGroupId` is only valid on a FIFO topic. It is refused
            # PER ENTRY: nine good entries are not lost to one bad one, which is
            # the whole reason SNS models a partial result.
            {"Id": "two", "Message": "batch-bad", "MessageGroupId": "g"},
            {"Id": "three", "Message": "batch-three"},
        ],
    )
    successful = answer.get("Successful", [])
    failed = answer.get("Failed", [])
    check_eq(
        "PublishBatch.successful_ids", sorted(e["Id"] for e in successful), ["one", "three"]
    )
    check_eq("PublishBatch.failed_ids", [e["Id"] for e in failed], ["two"])
    check(
        "PublishBatch.successful_entries_carry_message_ids",
        all(_is_uuid(e.get("MessageId")) for e in successful),
        f"got {[e.get('MessageId') for e in successful]}",
    )
    if failed:
        check_eq(
            "PublishBatch.failure_is_the_senders_fault_and_named",
            (failed[0].get("Code"), failed[0].get("SenderFault")),
            ("InvalidParameter", True),
        )
        check(
            "PublishBatch.failure_message_names_the_member",
            "MessageGroupId" in (failed[0].get("Message") or ""),
            f"got {failed[0].get('Message')!r}",
        )

    got, extra = collect_exactly(sqs, qurl, 2)
    check_eq(
        "PublishBatch.only_the_accepted_entries_are_delivered",
        sorted((notification(m) or {}).get("Message") for m in got),
        ["batch-one", "batch-three"],
    )
    check_eq("PublishBatch.the_refused_entry_delivered_nothing", bodies(extra), [])
    check_eq(
        "PublishBatch.delivered_ids_are_the_answered_ids",
        sorted((notification(m) or {}).get("MessageId") for m in got),
        sorted(e["MessageId"] for e in successful),
    )


# ------------------------------------------------------------------- fanout


@test
def t_fanout_to_two_queues(sns, sqs):
    _, topic = make_topic(sns, "fanout-topic")
    _, url_a, arn_a = make_queue(sqs, "fanout-a")
    _, url_b, arn_b = make_queue(sqs, "fanout-b")
    sub_a = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn_a)["SubscriptionArn"]
    sub_b = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn_b)["SubscriptionArn"]
    check("Fanout.two_distinct_subscriptions", sub_a != sub_b, "one ARN for two queues")

    counts = sns.get_topic_attributes(TopicArn=topic)["Attributes"]
    check_eq("Fanout.confirmed_count_is_two", counts.get("SubscriptionsConfirmed"), "2")

    message_id = sns.publish(TopicArn=topic, Message="fanned out")["MessageId"]

    a = one_message(sqs, url_a)
    b = one_message(sqs, url_b)
    if not check(
        "Fanout.both_queues_receive", a is not None and b is not None,
        f"a={a is not None} b={b is not None}",
    ):
        return
    envelope_a, envelope_b = notification(a) or {}, notification(b) or {}
    check_eq("Fanout.a_carries_the_message", envelope_a.get("Message"), "fanned out")
    check_eq("Fanout.b_carries_the_message", envelope_b.get("Message"), "fanned out")
    # ONE PUBLISH, ONE MessageId, both subscribers — the property that makes a
    # fan-out correlatable, and the observable half of "one publish is one
    # transaction".
    check_eq(
        "Fanout.same_publish_message_id_on_both",
        (envelope_a.get("MessageId"), envelope_b.get("MessageId")),
        (message_id, message_id),
    )
    check(
        "Fanout.sqs_message_ids_differ_per_delivery",
        a.get("MessageId") != b.get("MessageId"),
        "the two deliveries share a broker MessageId",
    )


# --------------------------------------------------------------------- fifo


@test
def t_fifo_topic_ordering(sns, sqs):
    # ContentBasedDeduplication so the ordering publishes need no dedup id of
    # their own; the dedup test below supplies one explicitly.
    _, topic = make_fifo_topic(sns, "fifo-order", {"ContentBasedDeduplication": "true"})
    _, qurl, qarn = make_fifo_queue(sqs, "fifo-order-q")
    arn = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)["SubscriptionArn"]
    check(
        "FifoTopic.subscription_arn_extends_the_fifo_topic_arn",
        arn.startswith(topic + ":"),
        f"got {arn!r}",
    )

    attributes = sns.get_topic_attributes(TopicArn=topic)["Attributes"]
    check_eq(
        "FifoTopic.attributes",
        (attributes.get("FifoTopic"), attributes.get("ContentBasedDeduplication")),
        ("true", "true"),
    )

    # A FIFO topic requires a group, and a standard queue cannot hold one.
    expect_sns_error(
        "FifoTopic.publish_without_a_group_is_refused",
        "InvalidParameter",
        lambda: sns.publish(TopicArn=topic, Message="no group"),
    )
    _, _, standard_arn = make_queue(sqs, "fifo-order-standard")
    expect_sns_error(
        "FifoTopic.standard_queue_cannot_subscribe",
        "InvalidParameter",
        lambda: sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=standard_arn),
    )

    published = [
        sns.publish(TopicArn=topic, Message=f"ordered-{i}", MessageGroupId="g-1")["MessageId"]
        for i in range(5)
    ]

    got = collect(sqs, qurl, 5, timeout=30.0)
    check_eq("FifoTopic.every_message_arrives", len(got), 5)
    check_eq(
        "FifoTopic.group_order_is_the_publish_order",
        [(notification(m) or {}).get("Message") for m in got],
        [f"ordered-{i}" for i in range(5)],
    )
    check_eq(
        "FifoTopic.publish_ids_arrive_in_order",
        [(notification(m) or {}).get("MessageId") for m in got],
        published,
    )


@test
def t_fifo_topic_deduplication(sns, sqs):
    _, topic = make_fifo_topic(sns, "fifo-dedup")
    _, qurl, qarn = make_fifo_queue(sqs, "fifo-dedup-q")
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)

    dedup = f"dedup-{RUN}"
    first = sns.publish(
        TopicArn=topic,
        Message="the original",
        MessageGroupId="g-dup",
        MessageDeduplicationId=dedup,
    )["MessageId"]
    # A REPEATED deduplication id is a SUCCESS, not an error: SQS answers one
    # that way and so must SNS. The body is deliberately different, so a
    # delivery of it would be unmistakable.
    try:
        second = sns.publish(
            TopicArn=topic,
            Message="the duplicate, which must not be delivered",
            MessageGroupId="g-dup",
            MessageDeduplicationId=dedup,
        )["MessageId"]
        ok("FifoTopic.repeated_dedup_id_is_a_success")
    except botocore.exceptions.ClientError as e:
        fail(
            "FifoTopic.repeated_dedup_id_is_a_success",
            f"{error_code(e)}: {error_message(e)}",
        )
        second = None

    marker = sns.publish(
        TopicArn=topic,
        Message="the marker",
        MessageGroupId="g-dup",
        MessageDeduplicationId=f"marker-{RUN}",
    )["MessageId"]

    got, extra = collect_exactly(sqs, qurl, 2, settle=4.0, timeout=30.0)
    check_eq(
        "FifoTopic.the_duplicate_is_not_delivered",
        [(notification(m) or {}).get("Message") for m in got],
        ["the original", "the marker"],
    )
    check_eq("FifoTopic.nothing_followed_the_marker", bodies(extra), [])
    check_eq(
        "FifoTopic.delivered_ids_are_the_first_and_the_marker",
        [(notification(m) or {}).get("MessageId") for m in got],
        [first, marker],
    )

    # AWS's SQS answers a repeated deduplication id with the ORIGINAL message's
    # id; `sns/publish.rs`'s own module header says this facade does the same.
    # It does not — a fresh uuid is minted before the broker refuses the write —
    # and whether SNS (as opposed to SQS) behaves that way is a question only a
    # run against real AWS settles. Recorded, not asserted. See M4_SMOKE.md.
    if second is not None:
        note(
            "FifoTopic dedup MessageId: first="
            f"{first} deduplicated-publish={second} "
            f"({'same' if second == first else 'DIFFERENT'})"
        )


# ------------------------------------------------- divergences, pinned live

@test
def t_recorded_divergences(sns, sqs):
    """The three places this facade knowingly answers something AWS does not.

    They are ASSERTED rather than left in the source's comments, because a
    divergence nobody measures is a divergence nobody notices changing. Each
    name says what it is; M4_SMOKE.md carries the sentence.
    """
    _, topic = make_topic(sns, "divergence-topic")
    _, _, fifo_arn = make_fifo_queue(sqs, "divergence-q")

    # DIVERGENCE, `deliberate` (sns/admin.rs, `subscribe`). AWS lets a STANDARD
    # topic deliver to a FIFO queue and picks a group id itself; this facade
    # refuses at Subscribe rather than inventing one per message, because a
    # group id it chose would put a FIFO consumer's ordering guarantee in this
    # facade's hands without saying so.
    expect_sns_error(
        "Divergence.standard_topic_to_a_fifo_queue_is_refused",
        "InvalidParameter",
        lambda: sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=fifo_arn),
    )

    # DIVERGENCE, `deliberate` (sns/admin.rs, `confirm_subscription`). Every
    # subscription this facade can create is same-account SQS, which is
    # confirmed AT Subscribe, so no token is ever minted and every token
    # presented is one this endpoint did not issue.
    expect_sns_error(
        "Divergence.confirm_subscription_has_nothing_to_confirm",
        "InvalidParameter",
        lambda: sns.confirm_subscription(TopicArn=topic, Token="0" * 64),
    )

    # FLAGGED for the differential lane (sns/admin.rs, `subscribe`): a repeat
    # Subscribe answers the existing ARN and does NOT apply the attributes the
    # second call carried. AWS documents the idempotency and is silent about the
    # differing case, so the behaviour is measured here rather than assumed.
    _, _, qarn = make_queue(sqs, "divergence-standard-q")
    first = json.dumps({"event": ["one"]})
    second = json.dumps({"event": ["two"]})
    arn = sns.subscribe(
        TopicArn=topic, Protocol="sqs", Endpoint=qarn, Attributes={"FilterPolicy": first}
    )["SubscriptionArn"]
    again = sns.subscribe(
        TopicArn=topic, Protocol="sqs", Endpoint=qarn, Attributes={"FilterPolicy": second}
    )["SubscriptionArn"]
    check_eq("Divergence.repeat_subscribe_answers_the_existing_arn", again, arn)
    stored = sns.get_subscription_attributes(SubscriptionArn=arn)["Attributes"]
    check_eq(
        "Divergence.repeat_subscribe_does_not_apply_the_new_attributes",
        json.loads(stored.get("FilterPolicy", "null")),
        json.loads(first),
    )


# ------------------------------------------------------------ delete cascade


@test
def t_delete_topic_cascades(sns, sqs):
    _, topic = make_topic(sns, "delete-topic")
    _, qurl, qarn = make_queue(sqs, "delete-queue")
    arn = sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)["SubscriptionArn"]

    # It really was live before the delete — otherwise the cascade proves
    # nothing about subscriptions and only something about topics.
    sns.publish(TopicArn=topic, Message="before the delete")
    before = one_message(sqs, qurl)
    check(
        "DeleteTopic.subscription_was_live_before_the_delete",
        before is not None and (notification(before) or {}).get("Message") == "before the delete",
        "the subscription delivered nothing before the topic was deleted",
    )

    sns.delete_topic(TopicArn=topic)
    if topic in TOPICS:
        TOPICS.remove(topic)

    expect_sns_error(
        "DeleteTopic.topic_is_gone",
        "NotFound",
        lambda: sns.get_topic_attributes(TopicArn=topic),
        status=404,
    )
    expect_sns_error(
        "DeleteTopic.listing_by_topic_is_gone",
        "NotFound",
        lambda: sns.list_subscriptions_by_topic(TopicArn=topic),
        status=404,
    )
    # THE CASCADE. A subscription that outlived its topic is a record no action
    # can reach and a delivery target nothing will ever write to.
    expect_sns_error(
        "DeleteTopic.subscription_is_gone",
        "NotFound",
        lambda: sns.get_subscription_attributes(SubscriptionArn=arn),
        status=404,
    )

    seen = set()
    token = None
    for _ in range(50):
        page = sns.list_subscriptions(**({"NextToken": token} if token else {}))
        seen.update(s.get("SubscriptionArn") for s in page.get("Subscriptions", []))
        token = page.get("NextToken")
        if not token:
            break
    check(
        "DeleteTopic.account_listing_no_longer_carries_the_subscription",
        arn not in seen,
        f"{arn} still listed among {len(seen)} subscriptions",
    )

    # AWS documents `DeleteTopic` idempotent in as many words.
    try:
        sns.delete_topic(TopicArn=topic)
        ok("DeleteTopic.is_idempotent")
    except botocore.exceptions.ClientError as e:
        fail("DeleteTopic.is_idempotent", f"{error_code(e)}: {error_message(e)}")

    # ...and the publish that a worker still holding the ARN will make.
    expect_sns_error(
        "Publish.to_a_deleted_topic_is_not_found",
        "NotFound",
        lambda: sns.publish(TopicArn=topic, Message="after the delete"),
        status=404,
    )
    expect_sns_error(
        "PublishBatch.to_a_deleted_topic_is_not_found",
        "NotFound",
        lambda: sns.publish_batch(
            TopicArn=topic,
            PublishBatchRequestEntries=[{"Id": "one", "Message": "after the delete"}],
        ),
        status=404,
    )
    expect_sns_error(
        "Subscribe.to_a_deleted_topic_is_not_found",
        "NotFound",
        lambda: sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn),
        status=404,
    )

    # Nothing was delivered by any of the refused calls.
    leftover = collect(sqs, qurl, 10, timeout=4.0)
    check_eq("DeleteTopic.no_delivery_survives_the_topic", bodies(leftover), [])


# ---------------------------------------------------------------------- main


def teardown(sns, sqs):
    for arn in list(TOPICS):
        try:
            sns.delete_topic(TopicArn=arn)
        except Exception:
            pass
    for url in list(QUEUES):
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass


def main():
    print(f"# endpoint {ENDPOINT}  region {REGION}  account {ACCOUNT}  run {RUN}", flush=True)
    sns, sqs = sns_client(), sqs_client()
    try:
        sns.list_topics()
        sqs.list_queues()
    except Exception as e:
        print(f"FAIL rig.reachable: {e}")
        print("RESULT: FAIL")
        return 1

    for fn in TESTS:
        try:
            fn(sns, sqs)
        except Exception:
            # One test blowing up must not cost the run every assertion after
            # it: the trace is printed, the test is a failure, and the rest go
            # on. `smoke_m0.py`'s rule.
            fail(fn.__name__, "unexpected exception")
            traceback.print_exc()

    teardown(sns, sqs)

    # The contract's protocol lines: what each client ACTUALLY put on the wire,
    # counted per request by `_protocol_recorder`, not inferred from a version.
    for (service, spoken), count in sorted(PROTOCOLS.items(), key=lambda kv: -kv[1]):
        print(
            f"# protocol spoken ({service}): {spoken} — {count} request(s)",
            flush=True,
        )
    print(f"# {PASSES} passed, {len(FAILURES)} failed", flush=True)
    for name in FAILURES:
        print(f"#   failed: {name}", flush=True)
    print(f"RESULT: {'FAIL' if FAILURES else 'PASS'}", flush=True)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
