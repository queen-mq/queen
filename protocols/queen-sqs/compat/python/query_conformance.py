#!/usr/bin/env python3
"""The Query/XML conformance corpus: no SDK, no AWS crate, no boto3.

WHY THIS FILE EXISTS AT ALL. `queen-sqs` speaks two wire protocols on one
listener, and every other live suite in `compat/` speaks only one of them:

    $ python -c "import boto3; \
        print(boto3.client('sqs', endpoint_url='http://x', region_name='r',
                           aws_access_key_id='a', aws_secret_access_key='b')
              .meta.service_model.protocol)"
    json

botocore's SQS model moved to AWS JSON 1.0 and dropped `query` from its
`protocols` list entirely (botocore 1.43.83, verified above), so boto3, the aws
CLI, Celery and every other Python client CANNOT be made to speak the Query
protocol any more, whatever transport option they are handed. The M0 run said
the same thing in its own words: "BOTH clients in this run speak AWS JSON 1.0.
The Query/XML codec is therefore not exercised by these two suites at all."

That leaves the whole of `src/proto/query.rs` (1292 lines), `src/proto/xml.rs`
and the Query half of the error catalog covered by unit tests and by nothing
live. This file closes that hole by being the client: it builds the form bodies
by hand, signs them with a SigV4 implementation written here, posts them with
`urllib`, and parses the XML that comes back with `xml.etree`. Nothing is
imported that could be doing the work for us and hiding a divergence — which is
the whole point, because a hand-rolled client is exactly what async-aws, the
older SDK majors, Terraform's SQS provider on its old path, and every curl
script in an operator's runbook are, from the facade's side of the socket.

THE CONTRACT (`protocols/queen-kafka/compat/CLIENT_MATRIX.md`, and `smoke_m0.py` before this):

  * the stack comes from the environment, never from a hardcoded address;
  * ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
  * `RESULT: PASS` or `RESULT: FAIL` as the last line;
  * a nonzero exit status when anything failed;
  * and the run REPORTS WHICH PROTOCOL THE CLIENT ACTUALLY SPOKE, read from the
    client's own record of what it put on the wire. Here that record is
    [`WIRE`], written by `call` itself: the request's Content-Type, whether an
    `X-Amz-Target` was sent (never, in this file — that header is what would
    switch the facade into its JSON codec), the response's Content-Type, and the
    root element of every document parsed. A run whose summary said anything but
    "Query/XML" would mean this suite had silently stopped testing the thing it
    exists to test.

  $ protocols/queen-sqs/compat/rig.sh up
  $ source protocols/queen-sqs/compat/.rig/env.sh
  $ python protocols/queen-sqs/compat/python/query_conformance.py

THE SIGNER IS OURS. `sigv4.rs` reconstructs the canonical request a client
built; this file builds one. The two were written from AWS's specification
independently and neither has seen the other's code, which is the only way a
verifier and a signer agreeing means anything. The rules that decide it:

  * the canonical URI is the path DOUBLE URI-ENCODED (`uri_encode` over a string
    that is already a URL path). S3's single-encoding quirk does not apply here;
  * the payload hash is the lowercase hex SHA-256 of the exact body bytes;
  * the canonical headers are the signed list, lowercased, sorted, values
    trimmed and inner whitespace collapsed, `\n` after each;
  * the string to sign carries the SCOPE date, which must agree with the first
    eight characters of `X-Amz-Date` (`check_scope` in `sigv4.rs` refuses the
    pair when it does not, so [`t_sigv4_refusals`] is careful to move both);
  * the signing key is HMAC-chained over date, region, service, `aws4_request`.

MD5s ARE OURS TOO, for the reason `smoke_m0.py` records: botocore does not check
them and never did, so a facade that answered a constant would sail past any
suite that leaned on an SDK. `md5_of_body` and `md5_of_attributes` below are
AWS's two algorithms, and the attribute one carries its own binary encoding
(length-prefixed name, length-prefixed type, one transport byte, length-prefixed
value) which is where a facade gets it wrong if it is going to.

NAMES ARE UNIQUE PER RUN. `DeleteQueue` arms a 60-second `QueueDeletedRecently`
tombstone exactly as AWS does, so fixed names could not be run twice in a
minute.
"""

import base64
import hashlib
import hmac
import os
import string
import struct
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------- stack

ENDPOINT = os.environ.get("QUEEN_SQS_ENDPOINT", "http://127.0.0.1:19324").rstrip("/")
REGION = os.environ.get("QUEEN_SQS_REGION", "queen-1")
ACCOUNT = os.environ.get("QUEEN_SQS_ACCOUNT", "000000000000")
AKID = os.environ.get("AWS_ACCESS_KEY_ID", "QSQSTEST")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "qsqssecret")

#: The SQS Query API version. Every request in this file names it, except the one
#: assertion that proves it may be omitted.
VERSION_SQS = "2012-11-05"
#: The namespace every SQS answer must carry, success or error.
NS_SQS = "http://queue.amazonaws.com/doc/2012-11-05/"
#: The service name the credential scope must carry (`sigv4.rs::SERVICE_SQS`).
SERVICE = "sqs"
ALGORITHM = "AWS4-HMAC-SHA256"

RUN = uuid.uuid4().hex[:8]
#: Queues this run made, torn down at the end whatever happened.
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


# ------------------------------------------------------- URI and form encoding

#: RFC 3986's unreserved set, which is the only set SigV4 leaves alone.
UNRESERVED = frozenset(string.ascii_letters + string.digits + "-._~")


def uri_encode(text, encode_slash=True):
    """Percent-encode `text` the way SigV4 does: unreserved kept, everything else
    `%XX` in UPPERCASE hex, byte by byte over UTF-8.

    `encode_slash=False` is the canonical-URI form, where `/` is a separator.
    Applying this to a string that is ALREADY a URL path is the double encoding
    the specification asks for, and it is the single rule most home-made signers
    get wrong: a queue named `my queue` reaches the wire as `/…/my%20queue` and
    must be signed as `/…/my%2520queue`.
    """
    out = []
    for byte in text.encode("utf-8"):
        c = chr(byte)
        if c in UNRESERVED:
            out.append(c)
        elif c == "/" and not encode_slash:
            out.append(c)
        else:
            out.append("%%%02X" % byte)
    return "".join(out)


def form_encode(pairs):
    """A `application/x-www-form-urlencoded` body, AWS-style.

    `urllib.parse.urlencode` would spell a space `+`; every AWS SDK spells it
    `%20`, and so does [`uri_encode`]. Both decode to a space on the facade's
    side (`form_urlencoded::parse` accepts either), but the BYTES are what the
    signature covers, so the encoder that builds the body and the encoder the
    signature was computed over have to be the same one — which is why the body
    is built here rather than by the standard library.
    """
    return "&".join(f"{uri_encode(key)}={uri_encode(value)}" for key, value in pairs)


# ------------------------------------------------------------------- SigV4

def _hmac(key, message):
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret, datestamp, region, service):
    """`HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`."""
    k_date = _hmac(f"AWS4{secret}".encode("utf-8"), datestamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    return _hmac(k_service, "aws4_request")


def sign(method, url, headers, body, akid, secret, region=REGION, service=SERVICE,
         when=None, scope_date=None):
    """Return `headers` with `Host`, `X-Amz-Date` and `Authorization` on it.

    `when` moves the request's clock and `scope_date` moves ONLY the credential
    scope's date, which are two different refusals in `sigv4.rs`: a skewed clock
    trips `check_skew`, a scope that disagrees with the timestamp trips
    `check_scope`, and a suite that could not separate them would not know which
    check it had exercised.
    """
    when = when or datetime.now(timezone.utc)
    amz_date = when.strftime("%Y%m%dT%H%M%SZ")
    datestamp = scope_date or when.strftime("%Y%m%d")

    parts = urllib.parse.urlsplit(url)
    headers = dict(headers)
    headers["Host"] = parts.netloc
    headers["X-Amz-Date"] = amz_date

    # The signed set is every header we control. `host` and `x-amz-date` are the
    # two SigV4 requires; `content-type` is signed because every SDK signs it and
    # a verifier that mishandled a signed-but-unlisted header would be found here
    # and nowhere else.
    signed = sorted(name.lower() for name in headers)
    lookup = {name.lower(): value for name, value in headers.items()}
    canonical_headers = "".join(
        # Trimmed, with inner runs of whitespace collapsed to one space — the
        # rule `sigv4.rs::header_value` applies on the other side.
        f"{name}:{' '.join(str(lookup[name]).split())}\n"
        for name in signed
    )

    canonical_request = "\n".join([
        method.upper(),
        uri_encode(parts.path or "/", encode_slash=False),
        canonical_query(parts.query),
        canonical_headers,
        ";".join(signed),
        hashlib.sha256(body).hexdigest(),
    ])
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        ALGORITHM,
        amz_date,
        scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        signing_key(secret, datestamp, region, service),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers["Authorization"] = (
        f"{ALGORITHM} Credential={akid}/{scope}, "
        f"SignedHeaders={';'.join(signed)}, Signature={signature}"
    )
    return headers


def canonical_query(query):
    """The canonical query string: pairs as they arrived, sorted by byte.

    Every request in this file posts its parameters in the BODY, so this is
    always the empty string in practice. It is here because the canonical
    request has a line for it and a signer that omitted the line would produce a
    signature that verifies against nothing.
    """
    if not query:
        return ""
    pairs = []
    for pair in query.split("&"):
        name, _, value = pair.partition("=")
        pairs.append((name, value))
    pairs.sort()
    return "&".join(f"{name}={value}" for name, value in pairs)


# ------------------------------------------------------------ the wire record

#: What this client actually put on the wire and got back, counted per shape.
#: The suite contract's protocol line is printed from this and from nothing else.
WIRE = {
    "request_content_types": {},
    "response_content_types": {},
    "root_elements": {},
    "x_amz_target_sent": 0,
    "requests": 0,
    "request_ids": [],
    "responses_without_request_id": [],
}


class Answer:
    """One HTTP answer, with the XML already parsed when there is any."""

    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body
        self.root = None
        self.parse_error = None
        if body.strip():
            try:
                self.root = ElementTree.fromstring(body)
            except ElementTree.ParseError as e:
                self.parse_error = e

    def header(self, name):
        """Case-insensitive header lookup — HTTP header names are not case
        sensitive and the facade writes `x-amzn-requestid` in lower case."""
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None

    @property
    def request_id(self):
        return self.header("x-amzn-RequestId")

    @property
    def namespace(self):
        if self.root is None or not self.root.tag.startswith("{"):
            return None
        return self.root.tag[1:].split("}", 1)[0]

    @property
    def root_tag(self):
        return None if self.root is None else local(self.root.tag)

    def result(self, action):
        """The `<{action}Result>` element, or `None` when the action has no
        output shape (`DeleteMessage`, `SetQueueAttributes`) or the answer was an
        error."""
        if self.root is None:
            return None
        return one(self.root, f"{action}Result")

    def error(self):
        """The `<Error>` element of an `<ErrorResponse>`, or `None`."""
        if self.root is None or self.root_tag != "ErrorResponse":
            return None
        return one(self.root, "Error")

    def error_code(self):
        error = self.error()
        return None if error is None else text_of(error, "Code")

    def __repr__(self):
        return f"<Answer {self.status} {self.root_tag} {self.body[:200]!r}>"


def call(action, params=(), *, url=None, akid=None, secret=None, version=VERSION_SQS,
         when=None, scope_date=None, region=REGION, service=SERVICE, authorization=True,
         timeout=35.0, method="POST", extra_headers=None):
    """One Query request: form body in, XML out.

    `params` is a list of `(name, value)` PAIRS and not a dict, because the whole
    subject of this file is a flattening in which the same name legitimately
    repeats (`AttributeName.1`, `AttributeName.2`) and in which ORDER is a thing
    a client chooses and the facade must not depend on.
    """
    url = url or ENDPOINT + "/"
    pairs = []
    if action is not None:
        pairs.append(("Action", action))
    if version is not None:
        pairs.append(("Version", version))
    pairs.extend(params)
    body = form_encode(pairs).encode("utf-8")

    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"}
    if extra_headers:
        headers.update(extra_headers)
    if authorization:
        headers = sign(
            method, url, headers, body,
            akid if akid is not None else AKID,
            secret if secret is not None else SECRET,
            region=region, service=service, when=when, scope_date=scope_date,
        )
    else:
        # Unsigned: still needs a Host to reach the listener, and the absence of
        # `Authorization` is the whole point of the request.
        headers = dict(headers)
        headers["Host"] = urllib.parse.urlsplit(url).netloc

    WIRE["requests"] += 1
    request_content_type = headers.get("Content-Type", "")
    WIRE["request_content_types"][request_content_type] = (
        WIRE["request_content_types"].get(request_content_type, 0) + 1
    )
    if any(name.lower() == "x-amz-target" for name in headers):
        # Never on this lane: that header is what puts the facade into its JSON
        # codec, and a request carrying one would not be testing this one.
        WIRE["x_amz_target_sent"] += 1

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            answer = Answer(response.status, dict(response.headers),
                            response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        # A 4xx/5xx is an ANSWER here, not an exception: half of this corpus is
        # about the shape of the error document.
        answer = Answer(e.code, dict(e.headers), e.read().decode("utf-8", "replace"))

    response_content_type = answer.header("content-type") or "(none)"
    WIRE["response_content_types"][response_content_type] = (
        WIRE["response_content_types"].get(response_content_type, 0) + 1
    )
    root = answer.root_tag or "(unparsed)"
    WIRE["root_elements"][root] = WIRE["root_elements"].get(root, 0) + 1
    if answer.request_id:
        WIRE["request_ids"].append(answer.request_id)
    else:
        WIRE["responses_without_request_id"].append(f"{action} -> {answer.status}")
    return answer


# -------------------------------------------------------------- XML helpers

def local(tag):
    """An element's name without its namespace."""
    return tag.split("}")[-1] if "}" in tag else tag


def all_of(element, name):
    """Every DIRECT child called `name`. Direct and not descendant: the SQS
    flattening puts `<Message>` under `<ReceiveMessageResult>` and `<Attribute>`
    under `<Message>`, and a descendant search would confuse the two."""
    if element is None:
        return []
    return [child for child in element if local(child.tag) == name]


def one(element, name):
    children = all_of(element, name)
    return children[0] if children else None


def text_of(element, name, default=None):
    child = one(element, name)
    return default if child is None or child.text is None else child.text


def pairs_of(element, entry, key="Name", value="Value"):
    """A SQS map, read back: `<Attribute><Name/><Value/></Attribute>` repeated.

    The ENTRY ELEMENT NAME is the parameter because it differs per member
    (`Attribute`, `MessageAttribute`, `Tag`) and the difference is exactly what
    `query.rs::map_member` decides — a facade that wrote `<entry><key/>` here
    would be writing SNS's shape into an SQS document.
    """
    out = {}
    for item in all_of(element, entry):
        name = text_of(item, key)
        if name is not None:
            out[name] = text_of(item, value, "")
    return out


# --------------------------------------------------------- AWS's own MD5s

def md5_of_body(body):
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def md5_of_attributes(attributes):
    """AWS's `MD5OfMessageAttributes` / `MD5OfMessageSystemAttributes`.

    `attributes` maps a name to `(data_type, "String"|"Binary", value)` where a
    String value is text and a Binary value is bytes. Names in ascending BYTE
    order, and for each: 4-byte big-endian length + UTF-8 name, the same for the
    data type string, one transport byte (1 String, 2 Binary), then 4-byte
    big-endian length + the value's bytes.
    """
    if not attributes:
        return None
    buf = bytearray()

    def field(raw):
        buf.extend(struct.pack(">I", len(raw)))
        buf.extend(raw)

    for name in sorted(attributes, key=lambda n: n.encode("utf-8")):
        data_type, transport, value = attributes[name]
        field(name.encode("utf-8"))
        field(data_type.encode("utf-8"))
        if transport == "String":
            buf.append(1)
            field(value.encode("utf-8"))
        else:
            buf.append(2)
            field(value)
    return hashlib.md5(bytes(buf)).hexdigest()


# ------------------------------------------------------------------ helpers

def queue_url_of(name):
    return f"{ENDPOINT}/{ACCOUNT}/{name}"


def make_queue(label, attributes=(), tags=()):
    """A queue named for this run, created through the Query protocol, and
    remembered so the teardown can remove it."""
    name = f"qc-{label}-{RUN}"
    params = [("QueueName", name)]
    for index, (key, value) in enumerate(attributes, start=1):
        params.append((f"Attribute.{index}.Name", key))
        params.append((f"Attribute.{index}.Value", value))
    for index, (key, value) in enumerate(tags, start=1):
        params.append((f"Tag.{index}.Key", key))
        params.append((f"Tag.{index}.Value", value))
    answer = call("CreateQueue", params)
    if answer.status != 200:
        raise RuntimeError(f"CreateQueue {name} answered {answer.status}: {answer.body}")
    url = text_of(answer.result("CreateQueue"), "QueueUrl")
    CREATED.append(url)
    return name, url, answer


def receive_one(url, params=(), *, wait=1, timeout=25.0):
    """ONE message, whatever it takes up to `timeout`, or `None`.

    A single receive legitimately comes back empty on a queue that has something
    in it: a receive in `exact` mode is N parallel pops of one message each, and
    a lane whose message is in flight serves no second pop (M0_SMOKE.md, D2). So
    every read in this file loops, and there is deliberately NO helper here that
    collects several messages without deleting them — that is what
    [`collect_deleting`] and [`hold`] are for, and which of the two a test wants
    is a decision it has to make on purpose.

    `params` carries the selectors under test (`AttributeName.N`,
    `MessageAttributeName.N`), which is why this is not [`poll_once`].
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        answer = call("ReceiveMessage", [
            ("QueueUrl", url),
            ("MaxNumberOfMessages", "1"),
            ("WaitTimeSeconds", str(wait)),
            *params,
        ], timeout=35.0 + wait)
        if answer.status != 200:
            raise RuntimeError(f"ReceiveMessage answered {answer.status}: {answer.body}")
        messages = all_of(answer.result("ReceiveMessage"), "Message")
        if messages:
            return messages[0]
    return None


def poll_once(url, maximum=10, wait=1):
    """One `ReceiveMessage`, no looping. The primitive the loops are built from."""
    answer = call("ReceiveMessage", [
        ("QueueUrl", url),
        ("MaxNumberOfMessages", str(maximum)),
        ("WaitTimeSeconds", str(wait)),
    ], timeout=35.0 + wait)
    if answer.status != 200:
        raise RuntimeError(f"ReceiveMessage answered {answer.status}: {answer.body}")
    return all_of(answer.result("ReceiveMessage"), "Message")


def delete_message(url, handle):
    return call("DeleteMessage", [("QueueUrl", url), ("ReceiptHandle", handle)])


def collect_deleting(url, count, timeout=60.0):
    """What a real consumer does: receive, DELETE, go round again.

    The only shape that can read a queue holding more messages than it has FREE
    LANES. A queue is M synthesized partitions and a receive is N parallel pops
    of one message each, so a lane with a message in flight hands out nothing
    else until that message is gone (M0_SMOKE.md, D2) — and two messages whose
    ids hashed into one lane can never be in flight together. Collecting without
    deleting therefore stalls, and then, when the first message's visibility
    lapses, returns it a second time; a suite that counted distinct bodies would
    read that as a lost message. This deletes as it goes, so every lane frees.
    """
    got = []
    deadline = time.monotonic() + timeout
    while len(got) < count and time.monotonic() < deadline:
        batch = poll_once(url, maximum=10, wait=1)
        for message in batch:
            delete_message(url, text_of(message, "ReceiptHandle"))
        got.extend(batch)
    return got


def hold(url, count, timeout=60.0):
    """`count` messages received and NOT deleted, all in flight at once.

    Receiving `count` messages does not get there on its own, for D2's reason
    above: a queue with two messages in one lane can hand out one and then
    nothing. When a poll comes back empty and the hold is still short, this SENDS
    another message — a fresh MessageId is a fresh lane — rather than waiting on
    a lane that will not open. It leaves stragglers behind, so a caller that
    needs the queue empty afterwards has to drain it.
    """
    held = []
    deadline = time.monotonic() + timeout
    while len(held) < count and time.monotonic() < deadline:
        batch = poll_once(url, maximum=10, wait=1)
        held.extend(batch)
        if not batch and len(held) < count:
            call("SendMessage", [
                ("QueueUrl", url), ("MessageBody", f"filler-{uuid.uuid4().hex}"),
            ])
    return held[:count]


def drain(url, timeout=40.0):
    """Receive and delete until nothing comes back for two consecutive polls."""
    deadline = time.monotonic() + timeout
    empty = 0
    while empty < 2 and time.monotonic() < deadline:
        messages = poll_once(url, maximum=10, wait=1)
        if not messages:
            empty += 1
            continue
        empty = 0
        for message in messages:
            delete_message(url, text_of(message, "ReceiptHandle"))


def depth(url, names=("ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible")):
    params = [("QueueUrl", url)]
    for index, name in enumerate(names, start=1):
        params.append((f"AttributeName.{index}", name))
    answer = call("GetQueueAttributes", params)
    attributes = pairs_of(answer.result("GetQueueAttributes"), "Attribute")
    return {name: int(attributes.get(name, "-1")) for name in names}


def wait_for_depth_zero(url, timeout=45.0):
    deadline = time.monotonic() + timeout
    seen = {}
    while time.monotonic() < deadline:
        seen = depth(url)
        if all(value == 0 for value in seen.values()):
            return True, seen
        time.sleep(1.0)
    return False, seen


# SQS spells every error twice and the two spellings are different words. This
# lane sees the QUERY spelling — the legacy `AWS.SimpleQueueService.` codes — and
# a JSON spelling appearing in an XML document is the failure this table exists
# to name. Left: `ErrorKind`'s JSON type. Right: what `<Code>` must say.
QUERY_CODES = {
    "QueueDoesNotExist": "AWS.SimpleQueueService.NonExistentQueue",
    "QueueNameExists": "QueueAlreadyExists",
    "QueueDeletedRecently": "AWS.SimpleQueueService.QueueDeletedRecently",
    "BatchEntryIdsNotDistinct": "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    "EmptyBatchRequest": "AWS.SimpleQueueService.EmptyBatchRequest",
    "TooManyEntriesInBatchRequest": "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    "MessageNotInflight": "AWS.SimpleQueueService.MessageNotInflight",
    "PurgeQueueInProgress": "AWS.SimpleQueueService.PurgeQueueInProgress",
    # The codes SQS spells the same way in both protocols.
    "ReceiptHandleIsInvalid": "ReceiptHandleIsInvalid",
    "InvalidAttributeName": "InvalidAttributeName",
    "InvalidAction": "InvalidAction",
    "MissingParameter": "MissingParameter",
    "InvalidParameterValue": "InvalidParameterValue",
    "SignatureDoesNotMatch": "SignatureDoesNotMatch",
    "MissingAuthenticationToken": "MissingAuthenticationToken",
    "InvalidClientTokenId": "InvalidClientTokenId",
}


def expect_error(name, json_spelling, answer, status=None):
    """Assert `answer` is an `<ErrorResponse>` carrying `json_spelling`'s QUERY
    code — and that the JSON spelling itself is nowhere in the document."""
    want = QUERY_CODES[json_spelling]
    if answer.root_tag != "ErrorResponse":
        fail(name, f"root element is {answer.root_tag!r}, want ErrorResponse ({answer.body[:300]!r})")
        return False
    got = answer.error_code()
    if got != want:
        fail(name, f"Code is {got!r}, want {want!r}")
        return False
    if status is not None and answer.status != status:
        fail(name, f"status is {answer.status}, want {status}")
        return False
    if want != json_spelling and f">{json_spelling}<" in answer.body:
        fail(name, f"the JSON spelling {json_spelling!r} leaked into the XML")
        return False
    ok(name)
    return True


# =========================================================================
#                                   tests
# =========================================================================


@test
def t_create_queue_and_the_answer_envelope():
    """CreateQueue through the Query codec, and the envelope every answer wears.

    The envelope is three claims at once and each one is a place a codec can be
    wrong on its own: the ROOT is `<{Action}Response>`, the namespace is SQS's
    (and not SNS's, which shares this listener), and the result lives one level
    down in `<{Action}Result>` beside a `<ResponseMetadata><RequestId>` that
    repeats the header.
    """
    name, url, answer = make_queue("crud", attributes=[
        ("VisibilityTimeout", "30"),
        ("MessageRetentionPeriod", "3600"),
    ])

    check_eq("CreateQueue.status", answer.status, 200)
    check_eq("CreateQueue.content_type_is_xml",
             (answer.header("content-type") or "").split(";")[0].strip(), "text/xml")
    check_eq("CreateQueue.root_element", answer.root_tag, "CreateQueueResponse")
    check_eq("CreateQueue.namespace", answer.namespace, NS_SQS)
    check("CreateQueue.result_element_exists", answer.result("CreateQueue") is not None,
          f"no <CreateQueueResult> in {answer.body[:300]!r}")
    check_eq("CreateQueue.queue_url", url, queue_url_of(name))

    # The two halves of the request id: the header every SDK logs, and the
    # element inside the document. AWS writes both and they are the same string.
    header_id = answer.request_id
    check("CreateQueue.request_id_header", bool(header_id), "no x-amzn-RequestId header")
    metadata = one(answer.root, "ResponseMetadata")
    check_eq("CreateQueue.request_id_element", text_of(metadata, "RequestId"), header_id)

    # ...and the attributes really landed, read back through the same codec.
    attributes = pairs_of(
        call("GetQueueAttributes", [
            ("QueueUrl", url),
            ("AttributeName.1", "VisibilityTimeout"),
            ("AttributeName.2", "MessageRetentionPeriod"),
        ]).result("GetQueueAttributes"),
        "Attribute",
    )
    check_eq("CreateQueue.attributes_were_applied",
             (attributes.get("VisibilityTimeout"), attributes.get("MessageRetentionPeriod")),
             ("30", "3600"))


@test
def t_create_queue_is_idempotent():
    """D1, live and through the Query codec.

    The highest-traffic call shape in the whole M0 surface: the create every
    framework performs at worker start-up against a queue somebody else made
    with non-default attributes. AWS returns the URL unless an attribute the
    REQUEST NAMES differs from the queue's current value, so a repeat that names
    a SUBSET wins and a repeat that names a CONFLICT is refused.
    """
    name, url, _ = make_queue("idem", attributes=[
        ("VisibilityTimeout", "42"),
        ("MessageRetentionPeriod", "1200"),
    ])

    bare = call("CreateQueue", [("QueueName", name)])
    check_eq("Idempotent.bare_repeat_status", bare.status, 200)
    check_eq("Idempotent.bare_repeat_url", text_of(bare.result("CreateQueue"), "QueueUrl"), url)

    subset = call("CreateQueue", [
        ("QueueName", name),
        ("Attribute.1.Name", "VisibilityTimeout"),
        ("Attribute.1.Value", "42"),
    ])
    check_eq("Idempotent.subset_repeat_url",
             text_of(subset.result("CreateQueue"), "QueueUrl"), url)

    # The other half of AWS's contract, and the reason the first half is not just
    # "always answer 200": a value that CONTRADICTS the queue is refused, and the
    # Query spelling of that refusal is `QueueAlreadyExists` while the JSON one
    # is `QueueNameExists`. The pair is inverted and this is where it shows.
    conflict = call("CreateQueue", [
        ("QueueName", name),
        ("Attribute.1.Name", "VisibilityTimeout"),
        ("Attribute.1.Value", "99"),
    ])
    expect_error("Idempotent.conflicting_attribute_is_refused", "QueueNameExists", conflict, status=400)


@test
def t_queue_url_and_listing():
    """GetQueueUrl, and the SQS list flattening.

    `<QueueUrl>` REPEATS directly under `<ListQueuesResult>`; the plural member
    name `QueueUrls` is never written at all. That is `query.rs::list_element`,
    and it is the difference between an SQS document and an SNS one — SNS wraps
    every list in `<member>` elements, and a codec that got the two the wrong way
    round would break every XML client of both services.
    """
    name, url, _ = make_queue("list")

    got = call("GetQueueUrl", [("QueueName", name)])
    check_eq("GetQueueUrl.returns_the_created_url",
             text_of(got.result("GetQueueUrl"), "QueueUrl"), url)

    listing = call("ListQueues", [("QueueNamePrefix", f"qc-list-{RUN}")])
    result = listing.result("ListQueues")
    urls = [element.text for element in all_of(result, "QueueUrl")]
    check("ListQueues.flattens_QueueUrl_elements", url in urls,
          f"{url} not among {urls}")
    check("ListQueues.never_writes_the_plural_member",
          not all_of(result, "QueueUrls") and "<QueueUrls>" not in listing.body,
          "a <QueueUrls> wrapper was written")

    # An empty listing writes no list at all, which is what an SDK's paginator
    # reads as the end. The element itself is still there, because ListQueues HAS
    # an output shape.
    empty = call("ListQueues", [("QueueNamePrefix", f"qc-nothing-{RUN}")])
    check("ListQueues.empty_result_element_exists",
          empty.result("ListQueues") is not None, empty.body[:200])
    check("ListQueues.empty_listing_has_no_QueueUrl",
          not all_of(empty.result("ListQueues"), "QueueUrl"), empty.body[:200])


@test
def t_send_message_with_flattened_attributes():
    """SendMessage with `MessageAttribute.N.*`, which is the flattening.

    Three attributes covering all three transport shapes AWS defines — a String,
    a Number (a String on the wire, a different `DataType`), and a Binary (base64
    on the wire, RAW BYTES in the digest) — plus a custom type suffix, plus the
    one system attribute SQS defines. Every MD5 is computed HERE, because no
    Python client checks them and a constant would otherwise pass.
    """
    name, url, _ = make_queue("send")
    body = "Query protocol & <XML> \"quoted\" 'apostrophed' — héllo 🐝"
    binary = b"\x00\x01\x02\xff raw bytes"

    answer = call("SendMessage", [
        ("QueueUrl", url),
        ("MessageBody", body),
        ("MessageAttribute.1.Name", "trace"),
        ("MessageAttribute.1.Value.DataType", "String"),
        ("MessageAttribute.1.Value.StringValue", "root=1-2-3"),
        ("MessageAttribute.2.Name", "attempt"),
        ("MessageAttribute.2.Value.DataType", "Number"),
        ("MessageAttribute.2.Value.StringValue", "42"),
        ("MessageAttribute.3.Name", "blob"),
        ("MessageAttribute.3.Value.DataType", "Binary.gzip"),
        ("MessageAttribute.3.Value.BinaryValue", base64.b64encode(binary).decode("ascii")),
        ("MessageSystemAttribute.1.Name", "AWSTraceHeader"),
        ("MessageSystemAttribute.1.Value.DataType", "String"),
        ("MessageSystemAttribute.1.Value.StringValue", "Root=1-5759e988-bd862e3fe1be46a994272793"),
    ])
    check_eq("SendMessage.status", answer.status, 200)
    result = answer.result("SendMessage")
    check("SendMessage.result_element_exists", result is not None, answer.body[:300])

    message_id = text_of(result, "MessageId")
    check("SendMessage.message_id_is_a_uuid", _is_uuid(message_id), f"got {message_id!r}")

    check_eq("SendMessage.md5_of_body", text_of(result, "MD5OfMessageBody"), md5_of_body(body))
    check_eq(
        "SendMessage.md5_of_message_attributes",
        text_of(result, "MD5OfMessageAttributes"),
        md5_of_attributes({
            "trace": ("String", "String", "root=1-2-3"),
            "attempt": ("Number", "String", "42"),
            "blob": ("Binary.gzip", "Binary", binary),
        }),
    )
    check_eq(
        "SendMessage.md5_of_system_attributes",
        text_of(result, "MD5OfMessageSystemAttributes"),
        md5_of_attributes({
            "AWSTraceHeader": ("String", "String",
                               "Root=1-5759e988-bd862e3fe1be46a994272793"),
        }),
    )

    # ...and it comes back. The body carries every character XML has to escape,
    # so a receive that returns it intact is also a proof about `xml.rs::escape`.
    message = receive_one(url, [("MessageAttributeName.1", "All")], wait=2, timeout=30.0)
    if not check("SendMessage.round_trip", message is not None, "nothing was received"):
        return
    check_eq("SendMessage.body_round_trips", text_of(message, "Body"), body)
    check_eq("SendMessage.md5_of_body_on_receive", text_of(message, "MD5OfBody"), md5_of_body(body))
    check_eq("SendMessage.message_id_round_trips", text_of(message, "MessageId"), message_id)
    check("SendMessage.receipt_handle_present", bool(text_of(message, "ReceiptHandle")),
          "no ReceiptHandle")

    attributes = {}
    for item in all_of(message, "MessageAttribute"):
        value = one(item, "Value")
        attributes[text_of(item, "Name")] = (
            text_of(value, "DataType"),
            text_of(value, "StringValue"),
            text_of(value, "BinaryValue"),
        )
    check_eq("SendMessage.string_attribute_round_trips",
             attributes.get("trace"), ("String", "root=1-2-3", None))
    check_eq("SendMessage.number_attribute_keeps_its_type",
             attributes.get("attempt"), ("Number", "42", None))
    check_eq("SendMessage.custom_binary_type_survives",
             (attributes.get("blob") or (None, None, None))[0], "Binary.gzip")
    check_eq(
        "SendMessage.binary_attribute_round_trips",
        base64.b64decode((attributes.get("blob") or (None, None, ""))[2] or ""),
        binary,
    )
    delete_message(url, text_of(message, "ReceiptHandle"))


@test
def t_send_message_batch():
    """SendMessageBatch: the deepest legal shape the Query protocol has.

    `SendMessageBatchRequestEntry.N.MessageAttribute.M.Value.StringValue` is five
    levels of flattening in one key, and the entry list is written SPARSE AND OUT
    OF ORDER on purpose. A reindexing that moved an entry would be invisible in
    the happy path and catastrophic in the failure path, because
    `BatchResultErrorEntry` reports failures by the CLIENT's own `Id`.
    """
    name, url, _ = make_queue("batch")

    answer = call("SendMessageBatch", [
        ("QueueUrl", url),
        # Index 7 first, then 2, then 5: legal, sparse, unordered.
        ("SendMessageBatchRequestEntry.7.Id", "seven"),
        ("SendMessageBatchRequestEntry.7.MessageBody", "body-seven"),
        ("SendMessageBatchRequestEntry.2.Id", "two"),
        ("SendMessageBatchRequestEntry.2.MessageBody", "body-two"),
        ("SendMessageBatchRequestEntry.2.MessageAttribute.1.Name", "lane"),
        ("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.DataType", "String"),
        ("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.StringValue", "second"),
        # ...and one entry written with the `.member.` segment some clients emit.
        ("SendMessageBatchRequestEntry.member.5.Id", "five"),
        ("SendMessageBatchRequestEntry.member.5.MessageBody", "body-five"),
    ])
    check_eq("SendMessageBatch.status", answer.status, 200)
    result = answer.result("SendMessageBatch")

    entries = all_of(result, "SendMessageBatchResultEntry")
    check_eq("SendMessageBatch.entries_are_named_after_the_action", len(entries), 3)
    check("SendMessageBatch.no_generic_Successful_wrapper",
          not all_of(result, "Successful") and "<Successful>" not in answer.body,
          "a <Successful> wrapper was written")
    check("SendMessageBatch.no_Failed_element_when_nothing_failed",
          not all_of(result, "Failed") and "BatchResultErrorEntry" not in answer.body,
          answer.body[:400])

    by_id = {text_of(entry, "Id"): entry for entry in entries}
    check_eq("SendMessageBatch.every_id_came_back", sorted(by_id), ["five", "seven", "two"])
    for entry_id, body in (("two", "body-two"), ("five", "body-five"), ("seven", "body-seven")):
        entry = by_id.get(entry_id)
        check_eq(f"SendMessageBatch.md5_for_{entry_id}",
                 None if entry is None else text_of(entry, "MD5OfMessageBody"),
                 md5_of_body(body))
    check_eq(
        "SendMessageBatch.md5_of_attributes_on_the_nested_entry",
        None if "two" not in by_id else text_of(by_id["two"], "MD5OfMessageAttributes"),
        md5_of_attributes({"lane": ("String", "String", "second")}),
    )
    check("SendMessageBatch.member_segment_entry_has_a_message_id",
          _is_uuid(text_of(by_id.get("five"), "MessageId") if "five" in by_id else None),
          "the .member. entry got no MessageId")

    # The three bodies really are on the queue, and the sparse indices did not
    # cross their bodies over. Read with `collect_deleting`, because three
    # messages over eight lanes collide often enough that a read which held them
    # all at once would be a coin flip (D2).
    bodies = {text_of(message, "Body")
              for message in collect_deleting(url, 3, timeout=45.0)}
    check_eq("SendMessageBatch.the_three_bodies_are_on_the_queue",
             sorted(bodies), ["body-five", "body-seven", "body-two"])


@test
def t_batch_refusals():
    """The whole-batch refusals, each in its Query spelling."""
    name, url, _ = make_queue("badbatch")

    duplicate = call("SendMessageBatch", [
        ("QueueUrl", url),
        ("SendMessageBatchRequestEntry.1.Id", "same"),
        ("SendMessageBatchRequestEntry.1.MessageBody", "a"),
        ("SendMessageBatchRequestEntry.2.Id", "same"),
        ("SendMessageBatchRequestEntry.2.MessageBody", "b"),
    ])
    expect_error("Batch.duplicate_ids", "BatchEntryIdsNotDistinct", duplicate, status=400)

    empty = call("SendMessageBatch", [("QueueUrl", url)])
    expect_error("Batch.empty_request", "EmptyBatchRequest", empty, status=400)

    eleven = [("QueueUrl", url)]
    for index in range(1, 12):
        eleven.append((f"SendMessageBatchRequestEntry.{index}.Id", f"e{index}"))
        eleven.append((f"SendMessageBatchRequestEntry.{index}.MessageBody", "x"))
    expect_error("Batch.eleven_entries", "TooManyEntriesInBatchRequest",
                 call("SendMessageBatch", eleven), status=400)


@test
def t_receive_with_attribute_names():
    """ReceiveMessage with `AttributeName.N`, and the selection rule.

    EMPTY IS EMPTY, not everything: a receive that named no attributes gets none,
    a receive that named one gets exactly that one, and `All` gets the set. The
    middle case is the one that matters, because an SDK recomputes
    `MD5OfMessageAttributes` over what it RECEIVED and a facade that answered
    more than was asked for would fail inside the client, naming the client.
    """
    name, url, _ = make_queue("recv")
    call("SendMessage", [
        ("QueueUrl", url),
        ("MessageBody", "attributes"),
        ("MessageAttribute.1.Name", "kept"),
        ("MessageAttribute.1.Value.DataType", "String"),
        ("MessageAttribute.1.Value.StringValue", "yes"),
        ("MessageAttribute.2.Name", "dropped"),
        ("MessageAttribute.2.Value.DataType", "String"),
        ("MessageAttribute.2.Value.StringValue", "no"),
    ])

    message = receive_one(url, [
        ("AttributeName.1", "ApproximateReceiveCount"),
        ("MessageAttributeName.1", "kept"),
    ], wait=2, timeout=30.0)
    if not check("Receive.a_message_arrived", message is not None, "nothing was received"):
        return

    system = pairs_of(message, "Attribute")
    check_eq("Receive.selected_system_attribute_is_present",
             system.get("ApproximateReceiveCount"), "1")
    check_eq("Receive.unselected_system_attributes_are_absent", sorted(system),
             ["ApproximateReceiveCount"])

    selected = pairs_of(message, "MessageAttribute", value="Value")
    check_eq("Receive.selected_message_attribute_names", sorted(selected), ["kept"])
    check_eq("Receive.message_attribute_value_is_a_structure",
             text_of(one(one(message, "MessageAttribute"), "Value"), "StringValue"), "yes")
    check_eq(
        "Receive.md5_covers_only_the_selected_attributes",
        text_of(message, "MD5OfMessageAttributes"),
        md5_of_attributes({"kept": ("String", "String", "yes")}),
    )

    handle = text_of(message, "ReceiptHandle")
    call("ChangeMessageVisibility", [
        ("QueueUrl", url), ("ReceiptHandle", handle), ("VisibilityTimeout", "0"),
    ])

    # ...and now `All`, unindexed, which is the "a single value under a list
    # parameter is a list of one" rule in `query.rs::as_list`.
    again = receive_one(url, [("AttributeName", "All")], wait=2, timeout=30.0)
    if not check("Receive.the_message_came_back_after_a_terminate", again is not None,
                 "the message did not become visible again"):
        return
    everything = pairs_of(again, "Attribute")
    check("Receive.All_selects_more_than_one_attribute", len(everything) >= 2,
          f"got {sorted(everything)}")
    check("Receive.All_includes_SentTimestamp", "SentTimestamp" in everything,
          f"got {sorted(everything)}")
    check("Receive.receive_count_grew_on_the_second_delivery",
          int(everything.get("ApproximateReceiveCount", "0")) >= 2,
          f"got {everything.get('ApproximateReceiveCount')!r}")
    delete_message(url, text_of(again, "ReceiptHandle"))

    # An empty receive: the RESULT ELEMENT is still written (ReceiveMessage has
    # an output shape) and there is no `<Message>` inside it. An SDK reads the
    # absence of `Messages`, not an absent document.
    empty = call("ReceiveMessage", [
        ("QueueUrl", url), ("MaxNumberOfMessages", "1"), ("WaitTimeSeconds", "1"),
    ], timeout=40.0)
    check_eq("Receive.empty_receive_status", empty.status, 200)
    check("Receive.empty_receive_writes_the_result_element",
          empty.result("ReceiveMessage") is not None, empty.body[:300])
    check("Receive.empty_receive_has_no_Message",
          not all_of(empty.result("ReceiveMessage"), "Message"), empty.body[:300])


@test
def t_delete_message_batch():
    """DeleteMessageBatch, with a partial failure in it.

    Two properties at once: the per-entry element is named after the ACTION
    (`DeleteMessageBatchResultEntry`), and a failed entry's `Code` is translated
    into the QUERY spelling on its way into the document. The second is
    `query.rs::batch_error`, and it exists because a client that branches on the
    code from a whole-request refusal has to find the same string inside a
    `BatchResultErrorEntry`.
    """
    name, url, _ = make_queue("delbatch")
    call("SendMessageBatch", [
        ("QueueUrl", url),
        ("SendMessageBatchRequestEntry.1.Id", "a"),
        ("SendMessageBatchRequestEntry.1.MessageBody", "delete-a"),
        ("SendMessageBatchRequestEntry.2.Id", "b"),
        ("SendMessageBatchRequestEntry.2.MessageBody", "delete-b"),
    ])
    # `hold` and not a plain receive: a batch delete needs two handles LIVE at
    # the same time, and two messages that hashed into one lane cannot be
    # (M0_SMOKE.md, D2). It tops the queue up with fillers rather than waiting
    # on a lane that will not open, which is why the queue is drained further
    # down instead of being asserted empty straight away.
    messages = hold(url, 2, timeout=45.0)
    if not check("DeleteBatch.two_messages_were_received", len(messages) == 2,
                 f"got {len(messages)}"):
        for message in messages:
            delete_message(url, text_of(message, "ReceiptHandle"))
        return

    handles = [text_of(message, "ReceiptHandle") for message in messages]
    answer = call("DeleteMessageBatch", [
        ("QueueUrl", url),
        ("DeleteMessageBatchRequestEntry.1.Id", "first"),
        ("DeleteMessageBatchRequestEntry.1.ReceiptHandle", handles[0]),
        ("DeleteMessageBatchRequestEntry.2.Id", "second"),
        ("DeleteMessageBatchRequestEntry.2.ReceiptHandle", handles[1]),
    ])
    check_eq("DeleteBatch.status", answer.status, 200)
    result = answer.result("DeleteMessageBatch")
    entries = all_of(result, "DeleteMessageBatchResultEntry")
    check_eq("DeleteBatch.entries_are_named_after_the_action",
             sorted(text_of(entry, "Id") for entry in entries), ["first", "second"])

    # The two deleted messages are gone; whatever `hold` had to add to get two
    # lanes open is not, so the queue is drained before the depth is read.
    drain(url)
    empty, seen = wait_for_depth_zero(url, timeout=40.0)
    check("DeleteBatch.the_queue_is_empty_afterwards", empty, f"depth {seen}")

    # Now the failure half: one handle that decodes to nothing at all, and one
    # that decoded fine and names a lease that is gone. The first is
    # `ReceiptHandleIsInvalid` in both protocols; the second is the interesting
    # one, because `MessageNotInflight` is `AWS.SimpleQueueService.MessageNotInflight`
    # here and the bare word in JSON.
    partial = call("DeleteMessageBatch", [
        ("QueueUrl", url),
        ("DeleteMessageBatchRequestEntry.1.Id", "garbage"),
        ("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "not-a-receipt-handle"),
    ])
    check_eq("DeleteBatch.bad_handle_status", partial.status, 200)
    failed = all_of(partial.result("DeleteMessageBatch"), "BatchResultErrorEntry")
    check_eq("DeleteBatch.bad_handle_is_a_per_entry_failure", len(failed), 1)
    if failed:
        check_eq("DeleteBatch.failure_reports_the_clients_own_id",
                 text_of(failed[0], "Id"), "garbage")
        check_eq("DeleteBatch.failure_code", text_of(failed[0], "Code"),
                 "ReceiptHandleIsInvalid")
        check_eq("DeleteBatch.failure_blames_the_sender",
                 text_of(failed[0], "SenderFault"), "true")

    stale = call("ChangeMessageVisibilityBatch", [
        ("QueueUrl", url),
        ("ChangeMessageVisibilityBatchRequestEntry.1.Id", "stale"),
        ("ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle", handles[0]),
        ("ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout", "60"),
    ])
    stale_failed = all_of(stale.result("ChangeMessageVisibilityBatch"), "BatchResultErrorEntry")
    code = text_of(stale_failed[0], "Code") if stale_failed else None
    check("ChangeVisibilityBatch.a_deleted_message_is_a_per_entry_failure",
          len(stale_failed) == 1, f"got {len(stale_failed)} failures: {stale.body[:300]!r}")
    # Either refusal is legitimate for a handle whose lease is gone; what is NOT
    # legitimate is the JSON spelling of either one inside an XML document.
    check("ChangeVisibilityBatch.failure_code_is_in_the_query_spelling",
          code in ("AWS.SimpleQueueService.MessageNotInflight", "ReceiptHandleIsInvalid"),
          f"got {code!r}")
    check("ChangeVisibilityBatch.no_json_spelling_leaked",
          ">MessageNotInflight<" not in stale.body, stale.body[:300])


@test
def t_queue_attributes():
    """GetQueueAttributes and SetQueueAttributes, both spellings of the map.

    The REQUEST spells a map two ways and the facade lifts both onto one shape:
    SQS's `Attribute.1.Name`/`.Value`, and the `Attributes.entry.1.key`/`.value`
    form older and SNS-shaped clients emit. The ANSWER spells it exactly one way,
    `<Attribute><Name/><Value/></Attribute>`, and never SNS's `<entry><key/>`.
    """
    name, url, _ = make_queue("attrs", attributes=[("VisibilityTimeout", "30")])

    everything = call("GetQueueAttributes", [("QueueUrl", url), ("AttributeName.1", "All")])
    attributes = pairs_of(everything.result("GetQueueAttributes"), "Attribute")
    for expected in ("QueueArn", "CreatedTimestamp", "LastModifiedTimestamp",
                     "VisibilityTimeout", "MessageRetentionPeriod", "MaximumMessageSize",
                     "DelaySeconds", "ReceiveMessageWaitTimeSeconds",
                     "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"):
        check(f"GetQueueAttributes.All_includes_{expected}", expected in attributes,
              f"got {sorted(attributes)}")
    check_eq("GetQueueAttributes.arn_is_this_deployments",
             attributes.get("QueueArn"), f"arn:aws:sqs:{REGION}:{ACCOUNT}:{name}")
    check("GetQueueAttributes.map_is_not_written_in_the_sns_shape",
          "<entry>" not in everything.body and "<key>" not in everything.body,
          everything.body[:300])

    selected = call("GetQueueAttributes", [
        ("QueueUrl", url),
        ("AttributeName.1", "QueueArn"),
        ("AttributeName.2", "VisibilityTimeout"),
    ])
    names = sorted(pairs_of(selected.result("GetQueueAttributes"), "Attribute"))
    check_eq("GetQueueAttributes.AttributeName_N_selects_exactly_those",
             names, ["QueueArn", "VisibilityTimeout"])

    expect_error("GetQueueAttributes.unknown_name_is_refused", "InvalidAttributeName",
                 call("GetQueueAttributes", [("QueueUrl", url),
                                             ("AttributeName.1", "NoSuchAttribute")]),
                 status=400)

    # SetQueueAttributes has NO output shape, so AWS writes no `<…Result>` at
    # all — an empty one would be a different document, and some XML clients
    # branch on its presence.
    changed = call("SetQueueAttributes", [
        ("QueueUrl", url),
        ("Attribute.1.Name", "VisibilityTimeout"),
        ("Attribute.1.Value", "45"),
    ])
    check_eq("SetQueueAttributes.status", changed.status, 200)
    check_eq("SetQueueAttributes.root_element", changed.root_tag, "SetQueueAttributesResponse")
    check("SetQueueAttributes.writes_no_result_element",
          changed.result("SetQueueAttributes") is None, changed.body[:300])
    check("SetQueueAttributes.still_carries_response_metadata",
          one(changed.root, "ResponseMetadata") is not None, changed.body[:300])
    check_eq(
        "SetQueueAttributes.the_change_landed",
        pairs_of(call("GetQueueAttributes", [("QueueUrl", url),
                                             ("AttributeName.1", "VisibilityTimeout")])
                 .result("GetQueueAttributes"), "Attribute").get("VisibilityTimeout"),
        "45",
    )

    # ...and the same change written the other way: `entry`/`key`/`value`, the
    # segments `query.rs` treats as transparent noise.
    entry_form = call("SetQueueAttributes", [
        ("QueueUrl", url),
        ("Attributes.entry.1.key", "VisibilityTimeout"),
        ("Attributes.entry.1.value", "50"),
    ])
    check_eq("SetQueueAttributes.entry_key_value_spelling_status", entry_form.status, 200)
    check_eq(
        "SetQueueAttributes.entry_key_value_spelling_landed",
        pairs_of(call("GetQueueAttributes", [("QueueUrl", url),
                                             ("AttributeName.1", "VisibilityTimeout")])
                 .result("GetQueueAttributes"), "Attribute").get("VisibilityTimeout"),
        "50",
    )

    # Tags: a map with a different entry element and a different key element,
    # which is why `map_member` is a table and not a rule.
    call("TagQueue", [
        ("QueueUrl", url),
        ("Tag.1.Key", "team"), ("Tag.1.Value", "queen"),
        ("Tag.2.Key", "lane"), ("Tag.2.Value", "query"),
    ])
    tags = call("ListQueueTags", [("QueueUrl", url)])
    check_eq("ListQueueTags.map_uses_Tag_Key_Value",
             pairs_of(tags.result("ListQueueTags"), "Tag", key="Key"),
             {"team": "queen", "lane": "query"})


@test
def t_error_documents():
    """The `<ErrorResponse>` shape, which is the one document every SDK parses.

    Four things have to be exactly right or an SDK maps the error onto the wrong
    exception class, or onto none: the ROOT element, the `<Type>` fault, the
    LEGACY code spelling, and the empty `<Detail/>` SQS writes and SNS does not.
    """
    missing = call("GetQueueUrl", [("QueueName", f"qc-absent-{RUN}")])
    if expect_error("Errors.nonexistent_queue", "QueueDoesNotExist", missing, status=400):
        error = missing.error()
        check_eq("Errors.type_is_Sender", text_of(error, "Type"), "Sender")
        check("Errors.message_is_not_empty", bool(text_of(error, "Message")), "empty <Message>")
        check("Errors.error_carries_an_empty_Detail",
              one(error, "Detail") is not None and not list(one(error, "Detail"))
              and not (one(error, "Detail").text or "").strip(),
              missing.body[:400])
        check("Errors.detail_is_self_closing_like_AWS", "<Detail/>" in missing.body,
              missing.body[:400])
        check_eq("Errors.namespace_is_sqs", missing.namespace, NS_SQS)
        # The request id lives at the TOP level of an ErrorResponse, not inside a
        # `<ResponseMetadata>` — AWS's own asymmetry between the two documents.
        check_eq("Errors.request_id_element_matches_the_header",
                 text_of(missing.root, "RequestId"), missing.request_id)
        check("Errors.no_json_namespace_leaks_into_the_xml",
              "com.amazonaws.sqs#" not in missing.body, missing.body[:400])

    expect_error("Errors.unknown_action", "InvalidAction",
                 call("NoSuchAction", [("QueueUrl", queue_url_of("x"))]), status=400)
    expect_error("Errors.no_action_at_all", "InvalidAction",
                 call(None, [("QueueUrl", queue_url_of("x"))]), status=400)
    expect_error("Errors.missing_required_parameter", "MissingParameter",
                 call("CreateQueue", []), status=400)

    # A queue URL for another account is not this facade's, and it is a MISSING
    # QUEUE rather than an authorization answer: the account segment is naming,
    # not policy.
    expect_error("Errors.queue_url_for_another_account", "QueueDoesNotExist",
                 call("ReceiveMessage", [("QueueUrl", f"{ENDPOINT}/999999999999/whatever")]),
                 status=400)

    # A message action addressed by PATH with no `QueueUrl` parameter. This is
    # the shape AWS's own Query documentation shows (`POST /<account>/<queue>`)
    # and the shape kombu's async Query mode still builds; the facade resolves
    # queues from the `QueueUrl` PARAMETER only (`actions/queues.rs::queue_of`),
    # so it is a refusal. Pinned rather than left unsaid: see README.md,
    # "What this lane found".
    name, url, _ = make_queue("path")
    by_path = call("SendMessage", [("MessageBody", "addressed by path")], url=url)
    expect_error("Errors.queue_addressed_by_path_only_is_refused_today",
                 "MissingParameter", by_path, status=400)

    # The depth cap on a parameter key. No conforming client can reach it — the
    # deepest key SQS defines is seven segments — and the only sender of a
    # thirty-three-segment key is one probing for the worker stack the cap
    # protects, so the refusal is the assertion.
    deep = call("SendMessage", [
        ("QueueUrl", url),
        ("MessageBody", "deep"),
        (".".join(["a"] * 40), "1"),
    ])
    expect_error("Errors.an_absurdly_deep_parameter_key_is_refused",
                 "InvalidParameterValue", deep, status=400)


@test
def t_sigv4_refusals():
    """The signer's negative space: four refusals, four different codes.

    That the POSITIVE case works is proved by every other test in this file. What
    is proved here is that the facade is really CHECKING, which a suite carrying
    one static credential cannot show any other way.
    """
    good = call("ListQueues", [("QueueNamePrefix", f"qc-{RUN}")])
    check_eq("SigV4.a_correctly_signed_request_is_served", good.status, 200)

    expect_error("SigV4.wrong_secret", "SignatureDoesNotMatch",
                 call("ListQueues", secret="not-the-secret"), status=403)
    expect_error("SigV4.unknown_access_key_id", "InvalidClientTokenId",
                 call("ListQueues", akid="NOBODYSKEY"), status=403)
    expect_error("SigV4.unsigned_request", "MissingAuthenticationToken",
                 call("ListQueues", authorization=False), status=403)

    # A clock thirty minutes out: outside AWS's own fifteen-minute window, and
    # the scope date moves WITH the timestamp so that this is the skew check
    # answering and not the scope check.
    old = datetime.now(timezone.utc) - timedelta(minutes=30)
    expect_error("SigV4.stale_clock", "SignatureDoesNotMatch",
                 call("ListQueues", when=old), status=403)

    # ...and a scope whose date does NOT match the timestamp, which is the other
    # check and the same code.
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    expect_error("SigV4.scope_date_disagreeing_with_the_timestamp", "SignatureDoesNotMatch",
                 call("ListQueues", scope_date=yesterday), status=403)

    # A signature scoped to another service reached the wrong door. The scope's
    # SERVICE is the one thing `sigv4.rs` does pin (the region deliberately is
    # not, because an SDK signs with the region its user configured).
    expect_error("SigV4.scope_naming_another_service", "SignatureDoesNotMatch",
                 call("ListQueues", service="s3"), status=403)

    # ...and a scope naming any region at all is SERVED, which is the promise
    # "change endpoint_url and nothing else" rests on: boto3 pointed at this
    # facade still signs with `us-east-1` unless its user changed `region_name`.
    check_eq("SigV4.a_foreign_region_in_the_scope_is_accepted",
             call("ListQueues", params=[("QueueNamePrefix", f"qc-{RUN}")],
                  region="us-east-1").status,
             200)


@test
def t_delete_queue():
    """DeleteQueue: no result element, and the 60-second tombstone."""
    name, url, _ = make_queue("gone")
    call("SendMessage", [("QueueUrl", url), ("MessageBody", "doomed")])

    answer = call("DeleteQueue", [("QueueUrl", url)])
    check_eq("DeleteQueue.status", answer.status, 200)
    check_eq("DeleteQueue.root_element", answer.root_tag, "DeleteQueueResponse")
    check("DeleteQueue.writes_no_result_element",
          answer.result("DeleteQueue") is None, answer.body[:300])
    CREATED.remove(url)

    expect_error("DeleteQueue.url_lookup_afterwards", "QueueDoesNotExist",
                 call("GetQueueUrl", [("QueueName", name)]), status=400)
    expect_error("DeleteQueue.name_is_reserved_for_sixty_seconds", "QueueDeletedRecently",
                 call("CreateQueue", [("QueueName", name)]), status=400)


@test
def t_version_and_envelope_details():
    """Two envelope details worth pinning on their own.

    DEFINED LAST ON PURPOSE: the request-id assertions below read what `call`
    accumulated across every request the run made, so they have to run after all
    of them.
    """
    # The `Version` parameter may be omitted: the XML renderer's namespace
    # selector treats "not SNS's version" as SQS, so a client that forgot it is
    # still answered in the right namespace rather than in SNS's.
    no_version = call("ListQueues", [("QueueNamePrefix", f"qc-{RUN}")], version=None)
    check_eq("Version.may_be_omitted", no_version.status, 200)
    check_eq("Version.omitted_still_answers_in_the_sqs_namespace",
             no_version.namespace, NS_SQS)

    # Every answer, success or error, carries the request id header, and no two
    # requests share one. Accumulated across the whole run by `call`.
    check("RequestId.on_every_answer", not WIRE["responses_without_request_id"],
          f"missing on: {WIRE['responses_without_request_id'][:5]}")
    check_eq("RequestId.is_unique_per_request",
             len(set(WIRE["request_ids"])), len(WIRE["request_ids"]))
    check_eq("Protocol.no_request_carried_an_X_Amz_Target", WIRE["x_amz_target_sent"], 0)
    check_eq("Protocol.every_answer_was_xml",
             sorted({content_type.split(";")[0].strip()
                     for content_type in WIRE["response_content_types"]}),
             ["text/xml"])


# ------------------------------------------------------------------ small fry

def _is_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


# ---------------------------------------------------------------------- main

def teardown():
    for url in list(CREATED):
        try:
            call("DeleteQueue", [("QueueUrl", url)], timeout=10.0)
        except Exception:
            pass


def report_protocol():
    """The contract's protocol line, from what this client actually sent."""
    for content_type, count in sorted(WIRE["request_content_types"].items(),
                                      key=lambda kv: -kv[1]):
        print(f"# protocol spoken: Query/XML (request Content-Type: {content_type}) "
              f"— {count} request(s)", flush=True)
    for content_type, count in sorted(WIRE["response_content_types"].items(),
                                      key=lambda kv: -kv[1]):
        print(f"# answers came back as: {content_type} — {count}", flush=True)
    print(f"# X-Amz-Target sent on: {WIRE['x_amz_target_sent']} of {WIRE['requests']} "
          f"requests (any number but zero means this lane stopped testing the "
          f"Query codec)", flush=True)
    roots = ", ".join(f"{root} x{count}" for root, count
                      in sorted(WIRE["root_elements"].items(), key=lambda kv: -kv[1]))
    print(f"# XML root elements parsed: {roots}", flush=True)


def main():
    print(f"# endpoint {ENDPOINT}  region {REGION}  account {ACCOUNT}  run {RUN}", flush=True)
    print("# client: hand-rolled Query/XML over urllib, SigV4 signed in-file, no SDK",
          flush=True)

    # The reachability probe, and the only place a transport failure is caught
    # by name: inside a test, a connection that dropped is an unexpected
    # exception and is reported as that test failing, which is what it is.
    try:
        reachable = call("ListQueues", [("QueueNamePrefix", f"qc-{RUN}")], timeout=15.0)
    except (urllib.error.URLError, OSError) as e:
        print(f"FAIL rig.reachable: {ENDPOINT} did not answer: {e}")
        print("RESULT: FAIL")
        return 1
    if reachable.status != 200:
        print(f"FAIL rig.reachable: ListQueues answered {reachable.status}: "
              f"{reachable.body[:300]!r}")
        print("RESULT: FAIL")
        return 1

    for fn in TESTS:
        try:
            fn()
        except Exception:
            # One test blowing up must not cost the run every assertion after
            # it: the trace is printed, the test is a failure, the rest go on.
            # `smoke_m0.py`'s rule.
            fail(fn.__name__, "unexpected exception")
            traceback.print_exc()

    teardown()

    report_protocol()
    print(f"# {PASSES} passed, {len(FAILURES)} failed", flush=True)
    for name in FAILURES:
        print(f"#   failed: {name}", flush=True)
    print(f"RESULT: {'FAIL' if FAILURES else 'PASS'}", flush=True)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
