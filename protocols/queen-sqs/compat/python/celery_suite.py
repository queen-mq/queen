#!/usr/bin/env python3
"""Celery over kombu's SQS transport, against a live queen-sqs.

This file is BOTH the suite and the Celery application, because a worker has to
import the module that defines its tasks:

    python protocols/queen-sqs/compat/python/celery_suite.py       # the suite (this runs it)
    python -m celery -A celery_suite worker --pool solo  # what the suite spawns

Everything above the `if __name__ == "__main__"` guard is import-safe: reading
the environment, building the app, and registering two tasks. The suite itself
only runs from `main`.

WHY CELERY IS THE INTERESTING CLIENT. boto3 exercises the API; Celery exercises
the WORKFLOW, and the workflow is where a facade's semantics show. kombu's SQS
transport does four things no plain SDK call does:

  * it LISTS and CREATES queues at start-up, which is the idempotent-create
    shape M0_SMOKE.md's D1 is about;
  * it long-polls in a loop and deletes each message as its own `DeleteMessage`,
    for the lifetime of the worker;
  * it lets the TASK decide whether the message is acknowledged, so a task that
    fails under `acks_late` + `acks_on_failure_or_timeout=False` leaves the
    message in flight and SQS's VISIBILITY TIMEOUT is what redelivers it
    (celery 5.6's `Request.on_failure`: with `acks_on_failure_or_timeout`
    false and nothing else true, it takes the last branch and calls
    `self.reject(requeue=False)`, which pops the message from the worker's local
    delivered map WITHOUT deleting it from the queue). That is the retry
    mechanism this suite proves, and it is a property of the broker, not of the
    client: nothing but a real lease expiry can produce it;
  * and it reads `ApproximateReceiveCount` off every delivery to drive its own
    backoff policy (`kombu/transport/SQS.py`, `QoS.extract_task_name_and_
    number_of_retries`), so that attribute is load-bearing here and not decoration.

TWO PHASES, because kombu has two queue-resolution modes and they exercise
opposite halves of the facade:

  * **discovered** — no `predefined_queues`. kombu calls `ListQueues` at channel
    open and `CreateQueue` for a queue it does not find. This is the mode that
    creates queues at start-up.
  * **predefined** — `predefined_queues` maps the queue name straight to a URL.
    kombu then calls NEITHER `ListQueues` NOR `CreateQueue`
    (kombu 5.6's `Channel._create_queue` returns `None` from its first statement
    when `predefined_queues` is set), and every call goes to the URL it was
    handed. This is the deployment shape where the worker has no queue-admin
    permissions at all, and it is the one the task brief names.

  ONE CONSEQUENCE WORTH STATING: because `predefined_queues` suppresses the
  create entirely, and because in `discovered` mode kombu's own `_queue_cache`
  plus its targeted `ListQueues(QueueNamePrefix=<full name>)` mean it never
  creates a queue it can already see, kombu does not by itself issue the
  idempotent REPEAT create that D1 is about. It issues the FIRST one. The repeat
  is therefore replayed here in kombu's exact shape — `CreateQueue(QueueName,
  Attributes={'VisibilityTimeout': ...})` against the queue kombu just made —
  which is byte for byte the call a second worker fleet, or a redeployed one
  whose cache is cold and whose ListQueues is filtered by IAM, would make.

THE CONTRACT (`protocols/queen-kafka/compat/CLIENT_MATRIX.md`, as `smoke_m0.py`):

  * the stack comes from the environment, never from a hardcoded address;
  * ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
  * `RESULT: PASS` or `RESULT: FAIL` as the last line;
  * a nonzero exit status when anything failed;
  * and the protocol the client actually spoke is REPORTED, read from the
    client's own debug stream. Here that stream is botocore's own
    `botocore.endpoint` DEBUG log, captured to a file by [`install_wire_log`] in
    both the suite process and the worker process, and parsed by
    [`read_wire_log`] for the `Content-Type` and `X-Amz-Target` of every request
    botocore built. Nothing is inferred from the SDK's version.

  THE ONE GAP IN THAT STREAM, stated rather than papered over: celery's worker
  uses kombu's ASYNC hub when `pycurl` is installed, and on that path kombu
  builds and sends the `ReceiveMessage` itself (`kombu/asynchronous/aws/`) —
  botocore's endpoint, and therefore its debug log, is not involved. Every other
  call (`CreateQueue`, `ListQueues`, `SendMessage`, `DeleteMessage`,
  `GetQueueAttributes`) goes through botocore on both paths, because
  `Channel.basic_ack` deletes with the synchronous client. So the `discovered`
  phase runs the worker with the transport's `asynchronous` flag turned off —
  celery then drains through `Channel._get_bulk`, which is a plain botocore
  `ReceiveMessage`, and the receive appears in the stream like everything else.
  The `predefined` phase leaves celery in its default configuration. Between
  them every operation is observed at least once, and both consumer paths run.

  (That flag is also why this lane does not need `pycurl`: with the async hub
  off, kombu's SQS transport is pure boto3. If `pycurl` is missing, BOTH phases
  fall back to the blocking consumer and the run says so.)

WHAT IS NOT ASSERTED. Ordering — a standard SQS queue does not promise it and
neither does this facade. Timing beyond the visibility floor. And the exact
number of `ReceiveMessage` calls, which is a polling-loop detail.

  $ protocols/queen-sqs/compat/rig.sh up
  $ source protocols/queen-sqs/compat/.rig/env.sh
  $ python protocols/queen-sqs/compat/python/celery_suite.py
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import uuid

import boto3
import botocore.exceptions
import kombu.pools
from celery import Celery
from celery.signals import celeryd_after_setup

# --------------------------------------------------------------------- stack

ENDPOINT = os.environ.get("QUEEN_SQS_ENDPOINT", "http://127.0.0.1:19324").rstrip("/")
REGION = os.environ.get("QUEEN_SQS_REGION", "queen-1")
ACCOUNT = os.environ.get("QUEEN_SQS_ACCOUNT", "000000000000")
AKID = os.environ.get("AWS_ACCESS_KEY_ID", "QSQSTEST")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "qsqssecret")

#: How long a message stays invisible after a delivery. It is the QUEUE's
#: attribute (kombu sets it at CreateQueue from `visibility_timeout`), it is what
#: the facade turns into a pop's `leaseSeconds`, and it is the clock the failing
#: task's redelivery is measured against — so it wants to be long enough that a
#: slow first delivery cannot be mistaken for an expiry and short enough that the
#: run does not sit waiting for it.
VISIBILITY = int(os.environ.get("QSQS_CELERY_VISIBILITY", "12"))

#: What the worker process is told, and how. Every one of these is read at IMPORT
#: time, because the worker's `-A celery_suite` import is where its app is built.
ENV_QUEUE = "QSQS_CELERY_QUEUE"
ENV_RESULTS = "QSQS_CELERY_RESULTS"
ENV_PREDEFINED = "QSQS_CELERY_PREDEFINED"
ENV_SYNC = "QSQS_CELERY_SYNC"
ENV_WIRELOG = "QSQS_CELERY_WIRELOG"

QUEUE = os.environ.get(ENV_QUEUE, "qsqs-celery-default")
RESULTS = os.environ.get(ENV_RESULTS, "")

RUN = uuid.uuid4().hex[:8]

#: Queue URLs this run brought into existence, whoever created them — kombu or
#: the suite. Appended the moment a URL is known rather than returned at the end
#: of a phase, so that a phase which gives up half way still leaves the rig as
#: it found it.
QUEUES = []

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


# ------------------------------------------------------- the client's own log

class _OnlyRequests(logging.Filter):
    """Keep botocore's request line and drop everything else on that logger.

    The line after it (`Sending http request: <AWSPreparedRequest ...>`) carries
    the `Authorization` header — a per-request HMAC rather than a secret, but
    still noise this file has no use for, and a log it is easier to promise
    nothing about.
    """

    def filter(self, record):
        return record.getMessage().startswith("Making request for")


_WIRE_HANDLER = None
_WIRE_PATH = None


def install_wire_log(path=None):
    """Capture botocore's own request log to `path`.

    This is the suite contract's "read from the client's own debug output": the
    line botocore writes before it sends anything, naming the operation and the
    headers its serializer built. Installed on the `botocore.endpoint` logger and
    nowhere else, with `propagate` off so that celery's own root handler — which
    the worker installs after this module is imported — cannot pick the records
    up and flood the worker log with them.

    Calling it again with a DIFFERENT path moves the capture, which is what the
    suite process needs: it publishes in both phases and each phase's assertions
    are about the calls that phase made. Calling it again with the SAME path (the
    `celeryd_after_setup` re-install below) does nothing.
    """
    global _WIRE_HANDLER, _WIRE_PATH
    path = path or os.environ.get(ENV_WIRELOG, "")
    if not path or path == _WIRE_PATH:
        return
    logger = logging.getLogger("botocore.endpoint")
    if _WIRE_HANDLER is not None:
        logger.removeHandler(_WIRE_HANDLER)
        _WIRE_HANDLER.close()
    handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.addFilter(_OnlyRequests())
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    _WIRE_HANDLER, _WIRE_PATH = handler, path


_OPERATION = re.compile(r"Making request for OperationModel\(name=(?P<op>[A-Za-z]+)\)")
_TARGET = re.compile(r"'X-Amz-Target':\s*'([^']*)'")
_CONTENT_TYPE = re.compile(r"'Content-Type':\s*'([^']*)'")


def read_wire_log(*paths):
    """What the client put on the wire, per operation: `{op: {(ct, target): n}}`."""
    seen = {}
    for path in paths:
        if not path or not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = _OPERATION.search(line)
                if not match:
                    continue
                target = _TARGET.search(line)
                content_type = _CONTENT_TYPE.search(line)
                shape = (
                    content_type.group(1) if content_type else "(none)",
                    target.group(1) if target else "(none)",
                )
                per_op = seen.setdefault(match.group("op"), {})
                per_op[shape] = per_op.get(shape, 0) + 1
    return seen


# ------------------------------------------------------ evidence the tasks write

def _evidence_path(name):
    return os.path.join(RESULTS, f"{name}.jsonl")


def record(name, entry):
    """One line of proof that a task body ran, written by the WORKER process.

    A file and not a result backend: this app deliberately has no backend (there
    is no Redis or database in the rig, and adding one would put a second broker
    in a test about the first), and a file the task itself appends to is the only
    evidence that the task's CODE ran rather than that a message moved.
    """
    if not RESULTS:
        return
    with open(_evidence_path(name), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_evidence(results_dir, name):
    path = os.path.join(results_dir, f"{name}.jsonl")
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                # A half-written last line is possible in principle; it is never
                # the line an assertion is about, so it is skipped rather than
                # made into a failure of the suite.
                continue
    return out


# ------------------------------------------------------------------- the app

def transport_options(queue=QUEUE, predefined=None):
    if predefined is None:
        predefined = os.environ.get(ENV_PREDEFINED, "") == "1"
    options = {
        "region": REGION,
        # kombu builds `endpoint_url` out of the broker URL's host and this
        # flag; without it the transport would address https on a plain rig.
        "is_secure": ENDPOINT.startswith("https://"),
        # What kombu writes as `VisibilityTimeout` when it creates the queue.
        "visibility_timeout": VISIBILITY,
        # Long poll, short enough that a stopped worker exits promptly.
        "wait_time_seconds": 2,
        "polling_interval": 0.5,
    }
    if predefined:
        options["predefined_queues"] = {
            queue: {
                "url": f"{ENDPOINT}/{ACCOUNT}/{queue}",
                "access_key_id": AKID,
                "secret_access_key": SECRET,
                "region": REGION,
            }
        }
    return options


def broker_url():
    """`sqs://<akid>:<secret>@<host>:<port>`.

    kombu takes the endpoint from the URL's host and port (`Channel.endpoint_url`
    is `scheme://hostname[:port]`), and the credentials from its userinfo, so
    this one string is the whole of "point Celery at queen-sqs". Both halves are
    percent-quoted because a secret is allowed to contain `/` and `@`.
    """
    host = urllib.parse.urlsplit(ENDPOINT).netloc
    return (
        f"sqs://{urllib.parse.quote(AKID, safe='')}:"
        f"{urllib.parse.quote(SECRET, safe='')}@{host}"
    )


def configure(application, queue=QUEUE, predefined=None):
    application.conf.update(
        broker_url=broker_url(),
        broker_transport_options=transport_options(queue, predefined),
        task_default_queue=queue,
        # No backend: see `record`. `task_ignore_result` keeps celery from
        # trying to store one anyway.
        result_backend=None,
        task_ignore_result=True,
        # SQS has no fanout, so celery would disable the remote-control pidbox
        # itself; saying so removes a start-up warning and one round trip.
        worker_enable_remote_control=False,
        worker_send_task_events=False,
        broker_connection_retry_on_startup=True,
        # Small enough that the worker does not hold the whole run in flight,
        # large enough that a queue of eleven messages drains without one
        # receive per message.
        worker_prefetch_multiplier=4,
        task_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
    )
    return application


app = configure(Celery("queen_sqs_celery_suite"))


@celeryd_after_setup.connect
def _reinstall_wire_log(sender=None, instance=None, **kwargs):
    """Celery reconfigures logging after the app module is imported; the capture
    is idempotent and reinstalling it here costs nothing and survives that."""
    install_wire_log()


# The two tasks. Their names are EXPLICIT and not derived from the module,
# because this file is imported as `__main__` by the suite and as `celery_suite`
# by the worker: auto-naming would give the same function two names and the
# worker would refuse every message the suite sent.

@app.task(name="qsqs.ok", bind=True, ignore_result=True)
def ok_task(self, index):
    """The happy path: acknowledged the moment celery hands it over."""
    record("ok", {
        "index": index,
        "task_id": self.request.id,
        "at": time.time(),
        "pid": os.getpid(),
    })
    return index


@app.task(
    name="qsqs.flaky",
    bind=True,
    ignore_result=True,
    # THE TWO SETTINGS THAT MAKE THIS A VISIBILITY TEST rather than a Celery
    # retry test. `acks_late` moves the acknowledgement to after the task body,
    # and `acks_on_failure_or_timeout=False` means a body that RAISED is not
    # acknowledged at all: celery calls `reject(requeue=False)`, which for
    # kombu's SQS transport pops the message from the worker's local map and
    # deletes NOTHING. The message is still in flight at the broker, and the
    # only thing that can bring it back is its lease expiring.
    #
    # `self.retry()` would have been the other way to write this and it would
    # have proved nothing about the broker: it republishes immediately, with a
    # new message id, which any transport can do.
    acks_late=True,
    acks_on_failure_or_timeout=False,
    max_retries=0,
)
def flaky_task(self, index):
    """Fails on its first delivery and succeeds on the redelivery.

    Which delivery this is comes from the evidence file rather than from any
    counter in memory: the point is that the SECOND run is a second DELIVERY of
    the same message — same celery task id, minutes-old — and not a second call.
    """
    previous = [
        entry for entry in read_evidence(RESULTS, "flaky")
        if entry.get("task_id") == self.request.id
    ]
    attempt = len(previous) + 1
    record("flaky", {
        "index": index,
        "task_id": self.request.id,
        "attempt": attempt,
        "at": time.time(),
        "pid": os.getpid(),
    })
    if attempt == 1:
        raise RuntimeError(
            "first delivery fails on purpose: the message must stay in flight "
            "and come back when its visibility timeout expires"
        )
    return attempt


# The transport's async flag, turned off when asked. It has to happen at IMPORT
# time, before celery's consumer reads `implements.asynchronous` to decide
# whether to run its event loop, and the import is the only hook that runs then.
def _maybe_force_synchronous_transport():
    if os.environ.get(ENV_SYNC, "") != "1":
        return False
    from kombu.transport import SQS as sqs_transport

    sqs_transport.Transport.implements = sqs_transport.Transport.implements.extend(
        asynchronous=False,
    )
    return True


SYNCHRONOUS = _maybe_force_synchronous_transport()
install_wire_log()


def pycurl_available():
    """Whether celery's default consumer can run kombu's async hub here.

    `kombu.asynchronous.http.Client` is `CurlClient` and nothing else, so the
    async `ReceiveMessage` path exists only when pycurl imports. Probed by
    importing rather than by `find_spec`, because a pycurl that is installed and
    does not load is the same fact as one that is missing.
    """
    try:
        __import__("pycurl")
    except Exception:
        return False
    return True


# =========================================================================
#                                the suite
# =========================================================================

HERE = os.path.dirname(os.path.abspath(__file__))


def sqs_client():
    """boto3, for the control-plane assertions the worker cannot make itself.

    It is not a second client under test: it is the instrument. Everything it is
    asked is a fact about the QUEUE — does it exist, what are its attributes, is
    it empty — that has to be read from outside the worker to mean anything.
    """
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=AKID,
        aws_secret_access_key=SECRET,
    )


def error_code(exc):
    return exc.response.get("Error", {}).get("Code")


def query_error_code(exc):
    return exc.response.get("Error", {}).get("QueryErrorCode")


def queue_exists(client, name):
    try:
        return client.get_queue_url(QueueName=name)["QueueUrl"]
    except botocore.exceptions.ClientError as e:
        if query_error_code(e) == "QueueDoesNotExist" or error_code(e) in (
            "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist",
        ):
            return None
        raise


def attributes_of(client, url, names=("All",)):
    return client.get_queue_attributes(
        QueueUrl=url, AttributeNames=list(names)
    ).get("Attributes", {})


def wait_until(predicate, timeout, interval=0.5, alive=None):
    """Poll `predicate` until it is true or `timeout` elapses.

    `alive`, when given, is called each turn and must stay true: it is how a
    worker that died gets reported as a dead worker instead of as a timeout
    thirty seconds later.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if alive is not None and not alive():
            return False
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return False


def tail(path, lines=30):
    if not os.path.exists(path):
        return "(no log)"
    with open(path, encoding="utf-8", errors="replace") as handle:
        return "".join(handle.readlines()[-lines:])


class Worker:
    """A `celery worker` in its own process, as a context manager.

    A subprocess and not an in-process worker on purpose: the tasks have to run
    somewhere that cannot see the suite's variables, so that the evidence files
    are the only channel between them and every assertion is about what really
    crossed the broker.
    """

    def __init__(self, phase, queue, env, log_path):
        self.phase = phase
        self.queue = queue
        self.env = env
        self.log_path = log_path
        self.process = None
        self._log = None

    def __enter__(self):
        self._log = open(self.log_path, "w", encoding="utf-8")
        command = [
            sys.executable, "-m", "celery",
            "-A", "celery_suite", "worker",
            "--pool", "solo",
            # The solo pool runs one task at a time whatever this says, but
            # celery multiplies it by `worker_prefetch_multiplier` to size the
            # prefetch, and the default is the machine's CPU count — which would
            # make how many messages the worker holds in flight a property of
            # whose laptop it ran on.
            "--concurrency", "1",
            "--loglevel", "INFO",
            "-Q", self.queue,
            "-n", f"qsqs-{self.phase}-{RUN}@%h",
            # AMQP-only features; SQS has no fanout, so celery would skip them
            # anyway. Named explicitly so a start-up that hangs cannot be
            # blamed on them.
            "--without-gossip", "--without-mingle", "--without-heartbeat",
        ]
        self.process = subprocess.Popen(
            command, cwd=HERE, env=self.env,
            stdout=self._log, stderr=subprocess.STDOUT,
        )
        return self

    def alive(self):
        return self.process is not None and self.process.poll() is None

    def __exit__(self, *_):
        if self.process is not None and self.process.poll() is None:
            # SIGTERM is celery's warm shutdown: it finishes what it is holding
            # and stops consuming. A worker killed outright would leave its
            # prefetched messages in flight and make the drain assertion a lie.
            self.process.terminate()
            try:
                self.process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self._log is not None:
            self._log.close()
        return False


def worker_env(queue, results, predefined, sync, wire_log):
    env = dict(os.environ)
    env[ENV_QUEUE] = queue
    env[ENV_RESULTS] = results
    env[ENV_PREDEFINED] = "1" if predefined else "0"
    env[ENV_SYNC] = "1" if sync else "0"
    env[ENV_WIRELOG] = wire_log
    env["QSQS_CELERY_VISIBILITY"] = str(VISIBILITY)
    # The worker's `-A celery_suite` has to find this file, and `cwd` alone is
    # not enough when the interpreter was started elsewhere.
    env["PYTHONPATH"] = HERE + os.pathsep + env.get("PYTHONPATH", "")
    # The stack, pinned rather than inherited. The worker resolves these itself
    # at import, with the same defaults this process used, so an inherited
    # environment would agree — right up until somebody runs the suite with an
    # override the worker would then read differently. Writing the values this
    # process actually resolved removes the question.
    env["QUEEN_SQS_ENDPOINT"] = ENDPOINT
    env["QUEEN_SQS_REGION"] = REGION
    env["QUEEN_SQS_ACCOUNT"] = ACCOUNT
    env["AWS_ACCESS_KEY_ID"] = AKID
    env["AWS_SECRET_ACCESS_KEY"] = SECRET
    return env


def drop_connections():
    """Make the next publish open a NEW broker connection.

    One process, two phases, and they differ in exactly the transport option
    that decides whether kombu lists and creates — so a connection carried over
    from the first phase would make the second phase's assertions describe the
    FIRST phase's configuration. `app.close()` is not enough, and the reason is
    worth writing down because it cost a live run to find:

    * `Celery.close()` (celery 5.6, `app/base.py`) does two things —
      `self._pool = None` and `_deregister_app(self)`. It does NOT touch
      `app.amqp`, and `AMQP.producer_pool` is where a publishing app actually
      keeps its connection (`app/amqp.py`: `self._producer_pool = pools.producers[
      self.app.connection_for_write()]`, memoized on the AMQP instance, which is
      itself a `cached_property` of the app).
    * so after `close()` the app still holds a producer pool built around a
      `Connection` whose `transport_options` are the ones that were in
      `conf.broker_transport_options` when the pool was first built. Reconfiguring
      the app afterwards changes the conf and nothing else.

    Symptom when this is missed: the predefined phase's publisher speaks with the
    discovered phase's transport options — no `predefined_queues` — so kombu's
    `_resolve_queue_url` misses its cache and calls
    `ListQueues(QueueNamePrefix=<the full queue name>)`, and
    `Predefined.publishing_sent_no_ListQueues` fails on a wire log that is
    describing a channel the phase never meant to open. The facade answered every
    one of those calls correctly; the suite was asking the wrong process state.

    Dropping the memoized `amqp` is what forces `producer_pool` to be rebuilt;
    `kombu.pools.reset()` then makes sure the rebuild cannot be handed the same
    pooled connection back out of kombu's process-wide pool group.
    """
    app.close()
    app.__dict__.pop("amqp", None)
    kombu.pools.reset()


def dispatch(queue, predefined, ok_count, with_flaky):
    """Publish from THIS process, with an app configured for the phase."""
    configure(app, queue=queue, predefined=predefined)
    drop_connections()
    ids = []
    for index in range(ok_count):
        ids.append(app.send_task("qsqs.ok", args=[index], queue=queue).id)
    flaky_id = None
    if with_flaky:
        flaky_id = app.send_task("qsqs.flaky", args=[ok_count], queue=queue).id
    drop_connections()
    return ids, flaky_id


def drained(client, url):
    attributes = attributes_of(client, url, (
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ))
    counts = {name: int(value) for name, value in attributes.items()}
    return counts if all(value == 0 for value in counts.values()) else None


# ------------------------------------------------------- phase: discovered

def phase_discovered(client, workdir):
    """kombu with no `predefined_queues`: it lists, it creates, it consumes.

    Run with the transport's async hub OFF, so that every call the worker makes
    — the `ReceiveMessage` included — is a botocore call and lands in the wire
    log the protocol line is read from.
    """
    phase = "discovered"
    queue = f"qsqs-cel-disc-{RUN}"
    results = os.path.join(workdir, phase)
    os.makedirs(results, exist_ok=True)
    parent_log = os.path.join(workdir, f"{phase}-parent-wire.log")
    worker_log = os.path.join(workdir, f"{phase}-worker-wire.log")
    stdout_log = os.path.join(workdir, f"{phase}-worker.log")

    check("Discovered.queue_does_not_exist_yet", queue_exists(client, queue) is None,
          "the run's queue was already there")

    # The suite process publishes, which is what makes kombu open a channel,
    # list, and create. Its own botocore traffic goes to the parent wire log.
    install_wire_log(parent_log)
    dispatch(queue, predefined=False, ok_count=3, with_flaky=False)

    # READ THE WIRE LOG HERE, before the boto3 control-plane calls below add
    # `CreateQueue` and `ListQueues` of their own: at this instant every line in
    # the file was written by kombu, so a `CreateQueue` in it is kombu's.
    published = read_wire_log(parent_log)
    check("Discovered.kombu_called_ListQueues", "ListQueues" in published,
          f"operations kombu made: {sorted(published)}")
    check("Discovered.kombu_called_CreateQueue", "CreateQueue" in published,
          f"operations kombu made: {sorted(published)}")
    check_eq("Discovered.kombu_sent_three_messages",
             sum(published.get("SendMessage", {}).values()), 3)

    url = queue_exists(client, queue)
    if not check("Discovered.kombu_created_the_queue", url is not None,
                 "no queue after publishing"):
        return
    QUEUES.append(url)
    attributes = attributes_of(client, url)
    check_eq("Discovered.created_with_kombus_visibility_timeout",
             attributes.get("VisibilityTimeout"), str(VISIBILITY))
    check("Discovered.facade_stamped_its_partition_count",
          "queen.partitions" in attributes, f"got {sorted(attributes)}")

    # D1, in kombu's exact shape and then in the two shapes around it. This is
    # the call every framework makes at worker start-up against a queue somebody
    # else created; M0_SMOKE.md's D1 is the record of it being refused, and this
    # is that record turned into a live assertion on the Celery lane.
    same_url = client.create_queue(QueueName=queue)["QueueUrl"]
    check_eq("Discovered.idempotent_create_with_no_attributes", same_url, url)
    kombu_shape = client.create_queue(
        QueueName=queue, Attributes={"VisibilityTimeout": str(VISIBILITY)},
    )["QueueUrl"]
    check_eq("Discovered.idempotent_create_in_kombus_own_shape", kombu_shape, url)
    try:
        client.create_queue(
            QueueName=queue, Attributes={"VisibilityTimeout": str(VISIBILITY + 7)},
        )
        fail("Discovered.conflicting_create_is_refused", "the request was served")
    except botocore.exceptions.ClientError as e:
        check("Discovered.conflicting_create_is_refused",
              query_error_code(e) == "QueueNameExists"
              or error_code(e) in ("QueueAlreadyExists", "QueueNameExists"),
              f"got {error_code(e)!r}/{query_error_code(e)!r}")

    env = worker_env(queue, results, predefined=False, sync=True, wire_log=worker_log)
    with Worker(phase, queue, env, stdout_log) as worker:
        got = wait_until(
            lambda: len(read_evidence(results, "ok")) >= 3,
            timeout=90, alive=worker.alive,
        )
        if not check("Discovered.the_worker_ran_every_task", bool(got),
                     f"only {len(read_evidence(results, 'ok'))} of 3 ran; "
                     f"worker alive={worker.alive()}; log tail:\n{tail(stdout_log)}"):
            return
        check_eq("Discovered.each_task_ran_exactly_once",
                 sorted(entry["index"] for entry in read_evidence(results, "ok")),
                 [0, 1, 2])
        empty = wait_until(lambda: drained(client, url), timeout=60, alive=worker.alive)
        check("Discovered.the_queue_drained", bool(empty),
              f"depth {attributes_of(client, url, ('ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'))}")

    check("Discovered.the_worker_shut_down_cleanly", not worker.alive(),
          "the worker was still running after SIGTERM")

    # The worker's own half of the stream. With the async hub off, the receive
    # is a botocore call like everything else — which is the whole reason this
    # phase turns it off.
    worker_wire = read_wire_log(worker_log)
    check("Discovered.the_worker_received_through_botocore",
          "ReceiveMessage" in worker_wire, f"operations seen: {sorted(worker_wire)}")
    check("Discovered.the_worker_deleted_through_botocore",
          "DeleteMessage" in worker_wire, f"operations seen: {sorted(worker_wire)}")
    check_eq("Discovered.the_worker_deleted_every_message",
             sum(worker_wire.get("DeleteMessage", {}).values()), 3)
    # The wire logs, for the run-wide protocol line. The queue itself is
    # already in `QUEUES` and is torn down from there.
    return [parent_log, worker_log]


# -------------------------------------------------------- phase: predefined

def phase_predefined(client, workdir):
    """kombu with `predefined_queues`, celery in its default configuration.

    The deployment shape the brief names, and the one a worker with no
    queue-admin permissions runs: the URL is handed to kombu, so it neither
    lists nor creates, and every call goes straight to that URL. The failing
    task lives here, because this is the phase whose consumer path is celery's
    real one.
    """
    phase = "predefined"
    queue = f"qsqs-cel-pre-{RUN}"
    results = os.path.join(workdir, phase)
    os.makedirs(results, exist_ok=True)
    parent_log = os.path.join(workdir, f"{phase}-parent-wire.log")
    worker_log = os.path.join(workdir, f"{phase}-worker-wire.log")
    stdout_log = os.path.join(workdir, f"{phase}-worker.log")

    # The queue is made OUT OF BAND, which is what `predefined_queues` means.
    url = client.create_queue(
        QueueName=queue,
        Attributes={"VisibilityTimeout": str(VISIBILITY),
                    "MessageRetentionPeriod": "3600"},
    )["QueueUrl"]
    QUEUES.append(url)
    check_eq("Predefined.the_url_kombu_will_be_handed", url,
             f"{ENDPOINT}/{ACCOUNT}/{queue}")

    install_wire_log(parent_log)
    ok_ids, flaky_id = dispatch(queue, predefined=True, ok_count=10, with_flaky=True)
    check_eq("Predefined.ten_tasks_were_dispatched", len(ok_ids), 10)
    check("Predefined.the_flaky_task_was_dispatched", bool(flaky_id), "no task id")

    parent_wire = read_wire_log(parent_log)
    check("Predefined.publishing_sent_no_ListQueues", "ListQueues" not in parent_wire,
          f"operations seen: {sorted(parent_wire)}")
    check("Predefined.publishing_sent_no_CreateQueue", "CreateQueue" not in parent_wire,
          f"operations seen: {sorted(parent_wire)}")
    check_eq("Predefined.eleven_messages_were_sent",
             sum(parent_wire.get("SendMessage", {}).values()), 11)

    env = worker_env(queue, results, predefined=True, sync=not pycurl_available(),
                     wire_log=worker_log)
    with Worker(phase, queue, env, stdout_log) as worker:
        got = wait_until(
            lambda: len(read_evidence(results, "ok")) >= 10,
            timeout=120, alive=worker.alive,
        )
        if not check("Predefined.all_ten_tasks_executed", bool(got),
                     f"only {len(read_evidence(results, 'ok'))} of 10 ran; "
                     f"worker alive={worker.alive()}; log tail:\n{tail(stdout_log)}"):
            return
        entries = read_evidence(results, "ok")
        check_eq("Predefined.every_index_ran", sorted(e["index"] for e in entries),
                 list(range(10)))
        check_eq("Predefined.no_task_was_delivered_twice",
                 len({e["task_id"] for e in entries}), 10)
        check_eq("Predefined.the_worker_is_one_process",
                 len({e["pid"] for e in entries}), 1)

        # The failing task. Its first delivery raised, so nothing acknowledged
        # it and nothing deleted it; only its lease expiring can bring it back.
        # `VISIBILITY + 45` is the deadline: the wait itself is `VISIBILITY`.
        redelivered = wait_until(
            lambda: len([e for e in read_evidence(results, "flaky")
                         if e.get("task_id") == flaky_id]) >= 2,
            timeout=VISIBILITY + 45, alive=worker.alive,
        )
        flaky = [e for e in read_evidence(results, "flaky")
                 if e.get("task_id") == flaky_id]
        if not check("Predefined.the_failing_task_was_redelivered", bool(redelivered),
                     f"{len(flaky)} deliveries in {VISIBILITY + 45}s; "
                     f"worker alive={worker.alive()}; log tail:\n{tail(stdout_log)}"):
            return
        check_eq("Predefined.the_redelivery_is_the_same_message",
                 len({e["task_id"] for e in flaky}), 1)
        check_eq("Predefined.the_second_delivery_knew_it_was_the_second",
                 sorted(e["attempt"] for e in flaky[:2]), [1, 2])

        # THE ASSERTION THIS PHASE EXISTS FOR. A celery `self.retry()` or a
        # requeue would have republished within milliseconds; a lease expiry
        # cannot come back before the visibility timeout has run. The floor is
        # the timeout less two seconds of slack for the clock either side of it.
        gap = flaky[1]["at"] - flaky[0]["at"]
        check("Predefined.the_redelivery_waited_for_the_visibility_timeout",
              gap >= VISIBILITY - 2,
              f"the second delivery came {gap:.1f}s after the first, and the "
              f"queue's VisibilityTimeout is {VISIBILITY}s — too soon to be an "
              f"expiry, so something republished it instead")

        empty = wait_until(lambda: drained(client, url), timeout=90, alive=worker.alive)
        check("Predefined.the_queue_drained", bool(empty),
              f"depth {attributes_of(client, url, ('ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'))}")

    check("Predefined.the_worker_shut_down_cleanly", not worker.alive(),
          "the worker was still running after SIGTERM")

    worker_wire = read_wire_log(worker_log)
    check("Predefined.the_worker_sent_no_ListQueues", "ListQueues" not in worker_wire,
          f"operations seen: {sorted(worker_wire)}")
    check("Predefined.the_worker_sent_no_CreateQueue", "CreateQueue" not in worker_wire,
          f"operations seen: {sorted(worker_wire)}")
    check("Predefined.the_worker_deleted_every_message_it_finished",
          sum(worker_wire.get("DeleteMessage", {}).values()) >= 11,
          f"only {sum(worker_wire.get('DeleteMessage', {}).values())} DeleteMessage calls")
    # The wire logs, for the run-wide protocol line. The queue itself is
    # already in `QUEUES` and is torn down from there.
    return [parent_log, worker_log]


# ---------------------------------------------------------------------- main

def report_protocol(wire_logs, async_hub_used):
    wire = read_wire_log(*wire_logs)
    shapes = {}
    total = 0
    for per_op in wire.values():
        for shape, count in per_op.items():
            shapes[shape] = shapes.get(shape, 0) + count
            total += count
    # ONE line per protocol, not one per operation. `shapes` is keyed by the
    # request's exact `(Content-Type, X-Amz-Target)` — and the target names the
    # ACTION (`AmazonSQS.SendMessage`, `AmazonSQS.ListQueues`, ...), so a run
    # that spoke one protocol has as many shapes as it called operations. The
    # counts are folded onto the rendered label, which is the thing the contract
    # asks the suite to report. `shapes` itself is returned unfolded, because the
    # assertion in `main` checks every target individually.
    spoken_counts = {}
    for (content_type, target), count in shapes.items():
        if "json" in content_type and target.startswith("AmazonSQS."):
            spoken = f"AWS JSON 1.0 ({content_type}, X-Amz-Target: AmazonSQS.*)"
        elif "x-www-form-urlencoded" in content_type:
            spoken = f"Query/XML ({content_type})"
        else:
            spoken = f"unrecognized (Content-Type: {content_type!r}, X-Amz-Target: {target!r})"
        spoken_counts[spoken] = spoken_counts.get(spoken, 0) + count
    for spoken, count in sorted(spoken_counts.items(), key=lambda kv: -kv[1]):
        print(f"# protocol spoken: {spoken} — {count} request(s)", flush=True)
    operations = ", ".join(
        f"{op} x{sum(per_op.values())}" for op, per_op in sorted(wire.items())
    )
    print(f"# operations in the client's own botocore debug stream: {operations}",
          flush=True)
    print(f"# total requests observed: {total}", flush=True)
    if async_hub_used:
        print("# note: the predefined phase ran celery's default consumer, whose "
              "ReceiveMessage is built and sent by kombu's async hub and is "
              "therefore NOT in the stream above; the discovered phase ran the "
              "blocking consumer, where it is.", flush=True)
    else:
        print("# note: pycurl is not installed, so both phases ran celery's "
              "blocking consumer and every call is in the stream above.",
              flush=True)
    return shapes


def main():
    print(f"# endpoint {ENDPOINT}  region {REGION}  account {ACCOUNT}  run {RUN}",
          flush=True)
    print(f"# client: celery {_version('celery')} / kombu {_version('kombu')} / "
          f"boto3 {_version('boto3')}, visibility {VISIBILITY}s", flush=True)

    client = sqs_client()
    try:
        client.list_queues()
    except Exception as e:
        print(f"FAIL rig.reachable: {e}")
        print("RESULT: FAIL")
        return 1

    workdir = tempfile.mkdtemp(prefix=f"qsqs-celery-{RUN}-")
    wire_logs = []
    async_hub_used = pycurl_available()
    try:
        for phase in (phase_discovered, phase_predefined):
            try:
                outcome = phase(client, workdir)
            except Exception:
                # One phase blowing up must not cost the run the other one.
                # `smoke_m0.py`'s rule, applied to a bigger unit.
                fail(phase.__name__, "unexpected exception")
                traceback.print_exc()
                continue
            if outcome:
                wire_logs.extend(outcome)

        shapes = report_protocol(wire_logs, async_hub_used)
        check("Protocol.every_observed_request_was_aws_json_1_0",
              bool(shapes) and all(
                  "json" in content_type and target.startswith("AmazonSQS.")
                  for content_type, target in shapes
              ),
              f"shapes seen: {sorted(shapes)}")
    finally:
        for url in QUEUES:
            try:
                client.delete_queue(QueueUrl=url)
            except Exception:
                pass
        if os.environ.get("QSQS_CELERY_KEEP", "") == "1":
            print(f"# worker logs and evidence kept at {workdir}", flush=True)
        else:
            shutil.rmtree(workdir, ignore_errors=True)

    print(f"# {PASSES} passed, {len(FAILURES)} failed", flush=True)
    for name in FAILURES:
        print(f"#   failed: {name}", flush=True)
    print(f"RESULT: {'FAIL' if FAILURES else 'PASS'}", flush=True)
    return 1 if FAILURES else 0


def _version(package):
    try:
        import importlib.metadata as metadata

        return metadata.version(package)
    except Exception:
        return "?"


if __name__ == "__main__":
    sys.exit(main())
