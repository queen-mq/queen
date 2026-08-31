"""Shared plumbing for the aiokafka compat scripts.

Three things every script here needs, and none of them is a framework:

  * the ``  ok  `` / ``  FAIL`` / ``=== `` output shape the rest of compat/ uses,
  * a capture of aiokafka's own DEBUG stream, so the API versions we report are
    the ones this client actually put on the wire rather than ones we assumed,
  * deadlines, because the characteristic failure of any kafka-python-lineage
    client against a facade that CLOSES a connection instead of returning an
    error code is a silent reconnect loop, and a script that waits forever for
    one reports nothing. A hang is a result.

aiokafka is asyncio, so the deadline story is nicer than kafka-python's: every
await can carry ``asyncio.wait_for``. The thread watchdog stays anyway, because
a reconnect loop lives inside aiokafka's own background tasks and a cancelled
``wait_for`` does not necessarily stop them.
"""

import asyncio
import logging
import os
import re
import sys
import threading
import time

FAILURES = []


def ok(msg):
    print(f"  ok   {msg}", flush=True)


def bad(msg):
    print(f"  FAIL {msg}", flush=True)
    FAILURES.append(msg)


def check(cond, msg):
    ok(msg) if cond else bad(msg)
    return bool(cond)


def info(msg):
    print(f"  ..   {msg}", flush=True)


def say(msg):
    print(f"\n=== {msg}", flush=True)


def report():
    print()
    if FAILURES:
        print(f"RESULT: FAIL ({len(FAILURES)})")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("RESULT: PASS")
    return 0


def watchdog(seconds, label="suite"):
    """Nothing in this directory may run forever.

    A daemon thread rather than SIGALRM or a supervising task: aiokafka retries
    inside background tasks it owns, and the only reliable way to turn a wedged
    event loop into a reported outcome is to exit the process from outside it.
    """

    def blow():
        time.sleep(seconds)
        print(f"\n  !!   TIMED OUT: {label} exceeded {seconds}s", flush=True)
        print(f"RESULT: FAIL (1)\n  - {label} hung", flush=True)
        os._exit(124)

    t = threading.Thread(target=blow, daemon=True)
    t.start()
    return t


async def deadline(coro, seconds, what):
    """await with a deadline; a timeout is a FAIL line, not an exception."""
    try:
        return await asyncio.wait_for(coro, seconds)
    except asyncio.TimeoutError:
        bad(f"{what}: timed out after {seconds}s")
        return None


class Trace:
    """aiokafka's own DEBUG stream, captured to a file.

    ``AIOKafkaConnection.send`` logs every request it writes with the request
    struct's repr (aiokafka/conn.py:430), and aiokafka names one class per
    version -- ``FetchRequest_v6`` -- so the repr is the only honest source for
    "which API version did this client actually send". We never assume.

    Since 0.13 the ``api_version`` parameter is deprecated and a no-op:
    ``request.prepare(self._versions)`` (conn.py:414) clamps every request
    against the per-API window read from ApiVersions. That is exactly the
    behaviour this facade wants, and it is why aiokafka needs no version hint
    where older kafka-python releases did.
    """

    _RE = re.compile(r"\b(\w+?)Request_v(\d+)\b")

    def __init__(self, path):
        self.path = path
        self.handler = logging.FileHandler(path, mode="w")
        self.handler.setFormatter(logging.Formatter("%(name)s %(message)s"))

    def __enter__(self):
        root = logging.getLogger("aiokafka")
        root.setLevel(logging.DEBUG)
        root.addHandler(self.handler)
        root.propagate = False
        return self

    def __exit__(self, *a):
        logging.getLogger("aiokafka").removeHandler(self.handler)
        self.handler.close()

    def text(self):
        try:
            with open(self.path, errors="replace") as fh:
                return fh.read()
        except FileNotFoundError:
            return ""

    def sent_versions(self):
        """{'Fetch': {6}, 'Produce': {7}, ...} -- requests that reached the wire."""
        seen = {}
        for line in self.text().splitlines():
            if "Request to " not in line:
                continue
            for name, ver in self._RE.findall(line):
                seen.setdefault(name, set()).add(int(ver))
        return seen

    def closes(self):
        """How many times the connection went away under us.

        An out-of-window version on an ADVERTISED key is answered by this facade
        by CLOSING the connection rather than with an error code, so this
        counter is the tell for a version-negotiation problem.
        """
        return len(re.findall(r"Closing connection", self.text()))


def fmt_versions(seen):
    if not seen:
        return "(none reached the wire)"
    return ", ".join(
        f"{k} v{'/'.join(str(v) for v in sorted(vs))}" for k, vs in sorted(seen.items())
    )


def print_environment(bootstrap, run, extra=""):
    import aiokafka

    print(
        f"aiokafka {aiokafka.__version__} on python "
        f"{'.'.join(str(x) for x in sys.version_info[:3])}"
    )
    try:
        import cramjam

        print(f"cramjam {cramjam.__version__} (gzip/snappy/lz4/zstd codecs)")
    except Exception:
        print("cramjam not installed: codec coverage will be partial")
    print(f"bootstrap={bootstrap} run={run}{extra}")
    return aiokafka.__version__


def codecs_available():
    from aiokafka.codec import has_gzip, has_lz4, has_snappy, has_zstd

    return {
        "gzip": has_gzip(),
        "snappy": has_snappy(),
        "lz4": has_lz4(),
        "zstd": has_zstd(),
    }
