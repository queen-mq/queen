"""Shared plumbing for the kafka-python compat scripts.

Three things every script here needs and none of them is a framework:

  * the `  ok  ` / `  FAIL` / `=== ` output shape the rest of compat/ uses,
  * a capture of kafka-python's own DEBUG stream, so the API versions we report
    are the ones the client actually put on the wire rather than ones we assumed,
  * deadlines, because kafka-python's characteristic failure against a facade
    that closes a connection instead of returning an error code is a silent
    reconnect loop, and a script that waits forever for one reports nothing.
"""
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


def supported(cls, **kw):
    """Drop config keys this kafka-python release does not have.

    The library's config surface moves between releases — 3.0 removed
    `api_version_auto_timeout_ms` — and an unknown key is a hard ValueError, so
    a suite that spans four releases asks the class what it accepts.
    """
    known = cls.DEFAULT_CONFIG
    dropped = tuple(sorted(k for k in kw if k not in known))
    if dropped and dropped not in _ANNOUNCED:
        _ANNOUNCED.add(dropped)  # say it once, not once per client built
        info(f"config keys not present in this release, dropped: {list(dropped)}")
    return {k: v for k, v in kw.items() if k in known}


_ANNOUNCED = set()


def api_version_from_env():
    """QUEEN_KAFKA_PYTHON_API_VERSION='0.11.0' -> (0, 11, 0); unset -> None."""
    raw = os.environ.get("QUEEN_KAFKA_PYTHON_API_VERSION", "").strip()
    if not raw:
        return None
    return tuple(int(p) for p in raw.replace(",", ".").split("."))


def watchdog(seconds, label="suite"):
    """A hang is a result. Nothing in this directory may run forever.

    A daemon thread rather than SIGALRM: kafka-python swallows exceptions inside
    its own poll loops, so the only reliable way to turn a hang into a reported
    outcome is to exit the process from outside it.
    """
    def blow():
        time.sleep(seconds)
        print(f"\n  !!   TIMED OUT: {label} exceeded {seconds}s", flush=True)
        print(f"RESULT: FAIL (1)\n  - {label} hung", flush=True)
        os._exit(124)

    t = threading.Thread(target=blow, daemon=True)
    t.start()
    return t


class Trace:
    """kafka-python's own DEBUG stream, captured to a file.

    BrokerConnection logs every request it sends with the request's repr, which
    is the only honest source for "which API version did this client send".
    Two repr shapes, because the library changed how it names generated request
    classes: <=2.3.x builds one class per version (FetchRequest_v6) and 3.x
    carries the version as a field (FetchRequest(version=6, ...)).
    """

    _RE_OLD = re.compile(r"\b(\w+?)Request_v(\d+)\(")
    _RE_NEW = re.compile(r"\b(\w+?)Request\(version=(\d+)")

    def __init__(self, path):
        self.path = path
        self.handler = logging.FileHandler(path)
        self.handler.setFormatter(logging.Formatter("%(name)s %(message)s"))

    def __enter__(self):
        root = logging.getLogger("kafka")
        root.setLevel(logging.DEBUG)
        root.addHandler(self.handler)
        root.propagate = False
        return self

    def __exit__(self, *a):
        logging.getLogger("kafka").removeHandler(self.handler)
        self.handler.close()

    def text(self):
        try:
            with open(self.path, errors="replace") as fh:
                return fh.read()
        except FileNotFoundError:
            return ""

    def sent_versions(self):
        """{'Fetch': {6}, 'Produce': {8}, ...} — requests that reached the wire."""
        sent = "\n".join(ln for ln in self.text().splitlines()
                         if "Sending request" in ln or re.search(r"Request \d+:", ln))
        seen = {}
        for rx in (self._RE_OLD, self._RE_NEW):
            for name, ver in rx.findall(sent):
                seen.setdefault(name, set()).add(int(ver))
        return seen

    def inferred(self):
        """The Apache Kafka release tuple this client decided it was talking to."""
        for pat in (r"Broker version identified as ([0-9.(), ]+)",
                    r"broker_version=\(?([0-9]+[.,) 0-9]*)",
                    r"Using api_version ([^\s(]+(?:\([^)]*\))?)"):
            m = re.search(pat, self.text())
            if m:
                return m.group(1).strip().rstrip(")").strip()
        return None

    def closes(self):
        """How many times the connection went away under us."""
        return len(re.findall(r"Closing connection|Connection reset|"
                              r"Broker is not v|socket disconnected", self.text()))


def fmt_versions(seen):
    if not seen:
        return "(none reached the wire)"
    return ", ".join(f"{k} v{'/'.join(str(v) for v in sorted(vs))}"
                     for k, vs in sorted(seen.items()))


def print_environment(bootstrap, run, extra=""):
    import kafka
    pkg = "kafka-python-ng" if _is_ng() else "kafka-python"
    print(f"{pkg} {kafka.__version__} on python "
          f"{'.'.join(str(x) for x in sys.version_info[:3])}")
    print(f"bootstrap={bootstrap} run={run}{extra}")
    return kafka.__version__


def _is_ng():
    """kafka-python-ng installs the same `kafka` package; the dist name differs."""
    try:
        from importlib.metadata import distributions
    except ImportError:  # py<3.8, not reachable here but harmless
        return False
    for dist in distributions():
        if (dist.metadata["Name"] or "").lower() == "kafka-python-ng":
            return True
    return False
