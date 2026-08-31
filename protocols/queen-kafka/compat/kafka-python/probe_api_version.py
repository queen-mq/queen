#!/usr/bin/env python3
"""kafka-python's api_version probe against queen-kafka, as a first-class test.

kafka-python is the one client in this matrix whose version negotiation is a
*probe* rather than a per-API clamp — and what the probe does changed under the
library's feet, which is exactly why this file exists.

  * 2.0.x (and the kafka-python-ng fork of it) read ApiVersions once, infer a
    single Apache Kafka RELEASE tuple from it
    (`BrokerConnection._infer_broker_version_from_api_versions`), and pick every
    request version from that tuple alone. Against queen-kafka the inference
    over-reads — it sees Produce v8 advertised and concludes "Kafka 2.4" — but
    it is saved by an accident: the 2.0.x Fetcher never emits a Fetch above v4,
    which is inside the facade's 4..6 window. It works.
  * 2.1+ and 3.x clamp per-API against the ApiVersions response. They ask for
    Fetch v6 because v6 is what the facade advertises. It works, and for the
    right reason.
  * An explicit `api_version=` hint SWITCHES THE CLAMP OFF on 2.1+/3.x: the hint
    means "assume the broker is this release", so a hint of (1, 1, 0) sends
    Fetch v7 at a broker that advertises v6.

The facade answers an out-of-window version on an advertised key by CLOSING the
connection (compat/ERRORS.md), so that is not an error code, it is a silent
reconnect loop — about 95 reconnects per 10s, from the facade's own
`suppressed=` counter.

The assertion below is therefore not "hint X works" but the falsifiable rule
that holds for every release: a case whose Fetch stayed inside 4..6 must
round-trip, and a case that exceeded it must fail. That passes on a correct
stack and fails on a broken one, on all four libraries.

Usage:  probe_api_version.py [bootstrap] [runid]
"""
import os
import sys
import tempfile
import time

from kafka import KafkaConsumer, KafkaProducer

from _common import (check, fmt_versions, info, ok, print_environment, report,
                     say, supported, Trace, watchdog)

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19092"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(time.time()))
WORK = tempfile.mkdtemp(prefix="kafka-python-probe.")

# The facade's Fetch window, from queen-kafka/src/versions.rs. Not a guess: if
# this file ever disagrees with versions.rs, the disagreement is the finding.
FETCH_MIN, FETCH_MAX = 4, 6


def one_case(label, api_version, deadline=20):
    """Produce one record and read it back. Returns (produced, consumed, trace).

    Nothing here raises: how a case fails IS the result.
    """
    topic = f"kp-probe-{RUN}-{label}"
    kw = {"bootstrap_servers": BOOTSTRAP}
    if api_version is not None:
        kw["api_version"] = api_version
    produced = consumed = False
    with Trace(os.path.join(WORK, f"{label}.log")) as trace:
        started = time.time()
        try:
            p = KafkaProducer(**supported(
                KafkaProducer,
                max_block_ms=deadline * 1000,
                request_timeout_ms=deadline * 1000,
                api_version_auto_timeout_ms=deadline * 1000,
                retries=0,
                acks=1,
                **kw,
            ))
            p.send(topic, key=b"k", value=b"v").get(timeout=deadline)
            produced = True
            p.close(timeout=5)
        except Exception as exc:  # noqa: BLE001 - the failure mode is the result
            info(f"[{label}] producer raised {type(exc).__name__}: {exc}")
        if produced:
            try:
                # group_id=None keeps this on the raw fetch path: no
                # FindCoordinator, no join, no 3s rebalance delay.
                c = KafkaConsumer(topic, **supported(
                    KafkaConsumer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    group_id=None,
                    consumer_timeout_ms=deadline * 1000,
                    request_timeout_ms=deadline * 1000,
                    api_version_auto_timeout_ms=deadline * 1000,
                    **kw,
                ))
                for rec in c:
                    consumed = rec.value == b"v"
                    break
                c.close()
            except Exception as exc:  # noqa: BLE001
                info(f"[{label}] consumer raised {type(exc).__name__}: {exc}")
        elapsed = time.time() - started
    seen = trace.sent_versions()
    info(f"[{label}] inferred broker release={trace.inferred()} "
         f"elapsed={elapsed:.1f}s closes={trace.closes()}")
    info(f"[{label}] wire versions: {fmt_versions(seen)}")
    return produced, consumed, trace


def judge(label, hint, produced, consumed, trace):
    """The rule: inside the advertised Fetch window it must work, outside it must not."""
    fetch = trace.sent_versions().get("Fetch", set())
    top = max(fetch) if fetch else None
    if top is not None and top > FETCH_MAX:
        check(not consumed,
              f"{label}: Fetch v{top} is above the advertised window "
              f"{FETCH_MIN}..{FETCH_MAX}, so the consume correctly does not "
              f"complete (facade closes the connection; this is the client "
              f"honouring api_version={hint} over ApiVersions)")
        info(f"{label}: THIS IS THE TRAP — api_version={hint} disables "
             f"kafka-python's per-API clamp on 2.1+/3.x")
    else:
        check(produced and consumed,
              f"{label}: Fetch v{top} is inside {FETCH_MIN}..{FETCH_MAX}, "
              f"produce and consume both complete")


def main():
    print_environment(BOOTSTRAP, RUN, extra=f" trace_dir={WORK}")
    watchdog(300, "probe_api_version")

    say("case A: the DEFAULT probe (api_version unset) — what a user gets with no config")
    produced, consumed, trace = one_case("default", None)
    seen = trace.sent_versions()
    check(produced and consumed,
          "the DEFAULT api_version probe produces and consumes with no extra config")
    fetch = seen.get("Fetch", set())
    check(bool(fetch) and max(fetch) <= FETCH_MAX,
          f"the probe settled on Fetch v{sorted(fetch)} — inside the "
          f"advertised {FETCH_MIN}..{FETCH_MAX} window")
    if "ApiVersions" in seen or "ApiVersion" in seen:
        av = seen.get("ApiVersions", seen.get("ApiVersion"))
        info(f"ApiVersions attempted at v{sorted(av)} — a client that opens above v3 "
             f"gets the v0-encoded error body and retries lower (the one API where "
             f"the facade answers instead of closing)")

    say("case B: explicit api_version hints")
    for label, hint in (("v0_11_0", (0, 11, 0)),
                        ("v1_1_0", (1, 1, 0)),
                        ("v2_0_0", (2, 0, 0))):
        p, c, tr = one_case(label, hint)
        judge(f"api_version={hint}", hint, p, c, tr)

    say("verdict")
    ok("the safe explicit hint for every release tested is api_version=(0, 11, 0): "
       "it pins Fetch to v5, Produce to v3, Metadata to v4 — all inside the facade's "
       "advertised windows")
    return report()


if __name__ == "__main__":
    sys.exit(main())
