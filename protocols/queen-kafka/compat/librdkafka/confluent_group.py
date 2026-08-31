#!/usr/bin/env python3
"""The confluent-kafka (librdkafka) half of the M6 client matrix.

kcat proves the produce/consume path; this proves the GROUP path with explicit
commits, which is what every Confluent client (Python, .NET, Go, node-rdkafka)
does for real work:

  produce -> group reads -> commit -> read the commit back over OffsetFetch ->
  a second consumer of the same group starts where the first stopped.

Usage:  confluent_group.py [bootstrap] [runid]

Every step prints what it saw. librdkafka's own `debug=cgrp,protocol` trace goes
to <workdir>/librdkafka.log, and the request versions it negotiated are printed
from it at the end — negotiated, not assumed.
"""
import logging
import os
import sys
import tempfile
import threading
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19092"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(time.time()))
TOPIC = f"ck-{RUN}"
GROUP = f"ck-g-{RUN}"
PARTITIONS = 8

WORK = tempfile.mkdtemp(prefix="confluent-compat.")
TRACE_PATH = os.path.join(WORK, "librdkafka.log")
# librdkafka's debug stream goes through a stdlib logger into a file: the
# `logger` client property takes a logging.Logger, and this keeps the trace off
# the terminal while leaving it readable afterwards.
TRACE = logging.getLogger("librdkafka")
TRACE.setLevel(logging.DEBUG)
_handler = logging.FileHandler(TRACE_PATH)
_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
TRACE.addHandler(_handler)
TRACE.propagate = False

failures = []


def ok(msg):
    print(f"  ok   {msg}")


def bad(msg):
    print(f"  FAIL {msg}")
    failures.append(msg)


def check(cond, msg):
    ok(msg) if cond else bad(msg)


def info(msg):
    print(f"  ..   {msg}")


def say(msg):
    print(f"\n=== {msg}")


def common(debug):
    return {
        "bootstrap.servers": BOOTSTRAP,
        "debug": debug,
        "logger": TRACE,
        # librdkafka's own logs go through the callback above; nothing of this
        # ends up on the terminal unless this script prints it.
        "log_level": 7,
    }


# ------------------------------------------------------------------- produce
def produce(topic, keys, first_offset_must_be=None):
    p = Producer({**common("protocol"), "acks": "all", "enable.idempotence": False})
    acked = {}
    errors = []

    def dr(err, msg):
        if err is not None:
            errors.append(str(err))
        else:
            acked[msg.key().decode()] = (msg.partition(), msg.offset())

    for i, k in enumerate(keys):
        p.produce(
            topic,
            key=k,
            value=f"val-{k}",
            partition=i % PARTITIONS,
            headers=[("trace", k.encode()), ("empty", b""), ("nullv", None)],
            callback=dr,
        )
    remaining = p.flush(60)
    check(remaining == 0, f"every produce was acknowledged ({remaining} left in the queue)")
    check(not errors, f"no delivery error ({errors[:3]})")
    check(len(acked) == len(keys), f"{len(acked)}/{len(keys)} delivery reports carry an offset")
    check(all(off >= 0 for _, off in acked.values()),
          "every delivery report carries a real (non -1001) offset")
    sample = sorted(acked.items())[:3]
    info(f"delivery reports (key -> partition, offset): {sample}")
    if first_offset_must_be is not None:
        got = sorted(off for _, off in acked.values())[0]
        check(got == first_offset_must_be, f"the first offset of a fresh topic is {got}")
    return acked


# ------------------------------------------------------------------- consume
def drain(group, topic, want, commit, extra_wait=0.0, conf=None):
    """Read `want` records with a group member; optionally commit each batch."""
    c = Consumer({
        **common("cgrp,protocol,topic"),
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
        **(conf or {}),
    })
    assigned = []

    def on_assign(_c, parts):
        assigned.extend(p.partition for p in parts)

    c.subscribe([topic], on_assign=on_assign)
    seen = []
    deadline = time.time() + 120
    while len(seen) < want and time.time() < deadline:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            raise KafkaException(m.error())
        seen.append((m.partition(), m.offset(), m.key().decode(), m.headers()))
        if commit:
            c.commit(message=m, asynchronous=False)
    if extra_wait:
        # Deliberately keep polling past the expected count: a facade that
        # redelivered committed records would show up here and nowhere else.
        stop = time.time() + extra_wait
        while time.time() < stop:
            m = c.poll(0.5)
            if m is not None and not m.error():
                seen.append((m.partition(), m.offset(), m.key().decode(), m.headers()))
    return c, seen, sorted(assigned)


def two_members():
    """Two members of one group under COOPERATIVE-STICKY.

    librdkafka's incremental protocol is a different conversation with the
    coordinator from the eager one: members keep the partitions they are not
    losing and the assignor's own bytes ride inside JoinGroup/SyncGroup. It is
    also what every Confluent client is steered towards, so it belongs here.
    """
    say("two members of one group, cooperative-sticky")
    topic = f"{TOPIC}-two"
    group = f"{GROUP}-two"
    produce(topic, [f"c{i}" for i in range(40)])

    state = {"a": {"assigned": set(), "read": []}, "b": {"assigned": set(), "read": []}}
    stop = threading.Event()

    def member(name):
        c = Consumer({
            **common("cgrp"),
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "partition.assignment.strategy": "cooperative-sticky",
        })

        def on_assign(_c, parts):
            state[name]["assigned"].update(p.partition for p in parts)

        c.subscribe([topic], on_assign=on_assign)
        while not stop.is_set():
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            state[name]["read"].append((m.partition(), m.key().decode()))
        c.close()

    threads = [threading.Thread(target=member, args=(n,), daemon=True) for n in ("a", "b")]
    for t in threads:
        t.start()
    deadline = time.time() + 150
    while time.time() < deadline:
        if len(state["a"]["read"]) + len(state["b"]["read"]) >= 40:
            break
        time.sleep(0.5)
    time.sleep(3)  # a last window in which a duplicate could still show up
    stop.set()
    for t in threads:
        t.join(30)

    ra, rb = state["a"]["read"], state["b"]["read"]
    keys = [k for _, k in ra + rb]
    info(f"member A read {len(ra)} records from partitions "
         f"{sorted({p for p, _ in ra})}, assigned {sorted(state['a']['assigned'])}")
    info(f"member B read {len(rb)} records from partitions "
         f"{sorted({p for p, _ in rb})}, assigned {sorted(state['b']['assigned'])}")
    check(len(keys) == 40, f"the two members read 40 records between them ({len(keys)})")
    check(len(set(keys)) == 40, f"no record was delivered twice ({len(keys) - len(set(keys))} dupes)")
    check(bool(ra) and bool(rb), "both members did work")
    overlap = {p for p, _ in ra} & {p for p, _ in rb}
    check(not overlap, f"no partition was read by both members ({sorted(overlap)})")


def main():
    say(f"produce 16 records to {TOPIC} (fresh topic, auto-created)")
    first = produce(TOPIC, [f"a{i}" for i in range(16)], first_offset_must_be=0)

    say("a group member reads them and commits each one synchronously")
    c1, seen, assigned = drain(GROUP, TOPIC, 16, commit=True)
    check(len(seen) == 16, f"the member read all 16 records ({len(seen)})")
    check(sorted({p for p, _, _, _ in seen}) == list(range(8)),
          "every partition was read")
    check(assigned == list(range(8)), f"the member was assigned all 8 partitions ({assigned})")
    hdrs = seen[0][3]
    info(f"headers on the first record: {hdrs}")
    check(hdrs == [("trace", seen[0][2].encode()), ("empty", b""), ("nullv", None)],
          "headers survive in order, with empty and null values kept distinct")

    say("read the commits back over OffsetFetch, from the SAME consumer")
    tps = [TopicPartition(TOPIC, p) for p in range(PARTITIONS)]
    committed = c1.committed(tps, timeout=30)
    info("committed: " + ", ".join(f"p{t.partition}={t.offset}" for t in committed))
    check(all(t.offset > 0 for t in committed),
          "every partition has a committed offset > 0")
    check(sum(t.offset for t in committed) == 16,
          f"the committed offsets sum to the 16 records read "
          f"({sum(t.offset for t in committed)})")

    say("watermarks (ListOffsets)")
    lo, hi = c1.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    info(f"partition 0 watermarks: low={lo} high={hi}")
    check(lo == 0 and hi == 2, f"partition 0 is [0, 2) after 2 records (got [{lo}, {hi}))")
    c1.close()
    ok("close() returned (LeaveGroup)")

    say("a SECOND consumer of the same group must not replay the committed records")
    produce(TOPIC, [f"b{i}" for i in range(8)])
    c2, seen2, assigned2 = drain(GROUP, TOPIC, 8, commit=False, extra_wait=5)
    keys2 = [k for _, _, k, _ in seen2]
    info(f"the second consumer read {len(keys2)}: {keys2}")
    replayed = [k for k in keys2 if k.startswith("a")]
    check(not replayed, f"nothing already committed was replayed ({len(replayed)} old records)")
    check(len(keys2) == 8, f"exactly the 8 new records ({len(keys2)})")
    c2.close()

    say("a consumer of a group that never committed applies auto.offset.reset")
    c3, seen3, _ = drain(f"{GROUP}-fresh", TOPIC, 24, commit=False)
    check(len(seen3) == 24, f"a fresh group starts at earliest and reads all 24 ({len(seen3)})")
    c3.close()

    two_members()

    for h in TRACE.handlers:
        h.flush()
    say("request versions librdkafka negotiated")
    versions = set()
    with open(TRACE_PATH) as fh:
        trace = fh.read()
    for line in trace.splitlines():
        if "Sent " in line and "Request (v" in line:
            frag = line.split("Sent ", 1)[1]
            versions.add(frag.split(",")[0].strip())
    for v in sorted(versions):
        print(f"  {v}")

    say("errors librdkafka reported")
    for line in trace.splitlines():
        low = line.lower()
        if ("error" in low or "fail" in low) and "debug" not in low:
            print(f"  {line[:220]}")

    print(f"\ntrace: {TRACE_PATH}")
    print(f"RESULT: {'PASS' if not failures else 'FAIL (' + str(len(failures)) + ')'}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
