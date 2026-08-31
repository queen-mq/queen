#!/usr/bin/env python3
"""The kafka-python row of the M6 client matrix, against queen-kafka.

kafka-python is a pure-Python client: no librdkafka underneath, its own
protocol encoders, its own group coordinator, its own partitioner. Whatever
kcat and confluent-kafka prove about librdkafka says nothing about it, which is
why it gets its own suite — and it is the client with the largest installed
base in the Python world, because it is what `pip install kafka-python` has
meant since 2016.

What this proves, in order:

  1. 512 records across 8 partitions with keys and headers, acks=all,
     uncompressed — and the same again under every compression codec this
     install can load (gzip is stdlib; lz4/zstd/snappy need their packages, and
     a codec the install lacks is REPORTED, not asserted).
  2. A consumer GROUP reads them all back: exact count, per-partition offset
     and payload ORDER, and byte-exact key / value / header round-trip.
  3. Commit, close, and a second consumer in the same group resumes from the
     committed offset rather than from the beginning.
  4. Producing to a topic that does not exist yet auto-creates it, at
     QUEEN_KAFKA_DEFAULT_PARTITIONS wide.
  5. beginning_offsets / end_offsets / seek / position on an assigned
     partition, plus what `offsets_for_times` answers (the facade returns -1
     for a concrete timestamp by design — PLAN_QUEEN_KAFKA.md STATUS).

Behaviours that are the CLIENT's, not the facade's, and are recorded rather
than filed:

  * kafka-python 2.0.x infers an Apache Kafka RELEASE from ApiVersions instead
    of clamping per API (see probe_api_version.py). It survives here only
    because its Fetcher never emits above Fetch v4.
  * An explicit `api_version=` hint above (0, 11, 0) turns the per-API clamp
    off on 2.1+/3.x and walks into the facade's Fetch v6 cap.
  * kafka-python has no InitProducerId path unless you ask for transactions, so
    unlike the Java producer it does not need idempotence turned off — there is
    nothing to turn off. `transactional_id` would need INIT_PRODUCER_ID and is
    not exercised.

Usage:  compat.py [bootstrap] [runid]
Env:    QUEEN_KAFKA_PYTHON_API_VERSION=0.11.0  pins an explicit api_version hint
        QUEEN_KAFKA_PARTITIONS=8               what the facade was booted with
"""
import os
import sys
import tempfile
import time
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from _common import (api_version_from_env, bad, check, fmt_versions, info, ok,
                     print_environment, report, say, supported, Trace, watchdog)

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19092"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(time.time()))
PARTITIONS = int(os.environ.get("QUEEN_KAFKA_PARTITIONS", "8"))
API_VERSION = api_version_from_env()

WORK = tempfile.mkdtemp(prefix="kafka-python-compat.")
TRACE = Trace(os.path.join(WORK, "kafka-python.log"))

TOPIC = f"kp-{RUN}"
GROUP = f"kp-g-{RUN}"
COUNT = 512
HEADERS = [("trace", b"abc"), ("empty", b""), ("bin", bytes(range(8)))]


def cfg(**kw):
    if API_VERSION is not None:
        kw["api_version"] = API_VERSION
    kw["bootstrap_servers"] = BOOTSTRAP
    return kw


def producer(**kw):
    return KafkaProducer(**supported(KafkaProducer, **cfg(
        acks="all",
        retries=0,
        max_block_ms=30000,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=20000,
        **kw,
    )))


def consumer(*topics, **kw):
    return KafkaConsumer(*topics, **supported(KafkaConsumer, **cfg(
        request_timeout_ms=40000,
        api_version_auto_timeout_ms=20000,
        **kw,
    )))


def payload(i):
    return f"kp-{RUN}-value-{i:04d}".encode()


def key(i):
    return f"key-{i % 16:02d}".encode()


def drain(c, expect, budget=90):
    """Poll until `expect` records or the budget runs out. A hang is a result."""
    got = []
    deadline = time.time() + budget
    idle = 0
    while len(got) < expect and time.time() < deadline:
        batch = c.poll(timeout_ms=1000, max_records=500)
        if not batch:
            idle += 1
            continue
        idle = 0
        for _tp, recs in batch.items():
            got.extend(recs)
    return got


# --------------------------------------------------------------- 1. produce
def produce_main():
    say(f"1. produce {COUNT} records over {PARTITIONS} partitions, keys + headers, acks=all")
    p = producer()
    parts = p.partitions_for(TOPIC)
    check(parts is not None and len(parts) == PARTITIONS,
          f"the topic auto-created at {len(parts) if parts else 0} partitions "
          f"(QUEEN_KAFKA_DEFAULT_PARTITIONS={PARTITIONS})")
    futures = []
    for i in range(COUNT):
        futures.append(p.send(TOPIC, key=key(i), value=payload(i),
                              headers=HEADERS, partition=i % PARTITIONS))
    p.flush(timeout=60)
    metas = []
    err = None
    for f in futures:
        try:
            metas.append(f.get(timeout=30))
        except Exception as exc:  # noqa: BLE001
            err = err or exc
    check(err is None and len(metas) == COUNT,
          f"all {COUNT} sends acknowledged with acks=all"
          + (f" (first error: {type(err).__name__}: {err})" if err else ""))
    by_part = defaultdict(list)
    for m in metas:
        by_part[m.partition].append(m.offset)
    check(len(by_part) >= 4,
          f"records landed on {len(by_part)} distinct partitions (bar: >= 4)")
    monotonic = all(offs == sorted(offs) for offs in by_part.values())
    check(monotonic, "the offsets the broker assigned rise monotonically within each partition")
    info("per-partition first/last offset: " + ", ".join(
        f"p{k}={v[0]}..{v[-1]}" for k, v in sorted(by_part.items())))
    p.close(timeout=10)
    return by_part


def produce_keyed_partitioner():
    say("1b. the default (murmur2) partitioner: one key always lands on one partition")
    topic = f"{TOPIC}-keyed"
    p = producer()
    seen = defaultdict(set)
    futures = [(key(i), p.send(topic, key=key(i), value=payload(i))) for i in range(128)]
    p.flush(timeout=60)
    for k, f in futures:
        seen[k].add(f.get(timeout=30).partition)
    p.close(timeout=10)
    check(all(len(v) == 1 for v in seen.values()),
          f"each of the {len(seen)} keys mapped to exactly one partition")
    check(len({next(iter(v)) for v in seen.values()}) >= 4,
          f"the keys spread over {len({next(iter(v)) for v in seen.values()})} partitions")


# ----------------------------------------------------------- 2. compression
def produce_compressed():
    say("2. compression codecs")
    results = {}
    for codec in ("gzip", "lz4", "snappy", "zstd"):
        topic = f"{TOPIC}-{codec}"
        try:
            p = producer(compression_type=codec)
        except Exception as exc:  # noqa: BLE001
            info(f"{codec}: not available in this install ({type(exc).__name__}: {exc}) "
                 f"- reported, not asserted")
            results[codec] = None
            continue
        try:
            futures = [p.send(topic, key=key(i), value=payload(i), headers=HEADERS,
                              partition=i % PARTITIONS) for i in range(100)]
            p.flush(timeout=60)
            for f in futures:
                f.get(timeout=30)
            p.close(timeout=10)
        except Exception as exc:  # noqa: BLE001
            bad(f"{codec}: produce failed with {type(exc).__name__}: {exc}")
            results[codec] = False
            continue
        c = consumer(topic, group_id=None, auto_offset_reset="earliest",
                     enable_auto_commit=False, consumer_timeout_ms=30000)
        got = drain(c, 100, budget=60)
        c.close()
        exact = (len(got) == 100
                 and {r.value for r in got} == {payload(i) for i in range(100)})
        check(exact, f"{codec}: 100 records produced compressed and read back byte-exact "
                     f"(got {len(got)})")
        results[codec] = exact
    missing = [k for k, v in results.items() if v is None]
    if missing:
        info(f"codecs this install cannot do: {', '.join(missing)}")
    check(any(v for v in results.values()),
          "at least one compression codec round-trips")
    return results


# ------------------------------------------------------- 3. group consume
def consume_group(by_part):
    say(f"3. consumer group {GROUP}: count, per-partition order, byte-exact round trip")
    started = time.time()
    c = consumer(TOPIC, group_id=GROUP, auto_offset_reset="earliest",
                 enable_auto_commit=False, consumer_timeout_ms=60000,
                 max_poll_records=200)
    got = drain(c, COUNT, budget=120)
    info(f"group formed and drained in {time.time() - started:.1f}s "
         f"(a join costs QUEEN_KAFKA_GROUP_JOIN_DELAY_MS, 3s by default)")
    check(len(got) == COUNT, f"the group read exactly {COUNT} records (got {len(got)})")

    values = {r.value for r in got}
    check(values == {payload(i) for i in range(COUNT)},
          "every value came back byte-exact, no loss and no duplicates")

    keys_ok = all(r.key == key(int(r.value.decode().rsplit("-", 1)[1])) for r in got)
    check(keys_ok, "every key came back byte-exact and paired with its own value")

    ordered = True
    seq_ordered = True
    for tp, recs in _by_tp(got).items():
        offs = [r.offset for r in recs]
        if offs != sorted(offs):
            ordered = False
        seqs = [int(r.value.decode().rsplit("-", 1)[1]) for r in recs]
        if seqs != sorted(seqs):
            seq_ordered = False
    check(ordered, "offsets arrive in ascending order within every partition")
    check(seq_ordered, "produce order is preserved within every partition "
                       "(the sequence number in each payload never goes backwards)")

    check(len(_by_tp(got)) == len(by_part),
          f"the same {len(by_part)} partitions that were produced to were consumed from")

    # keyed by (partition, offset): keying by offset alone would silently
    # collapse 512 records into 64 and check almost nothing.
    hdr = {(r.partition, r.offset): r.headers for r in got}
    sample = next(iter(hdr.values()))
    check([(k, v) for k, v in sample] == HEADERS,
          f"headers round-trip byte-exact including an EMPTY value and binary bytes: "
          f"{sample!r}")
    check(all([(k, v) for k, v in h] == HEADERS for h in hdr.values()),
          f"all {len(hdr)} records carry the identical header list, in order")

    ts_ok = all(r.timestamp and r.timestamp > 0 for r in got)
    check(ts_ok, "every record carries a positive timestamp")
    c.commit()
    committed = c.committed(TopicPartition(TOPIC, 0))
    check(committed is not None and committed > 0,
          f"commit() then committed() reads the offset back over OffsetFetch "
          f"(p0 committed at {committed})")
    c.close()
    return got


def _by_tp(records):
    out = defaultdict(list)
    for r in records:
        out[(r.topic, r.partition)].append(r)
    return out


# ---------------------------------------------- 4. resume from the commit
def resume_after_commit():
    say("4. a second consumer in the same group resumes from the committed offset")
    p = producer()
    extra = 32
    for i in range(extra):
        p.send(TOPIC, key=b"tail", value=f"tail-{RUN}-{i:02d}".encode(),
               partition=i % PARTITIONS)
    p.flush(timeout=60)
    p.close(timeout=10)
    info(f"produced {extra} more records after the commit")

    started = time.time()
    c2 = consumer(TOPIC, group_id=GROUP, auto_offset_reset="earliest",
                  enable_auto_commit=False, consumer_timeout_ms=45000,
                  max_poll_records=200)
    got = drain(c2, extra, budget=90)
    info(f"second member of {GROUP} joined and drained in {time.time() - started:.1f}s")
    tails = [r for r in got if r.value.startswith(b"tail-")]
    olds = [r for r in got if not r.value.startswith(b"tail-")]
    check(len(tails) == extra,
          f"the new consumer got all {extra} records written after the commit "
          f"(got {len(tails)})")
    check(not olds,
          f"and re-read NONE of the {COUNT} already-committed ones "
          f"(re-read {len(olds)}) - the commit was honoured")
    c2.commit()
    c2.close()


# ------------------------------------------------------- 5. auto-create
def auto_create():
    say("5. producing to a topic that does not exist yet")
    fresh = f"{TOPIC}-fresh"
    p = producer()
    meta = p.send(fresh, key=b"k", value=b"first").get(timeout=30)
    check(meta.offset == 0,
          f"the very first record on a brand-new topic got offset 0 (got {meta.offset})")
    parts = p.partitions_for(fresh)
    check(parts is not None and len(parts) == PARTITIONS,
          f"the new topic came up {len(parts) if parts else 0} partitions wide")
    p.close(timeout=10)
    c = consumer(fresh, group_id=None, auto_offset_reset="earliest",
                 enable_auto_commit=False, consumer_timeout_ms=30000)
    got = drain(c, 1, budget=40)
    c.close()
    check(len(got) == 1 and got[0].value == b"first",
          "and the record is readable from the auto-created topic")


# ------------------------------------------------ 6. offsets and seeking
def offsets_and_seek():
    say("6. beginning_offsets / end_offsets / seek / position")
    tp = TopicPartition(TOPIC, 0)
    c = consumer(group_id=None, enable_auto_commit=False, consumer_timeout_ms=30000)
    c.assign([tp])
    begin = c.beginning_offsets([tp])[tp]
    end = c.end_offsets([tp])[tp]
    check(begin == 0, f"beginning_offsets is {begin} (ListOffsets timestamp -2)")
    check(end > begin, f"end_offsets is {end}, above the beginning (ListOffsets timestamp -1)")

    c.seek(tp, begin)
    check(c.position(tp) == begin, f"seek to the beginning, position() reads back {begin}")
    first = drain(c, 1, budget=40)
    check(len(first) >= 1 and first[0].offset == begin,
          f"and the next record read is offset {first[0].offset if first else 'none'}")

    mid = begin + (end - begin) // 2
    c.seek(tp, mid)
    check(c.position(tp) == mid, f"seek to the middle, position() reads back {mid}")
    got = drain(c, 1, budget=40)
    check(len(got) >= 1 and got[0].offset == mid,
          f"and the next record read is exactly offset {mid} "
          f"(got {got[0].offset if got else 'none'})")

    c.seek_to_end(tp)
    check(c.position(tp) == end, f"seek_to_end() lands on {end}, the same as end_offsets")

    # PLAN_QUEEN_KAFKA.md STATUS: "a concrete ListOffsets timestamp answers -1
    # with no error". kafka-python maps that -1 to a None entry, so this is
    # recorded, not asserted as a defect.
    try:
        answer = c.offsets_for_times({tp: int(time.time() * 1000) - 3600_000})
        info(f"offsets_for_times(t-1h) -> {answer} "
             f"(the facade answers a concrete timestamp with -1 by design; "
             f"kafka-python renders that as None, and does NOT raise)")
        check(True, "offsets_for_times returns cleanly rather than raising or hanging")
    except Exception as exc:  # noqa: BLE001
        bad(f"offsets_for_times raised {type(exc).__name__}: {exc}")
    c.close()


def main():
    print_environment(BOOTSTRAP, RUN,
                      extra=f" partitions={PARTITIONS} api_version={API_VERSION or 'PROBE'}"
                            f" trace={TRACE.path}")
    watchdog(900, "compat.py")
    with TRACE:
        by_part = produce_main()
        produce_keyed_partitioner()
        produce_compressed()
        consume_group(by_part)
        resume_after_commit()
        auto_create()
        offsets_and_seek()

    say("API versions this client NEGOTIATED (read out of its own debug stream)")
    info(f"inferred broker release: {TRACE.inferred()}")
    info(fmt_versions(TRACE.sent_versions()))
    fetch = TRACE.sent_versions().get("Fetch", set())
    check(bool(fetch) and max(fetch) <= 6,
          f"every Fetch stayed inside the facade's advertised 4..6 window "
          f"(sent v{sorted(fetch)})")
    return report()


if __name__ == "__main__":
    sys.exit(main())
