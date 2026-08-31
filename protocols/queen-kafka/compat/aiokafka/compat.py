#!/usr/bin/env python
"""aiokafka against queen-kafka: the M6 compatibility bar.

    ./compat.py [bootstrap] [runId]

What this proves, in order:

  1. 512 records over 8 partitions with keys and headers, uncompressed, acks=all
  2. the same payloads through gzip / snappy / lz4 / zstd
  3. a consumer GROUP reads them all back: count, per-partition offset order,
     and byte-exact key / value / header round-trip
  4. strict submit order on one partition survives the round trip
  5. commit, stop, and a NEW consumer in the same group resumes with no loss
  6. produce to a topic that does not exist yet (the auto-create path)
  7. beginning_offsets / end_offsets / seek / position
  8. what happens with enable_idempotence=True, recorded rather than asserted

WHAT IS THE CLIENT'S FAULT AND NOT THE FACADE'S:

  * enable_idempotence=True cannot work here. InitProducerId is an M7 item and
    is not advertised at all (versions.rs lists 14 keys and that is not one of
    them). Section 8 drives it on purpose to record the failure mode, because
    real users will set that flag. It is expected, not a defect.
  * aiokafka picks the partition itself for keyed records (murmur2, the Kafka
    default). Which key lands on which partition is the CLIENT's arithmetic;
    all the facade owes us is that the record comes back off the partition the
    client's own RecordMetadata said it went to. That is what we assert.
  * ApiVersions is sent at v0 by aiokafka regardless of what is advertised.
    That is aiokafka's floor, not a facade cap.

Nothing here starts or stops a stack.
"""

import asyncio
import os
import sys

from _common import (
    FAILURES,
    Trace,
    bad,
    check,
    codecs_available,
    deadline,
    fmt_versions,
    info,
    ok,
    print_environment,
    report,
    say,
    watchdog,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

BOOTSTRAP = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:19092"
RUN = sys.argv[2] if len(sys.argv) > 2 else str(int(__import__("time").time()))
PARTITIONS = int(os.environ.get("QUEEN_KAFKA_PARTITIONS", "8"))
TRACE_PATH = os.environ.get("AIOKAFKA_TRACE", f"/tmp/aiokafka-compat-{RUN}.log")

N = 512
BAR = f"aiok-{RUN}-bar"
ORDER = f"aiok-{RUN}-order"
NEW = f"aiok-{RUN}-autocreate"
SEEKT = f"aiok-{RUN}-seek"
GROUP = f"aiok-{RUN}-g"


# ----------------------------------------------------------------- fixtures
def key_of(i):
    return f"k-{RUN}-{i:04d}".encode()


def value_of(i):
    """Deliberately not printable ASCII: the facade base64-envelopes values and
    we want a byte-exactness claim that a text-only payload could not make."""
    return f"aiokafka-{i:04d}|".encode() + bytes((i * 7 + j) % 256 for j in range(16))


def headers_of(i):
    return [
        ("seq", str(i).encode()),
        ("bin", bytes([0x00, 0xFF, 0xFE, i % 256])),
        ("utf8", "café-ünïcode".encode()),
    ]


def norm_headers(h):
    return tuple((k, bytes(v)) for k, v in (h or ()))


# ------------------------------------------------------------------ helpers
async def partitions_of(consumer, topic, tries=10):
    """Partitions of `topic` on a consumer that has not subscribed to it.

    A PURE aiokafka QUIRK, nothing to do with this facade, and the first thing
    that bites when you port a script over from kafka-python:

      * ``AIOKafkaConsumer.partitions_for_topic`` is SYNCHRONOUS and answers out
        of the client's cached cluster, so it is None for a topic nobody has
        asked about yet;
      * ``await consumer.topics()`` does go to the wire, but it answers from a
        throwaway ``fetch_all_metadata()`` result and does NOT update that
        cache, so calling it first does not help. Verified against this facade:
        ``topics()`` returned all 11 topics while ``partitions_for_topic`` on
        the very next line still returned None.

    ``client.add_topic()`` is the call that adds the topic to the tracked set
    and refreshes the cache, so that is what we use.
    """
    for _ in range(tries):
        parts = consumer.partitions_for_topic(topic)
        if parts:
            return parts
        await consumer._client.add_topic(topic)
        parts = consumer.partitions_for_topic(topic)
        if parts:
            return parts
        await asyncio.sleep(0.3)
    return set()


async def drain(bootstrap, topic, want, timeout=60.0, group=None, from_beginning=True):
    """Read up to `want` records off `topic`.

    group=None assigns the partitions directly, which skips the 3 second group
    formation delay (QUEEN_KAFKA_GROUP_JOIN_DELAY_MS, Kafka's
    group.initial.rebalance.delay.ms) -- worth doing for the sections that are
    not about group semantics.
    """
    kw = dict(bootstrap_servers=bootstrap, enable_auto_commit=False)
    if group:
        kw.update(group_id=group, auto_offset_reset="earliest")
        c = AIOKafkaConsumer(topic, **kw)
    else:
        c = AIOKafkaConsumer(**kw)
    await c.start()
    try:
        if not group:
            tps = [TopicPartition(topic, p) for p in sorted(await partitions_of(c, topic))]
            if not tps:
                bad(f"{topic}: no partitions in metadata, nothing to assign")
                return []
            c.assign(tps)
            if from_beginning:
                await c.seek_to_beginning(*tps)
        got = []
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while len(got) < want and loop.time() < end:
            batch = await c.getmany(timeout_ms=2000, max_records=want - len(got))
            if not batch:
                continue
            for tp, recs in batch.items():
                got.extend(recs)
        return got
    finally:
        try:
            await asyncio.shield(c.stop())
        except Exception as e:  # noqa: BLE001 - a failed teardown must not mask the result
            info(f"consumer stop raised {type(e).__name__}: {e}")


async def produce_batch(bootstrap, topic, indices, compression=None, acks="all"):
    p = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        acks=acks,
        compression_type=compression,
        enable_idempotence=False,  # InitProducerId is M7; see the module docstring
        client_id=f"aiokafka-compat-{RUN}",
    )
    await p.start()
    try:
        futs = [
            await p.send(topic, value_of(i), key=key_of(i), headers=headers_of(i))
            for i in indices
        ]
        return [await f for f in futs]
    finally:
        await p.stop()


# ------------------------------------------------------------------ the bar
async def section_produce(bootstrap):
    say(f"1. produce {N} records to {BAR} with keys + headers, uncompressed, acks=all")
    md = await produce_batch(bootstrap, BAR, range(N))
    check(len(md) == N, f"{len(md)}/{N} records acknowledged")
    parts = sorted({m.partition for m in md})
    check(
        len(parts) >= 4,
        f"spread over {len(parts)} partitions {parts} (bar wants >= 4)",
    )
    check(
        all(m.offset >= 0 for m in md),
        "every acknowledgement carried a real offset",
    )
    produced = {}
    for i, m in zip(range(N), md):
        produced[key_of(i)] = (value_of(i), norm_headers(headers_of(i)), m.partition, m.offset)
    per_part = {}
    for i, m in zip(range(N), md):
        per_part.setdefault(m.partition, []).append(m.offset)
    contiguous = all(
        sorted(v) == list(range(min(v), min(v) + len(v))) for v in per_part.values()
    )
    check(contiguous, "offsets are contiguous within each partition")
    return produced


async def section_codecs(bootstrap):
    say("2. compression codecs")
    avail = codecs_available()
    for name, present in sorted(avail.items()):
        if not present:
            info(f"{name}: codec library not installed, skipped")
            continue
        topic = f"aiok-{RUN}-codec-{name}"
        n = 40
        try:
            md = await produce_batch(bootstrap, topic, range(n), compression=name)
        except Exception as e:  # noqa: BLE001 - we want the message, whatever it is
            bad(f"{name}: produce raised {type(e).__name__}: {e}")
            continue
        if len(md) != n:
            bad(f"{name}: {len(md)}/{n} acknowledged")
            continue
        got = await drain(bootstrap, topic, n, timeout=45)
        exact = len(got) == n and all(
            r.value == value_of(int(r.key.decode().rsplit("-", 1)[1])) for r in got
        )
        check(exact, f"{name}: {len(got)}/{n} records round-tripped byte-exact")


async def section_group_consume(bootstrap, produced):
    say(f"3. consumer group {GROUP} reads {BAR} back")
    got = await drain(bootstrap, BAR, N, timeout=90, group=GROUP)
    if not check(len(got) == N, f"{len(got)}/{N} records consumed"):
        return got
    keys = {r.key for r in got}
    check(len(keys) == N, f"{len(keys)} distinct keys, no duplicates")

    mismatched_value = [r.key for r in got if produced.get(r.key, (None,))[0] != r.value]
    check(not mismatched_value, f"values byte-exact ({len(mismatched_value)} bad)")

    mismatched_hdr = [
        r.key for r in got if produced.get(r.key, (0, None))[1] != norm_headers(r.headers)
    ]
    check(not mismatched_hdr, f"headers byte-exact incl. NUL/0xFF/UTF-8 ({len(mismatched_hdr)} bad)")

    misplaced = [
        r.key
        for r in got
        if (r.partition, r.offset) != (produced.get(r.key, (0, 0, -1, -1))[2],
                                       produced.get(r.key, (0, 0, -1, -1))[3])
    ]
    check(
        not misplaced,
        f"every record came back off the (partition, offset) its ack named ({len(misplaced)} bad)",
    )

    seen_order = {}
    for r in got:
        seen_order.setdefault(r.partition, []).append(r.offset)
    monotonic = all(v == sorted(v) for v in seen_order.values())
    check(monotonic, f"per-partition offset order preserved across {len(seen_order)} partitions")
    return got


async def section_strict_order(bootstrap):
    say("4. strict submit order on a single partition")
    n = 32
    p = AIOKafkaProducer(
        bootstrap_servers=bootstrap, acks="all", enable_idempotence=False
    )
    await p.start()
    sent = []
    try:
        for i in range(n):
            # send_and_wait, one at a time: submit order is unambiguous, so any
            # reordering afterwards is the facade's and not the accumulator's.
            m = await p.send_and_wait(ORDER, value_of(i), key=key_of(i), partition=3)
            sent.append((m.offset, i))
    finally:
        await p.stop()
    check(
        [o for o, _ in sent] == sorted(o for o, _ in sent),
        "offsets were handed out in submit order",
    )
    got = await drain(bootstrap, ORDER, n, timeout=45)
    got = [r for r in got if r.partition == 3]
    replay = [int(r.key.decode().rsplit("-", 1)[1]) for r in sorted(got, key=lambda r: r.offset)]
    check(replay == list(range(n)), f"partition 3 replays in submit order ({len(replay)}/{n})")


async def section_commit_resume(bootstrap, produced):
    say("5. commit, stop, resume in the same group from the committed offset")
    group = f"{GROUP}-resume"
    first = AIOKafkaConsumer(
        BAR,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await first.start()
    seen_a, committed = [], {}
    try:
        loop = asyncio.get_running_loop()
        end = loop.time() + 60
        while len(seen_a) < N // 2 and loop.time() < end:
            batch = await first.getmany(timeout_ms=2000, max_records=N // 2 - len(seen_a))
            for _tp, recs in batch.items():
                seen_a.extend(recs)
        check(len(seen_a) >= N // 4, f"first consumer read {len(seen_a)} records")
        await first.commit()
        for tp in first.assignment():
            committed[tp] = await first.committed(tp)
        ok(f"committed offsets on {len([v for v in committed.values() if v])} partitions")
    finally:
        await first.stop()

    second = AIOKafkaConsumer(
        BAR,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await second.start()
    seen_b = []
    try:
        want = N - len(seen_a)
        loop = asyncio.get_running_loop()
        end = loop.time() + 90
        while len(seen_b) < want and loop.time() < end:
            batch = await second.getmany(timeout_ms=2000, max_records=want - len(seen_b))
            for _tp, recs in batch.items():
                seen_b.extend(recs)
    finally:
        await second.stop()

    union = {r.key for r in seen_a} | {r.key for r in seen_b}
    check(len(union) == N, f"no loss across the handoff: {len(union)}/{N} distinct keys")
    dupes = {r.key for r in seen_a} & {r.key for r in seen_b}
    check(
        not dupes,
        f"a clean sequential handoff redelivered nothing ({len(dupes)} duplicates)",
    )
    below = [
        (r.partition, r.offset)
        for r in seen_b
        if committed.get(TopicPartition(BAR, r.partition)) is not None
        and r.offset < committed[TopicPartition(BAR, r.partition)]
    ]
    check(not below, f"second consumer started at or after every committed offset ({len(below)} before)")


async def section_autocreate(bootstrap):
    say(f"6. produce to {NEW}, which does not exist yet")
    p = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=False)
    await p.start()
    try:
        m = await p.send_and_wait(NEW, b"auto-created", key=b"ac")
        ok(f"first record landed on partition {m.partition} offset {m.offset}")
        parts = await p.partitions_for(NEW)
    finally:
        await p.stop()
    check(
        len(parts) == PARTITIONS,
        f"topic materialised with {len(parts)} partitions (QUEEN_KAFKA_DEFAULT_PARTITIONS={PARTITIONS})",
    )
    got = await drain(bootstrap, NEW, 1, timeout=30)
    check(
        len(got) == 1 and got[0].value == b"auto-created" and got[0].key == b"ac",
        "the auto-created topic round-trips",
    )


async def section_offsets_and_seek(bootstrap):
    say("7. beginning_offsets / end_offsets / seek / position")
    n = 20
    await produce_batch(bootstrap, SEEKT, range(n))
    c = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    await c.start()
    try:
        tps = [TopicPartition(SEEKT, p) for p in sorted(await partitions_of(c, SEEKT))]
        if not check(bool(tps), f"{SEEKT}: metadata carried partitions to assign"):
            return
        c.assign(tps)
        begins = await c.beginning_offsets(tps)
        ends = await c.end_offsets(tps)
        check(all(v == 0 for v in begins.values()), f"beginning_offsets all 0 ({len(begins)} partitions)")
        total = sum(ends.values()) - sum(begins.values())
        check(total == n, f"end_offsets - beginning_offsets == {total}, expected {n}")

        target = max(ends, key=lambda tp: ends[tp] - begins[tp])
        depth = ends[target] - begins[target]
        if depth < 2:
            info(f"deepest partition holds only {depth} records; seek check is thin but still valid")
        mid = begins[target] + depth // 2
        c.seek(target, mid)
        check(await c.position(target) == mid, f"position() reports the seek to {target.partition}@{mid}")
        rest = []
        loop = asyncio.get_running_loop()
        end = loop.time() + 30
        want = ends[target] - mid
        while len(rest) < want and loop.time() < end:
            batch = await c.getmany(timeout_ms=2000, max_records=want - len(rest))
            for tp, recs in batch.items():
                rest.extend(r for r in recs if tp == target)
        check(
            len(rest) == want and rest[0].offset == mid,
            f"seek to {mid} replayed {len(rest)}/{want} records starting at "
            f"{rest[0].offset if rest else 'nothing'}",
        )
        await c.seek_to_end(target)
        check(await c.position(target) == ends[target], "seek_to_end lands on the log end")
        await c.seek_to_beginning(target)
        check(await c.position(target) == begins[target], "seek_to_beginning lands on the log start")
    finally:
        await c.stop()


async def section_idempotence(bootstrap):
    """Not a bar item. Recorded because real users will set this flag."""
    say("8. enable_idempotence=True (expected to fail: InitProducerId is M7)")
    p = AIOKafkaProducer(
        bootstrap_servers=bootstrap, enable_idempotence=True, acks="all"
    )
    try:
        await asyncio.wait_for(p.start(), 25)
        m = await asyncio.wait_for(
            p.send_and_wait(f"aiok-{RUN}-idem", b"x"), 25
        )
        info(f"UNEXPECTED: idempotent produce succeeded at {m.partition}@{m.offset}")
        info("that would mean InitProducerId is now implemented; check versions.rs")
    except asyncio.TimeoutError:
        info("failure mode: start()/send hung until our 25s deadline (no fast failure)")
    except Exception as e:  # noqa: BLE001
        info(f"failure mode: {type(e).__name__}: {str(e)[:300]}")
    finally:
        try:
            await asyncio.wait_for(p.stop(), 10)
        except Exception:  # noqa: BLE001
            pass
    ok("recorded; an idempotent producer failing here is expected, not a defect")


async def main():
    watchdog(int(os.environ.get("AIOKAFKA_SUITE_TIMEOUT", "900")), "compat.py")
    print_environment(BOOTSTRAP, RUN, extra=f" partitions={PARTITIONS}")
    with Trace(TRACE_PATH) as tr:
        produced = await section_produce(BOOTSTRAP)
        await section_codecs(BOOTSTRAP)
        await section_group_consume(BOOTSTRAP, produced)
        await section_strict_order(BOOTSTRAP)
        await section_commit_resume(BOOTSTRAP, produced)
        await section_autocreate(BOOTSTRAP)
        await section_offsets_and_seek(BOOTSTRAP)
        await section_idempotence(BOOTSTRAP)

        say("negotiated API versions, read out of aiokafka's own debug stream")
        seen = tr.sent_versions()
        print(f"  {fmt_versions(seen)}")
        info(f"connection closes seen in the trace: {tr.closes()}")
        info(f"trace: {TRACE_PATH}")
        check(seen.get("Fetch") == {6}, "Fetch stayed at v6, the facade's advertised cap")
        check(
            bool(seen.get("Produce")) and max(seen["Produce"]) <= 9,
            "Produce stayed inside the advertised 3-9 window",
        )
    return report()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
