"""Shared by every Python reader script: the verdict protocol and the exactness
check.

The check is the whole point of this lane. "It opened" is not a result — a
reader that silently drops the second window, or hands back `région-eu` as
`r?gion-eu`, opens the file perfectly well. So every reader is asked for the
same three things and judged on all of them:

* the record count, per queue and per partition;
* the number of NULL payloads (a reader that turns JSON `null` into `""` is
  wrong in a way no count catches);
* the sha256 of the `partition|offset` list **in the order the reader returned
  it** under its own `ORDER BY partition, offset`.

The digest is checked twice on failure, and the two answers mean different
things: if the reader's rows hash correctly after the script re-sorts them, the
CONTENT is right and the ENGINE's ordering (its string collation) differs; if
they do not, the reader lost or invented rows.
"""

import hashlib
import json
import sys

SAMPLES = "/samples"
EXPECTED = "/samples/expected.json"


def load_expected(path=EXPECTED):
    """`path` is a HOST path for the scripts that drive docker from outside and
    the mounted `/samples/expected.json` for the ones that run inside."""
    with open(path, "rb") as f:
        return json.load(f)


def emit(reader, version, fmt, comp, verdict, incantation, detail=""):
    """One machine-readable verdict line. `run.sh` greps for the prefix, so
    nothing else in the output can be mistaken for a result."""
    print(
        "VERDICT "
        + json.dumps(
            {
                "reader": reader,
                "version": version,
                "format": fmt,
                "compression": comp,
                "verdict": verdict,
                "incantation": incantation,
                "detail": detail,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def note(reader, text):
    """An observation that is not a (format, compression) verdict: a hive
    collision, a type surprise, a working alternative. It reaches MATRIX.md's
    notes section."""
    print(
        "NOTE " + json.dumps({"reader": reader, "text": text}, ensure_ascii=False),
        flush=True,
    )


def digest_rows(rows):
    """sha256 over "<partition>|<offset>\\n" per row, in the given order."""
    h = hashlib.sha256()
    for partition, offset in rows:
        h.update(("%s|%d\n" % (partition, int(offset))).encode("utf-8"))
    return h.hexdigest()


def check_queue(expected, queue, rows, null_payloads=None):
    """`rows` is [(partition, offset)] in the order the reader returned them.

    Returns `None` when everything matches, or a short `fail:` reason naming the
    FIRST thing that did not. The order of the checks is deliberate: a count
    mismatch explains a digest mismatch, so it is reported instead of it."""
    want = expected["queues"][queue]
    if len(rows) != want["records"]:
        return "fail:count %d != %d" % (len(rows), want["records"])

    got_counts = {}
    for partition, _ in rows:
        got_counts[partition] = got_counts.get(partition, 0) + 1
    if got_counts != want["partitions"]:
        missing = {
            k: (want["partitions"].get(k), got_counts.get(k))
            for k in set(want["partitions"]) | set(got_counts)
            if want["partitions"].get(k) != got_counts.get(k)
        }
        return "fail:per-partition count %s" % json.dumps(missing, ensure_ascii=False)

    # The reader's own ordering: partitions grouped, offsets increasing inside
    # each. This is what `ORDER BY partition, offset` has to deliver for the
    # "one entity's exact history" query of plan §7 to mean anything.
    seen = set()
    prev_partition, prev_offset = None, None
    for partition, offset in rows:
        offset = int(offset)
        if partition != prev_partition:
            if partition in seen:
                return "fail:order partition %r is not contiguous" % partition
            seen.add(partition)
            prev_partition, prev_offset = partition, None
        if prev_offset is not None and offset <= prev_offset:
            return "fail:order %r offset %d after %d" % (partition, offset, prev_offset)
        prev_offset = offset

    got = digest_rows(rows)
    if got != want["digest"]:
        resorted = digest_rows(sorted(rows, key=lambda r: (r[0], int(r[1]))))
        if resorted == want["digest"]:
            return "fail:collation digest matches only after a client-side re-sort"
        return "fail:digest %s != %s" % (got[:16], want["digest"][:16])

    if null_payloads is not None and int(null_payloads) != want["nullPayloads"]:
        return "fail:null-payloads %d != %d" % (int(null_payloads), want["nullPayloads"])
    return None


def check_spot(expected, queue, got_rows):
    """`got_rows` is a dict keyed by (partition, offset) with whatever fields the
    reader could decode; only the fields present are compared."""
    want = {(r["partition"], r["offset"]): r for r in expected["queues"][queue]["spot"]}
    for key, w in want.items():
        g = got_rows.get(key)
        if g is None:
            return "fail:spot %r missing" % (key,)
        for field, wv in w.items():
            if field not in g:
                continue
            gv = g[field]
            if field == "offset" or field == "amount" or field == "tsMicros":
                gv = None if gv is None else int(gv)
                wv = None if wv is None else int(wv)
            if gv != wv:
                return "fail:spot %r %s %r != %r" % (key, field, gv, wv)
    return None


def die(reader, message):
    print("ERROR %s: %s" % (reader, message), file=sys.stderr, flush=True)
    sys.exit(1)
