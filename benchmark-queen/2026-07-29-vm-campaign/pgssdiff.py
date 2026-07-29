#!/usr/bin/env python3
"""pgssdiff.py <outdir> <run-id> [--top N]

Diff the pg_stat_statements snapshots taken around one run and bucket the calls
into push / pop / ack / lease+watermark / maintenance / other, so
commits-per-delivered-message can be ATTRIBUTED instead of inferred.

Every broker call runs its procedure in its own transaction, so calls to the
top-level procedures are the commit sources; the nested statements inside a
procedure share their caller's transaction and are reported separately.
"""
import csv, json, os, re, sys

BUCKETS = [
    ("push",       re.compile(r"log_push|seg_push|queen\.push", re.I)),
    ("pop",        re.compile(r"log_pop|seg_pop|pop_unified", re.I)),
    ("ack",        re.compile(r"log_ack|seg_ack|ack_batch|log_txn", re.I)),
    ("lease/wm",   re.compile(r"log_consumers|consumer_watermarks|renew_lease|"
                              r"lease_expires_at|log_partitions", re.I)),
    ("configure",  re.compile(r"configure|log_queues|create_queue", re.I)),
    ("maint/stats", re.compile(r"stats|retention|vacuum|analyze|metrics|"
                               r"pg_stat|system_state|hotlist", re.I)),
]


def load(p):
    out = {}
    if not os.path.exists(p):
        return out
    with open(p) as f:
        for row in csv.reader(f):
            if len(row) < 5:
                continue
            try:
                out[row[0]] = (int(row[1]), int(row[2]), float(row[3]), row[4])
            except ValueError:
                continue
    return out


def bucket(q):
    for name, rx in BUCKETS:
        if rx.search(q):
            return name
    return "other"


def main():
    outdir, runid = sys.argv[1], sys.argv[2]
    top = 20
    if "--top" in sys.argv:
        top = int(sys.argv[sys.argv.index("--top") + 1])
    before = load(os.path.join(outdir, runid + ".pgss-before.csv"))
    after = load(os.path.join(outdir, runid + ".pgss-after.csv"))
    if not after:
        print(f"{runid}: no pg_stat_statements snapshots (cell not built with PGSS=1)")
        return
    rows = []
    for qid, (calls, nrows, ms, q) in after.items():
        b = before.get(qid, (0, 0, 0.0, q))
        d_calls, d_rows, d_ms = calls - b[0], nrows - b[1], ms - b[2]
        if d_calls <= 0:
            continue
        rows.append((d_calls, d_rows, d_ms, q.strip()))
    rows.sort(key=lambda r: -r[0])

    # the loader's own json tells us how many messages were actually delivered
    j = os.path.join(outdir, runid + ".json")
    delivered = None
    if os.path.exists(j):
        m = json.load(open(j))
        delivered = m["achieved"]["poppedMsgs"]

    tot = sum(r[0] for r in rows)
    print(f"=== {runid}: {tot} statement calls, {delivered} messages delivered")
    agg = {}
    for c, nr, ms, q in rows:
        k = bucket(q)
        a = agg.setdefault(k, [0, 0, 0.0])
        a[0] += c; a[1] += nr; a[2] += ms
    print(f"{'bucket':<14}{'calls':>10}{'calls/msg':>11}{'exec_ms':>12}{'ms/call':>9}")
    for k, (c, nr, ms) in sorted(agg.items(), key=lambda kv: -kv[1][0]):
        per = f"{c/delivered:.3f}" if delivered else "-"
        print(f"{k:<14}{c:>10}{per:>11}{ms:>12.0f}{ms/c:>9.3f}")
    print(f"\n--- top {top} statements by calls")
    for c, nr, ms, q in rows[:top]:
        per = f"{c/delivered:.3f}" if delivered else "-"
        print(f"{c:>9} {per:>8}/msg {ms:>10.0f}ms  {bucket(q):<12} {q[:96]}")


if __name__ == "__main__":
    main()
