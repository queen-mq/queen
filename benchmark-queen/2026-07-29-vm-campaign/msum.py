#!/usr/bin/env python3
"""msum.py <outdir> [run-id ...] — TASK M table.

One row per measured point, with the three things the minimum-pop-wait question
needs side by side: the window that was configured, the throughput/latency it
bought or cost, and the MECHANISM (commits per delivered message, and the
pg_stat_statements pop/ack call counts that say whether the pop really got
fatter). Reuses ptsum.summarise so CPU/commit accounting is identical to the
rest of the campaign — no second definition of the same number.
"""
import json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ptsum
import pgssdiff


def pgss_calls(outdir, runid, delivered):
    """calls-per-delivered-message for the buckets that matter here."""
    before = pgssdiff.load(os.path.join(outdir, runid + ".pgss-before.csv"))
    after = pgssdiff.load(os.path.join(outdir, runid + ".pgss-after.csv"))
    if not after or not delivered:
        return {}
    agg = {}
    for qid, (calls, nrows, ms, q) in after.items():
        b = before.get(qid, (0, 0, 0.0, q))
        d = calls - b[0]
        if d <= 0:
            continue
        agg[pgssdiff.bucket(q)] = agg.get(pgssdiff.bucket(q), 0) + d
    return {k: v / delivered for k, v in agg.items()}


def fillwait(outdir, runid):
    p = os.path.join(outdir, runid + ".fillwait.txt")
    if not os.path.exists(p):
        return (None, None)
    txt = open(p).read().split()
    d = dict(kv.split("=", 1) for kv in txt if "=" in kv)
    try:
        n = int(d.get("fill_waits", "0"))
        ms = float(d.get("fill_ms_total", "0"))
        return (n, ms / n if n else 0.0)
    except ValueError:
        return (None, None)


def row(outdir, runid):
    r = ptsum.summarise(outdir, runid)
    if not r:
        return None
    meta = json.load(open(os.path.join(outdir, runid + ".json")))
    cfg = meta["config"]
    r["window_ms"] = cfg.get("minPopWaitTime", 0)
    r["build"] = "M" if "build=M" in (cfg.get("note") or "") else (
        "base" if "build=base" in (cfg.get("note") or "") else "?")
    r["delivered"] = meta["achieved"]["poppedMsgs"]
    r["calls"] = pgss_calls(outdir, runid, r["delivered"])
    r["fill_n"], r["fill_avg_ms"] = fillwait(outdir, runid)
    return r


def main():
    outdir = sys.argv[1]
    runs = sys.argv[2:] or sorted(
        f[:-5] for f in os.listdir(outdir) if f.endswith(".json"))
    rows = [x for x in (row(outdir, r) for r in runs) if x]
    if "--json" in sys.argv:
        print(json.dumps(rows, indent=1))
        return
    fmt = ("{:<16}{:>5}{:>4}{:>6}{:>8}{:>8}{:>8}{:>9}{:>9}"
           "{:>7}{:>7}{:>7}{:>8}{:>8}{:>8}{:>8}  {:<22}")
    print(fmt.format("run", "bld", "W", "offer", "push", "pop", "e2e50",
                     "e2e95", "e2e99", "cpuPG", "cpuBRK", "cell",
                     "cmt/msg", "pop/msg", "ack/msg", "fillN", "correctness"))
    for r in rows:
        c = r["calls"]
        ok = "{} m={} d={} x={} c={}".format(r["verdict"] or "?", r["missing"],
                                             r["dup"], r["extra"], r["cross"])
        print(fmt.format(
            r["run"][:16], r["build"], r["window_ms"], r["offered"],
            r["push"], r["pop"], r["e2e_p50"], r["e2e_p95"], r["e2e_p99"],
            r["cpu_pg"], r["cpu_broker"], r["cpu_cell"],
            r["commits_per_msg"] if r["commits_per_msg"] is not None else "-",
            round(c.get("pop", 0), 3) if c else "-",
            round(c.get("ack", 0), 3) if c else "-",
            r["fill_n"] if r["fill_n"] is not None else "-",
            ok + ("" if not r["errs"] else " ERR") +
            ("" if not r.get("stall_intervals") else
             " STALL x{}".format(r["stall_intervals"]))))
    print("\n--- fill-wait engagement (broker counters, cumulative per run) and waits")
    for r in rows:
        fw = ("fill_waits={} avg_window={:.1f}ms".format(r["fill_n"], r["fill_avg_ms"])
              if r["fill_n"] is not None else "fill_waits=?")
        top = list(r["waits"].items())[:5]
        print("  {:<16} {:<38} {}".format(
            r["run"][:16], fw,
            " ".join(f"{k}={v*100:.0f}%" for k, v in top)))


if __name__ == "__main__":
    main()
