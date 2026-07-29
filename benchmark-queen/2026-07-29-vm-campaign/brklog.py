#!/usr/bin/env python3
"""brklog.py — attribute the BROKER's own `rates:` / `sizes:` log blocks to a run.

The broker emits one aggregated `rates` line and one `sizes` line per window
(~10s). They carry things no external sampler can see: pool saturation
(`pool="used/max"` + `pool_waiting`), the Vegas concurrency limits actually in
force, parked long-polls, the spool's health, dedup and hot-list sizes and the
process RSS. This maps them onto the STEADY-LOAD window of a runpt.sh run so a
throughput number and the broker's internal state come from the same seconds.

  brklog.py <outdir> [run-id ...]           table
  brklog.py --json <outdir> [run-id ...]    machine readable
  brklog.py --raw  <outdir> <run-id>        the raw lines inside the window

Window definition is imported from ptsum so it is defined in exactly one place.
"""
import json
import os
import re
import sys
import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ptsum import window  # noqa: E402

LOG = os.environ.get("BROKER_LOG", "/root/cell/broker.log")

TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)Z")
KV = re.compile(r'(\w+)="([^"]*)"|(\w+)=(\S+)')


def ts(line):
    m = TS.match(line)
    if not m:
        return None
    s = m.group(1)
    if "." in s:
        head, frac = s.split(".", 1)
        frac = (frac + "000000")[:6]
        s = head + "." + frac
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.datetime.strptime(s, fmt).replace(
        tzinfo=datetime.timezone.utc).timestamp()


def kvs(line):
    out = {}
    for a, b, c, d in KV.findall(line):
        if a:
            out[a] = b
        else:
            out[c] = d
    return out


def num(s, default=0.0):
    try:
        return float(re.sub(r"[^0-9.\-]", "", str(s)) or default)
    except ValueError:
        return default


def scan(a, b, path=LOG):
    """Return (rates_rows, sizes_rows) whose timestamp falls in [a, b]."""
    rates, sizes = [], []
    with open(path, errors="replace") as f:
        for line in f:
            if " rates: " not in line and " sizes: " not in line:
                continue
            t = ts(line)
            if t is None or not (a <= t <= b):
                continue
            row = kvs(line)
            row["_t"] = t
            row["_line"] = line.rstrip()
            (rates if " rates: " in line else sizes).append(row)
    return rates, sizes


def pool(row):
    """pool="used/max" -> (used, max)"""
    p = row.get("pool", "0/0")
    try:
        u, m = p.split("/")
        return int(num(u)), int(num(m))
    except Exception:
        return 0, 0


def hotlist(row):
    """hotlist="Nrings/Mready/Kwheel" -> dict"""
    h = row.get("hotlist", "")
    m = re.match(r"(\d+)rings/(\d+)ready/(\d+)wheel", h)
    return dict(zip(("rings", "ready", "wheel"),
                    (int(x) for x in m.groups()))) if m else {}


def summarise(outdir, runid):
    j = os.path.join(outdir, runid + ".json")
    if not os.path.exists(j):
        return None
    meta = json.load(open(j))
    if not isinstance(meta, dict) or "achieved" not in meta:
        return None
    a, b, _ = window(meta)
    rates, sizes = scan(a, b)
    if not rates and not sizes:
        return {"run": runid, "n_rates": 0, "n_sizes": 0}

    def mx(rows, f):
        vals = [f(r) for r in rows]
        return max(vals) if vals else 0

    def av(rows, f):
        vals = [f(r) for r in rows]
        return sum(vals) / len(vals) if vals else 0

    pu = [pool(r)[0] for r in rates] or [0]
    pm = [pool(r)[1] for r in rates] or [0]
    hl = [hotlist(r) for r in sizes if hotlist(r)]
    return {
        "run": runid,
        "n_rates": len(rates), "n_sizes": len(sizes),
        # rates block
        "push_s_max": mx(rates, lambda r: num(r.get("push_s"))),
        "pop_s_max": mx(rates, lambda r: num(r.get("pop_s"))),
        "ack_s_max": mx(rates, lambda r: num(r.get("ack_s"))),
        "p50_push_ms": av(rates, lambda r: num(r.get("p50_push_ms"))),
        "p99_push_ms": mx(rates, lambda r: num(r.get("p99_push_ms"))),
        "p99_pop_ms": mx(rates, lambda r: num(r.get("p99_pop_ms"))),
        "p99_ack_ms": mx(rates, lambda r: num(r.get("p99_ack_ms"))),
        "ack_hit_pct": av(rates, lambda r: num(r.get("ack_hit_pct"))),
        "pop_empty_pct": av(rates, lambda r: num(r.get("pop_empty_pct"))),
        "parked_max": mx(rates, lambda r: num(r.get("parked"))),
        "pool_used_max": max(pu), "pool_max": max(pm),
        "pool_used_avg": round(sum(pu) / len(pu), 1),
        "pool_waiting_max": mx(rates, lambda r: num(r.get("pool_waiting"))),
        "pool_waiting_avg": round(av(rates, lambda r: num(r.get("pool_waiting"))), 2),
        "vegas_push_min": min([num(r.get("vegas_push")) for r in rates] or [0]),
        "vegas_push_max": mx(rates, lambda r: num(r.get("vegas_push"))),
        "vegas_pop_min": min([num(r.get("vegas_pop")) for r in rates] or [0]),
        "vegas_pop_max": mx(rates, lambda r: num(r.get("vegas_pop"))),
        "buffered_any": any(r.get("buffered") == "true" for r in rates),
        # sizes block
        "rss_gb_max": mx(sizes, lambda r: num(r.get("rss_gb"))),
        "dedup": sizes[-1].get("dedup") if sizes else None,
        "dedup_suppressed_max": mx(sizes, lambda r: num(r.get("dedup_suppressed"))),
        "ack_reg": sizes[-1].get("ack_reg") if sizes else None,
        "hot_rings_max": max([h["rings"] for h in hl] or [0]),
        "hot_ready_max": max([h["ready"] for h in hl] or [0]),
        "hot_wheel_max": max([h["wheel"] for h in hl] or [0]),
        "spool_pending_max": mx(sizes, lambda r: num(r.get("spool_pending"))),
        "spool_unhealthy": any(r.get("spool_healthy") == "false" for r in sizes),
    }


def main():
    args = sys.argv[1:]
    mode = "table"
    while args and args[0].startswith("--"):
        mode = args.pop(0)[2:]
    outdir = args[0]
    runs = args[1:] or sorted(f[:-5] for f in os.listdir(outdir)
                              if f.endswith(".json"))
    if mode == "raw":
        meta = json.load(open(os.path.join(outdir, runs[0] + ".json")))
        a, b, _ = window(meta)
        for r in sum(scan(a, b), []):
            print(r["_line"])
        return
    rows = [r for r in (summarise(outdir, r) for r in runs) if r]
    if mode == "json":
        print(json.dumps(rows, indent=1))
        return
    fmt = "{:<22}{:>8}{:>8}{:>8}{:>9}{:>8}{:>8}{:>9}{:>7}{:>8}{:>9}{:>8}{:>7}"
    print(fmt.format("run", "push/s", "pop/s", "ack/s", "p99push", "p99pop",
                     "parked", "pool", "pwait", "vegasP", "vegasC", "hotring",
                     "rssGB"))
    for r in rows:
        if not r.get("n_rates"):
            print("{:<22}  (no broker rates lines in window)".format(r["run"]))
            continue
        print(fmt.format(
            r["run"][:22], int(r["push_s_max"]), int(r["pop_s_max"]),
            int(r["ack_s_max"]), r["p99_push_ms"], r["p99_pop_ms"],
            int(r["parked_max"]),
            "{}/{}".format(r["pool_used_max"], r["pool_max"]),
            r["pool_waiting_max"],
            "{:.0f}-{:.0f}".format(r["vegas_push_min"], r["vegas_push_max"]),
            "{:.0f}-{:.0f}".format(r["vegas_pop_min"], r["vegas_pop_max"]),
            r["hot_rings_max"], r["rss_gb_max"]))
    for r in rows:
        flags = []
        if r.get("buffered_any"):
            flags.append("BUFFERED")
        if r.get("spool_unhealthy"):
            flags.append("SPOOL-UNHEALTHY")
        if r.get("spool_pending_max"):
            flags.append("spool_pending={}".format(int(r["spool_pending_max"])))
        if r.get("pool_waiting_max"):
            flags.append("pool_waiting_max={}".format(int(r["pool_waiting_max"])))
        if flags:
            print("  {}: {}".format(r["run"], " ".join(flags)))


if __name__ == "__main__":
    main()
