#!/usr/bin/env python3
"""ptsum.py — summarise one measured load point (or many) into one row each.

  ptsum.py <outdir> [run-id ...]        # table
  ptsum.py --json <outdir> [run-id ...] # machine readable

CPU is computed over the STEADY-LOAD window only: the loader's own load phase
minus a warm-up head and a tail guard, so configure and drain never dilute it.
Commits/msg uses the same window, from pg_stat_database's own counter.
"""
import csv, json, os, sys, datetime

WARMUP = 10.0   # seconds of the load phase discarded as ramp
TAILCUT = 2.0   # seconds discarded before producers stop


def parse_iso(s):
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc).timestamp()


def window(meta):
    t0 = parse_iso(meta["startedAt"])
    load = meta.get("loadSec") or meta["config"]["durationSec"]
    # startedAt covers configure too; the loader logs it before configure, so
    # anchor the window on the END of the run instead and walk back.
    t1 = parse_iso(meta["endedAt"]) - (meta.get("drainSec") or 0)
    a, b = t1 - load + WARMUP, t1 - TAILCUT
    return a, b, (b - a)


def cpu_cores(path, a, b):
    """usage_usec is cumulative -> take the first/last sample inside [a,b]."""
    first, last = {}, {}
    ta, tb = {}, {}
    with open(path) as f:
        for r in csv.DictReader(f):
            t = float(r["t_unix"])
            if not (a <= t <= b):
                continue
            c, v = r["comp"], int(r["usage_usec"])
            if c not in first:
                first[c], ta[c] = v, t
            last[c], tb[c] = v, t
    out = {}
    for c in first:
        dt = tb[c] - ta[c]
        out[c] = (last[c] - first[c]) / 1e6 / dt if dt > 0 else 0.0
    return out


def pg_window(path, a, b):
    """returns (commit_rate, rollback_rate, wait distribution over ACTIVE backends)"""
    firstc = lastc = None
    ta = tb = None
    waits = {}
    nsamp = 0
    seen_t = set()
    with open(path) as f:
        for row in csv.reader(f):
            if not row or row[0] == "t_unix":
                continue
            t = float(row[0])
            if not (a <= t <= b):
                continue
            if row[1] == "d":
                c = int(row[2])
                if firstc is None:
                    firstc, ta = c, t
                lastc, tb = c, t
            elif row[1] == "w":
                state, wtype, wev, n = row[2], row[3], row[4], int(row[5])
                if state != "active":
                    continue
                if t not in seen_t:
                    seen_t.add(t)
                    nsamp += 1
                key = f"{wtype}:{wev}" if wtype != "CPU" else "CPU/running"
                waits[key] = waits.get(key, 0) + n
    dt = (tb - ta) if (ta is not None and tb is not None) else 0
    crate = (lastc - firstc) / dt if dt > 0 else 0.0
    total = sum(waits.values()) or 1
    dist = {k: v / total for k, v in sorted(waits.items(), key=lambda kv: -kv[1])}
    return crate, dist, nsamp


def stalls(outdir, runid):
    """A partition that stops being handed out shows up as pop falling behind
    push while push is unaffected, and later as a burst with a multi-second e2e.
    Detect it from the interval csv so it is an explicit attribute of the run
    instead of a silent p99 outlier."""
    p = os.path.join(outdir, runid + "-interval.csv")
    if not os.path.exists(p):
        return {}
    # one dark partition out of P is only a 1/P dip in the aggregate pop rate
    # (6.25% for the 16 partitions used here), so the threshold has to be tight;
    # the unambiguous second signature is the release burst in the drain, where
    # a whole interval's e2e p50 is measured in seconds.
    n_behind, worst_gap, burst_p50 = 0, 0.0, 0.0
    with open(p) as f:
        for r in csv.DictReader(f):
            push, pop = float(r["pushed_msg_s"]), float(r["popped_msg_s"])
            if r["phase"] == "load" and push > 50 and pop < 0.97 * push:
                n_behind += 1
                worst_gap = max(worst_gap, 1 - pop / push)
            if float(r["e2e_p50_ms"]) > 1000:
                burst_p50 = max(burst_p50, float(r["e2e_p50_ms"]))
    return {"stall_intervals": n_behind,
            "stall_worst_gap": round(worst_gap, 3),
            "stall_burst_p50_ms": round(burst_p50, 1),
            "stalled": bool(n_behind >= 2 or burst_p50 > 1000)}


def summarise(outdir, runid):
    j = os.path.join(outdir, runid + ".json")
    if not os.path.exists(j):
        return None
    meta = json.load(open(j))
    # the run directory also holds tenants.json and other non-run json
    if not isinstance(meta, dict) or "achieved" not in meta or "config" not in meta:
        return None
    a, b, span = window(meta)
    cpu = cpu_cores(os.path.join(outdir, runid + ".cpu.csv"), a, b)
    crate, dist, nsamp = pg_window(os.path.join(outdir, runid + ".pg.csv"), a, b)

    ach, lat = meta["achieved"], meta["latency"]
    ver = meta.get("verify") or {}
    errs = {}
    for op, e in (meta.get("errors") or {}).items():
        if not isinstance(e, dict):
            continue
        if e.get("total"):
            errs[op] = {"n": e["total"], "byCode": e.get("byCode") or {},
                        "byKind": e.get("byKind") or {}}
        else:
            # "canceled" is the loader tearing its own long-poll down at the end
            # of the drain, not a server refusal; keep it visible but out of the
            # error flag so a clean run reads clean.
            k = {kk: vv for kk, vv in (e.get("byKind") or {}).items()
                 if kk != "canceled"}
            if k:
                errs[op + "_kind"] = k
    cell = sum(cpu.get(k, 0) for k in ("pg", "pxdb", "broker", "proxy"))
    # delivered msg/s during the steady window: pop rate over the load phase
    deliv = ach.get("popMsgPerSecLoadPhase") or 0
    # request rate over the LOAD phase (pop/ack counts are whole-run, so scale
    # them by the load fraction the same way the loader reports pop rate)
    load_s = meta.get("loadSec") or meta["config"]["durationSec"]
    whole_s = load_s + (meta.get("drainSec") or 0)
    frac = load_s / whole_s if whole_s else 1
    reqs_s = ((ach.get("pushReqs", 0)
               + (ach.get("popReqs", 0) + ach.get("ackReqs", 0)) * frac) / load_s
              if load_s else 0)
    return {
        "run": runid,
        "target": meta["config"]["target"],
        "offered": meta["config"]["offeredMsgPerSecTotal"],
        "push": round(ach["pushMsgPerSec"], 1),
        "pop": round(deliv, 1),
        "shed": meta["offered"].get("shedMsgs", 0),
        "e2e_p50": lat["e2eFromSchedule"]["p50Ms"],
        "e2e_p95": lat["e2eFromSchedule"]["p95Ms"],
        "e2e_p99": lat["e2eFromSchedule"]["p99Ms"],
        "push_p50": lat["pushRtt"]["p50Ms"],
        "latency_push_p95": lat["pushRtt"]["p95Ms"],
        "push_p99": lat["pushRtt"]["p99Ms"],
        "ack_p50": lat["ackRtt"]["p50Ms"],
        "ack_p95": lat["ackRtt"]["p95Ms"],
        "cpu_pg": round(cpu.get("pg", 0), 3),
        "cpu_broker": round(cpu.get("broker", 0), 3),
        "cpu_proxy": round(cpu.get("proxy", 0), 3),
        "cpu_pxdb": round(cpu.get("pxdb", 0), 3),
        "cpu_cell": round(cell, 3),
        "cpu_slice": round(cpu.get("slice", 0), 3),
        "throttled_frac": round(cpu.get("slice_thr", 0), 3),
        "cpu_loader": round(cpu.get("loader", 0), 3),
        "cpu_dockersvc": round(cpu.get("dockersvc", 0), 3),
        "commits_s": round(crate, 1),
        "commits_per_msg": round(crate / deliv, 3) if deliv else None,
        # HTTP requests the proxy would have to handle, and what a request costs
        # it: far more portable than "% of the cell", which depends on the cell
        "reqs_s": round(reqs_s, 1),
        "proxy_us_per_req": (round(1e6 * cpu.get("proxy", 0) / reqs_s, 1)
                             if reqs_s else None),
        "errs": errs,
        "verdict": ver.get("verdict"),
        "sentOk": ver.get("sentOk"),
        "received": ver.get("received"),
        "missing": ver.get("missing"),
        "dup": ver.get("duplicate"),
        "extra": ver.get("extra"),
        "cross": ver.get("crossTenant"),
        "waits": dist,
        "wait_samples": nsamp,
        "window_s": round(span, 1),
        **stalls(outdir, runid),
    }


def main():
    args = sys.argv[1:]
    asjson = False
    if args and args[0] == "--json":
        asjson, args = True, args[1:]
    outdir = args[0]
    runs = args[1:] or sorted(
        f[:-5] for f in os.listdir(outdir) if f.endswith(".json"))
    rows = [r for r in (summarise(outdir, r) for r in runs) if r]
    if asjson:
        print(json.dumps(rows, indent=1))
        return
    fmt = ("{:<24}{:>6}{:>7}{:>8}{:>8}{:>7}{:>7}{:>8}{:>7}"
           "{:>7}{:>7}{:>7}{:>7}{:>7}{:>8}{:>8}  {:<14}")
    print(fmt.format("run", "tgt", "offer", "push", "pop", "e2e50", "e2e95",
                     "e2e99", "pRTT50", "cpuPG", "cpuBRK", "cpuPXY", "cell",
                     "loader", "cmt/s", "cmt/msg", "correctness"))
    for r in rows:
        ok = "{} m={} d={} x={} c={}".format(r["verdict"] or "?", r["missing"],
                                             r["dup"], r["extra"], r["cross"])
        print(fmt.format(
            r["run"][:24], r["target"][:6], r["offered"], r["push"], r["pop"],
            r["e2e_p50"], r["e2e_p95"], r["e2e_p99"], r["push_p50"],
            r["cpu_pg"], r["cpu_broker"], r["cpu_proxy"], r["cpu_cell"],
            r["cpu_loader"], r["commits_s"],
            r["commits_per_msg"] if r["commits_per_msg"] is not None else "-",
            ok + ("" if not r["errs"] else " ERR") +
            ("" if not r.get("stall_intervals") else
             " STALL x{}".format(r["stall_intervals"]))))
    for r in rows:
        if r["errs"]:
            print(f"  {r['run']}: errors {json.dumps(r['errs'])}")
        if r["waits"]:
            top = list(r["waits"].items())[:6]
            print(f"  {r['run']}: waits(active,n={r['wait_samples']}) " +
                  " ".join(f"{k}={v*100:.0f}%" for k, v in top))


if __name__ == "__main__":
    main()
