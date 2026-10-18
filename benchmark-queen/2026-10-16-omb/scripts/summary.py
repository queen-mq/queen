#!/usr/bin/env python3
"""summary.py <run dir>... [--json] — one row per OMB point.

From result.json (OMB's own output): the mean publish and consume rates over the measured windows, the aggregated
publish and end-to-end latency percentiles OMB computes over the whole measured period, publish errors and the largest
backlog. From samples/broker-*.txt (common/sampler.sh, every 2 s): the brokers' cores (busy CPU of the whole machine,
summed over the three) and each broker's disk writes, over the measured period, which is the last
`testDurationMinutes` before the benchmark ended, less 15 s at either end.
"""
import json, os, re, sys, glob


def env(d):
    out = {}
    p = os.path.join(d, "run.env")
    if os.path.exists(p):
        for l in open(p):
            if "=" in l:
                k, v = l.rstrip("\n").split("=", 1)
                out[k] = v.strip('"')
    return out


def workload_minutes(d):
    p = os.path.join(d, "workload.yaml")
    if os.path.exists(p):
        m = re.search(r"^testDurationMinutes:\s*(\d+)", open(p).read(), re.M)
        if m:
            return int(m.group(1))
    return 5


def samples(path, t0, t1):
    rows = []
    for l in open(path):
        f = l.split()
        if len(f) < 2 or "cpu_total=" not in l:
            continue
        try:
            t = float(f[0])
        except ValueError:
            continue
        r = {k: float(v) for k, v in re.findall(r"(\w+)=([\d.]+)", l)}
        r["t"] = t
        rows.append(r)
    w = [r for r in rows if t0 <= r["t"] <= t1]
    if len(w) < 2:
        return None
    a, b = w[0], w[-1]
    dt = b["t"] - a["t"]
    out = {}
    tot = b["cpu_total"] - a["cpu_total"]
    if tot > 0:
        out["cores"] = (b["cpu_busy"] - a["cpu_busy"]) / tot * b.get("ncpu", 1)
    if "disk_wsect" in b and dt > 0:
        out["disk_w_mbs"] = (b["disk_wsect"] - a["disk_wsect"]) * 512 / dt / 1e6
    if "net_tx" in b and dt > 0:
        out["net_tx_mbs"] = (b["net_tx"] - a["net_tx"]) / dt / 1e6
        out["net_rx_mbs"] = (b["net_rx"] - a["net_rx"]) / dt / 1e6
    return out


def row(d):
    e = env(d)
    r = {"tag": os.path.basename(d.rstrip("/")), "system": e.get("SYSTEM", "?"), "workload": e.get("WORKLOAD", "?")}
    p = os.path.join(d, "result.json")
    if not os.path.exists(p):
        r["error"] = "no result.json"
        return r
    j = json.load(open(p))
    mean = lambda xs: sum(xs) / len(xs) if xs else 0.0  # noqa: E731
    r["pub_rate"] = mean(j.get("publishRate", []))
    r["cons_rate"] = mean(j.get("consumeRate", []))
    r["pub_err"] = sum(j.get("publishErrorRate", []) or [0])
    r["backlog_max"] = max(j.get("backlog", [0]) or [0])
    for k, src in (("pub50", "aggregatedPublishLatency50pct"), ("pub99", "aggregatedPublishLatency99pct"),
                   ("pub999", "aggregatedPublishLatency999pct"), ("e2e50", "aggregatedEndToEndLatency50pct"),
                   ("e2e99", "aggregatedEndToEndLatency99pct"), ("e2e999", "aggregatedEndToEndLatency999pct"),
                   ("e2emax", "aggregatedEndToEndLatencyMax")):
        r[k] = j.get(src)
    try:
        t1 = float(e["END"]) - 15
        t0 = float(e["END"]) - workload_minutes(d) * 60 + 15
    except (KeyError, ValueError):
        t0, t1 = 0, 1e18
    cores, disks = 0.0, []
    for f in sorted(glob.glob(os.path.join(d, "samples", "broker-*.txt"))):
        s = samples(f, t0, t1)
        if s:
            cores += s.get("cores", 0)
            disks.append(s.get("disk_w_mbs", 0))
    r["broker_cores"] = cores
    r["disk_w_mbs"] = disks
    return r


def fmt(r):
    if "error" in r:
        return f"{r['tag']}: {r['error']}"
    ms = lambda v: "-" if v is None else (f"{v:.1f}" if v < 100 else f"{v:,.0f}")  # noqa: E731
    disk = "/".join(f"{x:.0f}" for x in r["disk_w_mbs"]) or "-"
    return (f"{r['tag']} [{r['system']}] {r['workload']}: pub {r['pub_rate']/1e3:,.0f}k/s cons {r['cons_rate']/1e3:,.0f}k/s "
            f"err {r['pub_err']:.0f} backlog max {r['backlog_max']:,.0f} | pub p50/p99/p99.9 {ms(r['pub50'])}/{ms(r['pub99'])}/{ms(r['pub999'])} ms "
            f"| e2e p50/p99/p99.9 {ms(r['e2e50'])}/{ms(r['e2e99'])}/{ms(r['e2e999'])} ms (max {ms(r['e2emax'])}) "
            f"| brokers {r['broker_cores']:.1f} cores, disk write {disk} MB/s")


if __name__ == "__main__":
    dirs = [a for a in sys.argv[1:] if not a.startswith("--")]
    rows = [row(d) for d in dirs]
    if "--json" in sys.argv:
        print(json.dumps(rows, indent=1))
    else:
        for r in rows:
            print(fmt(r))
