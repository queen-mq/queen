#!/usr/bin/env python3
"""report.py <run dir>... [--loaders] [--csv]

One row per run directory (runs/<system>/<tag>/), the same columns for Kafka, Pulsar and Queen-style goload logs:
  rate offered, last-30-s pushed / consumed / shed (sum over the load processes), e2e p50/p99 (worst process, like
  the 09-29 grid_report.py), e2e_local p99 (same-host samples: clock-skew free), produce p99 (worst process),
  broker cores (3 nodes summed, loaded window), system-process cores, max RSS per node, disk write MB/s and net MB/s
  per node, errors, loader CPU.
Load-process logs are found by content (a "[final]" line or window lines), sampler files by content ("cpu_total=").
run.env (KEY=VALUE lines written by run.sh) supplies the shape columns when present.
--loaders prints every load process's last-30-s numbers under its run (the follower/older-CPU check of 09-30).
Transactional runs (txn/run.sh: run.env TXN=1, "[txn]" lines in the loader logs) add a second table, "transactions":
committed txn/s and msg/s, msgs per transaction, commit latency p50/p99 (begin -> commit acknowledged, worst process),
e2e p50/p99 (input scheduled send -> committed output consumed, worst process), CPU (system processes summed over the
3 nodes; Redpanda: its shards' busy time from summary.txt), busiest node's RSS, mean disk per node, aborts, errors,
the verifier's verdict (verify.log) and the durability class on every row (run.env DUR; each system's full DURABILITY
sentence under the table). --txn prints only that table (with --csv, as CSV); the first table and its --csv output are
unchanged.
"""
import os, re, sys, statistics

WIN = re.compile(r'^\[(\d\d):(\d\d):(\d\d)\] offered=\s*(\d+)/s achieved=\s*(\d+)/s shed=\s*(\d+)/s .*?p50=\s*([\d.]+) p99=\s*([\d.]+)'
                 r'.*?\| push=(\d+) pop=(\d+).*?errs push=(\d+) pop=(\d+).*?ack=\s*(\d+)/s ackErr=(\d+)'
                 r'.*?e2e p50=([\d.]+) p99=([\d.]+)(?: p999=([\d.]+))?(?: n=(\d+))?(?:.*?e2e_local p50=([\d.]+) p99=([\d.]+))?')
FINAL = re.compile(r'^\[final\].*?pushErr=(\d+).*?pushed=(\d+) popped=(\d+).*?popErr=(\d+).*?ackErr=(\d+)')
CPU = re.compile(r'^(?:load|goload)_cpu=([\d.]+)%')
TXNW = re.compile(r'^\[txn\] (\d\d):(\d\d):(\d\d) txn=\s*([\d.]+)/s msgs=\s*([\d.]+)/s avg=\s*([\d.]+) msg/txn \| commit p50=([\d.]+) p99=([\d.]+) p999=([\d.]+) ms \| aborts=(\d+) errs=(\d+)')
TXNF = re.compile(r'^\[txn-final\] txns=(\d+) msgs=(\d+) aborts=(\d+) errs=(\d+)')
VERIFY = re.compile(r'^\[verify\] system=\S+ .*?produced=(\d+) ambiguous=(\d+) out_records=(\d+) out_unique=(\d+) duplicates=(\d+) dup_records=(\d+) missing=(\d+) pending_in=(\d+) in_and_out=(\d+) extra=(\d+).*?VERDICT (\w+)')
ROLES = ("kafka", "zk", "bookie", "broker", "queen", "redpanda")

def read(p):
    try:
        with open(p, errors="replace") as f: return f.read()
    except OSError: return ""

def walk(d):
    for root, _, files in os.walk(d):
        for f in sorted(files):
            if f.endswith((".gz", ".tgz", ".tar", ".jfr", ".hprof")): continue
            yield os.path.join(root, f)

def loader_logs(d):
    out = []
    for p in walk(d):
        txt = read(p)
        if "[final]" not in txt and not re.search(r'^\[\d\d:\d\d:\d\d\] offered=', txt, re.M): continue
        w = []
        for l in txt.splitlines():
            m = WIN.match(l)
            if m:
                g = m.groups()
                w.append(dict(t=int(g[0]) * 3600 + int(g[1]) * 60 + int(g[2]), off=float(g[3]), ach=float(g[4]),
                              shed=float(g[5]), pp50=float(g[6]), pp99=float(g[7]), push=int(g[8]), pop=int(g[9]),
                              perr=int(g[10]), poperr=int(g[11]), ack=float(g[12]), ackerr=int(g[13]),
                              e50=float(g[14]), e99=float(g[15]), el99=float(g[19]) if g[19] else None))
        fm = FINAL.search(txt, re.M) if "[final]" in txt else None
        fin = None
        for l in txt.splitlines():
            m = FINAL.match(l)
            if m: fin = dict(pushErr=int(m[1]), pushed=int(m[2]), popped=int(m[3]), popErr=int(m[4]), ackErr=int(m[5]))
        cpu = None
        for l in txt.splitlines():
            m = CPU.match(l.strip())
            if m: cpu = float(m[1])
        tw, tf = {}, None
        if "[txn" in txt:
            for l in txt.splitlines():
                m = TXNW.match(l)
                if m:
                    g = m.groups()
                    tw[int(g[0]) * 3600 + int(g[1]) * 60 + int(g[2])] = dict(txn=float(g[3]), msgs=float(g[4]), avg=float(g[5]),
                        c50=float(g[6]), c99=float(g[7]), c999=float(g[8]), aborts=int(g[9]), errs=int(g[10]))
                m = TXNF.match(l)
                if m: tf = dict(txns=int(m[1]), msgs=int(m[2]), aborts=int(m[3]), errs=int(m[4]))
        if w or fin: out.append(dict(path=p, windows=w, final=fin, cpu=cpu, txnw=tw, txnf=tf))
    return out

def steady(w):
    """the 3 full producing windows before the last one (the last may be partial), like grid_report's w[-4:-1]"""
    prod = [x for x in w if x["off"] > 0]
    return prod[-4:-1] if len(prod) >= 4 else prod[-3:]

def pops_rate(w):
    """consumed msg/s over the steady windows, from the cumulative pop counter (short runs: whatever windows exist)"""
    s = steady(w)
    if not s: return 0.0
    i0 = w.index(s[0])
    base = w[i0 - 1] if i0 > 0 else (s[0] if len(s) >= 2 else None)
    if base is None: return 0.0
    dt = (s[-1]["t"] - base["t"]) % 86400
    return (s[-1]["pop"] - base["pop"]) / dt if dt > 0 else 0.0

def samples(d):
    nodes = {}
    for p in walk(d):
        txt = read(p)
        if "cpu_total=" not in txt: continue
        rows = []
        for l in txt.splitlines():
            if l.startswith("#") or "cpu_total=" not in l: continue
            f = l.split()
            r = {k: float(v) for k, v in re.findall(r'(\w+)=([\d.]+)', l)}
            try: r["t"] = float(f[0])
            except ValueError: continue
            rows.append(r)
        if len(rows) > 3: nodes[os.path.relpath(p, d)] = rows
    return nodes

def node_stats(rows, win=None):
    """rates over the loaded window: [start + ramp, start + duration] from run.env when known (09-30: the busy-CPU
    heuristic below took in a 200-s topic creation at 50k partitions and halved the broker cores); else the rows whose
    busy CPU is >= 40% of the run's peak"""
    if win:
        sel = [i for i, r in enumerate(rows) if win[0] <= r["t"] <= win[1]]
        if len(sel) >= 2:
            a, b = rows[sel[0]], rows[sel[-1]]; dt = b["t"] - a["t"]
            idx = [sel[0], sel[-1] - 1]
        else: win = None
    if not win:
        busy = []
        for a, b in zip(rows, rows[1:]):
            dt = b["t"] - a["t"]
            if dt <= 0: continue
            busy.append((b["cpu_busy"] - a["cpu_busy"]) / dt)
        if not busy: return None
        peak = max(busy); idx = [i for i, v in enumerate(busy) if v >= 0.4 * peak]
        if len(idx) < 2: return None
        a, b = rows[idx[0]], rows[idx[-1] + 1]; dt = b["t"] - a["t"]
    rate = lambda k: (b.get(k, 0) - a.get(k, 0)) / dt
    hz = 100.0
    st = dict(cores=rate("cpu_busy") / hz, secs=dt,
              disk_w=rate("disk_wsect") * 512 / 1e6, disk_r=rate("disk_rsect") * 512 / 1e6,
              net_rx=rate("net_rx") / 1e6, net_tx=rate("net_tx") / 1e6, majflt=rate("pgmajfault"),
              iowait=rate("cpu_iowait") / hz, steal=rate("cpu_steal") / hz)
    for r in ROLES + ("kload", "pload", "goload", "qload"):
        if f"{r}_ticks" in b:
            st[f"{r}_cores"] = rate(f"{r}_ticks") / hz
            st[f"{r}_rss_gb"] = max(x.get(f"{r}_rss_kb", 0) for x in rows[idx[0]:idx[-1] + 2]) / 1048576
    st["memfree_min_gb"] = min(x.get("MemFree_kb", 0) for x in rows[idx[0]:idx[-1] + 2]) / 1048576
    st["system"] = [r for r in ROLES if st.get(f"{r}_cores", 0) > 0.05]
    return st

def run_env(d):
    env = {}
    for l in read(os.path.join(d, "run.env")).splitlines():
        if "=" in l and not l.startswith("#"):
            k, v = l.split("=", 1); env[k.strip()] = v.strip().strip('"')
    return env

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    per_loader = "--loaders" in sys.argv; csv = "--csv" in sys.argv; txn_only = "--txn" in sys.argv
    trows = []
    cols = ["run", "sys", "shape", "offered", "push/s", "cons/s", "shed/s", "e2e p50", "e2e p99", "local p99",
            "prod p99", "brk cores", "sys cores", "rss GB/node", "disk w MB/s/node", "net rx/tx MB/s/node", "errs", "load cpu"]
    rows = []
    for d in args:
        d = d.rstrip("/"); env = run_env(d); L = loader_logs(d); S = samples(d)
        win = None
        go = env.get("GO_MS") or env.get("START_MS")
        if go:
            def secs(v, dflt):
                m = re.fullmatch(r'(\d+)(ms|s|m|h)?', (v or '').strip())
                if not m: return dflt
                n, u = int(m[1]), m[2] or 's'
                return n / 1000 if u == 'ms' else n * {'s': 1, 'm': 60, 'h': 3600}[u]
            t0 = int(go) / 1000; ramp = secs(env.get("RAMP"), 10); dur = secs(env.get("DURATION"), 70)
            win = (t0 + ramp, t0 + dur)
        st = [x for x in (node_stats(r, win) for r in S.values()) if x]
        brokers = [x for x in st if x["system"]]
        loaders = [x for x in st if not x["system"]]
        sd = [steady(x["windows"]) for x in L]
        push = sum(sum(w["ach"] for w in s) / len(s) for s in sd if s)
        shed = sum(sum(w["shed"] for w in s) / len(s) for s in sd if s)
        cons = sum(pops_rate(x["windows"]) for x in L)
        e50 = max((w["e50"] for s in sd for w in s), default=0); e99 = max((w["e99"] for s in sd for w in s), default=0)
        el = [w["el99"] for s in sd for w in s if w["el99"] is not None]; el99 = max(el) if el else float("nan")
        pp = max((w["pp99"] for s in sd for w in s), default=0)
        errs = sum((x["final"] or {}).get(k, 0) for x in L for k in ("pushErr", "popErr", "ackErr"))
        sysname = env.get("SYSTEM") or ("kafka" if any("kafka" in b["system"] for b in brokers) else
                                        "pulsar" if any("broker" in b["system"] for b in brokers) else
                                        "queen" if any("queen" in b["system"] for b in brokers) else
                                        "redpanda" if any("redpanda" in b["system"] for b in brokers) else "?")
        shape = env.get("SHAPE") or " ".join(f"{k.lower()}={env[k]}" for k in ("TOPICS", "PARTITIONS", "ENTITIES", "MODE", "SUB_TYPE") if k in env)
        bc = sum(b["cores"] for b in brokers)
        sc = sum(sum(b.get(f"{r}_cores", 0) for r in ROLES) for b in brokers)
        rss = max((sum(b.get(f"{r}_rss_gb", 0) for r in ROLES) for b in brokers), default=0)
        dw = statistics.mean([b["disk_w"] for b in brokers]) if brokers else 0
        nrx = statistics.mean([b["net_rx"] for b in brokers]) if brokers else 0
        ntx = statistics.mean([b["net_tx"] for b in brokers]) if brokers else 0
        lc = sum(x["cpu"] or 0 for x in L)
        if env.get("TXN") == "1" or any(x.get("txnw") for x in L):
            trows.append(txn_row(d, env, L, sd, sysname, push, cons, e50, e99, brokers))
        rows.append([os.path.basename(d), sysname, shape or "-", env.get("RATE", "-"), f"{push/1000:.0f}k", f"{cons/1000:.0f}k",
                     f"{shed/1000:.0f}k", f"{e50:.0f}", f"{e99:.0f}", f"{el99:.0f}", f"{pp:.0f}", f"{bc:.1f}", f"{sc:.1f}",
                     f"{rss:.1f}", f"{dw:.0f}", f"{nrx:.0f}/{ntx:.0f}", str(errs), f"{lc/100:.1f}"])
        if per_loader:
            for x in sorted(L, key=lambda x: x["path"]):
                s = steady(x["windows"])
                if not s: continue
                rows.append(["  " + os.path.relpath(x["path"], d)[:40], "", "", "",
                             f"{sum(w['ach'] for w in s)/len(s)/1000:.1f}k", f"{pops_rate(x['windows'])/1000:.1f}k",
                             f"{sum(w['shed'] for w in s)/len(s)/1000:.1f}k", f"{max(w['e50'] for w in s):.0f}",
                             f"{max(w['e99'] for w in s):.0f}", "", f"{max(w['pp99'] for w in s):.0f}", "", "", "", "", "",
                             str(sum((x['final'] or {}).get(k, 0) for k in ('pushErr', 'popErr', 'ackErr'))),
                             f"{(x['cpu'] or 0)/100:.1f}"])
    if txn_only:
        print_txn(trows, csv); return
    if csv:
        print(",".join(cols)); [print(",".join(r)) for r in rows]; return
    wd = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
    print("  ".join(c.rjust(wd[i]) if i > 2 else c.ljust(wd[i]) for i, c in enumerate(cols)))
    for r in rows: print("  ".join(v.rjust(wd[i]) if i > 2 else v.ljust(wd[i]) for i, v in enumerate(r)))
    print("latencies in ms (e2e = consumer receive - scheduled send; local = same-host samples only); rates = last 30 s")
    if trows:
        print()
        print_txn(trows, False)

TCOLS = ["run", "sys", "shape", "offered", "in/s", "txn/s", "txn msg/s", "out/s", "msg/txn", "commit p50", "commit p99",
         "e2e p50", "e2e p99", "cores", "rss GB/node", "disk w MB/s/node", "aborts", "errs", "verify", "durability"]
# the durability class next to every number (SPEC §0.3): run.env's DUR (txn/run.sh), else the system's default class;
# the full sentence (run.env DURABILITY) is printed once per system under the table. No commas (the CSV stays plain).
DUR_DEFAULT = {"kafka": "3 copies / ack all ISR (min 2) / no fsync", "redpanda": "3 copies / ack after 2 fsyncs",
               "pulsar": "3 copies / ack after 2 fsyncs", "queen": "3 copies / ack after 2 fsyncs"}
DUR_FULL = {}

def txn_row(d, env, L, sd, sysname, push, cons, e50, e99, brokers):
    """one transactions-table row: the [txn] windows paired with the steady standard windows by their end second"""
    txs = msgs = 0.0; c50 = c99 = 0.0
    for x, s in zip(L, sd):
        tw = [x["txnw"][w["t"]] for w in s if w["t"] in x.get("txnw", {})]
        if not tw: continue
        txs += sum(t["txn"] for t in tw) / len(tw); msgs += sum(t["msgs"] for t in tw) / len(tw)
        c50 = max(c50, max(t["c50"] for t in tw)); c99 = max(c99, max(t["c99"] for t in tw))
    aborts = sum((x.get("txnf") or {}).get("aborts", 0) for x in L)
    terrs = sum((x.get("txnf") or {}).get("errs", 0) for x in L)
    errs = terrs + sum((x["final"] or {}).get(k, 0) for x in L for k in ("pushErr", "popErr", "ackErr"))
    cores = sum(sum(b.get(f"{r}_cores", 0) for r in ROLES) for b in brokers)
    summ = read(os.path.join(d, "summary.txt"))
    m = re.search(r"shards busy .*?sum=([\d.]+) cores", summ)
    if m: cores = float(m[1])
    rss = max((sum(b.get(f"{r}_rss_gb", 0) for r in ROLES) for b in brokers), default=0)
    dw = statistics.mean([b["disk_w"] for b in brokers]) if brokers else 0
    v = "-"
    for l in read(os.path.join(d, "verify.log")).splitlines():
        mm = VERIFY.match(l)
        if mm:
            g = mm.groups()
            v = f"{g[10]} miss={g[6]} dup={g[4]}" + (f" pend={g[7]}" if g[7] != "0" else "") + (f" both={g[8]}" if g[8] != "0" else "") + (f" extra={g[9]}" if g[9] != "0" else "")
    shape = env.get("SHAPE") or "-"
    dur = (env.get("DUR") or DUR_DEFAULT.get(sysname, "-")).replace(",", ";")
    if env.get("DURABILITY"): DUR_FULL.setdefault(sysname, set()).add(env["DURABILITY"])
    return [os.path.basename(d), sysname, shape, env.get("RATE", "-"), f"{push/1000:.1f}k", f"{txs:.0f}", f"{msgs/1000:.1f}k",
            f"{cons/1000:.1f}k", f"{msgs/txs:.1f}" if txs else "-", f"{c50:.0f}", f"{c99:.0f}", f"{e50:.0f}", f"{e99:.0f}",
            f"{cores:.1f}", f"{rss:.1f}", f"{dw:.0f}", str(aborts), str(errs), v, dur]

def print_txn(trows, csv):
    if csv:
        print(",".join(TCOLS)); [print(",".join(r)) for r in trows]; return
    print("transactions (exactly-once consume-transform-produce; last 30 s; worst process for latencies)")
    wd = [max(len(c), *(len(r[i]) for r in trows)) if trows else len(c) for i, c in enumerate(TCOLS)]
    print("  ".join(c.rjust(wd[i]) if 2 < i < len(TCOLS) - 2 else c.ljust(wd[i]) for i, c in enumerate(TCOLS)))
    for r in trows: print("  ".join(v.rjust(wd[i]) if 2 < i < len(TCOLS) - 2 else v.ljust(wd[i]) for i, v in enumerate(r)))
    print("commit = transaction begin -> commit acknowledged; e2e = input scheduled send -> committed output consumed (ms);"
          " cores = system processes summed over 3 nodes (Redpanda: shards busy); verify = every input id in out exactly once"
          " (miss/dup) or still unprocessed in in (pend)")
    for sysname in sorted(DUR_FULL):
        for full in sorted(DUR_FULL[sysname]):
            print(f"durability {sysname}: {full}")

if __name__ == "__main__":
    main()
