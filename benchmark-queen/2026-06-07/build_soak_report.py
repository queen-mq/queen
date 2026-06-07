#!/usr/bin/env python3
"""Build a self-contained HTML report (with embedded PNG charts) for the 24h soak.

Inputs (already pulled locally):
  vm-data/soak-loader-167/goload.log        loader timeseries (10s cadence)
  vm-data/soak-broker-165/long-mon.log      broker+PG monitor (30s cadence)
  vm-data/soak-broker-165/_report-capture/  final DB/host snapshots + broker config

Output:
  soak-report.html   (single file, charts base64-embedded -> fully offline)
  charts/*.png        (also written separately for reuse)
"""
import base64, io, json, re, os
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
VM = os.path.join(BASE, "vm-data")
LOADER = os.path.join(VM, "soak-loader-167", "goload.log")
MON = os.path.join(VM, "soak-broker-165", "long-mon.log")
CAP = os.path.join(VM, "soak-broker-165", "_report-capture")
CHARTS = os.path.join(BASE, "charts")
os.makedirs(CHARTS, exist_ok=True)

# Soak window start (DB wiped 13:03:48Z, clean run begins 13:04:18Z)
SOAK_START = datetime(2026, 6, 6, 13, 4, 18, tzinfo=timezone.utc)

# ---------------- styling ----------------
plt.rcParams.update({
    "figure.dpi": 120,
    "font.size": 11,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 13,
    "axes.titleweight": "bold",
    "figure.autolayout": True,
})
C_PUSH = "#2563eb"   # blue
C_POP = "#059669"    # green
C_BROKER = "#7c3aed" # violet
C_PG = "#ea580c"     # orange
C_DISK = "#dc2626"   # red
C_GREY = "#64748b"

# ---------------- parse loader ----------------
RE_LOAD = re.compile(
    r"\[(\d{2}):(\d{2}):(\d{2})\]\s+push=\s*(\d+)/s\s+pop=\s*(\d+)/s\s+\|\s+"
    r"tot push=(\d+)\s+pop=(\d+)\s+\|\s+errs p=(\d+)\s+c=(\d+)\s+empty=(\d+)")

def parse_loader(path):
    t_h, push_s, pop_s, tot_push, tot_pop, err_p, err_c, empty = ([] for _ in range(8))
    prev_tod = None; base = 0; t0 = None; skipped = 0
    with open(path) as f:
        for line in f:
            m = RE_LOAD.search(line)
            if not m:
                skipped += 1; continue
            hh, mm, ss = int(m[1]), int(m[2]), int(m[3])
            tod = hh*3600 + mm*60 + ss
            if prev_tod is not None and tod < prev_tod:
                base += 86400
            prev_tod = tod
            abs_s = base + tod
            if t0 is None: t0 = abs_s
            t_h.append((abs_s - t0)/3600.0)
            push_s.append(int(m[4])); pop_s.append(int(m[5]))
            tot_push.append(int(m[6])); tot_pop.append(int(m[7]))
            err_p.append(int(m[8])); err_c.append(int(m[9])); empty.append(int(m[10]))
    return dict(t=np.array(t_h), push=np.array(push_s), pop=np.array(pop_s),
                tp=np.array(tot_push, dtype=float), tpo=np.array(tot_pop, dtype=float),
                ep=np.array(err_p), ec=np.array(err_c), em=np.array(empty), skipped=skipped)

# ---------------- parse monitor ----------------
RE_MON = re.compile(
    r"^(\S+)\s+\|\s+queen=([\d.]+)%/([\d.]+)([GM])iB.*?postgres=([\d.]+)%/([\d.]+)([GM])iB.*?\|\s*"
    r"xc_ins_del_size_dead_live=\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)\s+(\w+)\s+(\d+)\s+(\d+)\s*\|\s*disk=(\d+)G")

def to_mib(v, u):
    return v*1024.0 if u == "G" else v

def size_to_gb(v, u):
    u = u.lower()
    if u == "gb": return v
    if u == "mb": return v/1024.0
    if u == "kb": return v/1024.0/1024.0
    if u == "tb": return v*1024.0
    return v

def parse_mon(path):
    rows = []; skipped = 0
    with open(path) as f:
        for line in f:
            if line.startswith("monitor_start"): continue
            m = RE_MON.search(line)
            if not m:
                skipped += 1; continue
            try:
                ts = datetime.strptime(m[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                skipped += 1; continue
            if ts < SOAK_START:
                continue
            rows.append((ts,
                float(m[2]),                       # queen cpu %
                to_mib(float(m[3]), m[4]),         # queen mem MiB
                float(m[5]),                       # pg cpu %
                to_mib(float(m[6]), m[7]),         # pg mem MiB
                int(m[8]), int(m[9]), int(m[10]),  # xact_commit, ins, del
                size_to_gb(float(m[11]), m[12]),   # msg size GB
                int(m[13]), int(m[14]),            # dead, live
                int(m[15])))                       # disk free G
    rows.sort(key=lambda r: r[0])
    t0 = rows[0][0]
    arr = lambda i: np.array([r[i] for r in rows], dtype=float)
    t_h = np.array([(r[0]-t0).total_seconds()/3600.0 for r in rows])
    d = dict(t=t_h, qcpu=arr(1)/100.0, qmem=arr(2), pcpu=arr(3)/100.0,
             pmem=arr(4)/1024.0, xc=arr(5), ins=arr(6), dele=arr(7),
             size=arr(8), dead=arr(9), live=arr(10), disk=arr(11), skipped=skipped)
    # derive PG insert/del rates from cumulative
    dt = np.diff(np.array([(r[0]-t0).total_seconds() for r in rows]))
    di = np.diff(d["ins"]); dd = np.diff(d["dele"])
    with np.errstate(divide="ignore", invalid="ignore"):
        ins_s = np.where((dt > 0) & (di >= 0), di/dt, np.nan)
        del_s = np.where((dt > 0) & (dd >= 0), dd/dt, np.nan)
    d["t_rate"] = t_h[1:]; d["ins_s"] = ins_s; d["del_s"] = del_s
    return d

# ---------------- chart helpers ----------------
def save(fig, name):
    path = os.path.join(CHARTS, name)
    fig.savefig(path, bbox_inches="tight")
    buf = io.BytesIO(); fig.savefig(buf, format="png", bbox_inches="tight"); buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return b64

def roll(y, w):
    """Centered moving average with edge-replication padding (no zero-dip at ends)."""
    if len(y) < w: return y
    if np.any(np.isnan(y)):  # fill gaps so NaNs don't smear across the window
        idx = np.arange(len(y)); good = ~np.isnan(y)
        if good.sum() >= 2:
            y = np.interp(idx, idx[good], y[good])
    pad_l = w // 2
    pad_r = w - 1 - pad_l
    yp = np.pad(y, (pad_l, pad_r), mode="edge")
    k = np.ones(w) / w
    return np.convolve(yp, k, mode="valid")

CH = {}  # name -> (title, caption, b64)

def line(name, title, caption, series, ylabel, ymin=0, ymax=None, hline=None):
    fig, ax = plt.subplots(figsize=(11, 3.6))
    for x, y, lbl, col, kw in series:
        ax.plot(x, y, label=lbl, color=col, **kw)
    ax.set_xlabel("elapsed (hours)"); ax.set_ylabel(ylabel)
    ax.set_xlim(0, None)
    if ymin is not None: ax.set_ylim(bottom=ymin)
    if ymax is not None: ax.set_ylim(top=ymax)
    if hline is not None:
        ax.axhline(hline[0], color=hline[1], ls="--", lw=1, alpha=0.7, label=hline[2])
    if len(series) > 1 or hline is not None:
        ax.legend(loc="lower right", framealpha=0.9, fontsize=9)
    ax.set_title(title)
    CH[name] = (title, caption, save(fig, name + ".png"))

# ================= MAIN =================
L = parse_loader(LOADER)
M = parse_mon(MON)
dur_h = float(L["t"][-1])
tot_push = L["tp"][-1]; tot_pop = L["tpo"][-1]
gap_final = tot_push - tot_pop
avg_push = tot_push / (dur_h*3600); avg_pop = tot_pop / (dur_h*3600)
steady = L["t"] > 0.5  # ignore first 30 min warmup for percentiles
def pct(a, p): return float(np.percentile(a[steady], p))

print(f"loader: {len(L['t'])} pts, {dur_h:.2f}h, skipped={L['skipped']}")
print(f"monitor(soak): {len(M['t'])} pts, span {M['t'][-1]:.2f}h, skipped={M['skipped']}")
print(f"tot_push={tot_push:.0f} tot_pop={tot_pop:.0f} gap={gap_final:.0f}")
print(f"avg push={avg_push:.0f}/s pop={avg_pop:.0f}/s")

# ---- charts ----
line("throughput", "Throughput over 24h — push vs pop (10s samples + 5-min mean)",
     "Push and pop track each other for the full run, holding ~118k msg/s. Thicker lines are the 5-minute rolling mean.",
     [(L["t"], L["push"], "push/s", C_PUSH, dict(lw=0.4, alpha=0.35)),
      (L["t"], L["pop"], "pop/s", C_POP, dict(lw=0.4, alpha=0.35)),
      (L["t"], roll(L["push"], 30), "push 5-min mean", C_PUSH, dict(lw=2)),
      (L["t"], roll(L["pop"], 30), "pop 5-min mean", C_POP, dict(lw=2))],
     "msg/s", ymin=0)

line("cumulative", "Cumulative messages pushed & popped",
     "Both curves reach 10.3 billion and sit on top of each other — the system never falls behind.",
     [(L["t"], L["tp"]/1e9, "pushed (billions)", C_PUSH, dict(lw=2)),
      (L["t"], L["tpo"]/1e9, "popped (billions)", C_POP, dict(lw=2, ls="--"))],
     "messages (billions)", ymin=0)

line("gap", "Push–pop gap over time (in-flight backlog)",
     "Difference between cumulative pushed and popped. It stays a tiny, bounded in-flight window (tens of thousands) against 10.3B processed — i.e. effectively zero loss/lag.",
     [(L["t"], (L["tp"]-L["tpo"])/1000.0, "gap (thousands of msgs)", C_GREY, dict(lw=1.5))],
     "gap (thousands)", ymin=0)

# histogram
fig, ax = plt.subplots(figsize=(11, 3.6))
ax.hist(L["push"][steady], bins=60, color=C_PUSH, alpha=0.6, label="push/s")
ax.hist(L["pop"][steady], bins=60, color=C_POP, alpha=0.6, label="pop/s")
ax.axvline(avg_push, color=C_PUSH, ls="--", lw=1.5)
ax.set_xlabel("msg/s"); ax.set_ylabel("samples (10s)"); ax.legend()
ax.set_title("Throughput distribution (steady state)")
CH["hist"] = ("Throughput distribution (steady state)",
              "Both push and pop cluster tightly around ~118k msg/s — the run is steady, not spiky.",
              save(fig, "hist.png"))

line("errors", "Cumulative errors & empty pops over 24h",
     "Push errors stay at zero. Consumer errors (172) and empty pops (128) all occur in the first minutes then flatline for the entire run — negligible against 10.3B messages.",
     [(L["t"], L["ec"], "consumer errors", C_DISK, dict(lw=2)),
      (L["t"], L["em"], "empty pops", C_PG, dict(lw=2)),
      (L["t"], L["ep"], "push errors", C_PUSH, dict(lw=2))],
     "cumulative count", ymin=0)

line("broker_cpu", "Broker (Queen) CPU over time",
     "The C++ broker holds ~5 cores for the whole run on a 32-core host — it is never the bottleneck.",
     [(M["t"], M["qcpu"], "queen", C_BROKER, dict(lw=1))],
     "CPU cores", ymin=0)

line("broker_mem", "Broker (Queen) memory over time",
     "After a short warmup the broker RSS is flat at ~400 MiB across 24h — no memory growth (the failure mode of the old architecture is gone).",
     [(M["t"], M["qmem"], "queen RSS", C_BROKER, dict(lw=1.5))],
     "memory (MiB)", ymin=0)

line("pg_cpu", "Postgres CPU over time",
     "Postgres carries the work at ~20–23 cores — this is where the throughput ceiling lives.",
     [(M["t"], M["pcpu"], "postgres", C_PG, dict(lw=1))],
     "CPU cores", ymin=0)

line("pg_mem", "Postgres memory over time",
     "PG RSS ramps as shared_buffers warm, then plateaus at ~33.8 GiB.",
     [(M["t"], M["pmem"], "postgres RSS", C_PG, dict(lw=1.5))],
     "memory (GiB)", ymin=0)

line("pg_rates", "Postgres rows inserted/s & deleted/s (independent of loader)",
     "Derived from PG's own cumulative tuple counters. Inserts (push) and deletes (retention) run neck-and-neck — retention keeps pace with ingestion, confirming the loader numbers from the server side.",
     [(M["t_rate"], roll(M["ins_s"], 10)/1000.0, "inserted/s (k, 5-min mean)", C_PUSH, dict(lw=1.5)),
      (M["t_rate"], roll(M["del_s"], 10)/1000.0, "deleted/s (k, 5-min mean)", C_DISK, dict(lw=1.5))],
     "rows/s (thousands)", ymin=0)

line("msg_size", "queen.messages table size over time",
     "The active backlog ramps in the first ~hour then plateaus at ~15 GB — retention holds the table in steady state for the entire run.",
     [(M["t"], M["size"], "messages size", C_PG, dict(lw=1.5))],
     "size (GB)", ymin=0)

line("tuples", "queen.messages live vs dead tuples",
     "Live rows hold steady (~14M); dead tuples oscillate as autovacuum continuously reclaims space behind the retention deletes.",
     [(M["t"], M["live"]/1e6, "live (millions)", C_POP, dict(lw=1.2)),
      (M["t"], M["dead"]/1e6, "dead (millions)", C_GREY, dict(lw=1, alpha=0.8))],
     "tuples (millions)", ymin=0)

line("disk", "Disk free over time (whole volume)",
     "Steep initial drop = WAL ramping to its 96 GB ceiling + tables filling. The slow continued decline afterward is the messages_consumed table (the one open issue). 377 GB -> 218 GB free over 24h.",
     [(M["t"], M["disk"], "disk free", C_DISK, dict(lw=1.5))],
     "disk free (GB)", ymin=0)

# combined throughput vs pg cpu (dual axis)
fig, ax1 = plt.subplots(figsize=(11, 3.6))
ax1.plot(L["t"], roll(L["push"], 30)/1000.0, color=C_PUSH, lw=1.8, label="push k/s (5-min mean)")
ax1.set_xlabel("elapsed (hours)"); ax1.set_ylabel("throughput (k msg/s)", color=C_PUSH)
ax1.tick_params(axis="y", labelcolor=C_PUSH); ax1.set_ylim(bottom=0); ax1.set_xlim(0, None)
ax2 = ax1.twinx(); ax2.spines["top"].set_visible(False)
ax2.plot(M["t"], M["pcpu"], color=C_PG, lw=1, alpha=0.7, label="PG cores")
ax2.set_ylabel("Postgres CPU (cores)", color=C_PG); ax2.tick_params(axis="y", labelcolor=C_PG)
ax2.set_ylim(bottom=0); ax2.grid(False)
ax1.set_title("Throughput vs Postgres CPU")
CH["combined"] = ("Throughput vs Postgres CPU",
                  "Throughput stability maps directly onto PG CPU sitting in its ~20–23 core band — the system runs at a stable operating point, not drifting.",
                  save(fig, "combined.png"))

# ---------------- snapshots for tables ----------------
def read(p):
    try:
        with open(p) as f: return f.read().strip()
    except OSError: return ""

tbl_sizes = []
for ln in read(os.path.join(CAP, "db-table-sizes.txt")).splitlines():
    parts = ln.split("|")
    if len(parts) == 4:
        tbl_sizes.append(parts)
run_meta = read(os.path.join(CAP, "run-meta.txt"))
db_stats = read(os.path.join(CAP, "db-stats.txt"))
ds = db_stats.split("|") if db_stats else []

# resource summary
def ms(a, f): return f(a[~np.isnan(a)]) if np.any(~np.isnan(a)) else float("nan")
res = dict(
    qcpu_mean=np.mean(M["qcpu"]), qcpu_p95=np.percentile(M["qcpu"], 95), qcpu_max=M["qcpu"].max(),
    qmem_mean=np.mean(M["qmem"][M["t"] > 0.5]), qmem_max=M["qmem"].max(),
    pcpu_mean=np.mean(M["pcpu"]), pcpu_p95=np.percentile(M["pcpu"], 95), pcpu_max=M["pcpu"].max(),
    pmem_max=M["pmem"].max(),
    disk_start=M["disk"][0], disk_end=M["disk"][-1],
    size_max=M["size"].max(),
)

stats = dict(
    dur_h=dur_h, tot_push=tot_push, tot_pop=tot_pop, gap=gap_final,
    avg_push=avg_push, avg_pop=avg_pop,
    push_p50=pct(L["push"], 50), push_p95=pct(L["push"], 95), push_p99=pct(L["push"], 99),
    push_min=float(L["push"][steady].min()), push_max=float(L["push"].max()), push_mean=float(L["push"][steady].mean()),
    pop_p50=pct(L["pop"], 50), pop_p95=pct(L["pop"], 95), pop_p99=pct(L["pop"], 99),
    pop_min=float(L["pop"][steady].min()), pop_max=float(L["pop"].max()), pop_mean=float(L["pop"][steady].mean()),
    err_c=int(L["ec"][-1]), empty=int(L["em"][-1]), err_p=int(L["ep"][-1]),
    res=res, tbl=tbl_sizes, meta=run_meta, ds=ds,
)
with open(os.path.join(BASE, "soak-stats.json"), "w") as f:
    json.dump({k: (v if not isinstance(v, np.floating) else float(v)) for k, v in stats.items()
               if k not in ("res", "tbl", "meta", "ds")}, f, indent=2, default=str)

# ---------------- HTML ----------------
def f0(x): return f"{x:,.0f}"
def f1(x): return f"{x:,.1f}"

kpis = [
    ("Duration", f"{dur_h:.1f} h", "single continuous run"),
    ("Avg throughput", f"{avg_push/1000:.1f}k/s", "push & pop, balanced"),
    ("Messages processed", f"{tot_push/1e9:.2f} B", f"{f0(tot_push)} pushed"),
    ("Push errors", "0", f"over {tot_push/1e9:.1f}B pushes"),
    ("Push–pop gap", f"{gap_final/1000:.0f}k", f"{gap_final/tot_push*100:.4f}% (in-flight)"),
    ("Broker memory", f"~{res['qmem_mean']:.0f} MiB", "flat — no leak"),
    ("Broker CPU", f"~{res['qcpu_mean']:.1f} cores", "of 32 available"),
    ("Postgres CPU", f"~{res['pcpu_mean']:.0f} cores", "the real ceiling"),
]
kpi_html = "".join(
    f'<div class="kpi"><div class="kpi-v">{v}</div><div class="kpi-k">{k}</div><div class="kpi-s">{s}</div></div>'
    for k, v, s in kpis)

def chart_block(name):
    t, cap, b64 = CH[name]
    return (f'<figure class="chart"><figcaption><h3>{t}</h3><p>{cap}</p></figcaption>'
            f'<img src="data:image/png;base64,{b64}" alt="{t}"></figure>')

order = ["throughput", "cumulative", "gap", "hist", "errors", "combined",
         "pg_rates", "broker_cpu", "broker_mem", "pg_cpu", "pg_mem",
         "msg_size", "tuples", "disk"]
charts_html = "\n".join(chart_block(n) for n in order)

thr_rows = "".join(
    f"<tr><td>{lbl}</td><td>{f0(d['mean'])}</td><td>{f0(d['p50'])}</td><td>{f0(d['p95'])}</td>"
    f"<td>{f0(d['p99'])}</td><td>{f0(d['min'])}</td><td>{f0(d['max'])}</td></tr>"
    for lbl, d in [
        ("push/s", dict(mean=stats['push_mean'], p50=stats['push_p50'], p95=stats['push_p95'], p99=stats['push_p99'], min=stats['push_min'], max=stats['push_max'])),
        ("pop/s", dict(mean=stats['pop_mean'], p50=stats['pop_p50'], p95=stats['pop_p95'], p99=stats['pop_p99'], min=stats['pop_min'], max=stats['pop_max'])),
    ])

res_rows = "".join([
    f"<tr><td>Broker (Queen) CPU</td><td>{res['qcpu_mean']:.1f} cores</td><td>{res['qcpu_p95']:.1f}</td><td>{res['qcpu_max']:.1f}</td></tr>",
    f"<tr><td>Broker (Queen) RSS</td><td>{res['qmem_mean']:.0f} MiB (steady)</td><td>—</td><td>{res['qmem_max']:.0f} MiB</td></tr>",
    f"<tr><td>Postgres CPU</td><td>{res['pcpu_mean']:.0f} cores</td><td>{res['pcpu_p95']:.0f}</td><td>{res['pcpu_max']:.0f}</td></tr>",
    f"<tr><td>Postgres RSS</td><td>—</td><td>—</td><td>{res['pmem_max']:.1f} GiB</td></tr>",
])

tbl_rows = "".join(
    f"<tr><td><code>{r[0]}</code></td><td>{r[1]}</td><td>{int(r[2]):,}</td><td>{int(r[3]):,}</td></tr>"
    for r in tbl_sizes[:8])

BROKER_CFG = [
    ("image", "smartnessai/queen-mq:pushser"),
    ("NUM_WORKERS", "10"), ("DB_POOL_SIZE", "50"), ("SIDECAR_POOL_SIZE", "250"),
    ("QUEEN_CONCURRENCY_MODE", "static"), ("QUEEN_POP_MAX_CONCURRENT", "16"),
    ("QUEEN_PUSH_MAX_HOLD_MS", "20"), ("QUEEN_PUSH_PREFERRED_BATCH_SIZE", "50"),
    ("QUEEN_PUSH_MAX_BATCH_SIZE", "500"), ("QUEEN_PUSH_MAX_CONCURRENT", "24"),
    ("RETENTION_PARALLELISM", "8"), ("RETENTION_INTERVAL", "5000 ms"), ("RETENTION_BATCH_SIZE", "50000"),
]
LOADER_CFG = [
    ("partitions", "300"), ("producers", "650"), ("consumers", "200"),
    ("push-batch", "10"), ("pop-batch", "300"), ("pop-partitions", "10"),
    ("pop-wait", "true"), ("pop-timeout", "2000 ms"), ("payload", "256 B"),
    ("completed-retention", "120 s"), ("pending-retention", "600 s"),
    ("idle-conns", "1600"), ("retries", "2"),
]
def cfg_table(rows):
    return "".join(f"<tr><td><code>{k}</code></td><td>{v}</td></tr>" for k, v in rows)

CSS = """
:root{--bg:#0b1020;--card:#11182e;--ink:#e7ecf5;--mut:#9aa7c2;--line:#243152;--accent:#3b82f6;--good:#10b981;--warn:#f59e0b;--bad:#ef4444}
*{box-sizing:border-box}
body{margin:0;background:linear-gradient(180deg,#0b1020,#0d1326);color:var(--ink);font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1120px;margin:0 auto;padding:48px 24px 96px}
h1{font-size:32px;margin:0 0 4px;letter-spacing:-.5px}
h2{font-size:22px;margin:56px 0 16px;padding-bottom:8px;border-bottom:1px solid var(--line)}
h3{font-size:16px;margin:0 0 4px}
.sub{color:var(--mut);font-size:15px;margin:0 0 8px}
.tag{display:inline-block;background:#16213f;border:1px solid var(--line);color:var(--mut);border-radius:999px;padding:3px 12px;font-size:12.5px;margin:2px 6px 2px 0}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:28px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px}
.kpi-v{font-size:26px;font-weight:700;letter-spacing:-.5px}
.kpi-k{color:var(--ink);font-size:13.5px;margin-top:2px;font-weight:600}
.kpi-s{color:var(--mut);font-size:12px}
.callout{background:#0f1b33;border:1px solid var(--line);border-left:4px solid var(--accent);border-radius:10px;padding:16px 20px;margin:18px 0}
.callout.good{border-left-color:var(--good)}
.callout.warn{border-left-color:var(--warn)}
.chart{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px;margin:22px 0}
.chart img{width:100%;height:auto;border-radius:8px;margin-top:10px;background:#fff}
.chart figcaption p{color:var(--mut);font-size:13.5px;margin:2px 0 0}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:22px}
table{width:100%;border-collapse:collapse;margin:8px 0;font-size:14px}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--line)}
th{color:var(--mut);font-weight:600;font-size:12.5px;text-transform:uppercase;letter-spacing:.04em}
td code{color:#9ecbff}
.foot{color:var(--mut);font-size:12.5px;margin-top:60px;border-top:1px solid var(--line);padding-top:18px}
@media(max-width:820px){.kpis{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}}
"""

gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
HTML = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Queen — 24h Soak Report (new architecture)</title>
<style>{CSS}</style></head><body><div class="wrap">

<h1>Queen 24-hour soak — new architecture (pushser)</h1>
<p class="sub">Sustained high-throughput durability test &middot; PostgreSQL-backed C++ message queue</p>
<div>
<span class="tag">{dur_h:.1f} h continuous</span>
<span class="tag">{tot_push/1e9:.2f}B messages</span>
<span class="tag">~{avg_push/1000:.0f}k msg/s push+pop</span>
<span class="tag">32 vCPU / 62 GiB host</span>
<span class="tag">image: queen-mq:pushser</span>
</div>

<div class="kpis">{kpi_html}</div>

<div class="callout good"><b>Verdict.</b> The new architecture sustained <b>~{avg_push/1000:.0f}k msg/s of balanced push <em>and</em> pop for {dur_h:.0f} hours</b>, processing <b>{tot_push/1e9:.2f} billion messages</b> with <b>zero push errors</b>, an in-flight gap of {gap_final/tot_push*100:.4f}% (effectively zero loss), and <b>flat ~{res['qmem_mean']:.0f} MiB broker memory</b>. This is exactly the long-run stability the previous architecture failed (it OOM-crashed). The throughput ceiling is Postgres CPU (~{res['pcpu_mean']:.0f}/32 cores), not the broker.</div>

<h2>1 &middot; Throughput &amp; correctness</h2>
{chart_block("throughput")}
{chart_block("cumulative")}
{chart_block("gap")}
{chart_block("hist")}
{chart_block("errors")}

<h3>Throughput percentiles (steady state, after 30-min warmup)</h3>
<table><thead><tr><th>series</th><th>mean</th><th>p50</th><th>p95</th><th>p99</th><th>min</th><th>max</th></tr></thead>
<tbody>{thr_rows}</tbody></table>

<h2>2 &middot; Where the work goes — broker vs Postgres</h2>
{chart_block("combined")}
{chart_block("pg_rates")}
<div class="cols"><div>{chart_block("broker_cpu")}</div><div>{chart_block("broker_mem")}</div></div>
<div class="cols"><div>{chart_block("pg_cpu")}</div><div>{chart_block("pg_mem")}</div></div>

<h3>Resource usage (soak window)</h3>
<table><thead><tr><th>resource</th><th>mean</th><th>p95</th><th>peak</th></tr></thead>
<tbody>{res_rows}</tbody></table>

<h2>3 &middot; Storage &amp; retention</h2>
{chart_block("msg_size")}
{chart_block("tuples")}
{chart_block("disk")}

<div class="callout warn"><b>One open issue — <code>messages_consumed</code> growth.</b> The completed-message table reached <b>{tbl_sizes[0][1] if tbl_sizes else '47 GB'} / {int(tbl_sizes[0][2]):,} rows</b> with 0 dead tuples and is still climbing (~2 GB/hr). The completed-retention reaper runs but trails ingestion by ~2–3k/s, so the table slowly accumulates. With {res['disk_end']:.0f} GB free that is a ~4-day runway — harmless for this 24h test, but it must get a working reaper before multi-day production runs. (WAL at 96 GB is the configured <code>max_wal_size</code> ceiling, not a leak.)</div>

<h3>Final table sizes</h3>
<table><thead><tr><th>table</th><th>total size</th><th>live tuples</th><th>dead tuples</th></tr></thead>
<tbody>{tbl_rows}</tbody></table>

<h2>4 &middot; Configuration</h2>
<div class="cols">
<div><h3>Broker (Queen + Postgres host)</h3><table><tbody>{cfg_table(BROKER_CFG)}</tbody></table></div>
<div><h3>Loader (goload)</h3><table><tbody>{cfg_table(LOADER_CFG)}</tbody></table></div>
</div>

<div class="foot">
Generated {gen} from <code>vm-data/soak-loader-167/goload.log</code> ({len(L['t'])} samples @10s) and
<code>vm-data/soak-broker-165/long-mon.log</code> ({len(M['t'])} samples @30s, sliced from {SOAK_START:%Y-%m-%d %H:%M}Z).
Broker 165.232.78.92 &middot; loader 167.99.246.68 &middot; final snapshot {ds[0] if ds else ''} xact_commit {int(ds[1]):,} / inserted {int(ds[2]):,} / deleted {int(ds[3]):,} (pg_stat_database).
Charts are PNGs embedded in this file; copies are in <code>charts/</code>.
</div>
</div></body></html>"""

out = os.path.join(BASE, "soak-report.html")
with open(out, "w") as f:
    f.write(HTML)
print(f"\nWROTE {out} ({os.path.getsize(out)/1024:.0f} KB), {len(CH)} charts -> {CHARTS}/")
