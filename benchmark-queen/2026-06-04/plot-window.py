import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import re, os
import numpy as np

os.chdir(os.path.dirname(os.path.abspath(__file__)))
OFFSET = timedelta(hours=2)            # UTC -> CEST (Italian local)
WIN_START = datetime(2026, 6, 4, 19, 45)   # CEST
WIN_END   = datetime(2026, 6, 5, 2, 0)     # CEST  (== memory-exhaustion / OOM edge)
OOM = datetime(2026, 6, 5, 2, 3)           # CEST  (memory hit 99%)

# --- accurate source: long-mon logs (direct PG counters, ~30s) ---
line_re = re.compile(
    r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)Z\s*\|\s*"
    r"queen=([0-9.]+)%.*?postgres=([0-9.]+)%.*?"
    r"xc_ins_del_size_dead_live=\s+(\d+)\s+(\d+)\s+(\d+)\s+"
)
samples = {}
for fn in ("chartdata/long-mon.log", "chartdata/long-mon2.log"):
    if not os.path.exists(fn):
        continue
    for line in open(fn):
        m = line_re.search(line)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
        samples[ts] = (float(m.group(2)) / 100.0, float(m.group(3)) / 100.0,
                       int(m.group(5)), int(m.group(6)))
ts_sorted = sorted(samples)

t_rate, push_r, del_r = [], [], []
for i in range(1, len(ts_sorted)):
    t0, t1 = ts_sorted[i - 1], ts_sorted[i]
    dt = (t1 - t0).total_seconds()
    if dt <= 0:
        continue
    di = samples[t1][2] - samples[t0][2]
    dd = samples[t1][3] - samples[t0][3]
    t_rate.append(t1 + OFFSET)
    if di < 0 or dd < 0 or dt > 180:   # counter reset / gap
        push_r.append(np.nan); del_r.append(np.nan)
    else:
        push_r.append(di / dt); del_r.append(dd / dt)

def smooth(y, w=5):
    y = np.asarray(y, float); out = np.copy(y); h = w // 2
    for i in range(len(y)):
        seg = y[max(0, i - h):min(len(y), i + h + 1)]; seg = seg[~np.isnan(seg)]
        out[i] = np.mean(seg) if len(seg) else np.nan
    return out

push_s, del_s = smooth(push_r), smooth(del_r)
t_cpu = [t + OFFSET for t in ts_sorted]
qv = [samples[t][0] for t in ts_sorted]
pgv = [samples[t][1] for t in ts_sorted]

# --- pop from worker_metrics (per-minute) ---
t_pop, pop = [], []
for line in open("chartdata/wm.tsv"):
    p = line.strip().split("|")
    if len(p) >= 3 and p[2].lstrip("-").isdigit():
        t_pop.append(datetime.strptime(p[0], "%Y-%m-%dT%H:%M") + OFFSET)
        pop.append(int(p[2]) / 60.0)

# --- clip every series to the window ---
def win(ts, *ys):
    keep = [i for i, t in enumerate(ts) if WIN_START <= t <= WIN_END]
    return ([ts[i] for i in keep],) + tuple([y[i] for i in keep] for y in ys)

t_rate, push_s, del_s = win(t_rate, push_s, del_s)
t_cpu, qv, pgv = win(t_cpu, qv, pgv)
t_pop, pop = win(t_pop, pop)

# --- plot ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
ax1.plot(t_rate, push_s, label="push (PG n_tup_ins)", color="#1f77b4", lw=1.8)
ax1.plot(t_rate, del_s, label="delete / retention (PG n_tup_del)", color="#d62728", lw=1.4, alpha=0.8)
if pop:
    ax1.plot(t_pop, pop, label="pop (worker_metrics)", color="#2ca02c", lw=1.3, ls="--")
ax1.set_ylabel("messages / s")
ax1.set_title("Queen 0.16 soak — 19:45\u201302:00 CEST (bench-q100, 100 partitions, batch=10)")
ax1.legend(loc="lower left", fontsize=9); ax1.grid(alpha=0.3); ax1.set_ylim(bottom=0)
ax1.axhline(100000, color="grey", ls=":", lw=0.9, alpha=0.7)

ax2.plot(t_cpu, qv, label="Queen broker", color="#ff7f0e", lw=1.6)
ax2.plot(t_cpu, pgv, label="Postgres", color="#9467bd", lw=1.6)
ax2.set_ylabel("vCPU (host has 32)")
ax2.set_title("Broker / Postgres CPU")
ax2.legend(loc="upper right"); ax2.grid(alpha=0.3); ax2.set_ylim(bottom=0)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
ax2.set_xlabel("time (CEST)")

for ax in (ax1, ax2):
    ax.set_xlim(WIN_START, WIN_END)
    ax.axvline(OOM, color="black", ls="--", lw=1.0, alpha=0.6)
ax1.text(OOM, ax1.get_ylim()[1] * 0.04, " mem 99%\n \u2192 OOM", fontsize=8, color="black", va="bottom", ha="right")

fig.tight_layout()
fig.savefig("push-pop-cpu-1945-0200.png", dpi=110)
print("saved push-pop-cpu-1945-0200.png")

def stats(name, t, y):
    yy = [v for v in y if v == v]
    if yy and t:
        print(f"{name}: {len(yy)} pts {t[0]:%H:%M}-{t[-1]:%H:%M} CEST | avg {sum(yy)/len(yy):,.0f}/s  peak {max(yy):,.0f}/s")
stats("push  ", t_rate, push_s)
stats("delete", t_rate, del_s)
stats("pop   ", t_pop, pop)
if qv:
    print(f"Queen vCPU avg {sum(qv)/len(qv):.1f} (peak {max(qv):.1f}); PG vCPU avg {sum(pgv)/len(pgv):.1f} (peak {max(pgv):.1f})")
