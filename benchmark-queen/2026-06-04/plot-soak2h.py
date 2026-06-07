import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import re, os
import numpy as np

os.chdir(os.path.dirname(os.path.abspath(__file__)))
OFFSET = timedelta(hours=2)            # UTC -> CEST (Italian local)
DAY = "2026-06-06"
SOAK_START = datetime.strptime("2026-06-06T13:04:00", "%Y-%m-%dT%H:%M:%S")  # UTC

# --- push/pop: direct from the loader log (goload), sampled every 10s ---
# line: [HH:MM:SS] push=  N/s pop=  M/s | tot push=.. pop=.. | errs ..
gl_re = re.compile(r"\[(\d\d:\d\d:\d\d)\]\s+push=\s*(\d+)/s\s+pop=\s*(\d+)/s")
t_lo, push_lo, pop_lo = [], [], []
for line in open("soak-2h/goload.log"):
    m = gl_re.search(line)
    if not m:
        continue
    t = datetime.strptime(DAY + "T" + m.group(1), "%Y-%m-%dT%H:%M:%S")  # UTC
    t_lo.append(t + OFFSET)
    push_lo.append(int(m.group(2)))
    pop_lo.append(int(m.group(3)))

# --- CPU + delete(retention) from long-mon, filtered to the soak window ---
lm_re = re.compile(
    r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)Z\s*\|\s*"
    r"queen=([0-9.]+)%.*?postgres=([0-9.]+)%.*?"
    r"xc_ins_del_size_dead_live=\s+(\d+)\s+(\d+)\s+(\d+)\s+"
)
samples = {}  # ts(UTC) -> (queen_vcpu, pg_vcpu, n_tup_ins, n_tup_del)
for line in open("soak-2h/long-mon.log"):
    m = lm_re.search(line)
    if not m:
        continue
    ts = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
    if ts < SOAK_START:
        continue
    samples[ts] = (float(m.group(2)) / 100.0, float(m.group(3)) / 100.0,
                   int(m.group(4)), int(m.group(5)))

ts_sorted = sorted(samples)
t_del, del_r = [], []
for i in range(1, len(ts_sorted)):
    t0, t1 = ts_sorted[i - 1], ts_sorted[i]
    dt = (t1 - t0).total_seconds()
    if dt <= 0:
        continue
    dd = samples[t1][3] - samples[t0][3]
    t_del.append(t1 + OFFSET)
    del_r.append(np.nan if (dd < 0 or dt > 180) else dd / dt)

t_cpu = [t + OFFSET for t in ts_sorted]
qv = [samples[t][0] for t in ts_sorted]
pgv = [samples[t][1] for t in ts_sorted]

def smooth(y, w=5):
    y = np.asarray(y, float); out = np.copy(y); h = w // 2
    for i in range(len(y)):
        seg = y[max(0, i - h):min(len(y), i + h + 1)]; seg = seg[~np.isnan(seg)]
        out[i] = np.mean(seg) if len(seg) else np.nan
    return out

# --- plot (2 panels: throughput + CPU) ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
ax1.plot(t_lo, smooth(push_lo), label="push", color="#1f77b4", lw=1.7)
ax1.plot(t_lo, smooth(pop_lo), label="pop", color="#2ca02c", lw=1.6)
ax1.plot(t_del, smooth(del_r), label="delete / retention (PG n_tup_del)", color="#d62728", lw=1.2, alpha=0.75)
ax1.set_ylabel("messages / s")
_durh = (t_lo[-1] - t_lo[0]).total_seconds() / 3600 if t_lo else 0
ax1.set_title(f"Queen pushser (pop static=16) — {_durh:.1f}h soak: push/pop throughput  (benchq: 300 partitions, 650 prod / 200 cons, push-batch 10 / pop-batch 300)")
ax1.legend(loc="lower left", fontsize=9); ax1.grid(alpha=0.3); ax1.set_ylim(bottom=0)
ax1.axhline(100000, color="grey", ls=":", lw=0.9, alpha=0.7)

ax2.plot(t_cpu, qv, label="Queen broker", color="#ff7f0e", lw=1.6)
ax2.plot(t_cpu, pgv, label="Postgres", color="#9467bd", lw=1.6)
ax2.set_ylabel("vCPU (host has 32)")
ax2.set_title("Broker / Postgres CPU")
ax2.legend(loc="upper right"); ax2.grid(alpha=0.3); ax2.set_ylim(bottom=0)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
ax2.set_xlabel("time (CEST)")

fig.tight_layout()
out = "soak-2026-06-06-pushser.png"
fig.savefig(out, dpi=110)
print("saved", out)

def st(name, y):
    yy = [v for v in y if v == v]
    if yy:
        print(f"{name}: avg {sum(yy)/len(yy):,.0f}/s  peak {max(yy):,.0f}/s  ({len(yy)} pts)")
st("push  ", push_lo); st("pop   ", pop_lo); st("delete", del_r)
if qv:
    print(f"Queen vCPU avg {sum(qv)/len(qv):.1f} (peak {max(qv):.1f}); "
          f"PG vCPU avg {sum(pgv)/len(pgv):.1f} (peak {max(pgv):.1f})")
if t_lo:
    print(f"window {t_lo[0]:%H:%M}-{t_lo[-1]:%H:%M} CEST")
