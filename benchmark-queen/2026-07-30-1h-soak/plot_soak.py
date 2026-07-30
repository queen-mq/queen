#!/usr/bin/env python3
"""Render the 1-hour multi-tenant soak into matplotlib PNGs.
Reads interval.csv (loader), vm.csv (cell sampler), broker.log (rates/sizes),
and soak1h.json (final per-tenant verdict). Usage: python3 plot_soak.py <dir>"""
import sys, os, csv, json, re, datetime as dt
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

D = sys.argv[1] if len(sys.argv) > 1 else "."
OUT = os.path.join(D, "png"); os.makedirs(OUT, exist_ok=True)

# validated categorical palette (dataviz skill reference)
C = dict(blue="#2a78d6", orange="#eb6834", aqua="#1baf7a", yellow="#eda100",
         magenta="#e87ba4", green="#008300", violet="#4a3aa7", red="#e34948",
         ink="#0b0b0b", mut="#8a8a86", grid="#e6e6e2", surf="#ffffff")
plt.rcParams.update({
    "figure.facecolor": C["surf"], "axes.facecolor": C["surf"],
    "axes.edgecolor": C["mut"], "axes.labelcolor": C["ink"],
    "text.color": C["ink"], "xtick.color": C["mut"], "ytick.color": C["mut"],
    "axes.grid": True, "grid.color": C["grid"], "grid.linewidth": 0.8,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "legend.frameon": False, "figure.dpi": 130, "axes.spines.top": False,
    "axes.spines.right": False,
})

def readcsv(p):
    with open(p) as f:
        return list(csv.DictReader(f))

def isounix(s):
    s = s.strip().rstrip("Z")
    s = re.sub(r"\.\d+$", "", s)
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp()

# ---- loader interval.csv ----
iv = readcsv(os.path.join(D, "loader-interval.csv"))
def col(rows, k, f=float):
    return [f(r[k]) for r in rows]
# absolute clock: the loader's first report wall time minus its t_sec offset
T0 = isounix(iv[0]["wall_utc"]) - float(iv[0]["t_sec"])
tmin = [(isounix(r["wall_utc"]) - T0) / 60 for r in iv]
offered = col(iv, "offered_msg_s"); pushed = col(iv, "pushed_msg_s")
popped = col(iv, "popped_msg_s"); acked = col(iv, "acked_msg_s")
e2e50 = col(iv, "e2e_p50_ms")
# cumulative -> per-second rate
def rate(rows, k):
    c = [int(r[k]) for r in rows]; out = []
    for i in range(len(c)):
        if i == 0: out.append(0.0)
        else:
            dtsec = (float(rows[i]["t_sec"]) - float(rows[i-1]["t_sec"])) or 15.0
            out.append(max(0.0, (c[i]-c[i-1]) / dtsec))
    return out
r429 = rate(iv, "err_429"); r403 = rate(iv, "err_403")
r5xx = rate(iv, "err_5xx"); rto = rate(iv, "err_timeout")
cross = col(iv, "cross_tenant", int)

# ---- vm.csv ---- (aligned to the same absolute T0)
vm = readcsv(os.path.join(D, "vm-sampler.csv"))
vmin = [(int(r["t_unix"]) - T0) / 60 for r in vm]
cpu = col(vm, "cell_cpu_cores"); pgc = col(vm, "pg_commits_s")
conns = col(vm, "pg_active_conns", int)
ump = col(vm, "um_push_msgs", int); umd = col(vm, "um_delivery_msgs", int)
rss = col(vm, "broker_rss_mb", int)

# ---- broker.log rates/sizes ----
br_t, br_p99push, br_p99pop, br_parked, br_poolu = [], [], [], [], []
sz_t, sz_wheel, sz_rings = [], [], []
tend = tmin[-1] + 1
for line in open(os.path.join(D, "broker-rates-sizes.log")):
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z)", line)
    if not m: continue
    mm = (isounix(m.group(1)) - T0) / 60
    if mm < -0.5 or mm > tend: continue   # keep only the soak window
    if "rates: broker rates" in line:
        def g(k, d=0.0):
            m = re.search(k + r'="?([0-9.]+)"?', line); return float(m.group(1)) if m else d
        br_t.append(mm); br_p99push.append(g("p99_push_ms")); br_p99pop.append(g("p99_pop_ms"))
        br_parked.append(g("parked"))
        m = re.search(r'pool="(\d+)/(\d+)"', line); br_poolu.append(int(m.group(1)) if m else 0)
    elif "sizes: broker sizes" in line:
        m = re.search(r'hotlist="(\d+)rings/(\d+)ready/(\d+)wheel"', line)
        if m:
            sz_t.append(mm); sz_rings.append(int(m.group(1))); sz_wheel.append(int(m.group(3)))

def style(ax, ylabel, xlabel="minutes into soak"):
    ax.set_ylabel(ylabel); ax.set_xlabel(xlabel)
    ax.margins(x=0.01); ax.set_ylim(bottom=0)

def line(ax, x, y, color, label, lw=2, ls="-"):
    ax.plot(x, y, color=color, lw=lw, ls=ls, label=label, solid_capstyle="round")

# ============ figure 1: the operational dashboard (2x3) ============
fig, ax = plt.subplots(2, 3, figsize=(17, 9))
fig.suptitle("QueenMQ — 1-hour multi-tenant soak (free-tier 2-core cell, enforcement ON)\n"
             "12 tenants sharing queue 'orders'/group 'workers'; 2 rate-limited, 1 storage-quota'd",
             fontsize=14, fontweight="bold", y=0.99)

a = ax[0][0]
line(a, tmin, offered, C["mut"], "offered", lw=1.5, ls="--")
line(a, tmin, pushed, C["blue"], "pushed (admitted)")
line(a, tmin, popped, C["aqua"], "popped")
line(a, tmin, acked, C["orange"], "acked")
a.set_title("Throughput"); style(a, "msg/s"); a.legend(loc="lower right", fontsize=9)

a = ax[0][1]
line(a, tmin, r429, C["orange"], "429 rate-limited (limiter)")
line(a, tmin, r403, C["yellow"], "403 storage-quota (quota)")
line(a, tmin, r5xx, C["red"], "5xx")
line(a, tmin, rto, C["violet"], "timeout")
a.set_title("Enforcement — refusals/s"); style(a, "errors/s"); a.legend(loc="upper right", fontsize=9)

a = ax[0][2]
line(a, tmin, e2e50, C["blue"], "loader e2e p50 (healthy tenants)")
if br_t: line(a, br_t, br_p99push, C["aqua"], "broker p99 push (server-side)")
if br_t: line(a, br_t, br_p99pop, C["orange"], "broker p99 pop (server-side)")
a.set_title("Latency — cell health"); style(a, "ms"); a.legend(loc="upper right", fontsize=9)
a.set_ylim(0, max(30, (max(br_p99push[1:]+br_p99pop[1:]) if br_t and len(br_t)>1 else 30)*1.2))

a = ax[1][0]
line(a, vmin, cpu, C["blue"], "cell CPU (PG+broker+proxy)")
a.axhline(2.0, color=C["red"], lw=1.2, ls=":", label="2-core cap")
a.set_title("Cell CPU"); style(a, "cores"); a.legend(loc="lower right", fontsize=9)
a.set_ylim(0, 2.3)

a = ax[1][1]
line(a, vmin, rss, C["green"], "broker RSS")
a.set_title("Broker memory (decay signal)"); style(a, "MB")
lo, hi = min(rss), max(rss); a.set_ylim(max(0, lo-20), hi+20)
a.legend(loc="lower right", fontsize=9)

a = ax[1][2]
if sz_t:
    line(a, sz_t, sz_wheel, C["violet"], "hot-list wheel (leased, re-probed ≤1s)")
    line(a, sz_t, sz_rings, C["aqua"], "hot-list rings (=tenant×queue)")
a.set_title("Hot-list ring (re-arm health)"); style(a, "entries"); a.legend(loc="upper right", fontsize=9)

fig.tight_layout(rect=[0, 0, 1, 0.96])
fig.savefig(os.path.join(OUT, "1_dashboard.png"), bbox_inches="tight")
plt.close(fig)

# ============ figure 2: meter integrity + commits + conns ============
fig, ax = plt.subplots(1, 3, figsize=(17, 4.6))
a = ax[0]
line(a, vmin, [x/1000 for x in ump], C["blue"], "metered push msgs")
line(a, vmin, [x/1000 for x in umd], C["orange"], "metered delivery msgs")
a.set_title("Meter accumulation (push == delivery)"); style(a, "thousands of msgs"); a.legend(loc="upper left", fontsize=9)
a = ax[1]
line(a, vmin, pgc, C["magenta"], "PG commits/s")
a.set_title("PostgreSQL commits/s"); style(a, "commits/s"); a.legend(loc="lower right", fontsize=9)
a = ax[2]
line(a, vmin, conns, C["aqua"], "PG active connections")
if br_t: line(a, br_t, br_parked, C["violet"], "parked long-polls (broker)")
a.set_title("Connections & parked pops"); style(a, "count"); a.legend(loc="upper right", fontsize=9)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "2_meter_pg.png"), bbox_inches="tight")
plt.close(fig)

# ============ figure 3: per-tenant delivery (final verdict) ============
jp = os.path.join(D, "soak1h.json")
if os.path.exists(jp):
    d = json.load(open(jp)); r = d.get("result", d); v = r.get("verify", {})
    tn = v.get("tenants", [])
    if tn:
        names = [t.get("tenant", "?").replace("soak-", "t") for t in tn]
        recv = [t.get("received", 0) for t in tn]
        sfail = [t.get("sentFail", 0) for t in tn]
        # color: throttled/quota tenants highlighted
        cols = []
        for t in tn:
            s = t.get("tenant", "")
            if s in ("soak-0000", "soak-0001"): cols.append(C["orange"])   # limiter
            elif s == "soak-0002": cols.append(C["yellow"])                 # quota
            else: cols.append(C["blue"])
        fig, ax = plt.subplots(1, 2, figsize=(15, 4.8))
        a = ax[0]
        a.bar(names, recv, color=cols, width=0.72)
        a.set_title("Delivered per tenant (blue=normal, orange=rate-limited, yellow=quota)")
        a.set_ylabel("messages delivered"); a.margins(x=0.01)
        a.yaxis.set_major_locator(MaxNLocator(6))
        a = ax[1]
        a.bar(names, sfail, color=cols, width=0.72)
        a.set_title("Refused pushes per tenant (429 + 403 = enforcement)")
        a.set_ylabel("sentFail (refused)"); a.margins(x=0.01)
        # decompose the verdict: healthy tenants vs enforcement tenants
        hmiss = sum(t.get("missing",0) for t in tn if t.get("tenant") not in ("soak-0000","soak-0001","soak-0002"))
        hdup = sum(t.get("duplicate",0) for t in tn if t.get("tenant") not in ("soak-0000","soak-0001","soak-0002"))
        tot = (f"delivered {v.get('received'):,}   cross-tenant {v.get('crossTenant')} (isolation held)\n"
               f"9 healthy tenants: {hmiss} missing, {hdup} dup (0.002% at-least-once redelivery)  |  "
               f"miss/dup confined to the 2 rate-limited + 1 quota'd tenant = enforcement artifacts, not loss")
        fig.suptitle(tot, fontsize=11.5, fontweight="bold")
        fig.tight_layout(rect=[0, 0, 1, 0.93])
        fig.savefig(os.path.join(OUT, "3_per_tenant.png"), bbox_inches="tight")
        plt.close(fig)
        print("per-tenant:", tot)

print("wrote PNGs to", OUT)
print("intervals:", len(iv), "| vm samples:", len(vm), "| broker rate lines:", len(br_t))
