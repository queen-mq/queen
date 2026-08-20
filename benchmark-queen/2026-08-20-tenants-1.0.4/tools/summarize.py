#!/usr/bin/env python3
"""Summarize a bench-sampler CSV the way the 2026-07-24 tenant report did:
mean/peak CPU in cores for PG and the broker, broker RSS, DB size, and the
commit rate — so the new numbers drop straight into the July table."""
import sys, csv, statistics as st

path = sys.argv[1]
rows = list(csv.DictReader(open(path)))
if not rows:
    print("(empty csv)"); sys.exit(0)

# Drop the first 20% as warm-up (provisioning + connection storm), like the
# July report did when it quoted steady-state means.
warm = max(1, len(rows) // 5)
body = rows[warm:] or rows

def col(name, src=body):
    out = []
    for r in src:
        v = (r.get(name) or "").strip()
        if v:
            try: out.append(float(v))
            except ValueError: pass
    return out

def line(label, vals, div=1.0, unit=""):
    if not vals:
        print(f"  {label:<22} n/a"); return
    print(f"  {label:<22} mean {st.mean(vals)/div:7.2f}   peak {max(vals)/div:7.2f}  {unit}")

pg, qn = col("pg_cpu_pct"), col("queen_cpu_pct")
print(f"file: {path}  samples={len(rows)} (steady={len(body)}, warm-up {warm} dropped)")
line("PG CPU", pg, 100.0, "cores")
line("Queen CPU", qn, 100.0, "cores")
if pg and qn:
    tot = [a + b for a, b in zip(pg, qn)]
    line("TOTAL CPU", tot, 100.0, "cores")
line("PG RSS", col("pg_mem_mb"), 1024.0, "GB")
line("Queen RSS", col("queen_mem_mb"), 1024.0, "GB")
line("active backends", col("active_backends"), 1.0, "")

db = col("db_size_bytes")
if db:
    print(f"  {'DB size':<22} start {db[0]/2**20:8.1f} MB  end {db[-1]/2**20:8.1f} MB")

xc = col("xact_commit_cum", rows)
ts = col("epoch_ms", rows)
if len(xc) > 1 and len(ts) > 1:
    dt = (ts[-1] - ts[0]) / 1000.0
    if dt > 0:
        print(f"  {'commits/s':<22} {(xc[-1]-xc[0])/dt:7.1f}  over {dt:.0f}s")

fs = col("wal_fsyncs_cum", rows)
if len(fs) > 1 and len(ts) > 1:
    dt = (ts[-1] - ts[0]) / 1000.0
    if dt > 0:
        print(f"  {'WAL fsyncs/s':<22} {(fs[-1]-fs[0])/dt:7.1f}")

waits = [(r.get("top_wait") or "").strip() for r in body]
waits = [w for w in waits if w]
if waits:
    top = sorted({w: waits.count(w) for w in set(waits)}.items(), key=lambda x: -x[1])[:3]
    print("  top waits:            " + ", ".join(f"{w} {c*100//len(waits)}%" for w, c in top))
