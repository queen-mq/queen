#!/usr/bin/env python3
"""profdiff.py <runs/profcmp-A> <runs/profcmp-B> [comm] : the flat profiles of two builds (perf-n1d.sh), in CORES
(share x the cores the broker used over the record): by thread name, then the symbols whose cores differ most
(B - A), for the threads named `comm` (default tokio-rt-worker; `all` for every thread)."""
import os, re, sys

def cores(d):
    m = re.search(r"cores (\d+)e-2", open(os.path.join(d, "cores.txt")).read())
    return int(m.group(1)) / 100.0

def table(path, ncols):
    rows = {}
    for l in open(path, errors="replace"):
        if l.startswith("#") or not l.strip():
            continue
        m = re.match(r"\s*([\d.]+)%\s+(.*)$", l.rstrip())
        if not m:
            continue
        pct, rest = float(m.group(1)), m.group(2)
        if ncols == 1:
            key = rest.strip()
        else:
            parts = re.split(r"\s{2,}", rest.strip(), maxsplit=2)
            if len(parts) < 3:
                continue
            sym = re.sub(r"^\[[.k]\]\s*", "", parts[2])
            key = (parts[0], parts[1], sym)
        rows[key] = rows.get(key, 0.0) + pct
    return rows

def short(sym):
    out, depth = [], 0
    for ch in sym:
        if ch == "<":
            depth += 1
            continue
        if ch == ">":
            depth -= 1
            continue
        if depth == 0:
            out.append(ch)
    s = re.sub(r"::h[0-9a-f]{16}$", "", "".join(out))
    return (s or sym)[:110]

a, b = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")
comm = sys.argv[3] if len(sys.argv) > 3 else "tokio-rt-worker"
ca, cb = cores(a), cores(b)
print(f"A = {os.path.basename(a)}: {ca:.2f} cores   B = {os.path.basename(b)}: {cb:.2f} cores   B - A = {cb - ca:+.2f}")
ta, tb = table(os.path.join(a, "flat-comm.txt"), 1), table(os.path.join(b, "flat-comm.txt"), 1)
print("\nby thread name (cores):")
for k in sorted(set(ta) | set(tb), key=lambda k: -(tb.get(k, 0) * cb + ta.get(k, 0) * ca)):
    x, y = ta.get(k, 0) * ca / 100, tb.get(k, 0) * cb / 100
    if max(x, y) >= 0.05:
        print(f"  {k:<18} A {x:6.2f}   B {y:6.2f}   {y - x:+6.2f}")
sa, sb = table(os.path.join(a, "flat-comm-sym.txt"), 3), table(os.path.join(b, "flat-comm-sym.txt"), 3)
def by_sym(t, c):
    out = {}
    for (cm, dso, sym), pct in t.items():
        if comm != "all" and cm != comm:
            continue
        k = (dso if dso != "queen" else "") + ("  " if dso != "queen" else "") + short(sym)
        out[k] = out.get(k, 0.0) + pct * c / 100
    return out
xa, xb = by_sym(sa, ca), by_sym(sb, cb)
print(f"\n{comm}: listed symbols A {sum(xa.values()):.2f} cores, B {sum(xb.values()):.2f} cores")
diff = sorted(set(xa) | set(xb), key=lambda k: -abs(xb.get(k, 0) - xa.get(k, 0)))
print("\nsymbols by |B - A| (cores):")
for k in diff[:45]:
    print(f"  {xb.get(k, 0) - xa.get(k, 0):+6.3f}   A {xa.get(k, 0):6.3f}   B {xb.get(k, 0):6.3f}   {k}")
print("\ntop symbols in B (cores):")
for k in sorted(xb, key=lambda k: -xb[k])[:30]:
    print(f"  B {xb[k]:6.3f}   A {xa.get(k, 0):6.3f}   {k}")
