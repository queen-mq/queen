#!/usr/bin/env python3
"""gapprof.py <probe dir> [top] : one broker's gapprobe.sh window (2026-10-14).
1. CPU per thread name from /proc (cpu-a -> cpu-b), threads of one name summed, with their count.
2. perf (stacks.txt.gz): per thread name, its share of the samples, the leaf functions most often on the CPU
   (exclusive) and the broker's own functions most often on the stack (inclusive).
3. gap.txt (bpftrace): off-CPU time per thread name (seconds of waiting per second of window, summed over the
   threads of that name) with its top kernel stacks, file syncs and file writes per thread name, wake-up -> run delay.
Thread names are grouped: a trailing -<n> or digits are dropped (queen-lane-3 -> queen-lane)."""
import collections, gzip, os, re, sys

D = sys.argv[1]
TOP = int(sys.argv[2]) if len(sys.argv) > 2 else 6
HZ = 100


def grp(comm):
    return re.sub(r"[-_]?\d+$", "", comm)


def demangle(s):
    if s.startswith("_ZN"):
        i, out = 3, []
        while i < len(s) and s[i].isdigit():
            j = i
            while s[j].isdigit():
                j += 1
            n = int(s[i:j]); out.append(s[j:j + n]); i = j + n
        out = [o for o in out if not re.fullmatch(r"h[0-9a-f]{16}", o)]
        out = [o.replace("$LT$", "<").replace("$GT$", ">").replace("$u20$", " ").replace("..", "::") for o in out]
        return "::".join(out)
    return s


def short(full):
    parts = [p for p in re.split(r"::", full) if p and not p.startswith("{{") and p != "{closure}"]
    return "::".join(parts[-3:]) if parts else full[:60]


# ---- 1. /proc CPU
def cpu(name):
    p = os.path.join(D, f"cpu-{name}")
    if not os.path.exists(p):
        return {}, None
    t = float(open(p + ".t").read())
    out = {}
    for l in open(p):
        f = l.split()
        if len(f) >= 4:
            out[f[0]] = (f[1], int(f[2]) + int(f[3]))
    return out, t


a, ta = cpu("a"); b, tb = cpu("b")
role = "?"
try:
    role = re.search(r'"role":"(\w+)"', open(os.path.join(D, "health")).read()).group(1)
except Exception:
    pass
print(f"#### {D}  role={role}")
if a and b:
    dt = tb - ta
    per = collections.defaultdict(lambda: [0.0, 0])
    for tid, (comm, v) in b.items():
        if tid in a:
            per[grp(comm)][0] += (v - a[tid][1]) / HZ / dt
            per[grp(comm)][1] += 1
    tot = sum(v[0] for v in per.values())
    print(f"CPU (/proc, {dt:.1f} s): {tot:.2f} cores | " + ", ".join(
        f"{k} {v[0]:.2f}" + (f" (x{v[1]})" if v[1] > 1 else "") for k, v in sorted(per.items(), key=lambda kv: -kv[1][0]) if v[0] >= 0.05))

# ---- 2. perf stacks
P = os.path.join(D, "stacks.txt.gz")
if os.path.exists(P):
    samples = []  # (group, [frames leaf first])
    cur = None
    for l in gzip.open(P, "rt", errors="replace"):
        if not l.strip():
            if cur:
                samples.append(cur)
            cur = None
            continue
        if not l[0].isspace():
            m = re.match(r"^(.*?)\s+(\d+)\s+(\d+\.\d+):", l)
            cur = (grp(m.group(1).strip()), []) if m else None
            continue
        if cur is not None:
            f = l.strip().split(None, 1)
            ip, sym = f[0], (f[1] if len(f) > 1 else "?")
            sym = re.sub(r"\s*\(.*\)$", "", sym)
            k = ip.startswith("ffffffff")
            cur[1].append(("[k] " if k else "") + demangle(sym))
    if cur:
        samples.append(cur)
    n = len(samples)
    by = collections.defaultdict(list)
    for g, fr in samples:
        by[g].append(fr)
    print(f"perf: {n} samples")
    for g, frs in sorted(by.items(), key=lambda kv: -len(kv[1]))[:12]:
        leaf = collections.Counter(short(fr[0]) if fr else "?" for fr in frs)
        inc = collections.Counter()
        for fr in frs:
            seen = set()
            for x in fr:
                if ("queen" in x or "rsm::" in x or "openraft" in x) and not x.startswith("[k]"):
                    s = short(x)
                    if s not in seen:
                        seen.add(s); inc[s] += 1
        print(f"  {g}: {len(frs) / n:.1%} of samples")
        print("     leaf: " + "; ".join(f"{k} {v / len(frs):.0%}" for k, v in leaf.most_common(TOP)))
        print("     broker frames: " + "; ".join(f"{k} {v / len(frs):.0%}" for k, v in inc.most_common(TOP)))

# ---- 3. bpftrace
G = os.path.join(D, "gap.txt")
if os.path.exists(G):
    txt = open(G).read()
    t0 = re.search(r"BEGIN (\d+)", txt); t1 = re.search(r"END (\d+)", txt)
    win = (int(t1.group(1)) - int(t0.group(1))) / 1e9 if t0 and t1 else 1.0
    off = collections.defaultdict(lambda: [0, 0])
    for m in re.finditer(r"@off_us\[(.*?), (\d+)\]: (\d+)", txt):
        off[grp(m.group(1))][0] += int(m.group(3)); off[grp(m.group(1))][1] += 1
    print(f"bpftrace window {win:.1f} s. Off-CPU (s of waiting per s, summed over the threads of the name):")
    print("  " + ", ".join(f"{k} {v[0] / 1e6 / win:.2f} (x{v[1]})" for k, v in sorted(off.items(), key=lambda kv: -kv[1][0])[:16]))
    # stacks: blocks "@off_stack_us[name, \n stack lines \n]: N"
    st = collections.defaultdict(list)
    for m in re.finditer(r"@off_stack_us\[(.*?), \n(.*?)\]: (\d+)", txt, re.S):
        frames = [re.sub(r"\+\d+$", "", x.strip()) for x in m.group(2).strip().splitlines() if x.strip()]
        frames = [f for f in frames if f not in ("perf_trace_sched_switch", "__schedule", "schedule", "io_schedule",
                                                 "preempt_schedule", "preempt_schedule_thunk", "preempt_schedule_irq",
                                                 "schedule_hrtimeout_range_clock", "schedule_timeout")]
        st[grp(m.group(1))].append((int(m.group(3)), frames))
    for g in ("queen-raft-sync", "queen-raft-log", "queen-qlog-writ", "queen-qlog-fsyn", "queen-rsm-apply", "consume-serve",
              "queen-planner", "queen-raft", "queen-rsm-ckpt"):
        if g in st:
            tot = sum(x[0] for x in st[g]) or 1
            tops = sorted(st[g], key=lambda x: -x[0])[:3]
            print(f"  {g} waits in: " + " | ".join(f"{100 * u / tot:.0f}% " + "<".join(re.sub(r"\+\d+$", "", f) for f in fr[:4]) for u, fr in tops))
    for m in re.finditer(r"@sync_n\[(.*?), (.*?)\]: (\d+)", txt):
        comm, probe, cnt = m.group(1), m.group(2), int(m.group(3))
        tot = re.search(re.escape(f"@sync_total_us[{comm}, {probe}]: ") + r"(\d+)", txt)
        mx = re.search(re.escape(f"@sync_max_us[{comm}, {probe}]: ") + r"(\d+)", txt)
        tu = int(tot.group(1)) if tot else 0
        print(f"  sync {comm} {probe.split(':')[-1].replace('sys_exit_', '')}: {cnt / win:,.0f}/s, mean {tu / max(cnt, 1) / 1000:.2f} ms, max {int(mx.group(1)) / 1000 if mx else 0:.1f} ms, busy {tu / 1e6 / win:.0%}")
    for m in re.finditer(r"@pwrite_n\[(.*?)\]: (\d+)", txt):
        comm, cnt = m.group(1), int(m.group(2))
        tot = re.search(re.escape(f"@pwrite_total_us[{comm}]: ") + r"(\d+)", txt)
        by_ = re.search(re.escape(f"@pwrite_bytes[{comm}]: ") + r"(\d+)", txt)
        print(f"  pwrite {comm}: {cnt / win:,.0f}/s, {int(by_.group(1)) / 1e6 / win if by_ else 0:,.0f} MB/s, mean {int(tot.group(1)) / max(cnt, 1) if tot else 0:.0f} us, busy {int(tot.group(1)) / 1e6 / win if tot else 0:.0%}")
    rq = {grp(m.group(1)): int(m.group(2)) for m in re.finditer(r"@runq_total_us\[(.*?)\]: (\d+)", txt)}
    q = {k: v for k, v in rq.items() if k.startswith(("queen", "tokio", "consume", "qlog"))}
    print("  wake-up -> run delay (s per s): " + ", ".join(f"{k} {v / 1e6 / win:.3f}" for k, v in sorted(q.items(), key=lambda kv: -kv[1])[:10]))
