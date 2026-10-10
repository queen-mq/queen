#!/usr/bin/env python3
"""Every 10 s, one line: wall time, the broker's memory, CPU ticks per thread name, the rows in RAM,
the machine's free memory and PSI. Ticks are cumulative (USER_HZ): the reader takes the differences."""
import os, sys, time, urllib.request, collections
pid, priv = sys.argv[1], sys.argv[2]
def metric(text, name):
    for l in text.split("\n"):
        if l.startswith(name + " "):
            return l.rsplit(" ", 1)[1]
    return "na"
while True:
    t = time.time()
    try:
        th = collections.Counter()
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                s = open(f"/proc/{pid}/task/{tid}/stat").read()
                comm = s[s.index("(") + 1:s.rindex(")")].replace(" ", "_")
                f = s[s.rindex(")") + 2:].split()
                th[comm] += int(f[11]) + int(f[12])
            except Exception:
                pass
        st = dict(l.split(":", 1) for l in open(f"/proc/{pid}/status").read().strip().split("\n") if ":" in l)
        anon = st.get("RssAnon", "0").split()[0]; rss = st.get("VmRSS", "0").split()[0]
        mem = dict((l.split(":")[0], l.split()[1]) for l in open("/proc/meminfo"))
        try:
            m = urllib.request.urlopen(f"http://{priv}:6632/metrics/prometheus", timeout=3).read().decode()
        except Exception:
            m = ""
        rows = metric(m, 'queen_raft_store_ram_rows{ks="txns",kind="live"}')
        cold = metric(m, "queen_consume_cold_claims_total")
        top = " ".join(f"th_{k}={v}" for k, v in th.most_common(14))
        total = sum(th.values())
        print(f"t={t:.1f} anon_kb={anon} rss_kb={rss} rows={rows} cold={cold} memfree_kb={mem.get('MemFree')} cached_kb={mem.get('Cached')} dirty_kb={mem.get('Dirty')} ticks={total} {top}", flush=True)
    except Exception as e:
        print(f"t={t:.1f} error={e!r}", flush=True)
    time.sleep(max(0.0, 10.0 - (time.time() - t)))
