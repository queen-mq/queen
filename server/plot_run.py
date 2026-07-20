#!/usr/bin/env python3
import csv, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

csv_path = sys.argv[1] if len(sys.argv) > 1 else "runAcd.csv"
out_path = sys.argv[2] if len(sys.argv) > 2 else "runAcd.png"

t, preq, oreq, pmsg, omsg, com, bcpu, pcpu = ([] for _ in range(8))
with open(csv_path) as f:
    for r in csv.DictReader(f):
        # drop the trailing tail sample (load winding down)
        t.append(int(r["t_s"]))
        preq.append(float(r["push_req_s"]))
        oreq.append(float(r["pop_req_s"]))
        pmsg.append(float(r["push_msg_s"]))
        omsg.append(float(r["pop_msg_s"]))
        com.append(float(r["commit_s"]))
        bcpu.append(float(r["broker_cpu"]) / 100.0)
        pcpu.append(float(r["pg_cpu"]) / 100.0)

# drop last point if PG cpu collapsed (load end artifact)
if len(t) > 2 and pcpu[-1] < pcpu[-2] * 0.3:
    for a in (t, preq, oreq, pmsg, omsg, com, bcpu, pcpu):
        a.pop()

PUSH, POP, COMMIT, BROK, PG = "#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e"
fig, ax = plt.subplots(4, 1, figsize=(11, 13), sharex=True)

ax[0].plot(t, [v/1e3 for v in pmsg], "-o", color=PUSH, label="push msg/s")
ax[0].plot(t, [v/1e3 for v in omsg], "-o", color=POP, label="pop msg/s")
ax[0].set_ylabel("messages/s (k)")
ax[0].set_title("Queen Rust — Segments engine + cross-request fusion  (100 partitions, hold 30ms, commit_delay=200µs)")
ax[0].set_ylim(0, max(max(pmsg), max(omsg))/1e3 * 1.25)
ax[0].legend(loc="lower right"); ax[0].grid(True, alpha=0.3)
avg_pm, avg_om = sum(pmsg)/len(pmsg)/1e3, sum(omsg)/len(omsg)/1e3
ax[0].axhline(avg_pm, color=PUSH, ls="--", alpha=0.4)
ax[0].axhline(avg_om, color=POP, ls="--", alpha=0.4)
ax[0].text(t[0], avg_pm, f" avg push {avg_pm:.0f}k", color=PUSH, va="bottom", fontsize=9)
ax[0].text(t[0], avg_om, f" avg pop {avg_om:.0f}k", color=POP, va="top", fontsize=9)

ax[1].plot(t, preq, "-o", color=PUSH, label="push req/s")
ax[1].plot(t, oreq, "-o", color=POP, label="pop req/s")
ax[1].set_ylabel("requests/s")
ax[1].set_ylim(0, max(preq)*1.25)
ax[1].legend(loc="lower right"); ax[1].grid(True, alpha=0.3)

ax[2].plot(t, com, "-o", color=COMMIT, label="commits/s (segments flushed)")
ax[2].set_ylabel("commits/s")
ax[2].set_ylim(0, max(com)*1.25)
ax[2].legend(loc="lower right"); ax[2].grid(True, alpha=0.3)

ax[3].plot(t, bcpu, "-o", color=BROK, label="Queen broker CPU")
ax[3].plot(t, pcpu, "-o", color=PG, label="Postgres CPU")
ax[3].set_ylabel("CPU (cores)")
ax[3].set_xlabel("time (s)")
ax[3].set_ylim(0, max(max(bcpu), max(pcpu))*1.3)
ax[3].legend(loc="lower right"); ax[3].grid(True, alpha=0.3)

fig.tight_layout()
fig.savefig(out_path, dpi=130)
print(f"wrote {out_path}")
print(f"avg push {avg_pm:.0f}k msg/s, pop {avg_om:.0f}k msg/s, "
      f"commits {sum(com)/len(com):.0f}/s, broker {sum(bcpu)/len(bcpu):.1f} cores, pg {sum(pcpu)/len(pcpu):.1f} cores")
