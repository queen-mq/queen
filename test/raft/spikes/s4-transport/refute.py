#!/usr/bin/env python3
"""Numbers for the 2026-09-18 refutation round of MEMO.md (WP-0.6).

Reads the two original VM passes (results/vm, results/vm-b), the cells added
on 2026-09-18 on the SAME binary (results/vm-c) and the pacer comparison
(results/vm-pace), and prints every ratio the revised memo quotes.

    python3 refute.py
"""
import json, sys

def load(p):
    return [json.loads(l) for l in open(p) if l.strip()]

A, B = load('results/vm/results.jsonl'), load('results/vm-b/results.jsonl')
C, P = load('results/vm-c/results.jsonl'), load('results/vm-pace/results.jsonl')

def pick(rows, t, tls, mac, rate, conns=None, pace=None):
    out = [r for r in rows if r['transport'] == t and r['tls'] == tls and r['mac'] == mac
           and r['rate_msgs'] == rate
           and (conns is None or r['conns_cfg'] == conns)
           and (pace is None or r.get('pace', 'timer') == pace)]
    assert len(out) == 1, (t, tls, mac, rate, conns, pace, len(out))
    return out[0]

def mean(field, *a, **k):
    return (pick(A, *a, **k)[field] + pick(B, *a, **k)[field]) / 2

L = 'leader_cpu_us_per_msg'
print("== passes A+B (2026-09-17), leader CPU us/msg @50k ==")
tcp   = mean(L, 'tcp',  False, False, 50000, 2)
tcpm  = mean(L, 'tcp',  False, True,  50000, 2)
tcpt  = mean(L, 'tcp',  True,  False, 50000, 2)
http  = mean(L, 'http', False, False, 50000, 64)
httpm = mean(L, 'http', False, True,  50000, 64)
httptm= mean(L, 'http', True,  True,  50000, 64)
tcp1  = mean(L, 'tcp',  False, False, 50000, 1)
tcp8  = mean(L, 'tcp',  False, False, 50000, 8)
for n, v in [('framed', tcp), ('framed+mac', tcpm), ('framed+tls', tcpt), ('http', http),
             ('http+mac', httpm), ('https+mac', httptm), ('framed@1', tcp1), ('framed@8', tcp8)]:
    print(f"  {n:12} {v:.3f}")

print("\n== the day-2 cells, same binary (results/vm-c) ==")
ctl   = pick(C, 'tcp',  False, False, 50000, 2)[L]
https = pick(C, 'http', True,  False, 50000, 64)[L]
https20 = pick(C, 'http', True, False, 20000, 64)[L]
pool2 = pick(C, 'http', False, False, 50000, 2)[L]
day = ctl / tcp
print(f"  framed control {ctl:.3f}  (pass A/B {tcp:.3f}; day factor {day:.3f})")
print(f"  HTTPS plain @50k {https:.3f}   HTTPS plain @20k {https20:.3f}")
print(f"  HTTP pool_max_idle=2 @50k {pool2:.3f}  conns opened "
      f"{pick(C,'http',False,False,50000,2)['leader_conns_opened']}")

print("\n== like-for-like pairs, leader CPU (framed / HTTP) ==")
print(f"  plain        @50k  {tcp/http:.3f}   (framed 2 sockets vs HTTP 15-49)")
print(f"  authenticated@50k  {tcpm/httpm:.3f}")
print(f"  encrypted    @50k  {tcpt*day/https:.3f}  same-day corrected "
      f"({tcpt*day:.3f} vs {https:.3f});  uncorrected {tcpt/https:.3f}")
print(f"  memo's old 'encrypted' (tls-only vs tls+hmac): {tcpt/httptm:.3f}")
tcp20, http20 = mean(L,'tcp',False,False,20000,2), mean(L,'http',False,False,20000,64)
tcpt20 = mean(L,'tcp',True,False,20000,2)
print(f"  plain        @20k  {tcp20/http20:.3f}")
print(f"  encrypted    @20k  {tcpt20*day/https20:.3f}  (same-day corrected); "
      f"memo's old {tcpt20/mean(L,'http',True,True,20000,64):.3f}")

print("\n== socket count ==")
print(f"  framed@8 vs HTTP@15-49   {tcp8/http:.3f}")
print(f"  framed@2 vs HTTP@2-idle  {ctl/pool2:.3f}  (HTTP churned "
      f"{pick(C,'http',False,False,50000,2)['leader_conns_opened']} connections in 60 s)")

print("\n== pacer (results/vm-pace, 40 s each, one binary, same hour) ==")
for pace in ('timer', 'spin'):
    t = pick(P, 'tcp', False, False, 50000, 2, pace=pace)
    h = pick(P, 'http', False, False, 50000, 64, pace=pace)
    print(f"  {pace:5}  framed {t[L]:.3f}  http {h[L]:.3f}  ratio {t[L]/h[L]:.3f} | "
          f"p50 {t['rt_send_p50_us']}/{h['rt_send_p50_us']} us | "
          f"due_p50 {t['rt_due_p50_us']}/{h['rt_due_p50_us']} us | "
          f"http conns {t['leader_conns_open']}/{h['leader_conns_open']}")
ts = pick(P,'tcp',False,False,50000,2,pace='spin')[L]; tt = pick(P,'tcp',False,False,50000,2,pace='timer')[L]
hs = pick(P,'http',False,False,50000,64,pace='spin')[L]; ht = pick(P,'http',False,False,50000,64,pace='timer')[L]
print(f"  coalescing share: framed +{100*(ts-tt)/tt:.0f} % CPU/msg without bursts, http +{100*(hs-ht)/ht:.0f} %")

print("\n== integrity checks that actually hold ==")
rows = A + B + C + P
print(f"  rows {len(rows)}; cmds_bad total {sum(r['cmds_bad'] for r in rows)}; "
      f"commands {sum(r['cmds_ok'] for r in rows):,}")
worst = max(abs(r['achieved_msgs_s'] - r['rate_msgs']) / r['rate_msgs'] for r in rows)
print(f"  worst achieved-vs-offered deviation {100*worst:.4f} %")
bad = [r for r in rows if abs(r['cmds_ok'] - r['achieved_cmds_s'] * r['secs']) / (r['achieved_cmds_s'] * r['secs']) > 0.01]
print(f"  rows where cmds_ok != achieved_cmds_s x secs (>1 %): {len(bad)}")
