#!/usr/bin/env python3
# check_lines.py <dir>: every window/[final] line of every *.log must match Queen's grid_report regex and the full
# SPEC §3 format; every [final] is followed by load_cpu; parsed numbers equal the -out JSON windows.
import re, sys, os, json, glob
Q = re.compile(r'^\[(\d\d):(\d\d):(\d\d)\] offered=\s*(\d+)/s achieved=\s*(\d+)/s shed=\s*(\d+)/s .*?p99=\s*([\d.]+).*?ack=\s*(\d+)/s.*?e2e p50=([\d.]+) p99=([\d.]+)')
F = r'-?\d+\.\d{2}'
W = re.compile(r'^\[\d\d:\d\d:\d\d\] offered=[ \d]{9}/s achieved=[ \d]{9}/s shed=[ \d]{9}/s inflight=[ \d]{6,} \| p50=[ \d.]{7} p99=[ \d.]{8} p999=[ \d.]{8} ms \| push=\d+ pop=\d+ lag=-?\d+ \| errs push=\d+ pop=\d+ empty=\d+ gor=\d+ \| ack=[ \d]{9}/s ackErr=\d+ ackAvg=\d+\.\d\dms \| e2e p50=\d+\.\d\d p99=\d+\.\d\d p999=\d+\.\d\d n=\d+ \| e2e_local p50=\d+\.\d\d p99=\d+\.\d\d n=\d+$')
FIN = re.compile(r'^\[final\] offered=\d+ achieved=\d+ shed=\d+ \(msgs: offered=\d+ achieved=\d+ shed=\d+\) pushErr=\d+ \| pushed=\d+ popped=\d+ lag=-?\d+ \| popErr=\d+ empty=\d+ \| overall p50=\d+\.\d\d p99=\d+\.\d\d p999=\d+\.\d\d ms \| acked=\d+ ackErr=\d+ ackLag=-?\d+ ackAvg=\d+\.\d\dms \| e2e p50=\d+\.\d\d p99=\d+\.\d\d p999=\d+\.\d\d ms$')
CPU = re.compile(r'^load_cpu=\d+\.\d%$')
files = sorted(glob.glob(os.path.join(sys.argv[1], '*.log')))
nwin = nfin = bad = files_with = jchk = jbad = 0
for f in files:
    lines = open(f, errors='replace').read().splitlines()
    wins = []
    for i, l in enumerate(lines):
        if re.match(r'^\[\d\d:\d\d:\d\d\]', l):
            nwin += 1
            q, w = Q.match(l), W.match(l)
            if not q or not w:
                bad += 1; print('BAD window', f, l[:160])
            else:
                wins.append(q)
        elif l.startswith('[final]'):
            nfin += 1
            if not FIN.match(l) or i + 1 >= len(lines) or not CPU.match(lines[i + 1]):
                bad += 1; print('BAD final', f, l[:160], '| next:', lines[i+1] if i+1 < len(lines) else None)
    if wins: files_with += 1
    j = f[:-4] + '.json'
    if wins and os.path.exists(j):
        try: jw = json.load(open(j))['windows']
        except Exception: continue
        if len(jw) != len(wins):
            jbad += 1; print('COUNT', f, len(jw), len(wins)); continue
        for q, w in zip(wins, jw):
            jchk += 1
            exp = (round(w['offered_per_s']), round(w['achieved_per_s']), round(w['shed_per_s']), '%.2f' % w['p99_ms'], round(w['acked_per_s']), '%.2f' % w['e2e_p50_ms'], '%.2f' % w['e2e_p99_ms'])
            got = (int(q[4]), int(q[5]), int(q[6]), '%.2f' % float(q[7]), int(q[8]), '%.2f' % float(q[9]), '%.2f' % float(q[10]))
            if exp != got:
                jbad += 1; print('MISMATCH', f, exp, got)
print(f'{len(files)} logs ({files_with} with windows): {nwin} window lines, {nfin} [final] lines; {bad} not matching; {jchk} windows cross-checked against -out JSON, {jbad} mismatches')
