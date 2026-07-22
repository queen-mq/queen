#!/usr/bin/env python3
# analyze.py RUNDIR RUNLABEL
# Parses collected artifacts for one run and prints the deliverable metrics.
import sys, os, re, csv, glob
from collections import defaultdict

RUN = sys.argv[1]
LABEL = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(RUN.rstrip('/'))

def p(x): print(x)

# ---------- goload per-minute rates ----------
# line: [HH:MM:SS] push=  637032/s pop=  636713/s | tot push=38221900 pop=38202800 | errs p=2 c=3063 empty=2
gl = os.path.join(RUN, 'goload.log')
reports = []
if os.path.exists(gl):
    for ln in open(gl):
        m = re.search(r'\[(\d\d:\d\d:\d\d)\]\s*push=\s*(\d+)/s\s*pop=\s*(\d+)/s.*?tot push=(\d+)\s*pop=(\d+).*?errs p=(\d+)\s*c=(\d+)\s*empty=(\d+)', ln)
        if m:
            reports.append(dict(t=m.group(1), push=int(m.group(2)), pop=int(m.group(3)),
                                totpush=int(m.group(4)), totpop=int(m.group(5)),
                                errp=int(m.group(6)), errc=int(m.group(7)), empty=int(m.group(8))))

p(f"\n########## {LABEL} ##########")
p("\n== goload per-minute (loader-side authoritative) ==")
p(f"{'time':>10} {'push/s':>10} {'pop/s':>10} {'side_avg/s':>11} {'errp':>6} {'errc':>7}")
for r in reports:
    p(f"{r['t']:>10} {r['push']:>10} {r['pop']:>10} {(r['push']+r['pop'])//2:>11} {r['errp']:>6} {r['errc']:>7}")
if reports:
    # sustained = minute 2..end (skip first report = warmup minute 1)
    sust = reports[1:] if len(reports) > 1 else reports
    sp = sum(r['push'] for r in sust)/len(sust)
    so = sum(r['pop'] for r in sust)/len(sust)
    p(f"  sustained avg (rpt2..end, n={len(sust)}): push={sp:,.0f}/s pop={so:,.0f}/s side_avg={(sp+so)/2:,.0f}/s")

# ---------- msg/commit from dbstat (commits) + goload (msgs); CPU from dockstats ----------
# NOTE: mon.csv push_msg_s/pop_msg_s scrape returns 0 in :fix build (metric-name
# mismatch), but mon.csv commit_s and pg CPU are fine. We use dbstat.csv (clean 60s
# xact_commit series) + goload totals for msg/commit -- the robust path.
dbf0=os.path.join(RUN,'dbstat.csv')
comm=[]
if os.path.exists(dbf0):
    for l in open(dbf0):
        f=l.strip().split(',')
        if f and f[0].isdigit() and len(f)>=3:
            comm.append((int(f[0]), int(f[2])))
p("\n== commits (dbstat xact_commit, 60s deltas) ==")
cps_list=[]
for i in range(1,len(comm)):
    dc=comm[i][1]-comm[i-1][1]; cps=dc/max(1,(comm[i][0]-comm[i-1][0])); cps_list.append(cps)
    p(f"  t={comm[i][0]} d_commit={dc:>8,} => {cps:>8,.0f} commit/s")
if reports and cps_list:
    push_final=reports[-1]['totpush']; pop_final=reports[-1]['totpop']
    # total msgs / total commits over run (use goload finals if present)
    gl_txt=open(os.path.join(RUN,'goload.log')).read() if os.path.exists(os.path.join(RUN,'goload.log')) else ''
    mf=re.search(r'\[final\] pushed=(\d+) popped=(\d+)', gl_txt)
    if mf:
        tot_msg=int(mf.group(1))+int(mf.group(2)); one_side=int(mf.group(1))
    else:
        tot_msg=push_final+pop_final; one_side=push_final
    avg_cps=sum(cps_list)/len(cps_list)
    # run duration from goload total / final rate approx: use reports span
    dur = 600 if len(reports)>=9 else (300 if len(reports)>=4 else 60*len(reports))
    total_msg_s = tot_msg/dur
    p(f"  avg commit/s (steady): {avg_cps:,.0f}")
    p(f"  total msgs (push+pop): {tot_msg:,} over ~{dur}s => {total_msg_s:,.0f} msg/s total")
    p(f"  ==> msg/commit TOTAL (push+pop)/commits = {total_msg_s/avg_cps:.1f}")
    p(f"  ==> msg/commit PER-SIDE (one side)/commits = {(one_side/dur)/avg_cps:.1f}   [historical 39-99 convention]")

# CPU from dockstats.csv
dks=os.path.join(RUN,'dockstats.csv')
if os.path.exists(dks):
    bc=[]; pc=[]
    for l in open(dks):
        f=l.split()
        if len(f)>=3 and f[1]=='r6682': bc.append(float(f[2].rstrip('%')))
        if len(f)>=3 and f[1]=='qbench-pg': pc.append(float(f[2].rstrip('%')))
    # keep active samples (pg cpu > 200%)
    pca=[x for x in pc if x>200]; bca=bc[:len(pca)] if pca else bc
    if pca:
        p(f"\n== CPU (dockstats, active samples) ==")
        p(f"  broker r6682: avg={sum(bca)/len(bca):.0f}%  max={max(bca):.0f}%")
        p(f"  postgres:     avg={sum(pca)/len(pca):.0f}%  max={max(pca):.0f}%   (3200% = 32 cores; {sum(pca)/len(pca)/32:.0f}% of box)")

# ---------- wal_stat.csv: WAL bytes, per-minute ----------
def parse_blocks(path):
    """yield (tag, header_list, [row_lists]) blocks separated by === lines."""
    if not os.path.exists(path): return
    cur_tag=None; header=None; rows=[]
    for ln in open(path):
        ln=ln.rstrip('\n')
        if ln.startswith('==='):
            if cur_tag is not None: yield (cur_tag, header, rows)
            cur_tag=ln.strip('= '); header=None; rows=[]
        elif ln.strip()=='':
            continue
        else:
            if header is None: header=ln.split(',')
            else: rows.append(ln.split(','))
    if cur_tag is not None: yield (cur_tag, header, rows)

ws = os.path.join(RUN, 'wal_stat.csv')
wal_pts=[]  # (tag, wal_bytes, wal_records, wal_buffers_full)
for tag,hdr,rows in parse_blocks(ws):
    if not rows: continue
    r=rows[0]
    d=dict(zip(hdr,r))
    wal_pts.append((tag, int(d['wal_bytes']), int(d['wal_records']), int(d['wal_buffers_full'])))
p("\n== pg_stat_wal per-minute deltas ==")
p(f"{'block':>16} {'d_wal_MB':>10} {'d_wal_records':>14} {'d_buffers_full':>14}")
for i in range(1,len(wal_pts)):
    db=(wal_pts[i][1]-wal_pts[i-1][1])/1e6
    dr=wal_pts[i][2]-wal_pts[i-1][2]
    dbf=wal_pts[i][3]-wal_pts[i-1][3]
    p(f"{wal_pts[i][0]:>16} {db:>10.1f} {dr:>14,} {dbf:>14,}")

# ---------- io_stat.csv: WAL write_time + fsync_time per-minute ----------
iost = os.path.join(RUN, 'io_stat.csv')
# columns: backend_type,object,context,reads,read_bytes,read_time,writes,write_bytes,write_time,writebacks,writeback_time,extends,extend_bytes,extend_time,hits,evictions,reuses,fsyncs,fsync_time,stats_reset
io_pts=[]  # (tag, sum_write_time_ms, sum_fsync_time_ms, sum_writes, sum_fsyncs, sum_write_bytes)
for tag,hdr,rows in parse_blocks(iost):
    idx={c:i for i,c in enumerate(hdr)}
    swt=sft=sw=sf=swb=0.0
    for r in rows:
        if len(r)<len(hdr): continue
        if r[idx['object']]!='wal': continue
        def f(c):
            v=r[idx[c]]; return float(v) if v not in ('',None) else 0.0
        swt+=f('write_time'); sft+=f('fsync_time'); sw+=f('writes'); sf+=f('fsyncs'); swb+=f('write_bytes')
    io_pts.append((tag,swt,sft,sw,sf,swb))
p("\n== pg_stat_io WAL timing per-minute deltas (track_wal_io_timing=on) ==")
p(f"{'block':>16} {'write_time_ms':>14} {'fsync_time_ms':>14} {'writes':>10} {'fsyncs':>10} {'wt+ft_ms':>10}")
for i in range(1,len(io_pts)):
    dwt=io_pts[i][1]-io_pts[i-1][1]; dft=io_pts[i][2]-io_pts[i-1][2]
    dw=io_pts[i][3]-io_pts[i-1][3]; df=io_pts[i][4]-io_pts[i-1][4]
    p(f"{io_pts[i][0]:>16} {dwt:>14,.0f} {dft:>14,.0f} {dw:>10,.0f} {df:>10,.0f} {dwt+dft:>10,.0f}")

# ---------- WAL bytes per message ----------
# messages per minute from goload totpush/totpop deltas aligned by count; use overall.
if wal_pts and len(wal_pts)>=2 and reports:
    total_wal = wal_pts[-1][1]-wal_pts[1][1] if len(wal_pts)>2 else wal_pts[-1][1]-wal_pts[0][1]
    # messages over same window ~ use goload totals last-first
    msgs = (reports[-1]['totpush']-reports[0]['totpush']) + (reports[-1]['totpop']-reports[0]['totpop'])
    if msgs>0:
        p(f"\n  WAL bytes / (push+pop) msg (whole run): {total_wal/ ((wal_pts[-1][2]-wal_pts[0][2]) if False else 1):.0f}  -- see per-min below")
    # simpler: per-minute wal_bytes / (side_avg*2*60)
    p("\n== WAL bytes per message (per-minute, wal_bytes_delta / (push+pop msgs that minute)) ==")
    p(f"{'block':>16} {'d_wal_MB':>10} {'push+pop_msgs':>14} {'WALbytes/msg':>13}")
    # align: wal_pts[i] tag t=EPOCH ; reports are per 60s too. Use msg-per-min from rates.
    for i in range(1,len(wal_pts)):
        db=wal_pts[i][1]-wal_pts[i-1][1]
        # msgs that minute: approximate via corresponding goload report if available
        ri = i-1
        if ri < len(reports):
            mm=(reports[ri]['push']+reports[ri]['pop'])*60
        else:
            mm=0
        wbpm = db/mm if mm else 0
        p(f"{wal_pts[i][0]:>16} {db/1e6:>10.1f} {mm:>14,} {wbpm:>13.1f}")

# ---------- waits ----------
wf=None
for cand in ('waits.csv','waitsA.csv','waitsB.csv'):
    pth=os.path.join(RUN,cand)
    if os.path.exists(pth) and os.path.getsize(pth)>0:
        wf=pth; break
if wf:
    agg=defaultdict(int); persec_txid=[]
    persec=defaultdict(lambda: defaultdict(int))
    for ln in open(wf):
        parts=ln.strip().split(',')
        if len(parts)!=4: continue
        t,wt,we,c=parts
        try: c=int(c)
        except: continue
        agg[f"{wt}:{we}"]+=c
        persec[t][f"{wt}:{we}"]+=c
    p(f"\n== waits ({os.path.basename(wf)}) total active-backend-seconds by event (top 12) ==")
    tot=sum(agg.values())
    for k,v in sorted(agg.items(),key=lambda x:-x[1])[:12]:
        p(f"  {k:<28} {v:>10,}  ({100*v/tot:4.1f}%)")
    # max transactionid lock in any second (wedge signal)
    mx=0
    for t,d in persec.items():
        mx=max(mx,d.get('Lock:transactionid',0))
    p(f"  >>> max Lock:transactionid in any 1s sample: {mx}  (wedge threshold >50 sustained)")

# ---------- dbstat: deadlocks, db size ----------
dbf=os.path.join(RUN,'dbstat.csv')
if os.path.exists(dbf):
    lines=[l.strip() for l in open(dbf) if l.strip()]
    p("\n== dbstat (db size / commits / deadlocks) ==")
    for l in lines:
        if l.startswith('FINAL'): p("  "+l)
    # deadlocks = last field of periodic rows
    dl=0
    for l in lines:
        f=l.split(',')
        if f[0].isdigit() and len(f)>=5:
            dl=int(f[4])
    p(f"  deadlocks (final counter): {dl}")
