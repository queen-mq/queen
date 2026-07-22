import csv, os, glob
base="/private/tmp/claude-502/-Users-alice-Work-queen/66ba70b8-0959-4d59-b60c-d9b45e6ebab3/scratchpad/vmab"

def cpu_stats(run):
    f=os.path.join(base,run,f"cpu-{run}.csv")
    rows=list(csv.DictReader(open(f)))
    # parse cpu% by container
    per={"r6682":[],"qbench-pg":[]}
    ts=[]
    for r in rows:
        try:
            c=float(r["cpu_perc"].replace("%",""))
        except: continue
        per[r["name"]].append((int(r["epoch"]),c))
    out={}
    for name,vals in per.items():
        vals.sort()
        # drop first 4 samples (~60s warmup) and idle-tail samples (<20%)
        trimmed=[c for i,(e,c) in enumerate(vals) if i>=4 and c>=20]
        out[name]=sum(trimmed)/len(trimmed) if trimmed else 0
        out[name+"_n"]=len(trimmed)
    return out

def wait_profile(run):
    # find the waits csv
    f=glob.glob(os.path.join(base,run,f"waits-{run}.csv"))[0]
    agg={}
    total=0
    for r in csv.DictReader(open(f)):
        try: cnt=int(r["active_count"])
        except: continue
        t=r["wait_event_type"]
        agg[t]=agg.get(t,0)+cnt
        total+=cnt
    # top events
    ev={}
    for r in csv.DictReader(open(f)):
        try: cnt=int(r["active_count"])
        except: continue
        k=r["wait_event_type"]+":"+r["wait_event"]
        ev[k]=ev.get(k,0)+cnt
    topev=sorted(ev.items(),key=lambda x:-x[1])[:6]
    return total,sorted(agg.items(),key=lambda x:-x[1]),topev

for run in ["OLD1","NEW1","OLD2","NEW2"]:
    c=cpu_stats(run)
    tot,agg,topev=wait_profile(run)
    print(f"\n===== {run} =====")
    print(f"  avg broker CPU% = {c['r6682']:.0f}  (n={c['r6682_n']})   avg PG CPU% = {c['qbench-pg']:.0f}  (n={c['qbench-pg_n']})")
    print(f"  wait samples total active-backend-obs = {tot}")
    print("  by type: "+", ".join(f"{k}={v}({100*v/tot:.0f}%)" for k,v in agg))
    print("  top events: "+", ".join(f"{k}={v}" for k,v in topev))
