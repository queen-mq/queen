import json, os, re, sys
BASE="/root/bench-runs/results"
def stats(p):
    qs=[];ps=[]
    if not os.path.exists(p): return (0,0,0)
    for line in open(p):
        mq=re.search(r"queen=([0-9.]+)%",line); mp=re.search(r"postgres=([0-9.]+)%",line)
        if mq and mp:
            q=float(mq.group(1)); pg=float(mp.group(1))
            if q>10: qs.append(q); ps.append(pg)
    if not qs: return (0,0,0)
    return (sum(qs)/len(qs), sum(ps)/len(ps), len(qs))
def res(p):
    out={}
    if not os.path.exists(p): return out
    t=open(p).read()
    for k in ["msgPerSec","errors","non2xx","timeouts","p99"]:
        m=re.search(r"\""+k+r"\": *([0-9]+)",t)
        if m: out[k]=int(m.group(1))
    return out
def jget(p):
    try: return json.load(open(p))
    except: return {}
order=["hi-part-1","hi-part-10","hi-part-100","hi-part-1000","hi-part-10000","bp-1","bp-10","bp-100","q-1","q-10","q-100"]
names=sys.argv[1:] if len(sys.argv)>1 else order
print("%-14s %-13s %9s %9s %4s %4s %8s %7s %8s %4s"%("test","tag","push/s","pop/s","2xx!","t/o","qCPU","pgCPU","CPU/Mms","p99"))
for n in names:
    d=os.path.join(BASE,n)
    if not os.path.isdir(d): print("%-14s (none)"%n); continue
    pr=res(d+"/producer.log"); q,pg,ns=stats(d+"/docker-stats.log")
    st=jget(d+"/status.json"); md=jget(d+"/metadata.json")
    dur=md.get("durationSec",300) or 300
    pop=st.get("messages",{}).get("completed",0)/dur
    msg=pr.get("msgPerSec",0); qv=q/100; pgv=pg/100
    cpm=(qv/(msg/1e6)) if msg else 0
    print("%-14s %-13s %9d %9d %4d %4d %8.2f %7.2f %8.1f %4d"%(n,md.get("queenImageTag","?"),msg,pop,pr.get("non2xx",0),pr.get("timeouts",0),qv,pgv,cpm,pr.get("p99",0)))
