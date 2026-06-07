import json, os, re
BASE="/root/bench-runs/results"
tests=["bp10","bp100","hipart1","cg10"]; imgs=["0.15.5","simd"]
def stats(p):
    qs=[]; ps=[]
    if not os.path.exists(p): return (None,None,0)
    for line in open(p):
        mq=re.search(r"queen=([0-9.]+)%",line); mp=re.search(r"postgres=([0-9.]+)%",line)
        if mq and mp:
            q=float(mq.group(1)); pg=float(mp.group(1))
            if q>10: qs.append(q); ps.append(pg)
    if not qs: return (None,None,0)
    return (sum(qs)/len(qs), sum(ps)/len(ps), len(qs))
def prod(p):
    out={}
    if not os.path.exists(p): return out
    txt=open(p).read()
    for k in ["msgPerSec","totalMessages","errors","non2xx","timeouts","p99","p50"]:
        m=re.search(r"\""+k+r"\"\s*:\s*([0-9]+)",txt)
        if m: out[k]=int(m.group(1))
    return out
def comp(p):
    try:
        d=json.load(open(p)); m=d.get("messages",{}); return m.get("total"),m.get("completed")
    except: return (None,None)
rows={}
print("%-10s %-8s %12s %6s %7s %11s %8s %10s %6s"%("test","img","push/s","err","samp","qCPU(vCPU)","pgCPU","CPU/Mmsg","p99"))
for t in tests:
  for im in imgs:
    d="%s/%s-%s"%(BASE,t,im)
    pr=prod(d+"/producer.log"); q,pg,n=stats(d+"/docker-stats.log")
    tot,cmp_=comp(d+"/status.json")
    msg=pr.get("msgPerSec",0); err=pr.get("errors",0)+pr.get("non2xx",0)
    qv=(q/100) if q else 0; pgv=(pg/100) if pg else 0
    cpm=(qv/(msg/1e6)) if msg else 0
    rows[(t,im)]=(msg,err,qv,pgv,cpm,pr.get("p99",0))
    print("%-10s %-8s %12d %6d %7d %11.2f %8.2f %10.1f %6d"%(t,im,msg,err,n,qv,pgv,cpm,pr.get("p99",0)))
print("\n=== simd vs 0.15.5 ===")
for t in tests:
  b=rows.get((t,"0.15.5")); s=rows.get((t,"simd"))
  if b and s and b[0] and s[0]:
    dmsg=(s[0]-b[0])/b[0]*100; dcpu=(s[2]-b[2])/b[2]*100 if b[2] else 0
    dcpm=(s[4]-b[4])/b[4]*100 if b[4] else 0
    print("%-10s throughput %+6.1f%%   QueenCPU %+6.1f%%   CPU/Mmsg %+6.1f%%"%(t,dmsg,dcpu,dcpm))
