import json, re
OUT="/root/bench-runs/cal"
def g(t,k):
    m=re.search("\""+k+"\":\\s*([0-9]+)",t); return int(m.group(1)) if m else 0
print("%-3s %-7s %-7s %9s %9s %8s %7s %6s %9s %7s %7s"%("cmb","prod","cons","push/s","popCeil","pp99","perr","pto","pendBLog","qCPU","pgCPU"))
for lbl,pw,pc,cw,cc in [("A","2x100","3x100","",""),("B","3x100","3x100","",""),("C","4x100","3x100","","")]:
    pr=open(OUT+"/"+lbl+"-prod.log").read(); co=open(OUT+"/"+lbl+"-cons.log").read()
    push=g(pr,"msgPerSec"); pp99=g(pr,"p99"); perr=g(pr,"errors"); pto=g(pr,"timeouts")
    creq=g(co,"reqPerSec"); popceil=creq*100
    try:
        qj=json.load(open(OUT+"/"+lbl+"-queue.json")); parts=qj.get("partitions",[])
        pend=sum(p["stats"]["pending"] for p in parts); tot=sum(p["stats"]["total"] for p in parts)
        nz=sum(1 for p in parts if p["stats"]["total"]>0)
    except Exception as e:
        pend=tot=nz=-1
    qcpu=[]; pgcpu=[]
    for line in open(OUT+"/"+lbl+"-cpu.txt"):
        m=re.match(r"(queen|postgres)\s+([0-9.]+)%",line)
        if m: (qcpu if m.group(1)=="queen" else pgcpu).append(float(m.group(2)))
    qa=(sum(qcpu)/len(qcpu)/100) if qcpu else 0; pa=(sum(pgcpu)/len(pgcpu)/100) if pgcpu else 0
    print("%-3s %-7s %-7s %9d %9d %8d %7d %6d %9d %7.2f %7.2f"%(lbl,pw,pc,push,popceil,pp99,perr,pto,pend,qa,pa))
    print("     (queue total rows=%d, partitions-with-data=%d)"%(tot,nz))
