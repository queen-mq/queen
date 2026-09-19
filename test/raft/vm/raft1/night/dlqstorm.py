#!/usr/bin/env python3
# Real active DLQ storm for the O19 test (NIGHT-A step 3): pop leased batches
# from ONE hot partition and ack them with status "dlq" (forced DLQ handoff),
# paced to a target rate. Reports the achieved ack-dlq rate. One keep-alive
# connection. Args: <queue> <partition> <duration_s> ; env TARGET (acks/s),
# BATCH, DLQ_STATUS, PORT.
import http.client, json, time, sys, os
PORT=int(os.environ.get("PORT","6698")); HOST="127.0.0.1"
queue=sys.argv[1] if len(sys.argv)>1 else "hot"
part=sys.argv[2] if len(sys.argv)>2 else "0"
dur=float(sys.argv[3]) if len(sys.argv)>3 else 90.0
batch=int(os.environ.get("BATCH","50"))
target=float(os.environ.get("TARGET","500"))   # acks/s
status=os.environ.get("DLQ_STATUS","dlq")
conn=http.client.HTTPConnection(HOST,PORT,timeout=30)
poppath=f"/api/v1/pop/queue/{queue}?batch={batch}&autoAck=false"
end=time.time()+dur; popcalls=empties=acked=ackerr=0
csv=os.environ.get("CSV","")
cf=open(csv,"w") if csv else None
if cf: cf.write("ts,acked_cum,rate_1s\n")
last_t=time.time(); last_a=0
while time.time()<end:
    t0=time.time()
    try:
        conn.request("GET",poppath); r=conn.getresponse(); body=r.read(); popcalls+=1
    except Exception as e:
        try: conn.close(); conn=http.client.HTTPConnection(HOST,PORT,timeout=30)
        except: pass
        time.sleep(0.05); continue
    try: doc=json.loads(body)
    except: continue
    msgs=doc.get("messages") or []
    top_pid=doc.get("partitionId"); top_lease=doc.get("leaseId")
    if not msgs:
        empties+=1; time.sleep(0.02); continue
    acks=[{"transactionId":m.get("transactionId"),"partitionId":m.get("partitionId") or top_pid,
           "status":status,"leaseId":m.get("leaseId") or top_lease} for m in msgs]
    payload=json.dumps({"acknowledgments":acks})
    try:
        conn.request("POST","/api/v1/ack",body=payload,headers={"content-type":"application/json"})
        r2=conn.getresponse(); rb=r2.read()
        if r2.status>=400: ackerr+=len(acks)
        else: acked+=len(acks)
    except Exception:
        try: conn.close(); conn=http.client.HTTPConnection(HOST,PORT,timeout=30)
        except: pass
        continue
    now=time.time()
    if cf and now-last_t>=1.0:
        cf.write(f"{int(now)},{acked},{acked-last_a}\n"); cf.flush(); last_t=now; last_a=acked
    # pace to target acks/s
    want=len(acks)/target
    el=time.time()-t0
    if el<want: time.sleep(want-el)
if cf: cf.close()
print(f"dlqstorm queue={queue}/p{part} dur={dur:.0f}s popcalls={popcalls} empties={empties} "
      f"acked_dlq={acked} ackErr={ackerr} achieved_rate={acked/dur:.0f}/s target={target:.0f}/s")
