# T3 commit_delay experiment — raw results

## CONFIG A: commit_delay=500 commit_siblings=8 (POOL=300 ZSTD=1 FSHARDS=24 FHOLD=30 MAXINFLIGHT=64 BUNDLEMAX=32 VA=6 VB=12)

### A @ R=1,000,000 (90s)  -> FAIL (pop-side lag monotonic growth; T2 signal)
loader: offered~1.00M/s achieved(push)~1.00M/s shed=0 ; pop~900-920k/s
lag: 386k,862k,1494k,2965k,3904k,4731k,5793k,6932k,7756k final=7814k  (monotonic, never drains)
p50~444ms p99 1.1-1.9s p999 1.4-2.5s
final: pushed=89,764,500 popped=81,950,600 lag=7,813,900 over 90s
mon commit/s during load: 2118,8181,8145,6711,7386,7526,7336,3524 (peak ~7500-8100)
CPU: broker ~550-630% pg ~580-635% (of 3200%)

WAL/IO cumulative since boot (== A/1M deltas; boot 05:40:55, only A/1M ran):
 wal_records=9,036,651  wal_fpi=1,255  wal_bytes=4,467,153,753 (4.467 GB)
 wal_writes=113,891  wal_write_time=3,099 ms
 wal_fsyncs=113,131   wal_fsync_time=34,240 ms
 xact_commit=518,628
Derived (over 90s):
 WAL MB/s = 49.6 ;  fsyncs/s = 1,257 ;  commits/s(avg)=5,762 (peak ~7500)
 commits/fsync = 4.58 (group commit batching ~4.6 commits/flush)
 msg/commit = 89.76M/518.6k = 173 (avg) ; ~133 at steady-state peak
 msg/fsync = 89.76M/113.1k = 794
 avg fsync latency = 303 us ; avg write latency = 27 us
NOTE: only 1,257 fsyncs/s -> WAL flush pipeline has huge headroom; knee is POP-bound, not WAL-bound.

## CONFIG B: commit_delay=1000 commit_siblings=12 (else same)

### B @ R=1,000,000 (90s) -> FAIL (worse than A)
loader: offered~1.00M/s achieved(push)~1.00M/s shed=0 ; pop~880-900k/s
lag: 689k,1460k,2403k,3958k,5135k,6312k,8072k,9334k,10684k final=10703k (monotonic, faster than A)
p50~490ms p99 1.2-2.0s
final: pushed=89,751,200 popped=79,048,600 lag=10,702,600 over 90s
mon commit/s: 4610,7673,7079,7002,7321,6389,7036 (peak ~7300)
CPU broker ~550-645% pg ~150-650%
top waits (mon.log): LWLock:WALWrite=10-31 + CPU:run dominant; occasional IO:WalSync, Lock:extend, LWLock:BufferContent

WAL/IO deltas (io+wal reset at load start):
 wal_records=8,878,759  wal_fpi=1,250  wal_bytes=4,458,498,766 (4.458 GB)
 wal_writes=111,673  wal_write_time=3,037 ms
 wal_fsyncs=110,913   wal_fsync_time=34,343 ms
 xact_commit=486,360
Derived (90s):
 WAL MB/s=49.5 ; fsyncs/s=1,232 ; commits/s avg=5,404 (peak ~7300)
 commits/fsync=4.39 ; msg/commit=184 avg (~135 steady) ; msg/fsync=809
 avg fsync latency=310us ; avg write latency=27us

## VERDICT: neither 500 nor 1000 us made 1M clean. A (500/8) better than B (1000/12).
## commit_delay window IS engaging (group commit ~4.4-4.6 commits/flush, only ~1230 fsyncs/s)
## but knee is POP-drain bound (~900-920k/s), not WAL-flush bound -> commit_delay cannot buy the last 5%.

## CONFIRMATION: CONFIG A (commit_delay=500/siblings=8) @ R=950,000 x 300s

lag trend (per 10s): 55k,109k,96k,447k,245k,257k,586k,678k,525k,763k,969k,1035k,1169k,1316k,1516k,1730k,2049k,2085k,2168k,2600k...(climbing)
=> SLOW MONOTONIC LEAK: pop trails push ~12-13k/s (~1.3%). p99 stable ~1.8-2.0s (NOT exploding), shed=0, no push errs beyond baseline 259.
=> Not a hard collapse, but lag does not bound/drain. True sustainable ~900k (T2-confirmed), 950k leaks slowly.
push/pop both ~945-955k/s. p50 130-500ms (oscillates). p99 ~1.8-2.0s. p999 ~2.4-2.8s.

WAL/IO deltas (io+wal reset; captured window ~160s of load, load 05:55:48->05:58:20):
 wal_records=18,230,611  wal_fpi=1,393  wal_bytes=8,779,338,718 (8.78 GB)
 wal_writes=224,988  wal_write_time=5,935 ms
 wal_fsyncs=224,228   wal_fsync_time=70,181 ms
 xact_commit=1,123,182
Derived (window-independent ratios solid; rates ~6% uncertain on ~160s window):
 commits/fsync=5.01 (best batching of the three -> lower rate = more per flush)
 msg/commit~135 ; msg/fsync~678 ; bytes/commit=7,817 ; records/commit=16.2
 avg fsync latency=313us ; avg write latency=26us
 WAL MB/s~55 ; fsyncs/s~1,400 ; commits/s~7,000

### Confirmation FINAL (A @ 950k x 300s)
[final] pushed=284,773,500 popped=279,881,700 final_lag=4,891,800 over 300s ; shed=0 ; pushErr=259(baseline)
overall p50=411.65 p99=1941.50 p999=2703.36 ms
lag full trend: 55k..1M(t120)..1.7M(t150)..2.6M(t190)..2.99M(t250)..4.2M(t270)..4.9M(final)
=> net leak ~16k/s (pop trails push ~1.7%); oscillates but does not bound over 300s.
mon commit/s steady ~7,700-8,200 ; CPU broker 510-640% pg 440-650% of 3200% (~80% idle)
top waits: LWLock:WALWrite (2-19 bk) + CPU:run dominant; also LWLock:WALInsert, LWLock:BufferContent, Lock:extend, IO:WalSync

## MECHANISM SUMMARY (across load points)
              A@1M    B@1M    A@950k
commit/s      ~7000   ~7000   ~7900   (peak/steady)
fsyncs/s      1257    1232    ~1400
commits/fsync 4.58    4.39    5.01
msg/fsync     794     809     678
avg fsync us  303     310     313
WAL MB/s      49.6    49.5    ~55
- Group commit engages at 500us already (commits/fsush ~4.6). 1000us added NO extra batching (4.39<=4.58) -> window already saturated; extra delay = pure latency, hurt pop drain (B worse).
- WALWrite stays top DB wait at all points but only handful of ~300 backends; CPU ~80% idle; fsync device has huge headroom (68us x ~1300/s < 10% one core).
- Ceiling is broker POP/consume drain (~900-920k/s sustained), downstream of WAL. commit_delay cannot move it.

## PRODUCT DEFAULT
- commit_delay only delays a flush if >= commit_siblings OTHER txns are active at the instant a backend writes its commit record (PG docs) -> at low concurrency it is bypassed, zero added latency. Safe to keep.
- No throughput reason to raise it: knee is pop-bound. Recommend keep modest (200/5 default, or 500/8 harmless). Do NOT market it as a capacity lever.
- Capacity work should target the pop/consume drain path.
