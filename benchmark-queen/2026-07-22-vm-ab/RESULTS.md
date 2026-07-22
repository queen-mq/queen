# VM A/B campaign — OLD seg (54697a3) vs NEW log (90c2494) — 2026-07-22

Bench queen-01 (32c, PG16 docker, doc-17 tuning) + loader queen-loader-01 (48c),
DO VPC 10.114.0.x. Shape: 1500 prod / 850 cons / 100 partitions / push-batch
100 / pop-batch 500 / pop-partitions 10 / long-poll / 256B / autoAck / dedup 0.
Fresh PG per data point.

## 5-min raw throughput (4 interleaved runs: OLD1 NEW1 OLD2 NEW2)

Min 2-5 averages: OLD 736.7k/s push+pop each way; NEW 765.0k/s (+3.8%).
Broker CPU: OLD 5.5 cores, NEW 4.8 (−12.4%). PG CPU ~13 cores both.
Caveat: hypervisor steal regimes swung single runs ±22% (cool ~800-845k, hot
~660-690k); interleaving balanced it (each engine 1 cool + 1 hot). WALWrite =
48-55% of PG wait samples in every run — the box's shared ceiling.
Wait signatures: OLD Lock:extend + LockManager; NEW Lock:transactionid on
log_consumers. Warm-up deadlocks: NEW 15 vs OLD 2 (first-contact convoy, see
bug note below).

## 10-min soak (decay test)

OLD (12:57): 713 643 627 659 670 585 602 628 612 573 k/s — total 378.7M
(631k/s), −20% first→last, monster-sweep dip at min 6 (t≈300s), end DB 10.4GB
mostly dead (12.5k live segments after final sweep).
NEW (13:29, after 15s pre-warm): 688 671 699 676 647 760 667 715 727 671 k/s —
total 415.4M (692k/s, +9.7%), NO trend (flat ±8%), retention 5s cadence
deleting 7-15k segments/cycle continuously, 0 deadlocks, end DB 14GB live
window (778k segments = legit 300s working set).
Min-10 comparison: 671k vs 573k (+17%), curves diverging.

## Open bug (NEW)

First-contact convoy: 850 consumers × 100 partitions racing to create
log_consumers rows at t=0 can wedge into a permanent Lock:transactionid chain
(127 stuck backends observed), amplified by client-side timeouts that never
cancel queries server-side. Workaround: 15s tiny-goload pre-warm after each
fresh PG. Fix planned: claim-first discipline in 043_log_pop.sql + query
cancellation on timeout.

Raw data: OLD1/NEW1/OLD2/NEW2/ (goload, mon, 1s waits, 15s cpu, deadlocks),
soak-old/. SOAK-NEW was run manually by Alice (numbers above, from goload
stdout). analyze.py aggregates.
