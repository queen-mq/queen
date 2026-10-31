# 2.5.0 against 2.6: the native path holds, the Kafka facade is slower near its ceiling (2026-10-20)

Alice asked for a quick check, beside the 2.6 Jepsen campaign, that 2.6 has not regressed against 2.5
on throughput, latency and disk writes, at 1,000 and 1,000,000 partitions, and on the Kafka facade.

**Rig.** GCP europe-west1-b, deleted after the runs:

- 3 brokers, c4-standard-24-lssd, RAID0 of the four local NVMe disks, XFS, at `/root/p3`;
- 3 loaders, c4-highcpu-16.

**Binaries.** Both built on the rig's brokers the same way (cargo 1.99.0, `--release --locked`):

| QBIN | md5 | What |
|------|-----|------|
| `250` | `a4d47b8a` | tag `v2.5.0` (`8d6fd153d`) |
| `260` | `3434fcfb` | `next-release` at `68f83bd73`: 2.6.0-beta.1 plus the node-recovery work (`dc606d9de`, `66b0a1486`, `68f83bd73`) |

**Method.** `ab.sh` and `abc.sh` (qload through `mrun.sh` of `../2026-10-12-three-node-new-forms`),
`kab.sh` and `kab2.sh` (kload against the facade). A fresh cluster before every run, one 70 s step,
latency over the last 30 s pooled over the load processes, the builds alternating. Disk is each
broker's write rate over seconds 30-60 of the step (`absum.sh`, `netdisk_rate.py`).

## The native path: no regression

| Point | Build | push p50 / p99 ms | e2e p50 / p99 ms | cores n1 / n2 / n3 | disk, B/msg |
|-------|-------|-------------------|------------------|--------------------|-------------|
| A. 3,000,000 msg/s, 256 B, 1,000 partitions, dedup off | 2.5.0 | 3.3 / 8.2, 3.4 / 8.1 | 6.8 / 17.7, 7.1 / 17.4 | 11.4 / 5.1 / 5.2 | 147 |
| | 2.6 | 3.3 / 7.2, 3.4 / 8.3 | 6.8 / 15.5, 7.0 / 17.7 | 11.4 / 5.1 / 5.1 | 147 |
| B. 1,000,000 msg/s, 256 B, 1,000,000 partitions, dedup off | 2.5.0 | 2.0 / 3.2, both rounds | 3.3 / 5.0, both rounds | 6.9 / 3.1 / 3.1 | 289 |
| | 2.6 | 2.0 / 3.2, both rounds | 3.3 / 5.0, both rounds | 6.9 / 3.1 / 3.2 | 289 |
| C. 1,000,000 msg/s, 1 KB, 100 partitions, dedup 60 s (4 rounds) | 2.5.0 | p50 10.0-10.2, p99 18.4-18.9 | p50 29.7-30.5, p99 50.2-51.7 (mean 51.1) | 8.6 / 2.3 / 2.3 | 547 |
| | 2.6 | p50 10.0-10.4, p99 17.2-20.5 | p50 30.2-30.7, p99 49.2-55.8 (mean 53.3) | 8.7 / 2.3 / 2.3 | 547 |

Every run carried its rate with no shed messages and no errors. At C, 2.6's e2e p99 is 2 ms higher on
average over four rounds, with a wider spread (its lowest run is below every 2.5.0 run): within the
noise of this rig, not a clear regression.

## The Kafka facade: equal at 500k, slower above

kload (franz-go): one topic, 100 partitions, RF 3, 1 KB, lz4, 4 producer clients and 4 consumers (classic
group) in each of three processes.

| Rate | Build | Carried | produce p50 / p99 ms | e2e p50 / p99 ms | cores n1 / n2 / n3 |
|------|-------|---------|----------------------|------------------|--------------------|
| 500,000 | 2.5.0 | yes | 8.8 / 12.0 | 11.8 / 17.7 | 1.9 / 1.6 / 6.7 |
| | 2.6 | yes | 9.5 / 14.0 | 10.1 / 15.0 | 0.7 / 0.7 / 7.4 |
| 700,000 | 2.5.0 | yes | 9.7 / 15.6 | 15.5 / 32.0 | 8.6 / 1.9 / 1.9 |
| | 2.6 | yes | 12.9 / 21.2 | 18.4 / 33.8 | 0.7 / 0.7 / 9.2 |
| 850,000 | 2.5.0 | no: 850k pushed, 756k consumed | 16.0 / 26.9 | 3.9 s / 10.9 s | 2.0 / 1.8 / 9.7 |
| | 2.6 | no: 850k pushed, 538k consumed | 40.4 / 94.2 | 17.6 s / 25.7 s | 0.7 / 10.2 / 0.7 |
| | 2.6, `QUEEN_KAFKA_THREADS=12` | no: 850k pushed, 681k consumed | 32.0 / 82.9 | 8.3 s / 16.5 s | 11.4 / 0.7 / 0.7 |
| 1,000,000 | 2.5.0 | no: 973k pushed, 66k consumed | 1.4 s / 1.6 s | 46 s / 58 s | 10.3 / 1.4 / 1.5 |
| | 2.6 | no: 701k pushed, 58k consumed | 2.3 s / 2.6 s | 58 s / 60 s | 0.4 / 6.8 / 0.4 |
| | 2.6, `QUEEN_KAFKA_THREADS=12` | no: 679k pushed, 61k consumed | 2.3 s / 2.7 s | 56 s / 60 s | 0.4 / 0.4 / 6.9 |

Neither build carries 850k with this client: past ~750k the consumers fall behind on both. Below that,
2.6 adds ~3 ms of produce latency at 700k; at 850k and 1M it falls behind sooner, and at 1M it pushes
30% less than 2.5.0.

**Where it goes (perf on 2.6's leader at 1M offered, 20 s at 99 Hz, `kprof-260.txt`):** no thread is
saturated: the whole broker used 5.9 of its 24 cores. The facade threads (`queen-kafka`, 4 of them)
took 21.6% of the samples, about 1.3 cores; the queue logs' zstd threads 32% (`ZSTD_compressBlock_fast`
~23%); the facade spent 4.9% of all samples base64-encoding payloads. What differs from 2.5.0:

- 2.6's facade takes produce only on the raft leader (`NOT_LEADER_OR_FOLLOWER` elsewhere), so all 12
  producer connections end on one node; 2.5.0's followers relayed produce to the leader, which
  spread the Kafka protocol work over the three nodes (the followers' 1.4-1.9 cores above);
- each connection reads its next request only once the previous push is in the raft log;
- the facade runs on `min(4, cores / 2)` threads. 12 threads move the 850k point's consumption from
  538k to 681k msg/s, and do nothing for produce at 1M.

**Not served by 2.6's facade: DescribeLogDirs** (API key 35). kload's topic creator waits for every
replica's log through it ("broker is too old" on 2.6; 2.5.0 answered it), so `kab.sh` creates the
topic in a step of its own for both builds and the load processes take the plain check (leaders and
ISR). Admin tools that list log directories hit the same.

## Files

- `ab.sh`, `abc.sh` -> `ab.out`, runs in `../2026-10-12-three-node-new-forms/runs/ab26-*`;
- `kab.sh`, `kab2.sh` (broker settings through `QENV`), `ksum.py` -> `kab.out`, runs in `kruns/`;
  `kab.out.first-try` and `kruns-failed/` are the first facade attempt, before the topic step;
- `kprof.sh` -> `kprof-260.txt`;
- `absum.sh`: the native table's lines and disk rates.

## 2026-10-21: the hash sidecars, a regression and its fix

Two later builds of `next-release` against the same 2.5.0 binary (`250`, `a4d47b8a`), on a new rig of
the same shape: `261` = `016292b98` (each sealed queue-log file gets a hash sidecar, for the boot's
refill; `20bfacad`), `262` = `d3809f2e5` (the sidecar written in steps the builder waits for;
`ff3e7789`). Scripts `ab261.sh`, `ab262.sh`; lines in `ab261.out`, `ab262.out`.

| Point | 2.5.0 e2e p50 / p99 ms | `261` | `262` |
|-------|------------------------|-------|-------|
| A. 3M msg/s, 1,000 partitions | 6.7-6.9 / 16.3-18.2 | 7.1-7.3 / **137-145** | 6.7-6.8 / 17.4-20.0 |
| B. 1M msg/s, 1M partitions | 3.3 / 5.0-5.1 | 3.3 / 6.4-7.2 | 3.3 / 5.1 |
| C. dedup, 1 KB, 1M msg/s | 29.4-30.2 / 51.2-54.3 | 30.5-31.0 / **76.8-85.0** | 30.0-30.2 / 54.3 |

`261` left the sidecars to the kernel's writeback: 48 MB/s of hash lists at point A piled up as dirty
pages (1.7 GB on the leader, against 48 MB on 2.5.0) and reached the device in bursts; the queue logs'
fsyncs waited behind them (longest 6 -> 206 ms; the device's queue 0.6 -> 8.2). `262` writes each
sidecar 8 MB at a time, each step pushed to the device and waited for (`dirx::sync_range`, as the
directory runs and the checkpoint already were): dirty pages back to 48 MB, latencies back to 2.5.0's.

What stays is the sidecars' own bytes, about 16 a message (each message's hash list, written a
second time): leader disk 147 -> 163 B/msg at A (+11%), 285 -> 304 at B (+7%), 547 -> 564 at C (+3%).
They buy the restart: a cold restart's refill on 254 GB went from 171-201 s to 20-21 s on the
network-disk rig the same morning. `QUEEN_QLOG_HASH_SIDECARS=0` turns them off.
