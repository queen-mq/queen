# Native path, released 2.4.0 against 2.5.0: no regression (2026-10-17)

Alice asked for a few tactical qload checks on the GCP rig, once the OpenMessaging Benchmark matrix
had finished, that 2.5.0's native performance is equal to or better than 2.4.0's.

**Rig.** The VMs of 2026-10-16 (deleted at 16:27Z after this run), europe-west1-b:

- 3 brokers, c4-standard-24-lssd;
- 3 loaders, c4-highcpu-16.

**Harness.** `mrun.sh` of `../2026-10-12-three-node-new-forms`, with qload built from
`../2026-09-30-kafka-pulsar/mqload` on 2026-10-17, md5 `e54d7bc0`. The loaders' earlier qload had no
wire-form flags, which voided the first attempt (`ab.out.failed-old-qload`).

**Binaries.**

| QBIN | md5 | What |
|------|-----|------|
| `240` | `b096e5c2` | The released 2.4.0 amd64 binary (`ghcr.io/queen-mq/queen@sha256:1a3e1cf8...`, what stage and prod run) |
| `250` | `485eb218` | 2.5.0: `next-release` at `3646d217f` (server code of `46b2d2eb1`), the release Dockerfile, amd64 (`./build.sh --amd64`) |

**Method.**

- A fresh cluster before every run.
- One 70 s step.
- Latency is qload's last 30 s, pooled.
- Cores are each broker's over the step.
- The builds alternate (240, 250, 240, 250).

The script is `ab.sh`; every run is in `../2026-10-12-three-node-new-forms/runs/ab25-*`.

## A. The docs' headline point: 3,000,000 msg/s, 256 B, 1,000 partitions, dedup off

The binary wire forms with client zstd, completed retention 1,800 s, as in the 2.4.0 A/B of
`../2026-10-15-bytes-per-msg`.

| Run | push p50 / p99 ms | e2e p50 / p99 ms | cores n1 / n2 / n3 | carried |
|-----|-------------------|------------------|--------------------|---------|
| 2.4.0, round 1 | 3.5 / 10.6 | 7.7 / 22.8 | 10.9 / 5.1 / 4.9 | 3,000,034 pushed, 0 shed, 0 errors |
| 2.5.0, round 1 | 3.6 / 10.8 | 7.7 / 22.8 | 10.9 / 5.1 / 4.9 | 3,000,027, 0, 0 |
| 2.4.0, round 2 | 3.3 / 8.3 | 6.8 / 17.9 | 11.2 / 5.3 / 5.1 | 2,999,982, 0, 0 |
| 2.5.0, round 2 | 3.3 / 7.0 | 6.8 / 14.8 | 11.2 / 5.3 / 5.1 | 3,000,024, 0, 0 |

## B. Dedup on: 1,000,000 msg/s, 1 KB, 100 partitions, dedup window 60 s

Binary push without zstd.

| Run | push p50 / p99 ms | e2e p50 / p99 ms | cores n1 / n2 / n3 | carried |
|-----|-------------------|------------------|--------------------|---------|
| 2.4.0, round 1 | 10.2 / 20.5 | 30.2 / 54.3 | 8.6 / 2.4 / 2.4 | 1,000,078 pushed, 0 shed, 0 errors |
| 2.5.0, round 1 | 10.4 / 21.2 | 30.7 / 54.8 | 8.6 / 2.4 / 2.4 | 999,935, 0, 0 |
| 2.4.0, round 2 | 10.2 / 22.3 | 30.5 / 59.4 | 8.5 / 2.4 / 2.4 | 1,000,021, 0, 0 |
| 2.5.0, round 2 | 10.1 / 20.0 | 29.7 / 53.2 | 8.6 / 2.4 / 2.3 | 999,959, 0, 0 |

## Verdict

No regression. At both points 2.5.0 carries the rate with the same cores, and its latencies are
within the run-to-run spread of 2.4.0's. At 3M msg/s, round 2's p99 is lower on 2.5.0 (14.8 against
17.9 ms), but round 1 is identical, so that is noise, not a gain.
