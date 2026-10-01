# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 975.49 | 20180.12 | 32074.53 | 33244.90 | 32 | 0.43 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2233.08 | 11671.00 | 18321.13 | 18956.20 | 32 | 5.28 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2212.74 | 11807.52 | 18423.15 | 19088.41 | 32 | 5.27 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 962.06 | 20809.37 | 33750.35 | 35667.16 | 32 | 0.33 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1064.55 | 19795.26 | 30186.27 | 31520.64 | 32 | 0.37 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2248.65 | 11342.52 | 17631.33 | 18261.73 | 32 | 4.98 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.104 | 58.6 | 1.000 | 96.8 | 68.134 | 924.0 | 1539.1 | 68.374 | 1005.5 | 17.771 | 146.5 | 86.145 | 1128.3 |
| queen-rust-fixed-r01 | 0.066 | 55.6 | 1.000 | 58.7 | 48.013 | 75.6 | 1051.7 | 48.952 | 128.4 | 12.398 | 99.8 | 61.351 | 228.2 |
| queen-rust-fixed-r02 | 0.068 | 55.7 | 1.000 | 58.9 | 48.472 | 75.8 | 1054.2 | 49.416 | 128.5 | 12.499 | 101.7 | 61.915 | 230.1 |
| horizon-fixed-r02 | 0.105 | 58.6 | 1.000 | 96.9 | 66.258 | 924.1 | 1540.9 | 66.498 | 1005.3 | 17.255 | 140.4 | 83.753 | 1124.5 |
| horizon-fixed-r03 | 0.098 | 58.6 | 1.000 | 96.8 | 70.654 | 924.2 | 1542.7 | 70.893 | 1005.4 | 17.582 | 138.9 | 88.475 | 1127.7 |
| queen-rust-fixed-r03 | 0.065 | 55.3 | 1.000 | 58.5 | 47.540 | 75.9 | 1055.8 | 48.472 | 128.8 | 12.310 | 96.8 | 60.782 | 225.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.052 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.266 | 500 | 12814 | 50000 | 1.256 | 3.90 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.267 | 500 | 12842 | 50000 | 1.257 | 3.89 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.050 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.048 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.267 | 500 | 12846 | 50000 | 1.257 | 3.89 | 1.00 |
