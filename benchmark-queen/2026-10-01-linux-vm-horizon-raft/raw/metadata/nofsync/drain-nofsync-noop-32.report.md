# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 1042.25 | 38720.29 | 64256.73 | 66869.83 | 32 | 0.32 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4351.38 | 11794.64 | 16712.40 | 17202.08 | 32 | 7.29 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4174.80 | 12516.62 | 17233.35 | 17766.07 | 32 | 8.29 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 972.65 | 42432.99 | 69871.97 | 71293.59 | 32 | 0.32 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 1004.73 | 41424.89 | 68027.82 | 69989.01 | 32 | 0.33 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4311.53 | 11644.18 | 16552.26 | 17012.35 | 32 | 7.32 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.203 | 58.7 | 1.000 | 97.4 | 100.551 | 924.5 | 1540.8 | 100.891 | 1036.1 | 31.560 | 371.6 | 132.451 | 1372.6 |
| queen-rust-fixed-r01 | 0.075 | 55.3 | 1.000 | 58.5 | 100.982 | 76.0 | 1052.0 | 102.515 | 159.1 | 19.748 | 155.2 | 122.263 | 314.2 |
| queen-rust-fixed-r02 | 0.072 | 47.5 | 1.000 | 58.9 | 102.012 | 76.2 | 1057.6 | 103.571 | 159.0 | 20.214 | 153.2 | 123.785 | 312.2 |
| horizon-fixed-r02 | 0.211 | 58.7 | 1.000 | 97.1 | 100.341 | 924.4 | 1539.3 | 100.692 | 1036.0 | 31.959 | 376.4 | 132.651 | 1378.1 |
| horizon-fixed-r03 | 0.207 | 58.6 | 1.000 | 96.9 | 98.977 | 924.2 | 1540.9 | 99.320 | 1036.2 | 31.311 | 376.0 | 130.631 | 1376.7 |
| queen-rust-fixed-r03 | 0.068 | 55.6 | 1.000 | 58.9 | 100.892 | 75.9 | 1050.3 | 102.420 | 158.8 | 19.555 | 155.1 | 121.975 | 313.7 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.038 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25368 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25414 | 100000 | 1.254 | 3.93 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.039 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.040 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.263 | 1000 | 25335 | 100000 | 1.253 | 3.95 | 1.00 |
