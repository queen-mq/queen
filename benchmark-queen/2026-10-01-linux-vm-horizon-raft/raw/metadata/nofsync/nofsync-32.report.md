# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1338.55 | 4895.85 | 16850.46 | 17789.81 | 32 | 0.82 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2787.62 | 7140.02 | 13593.49 | 14242.51 | 32 | 0.81 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2751.43 | 7166.46 | 13705.21 | 14339.75 | 32 | 0.82 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1270.61 | 6343.60 | 18478.54 | 19542.87 | 32 | 0.78 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1303.28 | 6063.53 | 17877.00 | 18649.32 | 32 | 0.79 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2764.95 | 7106.72 | 13524.41 | 14192.16 | 32 | 0.83 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.092 | 58.7 | 1.000 | 97.1 | 70.326 | 924.4 | 1540.6 | 70.418 | 1005.6 | 17.082 | 163.1 | 87.500 | 1164.5 |
| queen-rust-fixed-r01 | 0.025 | 26.5 | 1.000 | 58.6 | 49.503 | 82.4 | 1073.4 | 50.093 | 132.9 | 12.972 | 102.2 | 63.065 | 235.0 |
| queen-rust-fixed-r02 | 0.025 | 26.4 | 1.000 | 58.3 | 49.964 | 82.8 | 1074.8 | 50.556 | 134.0 | 12.959 | 101.0 | 63.515 | 234.6 |
| horizon-fixed-r02 | 0.090 | 58.6 | 1.000 | 96.9 | 72.843 | 924.4 | 1541.8 | 72.934 | 1005.6 | 17.167 | 161.8 | 90.101 | 1162.2 |
| horizon-fixed-r03 | 0.089 | 58.6 | 1.000 | 96.5 | 73.895 | 924.3 | 1541.8 | 73.986 | 1005.5 | 17.445 | 167.5 | 91.430 | 1168.8 |
| queen-rust-fixed-r03 | 0.025 | 27.1 | 1.000 | 58.7 | 49.906 | 81.9 | 1056.5 | 50.495 | 133.0 | 12.980 | 99.5 | 63.475 | 232.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.047 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.266 | 500 | 12824 | 50000 | 1.256 | 3.90 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.266 | 500 | 12804 | 50000 | 1.256 | 3.91 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.046 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.046 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.268 | 500 | 12898 | 50000 | 1.258 | 3.88 | 1.00 |
