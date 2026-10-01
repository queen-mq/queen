# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1338.55 | 5805.71 | 16055.15 | 16985.43 | 32 | 0.77 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2726.42 | 7140.94 | 13591.86 | 14305.61 | 32 | 0.40 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2774.44 | 7202.97 | 13690.59 | 14362.81 | 32 | 0.94 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1301.30 | 6129.32 | 18492.01 | 19413.36 | 32 | 0.83 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1177.24 | 8033.62 | 21414.69 | 22529.76 | 32 | 0.87 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2719.48 | 7197.18 | 13691.72 | 14346.39 | 32 | 0.91 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.089 | 58.6 | 1.000 | 97.3 | 63.291 | 924.4 | 1539.5 | 63.381 | 1005.5 | 16.359 | 127.6 | 79.740 | 1129.2 |
| queen-rust-fixed-r01 | 0.025 | 27.0 | 1.000 | 58.9 | 44.987 | 82.1 | 1064.0 | 45.547 | 132.8 | 11.830 | 103.5 | 57.377 | 236.3 |
| queen-rust-fixed-r02 | 0.024 | 27.1 | 1.000 | 58.9 | 44.959 | 81.9 | 1069.1 | 45.516 | 132.2 | 11.648 | 104.2 | 57.164 | 236.4 |
| horizon-fixed-r02 | 0.092 | 58.7 | 1.000 | 97.2 | 65.706 | 924.2 | 1542.1 | 65.799 | 1005.3 | 16.468 | 124.8 | 82.267 | 1125.9 |
| horizon-fixed-r03 | 0.090 | 58.7 | 1.000 | 97.0 | 64.726 | 924.3 | 1540.0 | 64.818 | 1005.5 | 16.496 | 127.5 | 81.313 | 1127.9 |
| queen-rust-fixed-r03 | 0.026 | 27.0 | 1.000 | 58.7 | 46.635 | 82.7 | 1067.2 | 47.204 | 133.3 | 12.192 | 101.0 | 59.396 | 234.2 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.047 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.268 | 500 | 12897 | 50000 | 1.258 | 3.88 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.267 | 500 | 12833 | 50000 | 1.257 | 3.90 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.046 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.047 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.268 | 500 | 12883 | 50000 | 1.258 | 3.88 | 1.00 |
