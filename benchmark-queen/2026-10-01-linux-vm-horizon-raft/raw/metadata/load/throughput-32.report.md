# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 855.26 | 158.43 | 205.77 | 229.58 | 32 | 0.83 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2794.53 | 7196.84 | 13638.18 | 14301.80 | 32 | 0.71 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2691.14 | 7797.30 | 14300.70 | 14934.36 | 32 | 0.48 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 862.58 | 157.10 | 202.96 | 221.26 | 32 | 0.87 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 796.51 | 165.87 | 238.63 | 302.28 | 32 | 0.87 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2776.76 | 7209.91 | 13718.62 | 14377.24 | 32 | 0.89 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.106 | 58.4 | 1.000 | 96.6 | 91.569 | 922.6 | 1535.4 | 91.675 | 1002.9 | 18.649 | 145.6 | 110.324 | 1148.2 |
| queen-rust-fixed-r01 | 0.024 | 26.9 | 1.000 | 58.5 | 44.907 | 81.8 | 1064.7 | 45.464 | 132.6 | 11.743 | 104.8 | 57.208 | 237.3 |
| queen-rust-fixed-r02 | 0.026 | 27.0 | 1.000 | 58.7 | 45.191 | 82.2 | 1061.7 | 45.759 | 133.1 | 11.679 | 100.8 | 57.438 | 233.8 |
| horizon-fixed-r02 | 0.104 | 58.5 | 1.000 | 97.0 | 91.287 | 922.6 | 1536.8 | 91.392 | 1003.2 | 18.536 | 142.9 | 109.928 | 1145.3 |
| horizon-fixed-r03 | 0.114 | 58.4 | 1.000 | 96.8 | 93.493 | 922.5 | 1537.2 | 93.608 | 1003.1 | 18.780 | 144.4 | 112.388 | 1146.9 |
| queen-rust-fixed-r03 | 0.024 | 26.9 | 1.000 | 58.6 | 44.828 | 81.8 | 1062.3 | 45.385 | 132.1 | 11.689 | 102.0 | 57.074 | 234.0 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.995 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.267 | 500 | 12836 | 50000 | 1.257 | 3.90 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.268 | 500 | 12890 | 50000 | 1.258 | 3.88 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.999 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.996 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.268 | 500 | 12878 | 50000 | 1.258 | 3.88 | 1.00 |
