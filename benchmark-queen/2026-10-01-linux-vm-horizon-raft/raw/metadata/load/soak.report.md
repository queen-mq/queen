# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 360000/360000 | 0 | 0 | 0 | 392.74 | 70.57 | 249.11 | 338.26 | 16 | 0.27 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 360000/360000 | 0 | 0 | 0 | 399.99 | 14.27 | 21.75 | 28.82 | 16 | 0.86 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 1.500 | 59.3 | 1.000 | 96.4 | 559.757 | 469.9 | 766.2 | 561.278 | 728.9 | 194.132 | 974.2 | 755.410 | 1703.1 |
| queen-rust-fixed-r01 | 0.918 | 27.6 | 1.000 | 58.6 | 504.891 | 58.9 | 542.3 | 525.092 | 288.7 | 350.169 | 625.9 | 875.261 | 912.8 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.357 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 3.987 | 360000 | 715378 | 360000 | 2.987 | 0.50 | 1.00 |
