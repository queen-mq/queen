# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 456.73 | 74.76 | 221.16 | 287.42 | 16 | 0.33 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.85 | 14.88 | 23.33 | 29.23 | 16 | 0.81 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.94 | 14.42 | 22.81 | 28.79 | 16 | 0.90 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 433.51 | 79.19 | 246.43 | 305.07 | 16 | 0.26 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 439.48 | 65.35 | 227.20 | 288.17 | 16 | 0.15 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.92 | 14.90 | 23.44 | 29.50 | 16 | 0.31 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.220 | 59.2 | 1.000 | 96.4 | 81.262 | 470.1 | 766.7 | 81.484 | 551.2 | 28.252 | 154.2 | 109.736 | 702.7 |
| queen-rust-fixed-r01 | 0.121 | 27.4 | 1.000 | 58.1 | 77.517 | 57.6 | 535.4 | 80.564 | 109.0 | 51.302 | 170.0 | 131.866 | 277.7 |
| queen-rust-fixed-r02 | 0.122 | 27.2 | 1.000 | 58.3 | 78.244 | 58.5 | 543.1 | 81.276 | 110.1 | 52.039 | 170.5 | 133.315 | 278.9 |
| horizon-fixed-r02 | 0.223 | 59.2 | 1.000 | 96.6 | 87.542 | 470.2 | 768.2 | 87.768 | 551.0 | 29.654 | 160.2 | 117.423 | 709.0 |
| horizon-fixed-r03 | 0.231 | 59.2 | 1.000 | 95.9 | 88.786 | 470.2 | 768.9 | 89.020 | 551.3 | 29.463 | 160.6 | 118.484 | 709.3 |
| queen-rust-fixed-r03 | 0.124 | 27.3 | 1.000 | 58.3 | 84.020 | 58.4 | 541.5 | 87.314 | 109.5 | 55.654 | 170.3 | 142.968 | 278.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.290 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 3.980 | 60000 | 118787 | 60000 | 2.980 | 0.51 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 3.980 | 60000 | 118772 | 60000 | 2.980 | 0.51 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.312 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.306 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 3.970 | 60000 | 118190 | 60000 | 2.970 | 0.51 | 1.00 |
