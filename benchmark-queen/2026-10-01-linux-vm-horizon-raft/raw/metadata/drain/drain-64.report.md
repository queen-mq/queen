# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 631.97 | 67652.74 | 80948.91 | 82273.79 | 64 | 0.77 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 3309.25 | 15382.68 | 22125.22 | 22980.38 | 64 | 9.07 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 3310.39 | 15462.94 | 22243.42 | 23084.15 | 64 | 9.10 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 567.11 | 79569.28 | 83658.60 | 84322.24 | 64 | 0.77 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 556.62 | 78554.33 | 87149.71 | 87641.67 | 64 | 0.77 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 3411.01 | 14379.48 | 21545.09 | 22391.28 | 64 | 8.09 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.261 | 59.0 | 1.000 | 97.9 | 198.318 | 1828.4 | 3080.3 | 198.716 | 1952.1 | 42.951 | 345.9 | 241.667 | 2261.1 |
| queen-rust-fixed-r01 | 0.130 | 55.5 | 1.000 | 59.4 | 111.707 | 121.6 | 2093.2 | 113.655 | 219.9 | 24.714 | 173.8 | 138.368 | 393.3 |
| queen-rust-fixed-r02 | 0.132 | 47.3 | 1.000 | 59.7 | 110.165 | 121.9 | 2111.8 | 112.085 | 219.0 | 24.346 | 174.1 | 136.430 | 393.1 |
| horizon-fixed-r02 | 0.290 | 58.9 | 1.000 | 97.4 | 209.662 | 1828.5 | 3082.6 | 210.092 | 1952.8 | 43.305 | 342.8 | 253.397 | 2260.6 |
| horizon-fixed-r03 | 0.288 | 58.9 | 1.000 | 98.1 | 196.140 | 1828.5 | 3081.4 | 196.564 | 1952.8 | 43.870 | 347.9 | 240.434 | 2263.7 |
| queen-rust-fixed-r03 | 0.134 | 47.1 | 1.000 | 59.7 | 110.060 | 121.9 | 2114.0 | 112.014 | 219.9 | 24.440 | 171.8 | 136.454 | 391.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.057 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.484 | 1000 | 47366 | 100000 | 1.474 | 2.11 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.487 | 1000 | 47669 | 100000 | 1.477 | 2.10 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.065 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.065 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.479 | 1000 | 46909 | 100000 | 1.469 | 2.13 | 1.00 |
