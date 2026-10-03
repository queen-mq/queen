# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1876.48 | 11472.00 | 21896.87 | 22826.25 | 32 | 0.44 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1884.23 | 11452.17 | 21815.61 | 22786.21 | 32 | 0.41 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1896.05 | 11442.17 | 21749.76 | 22722.15 | 32 | 0.43 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.035 | 15.5 | 1.000 | 57.8 | 53.157 | 75.5 | 1069.8 | 55.174 | 429.8 | 12.746 | 104.0 | 67.921 | 532.1 |
| queen-rust-fixed-r02 | 0.035 | 15.7 | 1.000 | 58.1 | 53.304 | 77.2 | 1064.1 | 55.378 | 430.7 | 13.142 | 102.3 | 68.520 | 532.2 |
| queen-rust-fixed-r03 | 0.036 | 15.7 | 1.000 | 57.8 | 54.028 | 76.0 | 1061.8 | 56.096 | 430.6 | 13.354 | 103.2 | 69.450 | 532.3 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.262 | 500 | 12611 | 50000 | 1.252 | 3.96 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.262 | 500 | 12603 | 50000 | 1.252 | 3.97 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.262 | 500 | 12608 | 50000 | 1.252 | 3.97 | 1.00 |
