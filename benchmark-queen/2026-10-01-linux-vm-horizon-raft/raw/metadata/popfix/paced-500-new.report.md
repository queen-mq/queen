# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.94 | 14.98 | 23.72 | 29.90 | 16 | 0.80 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.95 | 15.08 | 24.37 | 31.48 | 16 | 0.80 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.91 | 14.58 | 23.36 | 29.41 | 16 | 0.80 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.125 | 27.2 | 1.000 | 58.1 | 76.481 | 58.1 | 538.4 | 79.812 | 110.0 | 49.420 | 174.9 | 129.232 | 283.2 |
| queen-rust-fixed-r02 | 0.125 | 27.4 | 1.000 | 58.3 | 78.315 | 58.0 | 537.9 | 81.685 | 109.8 | 50.970 | 175.7 | 132.655 | 283.6 |
| queen-rust-fixed-r03 | 0.124 | 27.5 | 1.000 | 58.3 | 77.307 | 58.0 | 534.9 | 80.612 | 110.8 | 49.397 | 177.8 | 130.009 | 286.4 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 2.990 | 60000 | 59378 | 60000 | 1.990 | 1.01 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 2.988 | 60000 | 59292 | 60000 | 1.988 | 1.01 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 2.992 | 60000 | 59513 | 60000 | 1.992 | 1.01 | 1.00 |
