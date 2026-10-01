# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.88 | 15.08 | 24.92 | 32.99 | 16 | 0.31 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.95 | 15.16 | 24.39 | 30.49 | 16 | 0.80 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 60000/60000 | 0 | 0 | 0 | 499.92 | 15.16 | 24.84 | 32.08 | 16 | 0.80 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.127 | 27.2 | 1.000 | 58.2 | 84.104 | 58.2 | 536.5 | 87.440 | 109.9 | 55.149 | 172.2 | 142.589 | 281.1 |
| queen-rust-fixed-r02 | 0.125 | 27.0 | 1.000 | 58.2 | 84.953 | 58.6 | 542.7 | 88.282 | 109.6 | 55.681 | 174.8 | 143.963 | 283.3 |
| queen-rust-fixed-r03 | 0.124 | 27.4 | 1.000 | 58.3 | 83.720 | 58.7 | 541.7 | 87.054 | 110.6 | 55.519 | 177.7 | 142.572 | 286.0 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 3.961 | 60000 | 117631 | 60000 | 2.961 | 0.51 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 3.969 | 60000 | 118169 | 60000 | 2.969 | 0.51 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 3.966 | 60000 | 117988 | 60000 | 2.966 | 0.51 | 1.00 |
