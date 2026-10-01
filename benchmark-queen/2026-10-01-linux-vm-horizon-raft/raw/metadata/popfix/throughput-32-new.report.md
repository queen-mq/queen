# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2727.20 | 7193.35 | 13616.62 | 14293.18 | 32 | 0.89 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2729.91 | 7576.13 | 14015.56 | 14660.23 | 32 | 0.35 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2732.45 | 7264.27 | 13799.14 | 14488.64 | 32 | 0.41 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.026 | 26.8 | 1.000 | 58.5 | 48.639 | 82.0 | 1068.9 | 49.226 | 133.2 | 12.618 | 102.8 | 61.844 | 235.7 |
| queen-rust-fixed-r02 | 0.026 | 27.0 | 1.000 | 59.0 | 48.472 | 82.4 | 1068.6 | 49.060 | 133.4 | 12.695 | 101.4 | 61.755 | 234.6 |
| queen-rust-fixed-r03 | 0.027 | 26.7 | 1.000 | 58.4 | 47.945 | 81.7 | 1061.5 | 48.523 | 132.6 | 12.503 | 103.6 | 61.026 | 236.1 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.267 | 500 | 12845 | 50000 | 1.257 | 3.89 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.267 | 500 | 12851 | 50000 | 1.257 | 3.89 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.268 | 500 | 12892 | 50000 | 1.258 | 3.88 | 1.00 |
