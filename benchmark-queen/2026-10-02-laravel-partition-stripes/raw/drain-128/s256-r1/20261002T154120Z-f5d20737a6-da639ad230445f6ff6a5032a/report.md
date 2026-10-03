# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 5283.74 | 5376.53 | 6207.62 | 6381.57 | 128 | 6.03 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.168 | 57.9 | 1.000 | 63.4 | 33.740 | 248.4 | 4198.0 | 34.965 | 350.8 | 9.431 | 150.2 | 44.396 | 501.0 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.278 | 500 | 13384 | 50000 | 1.268 | 3.74 | 1.00 |
