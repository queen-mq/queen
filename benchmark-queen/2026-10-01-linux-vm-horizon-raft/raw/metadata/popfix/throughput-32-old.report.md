# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2721.19 | 7471.52 | 13932.35 | 14607.61 | 32 | 0.37 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2705.60 | 7588.85 | 14073.04 | 14721.47 | 32 | 0.35 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2741.12 | 7589.11 | 14042.18 | 14696.51 | 32 | 0.41 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.026 | 26.9 | 1.000 | 58.5 | 48.596 | 82.7 | 1062.2 | 49.180 | 133.5 | 12.686 | 96.8 | 61.866 | 230.1 |
| queen-rust-fixed-r02 | 0.026 | 26.9 | 1.000 | 58.5 | 48.898 | 82.0 | 1063.2 | 49.480 | 132.9 | 12.601 | 100.2 | 62.081 | 233.0 |
| queen-rust-fixed-r03 | 0.025 | 26.7 | 1.000 | 58.4 | 48.187 | 84.0 | 1074.5 | 48.769 | 134.5 | 12.469 | 99.5 | 61.238 | 233.8 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.267 | 500 | 12841 | 50000 | 1.257 | 3.89 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.267 | 500 | 12873 | 50000 | 1.257 | 3.88 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.267 | 500 | 12873 | 50000 | 1.257 | 3.88 | 1.00 |
