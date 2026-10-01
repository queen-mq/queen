# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5980.67 | 4564.46 | 8506.34 | 8991.80 | 32 | 0.86 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5980.92 | 4649.80 | 8596.47 | 9021.82 | 32 | 0.94 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5970.51 | 4625.19 | 8523.27 | 8973.26 | 32 | 0.86 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.024 | 26.7 | 1.000 | 58.7 | 101.305 | 83.5 | 1072.5 | 102.469 | 163.7 | 19.867 | 155.6 | 122.336 | 319.3 |
| queen-rust-fixed-r02 | 0.024 | 27.0 | 1.000 | 58.8 | 102.282 | 82.3 | 1067.2 | 103.473 | 162.7 | 20.286 | 152.7 | 123.758 | 315.2 |
| queen-rust-fixed-r03 | 0.024 | 26.7 | 1.000 | 58.7 | 101.054 | 82.6 | 1075.7 | 102.218 | 162.8 | 19.868 | 155.9 | 122.086 | 318.6 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25445 | 100000 | 1.254 | 3.93 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25401 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 1000 | 25410 | 100000 | 1.254 | 3.94 | 1.00 |
