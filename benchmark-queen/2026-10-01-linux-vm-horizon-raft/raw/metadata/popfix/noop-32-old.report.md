# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5940.28 | 4899.40 | 9000.49 | 9416.94 | 32 | 0.36 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5934.88 | 4496.26 | 8594.38 | 9080.04 | 32 | 0.95 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5940.96 | 4568.63 | 8590.71 | 9078.49 | 32 | 0.91 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.024 | 26.8 | 1.000 | 58.7 | 100.776 | 83.0 | 1072.6 | 101.955 | 164.0 | 19.725 | 156.8 | 121.681 | 320.8 |
| queen-rust-fixed-r02 | 0.024 | 26.9 | 1.000 | 58.7 | 101.402 | 82.1 | 1065.3 | 102.580 | 162.6 | 20.176 | 154.8 | 122.756 | 317.4 |
| queen-rust-fixed-r03 | 0.024 | 26.6 | 1.000 | 58.5 | 101.054 | 83.3 | 1067.4 | 102.229 | 164.2 | 19.863 | 157.6 | 122.092 | 321.7 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.263 | 1000 | 25341 | 100000 | 1.253 | 3.95 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25425 | 100000 | 1.254 | 3.93 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.265 | 1000 | 25464 | 100000 | 1.255 | 3.93 | 1.00 |
