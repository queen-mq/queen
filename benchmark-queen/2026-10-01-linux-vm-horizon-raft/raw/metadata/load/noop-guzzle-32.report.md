# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5424.71 | 4625.06 | 8795.89 | 9253.54 | 32 | 0.87 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5484.03 | 4617.91 | 8705.99 | 9169.15 | 32 | 0.92 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5376.51 | 5117.29 | 9313.51 | 9807.25 | 32 | 0.36 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.028 | 27.0 | 1.000 | 58.8 | 140.423 | 87.8 | 1092.6 | 141.561 | 168.1 | 21.484 | 156.7 | 163.045 | 324.7 |
| queen-rust-fixed-r02 | 0.026 | 26.7 | 1.000 | 58.8 | 140.187 | 89.4 | 1101.0 | 141.317 | 170.3 | 21.542 | 156.8 | 162.859 | 326.6 |
| queen-rust-fixed-r03 | 0.027 | 26.9 | 1.000 | 58.7 | 140.182 | 87.2 | 1089.2 | 141.319 | 167.1 | 21.517 | 155.4 | 162.837 | 322.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25386 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25363 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.265 | 1000 | 25465 | 100000 | 1.255 | 3.93 | 1.00 |
