# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 6011.82 | 4516.17 | 8512.40 | 8958.13 | 32 | 0.85 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5961.37 | 4444.78 | 8492.21 | 9006.58 | 32 | 0.88 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 6038.50 | 4603.35 | 8432.37 | 8834.24 | 32 | 0.86 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.024 | 26.9 | 1.000 | 58.5 | 101.708 | 82.3 | 1061.9 | 102.889 | 162.4 | 20.051 | 157.6 | 122.940 | 320.0 |
| queen-rust-fixed-r02 | 0.024 | 26.4 | 1.000 | 58.4 | 101.664 | 82.7 | 1069.0 | 102.845 | 162.5 | 20.107 | 153.5 | 122.952 | 316.0 |
| queen-rust-fixed-r03 | 0.023 | 26.7 | 1.000 | 58.5 | 100.918 | 82.3 | 1065.0 | 102.083 | 162.3 | 19.852 | 152.2 | 121.935 | 314.3 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25363 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25428 | 100000 | 1.254 | 3.93 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 1000 | 25353 | 100000 | 1.254 | 3.94 | 1.00 |
