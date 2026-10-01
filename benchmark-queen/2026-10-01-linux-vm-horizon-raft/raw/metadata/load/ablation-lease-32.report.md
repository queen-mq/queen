# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1911.69 | 11487.99 | 21876.08 | 22808.48 | 32 | 0.84 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1874.68 | 12062.54 | 22367.36 | 23297.61 | 32 | 0.33 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1870.91 | 11723.59 | 22132.26 | 23088.93 | 32 | 0.37 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.034 | 27.0 | 1.000 | 59.1 | 53.633 | 79.0 | 1066.0 | 54.364 | 130.0 | 13.173 | 100.7 | 67.537 | 230.0 |
| queen-rust-fixed-r02 | 0.035 | 26.9 | 1.000 | 58.7 | 53.552 | 79.2 | 1063.2 | 54.290 | 130.3 | 13.196 | 104.4 | 67.487 | 234.4 |
| queen-rust-fixed-r03 | 0.035 | 26.4 | 1.000 | 58.3 | 54.114 | 79.2 | 1064.4 | 54.854 | 130.6 | 13.324 | 99.9 | 68.178 | 230.5 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.262 | 500 | 12605 | 50000 | 1.252 | 3.97 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.262 | 500 | 12623 | 50000 | 1.252 | 3.96 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.262 | 500 | 12613 | 50000 | 1.252 | 3.96 | 1.00 |
