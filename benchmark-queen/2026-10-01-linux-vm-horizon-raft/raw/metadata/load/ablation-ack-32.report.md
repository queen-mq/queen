# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2234.94 | 9315.98 | 17716.10 | 18553.60 | 32 | 0.86 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2263.70 | 9612.68 | 17911.96 | 18688.55 | 32 | 0.35 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2288.64 | 9235.97 | 17613.16 | 18368.89 | 32 | 0.82 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | 0.029 | 27.0 | 1.000 | 58.6 | 52.719 | 81.4 | 1065.9 | 53.423 | 133.0 | 12.477 | 109.5 | 65.900 | 242.2 |
| queen-rust-fixed-r02 | 0.030 | 27.2 | 1.000 | 58.9 | 50.021 | 81.0 | 1063.0 | 50.715 | 132.0 | 12.220 | 110.2 | 62.935 | 241.5 |
| queen-rust-fixed-r03 | 0.030 | 27.1 | 1.000 | 58.6 | 52.010 | 80.8 | 1062.5 | 52.706 | 132.5 | 12.424 | 109.1 | 65.131 | 241.4 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| queen-rust-fixed-r01 | queen-process-prometheus | 1.262 | 500 | 12608 | 50000 | 1.252 | 3.97 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.262 | 500 | 12618 | 50000 | 1.252 | 3.96 | 1.00 |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.262 | 500 | 12606 | 50000 | 1.252 | 3.97 | 1.00 |
