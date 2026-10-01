# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 713.83 | 213.24 | 293.41 | 617.54 | 16 | 0.31 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 1417.58 | 9593.53 | 18072.32 | 18848.34 | 16 | 0.43 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 1428.55 | 9442.74 | 18097.61 | 18889.88 | 16 | 0.40 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 712.98 | 213.18 | 300.35 | 754.26 | 16 | 0.34 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 697.98 | 214.72 | 321.81 | 619.40 | 16 | 0.29 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 1431.07 | 9493.97 | 18002.67 | 18810.88 | 16 | 0.93 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.070 | 59.4 | 1.000 | 96.4 | 45.829 | 470.1 | 768.2 | 45.899 | 534.0 | 11.058 | 101.9 | 56.957 | 634.4 |
| queen-rust-fixed-r01 | 0.024 | 27.3 | 1.000 | 58.1 | 25.981 | 58.1 | 538.0 | 26.319 | 90.5 | 8.090 | 77.9 | 34.410 | 168.0 |
| queen-rust-fixed-r02 | 0.023 | 27.6 | 1.000 | 58.4 | 25.342 | 57.7 | 536.8 | 25.677 | 90.2 | 7.949 | 75.5 | 33.626 | 165.7 |
| horizon-fixed-r02 | 0.076 | 59.4 | 1.000 | 96.9 | 44.923 | 470.0 | 767.9 | 45.001 | 534.1 | 11.222 | 102.8 | 56.223 | 635.1 |
| horizon-fixed-r03 | 0.072 | 59.3 | 1.000 | 96.4 | 45.576 | 470.2 | 768.1 | 45.649 | 533.7 | 11.138 | 101.2 | 56.787 | 633.8 |
| queen-rust-fixed-r03 | 0.024 | 27.1 | 1.000 | 58.4 | 25.748 | 59.0 | 544.6 | 26.087 | 91.3 | 8.182 | 78.8 | 34.269 | 169.7 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.360 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.265 | 300 | 7641 | 30000 | 1.255 | 3.93 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 300 | 7620 | 30000 | 1.254 | 3.94 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.370 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.358 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 300 | 7625 | 30000 | 1.254 | 3.93 | 1.00 |
