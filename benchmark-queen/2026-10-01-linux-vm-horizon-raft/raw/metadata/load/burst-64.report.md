# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-auto-r01 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 824.41 | 164.61 | 1635.97 | 1806.19 | 34 | 1.31 | n/a |
| queen-rust-auto-r01 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 3146.64 | 4941.95 | 7211.40 | 7576.08 | 64 | 8.18 | 189.60 |
| queen-rust-auto-r02 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 3167.38 | 4892.34 | 7136.04 | 7504.84 | 64 | 8.15 | 189.63 |
| horizon-auto-r02 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 806.65 | 165.59 | 1618.07 | 1785.95 | 37 | 32.26 | n/a |
| horizon-auto-r03 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 810.44 | 168.72 | 1460.88 | 1617.54 | 33 | 1.31 | n/a |
| queen-rust-auto-r03 | yes | yes | 0 | 30000/30000 | 0 | 0 | 0 | 3232.00 | 4849.99 | 7192.97 | 7518.37 | 64 | 8.12 | 189.80 |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-auto-r01 | 0.457 | 70.8 | 1.000 | 96.9 | 63.018 | 978.4 | 1632.2 | 63.854 | 1034.0 | 11.787 | 103.4 | 75.642 | 1095.9 |
| queen-rust-auto-r01 | 0.399 | 43.0 | 1.000 | 60.3 | 32.414 | 124.3 | 2109.4 | 33.798 | 177.8 | 10.408 | 91.1 | 44.206 | 263.8 |
| queen-rust-auto-r02 | 0.402 | 42.8 | 1.000 | 60.3 | 32.535 | 124.3 | 2105.5 | 33.928 | 178.5 | 10.463 | 89.6 | 44.391 | 263.2 |
| horizon-auto-r02 | 0.467 | 70.5 | 1.000 | 96.9 | 66.715 | 1063.1 | 1777.8 | 67.542 | 1130.8 | 11.991 | 104.9 | 79.534 | 1217.0 |
| horizon-auto-r03 | 0.447 | 70.5 | 1.000 | 96.6 | 60.293 | 950.1 | 1585.0 | 61.197 | 1002.6 | 11.763 | 105.5 | 72.960 | 1039.2 |
| queen-rust-auto-r03 | 0.371 | 42.6 | 1.000 | 59.9 | 30.938 | 124.1 | 2094.7 | 32.260 | 178.2 | 9.747 | 88.1 | 42.007 | 262.9 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-auto-r01 | redis-info-commandstats | 34.225 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-auto-r01 | queen-process-prometheus | 1.508 | 300 | 14938 | 30000 | 1.498 | 2.01 | 1.00 |
| queen-rust-auto-r02 | queen-process-prometheus | 1.519 | 300 | 15279 | 30000 | 1.509 | 1.96 | 1.00 |
| horizon-auto-r02 | redis-info-commandstats | 34.331 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-auto-r03 | redis-info-commandstats | 34.103 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-auto-r03 | queen-process-prometheus | 1.509 | 300 | 14964 | 30000 | 1.499 | 2.00 | 1.00 |
