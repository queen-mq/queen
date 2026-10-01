# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 627.69 | 39590.06 | 43359.34 | 43592.86 | 32 | 0.30 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2218.82 | 11824.54 | 18434.27 | 19071.28 | 32 | 5.30 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2235.39 | 11819.56 | 18391.75 | 19058.06 | 32 | 5.30 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 629.69 | 39823.78 | 44630.99 | 44932.22 | 32 | 0.29 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 600.44 | 39898.42 | 44464.33 | 44714.93 | 32 | 0.84 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2242.72 | 11414.77 | 18121.75 | 18821.89 | 32 | 4.97 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.130 | 58.6 | 1.000 | 96.5 | 93.995 | 923.7 | 1540.2 | 94.263 | 1004.4 | 21.102 | 177.5 | 115.364 | 1166.6 |
| queen-rust-fixed-r01 | 0.066 | 55.3 | 1.000 | 58.5 | 48.847 | 75.8 | 1052.3 | 49.789 | 128.8 | 12.695 | 100.8 | 62.484 | 229.6 |
| queen-rust-fixed-r02 | 0.070 | 55.4 | 1.000 | 58.6 | 47.920 | 75.4 | 1046.6 | 48.866 | 128.5 | 12.334 | 97.9 | 61.200 | 226.2 |
| horizon-fixed-r02 | 0.127 | 58.7 | 1.000 | 97.2 | 92.085 | 923.8 | 1537.8 | 92.390 | 1004.7 | 20.820 | 177.4 | 113.209 | 1168.5 |
| horizon-fixed-r03 | 0.132 | 58.6 | 1.000 | 97.0 | 88.346 | 924.2 | 1540.9 | 88.612 | 1005.5 | 20.881 | 177.9 | 109.493 | 1167.7 |
| queen-rust-fixed-r03 | 0.065 | 54.3 | 1.000 | 58.9 | 48.032 | 75.5 | 1047.5 | 48.964 | 129.1 | 12.290 | 98.3 | 61.254 | 227.2 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.049 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.266 | 500 | 12814 | 50000 | 1.256 | 3.90 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.266 | 500 | 12802 | 50000 | 1.256 | 3.91 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.045 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.050 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.268 | 500 | 12906 | 50000 | 1.258 | 3.87 | 1.00 |
