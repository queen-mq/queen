# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 611.92 | 75867.90 | 85735.59 | 87150.75 | 32 | 0.35 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4327.09 | 11717.70 | 16581.60 | 17029.45 | 32 | 7.34 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4287.64 | 11951.06 | 16875.96 | 17384.67 | 32 | 7.29 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 581.22 | 77186.98 | 86550.50 | 88308.94 | 32 | 0.28 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 625.23 | 77036.95 | 87690.09 | 88457.62 | 32 | 0.29 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 4208.50 | 12542.15 | 16483.00 | 16940.22 | 32 | 8.29 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.265 | 58.7 | 1.000 | 97.5 | 161.728 | 924.5 | 1539.6 | 162.139 | 1035.1 | 41.970 | 345.9 | 204.109 | 1343.6 |
| queen-rust-fixed-r01 | 0.068 | 55.4 | 1.000 | 58.6 | 100.110 | 76.1 | 1057.3 | 101.637 | 158.5 | 19.458 | 153.7 | 121.094 | 312.1 |
| queen-rust-fixed-r02 | 0.071 | 55.2 | 1.000 | 58.5 | 101.370 | 76.1 | 1055.8 | 102.911 | 158.4 | 19.931 | 152.7 | 122.842 | 311.1 |
| horizon-fixed-r02 | 0.283 | 58.7 | 1.000 | 96.8 | 164.015 | 924.4 | 1540.9 | 164.455 | 1035.2 | 42.592 | 347.0 | 207.047 | 1350.2 |
| horizon-fixed-r03 | 0.265 | 58.9 | 1.000 | 97.4 | 178.141 | 924.2 | 1540.8 | 178.552 | 1035.0 | 42.232 | 348.3 | 220.784 | 1345.5 |
| queen-rust-fixed-r03 | 0.071 | 46.9 | 1.000 | 58.7 | 100.720 | 76.1 | 1056.8 | 102.269 | 159.4 | 19.836 | 157.0 | 122.105 | 315.6 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.045 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.263 | 1000 | 25337 | 100000 | 1.253 | 3.95 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25406 | 100000 | 1.254 | 3.94 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.046 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.041 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 1000 | 25406 | 100000 | 1.254 | 3.94 | 1.00 |
