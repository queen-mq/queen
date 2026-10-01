# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 818.73 | 141.70 | 213.66 | 256.17 | 32 | 0.81 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 6118.20 | 4577.05 | 8687.83 | 9101.50 | 32 | 0.88 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5884.90 | 5122.96 | 9196.41 | 9622.40 | 32 | 0.39 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 892.39 | 133.88 | 174.55 | 196.21 | 32 | 0.84 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 887.70 | 134.46 | 175.12 | 196.54 | 32 | 0.85 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5958.83 | 4888.21 | 9049.46 | 9534.64 | 32 | 0.40 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.218 | 58.5 | 1.000 | 97.0 | 190.125 | 922.6 | 1538.5 | 190.345 | 1032.6 | 38.270 | 265.3 | 228.615 | 1295.2 |
| queen-rust-fixed-r01 | 0.021 | 27.2 | 1.000 | 58.9 | 98.439 | 82.3 | 1062.7 | 99.555 | 162.8 | 18.648 | 151.1 | 118.203 | 313.6 |
| queen-rust-fixed-r02 | 0.024 | 26.9 | 1.000 | 58.7 | 98.803 | 82.3 | 1073.0 | 99.919 | 162.4 | 18.719 | 156.1 | 118.638 | 318.2 |
| horizon-fixed-r02 | 0.205 | 58.6 | 1.000 | 97.4 | 187.126 | 922.6 | 1535.1 | 187.333 | 1032.6 | 37.868 | 272.0 | 225.201 | 1302.4 |
| horizon-fixed-r03 | 0.209 | 58.6 | 1.000 | 96.8 | 187.380 | 922.6 | 1535.6 | 187.591 | 1032.4 | 37.985 | 275.4 | 225.576 | 1305.8 |
| queen-rust-fixed-r03 | 0.023 | 26.8 | 1.000 | 58.7 | 98.729 | 84.1 | 1070.6 | 99.850 | 165.3 | 18.814 | 155.1 | 118.664 | 320.1 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.994 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25371 | 100000 | 1.254 | 3.94 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25420 | 100000 | 1.254 | 3.93 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.992 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.993 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 1000 | 25429 | 100000 | 1.254 | 3.93 | 1.00 |
