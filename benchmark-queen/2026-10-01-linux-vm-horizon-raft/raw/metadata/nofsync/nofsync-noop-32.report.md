# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 1242.38 | 15508.68 | 41825.85 | 44562.67 | 32 | 0.81 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5708.37 | 4718.15 | 9182.39 | 9622.95 | 32 | 0.37 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5739.80 | 4821.50 | 8612.76 | 9033.09 | 32 | 0.85 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 1258.79 | 16697.56 | 41174.05 | 43600.00 | 32 | 0.78 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 1258.88 | 16663.03 | 39319.23 | 42669.37 | 32 | 0.79 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 100000/100000 | 0 | 0 | 0 | 5928.81 | 4547.70 | 8602.11 | 9079.79 | 32 | 0.88 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.195 | 58.6 | 1.000 | 97.1 | 107.499 | 924.3 | 1542.3 | 107.699 | 1035.7 | 31.244 | 340.6 | 138.942 | 1358.4 |
| queen-rust-fixed-r01 | 0.025 | 27.1 | 1.000 | 58.9 | 104.221 | 83.3 | 1071.4 | 105.435 | 163.7 | 21.050 | 155.4 | 126.485 | 319.1 |
| queen-rust-fixed-r02 | 0.024 | 26.9 | 1.000 | 58.9 | 102.490 | 82.5 | 1070.5 | 103.680 | 162.7 | 20.521 | 157.3 | 124.201 | 320.0 |
| horizon-fixed-r02 | 0.192 | 58.7 | 1.000 | 96.9 | 106.139 | 924.5 | 1539.3 | 106.334 | 1036.5 | 30.727 | 343.3 | 137.061 | 1363.2 |
| horizon-fixed-r03 | 0.186 | 58.6 | 1.000 | 97.1 | 107.265 | 924.5 | 1542.4 | 107.454 | 1035.6 | 32.303 | 352.2 | 139.757 | 1367.6 |
| queen-rust-fixed-r03 | 0.024 | 26.6 | 1.000 | 58.6 | 102.232 | 82.6 | 1068.7 | 103.419 | 163.0 | 20.144 | 155.1 | 123.564 | 318.1 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.192 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.264 | 1000 | 25432 | 100000 | 1.254 | 3.93 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.264 | 1000 | 25380 | 100000 | 1.254 | 3.94 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.184 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.198 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.264 | 1000 | 25410 | 100000 | 1.254 | 3.94 | 1.00 |
