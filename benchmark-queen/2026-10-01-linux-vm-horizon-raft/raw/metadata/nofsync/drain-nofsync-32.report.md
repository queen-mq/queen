# Laravel supervisor benchmark

Correctness is a gate: resource and latency comparisons are not valid for incomplete, duplicate, failed or non-quiescent runs.

| Scenario | Correct | Queue idle | Queue size | Completed | Missing | Duplicates | Failed | jobs/s | E2E p50 ms | p95 ms | p99 ms | Peak workers | To peak s | Return s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1013.44 | 19985.79 | 32707.99 | 34235.80 | 32 | 0.33 | n/a |
| queen-rust-fixed-r01 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2248.19 | 11198.03 | 17817.65 | 18482.27 | 32 | 4.78 | n/a |
| queen-rust-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2122.13 | 12193.07 | 18290.19 | 18949.12 | 32 | 6.11 | n/a |
| horizon-fixed-r02 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 1040.25 | 19308.68 | 30988.76 | 32313.80 | 32 | 0.33 | n/a |
| horizon-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 960.68 | 20360.30 | 34149.46 | 35500.86 | 32 | 0.41 | n/a |
| queen-rust-fixed-r03 | yes | yes | 0 | 50000/50000 | 0 | 0 | 0 | 2211.66 | 11821.71 | 18466.42 | 19096.10 | 32 | 4.94 | n/a |

## Resources (warm-up excluded, post-drain included)

| Scenario | Orch CPU s | Orch peak PSS MiB | PSS cov. | Orch peak RSS MiB | Workers CPU s | Workers peak PSS MiB | Workers peak RSS MiB | App CPU s | App peak MiB | Backend CPU s | Backend peak MiB | Stack CPU s | Stack peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | 0.102 | 58.7 | 1.000 | 97.2 | 69.783 | 923.8 | 1539.9 | 70.022 | 1005.2 | 17.493 | 180.2 | 87.515 | 1172.3 |
| queen-rust-fixed-r01 | 0.066 | 47.3 | 1.000 | 58.4 | 48.582 | 75.7 | 1050.6 | 49.529 | 128.7 | 12.639 | 98.6 | 62.168 | 227.1 |
| queen-rust-fixed-r02 | 0.071 | 55.7 | 1.000 | 58.9 | 49.935 | 75.7 | 1049.8 | 50.952 | 128.8 | 12.898 | 98.2 | 63.850 | 226.7 |
| horizon-fixed-r02 | 0.098 | 58.6 | 1.000 | 96.7 | 70.735 | 924.3 | 1539.8 | 70.972 | 1005.8 | 17.522 | 184.4 | 88.494 | 1175.5 |
| horizon-fixed-r03 | 0.100 | 58.6 | 1.000 | 97.0 | 68.100 | 924.1 | 1541.1 | 68.337 | 1006.0 | 17.279 | 184.4 | 85.616 | 1171.9 |
| queen-rust-fixed-r03 | 0.067 | 55.5 | 1.000 | 58.6 | 49.163 | 75.7 | 1051.4 | 50.118 | 129.4 | 12.683 | 101.1 | 62.801 | 230.4 |

## Backend operation shape

Redis values are protocol commands; Queen values are broker API requests. They are intentionally not used as cross-engine ratios because the units are not semantically identical.

| Scenario | Source | Backend units/job | Push requests | Pop requests | ACK requests | Consumer requests/job | Pop messages/request | ACK messages/request |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| horizon-fixed-r01 | redis-info-commandstats | 33.048 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r01 | queen-process-prometheus | 1.267 | 500 | 12841 | 50000 | 1.257 | 3.89 | 1.00 |
| queen-rust-fixed-r02 | queen-process-prometheus | 1.267 | 500 | 12844 | 50000 | 1.257 | 3.89 | 1.00 |
| horizon-fixed-r02 | redis-info-commandstats | 33.048 | n/a | n/a | n/a | n/a | n/a | n/a |
| horizon-fixed-r03 | redis-info-commandstats | 33.051 | n/a | n/a | n/a | n/a | n/a | n/a |
| queen-rust-fixed-r03 | queen-process-prometheus | 1.266 | 500 | 12819 | 50000 | 1.256 | 3.90 | 1.00 |
