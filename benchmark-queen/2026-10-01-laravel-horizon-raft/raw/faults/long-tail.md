# Fault/recovery smoke report

| Engine | Scenario | Respawn ms | Unique | Missing | Completion duplicates | Idempotency dedup hits | Effects | Conservation | Retry observed | Queue zero | Restarts/OOM | At-least-once | Strict execution |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- |
| queen-rust | worker-sigkill | 1542.4 | 16/16 | 0 | 0 | 0 | 16 | pass | yes | yes | pass | pass | FAIL |

The at-least-once gate requires worker respawn, every unique job, fixture-effect conservation, and no container restart/OOM. 
The stricter execution gate additionally requires one attempt per job, zero completion duplicates and no failure signal in logs. 
A retry is legal under at-least-once delivery; the `(run_id, job_id)` key converts a repeated fixture effect into an observable dedup hit.
The SQLite ledger witnesses only fixture-local idempotent effects; it does not prove exactly-once effects in external systems.

See `metadata.json`, each lane's `fault-result.json`, `ledger-check.json`, raw SQLite/JSONL results, process trees, resolved Compose file, container inspections and logs.
