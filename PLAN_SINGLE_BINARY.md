# PLAN_SINGLE_BINARY — one process, zero Postgres (next version, on raft)

Draft 2026-09-22. It is complete in scope but rough in detail: line counts are
measured, estimates are guesses. It depends on `PLAN_RAFT.md` finishing (the raft
class becomes the only storage the deployment needs).

## 0. End state

- **One `queen` binary, one process per node.** A single node is 1 process plus 1
  data directory. An HA cell is the same binary on 3 machines (raft).
- **No Postgres anywhere.** Broker state lives in the raft store plus the queue
  logs. Proxy state (tenants, users, API keys, limits, usage, operator), Kafka
  coordination (groups, offsets, fencing), SQS state and S3-sink cursors all
  become raft state.
- **No child processes.** Kafka, SQS, the S3 sink, the console and OAuth all run
  as in-process tokio tasks.
- The `proxy` binary and the `queen-kafka` / `queen-sqs` / `queen-s3` binaries
  are either deleted or kept only as an optional remote deploy. Their HTTP client
  stays for tests.

## 1. Starting point (measured 2026-09-22)

| Piece | Today | Size |
|---|---|---|
| Broker, raft router | 29 routes (push/pop/ack/txn/lease/kv/timers/ephemeral/auth/status) | — |
| Broker, PG router | 82 routes. PG-only so far: `/api/v1/fetch`, `/api/v1/messages`, `/api/v1/dlq/:id/replay`, `/api/v1/traces*`, `/streams/v1/cycle`, `/api/v1/analytics/workload`, `/api/v1/status/queues`, resources/admin | — |
| `protocols/queen-kafka` | lib + bin, spawned by `server/src/kafka_facade.rs` (`QUEEN_KAFKA_EMBEDDED`) | ~51k lines; broker seam `src/queen.rs` 4.6k |
| `protocols/queen-sqs` | lib + bin, spawned by `server/src/sqs_facade.rs` | ~43k; seam `src/queen.rs` 6.2k |
| `connectors/queen-s3` | lib + bin, spawned by `server/src/s3_sink.rs` | ~13.6k; seam `src/queen.rs` 2.1k |
| `proxy/` | separate binary, own PG (migrations 001–010) | ~24k: gateway 4.3k, oauth 2.6k, auth 2.2k, registry 1.2k, console 1.1k, limits 1.1k, meter 0.9k, operator 0.8k, spool, kafka_identity, kafka_kv, s3_kv, cache, webapp |
| Panic strategy | `server/Cargo.toml:165` `panic = "abort"` | — |

## 2. Workstreams

### W0 — Raft route parity (prerequisite; the other agent's phases)
Every PG-router route gets served by the raft router, above all `fetch` plus
`partitions/changed` (the S3 sink and Kafka fetch depend on them). Then
messages/DLQ browse and replay, traces, streams, analytics, and the dashboard
and resources routes.
**Gate:** the client matrix and the dashboard run against `QUEEN_STORAGE=raft`
with zero 503 `raft_phase1_unsupported`.

### W1 — Panic policy (independent, do first)
- Build with `panic = "unwind"` and install a panic hook that calls
  `process::abort()` when the panicking thread is a CORE thread: batcher/planner,
  log writer, apply, raft. Use a thread-local marker or a thread-name allowlist.
  The core keeps "crash, then replay from durable state".
- Every other task unwinds: a tokio task panic kills only that connection or
  task. Listener supervisors restart a dead listener.
- **Test:** a panic injected into a facade or console task leaves the broker
  serving. A panic injected into apply aborts, and the process replays cleanly on
  restart.

### W2 — Facades in-process (Kafka, SQS, S3 sink)
1. Turn each `src/queen.rs` into a trait (`QueenApi`: push/pop/ack/fetch/KV/queues/identity...).
   Two impls: `Http` (today's client, kept for tests and remote deploys) and
   `InProcess`.
2. The first `InProcess` version calls the broker's service layer (the same
   functions the raft handlers call), skipping HTTP but still passing JSON.
   Later, typed calls skip JSON too.
3. The broker links the libs. Listeners start as tasks behind cargo features
   `kafka`, `sqs`, `s3` and config switches. Delete the spawn/supervise code in
   `kafka_facade.rs`, `sqs_facade.rs` and `s3_sink.rs`.
4. Isolation: one tokio runtime per facade with capped worker threads, plus hard
   per-connection buffer limits (no memory isolation in one process).
5. Kafka coordination (groups, offsets, fencing) and S3-sink cursors go from HTTP
   KV to direct raft commands. That removes the "KV 429 shows up as
   COORDINATOR_NOT_AVAILABLE" trap.

**Gate:** the queen-kafka 19-client matrix, the SQS matrix and the S3 compat
lane all pass in-process. The perf campaign is rerun: the facade cost on top of
the broker was 43% of broker CPU, and it should drop.

**Status (2026-09-23, branch `kafka-inproc`): Kafka steps 1-4 done, raft mode.**
`queen::HttpQueen` got a transport (`Http` | `Local(LocalDispatch)`) instead of a
second impl, so every call site stays shared; the in-process dispatch is the
broker's axum `Router` called as a service, spawned onto the broker runtime
(server/src/kafka_inproc.rs). The boot moved from queen-kafka's `main.rs` into
`queen_kafka::boot` (`Config` + `serve`). Feature `kafka` (default on); the
release profile unwinds and `obs::install_panic_hook` aborts off the
`queen-kafka` threads (W1 for this one facade). Explicit `QUEEN_URL` stays on
HTTP. The Postgres boot keeps the child.

**Status (2026-09-23, phase 2 of the Kafka-on-raft plan): typed record path.**
Produce appends RecordBatch v2 bytes verbatim (one `Append` per partition,
riding a `PushCommand` whose single item starts with the `FF FF FF FF` magic,
planned by `rsm/planner/kafka.rs`: offsets stamped outside the CRC, synthetic
per-record hashes `kafka:<offset>` so native pop/ack work unchanged); Fetch
serves the stored bytes (`Rsm::kafka_read`), single-topic fetches park on the
queue gate. The idempotent-producer window moved into the log (KV row
`qk:seq:<pid>:<producer>` written in the append's entry). The facade's KV calls
skip the router and the KV rate ladder (`handlers::facade_kv`). Both the
verbatim append and the direct KV run on the raft leader only; a follower goes
through the router, which forwards to the leader. Not done: step 5 proper —
committed offsets as native consumer-group cursors (with commit metadata on the
cursor row) instead of `qk:group:` KV rows, approved 2026-09-23 and waiting for
the lanes rework; S3-sink cursors; transactions still use the facade stage +
EndTxn bundle.

### W3 — Proxy data plane into the broker
- **Auth:** API keys (hashed, RAM lookup) and JWT verification, with a cache of
  verified callers (port `cache.rs`). The tenant comes from the verified caller.
  The tenancy-header trust mode survives only as the migration bridge.
- **Registry:** tenants, users, keys, members and families become raft keyspaces
  (rare writes).
- **Limits:** they become planner admission. A push over the storage or rate
  quota is refused atomically in the plan. "Freeze = 429, never 403" is kept.
- **Metering:** per-tenant counters computed in apply, identical on every node
  and saved with the checkpoint. There are no per-request usage rows.
- **Tokens:** `/session-token` and JWKS. Signing keys are raft state; sessions
  are stateless signed tokens plus a replicated revocation epoch (sessions are
  not replicated).
- **Facade identity:** Kafka SASL and the S3/SQS credentials (`kafka_identity`,
  `kafka_kv`, `s3_kv`) resolve against the same registry.
- **Deleted:** `gateway.rs`. Routing becomes raft follower→leader forwarding.

### W4 — Proxy web plane into the broker
OAuth (GitHub/Google), console, `/me`, `/members`, `/users`, `/keys`, `/usage`,
`/overview` and operator acting-as run as in-process routes on a separate
listener or port (the internet-facing UX surface), protected by W1 and W7. The
dashboard login uses the same sessions.

### W5 — State and migration
- **Raft keyspaces:** one per proxy table family from migrations 001–010.
- **One-shot import:** proxy PG → raft (tenants, users, keys, limits, usage
  history cut to the retention window).
- **Existing PG-class cells:** messages are not migrated in place. Use
  drain-and-switch per queue or per cell: new writes go to raft, the old queue
  drains, then the cutover.
- **Backups:** raft snapshot plus queue-log files, and a restore drill.

### W6 — Cloud fleet layer
Tenant → cell placement, signups and billing sit above any single cell. They get
a home in a small "control" Queen cell (KV plus queues), not in PG. Terraform
stays the fleet provisioner only.

### W7 — Hardening (the broker becomes internet-facing)
- TLS (rustls) on every listener.
- Rate limits per IP and per tenant, fixing the XFF throttle finding from the
  2026-09-05 security sweep.
- Request size and timeout limits.
- Cookie, CSRF and CORS rules for the web listener.
- Brute-force protection on login.
- Fuzzing of every untrusted decoder: Kafka wire, SQS JSON/query, SigV4, the
  HTTP JSON bodies.

### W8 — Packaging, config, docs
- One Dockerfile and one helm chart: a StatefulSet of 1 or 3, no PG subchart, no
  proxy Deployment.
- Proxy env vars merge into the broker config under one namespace.
- Cargo features for slim builds.
- The embedded `queen::Broker` API gets the in-process facades too.
- webdoc: the "one binary, no Postgres" install page, plus a migration guide.

### W9 — Cleanup
Delete the child-process supervisors, the proxy's PG layer (`db.rs`, `pgtls.rs`,
migrations) and the spool if it is no longer needed. Retire the tenancy-header
mode after the migration window.

## 3. Milestones

| M | Content | Gate |
|---|---|---|
| M1 | W1 panic policy | injected panics behave as specified |
| M2 | W0 parity done (other agent) | client matrix + dashboard on raft, no 503 |
| M3 | W2 facades in-process | kafka/sqs/s3 matrices in-process, perf rerun |
| M4 | W3 + W4 + W5 proxy in-broker | proxy smoke (23/23 era) against a single binary, import tool round-trip |
| M5 | W6 + W7 + W8 | 3-node cell on the VM, soak, kill tests under load, restore drill |
| M6 | W9 | the repo builds one binary; proxy/facade bins optional or gone |

## 4. Open decisions

1. Keep the PG storage class as a product option ("use your Postgres"), or drop
   it in this version.
2. Keep the standalone facade and proxy binaries as an optional remote deploy,
   or delete them.
3. Web listener: a separate port (recommended) or the same one.
4. Metering granularity and retention of usage history in raft.
5. Where the fleet control cell lives, and who operates it.

## 5. Risks

- **One address space:** a memory blow-up in a facade OOMs the broker. Mitigate
  with hard limits and per-facade budgets.
- **CPU contention:** TLS, JWT and facade codecs compete with the planner. Use
  separate runtimes and measure at the FAT100 ceiling.
- **Blast radius of web code:** W1 plus W7 must land before W4 goes live.
- **Lockstep releases:** every proxy or facade fix is a broker release. Rolling
  raft restarts make that acceptable.
- **Migration of live cloud cells:** drain-and-switch needs a runbook and a
  rollback path.
