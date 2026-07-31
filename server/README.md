# queen — Queen Rust broker

The Queen message broker: an async Rust server (`tokio` + `axum` +
`deadpool-postgres`) on top of the **segments** storage engine in PostgreSQL.
The broker is the network layer — HTTP routes, message framing, zstd, cross-request
fusion, adaptive concurrency, JWT auth, and inter-instance (UDP) coordination.
All queue semantics live in PostgreSQL stored procedures (`queen.seg_*`).

The SQL schema (`sql/schema.sql` + `sql/procedures/*.sql`) is **embedded into the
binary at compile time** (`include_str!`) and applied automatically at startup, so
the running binary is fully self-contained — no schema files are needed at runtime.

## Prerequisites

- A recent stable **Rust** toolchain (the container build uses `rust:1-bookworm`).
- A reachable **PostgreSQL** (14+). The broker creates the `queen` / `queen_streams`
  schemas itself on first boot.

## Build

```bash
cd server
cargo build --release
# binary: target/release/queen
```

Behind an offline cache, add `CARGO_NET_OFFLINE=true`.

Editing any file under `sql/` changes the embedded schema, so **rebuild after
changing SQL**. The version reported by the broker comes from `server.json` via
`build.rs`, so a version bump there triggers a rebuild too.

## Run

Point it at a Postgres and start it. On boot it applies the schema (under a
`pg_advisory_lock`) and begins serving on `PORT` (default `6632`):

```bash
PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=postgres \
  ./target/release/queen
```

Boot log:

```
schema: applied schema.sql + 37 procedures
queen v1.0.0-beta.1 listening on 0.0.0.0:6632 (...)
```

Quick check:

```bash
curl localhost:6632/health
# {"status":"ok","database":"connected","engine":"segments-rust","version":"1.0.0-beta.1"}
```

## Configuration

All configuration is via environment variables. Only the Postgres ones are
typically needed; everything else has a working default.

**Boolean values** (`JWT_ENABLED`, `PG_USE_SSL`, the `QUEEN_*` kill switches, …)
accept `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`, case-insensitively. An
unset or empty value uses the documented default; **any other value fails the
boot** with a message naming the variable — a typo is never silently resolved to
a default (a `JWT_ENABLED=maybe` must not mean "authentication off"). At startup
the broker logs the effective resolved configuration under the `boot` target,
with secrets masked, so you can see exactly what it understood.

### Postgres

| Var | Default | |
|---|---|---|
| `PG_HOST` | `localhost` | |
| `PG_PORT` | `5432` | |
| `PG_USER` | `postgres` | |
| `PG_PASSWORD` | `postgres` | |
| `PG_DATABASE` | `postgres` | |
| `DB_POOL_SIZE` | `160` | connection pool size |

### Server

| Var | Default | |
|---|---|---|
| `PORT` | `6632` | HTTP listen port |
| `QUEEN_APPLY_SCHEMA` | `1` | set `0` to skip applying the schema at boot (externally managed DB) |
| `QUEEN_STATIC_DIR` | `webapp/dist` | directory served as the dashboard SPA (falls back to `index.html`); 404s if absent |
| `RETENTION_INTERVAL` | `300000` | retention/eviction sweep interval (ms) |
| `QUEEN_STMT_TIMEOUT_MS` | `30000` | per-statement timeout |
| `QUEEN_MAX_BODY_BYTES` | `67108864` | max request body (64 MiB) |
| `POP_DEFAULT_TIMEOUT_MS` | `2000` | default long-poll timeout |
| `POP_WAIT_POLL_MS` | `25` | long-poll re-check interval |

### Auth (JWT — off by default)

Set `JWT_ENABLED=true` to require a bearer token per request (5 access levels).

| Var | Default | |
|---|---|---|
| `JWT_ENABLED` | `false` | |
| `JWT_SECRET` | `""` | HS256 shared secret |
| `JWT_ALGORITHM` | `HS256` | `HS256`, or an RS/ES alg with `JWT_PUBLIC_KEY` |
| `JWT_PUBLIC_KEY` | `""` | PEM public key for asymmetric algs |
| `JWT_ISSUER` / `JWT_AUDIENCE` | `""` | optional `iss` / `aud` checks |
| `JWT_CLOCK_SKEW` | `30` | allowed skew (seconds) |
| `JWT_ROLES_CLAIM` / `JWT_ROLES_ARRAY_CLAIM` | `role` / `roles` | claim(s) carrying the role |
| `JWT_ROLE_ADMIN` / `_READ_WRITE` / `_READ_ONLY` / `_WRITE_ONLY` | `admin` / `read-write` / `read-only` / `write-only` | role-name mapping |
| `JWT_SKIP_PATHS` | — | comma-separated paths to leave unauthenticated |

### Multi-instance (UDP sync)

Peers coordinate over a raw UDP transport: cross-replica long-poll wakeups,
maintenance-mode propagation, and queue-config invalidation. With no peers it is a
pure in-process waker (no packets).

| Var | Default | |
|---|---|---|
| `QUEEN_SYNC_ENABLED` | `true` | |
| `QUEEN_UDP_PEERS` | `""` | comma-separated `host:port` peers |
| `QUEEN_UDP_NOTIFY_PORT` | `6633` | UDP listen port |
| `QUEEN_SYNC_SECRET` | `""` | HMAC secret for peer messages |
| `QUEEN_SYNC_HEARTBEAT_MS` | `1000` | |
| `QUEEN_SYNC_DEAD_THRESHOLD_MS` | `5000` | |
| `QUEEN_CACHE_REFRESH_INTERVAL_MS` | `60000` | queue-config cache refresh |
| `QUEEN_SERVER_ID` | hostname | replica identity |

### Performance tuning (internal — leave unset)

The fusion / concurrency engine self-tunes; these are override knobs, not part of
the product contract: `QUEEN_V2_FUSION_FRAMES` (500), `QUEEN_V2_FUSION_HOLD_MS`
(15), `QUEEN_V2_FUSION_SHARDS` (8), `QUEEN_V2_FUSION_MAX_INFLIGHT`,
`QUEEN_V2_ZSTD_LEVEL` (3), `QUEEN_V2_BUNDLE_MAX` / `QUEEN_V2_BUNDLE_LOG`, and the
Vegas bounds `QUEEN_SEG_{PUSH,POP}_{MIN,INIT,MAX}` (4 / 16 / 64).

## HTTP surface

- `GET /health` — liveness + DB connectivity + version.
- `GET /metrics`, `GET /metrics/prometheus` — metrics.
- `/api/v1/*` — push / pop / ack / transaction / lease, plus configure, resources,
  messages, DLQ, traces, status, analytics, and consumer-group management.
- `/streams/v1/*` — stream register / state / cycle.
- `/` and any unmatched path — the dashboard SPA from `QUEEN_STATIC_DIR` (a fallback
  only; it never shadows an API route).

## Offline rows→segments migration (legacy)

The binary carries an offline migration subcommand for moving data from the retired
rows engine into segments:

```bash
./target/release/queen migrate --mode {all|unconsumed|window} [--since <mins>] ...
```

Only relevant when upgrading an old rows-engine deployment; new deployments are
segments-only from the start.

## Docker

The crate's own image (broker only):

```bash
docker build -t queen server/
docker run -p 6632:6632 -e PG_HOST=your-db queen
```

The repo-root `Dockerfile` builds the full stack (broker + dashboard + `queenctl`
CLI); `build.sh <registry>` tags it `name:version` from `server.json`.

## Project layout

```
server/
├── Cargo.toml        # crate: queen, bin: queen
├── build.rs          # embeds server.json's version as QUEEN_VERSION
├── server.json       # { name, version } — source of truth for the version + image tag
├── Dockerfile        # broker-only image
├── sql/              # schema.sql + procedures/*.sql (embedded via include_str!)
└── src/
    ├── main.rs       # config, router, background services, startup
    ├── config.rs     # env → Config
    ├── schema.rs     # boot-time schema applier (advisory-locked)
    ├── db.rs         # SP call wrappers (tokio-postgres)
    ├── handlers/     # HTTP handlers, split by route domain
    ├── fusion.rs     # cross-request fusion + bundling
    ├── vegas.rs      # adaptive concurrency limiter
    ├── frames.rs     # zstd frame codec
    ├── auth.rs       # JWT validation + per-route access levels
    ├── udp.rs        # inter-instance UDP sync
    ├── notify.rs     # long-poll waker
    ├── retention.rs  # retention / eviction service
    ├── migrate.rs    # `queen migrate` subcommand
    └── metrics.rs
```

## Versioning

`server.json` is the single source of truth. `build.rs` embeds its `version` into
the binary (`/health`, startup log, `queenctl ping`), and `build.sh` uses it as the
Docker image tag. Bump it there.
