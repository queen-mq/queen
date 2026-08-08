# queen-engine

The Queen MQ broker: a PostgreSQL-backed message broker with per-entity ordered
partitions, consumer groups with replay, leases and explicit acks, transactional
handoff, windowed deduplication and a dead-letter queue. All queue semantics live
in PostgreSQL stored procedures, embedded into this crate at compile time and
applied at boot under an advisory lock — the running broker is self-contained and
needs no schema files at runtime.

One crate, two surfaces:

- **A library** (`use queen::…` — the package is `queen-engine` because the bare
  crates.io name `queen` belongs to an unrelated crate): run the broker inside
  your own Rust process. `Broker::start` boots the same engine the standalone
  binary runs, against the PostgreSQL you point it at.
- **A binary** (`queen`, behind the default `server` feature): the standalone
  HTTP broker the container images ship.

Documentation: **[queenmq.com](https://queenmq.com/)** — the embedded guide is at
[/use/embed](https://queenmq.com/use/embed), the Rust API reference at
[/reference/engine](https://queenmq.com/reference/engine), the HTTP API and the
generated configuration reference under [/reference](https://queenmq.com/reference).

## Embed the broker

```toml
[dependencies]
queen-engine = { version = "1.0.0-beta.4", default-features = false }
```

`default-features = false` skips the HTTP serve stack, the dashboard and the
tracing subscriber: an embedding application owns its own surface and its own
logging. Requires Rust 1.88.

```rust
use queen::{Broker, BrokerConfig};
use queen::protocol as qp;

let broker = Broker::start(
    BrokerConfig::new().pg("localhost", 5432, "postgres", "postgres", "postgres"),
)
.await?;

broker.configure(&qp::ConfigureRequest::new("jobs")).await?;
broker.push(vec![qp::PushItem::new("jobs", serde_json::json!({"n": 1}))]).await?;
let popped = broker.pop("jobs", &qp::PopParams::default()).await?;
```

Every operation invokes the same handler functions the broker's HTTP router
dispatches to, so behaviour and defaults are the broker's by construction. The
embedded API is **beta**: one `Broker` per process lifetime is the supported
shape, shutdown is best-effort, the outage spool defaults to a per-instance temp
directory (configure `spool_dir` for durability across restarts), and the surface
is the data plane plus the DLQ. The full list of boundaries:
[queenmq.com/use/embed](https://queenmq.com/use/embed).

## Run the binary

From the repository (the released container is `ghcr.io/queen-mq/queen`):

```bash
cargo build --release
PG_HOST=localhost PG_PASSWORD=postgres ./target/release/queen
```

On boot it applies the schema and serves on `PORT` (default `6632`): the HTTP
API under `/api/v1` and `/streams/v1`, `/health`, `/metrics` and
`/metrics/prometheus`, and the dashboard SPA as the fallback route.

Configuration is environment variables; only the `PG_*` ones are typically
needed. Boolean knobs accept `true/false`, `1/0`, `yes/no`, `on/off`; any other
value fails the boot with a message naming the variable (embedded, it becomes a
`StartError::Config` instead of an exit). The full generated reference is at
[queenmq.com/reference/config](https://queenmq.com/reference/config).

Note for `cargo install` users: the dashboard bundle is a build product of the
repository and the Docker image, not crate content — a broker built from
crates.io serves the API but not a working dashboard. Use the container image if
you want the UI.

## Layout

```
server/
├── Cargo.toml        # package: queen-engine, lib: queen, bin: queen (feature "server")
├── build.rs          # embeds server.json's version as QUEEN_VERSION
├── server.json       # { name, version } — source of truth for version + image tag
├── sql/              # schema.sql + procedures/*.sql (embedded via include_str!)
└── src/
    ├── main.rs       # the binary: config, router, background services, startup
    ├── lib.rs        # the library: same modules, plus src/embedded/ (queen::Broker)
    ├── embedded/     # the embedded facade: BrokerConfig, Broker, boot
    ├── handlers/     # HTTP handlers, split by route domain
    ├── fusion.rs     # cross-request commit fusion + bundling
    ├── admission.rs  # write-transaction admission arbiter
    ├── schema.rs     # boot-time schema applier (advisory-locked)
    └── ...
```

Editing anything under `sql/` changes the embedded schema, so rebuild after
changing SQL: the binary you are running still carries the previous version of
your stored procedure.

## Versioning

`server.json` is the single source of truth for the broker version (`/health`,
the boot log, the Docker tag via `build.sh`); `Cargo.toml`'s `version` must say
the same string, since it is what crates.io serves. Bump both together.

License: Apache-2.0. Source and issues: [github.com/queen-mq/queen](https://github.com/queen-mq/queen).
