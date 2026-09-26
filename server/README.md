# queen-engine

The Queen MQ broker: a message broker on its own replicated log, with per-entity
ordered partitions, consumer groups with replay, leases and explicit acks,
transactional handoff, windowed deduplication, a dead-letter queue, key/value state
and timers. Every node keeps its state in one data directory on local disk: queue
logs for the payloads and an embedded ordered store for everything the broker looks
up. A cluster of three or five nodes replicates the log with Raft. There is no
external database.

One crate, two surfaces:

- **A library** (`use queen::…`; the package is `queen-engine` because the bare
  crates.io name `queen` belongs to an unrelated crate): run the broker inside
  your own Rust process. `Broker::start` boots the same engine the standalone
  binary runs, on a data directory you give it.
- **A binary** (`queen`, behind the default `server` feature): the standalone
  HTTP broker the container image ships, with the proxy and the Kafka facade
  linked in and switched on by environment.

Documentation: **[queenmq.com](https://queenmq.com/)**. The embedded guide is at
[/use/embed](https://queenmq.com/use/embed), the Rust API reference at
[/reference/engine](https://queenmq.com/reference/engine), the HTTP API and the
generated configuration reference under [/reference](https://queenmq.com/reference).

## Embed the broker

```toml
[dependencies]
queen-engine = { version = "2.0.0-alpha.4", default-features = false }
```

`default-features = false` skips the HTTP serve stack, the dashboard and the
tracing subscriber: an embedding application owns its own surface and its own
logging. Requires Rust 1.88. The 2.0 line is in alpha: pin the exact version you
tested.

```rust
use queen::{Broker, BrokerConfig};
use queen::protocol as qp;

let broker = Broker::start(BrokerConfig::new().raft("/var/lib/myapp/queen")).await?;

broker.configure(&qp::ConfigureRequest::new("jobs")).await?;
broker.push(vec![qp::PushItem::new("jobs", serde_json::json!({"n": 1}))]).await?;
let popped = broker.pop("jobs", &qp::PopParams::default()).await?;
```

Every operation invokes the same handler functions the broker's HTTP router
dispatches to, so behaviour and defaults are the broker's by construction. The
embedded API is **beta**. A data directory is required, through
`BrokerConfig::raft` or `QUEEN_RAFT_DIR`, and one directory belongs to one
`Broker` at a time. The full list of boundaries:
[queenmq.com/use/embed](https://queenmq.com/use/embed).

## Run the binary

From the repository (the released container is `ghcr.io/queen-mq/queen`):

```bash
cargo build --release
QUEEN_RAFT_DIR=./queen-data ./target/release/queen
```

It opens the data directory, recovering whatever it holds, and serves on `PORT`
(default `6632`): the HTTP API under `/api/v1` and `/streams/v1`, `/health`,
`/metrics` and `/metrics/prometheus`, and the dashboard SPA as the fallback route.

Configuration is environment variables; `QUEEN_RAFT_DIR` is the one a single node
needs, and a cluster adds `QUEEN_RAFT_REPLICATOR=openraft`, `QUEEN_RAFT_NODE_ID`,
`QUEEN_RAFT_PEERS` and `QUEEN_RAFT_TOKEN`. Boolean knobs accept `true/false`,
`1/0`, `yes/no`, `on/off`; any other value fails the boot with a message naming the
variable (embedded, it becomes a `StartError::Config` instead of an exit). The full
generated reference is at
[queenmq.com/reference/config](https://queenmq.com/reference/config).

Note for `cargo install` users: the dashboard bundle is a build product of the
repository and the Docker image, not crate content. A broker built from crates.io
serves the API but not a working dashboard. Use the container image if you want
the UI.

## Layout

```
server/
├── Cargo.toml        # package: queen-engine, lib: queen, bin: queen (feature "server")
├── build.rs          # embeds server.json's version as QUEEN_VERSION
├── server.json       # { name, version }: source of truth for version + image tag
└── src/
    ├── main.rs       # the binary: config, router, the embedded proxy and Kafka facade
    ├── lib.rs        # the library: same modules, plus src/embedded/ (queen::Broker)
    ├── embedded/     # the embedded facade: BrokerConfig, Broker, boot
    ├── handlers/     # HTTP handlers, split by route domain; raft.rs holds the router
    ├── rsm/          # the replicated state machine: planner, log, store, apply, raft
    └── ...
```

## Versioning

`server.json` is the single source of truth for the broker version (`/health`,
the boot log, the Docker tag via `build.sh`); `Cargo.toml`'s `version` must say
the same string, since it is what crates.io serves. Bump both together.

License: Apache-2.0. Source and issues: [github.com/queen-mq/queen](https://github.com/queen-mq/queen).
