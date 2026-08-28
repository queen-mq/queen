//! queen-kafka — a Kafka wire-protocol front for QueenMQ.
//!
//! Spec: PLAN_QUEEN_KAFKA.md (repo root). The facade is a separate binary that
//! speaks Kafka to clients and plain HTTP to a Queen broker or proxy as a normal
//! client, so an unmodified Kafka producer or consumer reaches Queen by changing
//! `bootstrap.servers` and nothing else. It holds no durable state.
//!
//! Built so far: M0 (listener, framing, request-header decode, ApiVersions from
//! the static table in [`versions`]) and M1 (Metadata over the queue list, with
//! topic auto-create — [`handlers::metadata`] and [`queen`]).

pub mod conn;
pub mod handlers;
pub mod queen;
pub mod versions;

/// Everything a request handler is allowed to know: the cluster this facade
/// advertises itself as, and the way back to Queen.
///
/// One per process, shared by every connection behind an `Arc`. It is the only
/// state the facade has, and it is derived entirely from configuration and from
/// Queen — nothing durable, per PLAN_QUEEN_KAFKA.md ("no durable state in the
/// facade"), so a restart is a broker restart and clients recover the way they
/// already know how to.
pub struct Facade {
    /// The host and port handed to clients as the address of node 0. Validated
    /// at boot (main.rs): the classic Kafka footgun is advertising something
    /// clients cannot reach, which fails only *after* a successful bootstrap.
    pub advertised_host: String,
    pub advertised_port: u16,
    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS` — the width a topic is advertised at
    /// unless Queen already has more lanes than that. See
    /// [`handlers::metadata`].
    pub default_partitions: u32,
    /// `QUEEN_TOKEN`, the credential this facade reaches Queen with. Optional:
    /// a broker with auth disabled needs none. From M5 a connection may carry
    /// its own instead (SASL/PLAIN → tenant token), which is why every call
    /// takes the token as an argument rather than reading this field.
    pub queen_token: Option<String>,
    /// The queue list, cached briefly, and the auto-create call.
    pub catalog: queen::Catalog,
}

impl Facade {
    /// The credential for a connection that has not presented one of its own.
    /// The single call site of [`Facade::queen_token`], and the seam M5 widens.
    pub fn token(&self) -> Option<&str> {
        self.queen_token.as_deref()
    }
}
