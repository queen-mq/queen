//! queen-kafka — a Kafka wire-protocol front for QueenMQ.
//!
//! Spec: PLAN_QUEEN_KAFKA.md (repo root). The facade is a separate binary that
//! speaks Kafka to clients and plain HTTP to a Queen broker or proxy as a normal
//! client, so an unmodified Kafka producer or consumer reaches Queen by changing
//! `bootstrap.servers` and nothing else. It holds no durable state.
//!
//! Built so far: M0 (listener, framing, request-header decode, ApiVersions from
//! the static table in [`versions`]), M1 (Metadata over the queue list, with
//! topic auto-create — [`handlers::metadata`] and [`queen`]), M2 (Produce:
//! RecordBatch decode, the payload envelope in [`records`], and the batched
//! push — [`handlers::produce`]), M3 (group-less consume: Fetch capped at v6
//! over the batched `POST /api/v1/fetch`, and ListOffsets answering the two
//! watermark sentinels from the same call's bounds — [`handlers::fetch`] and
//! [`handlers::list_offsets`]), M4 (consumer GROUPS: the per-group
//! coordinator actor in [`coordinator`], FindCoordinator answering self, the
//! Join/Sync/Heartbeat/Leave conversation, and committed offsets kept in
//! Queen's key/value store — [`offsets`]) and M5 (the CLOUD fit: an optional
//! TLS listener that captures the client's SNI — [`tls`] — SASL/PLAIN mapping a
//! password to the tenant's bearer token — [`sasl`] — and Queen's 429 answered
//! as the `throttle_time_ms` every Kafka client already backs off on —
//! [`throttle`]).

pub mod cluster;
pub mod conn;
pub mod coordinator;
pub mod decompress;
pub mod handlers;
pub mod idempotent;
pub mod identity;
pub mod obs;
pub mod offsets;
pub mod queen;
pub mod records;
pub mod sasl;
pub mod secret;
pub mod throttle;
pub mod tls;
pub mod topic_config;
pub mod topic_record;
pub mod txn;
pub mod versions;
pub mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Everything a request handler is allowed to know: the cluster this facade
/// advertises itself as, and the way back to Queen.
///
/// ## One per CONNECTION, derived from one per process
///
/// Until M5 there was one of these for the whole process. There still is — the
/// root, built in `main.rs` — but a connection now has things of its own that a
/// handler has to see: the credential SASL/PLAIN presented, and the Host that
/// connection's calls to Queen must carry ([`tls`]). Rather than thread those
/// through every handler signature, a connection gets its own `Facade`
/// ([`Facade::for_connection`], [`Facade::authenticated_as`]) — a few `Arc`
/// clones and one `String`, built once when the connection is accepted. The
/// shared, expensive things stay shared by construction: one `reqwest` pool,
/// one queue-list cache per credential, one group registry.
///
/// It is still the only state the facade has, and it is still derived entirely
/// from configuration and from Queen — nothing durable, per PLAN_QUEEN_KAFKA.md
/// ("no durable state in the facade"), so a restart is a broker restart and
/// clients recover the way they already know how to.
pub struct Facade {
    /// The host and port handed to clients as the address of THIS node — node 0
    /// on its own, `QUEEN_KAFKA_NODE_ID` in a cluster. Validated at boot
    /// (main.rs): the classic Kafka footgun is advertising something clients
    /// cannot reach, which fails only *after* a successful bootstrap.
    pub advertised_host: String,
    pub advertised_port: u16,
    /// Whether this facade is one of several, and if so which one and who the
    /// others are ([`cluster`]).
    ///
    /// [`cluster::Cluster::Single`] — the default, and every deployment that
    /// sets no `QUEEN_KAFKA_NODE_ID` — is not a one-node cluster: it is the
    /// code path this facade has always had, and the Metadata and
    /// FindCoordinator bytes are identical to what they were before this field
    /// existed.
    pub cluster: cluster::Cluster,
    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS` — the width a topic is advertised at
    /// unless Queen already has more lanes than that. See
    /// [`handlers::metadata`].
    pub default_partitions: u32,
    /// The credential every call from THIS connection reaches Queen with: the
    /// token SASL/PLAIN presented, or — on a listener with no SASL —
    /// `QUEEN_TOKEN`, which is optional because a broker with auth disabled
    /// needs none.
    pub queen_token: Option<String>,
    /// Queen itself, for the data path: the batched push behind Produce and the
    /// fetch behind Fetch. The catalog below wraps the SAME object — the split
    /// is between the metadata calls, which are cached and single-flighted
    /// because every client re-asks them on a timer, and the data calls, which
    /// are neither.
    pub queen: Arc<dyn queen::QueenApi>,
    /// The queue list, cached briefly, and the auto-create call. Shared with
    /// every other connection that reaches Queen the same way — see [`Lanes`].
    pub catalog: Arc<queen::Catalog>,
    /// The consumer groups this facade is the coordinator of (M4), as THIS
    /// connection's credential sees them ([`coordinator::Coordinator::scoped`]).
    ///
    /// In cluster mode it is [`cluster::Cluster::owner_of_group`] that makes
    /// this registry globally unique rather than merely process-wide: a group
    /// this node does not own is refused NOT_COORDINATOR before the coordinator
    /// is touched, so no second actor for it is ever spawned here.
    ///
    /// The ONE piece of state that is neither configuration nor derived from
    /// Queen, and it is deliberately the one thing a restart is allowed to
    /// lose: membership is an agreement between clients that a broker
    /// arbitrates, and every Kafka client already knows how to rebuild it when
    /// the arbiter changes its mind. What must NOT be lost — the committed
    /// offsets — is not here; it is in Queen, through [`offsets`].
    pub coordinator: coordinator::Coordinator,
    /// The idempotent producers this facade is enforcing sequence numbers for
    /// (M7 F3), shared by every connection because a producer id is granted on
    /// one connection and used on that one connection while the state has to
    /// outlive the `Facade` clones [`Facade::for_connection`] and
    /// [`Facade::authenticated_as`] make.
    ///
    /// The SECOND piece of state that is neither configuration nor derived from
    /// Queen, and — like the coordinator's registry above — deliberately one a
    /// restart is allowed to lose. The difference, and it is the one worth
    /// saying out loud: a real Kafka broker does NOT lose producer state on a
    /// restart, so losing it here has a cost, and that cost is at-least-once for
    /// at most [`idempotent::WINDOW`] batches of a producer that was mid-stream
    /// ([`idempotent`]).
    pub producers: Arc<idempotent::Producers>,
    /// The transactional producers this facade is holding a STAGE for (M9,
    /// [`txn`]), shared by every connection for three reasons `txn::Txns` names
    /// and one that is visible from here: the InitProducerId fence has to drop
    /// a stage another connection built.
    ///
    /// The THIRD piece of state that is neither configuration nor derived from
    /// Queen, and the only one whose loss a client is told about in words: a
    /// restart loses every open transaction, and the next request naming one is
    /// answered INVALID_TXN_STATE, which is fatal. That is the honest answer
    /// and it is safe in the only direction that matters — nothing partial can
    /// be in the log, because a transaction's records are written by ONE call
    /// and only at commit.
    pub txns: Arc<txn::Txns>,
    /// Queen as each SNI name sees it. Shared by every connection.
    lanes: Arc<Lanes>,
    /// Listener policy, read by [`conn`] and by nothing downstream of it.
    pub policy: Policy,
}

/// What the listener does to a connection before a handler ever sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// `QUEEN_KAFKA_SASL=plain`. When on, a connection may send nothing but
    /// ApiVersions, SaslHandshake and SaslAuthenticate until it has presented a
    /// credential ([`sasl`]).
    pub sasl_plain: bool,
    /// `QUEEN_KAFKA_FORWARD_SNI_HOST=true`. When on, the name a connection
    /// asked for in its TLS SNI becomes the HTTP `Host` header of every call it
    /// makes to Queen — which is what the proxy routes on ([`tls`]).
    pub forward_sni_host: bool,
    /// `QUEEN_KAFKA_MAX_CONNECTIONS` — how many connections this listener
    /// serves at once ([`conn::serve`]). Past it a connection is closed
    /// immediately instead of being served, which is a client that reconnects
    /// rather than a facade that keeps accepting work it cannot do.
    pub max_connections: usize,
}

/// How many connections a listener serves at once when nothing says otherwise.
///
/// A Kafka client holds one connection per broker it talks to, and this facade
/// is one broker: 4096 is a couple of thousand consumers and producers on one
/// process, comfortably past any single Queen deployment and comfortably below
/// the point where the tasks, the read buffers and the file descriptors stop
/// being free.
pub const DEFAULT_MAX_CONNECTIONS: usize = 4_096;

/// Hand-written rather than derived, because a derived `usize` default is zero
/// and a listener that serves zero connections is a listener that is down.
impl Default for Policy {
    fn default() -> Policy {
        Policy {
            sasl_plain: false,
            forward_sni_host: false,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl Facade {
    /// The credential for a connection that has not presented one of its own —
    /// and, on a connection that has, the one it presented.
    pub fn token(&self) -> Option<&str> {
        self.queen_token.as_deref()
    }

    /// This facade as one CONNECTION sees it, before any credential.
    ///
    /// `sni` is the TLS server name the client asked for, if there was one. It
    /// changes nothing unless [`Policy::forward_sni_host`] is on, in which case
    /// it picks the [`Lane`] whose calls carry that `Host`.
    ///
    /// The coordinator is NOT re-scoped here, and there is one configuration
    /// where that is felt: SNI forwarding with SASL off. Every connection then
    /// reaches Queen with the one process credential, so the group scope is the
    /// anonymous one for all of them, while the Host they dialled may route
    /// them to different clusters — two clusters' groups of one name would be
    /// one coordinator. It is not fixed here on purpose: this runs at accept
    /// time, before any I/O, so it cannot resolve an identity, and scoping it
    /// on whatever happened to be resolved by then would let a group's key move
    /// under its own members ([`identity`]). The configuration that reaches it
    /// is a facade fronting several clusters with one shared token and no SASL,
    /// which is not one the Cloud fit describes — there, the credential is the
    /// tenant and [`Facade::authenticated_as`] is where the scope is decided.
    pub fn for_connection(&self, sni: Option<&str>) -> Facade {
        let host = self.policy.forward_sni_host.then_some(sni).flatten();
        let lane = self.lanes.for_host(host);
        Facade {
            advertised_host: self.advertised_host.clone(),
            advertised_port: self.advertised_port,
            cluster: self.cluster.clone(),
            default_partitions: self.default_partitions,
            queen_token: self.queen_token.clone(),
            queen: lane.queen,
            catalog: lane.catalog,
            coordinator: self.coordinator.clone(),
            // The Arc and not a fresh one: a producer's sequence window has to
            // be the same object on every connection of the process, or the
            // window would be lost at the one moment it is needed.
            producers: Arc::clone(&self.producers),
            // The Arc, for the same reason as the window above and one more:
            // the byte cap on staged records is a budget for the PROCESS, so a
            // per-connection copy would be one budget per connection.
            txns: Arc::clone(&self.txns),
            lanes: Arc::clone(&self.lanes),
            policy: self.policy,
        }
    }

    /// The same connection, now carrying the credential it presented.
    ///
    /// The coordinator is re-scoped as well as the token, and that is the whole
    /// of the multi-tenant group fix: two tenants naming the same group get two
    /// groups, and one tenant's two credentials get one
    /// (see [`coordinator::Coordinator::scoped`]).
    ///
    /// The scope is read from the catalog rather than computed here, and it is
    /// read SYNCHRONOUSLY: [`Facade::verify`] has just run for this very token
    /// on this very catalog, and resolving the tenant is part of what it did.
    /// A credential Queen would not identify answers with itself, which is the
    /// same key everything else files it under ([`identity`]).
    pub fn authenticated_as(&self, token: &str) -> Facade {
        Facade {
            advertised_host: self.advertised_host.clone(),
            advertised_port: self.advertised_port,
            cluster: self.cluster.clone(),
            default_partitions: self.default_partitions,
            queen_token: Some(token.to_string()),
            queen: Arc::clone(&self.queen),
            catalog: Arc::clone(&self.catalog),
            coordinator: self
                .coordinator
                .scoped(self.catalog.tenant_key(Some(token))),
            // NOT re-scoped, unlike the coordinator: the tenant is inside the
            // producer window's own key ([`idempotent`]) rather than around the
            // container, because the map is process-wide and its LRU has to be
            // one budget for the whole process.
            producers: Arc::clone(&self.producers),
            // NOT re-scoped either, and for the same reason: the tenant is
            // inside the stage's own key ([`txn`]) rather than around the
            // container.
            txns: Arc::clone(&self.txns),
            lanes: Arc::clone(&self.lanes),
            policy: self.policy,
        }
    }

    /// Is `token` a credential Queen accepts?
    ///
    /// One authenticated call, through the catalog's TTL-bypassing refresh —
    /// see [`handlers::sasl_authenticate`] for why that call and why not the
    /// cached one. `Ok` warms this credential's catalog entry, so the
    /// connection's first Metadata costs nothing.
    ///
    /// It is also where the credential's TENANT is resolved, once
    /// ([`identity`]), which is why [`Facade::authenticated_as`] can read the
    /// scope without awaiting: authentication is the moment a connection's
    /// scope is decided, and every later request only reads it.
    pub async fn verify(&self, token: &str) -> queen::Result<()> {
        self.catalog.refresh(Some(token)).await.map(|_| ())
    }
}

/// Queen as ONE `Host` header sees it: the client that stamps it on every call,
/// and the queue-list cache in front of that client.
#[derive(Clone)]
struct Lane {
    queen: Arc<dyn queen::QueenApi>,
    catalog: Arc<queen::Catalog>,
}

/// The lanes, made on demand and shared for the life of the process.
///
/// With `QUEEN_KAFKA_FORWARD_SNI_HOST` off — the default, and every OSS
/// deployment — there is exactly one, and `for_host` never allocates. With it
/// on there is one per SNI name, which is one per tenant hostname, and they
/// share the `reqwest` connection pool because
/// [`queen::QueenApi::with_host`] clones the client rather than building one.
///
/// ## Bounded, and REPLACEABLE
///
/// The SNI is a string a client chooses, and `Conn::new` reaches this map at
/// accept time — after the TLS handshake and before a single Kafka frame, so
/// before any credential. An unbounded map is therefore an unbounded
/// allocation any anonymous peer can drive, which is what [`MAX_LANES`] is
/// for; but a cap that only refuses is worse than it looks, because the names
/// that fill it are the attacker's and the names refused after it are the real
/// tenants'. A thousand junk handshakes would have permanently demoted every
/// genuine hostname to the root lane — served, but no longer routed by name —
/// for the life of the process.
///
/// So the cap EVICTS, least recently used first. A lane is a `reqwest` view and
/// a queue-list cache, nothing durable and nothing a client can miss: evicting
/// one costs its next connection a catalog fetch, and a tenant's own traffic
/// keeps its lane in front of any name that is not being used.
struct Lanes {
    root: Lane,
    by_host: Mutex<HashMap<String, Held>>,
    /// Ticks once per lookup. The ordering `by_host` is evicted in.
    clock: std::sync::atomic::AtomicU64,
}

/// One lane and when it was last asked for.
struct Held {
    lane: Lane,
    used: u64,
}

/// How many distinct SNI names get a lane of their own.
///
/// One per tenant hostname on one facade. A thousand is far past any real
/// deployment (a cell fronts tens of clusters) and far below what a hostile
/// client could spend memory with.
const MAX_LANES: usize = 1_024;

/// One line per window when the lane cap is evicting, not one per connection —
/// the cap is exactly the situation a flood produces. See [`obs::Sampler`].
static LANE_CAP: obs::Sampler = obs::Sampler::new(60_000);

impl Lanes {
    fn new(queen: Arc<dyn queen::QueenApi>, catalog: Arc<queen::Catalog>) -> Lanes {
        Lanes {
            root: Lane { queen, catalog },
            by_host: Mutex::new(HashMap::new()),
            clock: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The lane for `host`, made on first use. The lock is a `std::sync::Mutex`
    /// and nothing is awaited while it is held.
    fn for_host(&self, host: Option<&str>) -> Lane {
        let Some(host) = host.and_then(canonical_host) else {
            return self.root.clone();
        };
        let now = self
            .clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut lanes = self
            .by_host
            .lock()
            .expect("the lane map lock is never held across a panic");
        if let Some(held) = lanes.get_mut(&host) {
            held.used = now;
            return held.lane.clone();
        }
        // A double that has no HTTP to stamp says so, and keeps the root lane.
        // Asked before anything is evicted, so a map that cannot be added to is
        // not emptied by trying.
        let Some(queen) = self.root.queen.with_host(&host) else {
            return self.root.clone();
        };
        while lanes.len() >= MAX_LANES {
            let Some(coldest) = lanes
                .iter()
                .min_by_key(|(_, held)| held.used)
                .map(|(name, _)| name.clone())
            else {
                break;
            };
            lanes.remove(&coldest);
            if let Some(suppressed) = LANE_CAP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    lanes = MAX_LANES,
                    suppressed,
                    "more distinct TLS server names than QUEEN_KAFKA_FORWARD_SNI_HOST keeps \
                     lanes for; the least recently used are being dropped and will be rebuilt \
                     on their next connection"
                );
            }
        }
        let lane = Lane {
            catalog: Arc::new(queen::Catalog::new(Arc::clone(&queen))),
            queen,
        };
        lanes.insert(
            host,
            Held {
                lane: lane.clone(),
                used: now,
            },
        );
        lane
    }
}

/// A TLS server name reduced to what may go in a `Host` header, or `None` when
/// it may not go in one at all.
///
/// This value came off the wire, and its destination is an HTTP header — so it
/// is validated here rather than trusted. rustls already rejects a server name
/// that is not a plausible DNS name during the handshake, and this does not
/// rely on that: a name carrying anything outside `[a-z0-9.-]` is dropped,
/// which makes header injection (a CR, an LF, a space, a colon) impossible by
/// construction rather than by someone else's promise.
///
/// The normalisation matches the proxy's own `canonical_host`
/// (proxy/src/config.rs): lowercased, and the DNS root label — the trailing dot
/// in `try.queenmq.cloud.` — stripped, because the proxy compares against a
/// list that has no trailing dots and a name that keeps one misses every entry.
fn canonical_host(sni: &str) -> Option<String> {
    let host = sni.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 {
        return None;
    }
    host.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        .then_some(host)
}

impl Facade {
    /// The process-wide facade — the root every connection's view is derived
    /// from ([`Facade::for_connection`]).
    ///
    /// The one constructor, because two of these fields have to agree with each
    /// other: the root lane's client is the same object as `queen`, and its
    /// catalog is the same object as `catalog`. A caller that built them apart
    /// would give a connection with no SNI a different cache from the process's.
    // Nine positional arguments, and they are nine because a `Facade` is nine
    // independent things a boot decides — a parameter struct would move the
    // same list one file away and stop the compiler from telling a caller which
    // one it forgot.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        advertised_host: String,
        advertised_port: u16,
        cluster: cluster::Cluster,
        default_partitions: u32,
        queen_token: Option<String>,
        queen: Arc<dyn queen::QueenApi>,
        coordinator: coordinator::Coordinator,
        txns: Arc<txn::Txns>,
        policy: Policy,
    ) -> Facade {
        let catalog = Arc::new(queen::Catalog::new(Arc::clone(&queen)));
        Facade {
            advertised_host,
            advertised_port,
            cluster,
            default_partitions,
            queen_token,
            lanes: Arc::new(Lanes::new(Arc::clone(&queen), Arc::clone(&catalog))),
            queen,
            catalog,
            coordinator,
            // Built here rather than passed in, because there is exactly one
            // right answer (an empty tracker) and a caller that could choose
            // would be a caller that could give two connections two windows.
            producers: Arc::new(idempotent::Producers::new()),
            // PASSED, unlike the producer window above, because this one
            // carries the caps of §5.2 and those are configuration: a caller
            // that could not choose them would be a facade whose memory bound
            // is a constant an operator cannot move.
            txns,
            policy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_server_name_is_reduced_to_what_a_host_header_may_carry() {
        assert_eq!(
            canonical_host("Try.QueenMQ.Cloud"),
            Some("try.queenmq.cloud".to_string())
        );
        // The fully-qualified form, which is legal in SNI and misses every
        // entry of the proxy's shared-host list if the dot survives.
        assert_eq!(
            canonical_host("try.queenmq.cloud."),
            Some("try.queenmq.cloud".to_string())
        );
        assert_eq!(
            canonical_host("  acme-1.eu  "),
            Some("acme-1.eu".to_string())
        );
    }

    /// The value is a client's, and it ends up in an HTTP header. Everything
    /// that could split a header, add one, or address something else is
    /// dropped rather than escaped.
    #[test]
    fn a_server_name_that_could_forge_a_header_is_dropped() {
        for hostile in [
            "acme.test\r\nX-Queen-Tenant: someone-else",
            "acme.test\nHost: elsewhere",
            "acme.test:6711",
            "acme test",
            "acme_test.internal",
            "acme.test/../admin",
            "üñïçø∂é.test",
            "",
            "   ",
            ".",
            &"a".repeat(254),
        ] {
            assert_eq!(canonical_host(hostile), None, "{hostile:?} was accepted");
        }
    }

    fn lanes() -> (Lanes, Arc<queen::testing::FakeQueen>) {
        let api = queen::testing::FakeQueen::with(&[("orders", 1)]);
        let routed = queen::testing::Routed::over(Arc::clone(&api)) as Arc<dyn queen::QueenApi>;
        let catalog = Arc::new(queen::Catalog::new(Arc::clone(&routed)));
        (Lanes::new(routed, catalog), api)
    }

    /// The lane map is keyed by the CANONICAL name, so two spellings of one
    /// host are one lane — and therefore one queue-list cache, which is the
    /// thing that would otherwise be duplicated per spelling.
    #[tokio::test]
    async fn one_host_is_one_lane_however_it_is_spelled() {
        let (lanes, api) = lanes();
        for spelling in ["acme.test", "ACME.Test", "acme.test."] {
            lanes
                .for_host(Some(spelling))
                .queen
                .list_queues(None)
                .await
                .unwrap();
        }
        assert_eq!(lanes.by_host.lock().unwrap().len(), 1);
        assert_eq!(api.hosts(), ["acme.test", "acme.test", "acme.test"]);
    }

    /// A connection with no name, and one on a listener that is not forwarding,
    /// both reach Queen exactly as the process does: nothing is stamped.
    #[tokio::test]
    async fn the_root_lane_stamps_nothing() {
        let (lanes, api) = lanes();
        lanes.for_host(None).queen.list_queues(None).await.unwrap();
        // ...and a name that may not go in a header falls back to it rather
        // than being escaped into one.
        lanes
            .for_host(Some("acme.test\r\nX-Queen-Tenant: someone-else"))
            .queen
            .list_queues(None)
            .await
            .unwrap();
        assert!(api.hosts().is_empty());
        assert!(lanes.by_host.lock().unwrap().is_empty());
    }

    /// The SNI is a string a client chooses, so the map it fills is bounded.
    #[tokio::test]
    async fn the_lane_map_is_bounded() {
        let (lanes, _) = lanes();
        for i in 0..MAX_LANES + 10 {
            lanes.for_host(Some(&format!("t{i}.queenmq.cloud")));
        }
        assert_eq!(lanes.by_host.lock().unwrap().len(), MAX_LANES);
    }

    /// ...and the cap is reached BEFORE any credential — `Conn::new` reads the
    /// SNI at accept time — so what happens at the cap decides whether a
    /// hostile handshake can take a real tenant's routing away from it. It
    /// cannot: the lane that goes is the one nobody has used, a tenant that is
    /// using its lane keeps it, and a lane that was dropped comes back on the
    /// next connection that names it.
    #[tokio::test]
    async fn a_flood_of_server_names_cannot_take_a_tenants_lane() {
        let (lanes, api) = lanes();
        // A real tenant, connected and doing work.
        lanes
            .for_host(Some("acme.queenmq.cloud"))
            .queen
            .list_queues(None)
            .await
            .unwrap();

        for i in 0..MAX_LANES * 2 {
            // The tenant keeps connecting throughout, which is what keeps its
            // lane in front of names nobody is using.
            if i % 8 == 0 {
                lanes.for_host(Some("acme.queenmq.cloud"));
            }
            lanes.for_host(Some(&format!("junk-{i}.example")));
        }

        assert_eq!(lanes.by_host.lock().unwrap().len(), MAX_LANES);
        assert!(
            lanes
                .by_host
                .lock()
                .unwrap()
                .contains_key("acme.queenmq.cloud"),
            "the flood evicted the lane of a tenant that was using it"
        );
        // ...and it is still the routed lane, not the root one.
        lanes
            .for_host(Some("acme.queenmq.cloud"))
            .queen
            .list_queues(None)
            .await
            .unwrap();
        assert_eq!(api.hosts(), ["acme.queenmq.cloud", "acme.queenmq.cloud"]);

        // A name that WAS evicted is not refused for ever: it is rebuilt, and
        // it routes.
        lanes
            .for_host(Some("junk-0.example"))
            .queen
            .list_queues(None)
            .await
            .unwrap();
        assert_eq!(api.hosts().len(), 3);
        assert_eq!(api.hosts()[2], "junk-0.example");
    }

    /// The knob, at the level a connection sees it: off, the name changes
    /// nothing; on, the connection's calls carry it.
    #[tokio::test]
    async fn forwarding_is_what_turns_a_server_name_into_a_host() {
        let api = queen::testing::FakeQueen::with(&[("orders", 1)]);
        let routed = queen::testing::Routed::over(Arc::clone(&api)) as Arc<dyn queen::QueenApi>;

        let off = handlers::testing::over(Arc::clone(&routed), Policy::default());
        off.for_connection(Some("acme.test"))
            .catalog
            .list(None)
            .await
            .unwrap();
        assert!(api.hosts().is_empty());

        let on = handlers::testing::over(
            routed,
            Policy {
                forward_sni_host: true,
                ..Default::default()
            },
        );
        on.for_connection(Some("acme.test"))
            .catalog
            .list(None)
            .await
            .unwrap();
        assert_eq!(api.hosts(), ["acme.test"]);
    }

    // ------------------------------------------------- the scope of a group

    /// One connection, authenticated the way the SASL path authenticates one:
    /// the credential is checked against Queen, which is also where its tenant
    /// is resolved ([`Facade::verify`]).
    async fn authenticated(root: &Facade, token: &str) -> Facade {
        let connection = root.for_connection(None);
        connection
            .verify(token)
            .await
            .expect("the fixture's Queen accepts every credential");
        connection.authenticated_as(token)
    }

    /// A client's whole first join: the KIP-394 round trip, then the real one.
    async fn join(f: &Facade, group: &str, client: &str) -> coordinator::JoinAnswer {
        let mut req = coordinator::JoinRequest {
            member_id: String::new(),
            client_id: client.to_string(),
            client_host: "/127.0.0.1".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![coordinator::Protocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"subscription"),
            }],
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 60_000,
            member_id_required: true,
        };
        let minted = f.coordinator.join(group, req.clone()).await;
        req.member_id = minted.member_id;
        f.coordinator.join(group, req).await
    }

    /// How long the join window is held open in the test below. The handler
    /// fixture uses a zero window, which is right for the questions it asks
    /// (does this request become that answer) and wrong for this one: two
    /// consumers land in ONE generation only if both are inside the window,
    /// which is exactly what a fleet starting together does.
    const JOIN_WINDOW: Duration = Duration::from_millis(3_000);

    fn facade_with_a_join_window(api: Arc<dyn queen::QueenApi>) -> Facade {
        Facade::new(
            "kafka.example.com".into(),
            9092,
            cluster::Cluster::Single,
            4,
            None,
            api,
            coordinator::Coordinator::new(coordinator::GroupConfig {
                join_delay: JOIN_WINDOW,
                ..Default::default()
            }),
            Arc::new(txn::Txns::default()),
            Policy::default(),
        )
    }

    fn spawn_join(
        f: Facade,
        group: &str,
        client: &str,
    ) -> tokio::task::JoinHandle<coordinator::JoinAnswer> {
        let (group, client) = (group.to_string(), client.to_string());
        tokio::spawn(async move { join(&f, &group, &client).await })
    }

    /// THE fix: one tenant with two credentials — a key rotation, a
    /// per-service key — is ONE coordinator.
    ///
    /// Keyed by the credential, these two consumers were the sole member of
    /// two groups, each assigned everything by its own leader, both writing
    /// the same offset keys (Queen scopes those by tenant) with a generation
    /// the other could not see. Keyed by the tenant Queen names for each of
    /// them, they are one group: one generation, one leader, and the leader is
    /// handed both members.
    #[tokio::test(start_paused = true)]
    async fn a_tenants_two_credentials_share_one_coordinator() {
        let api = queen::testing::FakeQueen::with(&[("orders", 2)]);
        // What a Queen that identifies bearers answers: both keys act on one
        // cluster, and a cluster is one broker tenant is one offset namespace.
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-1");
        let root = facade_with_a_join_window(Arc::clone(&api) as Arc<dyn queen::QueenApi>);

        let a = authenticated(&root, "key-a").await;
        let b = authenticated(&root, "key-b").await;
        let first = spawn_join(a, "orders-consumer", "consumer-a");
        let second = spawn_join(b, "orders-consumer", "consumer-b");

        // Both are inside the window before it closes.
        for _ in 0..1_000 {
            if root
                .coordinator
                .describe("orders-consumer")
                .await
                .is_some_and(|s| s.members.len() == 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            root.coordinator.live_groups(),
            1,
            "one tenant's two credentials are two group actors"
        );
        tokio::time::advance(JOIN_WINDOW + Duration::from_millis(1)).await;

        let (first, second) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(first.error, None);
        assert_eq!(second.error, None);
        assert_eq!(first.generation, second.generation, "two generations");
        assert_eq!(first.leader, second.leader, "two leaders");
        assert_ne!(first.member_id, second.member_id);
        // One of them leads, and the leader is handed EVERY member of the
        // group — which is the whole of what a shared coordinator means.
        let leader = [&first, &second]
            .into_iter()
            .find(|a| a.member_id == a.leader)
            .expect("neither credential's member leads the group they share");
        assert_eq!(leader.members.len(), 2);
        assert_eq!(root.coordinator.live_groups(), 1);
    }

    /// ...and the direction the scope already fixed is not lost with it: two
    /// TENANTS that pick the same group name — `orders-consumer` is a name two
    /// strangers pick — stay two groups.
    #[tokio::test]
    async fn two_tenants_that_pick_one_group_id_stay_apart() {
        let (root, api) = handlers::testing::facade_and_queen(&[("orders", 2)]);
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-2");

        let a = authenticated(&root, "key-a").await;
        let b = authenticated(&root, "key-b").await;
        let first = join(&a, "orders-consumer", "consumer-a").await;
        let second = join(&b, "orders-consumer", "consumer-b").await;

        assert_eq!(root.coordinator.live_groups(), 2);
        let seen_by_a = a.coordinator.describe("orders-consumer").await.unwrap();
        let seen_by_b = b.coordinator.describe("orders-consumer").await.unwrap();
        assert_eq!(seen_by_a.members, vec![first.member_id]);
        assert_eq!(seen_by_b.members, vec![second.member_id]);
        assert_eq!(first.generation, 1, "one tenant's join rebalanced another");
        assert_eq!(second.generation, 1);
    }

    /// A Queen that identifies nobody — every deployment's today, see
    /// [`identity`] — leaves each credential filed under itself, which is the
    /// behaviour this facade had before the tenant was ever asked for.
    #[tokio::test]
    async fn an_unidentified_credential_still_stands_for_itself() {
        let (root, _) = handlers::testing::facade_and_queen(&[("orders", 2)]);
        let a = authenticated(&root, "key-a").await;
        let b = authenticated(&root, "key-b").await;
        join(&a, "orders-consumer", "consumer-a").await;
        join(&b, "orders-consumer", "consumer-b").await;

        assert_eq!(
            root.coordinator.live_groups(),
            2,
            "two credentials nobody could identify were pooled into one group"
        );
    }

    /// A connection's credential replaces the process's for every call it makes
    /// — the whole point of SASL — and its consumer groups move with it.
    #[tokio::test]
    async fn a_credential_replaces_the_process_one_for_that_connection() {
        let (facade, api) = handlers::testing::facade_and_queen(&[("orders", 1)]);
        assert_eq!(facade.token(), None, "the fixture has no QUEEN_TOKEN");

        let connection = facade.for_connection(None).authenticated_as("tenant-token");
        assert_eq!(connection.token(), Some("tenant-token"));
        connection.catalog.list(connection.token()).await.unwrap();
        assert_eq!(
            api.tokens.lock().unwrap().as_slice(),
            [Some("tenant-token".to_string())]
        );
    }
}
