//! The Queen side of the facade: an HTTP client for the broker's admin surface,
//! plus the short-lived cache the Kafka metadata path reads through.
//!
//! `queen-kafka` is a *client* of Queen (PLAN_QUEEN_KAFKA.md: "speaks plain HTTP
//! to broker/proxy as a normal client"), so the obvious move is to depend on
//! `clients/client-rust`. It does not fit, for one reason that is structural
//! rather than cosmetic: that client binds its bearer token once, into the
//! `reqwest` default headers built in `Queen::new` (clients/client-rust/src/
//! http.rs), and there is no per-request override. M5 forwards a *per-connection*
//! credential — SASL/PLAIN maps a username/password to the tenant's token — so a
//! token that belongs to the client object would mean one client object per
//! Kafka connection: a fresh connection pool, TLS session cache and load
//! balancer per consumer. The token is therefore an argument of every call here,
//! and the caller (M1: `QUEEN_TOKEN`, M5: the connection's own) decides it. The
//! admin surface would not have carried its weight either — it returns
//! `serde_json::Value` for these routes, which is the same parsing this module
//! does, minus the queue/kv/timer/streams/buffer modules that come with it.
//!
//! Routes used, and their shapes, read off server/src/main.rs and
//! server/src/handlers/queues.rs (never guessed):
//!
//!   * `GET /api/v1/resources/queues` → `queen.get_queues_v2` enriched with the
//!     live per-queue counts, i.e. `{"queues":[{"name":…,"partitions":N,…}],…}`
//!     (server/sql/procedures/018_stats.sql). `partitions` is the number of
//!     `queen.log_partitions` rows the queue has — see [`Queue::partitions`].
//!   * `POST /api/v1/configure` `{"queue":"<name>"}` → `queen.configure_queue_v1`
//!     (server/sql/procedures/012_configure.sql), which upserts the
//!     `queen.queues` row and answers `{"configured":true,…}`. An empty options
//!     bag is deliberate: it takes the SP's own defaults, and those leave
//!     retention OFF (`retentionEnabled` false, `retentionSeconds` 0), so a
//!     topic auto-created for a Kafka producer cannot quietly start expiring the
//!     records it is handed.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// A boxed future, so [`QueenApi`] stays dyn-compatible. `async fn` in a trait
/// is not: it desugars to an opaque associated type, which no trait object can
/// name. The facade holds `Arc<dyn QueenApi>` precisely so the tests can hand it
/// a double instead of a socket.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, Error>;

/// Why a call to Queen did not produce an answer. Kept coarse on purpose: every
/// variant maps to the same retriable Kafka error at the metadata boundary, and
/// the distinction exists for the log line, not for control flow.
#[derive(Debug)]
pub enum Error {
    /// The request never completed: DNS, connect, TLS, timeout, reset.
    Transport(String),
    /// Queen answered, with a status that is not a success.
    Status { code: u16, body: String },
    /// Queen answered 2xx with a body this client cannot read.
    Body(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "transport: {e}"),
            Error::Status { code, body } => write!(f, "HTTP {code}: {}", Snippet(body)),
            Error::Body(e) => write!(f, "body: {e}"),
        }
    }
}

impl std::error::Error for Error {}

/// A response body in a log line, clamped. Queen error bodies are small JSON,
/// but a misrouted request can land on anything (an ingress error page, a
/// dashboard bundle), and that must not become the log.
struct Snippet<'a>(&'a str);

impl std::fmt::Display for Snippet<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX: usize = 200;
        let s = self.0.trim();
        match s.char_indices().nth(MAX) {
            Some((cut, _)) => write!(f, "{}…", &s[..cut]),
            None => write!(f, "{s}"),
        }
    }
}

/// One queue as the admin list reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Queue {
    pub name: String,
    /// Live count of `queen.log_partitions` rows for this queue.
    ///
    /// It is NOT a declared width the way a Kafka partition count is: Queen has
    /// no such declaration. `/configure` creates the queue row and nothing else
    /// ("the log engine creates partitions lazily on the first push",
    /// 012_configure.sql), so a queue that has never been pushed to reports 0
    /// and a queue pushed to on lanes 0 and 7 reports 2. Turning this into the
    /// number Kafka needs is [`crate::handlers::metadata`]'s job, not this
    /// module's.
    #[serde(default)]
    pub partitions: i64,
}

/// The two admin calls M1 needs. A trait, and not just the concrete client
/// below, so the auto-create policy can be tested against a double that counts
/// its calls rather than against a broker.
pub trait QueenApi: Send + Sync + 'static {
    /// `GET /api/v1/resources/queues`, in the queue list's own order.
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>>;

    /// `POST /api/v1/configure` — create the queue if it is not there, leave it
    /// exactly as it is if it is (the SP is an upsert of the config columns, and
    /// an empty options bag rewrites them to the SP defaults, so this is only
    /// ever called for a name the catalog does not know).
    fn create_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>>;
}

// --------------------------------------------------------------------- client

/// Total budget for one admin call. The Kafka connection that triggered it is
/// muted until it answers (conn.rs: one request in flight), so an unbounded wait
/// here is a hung consumer with no error to show for it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The real client. One `reqwest::Client` for the process: it owns the
/// connection pool, and the facade's whole traffic to Queen is a handful of
/// admin calls per metadata refresh.
pub struct HttpQueen {
    /// `QUEEN_URL`, without a trailing slash (see [`normalize_base_url`]).
    base: String,
    http: reqwest::Client,
}

impl HttpQueen {
    pub fn new(base_url: &str) -> std::result::Result<HttpQueen, String> {
        let base = normalize_base_url(base_url)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client for QUEEN_URL={base}: {e}"))?;
        Ok(HttpQueen { base, http })
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        // Bearer, matching what the broker's auth layer extracts
        // (server/src/auth.rs::extract_bearer). Per call, never on the client:
        // see the module header.
        match token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    async fn send(req: reqwest::RequestBuilder) -> Result<String> {
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(Error::Status {
                code: status.as_u16(),
                body,
            });
        }
        Ok(body)
    }
}

/// The list body. Only the fields this facade reads are named; the broker adds
/// to this response for the dashboard (retainedBytes, messages, the top-level
/// kv/timer byte counters) and none of that may turn into a parse failure here.
#[derive(Debug, Deserialize)]
struct QueueListBody {
    #[serde(default)]
    queues: Vec<Queue>,
}

impl QueenApi for HttpQueen {
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
        Box::pin(async move {
            // Not `?stats=cached`: that serves `queen.stats.child_count` as of
            // the last stats refresh, which for a queue created seconds ago (the
            // auto-create path, every time) is a partition count from before the
            // queue existed. The enriched form costs a pass over the tenant's
            // partitions, which is what the TTL below is for.
            let body =
                Self::send(self.request(reqwest::Method::GET, "/api/v1/resources/queues", token))
                    .await?;
            let parsed: QueueListBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            Ok(parsed.queues)
        })
    }

    fn create_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let payload = serde_json::json!({ "queue": name }).to_string();
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/configure", token)
                    .body(payload),
            )
            .await?;
            // handle_configure surfaces a stored-procedure failure as a non-2xx,
            // but the SP can also echo `{"error":…}` inside a 200 — the handler
            // only re-maps it when it parses (server/src/handlers/queues.rs,
            // "RUSTFIX item 25"). Check for it rather than reporting a queue we
            // did not create.
            let parsed: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            if let Some(e) = parsed.get("error").filter(|e| !e.is_null()) {
                return Err(Error::Body(format!("configure answered {e}")));
            }
            if parsed.get("configured").and_then(|c| c.as_bool()) != Some(true) {
                return Err(Error::Body(format!(
                    "configure did not confirm: {}",
                    Snippet(&body)
                )));
            }
            Ok(())
        })
    }
}

/// Reject a `QUEEN_URL` at boot instead of on the first metadata refresh, and
/// strip the trailing slash so paths concatenate to exactly one.
pub fn normalize_base_url(raw: &str) -> std::result::Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let url: reqwest::Url = trimmed
        .parse()
        .map_err(|e| format!("QUEEN_URL={raw} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "QUEEN_URL={raw} has scheme `{}` — the facade speaks HTTP to Queen, so it must be \
             http:// or https://",
            url.scheme()
        ));
    }
    if url.host_str().is_none() {
        return Err(format!("QUEEN_URL={raw} has no host"));
    }
    Ok(trimmed.to_string())
}

// -------------------------------------------------------------------- catalog

/// How long a queue list is served without asking Queen again.
///
/// Every Kafka client refreshes metadata on its own timer (`metadata.max.age.ms`
/// defaults to 5 minutes) and *immediately* on any retriable error, which is how
/// a hiccup turns into every consumer in a fleet asking at the same instant. The
/// window only has to be long enough to collapse that burst into one admin call;
/// past a few seconds it starts hiding real topic creations from clients that
/// just made them. Not a knob: PLAN_QUEEN_KAFKA.md's config surface is the
/// operator's, and this number has no operator-visible effect worth tuning.
const LIST_TTL: Duration = Duration::from_secs(3);

/// The queue list, cached briefly, with single-flight refresh.
pub struct Catalog {
    api: Arc<dyn QueenApi>,
    ttl: Duration,
    /// Keyed by credential. M1 has exactly one (`QUEEN_TOKEN`), but M5 hands
    /// every connection its own tenant token, and a catalog shared across them
    /// would show one tenant another's topics. Keying it now costs a hash and
    /// makes that impossible rather than merely unintended.
    ///
    /// A `tokio` mutex, held across the fetch: that is what makes the refresh
    /// single-flight, so a metadata storm is one admin call and not one per
    /// connection.
    entries: tokio::sync::Mutex<HashMap<Option<String>, Entry>>,
}

/// The last list we got, and when. Kept even after it goes stale: see
/// [`Catalog::list`].
struct Entry {
    queues: Arc<Vec<Queue>>,
    fetched_at: Instant,
}

impl Catalog {
    pub fn new(api: Arc<dyn QueenApi>) -> Catalog {
        Catalog::with_ttl(api, LIST_TTL)
    }

    /// Same, with an explicit TTL. For tests, which cannot wait three seconds to
    /// prove that the cache expires.
    pub fn with_ttl(api: Arc<dyn QueenApi>, ttl: Duration) -> Catalog {
        Catalog {
            api,
            ttl,
            entries: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The queue list for `token`, from cache when it is fresh.
    ///
    /// On a refresh failure with a previous list in hand, that list is served
    /// stale and the error is logged. A Kafka client reacts to "topic unknown"
    /// by dropping the topic from its subscription or by re-resolving the
    /// leader, and doing that because the admin API blipped for one second is a
    /// far worse answer than a topic list three seconds old. With nothing
    /// cached, the error is returned and the caller decides the Kafka error code.
    pub async fn list(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        let key = token.map(|t| t.to_string());
        let mut entries = self.entries.lock().await;
        if let Some(e) = entries.get(&key) {
            if e.fetched_at.elapsed() < self.ttl {
                return Ok(Arc::clone(&e.queues));
            }
        }
        self.fetch(&mut entries, key, token, true).await
    }

    /// The queue list as of now, ignoring the TTL — and reporting a failure
    /// rather than falling back to the stale copy.
    ///
    /// The one caller is the auto-create path, and the reason is that
    /// `configure_queue_v1` is an UPSERT: called for a queue that already
    /// exists, it rewrites every config column to the stored procedure's
    /// defaults, so an auto-create decided from a three-second-old list could
    /// silently reset a native queue's leaseTime, retention and dedup window.
    /// There is no create-if-absent on the broker to ask for instead, so the
    /// window is closed as far as it can be: down from the TTL to the length of
    /// one admin call.
    pub async fn refresh(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        let key = token.map(|t| t.to_string());
        let mut entries = self.entries.lock().await;
        self.fetch(&mut entries, key, token, false).await
    }

    /// Ask Queen and store the answer. `fall_back_to_stale` decides what a
    /// failure means: for [`Catalog::list`] a slightly old world, for
    /// [`Catalog::refresh`] an error.
    async fn fetch(
        &self,
        entries: &mut HashMap<Option<String>, Entry>,
        key: Option<String>,
        token: Option<&str>,
        fall_back_to_stale: bool,
    ) -> Result<Arc<Vec<Queue>>> {
        match self.api.list_queues(token).await {
            Ok(queues) => {
                let queues = Arc::new(queues);
                entries.insert(
                    key,
                    Entry {
                        queues: Arc::clone(&queues),
                        fetched_at: Instant::now(),
                    },
                );
                Ok(queues)
            }
            Err(e) => match entries.get(&key).filter(|_| fall_back_to_stale) {
                Some(stale) => {
                    tracing::warn!(
                        target: "kafka",
                        error = %e,
                        age_ms = stale.fetched_at.elapsed().as_millis() as u64,
                        "queue list refresh failed; serving the last one"
                    );
                    Ok(Arc::clone(&stale.queues))
                }
                None => Err(e),
            },
        }
    }

    /// Create a queue and drop the cached list, so the next [`Catalog::list`]
    /// sees it. The caller does not wait for that refresh: it already knows the
    /// name it just created.
    pub async fn create(&self, name: &str, token: Option<&str>) -> Result<()> {
        self.api.create_queue(name, token).await?;
        self.entries
            .lock()
            .await
            .remove(&token.map(|t| t.to_string()));
        Ok(())
    }
}

/// The test double, shared with `handlers::metadata`'s tests: the auto-create
/// policy is decided there and executed here, and both halves are worth testing
/// against something that records what it was asked rather than a broker.
#[cfg(test)]
pub mod testing {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// A [`QueenApi`] that answers from a script and counts what it was asked.
    pub struct FakeQueen {
        pub queues: Mutex<Vec<Queue>>,
        pub lists: AtomicUsize,
        pub creates: Mutex<Vec<String>>,
        pub tokens: Mutex<Vec<Option<String>>>,
        /// When set, every call fails with it.
        pub fail: Mutex<Option<String>>,
    }

    impl FakeQueen {
        pub fn with(queues: &[(&str, i64)]) -> Arc<FakeQueen> {
            Arc::new(FakeQueen {
                queues: Mutex::new(
                    queues
                        .iter()
                        .map(|(n, p)| Queue {
                            name: n.to_string(),
                            partitions: *p,
                        })
                        .collect(),
                ),
                lists: AtomicUsize::new(0),
                creates: Mutex::new(Vec::new()),
                tokens: Mutex::new(Vec::new()),
                fail: Mutex::new(None),
            })
        }

        pub fn list_count(&self) -> usize {
            self.lists.load(Ordering::SeqCst)
        }

        pub fn created(&self) -> Vec<String> {
            self.creates.lock().unwrap().clone()
        }

        pub fn fail_with(&self, why: &str) {
            *self.fail.lock().unwrap() = Some(why.to_string());
        }
    }

    impl QueenApi for FakeQueen {
        fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
            Box::pin(async move {
                self.lists.fetch_add(1, Ordering::SeqCst);
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                match self.fail.lock().unwrap().clone() {
                    Some(e) => Err(Error::Transport(e)),
                    None => Ok(self.queues.lock().unwrap().clone()),
                }
            })
        }

        fn create_queue<'a>(
            &'a self,
            name: &'a str,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.fail.lock().unwrap().clone() {
                    return Err(Error::Transport(e));
                }
                self.creates.lock().unwrap().push(name.to_string());
                self.queues.lock().unwrap().push(Queue {
                    name: name.to_string(),
                    partitions: 0,
                });
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeQueen;
    use super::*;
    use std::sync::atomic::Ordering;

    // ------------------------------------------------------------ parsing

    /// The exact body `GET /api/v1/resources/queues` serves, trimmed of nothing:
    /// get_queues_v2's per-queue object plus the live `partitions`/`segments`
    /// enrichment and the top-level kv/timer byte counters
    /// (server/src/handlers/queues.rs). Unknown fields must be ignored, not
    /// rejected, or a dashboard-driven addition to the response breaks the
    /// facade.
    const REAL_LIST_BODY: &str = r#"{
      "queues": [
        {"id":"0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11","name":"orders","namespace":"","task":"",
         "createdAt":"2026-08-27T10:00:00.000Z","partitions":12,"retainedBytes":4096,
         "segments":{"segments":3,"messages":900},
         "messages":{"total":900,"pending":10,"processing":2}},
        {"id":"1d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a12","name":"clicks","namespace":"ns","task":"t",
         "createdAt":"2026-08-27T10:00:01.000Z","partitions":0,"retainedBytes":0,
         "messages":{"total":0,"pending":0,"processing":0}}
      ],
      "kvBytes": 0, "timerBytes": 0
    }"#;

    #[test]
    fn the_queue_list_body_parses_to_names_and_partition_counts() {
        let parsed: QueueListBody = serde_json::from_str(REAL_LIST_BODY).unwrap();
        assert_eq!(
            parsed.queues,
            vec![
                Queue {
                    name: "orders".into(),
                    partitions: 12
                },
                Queue {
                    name: "clicks".into(),
                    partitions: 0
                },
            ]
        );
    }

    #[test]
    fn an_empty_broker_parses_to_an_empty_list() {
        let parsed: QueueListBody = serde_json::from_str(r#"{"queues":[]}"#).unwrap();
        assert!(parsed.queues.is_empty());
    }

    // -------------------------------------------------------------- base url

    #[test]
    fn the_base_url_is_checked_at_boot_and_loses_its_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://queen-mq-v1:6632/").unwrap(),
            "http://queen-mq-v1:6632"
        );
        assert_eq!(
            normalize_base_url("  https://cloud.queenmq.com  ").unwrap(),
            "https://cloud.queenmq.com"
        );
        for bad in ["queen-mq-v1:6632", "", "ftp://host", "postgres://h/db", "/"] {
            assert!(normalize_base_url(bad).is_err(), "{bad} was accepted");
        }
    }

    // --------------------------------------------------------------- catalog

    #[tokio::test]
    async fn the_list_is_served_from_cache_until_it_expires() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(40));

        assert_eq!(catalog.list(None).await.unwrap().len(), 1);
        assert_eq!(catalog.list(None).await.unwrap().len(), 1);
        assert_eq!(api.lists.load(Ordering::SeqCst), 1, "second call refetched");

        tokio::time::sleep(Duration::from_millis(60)).await;
        catalog.list(None).await.unwrap();
        assert_eq!(api.lists.load(Ordering::SeqCst), 2);
    }

    /// A metadata storm is one admin call: the refresh is single-flight, and the
    /// callers that queue behind it find the entry fresh.
    #[tokio::test]
    async fn concurrent_refreshes_collapse_into_one_call() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Arc::new(Catalog::new(api.clone()));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let c = Arc::clone(&catalog);
            tasks.push(tokio::spawn(
                async move { c.list(None).await.unwrap().len() },
            ));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap(), 1);
        }
        assert_eq!(api.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn creating_a_queue_invalidates_the_cache() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::new(api.clone());

        catalog.list(None).await.unwrap();
        catalog.create("clicks", None).await.unwrap();
        let after = catalog.list(None).await.unwrap();

        assert_eq!(api.creates.lock().unwrap().as_slice(), ["clicks"]);
        assert_eq!(api.lists.load(Ordering::SeqCst), 2, "cache was not dropped");
        assert!(after.iter().any(|q| q.name == "clicks"));
    }

    /// A failed refresh serves the previous list rather than an empty world.
    #[tokio::test]
    async fn a_failed_refresh_falls_back_to_the_last_good_list() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::with_ttl(api.clone(), Duration::from_millis(10));
        catalog.list(None).await.unwrap();

        *api.fail.lock().unwrap() = Some("connection refused".into());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let stale = catalog.list(None).await.unwrap();
        assert_eq!(stale[0].name, "orders");
    }

    /// ...but a first call with nothing cached reports the failure, because
    /// there is no answer to give.
    #[tokio::test]
    async fn a_cold_failure_is_an_error() {
        let api = FakeQueen::with(&[]);
        *api.fail.lock().unwrap() = Some("connection refused".into());
        let catalog = Catalog::new(api);
        assert!(catalog.list(None).await.is_err());
    }

    /// Two credentials are two catalogs — the M5 seam, wired now so it cannot be
    /// forgotten when SASL lands.
    #[tokio::test]
    async fn each_token_gets_its_own_entry() {
        let api = FakeQueen::with(&[("orders", 4)]);
        let catalog = Catalog::new(api.clone());
        catalog.list(Some("tenant-a")).await.unwrap();
        catalog.list(Some("tenant-b")).await.unwrap();
        catalog.list(Some("tenant-a")).await.unwrap();
        assert_eq!(api.lists.load(Ordering::SeqCst), 2);
        assert_eq!(
            api.tokens.lock().unwrap().as_slice(),
            [Some("tenant-a".to_string()), Some("tenant-b".to_string())]
        );
    }
}
