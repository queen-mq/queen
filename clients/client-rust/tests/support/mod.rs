//! A fake broker: an HTTP/1.1 server the tests drive from a script.
//!
//! Every other SDK has one of these — `test-v2/http-unit` and
//! `streams-unit/fakeServer.js` in JS, `httptest` in Go, `httpx.MockTransport`
//! in Python — and the Rust client was the only one without. That is why its
//! retry, 429, failover and `Host`-pinning code had never once been run against
//! a real response: a live broker answers 200, so the interesting branches only
//! exist on paper.
//!
//! It speaks just enough HTTP/1.1 for `reqwest` over plain `http://`: read the
//! head, honour `Content-Length`, answer from the script, keep the connection
//! alive. No new dependency — `tokio` is already a dev-dependency with the
//! `full` feature, which brings `tokio::net`.
//!
//! The script is a list of [`Reply`]s consumed in order, and **the last one
//! repeats forever**. So `[429, 429, 200]` throttles twice then succeeds, while
//! `[429]` throttles every request until the client gives up — which is how you
//! measure a retry budget: count the hits.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Chooses the reply for request number `n`.
type Decider = Arc<dyn Fn(usize, &Hit) -> Reply + Send + Sync>;

/// One request as the server saw it.
#[derive(Debug, Clone)]
pub struct Hit {
    pub method: String,
    /// Path including the query string, exactly as it arrived.
    pub path: String,
    /// Header names lowercased; a repeated header keeps the last value.
    pub headers: HashMap<String, String>,
    pub body: String,
    /// When the request arrived, for asserting on backoff gaps.
    pub at: Instant,
}

impl Hit {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(|s| &**s)
    }

    /// The path with the query string stripped.
    pub fn route(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }

    /// One query parameter, undecoded.
    pub fn query(&self, key: &str) -> Option<&str> {
        let q = self.path.split_once('?')?.1;
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == key).then_some(v)
        })
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

/// What the server does with one request.
#[derive(Debug, Clone)]
pub enum Action {
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
    /// Read the request and never answer. For client-side timeouts.
    Hang,
    /// Close the connection without answering, like a proxy dropping a
    /// connection from its pool.
    Close,
    /// Send a head promising more bytes than follow, then close. A response
    /// that is valid until it is truncated.
    Truncate { status: u16, body: String },
}

#[derive(Debug, Clone)]
pub struct Reply(pub Action);

impl Reply {
    /// A status with an empty JSON object as its body.
    pub fn status(status: u16) -> Self {
        Reply(Action::Respond {
            status,
            headers: Vec::new(),
            body: "{}".into(),
        })
    }

    /// 200 with a JSON body.
    pub fn ok(body: impl Into<String>) -> Self {
        Reply(Action::Respond {
            status: 200,
            headers: Vec::new(),
            body: body.into(),
        })
    }

    /// A status with a JSON body.
    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Reply(Action::Respond {
            status,
            headers: Vec::new(),
            body: body.into(),
        })
    }

    /// A status with a body that is deliberately not JSON — an ingress error
    /// page, which is what a 502 actually looks like in front of a broker.
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Reply(Action::Respond {
            status,
            headers: vec![("Content-Type".into(), "text/html".into())],
            body: body.into(),
        })
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let Action::Respond { headers, .. } = &mut self.0 {
            headers.push((name.into(), value.into()));
        }
        self
    }

    pub fn hang() -> Self {
        Reply(Action::Hang)
    }

    pub fn close() -> Self {
        Reply(Action::Close)
    }

    pub fn truncated(status: u16, body: impl Into<String>) -> Self {
        Reply(Action::Truncate {
            status,
            body: body.into(),
        })
    }
}

/// A running fake broker. Dropping it stops the accept loop.
pub struct FakeBroker {
    port: u16,
    hits: Arc<Mutex<Vec<Hit>>>,
    served: Arc<AtomicUsize>,
    accept: tokio::task::JoinHandle<()>,
    /// Per-connection tasks. `Reply::hang()` parks one forever on purpose, so
    /// they have to be cancelled explicitly or they outlive the test.
    connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.accept.abort();
        for task in self.connections.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

impl FakeBroker {
    /// Start on an ephemeral port with a fixed script.
    pub async fn start(plan: Vec<Reply>) -> Self {
        assert!(!plan.is_empty(), "a fake broker needs at least one reply");
        Self::start_with(move |n, _hit| plan[n.min(plan.len() - 1)].clone()).await
    }

    /// Start with a decision function: `(index, request) -> reply`.
    ///
    /// The index counts requests served by this broker, so a script can vary by
    /// attempt and by route at once — which is what a pop-vs-push retry budget
    /// test needs.
    pub async fn start_with<F>(decide: F) -> Self
    where
        F: Fn(usize, &Hit) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("could not bind a fake broker");
        let port = listener.local_addr().unwrap().port();
        let hits: Arc<Mutex<Vec<Hit>>> = Arc::new(Mutex::new(Vec::new()));
        let served = Arc::new(AtomicUsize::new(0));

        let decide = Arc::new(decide);
        let connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let accept = {
            let hits = Arc::clone(&hits);
            let served = Arc::clone(&served);
            let connections = Arc::clone(&connections);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let hits = Arc::clone(&hits);
                    let served = Arc::clone(&served);
                    let decide = Arc::clone(&decide);
                    let task = tokio::spawn(async move {
                        serve(stream, hits, served, decide).await;
                    });
                    let mut open = connections.lock().unwrap();
                    open.retain(|t| !t.is_finished());
                    open.push(task);
                }
            })
        };

        Self {
            port,
            hits,
            served,
            accept,
            connections,
        }
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Every request served so far, in arrival order.
    pub fn hits(&self) -> Vec<Hit> {
        self.hits.lock().unwrap().clone()
    }

    pub fn hit_count(&self) -> usize {
        self.hits.lock().unwrap().len()
    }

    /// Gaps between consecutive requests. What a backoff assertion reads.
    pub fn gaps(&self) -> Vec<Duration> {
        let hits = self.hits.lock().unwrap();
        hits.windows(2).map(|w| w[1].at - w[0].at).collect()
    }

    /// Wait until at least `n` requests have arrived, or the deadline passes.
    /// Returns the count actually reached.
    pub async fn wait_for_hits(&self, n: usize, timeout: Duration) -> usize {
        let deadline = Instant::now() + timeout;
        loop {
            let seen = self.hit_count();
            if seen >= n || Instant::now() >= deadline {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn serve(
    mut stream: TcpStream,
    hits: Arc<Mutex<Vec<Hit>>>,
    served: Arc<AtomicUsize>,
    decide: Decider,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    loop {
        // Head: everything up to the blank line.
        let head_end = loop {
            if let Some(i) = find(&buf, b"\r\n\r\n") {
                break i + 4;
            }
            let mut chunk = [0u8; 2048];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };

        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default().to_string();
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();

        let mut headers: HashMap<String, String> = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }

        let want: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        buf.drain(..head_end);
        while buf.len() < want {
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let body = String::from_utf8_lossy(&buf[..want]).to_string();
        buf.drain(..want);

        let hit = Hit {
            method,
            path,
            headers,
            body,
            at: Instant::now(),
        };

        let n = served.fetch_add(1, Ordering::SeqCst);
        let reply = decide(n, &hit);
        hits.lock().unwrap().push(hit);

        match reply.0 {
            Action::Hang => {
                // Hold the connection open and answer nothing. The client's own
                // timeout is what ends this.
                std::future::pending::<()>().await;
            }
            Action::Close => return,
            Action::Truncate { status, body } => {
                let head = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                     Content-Length: {len}\r\n\r\n",
                    reason = reason(status),
                    len = body.len() + 32,
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body.as_bytes()).await;
                return;
            }
            Action::Respond {
                status,
                headers,
                body,
            } => {
                let mut head = format!("HTTP/1.1 {status} {reason}\r\n", reason = reason(status));
                let has_type = headers
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
                if !has_type {
                    head.push_str("Content-Type: application/json\r\n");
                }
                for (k, v) in &headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                // 204 carries no body by definition, and the client is entitled
                // to stop reading at the head.
                let body = if status == 204 { String::new() } else { body };
                head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
                if stream.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                if !body.is_empty() && stream.write_all(body.as_bytes()).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    }
}
