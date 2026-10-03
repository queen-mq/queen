//! The sink's log lines say whose queue they are about.
//!
//! A broker runs one sink per tenant, and two tenants may both have a queue
//! called `orders`: a line that names only the queue cannot be told apart from
//! the other tenant's. Every line a sink logs is inside a `sink` span carrying
//! the tenant (when the sink's configuration names a tenant label — every
//! tenant but the broker's default one), and every line of one queue's task
//! inside a `queue` span below it. A subscriber prints the spans in front of
//! the line: `sink{tenant=acme}:queue{queue=orders}: queen-s3: window
//! committed …`.
//!
//! The subscriber here records what a real one would print: each line's
//! message and fields, and the spans it was inside, outermost first.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use queen_s3::queen::FakeQueen;
use queen_s3::s3::MemoryStore;
use queen_s3::{Config, NodeKnobs, Sink};

#[path = "driver_support.rs"]
mod support;
use support::*;

/// `(name, value)` pairs, as a subscriber would print them.
#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

/// One line: its message, its own fields, and its spans, outermost first.
#[derive(Clone, Debug)]
struct Line {
    message: String,
    fields: Vec<(String, String)>,
    spans: Vec<(String, Vec<(String, String)>)>,
}

impl Line {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The value of `name` on the span called `span`, if the line is in one.
    fn span_field(&self, span: &str, name: &str) -> Option<&str> {
        self.spans
            .iter()
            .find(|(n, _)| n == span)
            .and_then(|(_, fields)| fields.iter().find(|(k, _)| k == name))
            .map(|(_, v)| v.as_str())
    }
}

/// Span id → (name, fields, parent).
type SpanTable = HashMap<u64, (String, Vec<(String, String)>, Option<u64>)>;

#[derive(Default)]
struct Capture {
    next: AtomicU64,
    spans: Mutex<SpanTable>,
    /// The entered spans of the one thread the test runs on.
    stack: Mutex<Vec<u64>>,
    lines: Mutex<Vec<Line>>,
}

struct Handle(Arc<Capture>);

impl Subscriber for Handle {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let c = &self.0;
        let id = c.next.fetch_add(1, Ordering::SeqCst) + 1;
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let parent = match attrs.parent() {
            Some(p) => Some(p.into_u64()),
            None if attrs.is_contextual() => c.stack.lock().unwrap().last().copied(),
            None => None,
        };
        c.spans
            .lock()
            .unwrap()
            .insert(id, (attrs.metadata().name().to_string(), fields.0, parent));
        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut fields = Fields::default();
        values.record(&mut fields);
        if let Some(s) = self.0.spans.lock().unwrap().get_mut(&span.into_u64()) {
            s.1.extend(fields.0);
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let c = &self.0;
        let mut fields = Fields::default();
        event.record(&mut fields);
        let message = fields
            .0
            .iter()
            .find(|(k, _)| k == "message")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let table = c.spans.lock().unwrap();
        let mut spans = Vec::new();
        let mut at = c.stack.lock().unwrap().last().copied();
        while let Some(id) = at {
            let Some((name, fields, parent)) = table.get(&id) else {
                break;
            };
            spans.push((name.clone(), fields.clone()));
            at = *parent;
        }
        spans.reverse();
        c.lines.lock().unwrap().push(Line {
            message,
            fields: fields.0,
            spans,
        });
    }

    fn enter(&self, span: &Id) {
        self.0.stack.lock().unwrap().push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        let mut stack = self.0.stack.lock().unwrap();
        if let Some(at) = stack.iter().rposition(|id| *id == span.into_u64()) {
            stack.remove(at);
        }
    }
}

fn node() -> NodeKnobs {
    NodeKnobs::from_pairs_with(
        &[
            ("QUEEN_S3_SAFE_GUARD_MS", "0"),
            ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "50"),
        ],
        "node-1",
    )
    .unwrap()
}

fn doc() -> Value {
    json!({
        "endpoint": "http://gw:7070",
        "region": "us-east-1",
        "bucket": "lake",
        "accessKey": "ak",
        "queues": "orders",
        "start": "earliest",
    })
}

/// Run `cfg`'s sink over a seeded queue until both windows commit, then stop
/// it, and hand back every line it logged.
async fn lines_of(cfg: Config) -> Vec<Line> {
    let capture = Arc::new(Capture::default());
    let _guard = tracing::subscriber::set_default(Handle(capture.clone()));
    let queen = Arc::new(FakeQueen::new());
    seed_two_hours(&queen, "orders", &["a"]);
    let sink = Arc::new(Sink::new(cfg, queen, Some(Arc::new(MemoryStore::new()))).unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let running = {
        let sink = sink.clone();
        tokio::spawn(async move {
            sink.run(async {
                let _ = rx.await;
            })
            .await
        })
    };
    for _ in 0..20_000 {
        let st = sink.status();
        let committed = st["queues"]
            .as_array()
            .and_then(|qs| qs.iter().find(|q| q["name"] == "orders"))
            .is_some_and(|q| q["k"] == 2);
        if committed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tx.send(()).unwrap();
    running.await.unwrap();
    let lines = capture.lines.lock().unwrap().clone();
    lines
}

/// A tenant's sink: every line about a queue is inside `sink{tenant=acme}`,
/// and the queue task's own lines inside `queue{queue=orders}` below it.
#[tokio::test(start_paused = true)]
async fn a_tenant_s_lines_carry_the_tenant_and_the_queue() {
    let cfg = Config::from_tenant_doc(&node(), "acme", &doc(), "sk")
        .unwrap()
        .with_tenant_label(Some("acme".into()));
    let lines = lines_of(cfg).await;

    let committed: Vec<&Line> = lines
        .iter()
        .filter(|l| l.message == "window committed")
        .collect();
    assert_eq!(committed.len(), 2, "{lines:#?}");
    for l in &committed {
        assert_eq!(l.span_field("sink", "tenant"), Some("acme"), "{l:?}");
        assert_eq!(l.span_field("queue", "queue"), Some("orders"), "{l:?}");
        assert_eq!(
            l.spans.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["sink", "queue"],
            "the queue span is below the sink's: {l:?}"
        );
    }
    // Every line that names a queue names its tenant too.
    let per_queue: Vec<&Line> = lines
        .iter()
        .filter(|l| l.field("queue").is_some())
        .collect();
    assert!(per_queue.len() >= 4, "{lines:#?}");
    for l in per_queue {
        assert_eq!(l.span_field("sink", "tenant"), Some("acme"), "{l:?}");
    }
}

/// The broker's default tenant has no tenant label, so its lines carry no
/// tenant either — the same rule as its metrics — and still the queue span.
#[tokio::test(start_paused = true)]
async fn the_default_tenant_s_lines_carry_the_queue_and_no_tenant() {
    let pairs = [
        ("QUEEN_S3_QUEUES", "orders"),
        ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
        ("QUEEN_S3_REGION", "us-east-1"),
        ("QUEEN_S3_BUCKET", "lake"),
        ("QUEEN_S3_ACCESS_KEY", "ak"),
        ("QUEEN_S3_SECRET_KEY", "sk"),
        ("QUEEN_S3_START", "earliest"),
    ];
    let cfg = Config::from_env_pairs(&node(), queen_s3::config::DEFAULT_TENANT, &pairs)
        .unwrap()
        .unwrap();
    let lines = lines_of(cfg).await;
    let committed: Vec<&Line> = lines
        .iter()
        .filter(|l| l.message == "window committed")
        .collect();
    assert_eq!(committed.len(), 2, "{lines:#?}");
    for l in committed {
        assert_eq!(l.span_field("sink", "tenant"), None, "{l:?}");
        assert_eq!(l.span_field("queue", "queue"), Some("orders"), "{l:?}");
    }
}
