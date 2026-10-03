//! One connector, source or sink, as the broker runs it. OWNER: agent C.
//!
//! The broker's manager knows connectors only through this type: build one
//! from a stored document ([`Connector::new`], which validates it again — a
//! document written by an older broker must not run with rules it never
//! met), run it until its stop signal, read its status. The engines
//! ([`Source`], [`Sink`]) own everything else.

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::config::{ConnectorDoc, Kind, NodeKnobs};
use crate::error::Result;
use crate::metrics::{ConnMetrics, Metrics};
use crate::queen::QueenApi;
use crate::sink::Sink;
use crate::source::Source;
use crate::stop::Stop;

/// What a connector needs from the node that runs it.
#[derive(Clone)]
pub struct Context {
    /// The connector's tenant (the broker tenant id).
    pub tenant: String,
    /// `None` for the default tenant: the metrics label is absent then.
    pub tenant_label: Option<String>,
    /// The connector's name (PLAN §3.1).
    pub name: String,
    /// The Queen API, bound to `tenant`.
    pub api: Arc<dyn QueenApi>,
    pub knobs: Arc<NodeKnobs>,
    pub metrics: Arc<Metrics>,
}

impl Context {
    pub fn conn_metrics(&self, kind: Kind) -> Arc<ConnMetrics> {
        self.metrics
            .handle(self.tenant_label.as_deref(), &self.name, kind)
    }
}

/// How [`Connector::run`] ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnd {
    /// The stop signal.
    Stopped,
    /// A source's `deleting` request was carried out (slot and managed
    /// publication dropped, runtime KV removed): the broker removes the
    /// document now.
    TornDown,
}

enum Engine {
    Source(Arc<Source>),
    Sink(Arc<Sink>),
}

/// A source or a sink.
pub struct Connector {
    name: String,
    kind: Kind,
    enabled: bool,
    engine: Engine,
}

impl Connector {
    /// Validate `doc` and build the engine. `password` is the unsealed
    /// password (the broker unseals `connection.passwordSealed`).
    ///
    /// Nothing is contacted here (no database, no broker call): a document
    /// that validates always builds, and every runtime failure surfaces
    /// through [`Connector::run`] and the status.
    pub fn new(ctx: Context, doc: ConnectorDoc, password: Option<String>) -> Result<Connector> {
        doc.validate(&ctx.name)?;
        let name = ctx.name.clone();
        let (kind, enabled) = (doc.kind, doc.enabled);
        let engine = match kind {
            Kind::Source => Engine::Source(Source::new(ctx, doc, password)?),
            Kind::Sink => Engine::Sink(Sink::new(ctx, doc, password)?),
        };
        Ok(Connector {
            name,
            kind,
            enabled,
            engine,
        })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Run until stopped, torn down, or a non-retryable error. Retryable
    /// errors are handled inside (backoff); a returned `Err` is reported and
    /// the broker restarts the connector after its own backoff.
    pub async fn run(&self, stop: Stop) -> Result<RunEnd> {
        match &self.engine {
            Engine::Source(s) => Arc::clone(s).run(stop).await,
            Engine::Sink(s) => Arc::clone(s).run(stop).await,
        }
    }

    /// The status block (PLAN §4.7 / §5.5), `kind` and `name` included.
    ///
    /// `name`, `kind` and `enabled` come from the document and win over a
    /// same-named field of the engine's block, so a reader can always tell
    /// what it is looking at.
    pub fn status(&self) -> Value {
        let engine = match &self.engine {
            Engine::Source(s) => s.status(),
            Engine::Sink(s) => s.status(),
        };
        let mut out = Map::new();
        out.insert("name".into(), Value::String(self.name.clone()));
        out.insert("kind".into(), Value::String(self.kind.as_str().into()));
        out.insert("enabled".into(), Value::Bool(self.enabled));
        match engine {
            Value::Object(m) => {
                for (k, v) in m {
                    if !out.contains_key(&k) {
                        out.insert(k, v);
                    }
                }
            }
            Value::Null => {}
            other => {
                out.insert("engine".into(), other);
            }
        }
        Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeQueen;
    use serde_json::json;

    fn ctx(name: &str) -> Context {
        Context {
            tenant: "00000000-0000-0000-0000-000000000001".into(),
            tenant_label: None,
            name: name.into(),
            api: FakeQueen::new(),
            knobs: Arc::new(NodeKnobs::defaults("node-1")),
            metrics: Metrics::new(),
        }
    }

    fn sink(enabled: bool) -> ConnectorDoc {
        ConnectorDoc::from_json(&json!({
            "kind": "sink",
            "enabled": enabled,
            "connection": {"host": "db.example.com", "database": "d", "user": "u"},
            "sink": {"queue": "orders", "table": "orders_copy", "mode": "append"}
        }))
        .unwrap()
    }

    #[test]
    fn an_invalid_document_never_builds() {
        let mut d = sink(true);
        d.sink.as_mut().unwrap().batch = 0;
        let e = Connector::new(ctx("c1"), d, None).err().unwrap();
        assert_eq!(e.code(), "config");
        assert!(e.to_string().contains("sink.batch"), "{e}");
        let e = Connector::new(ctx("Bad Name"), sink(true), None)
            .err()
            .unwrap();
        assert!(e.to_string().contains("name:"), "{e}");
        let mut wrong_kind = sink(true);
        wrong_kind.kind = Kind::Source;
        assert!(Connector::new(ctx("c1"), wrong_kind, None).is_err());
    }

    #[tokio::test]
    async fn a_sink_builds_reports_and_a_disabled_one_runs_until_stopped() {
        let c = Connector::new(ctx("copy"), sink(false), Some("pw".into())).unwrap();
        assert_eq!(c.kind(), Kind::Sink);
        let s = c.status();
        assert_eq!(s["name"], "copy");
        assert_eq!(s["kind"], "sink");
        assert_eq!(s["enabled"], false);
        assert!(s.get("phase").is_some(), "{s}");
        let (h, stop) = crate::stop::stop_pair();
        let run = async { c.run(stop).await };
        h.stop();
        assert_eq!(run.await.unwrap(), RunEnd::Stopped);
        assert_eq!(c.status()["phase"], "disabled");
    }
}
