//! Counters and gauges of every connector of a node, one registry, rendered
//! in the Prometheus text format. OWNER: agent C.
//!
//! Families (labels `tenant` — absent for the default tenant — and
//! `connector`):
//! `queen_pg_source_transactions_total`, `queen_pg_source_messages_total`,
//! `queen_pg_source_bundles_total`, `queen_pg_source_snapshot_rows_total`,
//! `queen_pg_source_slot_lag_bytes` (gauge), `queen_pg_source_pointer_lsn`
//! (gauge), `queen_pg_sink_applied_total`, `queen_pg_sink_skipped_total`,
//! `queen_pg_sink_dlq_total`, `queen_pg_sink_batches_total`,
//! `queen_pg_connector_errors_total`, `queen_pg_connector_owner` (gauge: 1
//! while this node runs the connector's work).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::Kind;

/// One connector's series. Engines update the atomics directly.
#[derive(Debug, Default)]
pub struct ConnMetrics {
    pub transactions: AtomicU64,
    pub messages: AtomicU64,
    pub bundles: AtomicU64,
    pub snapshot_rows: AtomicU64,
    pub slot_lag_bytes: AtomicI64,
    pub pointer_lsn: AtomicU64,
    pub applied: AtomicU64,
    pub skipped: AtomicU64,
    pub dlq: AtomicU64,
    pub batches: AtomicU64,
    pub errors: AtomicU64,
    pub owner: AtomicI64,
}

/// `(tenant label, connector)`: `None` is the default tenant.
type SeriesKey = (Option<String>, String);

/// The node's registry.
#[derive(Debug, Default)]
pub struct Metrics {
    series: Mutex<BTreeMap<SeriesKey, (Kind, Arc<ConnMetrics>)>>,
}

/// One metric family: its name, help, type, which connectors carry it
/// (`None`: both kinds) and how to read it.
struct Family {
    name: &'static str,
    help: &'static str,
    gauge: bool,
    kind: Option<Kind>,
    read: fn(&ConnMetrics) -> String,
}

fn n(a: &AtomicU64) -> String {
    a.load(Ordering::Relaxed).to_string()
}

fn i(a: &AtomicI64) -> String {
    a.load(Ordering::Relaxed).to_string()
}

const FAMILIES: &[Family] = &[
    Family {
        name: "queen_pg_source_transactions_total",
        help: "Source transactions committed into Queen.",
        gauge: false,
        kind: Some(Kind::Source),
        read: |m| n(&m.transactions),
    },
    Family {
        name: "queen_pg_source_messages_total",
        help: "Messages the source pushed (stream and snapshot).",
        gauge: false,
        kind: Some(Kind::Source),
        read: |m| n(&m.messages),
    },
    Family {
        name: "queen_pg_source_bundles_total",
        help: "Queen transactions the source committed (each carries the WAL pointer).",
        gauge: false,
        kind: Some(Kind::Source),
        read: |m| n(&m.bundles),
    },
    Family {
        name: "queen_pg_source_snapshot_rows_total",
        help: "Rows the source's snapshot pushed.",
        gauge: false,
        kind: Some(Kind::Source),
        read: |m| n(&m.snapshot_rows),
    },
    Family {
        name: "queen_pg_source_slot_lag_bytes",
        help: "WAL the replication slot retains: pg_current_wal_lsn() - confirmed_flush_lsn.",
        gauge: true,
        kind: Some(Kind::Source),
        read: |m| i(&m.slot_lag_bytes),
    },
    Family {
        name: "queen_pg_source_pointer_lsn",
        help: "The WAL position (LSN, as a number) of the source pointer stored in Queen.",
        gauge: true,
        kind: Some(Kind::Source),
        read: |m| n(&m.pointer_lsn),
    },
    Family {
        name: "queen_pg_sink_applied_total",
        help: "Messages the sink applied to PostgreSQL.",
        gauge: false,
        kind: Some(Kind::Sink),
        read: |m| n(&m.applied),
    },
    Family {
        name: "queen_pg_sink_skipped_total",
        help: "Redelivered messages the sink dropped as already applied.",
        gauge: false,
        kind: Some(Kind::Sink),
        read: |m| n(&m.skipped),
    },
    Family {
        name: "queen_pg_sink_dlq_total",
        help: "Messages the sink dead-lettered.",
        gauge: false,
        kind: Some(Kind::Sink),
        read: |m| n(&m.dlq),
    },
    Family {
        name: "queen_pg_sink_batches_total",
        help: "Batches the sink committed.",
        gauge: false,
        kind: Some(Kind::Sink),
        read: |m| n(&m.batches),
    },
    Family {
        name: "queen_pg_connector_errors_total",
        help: "Errors the connector recorded.",
        gauge: false,
        kind: None,
        read: |m| n(&m.errors),
    },
    Family {
        name: "queen_pg_connector_owner",
        help: "1 while this node runs the connector's work, else 0.",
        gauge: true,
        kind: None,
        read: |m| i(&m.owner),
    },
];

/// A label value as the text format wants it: backslash, double quote and
/// newline escaped (`\\`, `\"`, `\n`).
fn escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

impl Metrics {
    pub fn new() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<SeriesKey, (Kind, Arc<ConnMetrics>)>> {
        self.series.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The series of `(tenant, connector)`, created on first use. `tenant` is
    /// `None` for the default tenant.
    ///
    /// The same name asked for again gets the SAME series (a restarted
    /// connector keeps counting where it was, as a Prometheus counter
    /// should); asked for with another kind (the document was replaced by one
    /// of the other kind), the series keeps its counters and reports the
    /// families of the new kind from then on.
    pub fn handle(&self, tenant: Option<&str>, connector: &str, kind: Kind) -> Arc<ConnMetrics> {
        let mut g = self.lock();
        let e = g
            .entry((tenant.map(str::to_string), connector.to_string()))
            .or_insert_with(|| (kind, Arc::new(ConnMetrics::default())));
        e.0 = kind;
        Arc::clone(&e.1)
    }

    /// Drop a removed connector's series.
    pub fn forget(&self, tenant: Option<&str>, connector: &str) {
        self.lock()
            .remove(&(tenant.map(str::to_string), connector.to_string()));
    }

    /// Every family, one `# TYPE` line each.
    ///
    /// A family is written only when at least one connector carries it, so a
    /// node with sinks only shows no source families (and an empty registry
    /// renders as nothing). Series are in (tenant, connector) order.
    pub fn prometheus(&self) -> String {
        let g = self.lock();
        let mut out = String::new();
        for f in FAMILIES {
            let mut lines = g
                .iter()
                .filter(|(_, (k, _))| f.kind.is_none_or(|fk| fk == *k))
                .peekable();
            if lines.peek().is_none() {
                continue;
            }
            let _ = writeln!(out, "# HELP {} {}", f.name, f.help);
            let _ = writeln!(
                out,
                "# TYPE {} {}",
                f.name,
                if f.gauge { "gauge" } else { "counter" }
            );
            for ((tenant, connector), (_, m)) in lines {
                out.push_str(f.name);
                out.push('{');
                if let Some(t) = tenant {
                    let _ = write!(out, "tenant=\"{}\",", escape(t));
                }
                let _ = writeln!(out, "connector=\"{}\"}} {}", escape(connector), (f.read)(m));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_are_shared_per_name_and_forgotten_on_removal() {
        let m = Metrics::new();
        let a = m.handle(None, "c1", Kind::Source);
        a.messages.fetch_add(3, Ordering::Relaxed);
        let b = m.handle(None, "c1", Kind::Source);
        assert!(Arc::ptr_eq(&a, &b));
        let other_tenant = m.handle(Some("t2"), "c1", Kind::Source);
        assert!(!Arc::ptr_eq(&a, &other_tenant));
        m.forget(None, "c1");
        let c = m.handle(None, "c1", Kind::Source);
        assert_eq!(c.messages.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn an_empty_registry_renders_nothing() {
        assert_eq!(Metrics::new().prometheus(), "");
    }

    #[test]
    fn the_text_format_per_kind_with_labels() {
        let m = Metrics::new();
        let src = m.handle(None, "orders-src", Kind::Source);
        src.transactions.store(5, Ordering::Relaxed);
        src.messages.store(12, Ordering::Relaxed);
        src.slot_lag_bytes.store(-1, Ordering::Relaxed);
        src.pointer_lsn.store(0x16B3748, Ordering::Relaxed);
        src.owner.store(1, Ordering::Relaxed);
        let snk = m.handle(Some("acme\"q\\x\ny"), "copy", Kind::Sink);
        snk.applied.store(7, Ordering::Relaxed);
        snk.errors.store(2, Ordering::Relaxed);
        let text = m.prometheus();

        // One HELP and one TYPE per family, never twice.
        for f in FAMILIES {
            assert_eq!(
                text.matches(&format!("# TYPE {} ", f.name)).count(),
                1,
                "{}",
                f.name
            );
            assert_eq!(
                text.matches(&format!("# HELP {} ", f.name)).count(),
                1,
                "{}",
                f.name
            );
        }
        assert!(text.contains("# TYPE queen_pg_source_messages_total counter\n"));
        assert!(text.contains("# TYPE queen_pg_source_slot_lag_bytes gauge\n"));
        assert!(text.contains("queen_pg_source_messages_total{connector=\"orders-src\"} 12\n"));
        assert!(text.contains("queen_pg_source_slot_lag_bytes{connector=\"orders-src\"} -1\n"));
        assert!(text.contains("queen_pg_source_pointer_lsn{connector=\"orders-src\"} 23803720\n"));
        let t = "tenant=\"acme\\\"q\\\\x\\ny\",connector=\"copy\"";
        assert!(
            text.contains(&format!("queen_pg_sink_applied_total{{{t}}} 7\n")),
            "{text}"
        );
        // Sources carry no sink families and the reverse.
        assert!(!text.contains("queen_pg_sink_applied_total{connector=\"orders-src\"}"));
        assert!(!text.contains(&format!("queen_pg_source_messages_total{{{t}}}")));
        // Both carry errors and owner.
        assert!(text.contains("queen_pg_connector_errors_total{connector=\"orders-src\"} 0\n"));
        assert!(text.contains(&format!("queen_pg_connector_errors_total{{{t}}} 2\n")));
        assert!(text.contains("queen_pg_connector_owner{connector=\"orders-src\"} 1\n"));
        // Every non-comment line is `name{labels} value`.
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(series.ends_with('}'), "{line}");
            assert!(value.parse::<f64>().is_ok(), "{line}");
        }
    }

    #[test]
    fn sinks_only_show_no_source_families() {
        let m = Metrics::new();
        m.handle(None, "s", Kind::Sink);
        let text = m.prometheus();
        assert!(!text.contains("queen_pg_source_"));
        assert!(text.contains("queen_pg_sink_batches_total{connector=\"s\"} 0"));
        // A replaced document of the other kind switches its families.
        m.handle(None, "s", Kind::Source);
        let text = m.prometheus();
        assert!(!text.contains("queen_pg_sink_"));
        assert!(text.contains("queen_pg_source_bundles_total{connector=\"s\"} 0"));
    }
}
