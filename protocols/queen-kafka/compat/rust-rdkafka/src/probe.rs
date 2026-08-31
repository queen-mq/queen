//! The client context every producer and consumer in this suite is built with.
//!
//! Its job is to make the run auditable from the CLIENT's side. librdkafka is
//! the only party that knows which `(api_key, api_version)` it actually chose
//! after reading ApiVersions, and it says so — but only into its own
//! `debug=protocol` stream, which by default goes to stderr and is never read.
//! So every client here is created with `debug=protocol` and
//! `set_log_level(Debug)`, the log callback is overridden, and the
//! `Sent <Api>Request (v<N>` lines are parsed into a table that the suite
//! prints at the end.
//!
//! That is the difference between "the facade advertises Fetch 4-6" (a claim
//! about `versions.rs`) and "this client sent Fetch v6" (a fact about the wire).
//! Only the second one is evidence.
//!
//! The same callback also keeps:
//!
//! * every line librdkafka logged at WARNING or worse, which is where the
//!   client tells you it silently downgraded something — the zstd/Fetch-v10
//!   gate being the case this facade actually hits;
//! * the rebalance transitions, from `ConsumerContext`, so a group test can
//!   assert what the coordinator handed out rather than infer it from which
//!   messages turned up.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::TopicPartitionList;

#[derive(Default)]
pub struct Recorder {
    /// api name -> the set of versions this process was observed SENDING.
    pub sent: BTreeMap<String, Vec<i32>>,
    /// api name -> the set of versions the facade ANSWERED at.
    pub received: BTreeMap<String, Vec<i32>>,
    /// Everything at WARNING or worse, verbatim.
    pub warnings: Vec<String>,
    /// Rebalance transitions, newest last.
    pub rebalances: Vec<String>,
    /// Every log line, capped. Kept for post-mortem greps (the codec downgrade
    /// notice, for one, is logged at NOTICE and would not make `warnings`).
    pub lines: Vec<String>,
}

const MAX_LINES: usize = 40_000;

impl Recorder {
    fn note(&mut self, level: RDKafkaLogLevel, fac: &str, msg: &str) {
        if self.lines.len() < MAX_LINES {
            self.lines.push(format!("[{level:?}] {fac}: {msg}"));
        }
        if matches!(
            level,
            RDKafkaLogLevel::Emerg
                | RDKafkaLogLevel::Alert
                | RDKafkaLogLevel::Critical
                | RDKafkaLogLevel::Error
                | RDKafkaLogLevel::Warning
        ) {
            self.warnings.push(format!("{fac}: {msg}"));
        }
        // "…: Sent FetchRequest (v6, 91 bytes @ 0, CorrId 12)"
        // "…: Received FetchResponse (v6, 152 bytes, CorrId 12, rtt 3.10ms)"
        for (needle, table) in [
            ("Sent ", &mut self.sent as &mut BTreeMap<String, Vec<i32>>),
            ("Received ", &mut self.received),
        ] {
            if let Some(rest) = msg.find(needle).map(|i| &msg[i + needle.len()..]) {
                if let Some((name, tail)) = rest.split_once(" (v") {
                    let name = name
                        .trim_end_matches("Request")
                        .trim_end_matches("Response");
                    if !name.is_empty()
                        && name.chars().all(|c| c.is_ascii_alphanumeric())
                        && !name.contains(' ')
                    {
                        if let Some(v) = tail
                            .split(|c: char| !c.is_ascii_digit())
                            .find(|s| !s.is_empty())
                            .and_then(|s| s.parse::<i32>().ok())
                        {
                            let e = table.entry(name.to_string()).or_default();
                            if !e.contains(&v) {
                                e.push(v);
                                e.sort_unstable();
                            }
                        }
                    }
                }
            }
        }
    }

    /// Lines whose text contains `needle`, case-sensitive. Used by the
    /// compression lane to find librdkafka's own downgrade notice.
    pub fn grep(&self, needle: &str) -> Vec<String> {
        self.lines
            .iter()
            .filter(|l| l.contains(needle))
            .cloned()
            .collect()
    }
}

#[derive(Clone)]
pub struct Probe {
    pub rec: Arc<Mutex<Recorder>>,
    /// Printed beside a rebalance so a two-consumer test can tell the members
    /// apart in the transcript.
    pub who: String,
}

impl Probe {
    pub fn new(rec: &Arc<Mutex<Recorder>>, who: &str) -> Probe {
        Probe {
            rec: Arc::clone(rec),
            who: who.to_string(),
        }
    }
}

impl ClientContext for Probe {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if let Ok(mut r) = self.rec.lock() {
            r.note(level, fac, log_message);
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if let Ok(mut r) = self.rec.lock() {
            r.warnings.push(format!("error cb: {error} ({reason})"));
        }
    }
}

impl ConsumerContext for Probe {
    fn pre_rebalance(&self, _c: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        self.rebalance("pre", rebalance);
    }

    fn post_rebalance(&self, _c: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        self.rebalance("post", rebalance);
    }

    fn commit_callback(&self, result: Result<(), KafkaError>, offsets: &TopicPartitionList) {
        if let Ok(mut r) = self.rec.lock() {
            r.rebalances.push(match result {
                Ok(()) => format!("{}: commit ok {}", self.who, tpl_str(offsets)),
                Err(e) => format!("{}: commit ERR {e}", self.who),
            });
        }
    }
}

impl Probe {
    fn rebalance(&self, phase: &str, rebalance: &Rebalance<'_>) {
        let text = match rebalance {
            Rebalance::Assign(tpl) => format!("assign {}", tpl_str(tpl)),
            Rebalance::Revoke(tpl) => format!("revoke {}", tpl_str(tpl)),
            Rebalance::Error(e) => format!("error {e}"),
        };
        if let Ok(mut r) = self.rec.lock() {
            r.rebalances.push(format!("{} {phase}: {text}", self.who));
        }
    }
}

pub fn tpl_str(tpl: &TopicPartitionList) -> String {
    let mut parts: Vec<String> = tpl
        .elements()
        .iter()
        .map(|e| match e.offset().to_raw() {
            Some(o) if o >= 0 => format!("{}:{}@{}", e.topic(), e.partition(), o),
            _ => format!("{}:{}", e.topic(), e.partition()),
        })
        .collect();
    parts.sort();
    format!("[{}]", parts.join(" "))
}
