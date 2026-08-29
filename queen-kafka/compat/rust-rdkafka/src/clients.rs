//! Every producer and consumer this suite builds, and the exact config it
//! builds them with.
//!
//! The config is here rather than scattered through the scenarios because the
//! config IS half the compatibility result: "rdkafka works against queen-kafka"
//! is only true beside the list of settings it takes to make it true. Three of
//! them are not defaults and are not optional:
//!
//! * **`enable.idempotence=false`.** librdkafka already defaults it off, so
//!   this line changes nothing today. It used to be the single setting that
//!   broke every other Kafka client against this facade; M7 F3 advertised
//!   InitProducerId and retired that, and the line is kept only so the rest of
//!   the suite keeps measuring the NON-idempotent path whatever a future
//!   librdkafka does with its default. `scenarios::idempotence` is the one
//!   place that turns it back on, and it asserts the send lands.
//! * **`debug=protocol` + log level Debug.** Not for troubleshooting: it is the
//!   only way to read which API versions were actually negotiated (see
//!   `probe.rs`), which the suite prints as its version table.
//! * **`session.timeout.ms` inside 6000..=300000.** Outside the facade's
//!   configured window the join is answered INVALID_SESSION_TIMEOUT (26). The
//!   default 45000 is inside it; the value is set explicitly so the window is
//!   visible at the call site.
//!
//! Everything else is librdkafka's default on purpose. A suite that tunes its
//! way to a pass has proved something about the tuning.

use std::time::Duration;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;

use crate::probe::Probe;

#[derive(Clone)]
pub struct Env {
    pub bootstrap: String,
    pub run: String,
    pub partitions: i32,
    /// SASL_SSL listener, if the M5 lane is wanted.
    pub tls_bootstrap: Option<String>,
    pub sasl_token: Option<String>,
    pub tls_ca: Option<String>,
}

impl Env {
    pub fn topic(&self, suffix: &str) -> String {
        format!("rdk-rs-{}-{}", self.run, suffix)
    }

    pub fn group(&self, suffix: &str) -> String {
        format!("rdk-rs-g-{}-{}", self.run, suffix)
    }
}

/// Settings shared by every client: where the broker is, and how to see what
/// the client said to it.
fn base(env: &Env) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &env.bootstrap)
        .set("debug", "protocol")
        // Bound the reconnect storm a fail-fast run would otherwise sit in.
        .set("socket.timeout.ms", "10000")
        .set("reconnect.backoff.max.ms", "2000")
        .set("client.id", format!("rdk-rs-{}", env.run));
    c.set_log_level(RDKafkaLogLevel::Debug);
    c
}

/// Apply the SASL_SSL half. `token` is the Queen bearer token: the facade's
/// SASL/PLAIN handler forwards the PASSWORD to Queen as a bearer and the
/// username is a free label.
pub fn with_sasl_ssl(c: &mut ClientConfig, token: &str, ca: Option<&str>, user: &str) {
    c.set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", "PLAIN")
        .set("sasl.username", user)
        .set("sasl.password", token);
    match ca {
        // Verify the chain for real when the caller hands over the PEM.
        Some(pem) => {
            c.set("ssl.ca.location", pem);
        }
        // Otherwise the certificate is self-signed and unknown; the connection
        // is still encrypted and still SASL-authenticated, which is what the
        // lane is proving.
        None => {
            c.set("enable.ssl.certificate.verification", "false");
        }
    }
}

pub fn producer(env: &Env, probe: Probe, extra: &[(&str, &str)]) -> FutureProducer<Probe> {
    let mut c = base(env);
    c.set("enable.idempotence", "false")
        .set("acks", "all")
        // Long enough that a 3s group-join delay elsewhere in the run, or a
        // cold auto-create, never looks like a delivery failure.
        .set("message.timeout.ms", "60000")
        // Force real batches: without a linger the 512-record corpus can go out
        // as 512 one-record RecordBatches and the compression lane would be
        // testing nothing.
        .set("linger.ms", "50")
        .set("batch.num.messages", "1000");
    for (k, v) in extra {
        c.set(*k, *v);
    }
    c.create_with_context(probe).expect("producer")
}

pub fn consumer(
    env: &Env,
    probe: Probe,
    group: &str,
    extra: &[(&str, &str)],
) -> StreamConsumer<Probe> {
    let mut c = base(env);
    c.set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Inside QUEEN_KAFKA_GROUP_MIN/MAX_SESSION_TIMEOUT_MS (6000..300000).
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "3000")
        .set("max.poll.interval.ms", "300000")
        .set("fetch.wait.max.ms", "500");
    for (k, v) in extra {
        c.set(*k, *v);
    }
    c.create_with_context(probe).expect("consumer")
}

pub const POLL: Duration = Duration::from_secs(20);
