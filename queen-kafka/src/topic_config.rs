//! The Kafka topic-config vocabulary, in ONE table read in both directions.
//!
//! CreateTopics WRITES a config (`handlers::create_topics`, through
//! `POST /api/v1/configure`) and DescribeConfigs READS one back
//! (`handlers::describe_configs`). Those two must not disagree about what a
//! config NAME means here, so neither of them owns the vocabulary: this module
//! does, and both go through it.
//!
//! ## The rule that makes the read side short
//!
//! **A key is reported only when the facade can name the thing that enforces its
//! value; every other key is omitted.** Omission is protocol-legal — a
//! DescribeConfigs resource result carries the configs it has, not a fixed set —
//! and it is the only answer that cannot mislead. The alternative, reporting a
//! plausible Kafka default for a knob nothing here honours, would tell a client
//! it has a durability or retention setting it does not have.
//!
//! That rule bites hardest on `retention.ms`. Queen exposes NO HTTP read of a
//! queue's configuration: `GET /api/v1/resources/queues/:queue` answers
//! id/name/namespace/task/createdAt/partitions/messages and no config at all,
//! and `GET /api/v1/status/queues/:queue` answers a `config` object carrying
//! leaseTime, retryLimit, retryDelay, ttl, maxQueueSize and deadLetterQueue —
//! and NOT `retentionEnabled`/`retentionSeconds`, which is the one pair
//! `retention.ms` maps to. So retention is **writable and not readable** through
//! this facade: a create may set it, and a following describe cannot see it. It
//! is therefore not reported at all rather than reported as a guess, and the
//! loop is closed where it can be — the CreateTopics v5+ response echoes what
//! the create actually applied ([`Applied::echo`]).
//!
//! ## `read_only = true` on everything
//!
//! AlterConfigs is not advertised (`versions::ADVERTISED`), so nothing reported
//! here can be changed through this facade. A UI that greys out its edit button
//! on the strength of that flag is being told the truth.

use serde_json::json;

/// Kafka's `cleanup.policy`.
pub const CLEANUP_POLICY: &str = "cleanup.policy";
/// Kafka's `retention.ms`.
pub const RETENTION_MS: &str = "retention.ms";
/// Kafka's `min.insync.replicas`.
pub const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// The only cleanup policy this facade has. Nothing anywhere compacts.
pub const CLEANUP_DELETE: &str = "delete";

/// Kafka's `ConfigResource.ConfigSource`, the values the wire carries.
///
/// Only the three this facade can honestly claim are named. `TOPIC_CONFIG` is
/// "somebody set this on this topic", `DEFAULT_CONFIG` is "nobody set it, this
/// is what it is", and `STATIC_BROKER_CONFIG` is "this came out of the process's
/// start-up configuration" — which is exactly what a `QUEEN_KAFKA_*` knob is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// 1 — set on this resource. Used for a config a CreateTopics applied.
    Topic = 1,
    /// 4 — the process's start-up configuration.
    StaticBroker = 4,
    /// 5 — nobody set it; this is the value in force.
    Default = 5,
}

/// Kafka's `ConfigType`, answered from DescribeConfigs v3 on.
///
/// Only the three this facade reports are named; a key whose type is not one of
/// these is a key this facade does not report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// 1
    Boolean = 1,
    /// 2
    String = 2,
    /// 3
    Int = 3,
    /// 5 — Kafka's own type for the millisecond knobs that are `long` in
    /// `KafkaConfig` (`connections.max.idle.ms` is one), reported as such
    /// rather than as INT so a tool that renders the type is not told a
    /// narrower one than Kafka would.
    Long = 5,
}

/// Nothing this facade reports is a credential, a password or a key, so the
/// flag is a constant rather than a per-row field that could one day be set
/// wrong for a row that is not sensitive either.
pub const IS_SENSITIVE: bool = false;

/// See the module header: AlterConfigs is not advertised.
pub const READ_ONLY: bool = true;

/// One config row, as both the describe path and the create echo answer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub name: &'static str,
    /// Rendered as Kafka renders it: every config value on the wire is a
    /// string, whatever its `kind` says it means.
    pub value: String,
    pub source: Source,
    pub kind: Kind,
    /// One line, answered only when the request set `include_documentation`
    /// (v3+). Written here rather than in the handler so the sentence and the
    /// value it explains cannot drift apart.
    pub documentation: &'static str,
}

impl Reported {
    fn new(
        name: &'static str,
        value: impl Into<String>,
        source: Source,
        kind: Kind,
        documentation: &'static str,
    ) -> Reported {
        Reported {
            name,
            value: value.into(),
            source,
            kind,
            documentation,
        }
    }
}

/// The TOPIC configs this facade can name the enforcer of, for any queue the
/// catalog has.
///
/// Two rows, and both are the truest sentence the facade can say about a topic:
/// log compaction is a stated non-goal and nothing anywhere compacts, and there
/// is one logical broker — every Metadata answer already says `replicas=[0],
/// isr=[0]`, so a tool computing under-replication from `min.insync.replicas=1`
/// is right. See the module header for why the list is not longer.
pub fn topic_configs() -> Vec<Reported> {
    vec![
        Reported::new(
            CLEANUP_POLICY,
            CLEANUP_DELETE,
            Source::Default,
            Kind::String,
            "Always `delete`. Log compaction is a stated non-goal of queen-kafka and nothing \
             compacts a Queen queue.",
        ),
        Reported::new(
            MIN_INSYNC_REPLICAS,
            "1",
            Source::Default,
            Kind::Int,
            "Always 1. The facade advertises one logical broker and Metadata reports \
             replicas=[0], isr=[0]; durability is Postgres's, not a replica count's.",
        ),
    ]
}

/// What one topic's `configs[]` asked for, once it is understood.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// The options bag `POST /api/v1/configure` takes. Empty when the request
    /// carried nothing this facade acts on, which is the ordinary case and is
    /// byte-identical to what the auto-create path already sends.
    pub options: serde_json::Map<String, serde_json::Value>,
    /// The configs this create actually applied, for the CreateTopics v5+
    /// response.
    ///
    /// It carries [`topic_configs`] plus whatever this call set, which is the
    /// one place `retention.ms` can be answered at all — see the module header.
    pub echo: Vec<Reported>,
}

/// Turn one topic's requested `configs[]` into what Queen is told, or into the
/// reason the whole topic is refused INVALID_CONFIG.
///
/// `configs` is `(name, value)` as the request carried it; a `None` value is
/// Kafka's "unset, use the default", and every default here is the facade's own
/// behaviour, so it is a no-op rather than an error.
///
/// Refusing an unknown name is Kafka's own answer and it is the point: silently
/// dropping `min.insync.replicas=2` would be telling a client it got a
/// durability setting it did not get.
pub fn apply(configs: &[(&str, Option<&str>)]) -> Result<Applied, String> {
    let mut options = serde_json::Map::new();
    let mut retention: Option<Reported> = None;

    for (name, value) in configs {
        match *name {
            CLEANUP_POLICY => match value.map(str::trim) {
                // The default and the only one there is.
                None => {}
                Some(v) if v.eq_ignore_ascii_case(CLEANUP_DELETE) => {}
                // THE refusal that decides whether Kafka Connect can run.
                //
                // Connect creates its three internal topics with
                // `cleanup.policy=compact` (hard-coded in
                // `TopicAdmin.NewTopicBuilder.compacted()`), and its config
                // topic is a compacted log used as a database. A facade that
                // accepted the setting and compacted nothing would let Connect
                // start and then lose the connector configuration on the first
                // restart. Refusing here kills Connect at startup instead,
                // which is the loud failure rather than the silent one.
                Some(v) => {
                    return Err(format!(
                        "{CLEANUP_POLICY}={v} is not supported: log compaction is a stated \
                         non-goal of queen-kafka and nothing compacts a Queen queue. Only \
                         `{CLEANUP_DELETE}` is accepted. A tool whose internal topics are \
                         compacted (Kafka Connect, Kafka Streams) cannot run against this \
                         facade, and failing here is why it does not lose records later"
                    ))
                }
            },
            RETENTION_MS => match value.map(str::trim) {
                // Unset: the facade's default, which is retention OFF — which
                // is why auto-created topics do not quietly expire.
                None => {}
                Some(v) => {
                    let ms: i64 = v.parse().map_err(|_| {
                        format!("{RETENTION_MS}={v} is not a number of milliseconds")
                    })?;
                    match ms {
                        // Kafka's "infinite", and the facade's default.
                        -1 => {
                            options.insert("retentionEnabled".into(), json!(false));
                            retention = Some(Reported::new(
                                RETENTION_MS,
                                "-1",
                                Source::Topic,
                                Kind::Int,
                                "-1 is Kafka's infinite retention and is this facade's default: \
                                 a Queen queue created here has retention disabled.",
                            ));
                        }
                        // Queen's retention is in SECONDS, so a sub-second
                        // window cannot be expressed. Rounding it down would
                        // reach zero, and a retention of zero seconds means
                        // "delete everything" — refusing is the only answer
                        // that is not a data-loss surprise.
                        0..=999 => {
                            return Err(format!(
                                "{RETENTION_MS}={ms} is below the one second Queen's retention \
                                 can express (it is configured in whole seconds); rounding it \
                                 down would reach zero, which deletes everything"
                            ))
                        }
                        ms if ms >= 1_000 => {
                            // Rounded DOWN, so the facade never retains less
                            // than the client asked for by mistake — it retains
                            // at most one second less than asked, never more.
                            let seconds = ms / 1_000;
                            options.insert("retentionEnabled".into(), json!(true));
                            options.insert("retentionSeconds".into(), json!(seconds));
                            retention = Some(Reported::new(
                                RETENTION_MS,
                                (seconds * 1_000).to_string(),
                                Source::Topic,
                                Kind::Int,
                                "Queen's retention is configured in whole seconds, so the value \
                                 asked for is rounded down to the second and reported back at \
                                 the resolution it was actually stored at.",
                            ));
                        }
                        // Everything below -1. Kafka defines exactly one
                        // negative value and this is not it.
                        _ => {
                            return Err(format!(
                                "{RETENTION_MS}={ms} is not a retention: -1 is infinite and \
                                 every other value must be a non-negative number of milliseconds"
                            ))
                        }
                    }
                }
            },
            other => {
                return Err(format!(
                    "`{other}` is not a topic config this facade understands. It accepts \
                     `{CLEANUP_POLICY}={CLEANUP_DELETE}` and `{RETENTION_MS}`; every other \
                     Kafka topic config names a mechanism Queen does not have, and accepting \
                     one silently would report a setting that is not in force"
                ))
            }
        }
    }

    let mut echo = topic_configs();
    if let Some(r) = retention {
        echo.push(r);
    }
    Ok(Applied { options, echo })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn applied(configs: &[(&str, Option<&str>)]) -> Applied {
        apply(configs).expect("accepted")
    }

    fn refused(configs: &[(&str, Option<&str>)]) -> String {
        apply(configs).expect_err("refused")
    }

    /// The ordinary create: no configs at all. The bag is EMPTY, which is what
    /// makes a CreateTopics with no configs send exactly the body the
    /// auto-create path already sends.
    #[test]
    fn no_configs_is_an_empty_options_bag() {
        let a = applied(&[]);
        assert!(a.options.is_empty());
        // ...and the echo is still the two rows a describe would report, so a
        // client that reads the create response learns the same thing.
        assert_eq!(a.echo.len(), 2);
        assert_eq!(a.echo[0].name, CLEANUP_POLICY);
        assert_eq!(a.echo[0].value, CLEANUP_DELETE);
        assert_eq!(a.echo[1].name, MIN_INSYNC_REPLICAS);
        assert_eq!(a.echo[1].value, "1");
    }

    #[test]
    fn cleanup_policy_delete_is_a_no_op() {
        for spelling in ["delete", "DELETE", " delete "] {
            let a = applied(&[(CLEANUP_POLICY, Some(spelling))]);
            assert!(a.options.is_empty(), "{spelling}");
        }
        // A null value is Kafka's "unset": the default, which is delete.
        assert!(applied(&[(CLEANUP_POLICY, None)]).options.is_empty());
    }

    /// THE refusal, and the one whose message a user reads: Kafka Connect's
    /// internal topics are compacted, so this is where Connect stops.
    #[test]
    fn compaction_is_refused_loudly() {
        for policy in ["compact", "compact,delete", "delete,compact", "COMPACT"] {
            let why = refused(&[(CLEANUP_POLICY, Some(policy))]);
            assert!(
                why.contains("compaction") && why.contains(policy),
                "{policy}: {why}"
            );
        }
    }

    #[test]
    fn retention_minus_one_is_infinite_and_writes_the_default() {
        let a = applied(&[(RETENTION_MS, Some("-1"))]);
        assert_eq!(a.options["retentionEnabled"], json!(false));
        assert!(!a.options.contains_key("retentionSeconds"));
        assert_eq!(a.echo.last().unwrap().name, RETENTION_MS);
        assert_eq!(a.echo.last().unwrap().value, "-1");
    }

    /// Milliseconds to whole seconds, rounded DOWN, and echoed back at the
    /// resolution it was stored at rather than the one that was asked for.
    #[test]
    fn retention_is_rounded_down_to_whole_seconds() {
        for (ms, seconds) in [("1000", 1i64), ("604800000", 604_800), ("1999", 1)] {
            let a = applied(&[(RETENTION_MS, Some(ms))]);
            assert_eq!(a.options["retentionEnabled"], json!(true), "{ms}");
            assert_eq!(a.options["retentionSeconds"], json!(seconds), "{ms}");
            assert_eq!(
                a.echo.last().unwrap().value,
                (seconds * 1_000).to_string(),
                "{ms}"
            );
        }
    }

    /// A sub-second retention would round to zero, and a retention of zero
    /// seconds means "delete everything".
    #[test]
    fn a_sub_second_retention_is_refused_rather_than_rounded_to_zero() {
        for ms in ["0", "1", "999"] {
            let why = refused(&[(RETENTION_MS, Some(ms))]);
            assert!(why.contains("zero"), "{ms}: {why}");
        }
        // ...and so is a negative that is not Kafka's one sentinel.
        assert!(refused(&[(RETENTION_MS, Some("-2"))]).contains("infinite"));
        assert!(refused(&[(RETENTION_MS, Some("later"))]).contains("not a number"));
    }

    /// An unknown key is refused rather than dropped: dropping it tells the
    /// client it got a setting it did not get.
    #[test]
    fn an_unknown_config_is_refused_by_name() {
        for name in [
            "min.insync.replicas",
            "max.message.bytes",
            "segment.bytes",
            "compression.type",
        ] {
            let why = refused(&[(name, Some("2"))]);
            assert!(why.contains(name), "{name}: {why}");
        }
    }

    /// One bad key refuses the whole topic, and nothing is half-applied — the
    /// bag the caller would have sent is never built.
    #[test]
    fn one_bad_key_refuses_the_whole_topic() {
        assert!(apply(&[
            (RETENTION_MS, Some("86400000")),
            ("segment.ms", Some("1000")),
        ])
        .is_err());
    }

    /// The two enums are the numbers the wire carries; a renumbering here would
    /// silently change what every client renders.
    #[test]
    fn the_wire_numbers_are_kafkas_own() {
        assert_eq!(Source::Topic as i8, 1);
        assert_eq!(Source::StaticBroker as i8, 4);
        assert_eq!(Source::Default as i8, 5);
        assert_eq!(Kind::Boolean as i8, 1);
        assert_eq!(Kind::String as i8, 2);
        assert_eq!(Kind::Int as i8, 3);
        assert_eq!(Kind::Long as i8, 5);
    }
}
