//! The Kafka topic-config vocabulary, in ONE table read in both directions.
//!
//! CreateTopics WRITES a config (`handlers::create_topics`, through
//! `POST /api/v1/configure`), AlterConfigs and IncrementalAlterConfigs REWRITE
//! one (`handlers::alter_configs`, `handlers::incremental_alter_configs`) and
//! DescribeConfigs READS one back (`handlers::describe_configs`). Those must not
//! disagree about what a config NAME means here, so none of them owns the
//! vocabulary: this module does, and all four go through it.
//!
//! ## The rule that makes the read side short
//!
//! **A key is reported only when the facade can name the thing that enforces its
//! value, or the topic's own record of what a client SET it to; every other key
//! is omitted.** Omission is protocol-legal — a DescribeConfigs resource result
//! carries the configs it has, not a fixed set — and it is the only answer that
//! cannot mislead. The alternative, reporting a plausible Kafka default for a
//! knob nothing here honours, would tell a client it has a durability or
//! retention setting it does not have.
//!
//! That rule bites hardest on `retention.ms`. Queen exposes NO HTTP read of a
//! queue's configuration: `GET /api/v1/resources/queues/:queue` answers
//! id/name/namespace/task/createdAt/partitions/messages and no config at all,
//! and `GET /api/v1/status/queues/:queue` answers a `config` object carrying
//! leaseTime, retryLimit, retryDelay, ttl, maxQueueSize and deadLetterQueue —
//! and NOT `retentionEnabled`/`retentionSeconds`, which is the one pair
//! `retention.ms` maps to.
//!
//! The round trip is closed anyway, and NOT by inventing the value: the facade
//! keeps its own record of the bag it last posted for a topic
//! ([`crate::topic_record`]), and a describe reports retention from that record
//! for the topics it has one for. A topic this facade did not create has no
//! record and is answered as it always was — the key omitted rather than
//! guessed at.
//!
//! ## Three kinds of key (2026-10-01)
//!
//! Kafka Streams, Kafka Connect and every provisioner create their topics WITH
//! configs, and refuse to run when the create is refused: Streams' repartition
//! topics carry `segment.bytes`, `retention.ms=-1` and
//! `message.timestamp.type=CreateTime`, its changelogs `cleanup.policy=compact`
//! (and `compact,delete` with a retention when windowed), and Connect creates
//! its three internal topics compacted and then CHECKS, through
//! DescribeConfigs, that they are exactly `compact`. So the vocabulary is
//! wider than what Queen enforces, and each key is one of three kinds:
//!
//!   * **Mapped onto Queen** — `retention.ms` becomes the queue's retention
//!     options, and `cleanup.policy` decides whether retention may delete at
//!     all ([`settle`]).
//!   * **True of every topic here**, so a client may set exactly that value —
//!     `message.timestamp.type=CreateTime` (a fetched record carries the
//!     producer's own timestamp, [`crate::records`]), `min.insync.replicas` up
//!     to what every acknowledged write already has.
//!   * **Recorded** ([`RECORDED`]) — layout and compaction knobs nothing in
//!     Queen has an equivalent of: accepted in Kafka's own range, kept on the
//!     topic's record and reported back as set, TOPIC-sourced, with a
//!     `documentation` line that says in words that nothing enforces it. None
//!     of them can cost a reader a record — a segment size, a cleaner ratio, a
//!     compaction lag under compaction that removes nothing — and
//!     `retention.bytes` and `max.message.bytes` bound the topic from ABOVE:
//!     not enforcing them keeps more than asked, and admits what Queen's own
//!     request ceiling admits.
//!
//! Every other name is still refused by name, and so is a value outside
//! Kafka's own range: silently dropping a key would be telling a client it got
//! a setting it did not get.
//!
//! ## Compaction, phase 1: keep forever
//!
//! `cleanup.policy=compact` is accepted, and what the facade does with it is
//! the one thing compaction GUARANTEES a reader: the last value of every key
//! is in the log. It keeps EVERY value, because nothing compacts a Queen queue
//! — retention is switched off on the queue whatever `retention.ms` says while
//! `delete` is not in the policy, so a compacted topic never loses a record.
//! Readers that replay a compacted topic (a Streams changelog, Connect's
//! config, offset and status topics) rebuild the same state from the whole log
//! as from a compacted one; they read more, and the topic grows without bound.
//! `compact,delete` keeps the retention: records older than `retention.ms` are
//! deleted, nothing within it is compacted. Real compaction, as a generic Queen
//! capability, is designed in `COMPACTION.md` beside this crate.
//!
//! ## `read_only` is per row
//!
//! It was a module constant `true` for as long as AlterConfigs was not
//! advertised. It is now a field on [`Reported`], because the truth differs by
//! row: on a topic this facade does not track every row is read-only, because
//! an alter of it is refused; on one it tracks every row is writable, because
//! an alter of it lands on the record. A UI acting on this flag is still being
//! told the truth, which is the property the constant was written to keep.

use serde_json::{json, Map, Value};

/// Kafka's `cleanup.policy`.
pub const CLEANUP_POLICY: &str = "cleanup.policy";
/// Kafka's `retention.ms`.
pub const RETENTION_MS: &str = "retention.ms";
/// Kafka's `min.insync.replicas`.
pub const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
/// Kafka's `message.timestamp.type`.
pub const MESSAGE_TIMESTAMP_TYPE: &str = "message.timestamp.type";

/// The default cleanup policy: retention deletes, nothing compacts.
pub const CLEANUP_DELETE: &str = "delete";
/// The policy a compacted topic names. Kept forever here: see the module
/// header.
pub const CLEANUP_COMPACT: &str = "compact";
/// The one `message.timestamp.type` this facade serves.
pub const CREATE_TIME: &str = "CreateTime";

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
/// Only the types this facade reports are named; a key whose type is not one
/// of these is a key this facade does not report.
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
    /// 6 — `min.cleanable.dirty.ratio`.
    Double = 6,
}

/// Nothing this facade reports is a credential, a password or a key, so the
/// flag is a constant rather than a per-row field that could one day be set
/// wrong for a row that is not sensitive either.
pub const IS_SENSITIVE: bool = false;

/// One config row, as both the describe path and the create echo answer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub name: &'static str,
    /// Rendered as Kafka renders it: every config value on the wire is a
    /// string, whatever its `kind` says it means.
    pub value: String,
    pub source: Source,
    pub kind: Kind,
    /// Whether this row can be changed through this facade. Per row; see the
    /// module header for why it stopped being a constant.
    pub read_only: bool,
    /// One line, answered only when the request set `include_documentation`
    /// (v3+). Written here rather than in the handler so the sentence and the
    /// value it explains cannot drift apart.
    pub documentation: &'static str,
}

impl Reported {
    /// A row nothing can change. Every row of an untracked topic and every
    /// broker row is this, and the constructor spells it so that a new row has
    /// to say which it is rather than inheriting a default.
    fn fixed(
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
            read_only: true,
            documentation,
        }
    }

    /// A row an alter can change.
    fn writable(
        name: &'static str,
        value: impl Into<String>,
        source: Source,
        kind: Kind,
        documentation: &'static str,
    ) -> Reported {
        Reported {
            read_only: false,
            ..Reported::fixed(name, value, source, kind, documentation)
        }
    }

    /// The same row, writable: what it is on a topic this facade tracks.
    fn into_writable(self) -> Reported {
        Reported {
            read_only: false,
            ..self
        }
    }
}

/// The TOPIC configs this facade can name the enforcer of, for any queue the
/// catalog has.
///
/// Two rows, and both are the truest sentence the facade can say about a topic
/// it has no record of: nothing compacts it, and there is one logical broker —
/// every Metadata answer already says `replicas=[0], isr=[0]`, so a tool
/// computing under-replication from `min.insync.replicas=1` is right. See the
/// module header for why the list is not longer.
pub fn topic_configs() -> Vec<Reported> {
    topic_configs_with(1)
}

/// [`topic_configs`] for a broker where every acknowledged write is on `isr`
/// replicas — a raft majority when the facade runs inside a raft broker
/// ([`crate::Facade::in_sync_replicas`]), 1 everywhere else.
pub fn topic_configs_with(isr: u32) -> Vec<Reported> {
    vec![default_policy(), default_min_insync(isr)]
}

fn default_policy() -> Reported {
    Reported::fixed(
        CLEANUP_POLICY,
        CLEANUP_DELETE,
        Source::Default,
        Kind::String,
        "`delete`: retention.ms, when one is set, deletes by age and nothing compacts. A topic \
         created or altered here with `compact` keeps every record instead.",
    )
}

fn default_min_insync(isr: u32) -> Reported {
    if isr > 1 {
        Reported::fixed(
            MIN_INSYNC_REPLICAS,
            isr.to_string(),
            Source::Default,
            Kind::Int,
            "The raft majority: every write this broker acknowledges is on this many nodes, \
             and none is acknowledged with fewer. Lower values are accepted and change nothing.",
        )
    } else {
        Reported::fixed(
            MIN_INSYNC_REPLICAS,
            "1",
            Source::Default,
            Kind::Int,
            "Always 1. The facade advertises one logical broker and Metadata reports \
             replicas=[0], isr=[0]; durability is the Queen broker's, not a replica count's.",
        )
    }
}

/// The `retention.ms` row a DESCRIBE reports for a topic this facade TRACKS
/// ([`crate::topic_record`]).
///
/// `seconds` is what the stored record says: `Some(n)` for retention enabled at
/// `n` seconds, `None` for retention off — which is the stored procedure's own
/// default (`retention_enabled = false`, 012_configure.sql) and IS Kafka's -1.
///
/// It is separate from what [`apply`] echoes, and deliberately so: the echo
/// answers "this is what your create just applied", which is a TOPIC-sourced
/// claim, while this answers "this is what is in force", where an absent
/// retention key is the default and says so. Both are read out of the same
/// record, so neither can drift from the other's value.
///
/// `read_only` is false: the record is what an alter merges onto, so on a
/// tracked topic this row really can be changed here.
pub fn reported_retention(seconds: Option<i64>) -> Reported {
    match seconds {
        Some(seconds) => Reported::writable(
            RETENTION_MS,
            (seconds * 1_000).to_string(),
            Source::Topic,
            Kind::Int,
            "Read from the record this facade keeps of the configuration it last applied to \
             this topic. Queen's retention is in whole seconds, so the value is reported at \
             the resolution it was stored at. A retention changed outside this facade — the \
             Queen console, another SDK — is not visible here.",
        ),
        None => Reported::writable(
            RETENTION_MS,
            "-1",
            Source::Default,
            Kind::Int,
            "-1 is Kafka's infinite retention and is Queen's default: this facade created the \
             queue and did not enable retention on it, so nothing expires. DEFAULT rather than \
             TOPIC because nobody set it.",
        ),
    }
}

/// What one topic's `configs[]` asked for, once it is understood.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// The options bag `POST /api/v1/configure` takes. Empty when the request
    /// carried nothing this facade acts on, which is the ordinary case and is
    /// byte-identical to what the auto-create path already sends.
    pub options: Map<String, Value>,
    /// The Kafka-side values the topic's record keeps beside the bag
    /// ([`crate::topic_record::Record::kafka`]): what a later describe reports
    /// for every key that is not a `/configure` option.
    pub kafka: Map<String, Value>,
    /// The configs this create actually applied, for the CreateTopics v5+
    /// response.
    ///
    /// It carries what a describe of the new topic will report, and the
    /// `retention.ms` this call set, which is the one place it can be answered
    /// at all for a topic whose record failed to land — see the module header.
    pub echo: Vec<Reported>,
}

/// One entry of an options-bag delta: `Some(v)` writes the key,
/// `None` REMOVES it so the stored procedure's own default is what takes
/// effect.
///
/// The removal half is what makes IncrementalAlterConfigs' DELETE lossless: a
/// key dropped from the bag reads back exactly as a key the facade never set,
/// which is the state [`crate::topic_record`]'s invariant is written in terms
/// of.
pub type Delta = Vec<(String, Option<Value>)>;

/// What one config does to the two maps a topic's record keeps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Change {
    /// The `/configure` options bag: what Queen enforces.
    pub queen: Delta,
    /// The Kafka-side values ([`crate::topic_record::Record::kafka`]): what is
    /// reported for the keys that are not options.
    pub kafka: Delta,
}

impl Change {
    fn kafka(name: &str, value: Option<String>) -> Change {
        Change {
            queen: Vec::new(),
            kafka: vec![(name.to_string(), value.map(Value::String))],
        }
    }
}

/// The `/configure` key for whether retention runs at all.
pub const RETENTION_ENABLED: &str = "retentionEnabled";
/// The `/configure` key for the retention window, in WHOLE SECONDS.
pub const RETENTION_SECONDS: &str = "retentionSeconds";

/// A key this facade RECORDS: accepted in Kafka's own range, kept on the
/// topic's record, reported back as set. See the module header.
pub struct Recorded {
    pub name: &'static str,
    pub kind: Kind,
    /// Kafka's own validator for the value (`LogConfig`'s `ConfigDef`), in
    /// words that go into the refusal.
    range: &'static str,
    /// The value, normalised, or `None` when it is outside `range`.
    check: fn(&str) -> Option<String>,
    pub documentation: &'static str,
}

fn int_at_least(v: &str, min: i64) -> Option<String> {
    v.parse::<i32>()
        .ok()
        .filter(|n| i64::from(*n) >= min)
        .map(|n| n.to_string())
}

fn long_at_least(v: &str, min: i64) -> Option<String> {
    v.parse::<i64>()
        .ok()
        .filter(|n| *n >= min)
        .map(|n| n.to_string())
}

/// The recorded keys. Each is one a framework sets on a topic it creates, or
/// one a provisioner commonly carries, and none has a Queen mechanism behind
/// it — so each is accepted in Kafka's range, kept, and documented as kept.
pub const RECORDED: &[Recorded] = &[
    Recorded {
        name: "segment.bytes",
        kind: Kind::Int,
        range: "an int of at least 14",
        check: |v| int_at_least(v, 14),
        documentation: "Recorded as set; not enforced. Queen's queue log sizes its own segment \
                        files, so this describes no file here. It changes nothing a reader sees.",
    },
    Recorded {
        name: "segment.ms",
        kind: Kind::Long,
        range: "a long of at least 1",
        check: |v| long_at_least(v, 1),
        documentation: "Recorded as set; not enforced. Queen's queue log rolls its own segment \
                        files. It changes nothing a reader sees.",
    },
    Recorded {
        name: "retention.bytes",
        kind: Kind::Long,
        range: "a long (-1 is unlimited)",
        check: |v| v.parse::<i64>().ok().map(|n| n.to_string()),
        documentation: "Recorded as set; not enforced. Queen's retention is by age only \
                        (retention.ms), so a byte cap above -1 is not applied and the topic can \
                        hold more than it.",
    },
    Recorded {
        name: "max.message.bytes",
        kind: Kind::Int,
        range: "an int of at least 0",
        check: |v| int_at_least(v, 0),
        documentation: "Recorded as set; not enforced per topic. What a produce meets is the \
                        broker's request ceiling (QUEEN_MAX_BODY_BYTES) and this facade's frame \
                        ceiling, whatever this says.",
    },
    Recorded {
        name: "min.compaction.lag.ms",
        kind: Kind::Long,
        range: "a long of at least 0",
        check: |v| long_at_least(v, 0),
        documentation: "Recorded as set. Nothing here compacts (a compacted topic keeps every \
                        record), so every record outlives any compaction lag.",
    },
    Recorded {
        name: "max.compaction.lag.ms",
        kind: Kind::Long,
        range: "a long of at least 1",
        check: |v| long_at_least(v, 1),
        documentation: "Recorded as set; not enforced. Nothing here compacts (a compacted topic \
                        keeps every record), so no record is ever compacted away, late or early.",
    },
    Recorded {
        name: "delete.retention.ms",
        kind: Kind::Long,
        range: "a long of at least 0",
        check: |v| long_at_least(v, 0),
        documentation: "Recorded as set; not enforced. Nothing here compacts, so a tombstone is \
                        kept like every other record and a reader replaying the topic meets it.",
    },
    Recorded {
        name: "min.cleanable.dirty.ratio",
        kind: Kind::Double,
        range: "a double between 0 and 1",
        check: |v| {
            v.parse::<f64>()
                .ok()
                .filter(|r| (0.0..=1.0).contains(r))
                .map(|r| r.to_string())
        },
        documentation: "Recorded as set; not enforced. There is no log cleaner here: nothing \
                        compacts, so no ratio triggers one.",
    },
];

/// The recorded key called `name`, if it is one.
pub fn recorded(name: &str) -> Option<&'static Recorded> {
    RECORDED.iter().find(|r| r.name == name)
}

/// The policies a `cleanup.policy` value names, lowercased, in order, once
/// each, or the reason it is not one.
fn parse_policy(v: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for p in v.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let p = p.to_ascii_lowercase();
        if p != CLEANUP_DELETE && p != CLEANUP_COMPACT {
            return Err(format!(
                "{CLEANUP_POLICY}={v} is not a cleanup policy: the policies are \
                 `{CLEANUP_DELETE}` and `{CLEANUP_COMPACT}`, alone or together"
            ));
        }
        if !out.contains(&p) {
            out.push(p);
        }
    }
    Ok(out)
}

/// The policies a topic's Kafka-side values say it has: what was set, or
/// `[delete]`, the default, when nothing was.
pub fn policies(kafka: &Map<String, Value>) -> Vec<String> {
    kafka
        .get(CLEANUP_POLICY)
        .and_then(Value::as_str)
        .and_then(|v| parse_policy(v).ok())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| vec![CLEANUP_DELETE.to_string()])
}

/// Whether a topic compacts and does NOT delete: the one policy under which
/// retention must not run, because a compacted topic keeps the last value of
/// every key and Queen's retention deletes by age, whatever the key.
pub fn compacts_only(kafka: &Map<String, Value>) -> bool {
    !policies(kafka).iter().any(|p| p == CLEANUP_DELETE)
}

/// What one `(name, value)` pair does, or the reason it is refused
/// INVALID_CONFIG.
///
/// This is the whole alter vocabulary and it is the SAME one [`apply`] uses, so
/// a create and an alter cannot come to disagree about what a key means. A
/// `None` value is Kafka's "unset, use the default", which removes what was
/// set rather than being an error.
///
/// What it answers is a CHANGE, not a state: [`settle`] is what makes the
/// state of a whole topic consistent, and every path runs it after the last
/// change of a request.
pub fn alter(name: &str, value: Option<&str>) -> Result<Change, String> {
    alter_with(name, value, 1)
}

/// [`alter`] on a broker whose acknowledged writes are on `isr` replicas
/// ([`topic_configs_with`]): `min.insync.replicas` is accepted up to `isr`.
pub fn alter_with(name: &str, value: Option<&str>, isr: u32) -> Result<Change, String> {
    let Some(v) = value.map(str::trim) else {
        return reset(name);
    };
    match name {
        CLEANUP_POLICY => {
            let policies = parse_policy(v)?;
            if policies.is_empty() {
                // An EMPTY policy, which is what `SUBTRACT delete` computes.
                // Kafka will not have a topic with no cleanup policy either.
                return Err(format!(
                    "{CLEANUP_POLICY} cannot be emptied: a topic deletes, compacts, or both, \
                     and there is no absence of a policy"
                ));
            }
            // `delete` alone is the default, and is stored as the absence of a
            // value, so a topic that never named one and a topic that named the
            // default read back the same.
            if policies == [CLEANUP_DELETE] {
                return Ok(Change::kafka(CLEANUP_POLICY, None));
            }
            Ok(Change::kafka(CLEANUP_POLICY, Some(policies.join(","))))
        }
        // Up to what every acknowledged write already has, and RECORDED: a
        // client that sets 1 on a broker of three is told 1, which is true —
        // every write is on at least one in-sync replica — and the default it
        // replaces says how many it is really on. The default itself is stored
        // as the absence of a value.
        MIN_INSYNC_REPLICAS => match v.parse::<u32>() {
            Ok(n) if n == isr => Ok(Change::kafka(MIN_INSYNC_REPLICAS, None)),
            Ok(n) if (1..=isr).contains(&n) => {
                Ok(Change::kafka(MIN_INSYNC_REPLICAS, Some(n.to_string())))
            }
            _ if isr > 1 => Err(format!(
                "{MIN_INSYNC_REPLICAS}={v} cannot be honoured: every write this broker \
                 acknowledges is on a raft majority of {isr} nodes, so 1 to {isr} is what is \
                 in force. A higher number would report a durability setting that is not"
            )),
            _ => Err(format!(
                "{MIN_INSYNC_REPLICAS}={v} cannot be honoured: this facade advertises ONE \
                 logical broker and every Metadata answer says replicas=[0], isr=[0], so the \
                 only in-sync replica count there is is 1. Durability here is the Queen \
                 broker's, not a replica count's. Accepting a higher number would report a \
                 durability setting that is not in force"
            )),
        },
        // CreateTime is what a fetched record carries: the producer's own
        // timestamp, kept in the envelope (`crate::records`). LogAppendTime
        // would tell a consumer it reads the broker's clock, and it would read
        // the producer's.
        MESSAGE_TIMESTAMP_TYPE if v == CREATE_TIME => Ok(Change::kafka(
            MESSAGE_TIMESTAMP_TYPE,
            Some(CREATE_TIME.into()),
        )),
        MESSAGE_TIMESTAMP_TYPE => Err(format!(
            "{MESSAGE_TIMESTAMP_TYPE}={v} is not supported: a record fetched through this facade \
             carries the producer's timestamp, so `{CREATE_TIME}` is the one timestamp type \
             there is. `LogAppendTime` would tell a consumer it reads the broker's clock"
        )),
        RETENTION_MS => {
            let ms: i64 = v
                .parse()
                .map_err(|_| format!("{RETENTION_MS}={v} is not a number of milliseconds"))?;
            let queen = match ms {
                // Kafka's "infinite", and the facade's default. The window
                // is cleared as well as disabled, so the bag says one thing
                // about retention rather than two.
                -1 => vec![
                    (RETENTION_ENABLED.to_string(), Some(json!(false))),
                    (RETENTION_SECONDS.to_string(), None),
                ],
                // Queen's retention is in SECONDS, so a sub-second window
                // cannot be expressed. Rounding it down would reach zero,
                // and a retention of zero seconds means "delete everything"
                // — refusing is the only answer that is not a data-loss
                // surprise.
                0..=999 => {
                    return Err(format!(
                        "{RETENTION_MS}={ms} is below the one second Queen's retention \
                         can express (it is configured in whole seconds); rounding it \
                         down would reach zero, which deletes everything"
                    ))
                }
                // Rounded DOWN, so the facade never retains less than the
                // client asked for by mistake — it retains at most one
                // second less than asked, never more.
                ms if ms >= 1_000 => vec![
                    (RETENTION_ENABLED.to_string(), Some(json!(true))),
                    (RETENTION_SECONDS.to_string(), Some(json!(ms / 1_000))),
                ],
                // Everything below -1. Kafka defines exactly one negative
                // value and this is not it.
                _ => {
                    return Err(format!(
                        "{RETENTION_MS}={ms} is not a retention: -1 is infinite and \
                         every other value must be a non-negative number of milliseconds"
                    ))
                }
            };
            // A retention kept while the topic only compacted is replaced by
            // this one, whatever the policy is now ([`settle`]).
            Ok(Change {
                queen,
                kafka: vec![(RETENTION_MS.to_string(), None)],
            })
        }
        other => match recorded(other) {
            Some(r) => match (r.check)(v) {
                Some(normalised) => Ok(Change::kafka(r.name, Some(normalised))),
                None => Err(format!(
                    "{other}={v} is outside the range Kafka itself accepts for it: {}",
                    r.range
                )),
            },
            None => Err(unknown(other)),
        },
    }
}

/// The refusal of a name the vocabulary does not have.
fn unknown(name: &str) -> String {
    let recorded: Vec<&str> = RECORDED.iter().map(|r| r.name).collect();
    format!(
        "`{name}` is not a topic config this facade understands. It accepts \
         `{CLEANUP_POLICY}`, `{RETENTION_MS}`, `{MIN_INSYNC_REPLICAS}`, \
         `{MESSAGE_TIMESTAMP_TYPE}={CREATE_TIME}` and, recorded as set, {}; every other \
         Kafka topic config names a mechanism Queen does not have, and accepting one silently \
         would report a setting that is not in force",
        recorded.join(", ")
    )
}

/// What resetting one key to its default does — Kafka's `AlterConfigOp`
/// DELETE, and the unnamed half of AlterConfigs' full replacement.
///
/// `retention.ms` drops out of the bag entirely, which leaves
/// `configure_queue_v1`'s own default (`retention_enabled = false`) in force —
/// and that IS Kafka's -1. Every other key drops out of the Kafka-side values,
/// which is what reports it as a default again. An unknown key is refused with
/// the same sentence [`alter`] refuses it with, because a client deleting a
/// key this facade never had should learn the same thing as one setting it.
pub fn reset(name: &str) -> Result<Change, String> {
    match name {
        RETENTION_MS => Ok(Change {
            queen: vec![
                (RETENTION_ENABLED.to_string(), None),
                (RETENTION_SECONDS.to_string(), None),
            ],
            kafka: vec![(RETENTION_MS.to_string(), None)],
        }),
        CLEANUP_POLICY | MIN_INSYNC_REPLICAS | MESSAGE_TIMESTAMP_TYPE => {
            Ok(Change::kafka(name, None))
        }
        other => match recorded(other) {
            Some(r) => Ok(Change::kafka(r.name, None)),
            None => Err(unknown(other)),
        },
    }
}

/// Apply a delta to one map in place. One place, so that "a `None` removes
/// the key" cannot be implemented two ways.
pub fn absorb(options: &mut Map<String, Value>, delta: &Delta) {
    for (name, value) in delta {
        match value {
            Some(v) => {
                options.insert(name.clone(), v.clone());
            }
            None => {
                options.remove(name);
            }
        }
    }
}

/// Apply a [`Change`] to a topic's two maps.
pub fn absorb_change(
    options: &mut Map<String, Value>,
    kafka: &mut Map<String, Value>,
    change: &Change,
) {
    absorb(options, &change.queen);
    absorb(kafka, &change.kafka);
}

/// Make a topic's two maps say ONE thing about whether records may expire.
/// Every write path runs this after the last change of a request and before
/// it compares or posts anything.
///
/// **A topic that compacts and does not delete never loses a record**: the
/// bag says `retentionEnabled: false`, EXPLICITLY — not as an absent key a
/// merge would leave at whatever the queue had — and a retention the request
/// or the record carried is kept beside it, in the Kafka-side values, where a
/// describe reports it as set and not applied. That is Kafka's own behaviour:
/// `retention.ms` is stored on a compacted topic and ignored until `delete`
/// joins the policy. When `delete` does, the kept retention is in force again.
pub fn settle(options: &mut Map<String, Value>, kafka: &mut Map<String, Value>) {
    if compacts_only(kafka) {
        if options.get(RETENTION_ENABLED) == Some(&json!(true)) {
            if let Some(seconds) = options.get(RETENTION_SECONDS).and_then(Value::as_i64) {
                kafka.insert(
                    RETENTION_MS.to_string(),
                    Value::String((seconds * 1_000).to_string()),
                );
            }
        }
        options.insert(RETENTION_ENABLED.to_string(), json!(false));
        options.remove(RETENTION_SECONDS);
    } else if let Some(kept) = kafka.remove(RETENTION_MS) {
        if let Some(Ok(change)) = kept.as_str().map(|ms| alter(RETENTION_MS, Some(ms))) {
            absorb(options, &change.queen);
        }
    }
}

/// Every row a DESCRIBE reports for a topic this facade TRACKS: its policy,
/// its in-sync count, its retention, and every recorded key it carries — each
/// writable, because an alter of it lands on the record.
pub fn described(
    isr: u32,
    options: &Map<String, Value>,
    kafka: &Map<String, Value>,
) -> Vec<Reported> {
    let mut rows = vec![policy_row(kafka), min_insync_row(isr, kafka)];
    rows.push(retention_row(options, kafka));
    rows.extend(set_rows(kafka));
    rows
}

/// The `cleanup.policy` row of a tracked topic.
fn policy_row(kafka: &Map<String, Value>) -> Reported {
    match kafka.get(CLEANUP_POLICY).and_then(Value::as_str) {
        Some(set) if compacts_only(kafka) => Reported::writable(
            CLEANUP_POLICY,
            set,
            Source::Topic,
            Kind::String,
            "Compacted by KEEPING EVERY RECORD: retention never runs on this topic, so the last \
             value of every key is always in the log, beside every earlier one. A reader that \
             replays it rebuilds the same state; the topic grows without bound.",
        ),
        Some(set) => Reported::writable(
            CLEANUP_POLICY,
            set,
            Source::Topic,
            Kind::String,
            "Compact and delete: retention.ms deletes by age, and nothing within it is compacted \
             away — every record younger than the retention is kept.",
        ),
        None => default_policy().into_writable(),
    }
}

/// The `min.insync.replicas` row of a tracked topic.
fn min_insync_row(isr: u32, kafka: &Map<String, Value>) -> Reported {
    match kafka.get(MIN_INSYNC_REPLICAS).and_then(Value::as_str) {
        Some(set) => Reported::writable(
            MIN_INSYNC_REPLICAS,
            set,
            Source::Topic,
            Kind::Int,
            "As set, and true: every write this broker acknowledges is on at least this many \
             nodes. The broker default says how many it really is on.",
        ),
        None => default_min_insync(isr).into_writable(),
    }
}

/// The `retention.ms` row of a tracked topic: the retention in force, or the
/// one a compacted topic keeps and does not apply.
fn retention_row(options: &Map<String, Value>, kafka: &Map<String, Value>) -> Reported {
    if compacts_only(kafka) {
        return match kafka.get(RETENTION_MS).and_then(Value::as_str) {
            Some(kept) => Reported::writable(
                RETENTION_MS,
                kept,
                Source::Topic,
                Kind::Int,
                "As set, and NOT applied: the topic compacts and does not delete, so it keeps \
                 every record. It comes into force if `delete` joins cleanup.policy.",
            ),
            None => reported_retention(None),
        };
    }
    // A record whose `retentionEnabled` is true but which carries no
    // `retentionSeconds` is read as retention OFF rather than as some invented
    // window — [`alter`] only ever writes the pair together, so such a record
    // did not come from this facade's vocabulary.
    let enabled = options
        .get(RETENTION_ENABLED)
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let seconds = enabled
        .then(|| options.get(RETENTION_SECONDS).and_then(Value::as_i64))
        .flatten();
    reported_retention(seconds)
}

/// One TOPIC-sourced row per key the topic's record carries beyond the three
/// rows that are always reported, in a fixed order.
fn set_rows(kafka: &Map<String, Value>) -> Vec<Reported> {
    let mut rows = Vec::new();
    if let Some(v) = kafka.get(MESSAGE_TIMESTAMP_TYPE).and_then(Value::as_str) {
        rows.push(Reported::writable(
            MESSAGE_TIMESTAMP_TYPE,
            v,
            Source::Topic,
            Kind::String,
            "As set, and true of every topic here: a fetched record carries the producer's own \
             timestamp. A timestamp lookup (ListOffsets) is answered on the broker's APPEND \
             clock, which the producer's agrees with when it stamps records as it sends them.",
        ));
    }
    for r in RECORDED {
        if let Some(v) = kafka.get(r.name).and_then(Value::as_str) {
            rows.push(Reported::writable(
                r.name,
                v,
                Source::Topic,
                r.kind,
                r.documentation,
            ));
        }
    }
    rows
}

/// Turn one topic's requested `configs[]` into what Queen is told, or into the
/// reason the whole topic is refused INVALID_CONFIG.
///
/// The vocabulary is [`alter`]'s, in full, and the state is [`settle`]d once
/// every config is in. What this adds is the CreateTopics v5+ echo, which
/// answers "this is what your create applied" and is therefore TOPIC-sourced
/// for retention either way — unlike [`reported_retention`], which answers
/// "this is what is in force" and calls an unset retention what it is.
pub fn apply(configs: &[(&str, Option<&str>)]) -> Result<Applied, String> {
    apply_with(configs, 1)
}

/// [`apply`] on a broker whose acknowledged writes are on `isr` replicas
/// ([`topic_configs_with`]).
pub fn apply_with(configs: &[(&str, Option<&str>)], isr: u32) -> Result<Applied, String> {
    let mut options = Map::new();
    let mut kafka = Map::new();
    let mut named_retention = false;

    for (name, value) in configs {
        absorb_change(&mut options, &mut kafka, &alter_with(name, *value, isr)?);
        named_retention |= *name == RETENTION_MS && value.is_some();
    }
    settle(&mut options, &mut kafka);

    // What a describe of the new topic will report — writable, because the
    // record this create writes is what makes an alter of it land — with the
    // retention row in the create's own words when the create named one.
    let mut echo: Vec<Reported> = described(isr, &options, &kafka)
        .into_iter()
        .filter(|r| r.name != RETENTION_MS)
        .collect();
    if named_retention && !compacts_only(&kafka) {
        echo.insert(
            2,
            match options.get(RETENTION_SECONDS).and_then(|s| s.as_i64()) {
                Some(seconds) => Reported::writable(
                    RETENTION_MS,
                    (seconds * 1_000).to_string(),
                    Source::Topic,
                    Kind::Int,
                    "Queen's retention is configured in whole seconds, so the value asked for is \
                     rounded down to the second and reported back at the resolution it was \
                     actually stored at.",
                ),
                None => Reported::writable(
                    RETENTION_MS,
                    "-1",
                    Source::Topic,
                    Kind::Int,
                    "-1 is Kafka's infinite retention and is this facade's default: a Queen queue \
                     created here has retention disabled.",
                ),
            },
        );
    } else if compacts_only(&kafka) && kafka.contains_key(RETENTION_MS) {
        echo.insert(2, retention_row(&options, &kafka));
    }
    Ok(Applied {
        options,
        kafka,
        echo,
    })
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

    fn row<'a>(rows: &'a [Reported], name: &str) -> &'a Reported {
        rows.iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("{name} is not reported: {rows:?}"))
    }

    /// The ordinary create: no configs at all. The bag is EMPTY, which is what
    /// makes a CreateTopics with no configs send exactly the body the
    /// auto-create path already sends.
    #[test]
    fn no_configs_is_an_empty_options_bag() {
        let a = applied(&[]);
        assert!(a.options.is_empty());
        assert!(a.kafka.is_empty());
        // ...and the echo is still the two rows a describe would report, so a
        // client that reads the create response learns the same thing.
        assert_eq!(a.echo.len(), 2);
        assert_eq!(a.echo[0].name, CLEANUP_POLICY);
        assert_eq!(a.echo[0].value, CLEANUP_DELETE);
        assert_eq!(a.echo[1].name, MIN_INSYNC_REPLICAS);
        assert_eq!(a.echo[1].value, "1");
    }

    /// Inside a raft broker of three, every acknowledged write is on two
    /// nodes: `min.insync.replicas` up to 2 is accepted and reported, 3 is
    /// refused because nothing waits for a third.
    #[test]
    fn min_insync_replicas_follows_the_raft_majority() {
        assert!(apply_with(&[(MIN_INSYNC_REPLICAS, Some("2"))], 2).is_ok());
        assert!(apply_with(&[(MIN_INSYNC_REPLICAS, Some("1"))], 2).is_ok());
        let refused = apply_with(&[(MIN_INSYNC_REPLICAS, Some("3"))], 2).expect_err("3 > 2");
        assert!(refused.contains("raft majority"), "{refused}");
        let echo = apply_with(&[], 2).unwrap().echo;
        assert_eq!(echo[1].name, MIN_INSYNC_REPLICAS);
        assert_eq!(echo[1].value, "2");
        // ...and a facade with no raft behind it is unchanged.
        assert!(apply(&[(MIN_INSYNC_REPLICAS, Some("2"))]).is_err());
    }

    /// A client that sets `min.insync.replicas` below the majority is told
    /// what it set — it is true, every write is on at least that many — and
    /// one that sets the majority itself reads the default back.
    #[test]
    fn a_lower_min_insync_replicas_is_recorded_and_reported_as_set() {
        let a = apply_with(&[(MIN_INSYNC_REPLICAS, Some("1"))], 2).unwrap();
        assert!(a.options.is_empty(), "nothing for Queen to do");
        assert_eq!(a.kafka[MIN_INSYNC_REPLICAS], json!("1"));
        let r = row(&a.echo, MIN_INSYNC_REPLICAS);
        assert_eq!((r.value.as_str(), r.source), ("1", Source::Topic));
        let a = apply_with(&[(MIN_INSYNC_REPLICAS, Some("2"))], 2).unwrap();
        assert!(a.kafka.is_empty());
    }

    #[test]
    fn cleanup_policy_delete_is_a_no_op() {
        for spelling in ["delete", "DELETE", " delete "] {
            let a = applied(&[(CLEANUP_POLICY, Some(spelling))]);
            assert!(a.options.is_empty(), "{spelling}");
            assert!(a.kafka.is_empty(), "{spelling}");
        }
        // A null value is Kafka's "unset": the default, which is delete.
        assert!(applied(&[(CLEANUP_POLICY, None)]).options.is_empty());
    }

    /// THE acceptance Kafka Connect and Kafka Streams need: a compacted topic
    /// is created, it reports `compact` back — Connect checks exactly that —
    /// and retention is OFF on its queue, explicitly, so it never loses a
    /// record. Phase 1 of compaction: keep everything.
    #[test]
    fn compaction_keeps_every_record_and_reports_compact() {
        for (asked, stored) in [
            ("compact", "compact"),
            ("COMPACT", "compact"),
            (" compact , compact ", "compact"),
        ] {
            let a = applied(&[(CLEANUP_POLICY, Some(asked))]);
            assert_eq!(a.options[RETENTION_ENABLED], json!(false), "{asked}");
            assert_eq!(a.kafka[CLEANUP_POLICY], json!(stored), "{asked}");
            let r = row(&a.echo, CLEANUP_POLICY);
            assert_eq!((r.value.as_str(), r.source), (stored, Source::Topic));
            assert!(!r.read_only);
        }
    }

    /// `compact` with a retention: Kafka stores the retention and ignores it
    /// on a topic that does not delete, and so does this — the queue keeps
    /// every record, and the retention is reported as set and not applied.
    /// The order of the two configs in the request does not matter.
    #[test]
    fn a_retention_on_a_compacted_topic_is_kept_and_not_applied() {
        for configs in [
            [
                (CLEANUP_POLICY, Some("compact")),
                (RETENTION_MS, Some("60000")),
            ],
            [
                (RETENTION_MS, Some("60000")),
                (CLEANUP_POLICY, Some("compact")),
            ],
        ] {
            let a = applied(&configs);
            assert_eq!(a.options[RETENTION_ENABLED], json!(false));
            assert!(!a.options.contains_key(RETENTION_SECONDS));
            assert_eq!(a.kafka[RETENTION_MS], json!("60000"));
            let r = row(&described(1, &a.options, &a.kafka), RETENTION_MS).clone();
            assert_eq!((r.value.as_str(), r.source), ("60000", Source::Topic));
        }
    }

    /// `compact,delete` — a windowed Streams changelog — keeps its retention:
    /// records older than it are deleted, nothing younger is compacted.
    #[test]
    fn compact_and_delete_keeps_the_retention_in_force() {
        let a = applied(&[
            (CLEANUP_POLICY, Some("compact,delete")),
            (RETENTION_MS, Some("86400000")),
        ]);
        assert_eq!(a.options[RETENTION_ENABLED], json!(true));
        assert_eq!(a.options[RETENTION_SECONDS], json!(86_400));
        assert_eq!(a.kafka[CLEANUP_POLICY], json!("compact,delete"));
        assert!(!a.kafka.contains_key(RETENTION_MS));
        assert!(!compacts_only(&a.kafka));
    }

    /// The policy changes, and the retention follows it both ways: a
    /// compacted topic's kept retention comes into force when `delete` joins
    /// the policy, and an enabled one is kept aside when `delete` leaves it.
    #[test]
    fn the_retention_follows_the_policy_both_ways() {
        let mut options = Map::new();
        let mut kafka = Map::new();
        for (name, value) in [(RETENTION_MS, "60000"), (CLEANUP_POLICY, "compact")] {
            absorb_change(&mut options, &mut kafka, &alter(name, Some(value)).unwrap());
        }
        settle(&mut options, &mut kafka);
        assert_eq!(options[RETENTION_ENABLED], json!(false));
        assert_eq!(kafka[RETENTION_MS], json!("60000"));

        absorb_change(
            &mut options,
            &mut kafka,
            &alter(CLEANUP_POLICY, Some("delete")).unwrap(),
        );
        settle(&mut options, &mut kafka);
        assert_eq!(options[RETENTION_ENABLED], json!(true));
        assert_eq!(options[RETENTION_SECONDS], json!(60));
        assert!(kafka.is_empty(), "{kafka:?}");

        // And a retention set to -1 while compacted forgets the kept one.
        absorb_change(
            &mut options,
            &mut kafka,
            &alter(CLEANUP_POLICY, Some("compact")).unwrap(),
        );
        settle(&mut options, &mut kafka);
        absorb_change(
            &mut options,
            &mut kafka,
            &alter(RETENTION_MS, Some("-1")).unwrap(),
        );
        settle(&mut options, &mut kafka);
        assert!(!kafka.contains_key(RETENTION_MS));
        absorb_change(&mut options, &mut kafka, &reset(CLEANUP_POLICY).unwrap());
        settle(&mut options, &mut kafka);
        assert_eq!(options[RETENTION_ENABLED], json!(false));
    }

    #[test]
    fn a_policy_that_is_not_one_is_refused() {
        for policy in ["purge", "compact,purge", "delete;compact"] {
            let why = refused(&[(CLEANUP_POLICY, Some(policy))]);
            assert!(why.contains("not a cleanup policy"), "{policy}: {why}");
        }
    }

    /// Streams' repartition topic, exactly as `RepartitionTopicConfig` creates
    /// it, and its changelogs: every config accepted, every one reported back
    /// as set.
    #[test]
    fn the_configs_kafka_streams_creates_its_topics_with_are_accepted() {
        let repartition = applied(&[
            (CLEANUP_POLICY, Some("delete")),
            ("segment.bytes", Some("52428800")),
            (RETENTION_MS, Some("-1")),
            (MESSAGE_TIMESTAMP_TYPE, Some("CreateTime")),
        ]);
        assert_eq!(repartition.options[RETENTION_ENABLED], json!(false));
        let rows = described(1, &repartition.options, &repartition.kafka);
        assert_eq!(row(&rows, "segment.bytes").value, "52428800");
        assert_eq!(row(&rows, "segment.bytes").kind, Kind::Int);
        assert_eq!(row(&rows, MESSAGE_TIMESTAMP_TYPE).value, "CreateTime");

        let versioned = applied(&[
            (CLEANUP_POLICY, Some("compact")),
            ("min.compaction.lag.ms", Some("86400000")),
            (MESSAGE_TIMESTAMP_TYPE, Some("CreateTime")),
        ]);
        let rows = described(1, &versioned.options, &versioned.kafka);
        assert_eq!(row(&rows, "min.compaction.lag.ms").value, "86400000");
        assert_eq!(row(&rows, "min.compaction.lag.ms").kind, Kind::Long);
        assert_eq!(row(&rows, CLEANUP_POLICY).value, "compact");
    }

    /// Every recorded key is accepted in Kafka's range, normalised, reported
    /// TOPIC-sourced and writable — with a documentation line that says
    /// nothing enforces it — and refused outside the range by name.
    #[test]
    fn every_recorded_key_is_kept_in_range_and_refused_outside_it() {
        for (name, good, normal, bad) in [
            ("segment.bytes", "1073741824", "1073741824", "13"),
            ("segment.ms", "604800000", "604800000", "0"),
            ("retention.bytes", "-1", "-1", "lots"),
            ("max.message.bytes", "1048588", "1048588", "-1"),
            ("min.compaction.lag.ms", "0", "0", "-1"),
            (
                "max.compaction.lag.ms",
                "9223372036854775807",
                "9223372036854775807",
                "0",
            ),
            ("delete.retention.ms", "86400000", "86400000", "-5"),
            ("min.cleanable.dirty.ratio", "0.50", "0.5", "1.5"),
        ] {
            let a = applied(&[(name, Some(good))]);
            assert!(a.options.is_empty(), "{name} reached Queen");
            assert_eq!(a.kafka[name], json!(normal), "{name}");
            let r = row(&a.echo, name);
            assert_eq!((r.source, r.read_only), (Source::Topic, false), "{name}");
            assert!(!r.documentation.is_empty());
            let why = refused(&[(name, Some(bad))]);
            assert!(why.contains(name) && why.contains("range"), "{name}: {why}");
        }
    }

    /// `CreateTime` is what a fetched record carries; `LogAppendTime` is not
    /// something this facade serves and is refused, not recorded.
    #[test]
    fn create_time_is_accepted_and_log_append_time_is_refused() {
        assert_eq!(
            applied(&[(MESSAGE_TIMESTAMP_TYPE, Some("CreateTime"))]).kafka[MESSAGE_TIMESTAMP_TYPE],
            json!("CreateTime")
        );
        let why = refused(&[(MESSAGE_TIMESTAMP_TYPE, Some("LogAppendTime"))]);
        assert!(why.contains("producer's timestamp"), "{why}");
    }

    #[test]
    fn retention_minus_one_is_infinite_and_writes_the_default() {
        let a = applied(&[(RETENTION_MS, Some("-1"))]);
        assert_eq!(a.options["retentionEnabled"], json!(false));
        assert!(!a.options.contains_key("retentionSeconds"));
        let r = row(&a.echo, RETENTION_MS);
        assert_eq!(r.value, "-1");
        assert_eq!(r.source, Source::Topic);
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
                row(&a.echo, RETENTION_MS).value,
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
            "compression.type",
            "unclean.leader.election.enable",
            "preallocate",
        ] {
            let why = refused(&[(name, Some("2"))]);
            assert!(why.contains(name), "{name}: {why}");
        }
    }

    /// `min.insync.replicas` above what is in force is refused, at every
    /// spelling of "more than there is".
    #[test]
    fn min_insync_replicas_one_is_a_no_op_and_anything_else_is_refused() {
        assert!(applied(&[(MIN_INSYNC_REPLICAS, Some("1"))])
            .kafka
            .is_empty());
        assert!(applied(&[(MIN_INSYNC_REPLICAS, Some(" 1 "))])
            .options
            .is_empty());
        // Kafka's "unset": the default, which is 1.
        assert!(applied(&[(MIN_INSYNC_REPLICAS, None)]).options.is_empty());

        for v in ["2", "3", "0", "-1", "many"] {
            let why = refused(&[(MIN_INSYNC_REPLICAS, Some(v))]);
            assert!(
                why.contains(MIN_INSYNC_REPLICAS) && why.contains("logical broker"),
                "{v}: {why}"
            );
        }
    }

    /// `read_only` is per row: every row of an UNTRACKED topic is fixed —
    /// an alter of it is refused — and every row of a tracked one is not.
    #[test]
    fn read_only_is_per_row_now() {
        for row in topic_configs() {
            assert!(row.read_only, "{} is not fixed", row.name);
        }
        for row in described(1, &Map::new(), &Map::new()) {
            assert!(!row.read_only, "{} of a tracked topic is fixed", row.name);
        }
        assert!(!reported_retention(None).read_only);
        assert!(!reported_retention(Some(604_800)).read_only);
        // ...and so is the create's own echo of a retention it just applied.
        let echoed = applied(&[(RETENTION_MS, Some("604800000"))]);
        assert!(!row(&echoed.echo, RETENTION_MS).read_only);
    }

    /// What a DESCRIBE reports for a tracked topic, in both states. An unset
    /// retention is DEFAULT and not TOPIC: nobody set it, and Queen's default
    /// off IS Kafka's -1.
    #[test]
    fn the_reported_retention_names_its_own_source() {
        let off = reported_retention(None);
        assert_eq!(off.value, "-1");
        assert_eq!(off.source, Source::Default);

        let on = reported_retention(Some(604_800));
        assert_eq!(on.value, "604800000");
        assert_eq!(on.source, Source::Topic);
        assert_eq!(on.kind, Kind::Int);
    }

    /// The DELETE half. `retention.ms` leaves the bag entirely, so what takes
    /// effect is `configure_queue_v1`'s own default — which is retention off,
    /// which is Kafka's -1 — and a recorded key leaves the Kafka-side values.
    #[test]
    fn resetting_a_key_drops_it() {
        let a = apply(&[
            (RETENTION_MS, Some("604800000")),
            ("segment.bytes", Some("1048576")),
        ])
        .unwrap();
        let (mut bag, mut kafka) = (a.options, a.kafka);
        assert_eq!(bag.len(), 2);
        absorb_change(&mut bag, &mut kafka, &reset(RETENTION_MS).unwrap());
        absorb_change(&mut bag, &mut kafka, &reset("segment.bytes").unwrap());
        assert!(bag.is_empty(), "{bag:?}");
        assert!(kafka.is_empty(), "{kafka:?}");

        // The policy and the in-sync count go back to their defaults.
        assert_eq!(
            reset(CLEANUP_POLICY).unwrap(),
            Change::kafka(CLEANUP_POLICY, None)
        );
        assert_eq!(
            reset(MIN_INSYNC_REPLICAS).unwrap(),
            Change::kafka(MIN_INSYNC_REPLICAS, None)
        );
        // And an unknown key is refused the same way a SET of it would be.
        assert!(reset("preallocate").unwrap_err().contains("preallocate"));
    }

    /// `SUBTRACT delete` computes an empty policy, and a topic with no cleanup
    /// policy is not a thing this facade — or Kafka — will have.
    #[test]
    fn an_empty_cleanup_policy_is_refused() {
        let why = alter(CLEANUP_POLICY, Some("")).unwrap_err();
        assert!(why.contains("cannot be emptied"), "{why}");
    }

    /// One bad key refuses the whole topic, and nothing is half-applied — the
    /// bag the caller would have sent is never built.
    #[test]
    fn one_bad_key_refuses_the_whole_topic() {
        assert!(apply(&[
            (RETENTION_MS, Some("86400000")),
            ("preallocate", Some("true")),
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
        assert_eq!(Kind::Double as i8, 6);
    }
}
