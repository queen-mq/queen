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
//! `retention.ms` maps to.
//!
//! The round trip is closed anyway, and NOT by inventing the value: the facade
//! keeps its own record of the bag it last posted for a topic
//! ([`crate::topic_record`]), and a describe reports retention from that record
//! for the topics it has one for. A topic this facade did not create has no
//! record and is answered as it always was — the key omitted rather than
//! guessed at.
//!
//! ## `read_only` is per row
//!
//! It was a module constant `true` for as long as AlterConfigs was not
//! advertised. It is now a field on [`Reported`], because the truth differs by
//! row: `cleanup.policy` and `min.insync.replicas` accept exactly the value
//! they already report, so nothing about them can be changed and `true` is
//! still honest; `retention.ms` is genuinely writable on a topic this facade
//! tracks, and reporting it read-only would grey out an edit control that works.
//! A UI acting on this flag is still being told the truth, which is the property
//! the constant was written to keep.

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
    /// A row nothing can change. The two topic rows and every broker row are
    /// this, and the constructor spells it so that a new row has to say which
    /// it is rather than inheriting a default.
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
        Reported::fixed(
            CLEANUP_POLICY,
            CLEANUP_DELETE,
            Source::Default,
            Kind::String,
            "Always `delete`. Log compaction is a stated non-goal of queen-kafka and nothing \
             compacts a Queen queue.",
        ),
        Reported::fixed(
            MIN_INSYNC_REPLICAS,
            "1",
            Source::Default,
            Kind::Int,
            "Always 1. The facade advertises one logical broker and Metadata reports \
             replicas=[0], isr=[0]; durability is Postgres's, not a replica count's.",
        ),
    ]
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
    pub options: serde_json::Map<String, serde_json::Value>,
    /// The configs this create actually applied, for the CreateTopics v5+
    /// response.
    ///
    /// It carries [`topic_configs`] plus whatever this call set, which is the
    /// one place `retention.ms` can be answered at all — see the module header.
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
pub type Delta = Vec<(String, Option<serde_json::Value>)>;

/// The `/configure` key for whether retention runs at all.
pub const RETENTION_ENABLED: &str = "retentionEnabled";
/// The `/configure` key for the retention window, in WHOLE SECONDS.
pub const RETENTION_SECONDS: &str = "retentionSeconds";

/// What one `(name, value)` pair does to the options bag, or the reason it is
/// refused INVALID_CONFIG.
///
/// This is the whole alter vocabulary and it is the SAME one [`apply`] uses, so
/// a create and an alter cannot come to disagree about what a key means. A
/// `None` value is Kafka's "unset, use the default", and every default here is
/// the facade's own behaviour, so it is a no-op rather than an error.
///
/// Refusing an unknown name is Kafka's own answer and it is the point: silently
/// dropping `segment.bytes=1073741824` would be telling a client it got a
/// setting it did not get.
pub fn alter(name: &str, value: Option<&str>) -> Result<Delta, String> {
    match name {
        CLEANUP_POLICY => match value.map(str::trim) {
            // The default and the only one there is.
            None => Ok(Vec::new()),
            Some(v) if v.eq_ignore_ascii_case(CLEANUP_DELETE) => Ok(Vec::new()),
            // An EMPTY policy, which is what `SUBTRACT delete` computes. Kafka
            // will not have a topic with no cleanup policy either, and here it
            // would be a topic whose one true statement about itself has been
            // erased.
            Some("") => Err(format!(
                "{CLEANUP_POLICY} cannot be emptied: every Queen queue deletes and none \
                 compacts, so `{CLEANUP_DELETE}` is the policy and there is no other value \
                 and no absence of one"
            )),
            // THE refusal that decides whether Kafka Connect can run.
            //
            // Connect creates its three internal topics with
            // `cleanup.policy=compact` (hard-coded in
            // `TopicAdmin.NewTopicBuilder.compacted()`), and its config topic is
            // a compacted log used as a database. A facade that accepted the
            // setting and compacted nothing would let Connect start and then
            // lose the connector configuration on the first restart. Refusing
            // here kills Connect at startup instead, which is the loud failure
            // rather than the silent one.
            Some(v) => Err(format!(
                "{CLEANUP_POLICY}={v} is not supported: log compaction is a stated \
                 non-goal of queen-kafka and nothing compacts a Queen queue. Only \
                 `{CLEANUP_DELETE}` is accepted. A tool whose internal topics are \
                 compacted (Kafka Connect, Kafka Streams) cannot run against this \
                 facade, and failing here is why it does not lose records later"
            )),
        },
        // Set to the value the facade already REPORTS, and it is a no-op rather
        // than an unknown key. The asymmetry it removes was real and small and
        // bit exactly the obvious command: DescribeConfigs answers
        // `min.insync.replicas=1`, so `--add-config min.insync.replicas=1` is a
        // user echoing the broker back at itself, and refusing that read as the
        // facade not knowing its own answer.
        MIN_INSYNC_REPLICAS => match value.map(str::trim) {
            None => Ok(Vec::new()),
            Some("1") => Ok(Vec::new()),
            Some(v) => Err(format!(
                "{MIN_INSYNC_REPLICAS}={v} cannot be honoured: this facade advertises ONE \
                 logical broker and every Metadata answer says replicas=[0], isr=[0], so the \
                 only in-sync replica count there is is 1. Durability here is Postgres's, not \
                 a replica count's. Accepting a higher number would report a durability \
                 setting that is not in force"
            )),
        },
        RETENTION_MS => match value.map(str::trim) {
            // Unset: the facade's default, which is retention OFF — which is
            // why auto-created topics do not quietly expire.
            None => Ok(Vec::new()),
            Some(v) => {
                let ms: i64 = v
                    .parse()
                    .map_err(|_| format!("{RETENTION_MS}={v} is not a number of milliseconds"))?;
                match ms {
                    // Kafka's "infinite", and the facade's default. The window
                    // is cleared as well as disabled, so the bag says one thing
                    // about retention rather than two.
                    -1 => Ok(vec![
                        (RETENTION_ENABLED.to_string(), Some(json!(false))),
                        (RETENTION_SECONDS.to_string(), None),
                    ]),
                    // Queen's retention is in SECONDS, so a sub-second window
                    // cannot be expressed. Rounding it down would reach zero,
                    // and a retention of zero seconds means "delete everything"
                    // — refusing is the only answer that is not a data-loss
                    // surprise.
                    0..=999 => Err(format!(
                        "{RETENTION_MS}={ms} is below the one second Queen's retention \
                         can express (it is configured in whole seconds); rounding it \
                         down would reach zero, which deletes everything"
                    )),
                    // Rounded DOWN, so the facade never retains less than the
                    // client asked for by mistake — it retains at most one
                    // second less than asked, never more.
                    ms if ms >= 1_000 => Ok(vec![
                        (RETENTION_ENABLED.to_string(), Some(json!(true))),
                        (RETENTION_SECONDS.to_string(), Some(json!(ms / 1_000))),
                    ]),
                    // Everything below -1. Kafka defines exactly one negative
                    // value and this is not it.
                    _ => Err(format!(
                        "{RETENTION_MS}={ms} is not a retention: -1 is infinite and \
                         every other value must be a non-negative number of milliseconds"
                    )),
                }
            }
        },
        other => Err(format!(
            "`{other}` is not a topic config this facade understands. It accepts \
             `{CLEANUP_POLICY}={CLEANUP_DELETE}`, `{MIN_INSYNC_REPLICAS}=1` and \
             `{RETENTION_MS}`; every other Kafka topic config names a mechanism Queen does \
             not have, and accepting one silently would report a setting that is not in force"
        )),
    }
}

/// What resetting one key to its default does to the options bag — Kafka's
/// `AlterConfigOp` DELETE, and the unnamed half of AlterConfigs' full
/// replacement.
///
/// `retention.ms` drops out of the bag entirely, which leaves
/// `configure_queue_v1`'s own default (`retention_enabled = false`) in force —
/// and that IS Kafka's -1. The other two keys are already at their only value,
/// so resetting them does nothing. An unknown key is refused with the same
/// sentence [`alter`] refuses it with, because a client deleting a key this
/// facade never had should learn the same thing as one setting it.
pub fn reset(name: &str) -> Result<Delta, String> {
    match name {
        CLEANUP_POLICY | MIN_INSYNC_REPLICAS => Ok(Vec::new()),
        RETENTION_MS => Ok(vec![
            (RETENTION_ENABLED.to_string(), None),
            (RETENTION_SECONDS.to_string(), None),
        ]),
        other => alter(other, None).map(|_| Vec::new()),
    }
}

/// Apply a delta to an options bag in place. One place, so that "a `None`
/// removes the key" cannot be implemented two ways.
pub fn absorb(options: &mut serde_json::Map<String, serde_json::Value>, delta: &Delta) {
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

/// Turn one topic's requested `configs[]` into what Queen is told, or into the
/// reason the whole topic is refused INVALID_CONFIG.
///
/// The vocabulary is [`alter`]'s, in full. What this adds is the CreateTopics
/// v5+ echo, which answers "this is what your create applied" and is therefore
/// TOPIC-sourced for retention either way — unlike [`reported_retention`],
/// which answers "this is what is in force" and calls an unset retention what
/// it is.
pub fn apply(configs: &[(&str, Option<&str>)]) -> Result<Applied, String> {
    let mut options = serde_json::Map::new();
    let mut named_retention = false;

    for (name, value) in configs {
        absorb(&mut options, &alter(name, *value)?);
        named_retention |= *name == RETENTION_MS && value.is_some();
    }

    let mut echo = topic_configs();
    if named_retention {
        echo.push(
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
        for name in ["max.message.bytes", "segment.bytes", "compression.type"] {
            let why = refused(&[(name, Some("2"))]);
            assert!(why.contains(name), "{name}: {why}");
        }
    }

    /// The asymmetry F4 removes: `min.insync.replicas` is REPORTED at 1 by
    /// every describe, so setting it to 1 is a client echoing the broker back
    /// at itself and must not be an error. Anything else still is.
    #[test]
    fn min_insync_replicas_one_is_a_no_op_and_anything_else_is_refused() {
        assert!(applied(&[(MIN_INSYNC_REPLICAS, Some("1"))])
            .options
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

    /// `read_only` is per row now, and the split is the one an edit control
    /// should act on: the two rows whose only legal value is the one already
    /// reported cannot be changed, and retention can.
    #[test]
    fn read_only_is_per_row_now() {
        for row in topic_configs() {
            assert!(row.read_only, "{} is not fixed", row.name);
        }
        assert!(!reported_retention(None).read_only);
        assert!(!reported_retention(Some(604_800)).read_only);
        // ...and so is the create's own echo of a retention it just applied.
        let echoed = applied(&[(RETENTION_MS, Some("604800000"))]);
        assert!(!echoed.echo.last().unwrap().read_only);
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
    /// which is Kafka's -1.
    #[test]
    fn resetting_a_key_drops_it_from_the_bag() {
        let mut bag = apply(&[(RETENTION_MS, Some("604800000"))]).unwrap().options;
        assert_eq!(bag.len(), 2);
        absorb(&mut bag, &reset(RETENTION_MS).unwrap());
        assert!(bag.is_empty(), "{bag:?}");

        // The other two are already at their only value.
        assert!(reset(CLEANUP_POLICY).unwrap().is_empty());
        assert!(reset(MIN_INSYNC_REPLICAS).unwrap().is_empty());
        // And an unknown key is refused the same way a SET of it would be.
        assert!(reset("segment.bytes")
            .unwrap_err()
            .contains("segment.bytes"));
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
