//! SNS: topics, subscriptions, and the fanout they describe.
//!
//! CONTRACT. SNS here is a FACADE-LEVEL CONSTRUCT and the broker needs nothing
//! for it (PLAN_QUEEN_SQS.md, SNS): a topic is a key in Queen's key/value store,
//! a subscription is another, and a publish will be one
//! `POST /api/v1/transaction` bundling one push per matched subscription. There
//! is no Queen object called a topic and there is no `/configure` on this path.
//!
//! The five files:
//!
//!   * this one — the names, the ARNs and the refusals every half shares;
//!   * [`registry`] — the records and their compare-and-set writes, a second
//!     `impl` block on [`crate::registry::Registry`];
//!   * [`admin`] — one function per administrative action;
//!   * [`filter`] — the `FilterPolicy` grammar, validated at write time and
//!     evaluated at publish;
//!   * [`publish`] — `Publish` and `PublishBatch`, and the transaction the
//!     fan-out is.
//!
//! ## v0 is SQS-queue subscriptions and nothing else
//!
//! `Protocol=sqs` is the only protocol [`admin::subscribe`] accepts, and the
//! refusal says so by name rather than pretending the endpoint is malformed.
//! That is the plan's scope decision and not an accident of implementation:
//! HTTP/S subscribers are M6 and are delegated to queen-relay, and the
//! `SubscriptionConfirmation` handshake exists only for them. Every subscription
//! this facade can create today is same-account SQS, which AWS itself
//! auto-confirms — so `PendingConfirmation` is a state no record here ever
//! occupies, and [`admin::confirm_subscription`] says that out loud instead of
//! answering a token nobody minted.
//!
//! ## Why the two services do not share their error codes
//!
//! A missing topic is `NotFound` with **HTTP 404** and a bad parameter is
//! `InvalidParameter` — neither is SQS's spelling, and the status is not SQS's
//! either. [`crate::error`] carries both halves for exactly this reason.

pub mod admin;
pub mod filter;
pub mod publish;
pub mod registry;

use std::collections::BTreeMap;

use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::registry::{Naming, FIFO_SUFFIX};

/// The only `Protocol` this facade subscribes today. See the module header.
pub const PROTOCOL_SQS: &str = "sqs";

/// The longest topic name AWS accepts, `.fifo` included.
///
/// Three times a queue's 80, which is AWS's own asymmetry and not a typo: a
/// topic name is not a queue name and the two limits are documented apart.
pub const MAX_TOPIC_NAME_LEN: usize = 256;

/// The attribute that DECLARES a FIFO topic, which must agree with the `.fifo`
/// suffix in both directions ([`validate_topic_name`]).
pub const ATTR_FIFO_TOPIC: &str = "FifoTopic";

/// The most tags one topic may carry (AWS's own cap, and the reason
/// [`ErrorKind::TagLimitExceeded`] is in the catalog).
pub const MAX_TAGS: usize = 50;

// --------------------------------------------------------------------- names

/// Whether a topic name declares a FIFO topic.
///
/// A period is not in the topic-name charset at all, so the suffix is the ONLY
/// way a legal name can contain one — which is what makes `.fifo` a declaration
/// rather than a convention.
pub fn is_fifo_topic(name: &str) -> bool {
    name.ends_with(FIFO_SUFFIX) && name.len() > FIFO_SUFFIX.len()
}

/// Whether a string is a topic name AWS would have accepted, `.fifo` included.
/// Charset and length only — the agreement with `FifoTopic` is
/// [`validate_topic_name`]'s.
pub fn topic_name_is_wellformed(name: &str) -> bool {
    let stem = name.strip_suffix(FIFO_SUFFIX).unwrap_or(name);
    (1..=MAX_TOPIC_NAME_LEN).contains(&name.len())
        && !stem.is_empty()
        && stem
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Whether a string is a subscription id this facade would have minted: a v4
/// UUID, and therefore hex and hyphens.
///
/// It is checked when a `SubscriptionArn` is parsed BACK, for the reason
/// [`crate::registry::Naming::name_of`] checks a queue name there: the id
/// becomes half of a key, and a component that reached the store unexamined is
/// one an ARN could have chosen.
fn id_is_wellformed(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// The name rules, and the one rule that ties a name to an attribute.
///
/// `.fifo` and `FifoTopic=true` must BOTH be present or BOTH absent. The queue
/// side deliberately relaxes the analogous rule for a RE-create (a queue's type
/// is already declared by its own name, and an SDK that remembers the name but
/// not the attribute is the common shape — `compat/M0_SMOKE.md` D1); it is not
/// relaxed here, because the frameworks that create topics (MassTransit,
/// JustSaying) send the attribute set they want on every call, and a relaxation
/// nobody needs is a divergence nobody checked. If the differential lane shows
/// AWS accepting the bare `.fifo` re-create, the queue's shape is the one to
/// copy.
pub fn validate_topic_name(name: &str, attributes: &BTreeMap<String, String>) -> SqsResult<()> {
    let declared = attributes
        .get(ATTR_FIFO_TOPIC)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let suffixed = is_fifo_topic(name);
    if !topic_name_is_wellformed(name) {
        // AWS's own sentence for this one, which names the MEMBER and not the
        // charset: an operator reconciling this facade against the real service
        // compares the strings.
        return Err(invalid_member("Topic Name"));
    }
    if declared != suffixed {
        return Err(invalid(
            "Topic Name",
            "FIFO topic names must end with .fifo and be declared with \
             FifoTopic set to true; a standard topic can be neither",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------- arns

/// The SNS spellings of an ARN, on the type that already holds the region and
/// the account ([`Naming`]). See that type's header for why the parsers live
/// beside the formatters.
impl Naming {
    /// `arn:aws:sns:<region>:<account>:<name>`.
    pub fn topic_arn(&self, name: &str) -> String {
        format!("arn:aws:sns:{}:{}:{name}", self.region, self.account)
    }

    /// `arn:aws:sns:<region>:<account>:<topic>:<subscription-id>` — SNS's own
    /// shape, which is the topic's ARN with a seventh segment.
    pub fn subscription_arn(&self, topic: &str, id: &str) -> String {
        format!("{}:{id}", self.topic_arn(topic))
    }

    /// The topic a `TopicArn` names, or `None` when the ARN is not one this
    /// deployment would have minted.
    ///
    /// Run BACKWARDS on untrusted input, exactly like
    /// [`Naming::name_of_arn`]: `TopicArn` is a client-supplied string on every
    /// SNS action. Six segments EXACTLY — a seventh makes it a subscription's
    /// ARN and not a topic's, and reading one as the other would let
    /// `GetTopicAttributes` answer for a key nobody named. The partition segment
    /// is accepted whatever it says, for that method's reason.
    pub fn topic_of_arn(&self, arn: &str) -> Option<String> {
        let segments: Vec<&str> = arn.split(':').collect();
        let ["arn", _partition, "sns", region, account, name] = segments.as_slice() else {
            return None;
        };
        if *region != self.region || *account != self.account || !topic_name_is_wellformed(name) {
            return None;
        }
        Some((*name).to_string())
    }

    /// The `(topic, id)` a `SubscriptionArn` names. Seven segments exactly.
    pub fn subscription_of_arn(&self, arn: &str) -> Option<(String, String)> {
        let segments: Vec<&str> = arn.split(':').collect();
        let ["arn", _partition, "sns", region, account, topic, id] = segments.as_slice() else {
            return None;
        };
        if *region != self.region
            || *account != self.account
            || !topic_name_is_wellformed(topic)
            || !id_is_wellformed(id)
        {
            return None;
        }
        Some(((*topic).to_string(), (*id).to_string()))
    }
}

// ------------------------------------------------------------------- refusals

/// SNS's "that resource is not there" — a 404, and never SQS's 400.
pub(crate) fn not_found(what: impl std::fmt::Display) -> SqsError {
    SqsError::with(ErrorKind::NotFound, format!("{what}"))
}

/// `Invalid parameter: <member> Reason: <why>`, which is the shape SNS's own
/// messages take. The member is named because it is the whole of what an
/// operator needs, and the reason never carries a value the client sent.
pub(crate) fn invalid(member: &str, why: impl std::fmt::Display) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameter,
        format!("Invalid parameter: {member} Reason: {why}"),
    )
}

/// The same refusal with no reason clause — the form AWS uses when the member
/// name is the whole explanation (`Invalid parameter: Topic Name`).
pub(crate) fn invalid_member(member: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameter,
        format!("Invalid parameter: {member}"),
    )
}

/// A required parameter that is not there.
///
/// `MissingParameter` — the Query protocol's own common error, which SNS's
/// framing layer shares — rather than a new code: SNS itself answers a
/// `ValidationError` from a request-validation layer this facade does not model,
/// and of the codes in the closed catalog this is the one that says "you left
/// something out" and names it.
pub(crate) fn missing(member: &str) -> SqsError {
    SqsError::with(
        ErrorKind::MissingParameter,
        format!("The request must contain the parameter {member}."),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naming() -> Naming {
        Naming::new("queen-1", "000000000000")
    }

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_topic_name_rules_are_awss() {
        let fifo = attrs(&[("FifoTopic", "true")]);
        let none = BTreeMap::new();
        for (name, attributes, ok) in [
            ("events", &none, true),
            ("Events_2-x", &none, true),
            ("e", &none, true),
            (&"e".repeat(256), &none, true),
            (&"e".repeat(257), &none, false),
            ("", &none, false),
            ("with space", &none, false),
            ("with.dot", &none, false),
            ("with/slash", &none, false),
            ("events.fifo", &fifo, true),
            // The suffix and the attribute must AGREE, in both directions.
            ("events.fifo", &none, false),
            ("events", &fifo, false),
            (".fifo", &fifo, false),
            // 256 characters counts the suffix.
            (&format!("{}.fifo", "e".repeat(251)), &fifo, true),
            (&format!("{}.fifo", "e".repeat(252)), &fifo, false),
        ] {
            let got = validate_topic_name(name, attributes);
            assert_eq!(got.is_ok(), ok, "{name:?} with {attributes:?}: {got:?}");
        }
        // A topic name is three times a queue name's length, which is AWS's own
        // asymmetry: the 81st character is legal here and not there.
        assert!(topic_name_is_wellformed(&"e".repeat(81)));
    }

    #[test]
    fn a_bad_topic_name_is_snss_own_refusal() {
        let e = validate_topic_name("with space", &BTreeMap::new()).expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert_eq!(e.kind.http_status(), 400);
        assert_eq!(e.message, "Invalid parameter: Topic Name");
    }

    /// The two ARNs, minted and parsed back — and the property that keeps them
    /// apart: a subscription's ARN is a topic's with a seventh segment, and
    /// neither parser may accept the other's.
    #[test]
    fn an_arn_round_trips_and_the_two_shapes_cannot_be_confused() {
        let naming = naming();
        let topic = naming.topic_arn("events");
        assert_eq!(topic, "arn:aws:sns:queen-1:000000000000:events");
        assert_eq!(naming.topic_of_arn(&topic).as_deref(), Some("events"));
        assert_eq!(naming.subscription_of_arn(&topic), None);

        let subscription = naming.subscription_arn("events", "6b0f-1");
        assert_eq!(
            subscription,
            "arn:aws:sns:queen-1:000000000000:events:6b0f-1"
        );
        assert_eq!(
            naming.subscription_of_arn(&subscription),
            Some(("events".to_string(), "6b0f-1".to_string()))
        );
        assert_eq!(naming.topic_of_arn(&subscription), None);
    }

    /// Every way an ARN can fail to be one of ours. Each is `None`, which the
    /// caller answers `NotFound` or `InvalidParameter` for — never a lookup of
    /// whatever the string happened to contain.
    #[test]
    fn an_arn_from_elsewhere_is_not_ours() {
        let naming = naming();
        for arn in [
            "",
            "events",
            "arn:aws:sqs:queen-1:000000000000:events",
            "arn:aws:sns:other-region:000000000000:events",
            "arn:aws:sns:queen-1:999999999999:events",
            "arn:aws:sns:queen-1:000000000000:with space",
            "arn:aws:sns:queen-1:000000000000:",
            "arn:aws:sns:queen-1:000000000000",
        ] {
            assert_eq!(naming.topic_of_arn(arn), None, "{arn}");
        }
        // A subscription ARN whose id could address another key.
        for arn in [
            "arn:aws:sns:queen-1:000000000000:events:",
            "arn:aws:sns:queen-1:000000000000:events:a:b",
            "arn:aws:sns:queen-1:000000000000:events:../../q",
            "arn:aws:sqs:queen-1:000000000000:events:id",
        ] {
            assert_eq!(naming.subscription_of_arn(arn), None, "{arn}");
        }
        // A DIFFERENT partition is accepted: it names an AWS realm this
        // deployment is not in, and the two segments that carry meaning are
        // still ours.
        assert_eq!(
            naming
                .topic_of_arn("arn:aws-cn:sns:queen-1:000000000000:events")
                .as_deref(),
            Some("events")
        );
    }

    /// The two refusal shapes, which are the strings a client's log shows.
    #[test]
    fn the_refusals_are_snss_own_shapes() {
        assert_eq!(
            invalid(
                "Endpoint",
                "a FIFO topic cannot fan out to a standard queue"
            )
            .message,
            "Invalid parameter: Endpoint Reason: a FIFO topic cannot fan out to a standard queue"
        );
        assert_eq!(
            invalid_member("TopicArn").message,
            "Invalid parameter: TopicArn"
        );
        assert_eq!(not_found("Topic does not exist").kind, ErrorKind::NotFound);
        assert_eq!(not_found("x").kind.http_status(), 404);
        assert_eq!(missing("Name").kind, ErrorKind::MissingParameter);
    }
}
