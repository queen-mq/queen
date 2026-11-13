//! Routing without copies (broker catalogue version 8): headers on messages,
//! the `where` filtered groups and reading queues take, and the source of a
//! `link`.
//!
//! - **Headers** ride beside the payload: a map of strings, stored in clear and
//!   never part of dedup. Set them with [`QueueBuilder::headers`],
//!   [`PushItem::headers`] or [`TxnPushItem::headers`]; a popped message has
//!   them in [`Message::headers`], a fetched record in
//!   [`FetchRecord::headers`](queen_protocol::FetchRecord::headers), a feed
//!   entry in [`FeedEntry::headers`].
//! - **A filtered group** gets only the messages its `where` takes:
//!   [`QueueBuilder::where_`] and [`QueueBuilder::where_str`] on a consumer
//!   group, sent on every pop.
//! - **A reading queue** holds links to the messages of the queues it reads:
//!   `from`, `where_`, `partition_by` and `start` on [`QueueOptions`].
//! - **`link`** hands a message on to another queue without sending its
//!   payload again: [`TransactionBuilder::link`].
//!
//! A cluster with a member older than catalogue version 8 answers every one of
//! them with `cluster_version`.
//!
//! ```no_run
//! # use queen_mq::Queen;
//! # use serde_json::json;
//! # async fn example(queen: Queen) -> queen_mq::Result<()> {
//! queen.queue("orders")
//!     .partition("order-9137")
//!     .headers([("type", "order.paid")])
//!     .push(json!({ "orderId": 9137, "total": 120 }))
//!     .await?;
//!
//! queen.queue("orders")
//!     .group("billing")
//!     .where_("headers.type", "in", json!(["order.paid", "order.refunded"]))
//!     .consume(|msg| async move {
//!         println!("{:?} {}", msg.headers, msg.data);
//!         Ok::<_, std::convert::Infallible>(())
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`QueueBuilder::headers`]: crate::QueueBuilder::headers
//! [`QueueBuilder::where_`]: crate::QueueBuilder::where_
//! [`QueueBuilder::where_str`]: crate::QueueBuilder::where_str
//! [`PushItem::headers`]: crate::PushItem::headers
//! [`TxnPushItem::headers`]: crate::TxnPushItem::headers
//! [`QueueOptions`]: crate::QueueOptions
//! [`TransactionBuilder::link`]: crate::TransactionBuilder::link

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use queen_protocol::Message;

use crate::error::{Error, Result};
use crate::feed::{FeedEntry, WhereCondition};
use crate::js;

/// A message's headers: names to string values, in name order.
pub type Headers = BTreeMap<String, String>;

/// The longest header name the broker takes.
const MAX_HEADER_NAME_BYTES: usize = 64;

/// The names of the headers the broker sets itself.
const RESERVED_HEADER_PREFIX: &str = "queen-";

/// Refuse a header name the broker would refuse: 1 to 64 bytes of
/// `a-z 0-9 . _ -`, not starting with `queen-`.
///
/// Checked here because a buffered batch the broker refuses is not dropped but
/// retried (see [`crate::buffer`]): one bad name would hold its whole buffer.
/// The limits on how many and how large are the broker's settings
/// (`QUEEN_MAX_HEADERS`, `QUEEN_MAX_HEADER_BYTES`) and stay its to apply.
pub(crate) fn check_headers(headers: Option<&Headers>) -> Result<()> {
    for name in headers.into_iter().flat_map(|h| h.keys()) {
        let ok = !name.is_empty()
            && name.len() <= MAX_HEADER_NAME_BYTES
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b));
        if !ok {
            return Err(Error::Invalid(format!(
                "header name {}: 1 to {MAX_HEADER_NAME_BYTES} bytes of a-z, 0-9, '.', '_' or '-'",
                js::string_json(name)
            )));
        }
        if name.starts_with(RESERVED_HEADER_PREFIX) {
            return Err(Error::Invalid(format!(
                "header name {}: names starting with \"{RESERVED_HEADER_PREFIX}\" are the broker's",
                js::string_json(name)
            )));
        }
    }
    Ok(())
}

/// `pairs` added to `headers` (a name given twice keeps its last value).
pub(crate) fn extend_headers<I, K, V>(headers: &mut Option<Headers>, pairs: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    headers
        .get_or_insert_with(Headers::new)
        .extend(pairs.into_iter().map(|(k, v)| (k.into(), v.into())));
}

// ------------------------------------------------------------------ where

/// A `where`: conditions on a message, ANDed, as filtered groups, reading
/// queues and the feed read them.
///
/// Two forms say the same thing: conditions added with [`Where::and`] go as
/// the list `[{"field","op","value"}]`; text added with [`Where::and_text`]
/// goes as one string, `data.kind in ('a', 'b') and data.total > 100` (several
/// texts are joined with `and`). One `Where` holds one form: mixing them is
/// refused when it is sent.
///
/// Fields: `queue`, `partition`, `transactionId`, `data`, `data.<path>`,
/// `headers.<name>`. Ops: `= != < <= > >= in exists prefix !prefix`. A field
/// the message lacks holds for no op, `!=` included.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Where {
    conditions: Vec<WhereCondition>,
    text: Vec<String>,
    /// The first condition that failed its check, said when the `where` is sent.
    error: Option<String>,
}

/// A `where` ready for the wire.
enum Form {
    List(String),
    Text(String),
}

impl Where {
    pub fn new() -> Self {
        Self::default()
    }

    /// The string form: `Where::text("data.country = 'IT'")`.
    pub fn text(text: impl Into<String>) -> Self {
        Self::new().and_text(text)
    }

    /// One more condition, `field op value` (`value`: `None` for `exists`).
    pub fn and(mut self, field: &str, op: &str, value: impl Into<Option<Value>>) -> Self {
        match WhereCondition::new(field, op, value.into()) {
            Ok(c) => self.conditions.push(c),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(e.to_string());
                }
            }
        }
        self
    }

    /// One more condition, already checked.
    pub fn condition(mut self, c: WhereCondition) -> Self {
        self.conditions.push(c);
        self
    }

    /// More of the string form, ANDed with what is there.
    pub fn and_text(mut self, text: impl Into<String>) -> Self {
        let text = text.into();
        if !text.trim().is_empty() {
            self.text.push(text.trim().to_string());
        }
        self
    }

    /// Nothing to send.
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty() && self.text.is_empty() && self.error.is_none()
    }

    fn form(&self) -> Result<Option<Form>> {
        if let Some(e) = &self.error {
            return Err(Error::Invalid(e.clone()));
        }
        match (self.conditions.is_empty(), self.text.is_empty()) {
            (true, true) => Ok(None),
            (false, true) => {
                // As JS writes the list (`JSON.stringify`): a filtered group's
                // `where` must be the same on every pop, and a number written
                // `1.0` by one SDK and `1` by another is a different `where`.
                let items: Vec<String> = self.conditions.iter().map(|c| c.to_json_text()).collect();
                Ok(Some(Form::List(format!("[{}]", items.join(",")))))
            }
            (true, false) => Ok(Some(Form::Text(self.text.join(" and ")))),
            (false, false) => Err(Error::Invalid(
                "where: use the (field, op, value) form or the string form, not both".into(),
            )),
        }
    }

    /// The `where` as a JSON body carries it: the string, or the list of
    /// `{field, op, value}`. `None` when there is nothing to send; an error
    /// for a condition that failed its check.
    pub fn to_value(&self) -> Result<Option<Value>> {
        Ok(match self.form()? {
            None => None,
            Some(Form::Text(t)) => Some(Value::String(t)),
            Some(Form::List(l)) => Some(serde_json::from_str(&l)?),
        })
    }

    /// The `where` as a query parameter carries it: the string, or the list
    /// as JSON text.
    pub(crate) fn to_query(&self) -> Result<Option<String>> {
        Ok(self.form()?.map(|f| match f {
            Form::List(l) => l,
            Form::Text(t) => t,
        }))
    }
}

impl From<&str> for Where {
    fn from(text: &str) -> Self {
        Self::text(text)
    }
}

impl From<String> for Where {
    fn from(text: String) -> Self {
        Self::text(text)
    }
}

impl From<WhereCondition> for Where {
    fn from(c: WhereCondition) -> Self {
        Self::new().condition(c)
    }
}

impl From<Vec<WhereCondition>> for Where {
    fn from(conditions: Vec<WhereCondition>) -> Self {
        Self {
            conditions,
            ..Self::default()
        }
    }
}

// ------------------------------------------------------------------- link

/// Where the message of a `link` is.
///
/// As a pop returned it: its `partitionId`, `offset` and `transactionId`, and
/// the consumer group that popped it (a scoped credential must be allowed to
/// consume the source with that group). From a feed entry, whose partition id
/// is not the one a pop knows: by queue and partition name. The offset finds
/// the message; the transaction id proves it is the one meant
/// (`source_mismatch` otherwise).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinkFrom {
    #[serde(rename = "partitionId", skip_serializing_if = "Option::is_none")]
    partition_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
    offset: i64,
    #[serde(rename = "transactionId")]
    transaction_id: String,
    #[serde(rename = "consumerGroup", skip_serializing_if = "Option::is_none")]
    consumer_group: Option<String>,
}

impl LinkFrom {
    /// A message named as a pop names it.
    pub fn by_id(
        partition_id: impl Into<String>,
        offset: i64,
        transaction_id: impl Into<String>,
    ) -> Self {
        Self {
            partition_id: Some(partition_id.into()),
            queue: None,
            partition: None,
            offset,
            transaction_id: transaction_id.into(),
            consumer_group: None,
        }
    }

    /// A message named by its queue and partition, as a feed entry or a
    /// fetched record names it.
    pub fn by_name(
        queue: impl Into<String>,
        partition: impl Into<String>,
        offset: i64,
        transaction_id: impl Into<String>,
    ) -> Self {
        Self {
            partition_id: None,
            queue: Some(queue.into()),
            partition: Some(partition.into()),
            offset,
            transaction_id: transaction_id.into(),
            consumer_group: None,
        }
    }

    /// The consumer group the message was popped with.
    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into()).filter(|g| !g.is_empty());
        self
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
}

/// Something a `link` can name: a popped [`Message`], a [`FeedEntry`], or a
/// [`LinkFrom`] built by hand.
pub trait LinkSource {
    /// Where this message is. An error when it does not say (a message from
    /// an answer without offsets).
    fn link_from(&self) -> Result<LinkFrom>;
}

impl LinkSource for LinkFrom {
    fn link_from(&self) -> Result<LinkFrom> {
        Ok(self.clone())
    }
}

impl LinkSource for Message {
    fn link_from(&self) -> Result<LinkFrom> {
        let Some(offset) = self.offset else {
            return Err(Error::Invalid(format!(
                "cannot link message {}: it carries no offset (link a message as a pop \
                 returned it)",
                js::string_json(&self.transaction_id)
            )));
        };
        if self.partition_id.is_empty() {
            return Err(Error::Invalid(format!(
                "cannot link message {}: it carries no partitionId",
                js::string_json(&self.transaction_id)
            )));
        }
        Ok(
            LinkFrom::by_id(&self.partition_id, offset, &self.transaction_id)
                .consumer_group(&self.consumer_group),
        )
    }
}

impl LinkSource for FeedEntry {
    fn link_from(&self) -> Result<LinkFrom> {
        let offset = js::int_of(Some(&self.offset))
            .filter(|o| *o >= 0)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "cannot link feed entry {}: it carries no offset",
                    js::string_json(&self.transaction_id)
                ))
            })?;
        if self.queue.is_empty() || self.partition.is_empty() {
            return Err(Error::Invalid(format!(
                "cannot link feed entry {}: it names no queue and partition",
                js::string_json(&self.transaction_id)
            )));
        }
        Ok(LinkFrom::by_name(
            &self.queue,
            &self.partition,
            offset,
            &self.transaction_id,
        ))
    }
}

impl<T: LinkSource + ?Sized> LinkSource for &T {
    fn link_from(&self) -> Result<LinkFrom> {
        (**self).link_from()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn headers(pairs: &[(&str, &str)]) -> Headers {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn header_names_are_checked_as_the_broker_checks_them() {
        assert!(check_headers(None).is_ok());
        assert!(check_headers(Some(&headers(&[]))).is_ok());
        assert!(check_headers(Some(&headers(&[
            ("type", "order.paid"),
            ("traceparent", "00-ab-cd-01"),
            ("a.b_c-9", ""),
        ])))
        .is_ok());
        for bad in ["", "Type", "with space", "ünï", &"k".repeat(65)] {
            let err = check_headers(Some(&headers(&[(bad, "v")])))
                .expect_err(bad)
                .to_string();
            assert!(err.contains("header name"), "{bad}: {err}");
        }
        let err = check_headers(Some(&headers(&[("queen-route", "x")])))
            .unwrap_err()
            .to_string();
        assert!(err.contains("are the broker's"), "{err}");
        assert!(check_headers(Some(&headers(&[(&"k".repeat(64), "v")]))).is_ok());
    }

    #[test]
    fn a_where_of_conditions_is_the_list_as_js_writes_it() {
        let w = Where::new()
            .and("data.kind", "in", json!(["order.paid", "order.refunded"]))
            .and("data.total", ">", json!(100.0))
            .and("headers.trace", "exists", None);
        assert_eq!(
            w.to_query().unwrap().unwrap(),
            concat!(
                r#"[{"field":"data.kind","op":"in","value":["order.paid","order.refunded"]},"#,
                r#"{"field":"data.total","op":">","value":100},"#,
                r#"{"field":"headers.trace","op":"exists"}]"#,
            ),
            "100.0 is written 100, as every other SDK writes it"
        );
        assert_eq!(
            w.to_value().unwrap().unwrap()[1],
            json!({"field": "data.total", "op": ">", "value": 100})
        );
    }

    #[test]
    fn a_where_of_text_is_one_string() {
        let w = Where::text("data.country = 'IT'").and_text("  headers.type prefix 'order.' ");
        assert_eq!(
            w.to_query().unwrap().as_deref(),
            Some("data.country = 'IT' and headers.type prefix 'order.'")
        );
        assert_eq!(
            w.to_value().unwrap(),
            Some(json!(
                "data.country = 'IT' and headers.type prefix 'order.'"
            ))
        );
        assert_eq!(Where::from("a = 1"), Where::text("a = 1"));
    }

    #[test]
    fn an_empty_where_sends_nothing_and_a_bad_one_says_so_when_sent() {
        assert!(Where::new().is_empty());
        assert_eq!(Where::new().to_query().unwrap(), None);
        assert_eq!(Where::text("   ").to_value().unwrap(), None);

        let bad = Where::new().and("data.kind", "~", json!("x"));
        assert!(!bad.is_empty());
        assert!(bad
            .to_query()
            .unwrap_err()
            .to_string()
            .contains("unknown op"));

        let mixed = Where::new()
            .and("data.a", "=", json!(1))
            .and_text("data.b = 2");
        assert!(mixed
            .to_value()
            .unwrap_err()
            .to_string()
            .contains("not both"));
    }

    #[test]
    fn a_link_names_its_message_as_the_contract_says() {
        let popped = LinkFrom::by_id("123", 42, "t1").consumer_group("billing");
        assert_eq!(
            serde_json::to_value(&popped).unwrap(),
            json!({"partitionId": "123", "offset": 42, "transactionId": "t1", "consumerGroup": "billing"})
        );
        let named = LinkFrom::by_name("orders", "order-1", 7, "t2");
        assert_eq!(
            serde_json::to_string(&named).unwrap(),
            r#"{"queue":"orders","partition":"order-1","offset":7,"transactionId":"t2"}"#
        );
        assert_eq!(
            LinkFrom::by_id("1", 0, "t").consumer_group(""),
            LinkFrom::by_id("1", 0, "t"),
            "no group is no key"
        );
    }

    fn message(offset: Option<i64>) -> Message {
        Message {
            id: "m".into(),
            transaction_id: "t1".into(),
            trace_id: None,
            data: json!(null),
            producer_sub: None,
            headers: None,
            created_at: "2026-11-12T10:00:00.000Z".into(),
            partition_id: "123".into(),
            partition: "order-1".into(),
            lease_id: "L1".into(),
            consumer_group: "billing".into(),
            offset,
        }
    }

    #[test]
    fn a_popped_message_links_by_its_partition_id_offset_and_group() {
        assert_eq!(
            message(Some(42)).link_from().unwrap(),
            LinkFrom::by_id("123", 42, "t1").consumer_group("billing")
        );
        let err = message(None).link_from().unwrap_err().to_string();
        assert!(err.contains("carries no offset"), "{err}");
        let mut no_pid = message(Some(1));
        no_pid.partition_id.clear();
        assert!(no_pid.link_from().is_err());
        // Through a list of references too, as `link(.., &refs)` passes them.
        let m = message(Some(1));
        let refs: Vec<&Message> = vec![&m];
        assert!(refs.iter().all(|r| LinkSource::link_from(r).is_ok()));
    }
}
