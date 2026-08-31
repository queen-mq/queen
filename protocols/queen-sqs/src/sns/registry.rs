//! The SNS half of the registry: topics under `qs:t:` and subscriptions under
//! `qs:s:`, in Queen's key/value store.
//!
//! CONTRACT. A second `impl` block on [`Registry`] rather than a second object,
//! because there is one store, one credential and one connection to Queen, and
//! because the two halves share the rules that make the whole thing work:
//!
//!   * **records are `forever`**, and nothing here has a TTL — a topic is not
//!     derived state, and expiry is mandatory on every write (024_kv.sql §5.1),
//!     so the choice has to be made rather than defaulted;
//!   * **every mutation is a compare-and-set.** Two instances behind one load
//!     balancer is the NORMAL case, and last-writer-wins on an attribute set
//!     would silently drop one client's `FilterPolicy`. A lost CAS re-applies
//!     onto the WINNER — which the answer already carries — and writes once
//!     more; losing twice is `ServiceUnavailable`, the one code that means "this
//!     may work if you send it again";
//!   * **key components are escaped** ([`crate::registry::escape`], kafka's
//!     rule), so no topic name can address another topic's subscriptions and no
//!     subscription id can reach out of its own topic.
//!
//! ## There is no index, and that is a decision
//!
//! `ListSubscriptionsByTopic` is a PREFIX READ over `qs:s:<topic>:` and nothing
//! else. The alternative — a per-topic index key holding the id list — is one
//! more thing to keep consistent under a race, for a listing the store already
//! answers in key order. The cost is that the prefix must be exact, which is
//! what the trailing separator in [`Registry::key_subscriptions`] buys.
//!
//! ## One thing here is cached, and it says so
//!
//! The queue record has a three-second cache because it stands between a
//! `SendMessage` and the broker on every request. A TOPIC is read by
//! administrative actions, which are rare and whose whole point is to observe
//! the change the previous call made — so no topic read is cached.
//!
//! The one exception is [`Registry::subscriptions_cached`], which the publish
//! path calls on EVERY `Publish`: without it, a fanout costs a prefix scan per
//! message. It is a TTL cache and not a version-checked one, and the difference
//! is a fact about the store rather than a shortcut — a subscription list is a
//! KEY RANGE, and a range has no version to compare. The only cheap check
//! available would be a per-topic generation key written by every `Subscribe`,
//! which is a WRITE on the administrative path to save a READ on the publish
//! path, and the read is one scan. So instead:
//!
//!   * a mutation made through THIS instance clears its own entry
//!     immediately ([`Registry::forget_subscriptions`], called by every action
//!     in [`super::admin`] that changes one), so provision-then-publish against
//!     one facade is exact;
//!   * a mutation made through ANOTHER instance is visible within the TTL,
//!     which is three seconds and far inside SNS's own documented eventual
//!     consistency for subscription and filter-policy changes.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use crate::error::ErrorKind;
use crate::queen::{self, KvAnswer, KvOp, KvRow, Result};
use crate::registry::{one, refuse, string_map, Registry, RegistryError, RegistryResult, NS};

/// How many topics or subscriptions one page answers. AWS's own page size for
/// all three SNS listings, and it is not configurable there — a client's
/// paginator is written against it.
pub const PAGE: usize = 100;

/// Records one unpaginated scan will read before it gives up. It bounds the
/// three internal walks — the `Subscribe` duplicate check, the `DeleteTopic`
/// cascade and the subscription counts on `GetTopicAttributes` — none of which
/// a client paginates, so each needs a ceiling of its own.
pub const MAX_SCANNED: usize = 10_000;

/// A topic past [`MAX_SCANNED`] subscriptions is a standing condition, and the
/// scan behind it runs once per cache miss per topic — so the line that reports
/// it is rate-limited exactly like the publish path's own
/// ([`super::publish`]).
static TRUNCATED_SCAN: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// How many topics' subscription lists one instance keeps
/// ([`Registry::subscriptions_cached`]). The same number as the queue cache's,
/// for the same reason: a ceiling rather than a slow leak.
const MAX_CACHED_TOPICS: usize = 4_096;

/// `RawMessageDelivery`: whether a delivery carries the message alone instead of
/// the SNS notification envelope. Read by the publish path, so it is derived
/// from the attribute map rather than stored twice.
pub const ATTR_RAW_MESSAGE_DELIVERY: &str = "RawMessageDelivery";
/// The filter policy document.
pub const ATTR_FILTER_POLICY: &str = "FilterPolicy";
/// Whether the policy is matched against `MessageAttributes` (the default) or
/// `MessageBody`.
pub const ATTR_FILTER_POLICY_SCOPE: &str = "FilterPolicyScope";
/// The default scope, which AWS reports whether or not it was set.
pub const FILTER_SCOPE_DEFAULT: &str = "MessageAttributes";

// ------------------------------------------------------------------ records

/// One SNS topic, as this facade knows it.
///
/// `attributes` is the SNS attribute map verbatim — the client's own vocabulary
/// — and never parsed fields, for [`crate::registry::QueueRecord`]'s reason:
/// `GetTopicAttributes` has to answer what was SET, including the documents this
/// facade accepts and does not enforce (`Policy`, `DeliveryPolicy`), and a
/// record of parsed fields cannot.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TopicRecord {
    pub name: String,
    pub attributes: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    /// Derived from the `.fifo` suffix, which is the whole declaration — never
    /// stored, so no row can disagree with its own name.
    pub fifo: bool,
    pub created_ms: i64,
    /// `arn:aws:sns:<region>:<account>:<name>`, as it was minted at create.
    /// STORED rather than re-derived: it pins the account the topic was created
    /// under, so an operator who changes `QUEEN_SQS_ACCOUNT` does not silently
    /// re-label every ARN in a client's configuration.
    pub arn: String,
    /// The KV version this record was read at, for the CAS on the next write. 0
    /// means "was not there".
    pub version: i64,
}

impl TopicRecord {
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            "attributes": self.attributes,
            "tags": self.tags,
            "createdTs": self.created_ms,
            "arn": self.arn,
        })
    }

    /// Read a stored row back. LENIENT, for the reason the queue record is: the
    /// KEY is the existence claim, so a row with a field this facade cannot read
    /// is still a topic that exists, and dropping it would hide a topic clients
    /// are publishing to.
    pub fn from_value(name: &str, value: &Value, version: i64) -> TopicRecord {
        TopicRecord {
            name: name.to_string(),
            attributes: string_map(value.get("attributes"), name, "attributes"),
            tags: string_map(value.get("tags"), name, "tags"),
            fifo: super::is_fifo_topic(name),
            created_ms: value
                .get("createdTs")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            arn: value
                .get("arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            version,
        }
    }
}

/// One SNS subscription.
///
/// The `protocol` is stored for every kind, not only for `sqs`, so that M6's
/// HTTP/S subscriptions are a new DELIVERY PATH and not a new record.
///
/// `endpoint` is the string the client sent — for `sqs` that is the queue's ARN,
/// which is what `GetSubscriptionAttributes` must answer back. The queue NAME is
/// derived from it through [`crate::registry::Naming::name_of_arn`] at the point
/// of use; storing the name instead would answer a different string than the one
/// that was subscribed.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionRecord {
    pub topic: String,
    pub id: String,
    pub protocol: String,
    pub endpoint: String,
    /// The account, which is this deployment's — cross-account anything is out
    /// of PLAN_QUEEN_SQS.md by name. Stored because AWS reports it as an
    /// attribute of the subscription and a client may compare it.
    pub owner: String,
    /// `RawMessageDelivery`, `FilterPolicy`, `FilterPolicyScope`, and whatever
    /// else was accepted and stored.
    pub attributes: BTreeMap<String, String>,
    pub created_ms: i64,
    pub arn: String,
    pub version: i64,
}

impl SubscriptionRecord {
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            "protocol": self.protocol,
            "endpoint": self.endpoint,
            "owner": self.owner,
            "attributes": self.attributes,
            "createdTs": self.created_ms,
            "arn": self.arn,
        })
    }

    pub fn from_value(topic: &str, id: &str, value: &Value, version: i64) -> SubscriptionRecord {
        let text = |field: &str| {
            value
                .get(field)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        SubscriptionRecord {
            topic: topic.to_string(),
            id: id.to_string(),
            protocol: text("protocol"),
            endpoint: text("endpoint"),
            owner: text("owner"),
            attributes: string_map(value.get("attributes"), id, "attributes"),
            created_ms: value
                .get("createdTs")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            arn: text("arn"),
            version,
        }
    }

    /// Whether this subscription takes the message alone instead of the
    /// notification envelope. DERIVED from the attribute map, which is the one
    /// source of truth — a stored boolean beside it could disagree with what
    /// `GetSubscriptionAttributes` answers.
    pub fn raw_message_delivery(&self) -> bool {
        self.attributes
            .get(ATTR_RAW_MESSAGE_DELIVERY)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// Whether a `FilterPolicy` was SET, whatever it says.
    ///
    /// Separate from [`SubscriptionRecord::filter_policy`] because the two
    /// absences are different absences and the publish path answers them
    /// differently: no policy takes everything, an unreadable policy takes
    /// nothing ([`super::publish::Prepared::wanted_by`]). A single
    /// `Option<Value>` cannot say which of the two it is.
    pub fn has_filter_policy(&self) -> bool {
        self.attributes
            .get(ATTR_FILTER_POLICY)
            .is_some_and(|text| !text.trim().is_empty())
    }

    /// The filter policy as JSON, or `None` when there is none to apply and when
    /// the stored document is not one this facade can read.
    ///
    /// It never FAILS a publish — the publish path decides what an unreadable
    /// policy means, and it is not this function's decision to make. The write
    /// path is where a policy is validated ([`super::admin`],
    /// [`super::filter::validate`]), so a stored document that does not parse can
    /// only be one written before that validation existed.
    pub fn filter_policy(&self) -> Option<Value> {
        let text = self.attributes.get(ATTR_FILTER_POLICY)?;
        serde_json::from_str::<Value>(text)
            .ok()
            .filter(Value::is_object)
    }

    /// `MessageAttributes` unless the subscription says `MessageBody`.
    pub fn filter_scope(&self) -> &str {
        self.attributes
            .get(ATTR_FILTER_POLICY_SCOPE)
            .map_or(FILTER_SCOPE_DEFAULT, String::as_str)
    }
}

/// A conditional write on a topic: `Ok` is the record as it now stands, `Err` is
/// the WINNER's — which the answer carries, so a caller that lost can compare
/// without a second read. Boxed for [`crate::registry::Cas`]'s reason: losing is
/// the cold path and a bare `Result` would make every success pay for it.
pub type TopicCas = std::result::Result<TopicRecord, Box<TopicRecord>>;

/// One page of topics.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TopicPage {
    pub topics: Vec<TopicRecord>,
    /// The opaque cursor a client hands back as `NextToken`. `None` is the end
    /// of the listing and is the ONLY thing that means it.
    pub next_token: Option<String>,
}

/// One page of subscriptions.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SubscriptionPage {
    pub subscriptions: Vec<SubscriptionRecord>,
    pub next_token: Option<String>,
}

// -------------------------------------------------------------- the surface

impl Registry {
    /// One topic, or `None`. Always a fresh read — see the module header on why
    /// nothing here is cached.
    pub async fn topic(&self, name: &str, token: Option<&str>) -> Result<Option<TopicRecord>> {
        let key = Registry::key_topic(name);
        let answer = one(self.api.kv(&[KvOp::get(NS, &key)], token).await?)?;
        Ok(match answer.found {
            false => None,
            true => Some(TopicRecord::from_value(name, &answer.value, answer.version)),
        })
    }

    /// The same read, refused with SNS's own 404 when the topic is not there —
    /// which is what every action that names a `TopicArn` needs first.
    pub async fn require_topic(
        &self,
        name: &str,
        token: Option<&str>,
    ) -> RegistryResult<TopicRecord> {
        match self.topic(name, token).await? {
            Some(record) => Ok(record),
            None => Err(missing_topic(name)),
        }
    }

    /// Claim a topic's key, refusing to overwrite one. Answers the WINNER's
    /// record when it loses, which is what `CreateTopic`'s idempotency needs in
    /// order to compare attributes without a second round trip.
    pub async fn create_topic(
        &self,
        record: &TopicRecord,
        token: Option<&str>,
    ) -> RegistryResult<TopicCas> {
        let value = record.to_value();
        // Before the call, so a document the store would refuse costs no round
        // trip and reads as the client's error that it is — the same guard the
        // mutating path applies, and reachable from the same unbounded
        // client-supplied `Policy` on the very first write.
        guard_size("Attributes", "topic's attributes and tags", &value)?;
        let key = Registry::key_topic(&record.name);
        let ops = [KvOp::put_if_absent(NS, &key, value)];
        let answer = one(self.api.kv(&ops, token).await?)?;
        Ok(settle_topic(record, answer))
    }

    /// `SetTopicAttributes`: merge, and remove the keys whose change is `None`.
    ///
    /// Never a replace — SNS sets ONE attribute per call and has no way to touch
    /// the others — and the removal is not a convenience: an omitted
    /// `AttributeValue` is the only spelling SNS has for unsetting an attribute,
    /// and storing `""` for it would answer an empty string where the service
    /// answers its default.
    pub async fn set_topic_attributes(
        &self,
        name: &str,
        changes: &BTreeMap<String, Option<String>>,
        token: Option<&str>,
    ) -> RegistryResult<TopicRecord> {
        self.mutate_topic(name, token, |record| {
            for (key, value) in changes {
                match value {
                    Some(value) => record.attributes.insert(key.clone(), value.clone()),
                    None => record.attributes.remove(key),
                };
            }
            Ok(())
        })
        .await
    }

    /// `TagResource`.
    ///
    /// The cap is checked INSIDE the compare-and-set, over the RESULT: fifty
    /// tags added ten at a time is still fifty-one on the sixth call, and a check
    /// before the write would be one two concurrent callers could both pass. The
    /// refusal therefore happens before anything is stored, which is the only
    /// order in which an over-cap answer is honest.
    pub async fn tag_topic(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        token: Option<&str>,
    ) -> RegistryResult<TopicRecord> {
        self.mutate_topic(name, token, |record| {
            for (key, value) in tags {
                record.tags.insert(key.clone(), value.clone());
            }
            match record.tags.len() > super::MAX_TAGS {
                true => Err(RegistryError::Refused(crate::error::SqsError::new(
                    ErrorKind::TagLimitExceeded,
                ))),
                false => Ok(()),
            }
        })
        .await
    }

    /// `UntagResource`. A key that is not there is not an error: AWS's untag is
    /// idempotent, and a fleet re-running its provisioning would otherwise fail
    /// the second time.
    pub async fn untag_topic(
        &self,
        name: &str,
        keys: &[String],
        token: Option<&str>,
    ) -> RegistryResult<TopicRecord> {
        self.mutate_topic(name, token, |record| {
            for key in keys {
                record.tags.remove(key);
            }
            Ok(())
        })
        .await
    }

    /// Remove a topic's own key. Answers whether it was there — which
    /// `DeleteTopic` deliberately does NOT report, because AWS's delete is
    /// idempotent.
    pub async fn delete_topic(&self, name: &str, token: Option<&str>) -> Result<bool> {
        let key = Registry::key_topic(name);
        let answer = one(self.api.kv(&[KvOp::delete(NS, &key, None)], token).await?)?;
        Ok(answer.applied())
    }

    /// `ListTopics`: one page, with the opaque `NextToken` a client hands back.
    pub async fn list_topics(
        &self,
        limit: usize,
        next_token: Option<&str>,
        token: Option<&str>,
    ) -> RegistryResult<TopicPage> {
        let prefix = Registry::key_topics();
        let after = cursor_after(next_token, prefix)?;
        let (rows, more) = self
            .walk_rows(prefix, limit.max(1), after, token, "topics")
            .await?;
        let last = rows.last().map(|row| row.key.clone());
        let topics = rows
            .iter()
            .filter_map(|row| {
                Registry::topic_of_key(&row.key)
                    .map(|name| TopicRecord::from_value(&name, &row.value, row.version))
            })
            .collect();
        Ok(TopicPage {
            topics,
            next_token: more.then(|| last.map(|key| encode_cursor(&key))).flatten(),
        })
    }

    /// One subscription, or `None`.
    pub async fn subscription(
        &self,
        topic: &str,
        id: &str,
        token: Option<&str>,
    ) -> Result<Option<SubscriptionRecord>> {
        let key = Registry::key_subscription(topic, id);
        let answer = one(self.api.kv(&[KvOp::get(NS, &key)], token).await?)?;
        Ok(match answer.found {
            false => None,
            true => Some(SubscriptionRecord::from_value(
                topic,
                id,
                &answer.value,
                answer.version,
            )),
        })
    }

    /// Write a subscription's key, refusing to overwrite one.
    ///
    /// `putIfAbsent` even though the id is a fresh v4 UUID that cannot collide:
    /// the guard costs nothing and it is what keeps a defect in whoever mints
    /// the id from silently replacing a live subscriber's filter policy.
    pub async fn create_subscription(
        &self,
        record: &SubscriptionRecord,
        token: Option<&str>,
    ) -> RegistryResult<bool> {
        let value = record.to_value();
        // A `FilterPolicy` is an unbounded client-supplied document and SNS's
        // own ceiling for one is above this store's, so an oversized policy is
        // reachable from a request rather than only from a defect.
        guard_size("Attributes", "subscription's attributes", &value)?;
        let key = Registry::key_subscription(&record.topic, &record.id);
        let ops = [KvOp::put_if_absent(NS, &key, value)];
        Ok(one(self.api.kv(&ops, token).await?)?.applied())
    }

    /// `SetSubscriptionAttributes`: merge, and remove the keys whose change is
    /// `None` — which is how a `FilterPolicy` is cleared.
    pub async fn set_subscription_attributes(
        &self,
        topic: &str,
        id: &str,
        changes: &BTreeMap<String, Option<String>>,
        token: Option<&str>,
    ) -> RegistryResult<SubscriptionRecord> {
        let mut current = self
            .subscription(topic, id, token)
            .await?
            .ok_or_else(|| missing_subscription(topic, id))?;
        for _ in 0..2 {
            let mut next = current.clone();
            for (key, value) in changes {
                match value {
                    Some(value) => next.attributes.insert(key.clone(), value.clone()),
                    None => next.attributes.remove(key),
                };
            }
            let value = next.to_value();
            guard_size("AttributeValue", "subscription's attributes", &value)?;
            let key = Registry::key_subscription(topic, id);
            let ops = [KvOp::put_expecting(NS, &key, value, next.version)];
            let answer = one(self.api.kv(&ops, token).await?)?;
            if answer.applied() {
                next.version = answer.version;
                return Ok(next);
            }
            // Version 0 is not a competitor, it is an ABSENCE: the subscription
            // was removed between the read and the write, and re-applying would
            // resurrect it.
            if answer.version == 0 {
                return Err(missing_subscription(topic, id));
            }
            current = SubscriptionRecord::from_value(topic, id, &answer.value, answer.version);
        }
        Err(refuse(
            ErrorKind::ServiceUnavailable,
            format!("Concurrent updates to subscription {id}; please retry"),
        ))
    }

    /// Remove one subscription. Answers whether it was there.
    pub async fn delete_subscription(
        &self,
        topic: &str,
        id: &str,
        token: Option<&str>,
    ) -> Result<bool> {
        let key = Registry::key_subscription(topic, id);
        let answer = one(self.api.kv(&[KvOp::delete(NS, &key, None)], token).await?)?;
        Ok(answer.applied())
    }

    /// `ListSubscriptions` (`topic` = `None`) and `ListSubscriptionsByTopic`.
    ///
    /// ONE function for both, because they are one prefix read at two depths:
    /// `qs:s:` against `qs:s:<topic>:`. A separate implementation per action
    /// would be a second cursor format for a client to be confused by.
    pub async fn list_subscriptions(
        &self,
        topic: Option<&str>,
        limit: usize,
        next_token: Option<&str>,
        token: Option<&str>,
    ) -> RegistryResult<SubscriptionPage> {
        let prefix = match topic {
            Some(topic) => Registry::key_subscriptions(topic),
            None => Registry::key_all_subscriptions().to_string(),
        };
        let after = cursor_after(next_token, &prefix)?;
        let (rows, more) = self
            .walk_rows(&prefix, limit.max(1), after, token, "subscriptions")
            .await?;
        let last = rows.last().map(|row| row.key.clone());
        Ok(SubscriptionPage {
            subscriptions: records_of(&rows),
            next_token: more.then(|| last.map(|key| encode_cursor(&key))).flatten(),
        })
    }

    /// Every subscription of one topic, up to [`MAX_SCANNED`], in one call.
    ///
    /// The unpaginated read the facade itself needs: the duplicate check in
    /// `Subscribe`, the cascade in `DeleteTopic`, the counts on
    /// `GetTopicAttributes`, and — next — the fanout at publish. It is bounded
    /// rather than unbounded because it runs inside ONE client request.
    pub async fn subscriptions_of(
        &self,
        topic: &str,
        token: Option<&str>,
    ) -> Result<Vec<SubscriptionRecord>> {
        let prefix = Registry::key_subscriptions(topic);
        let (rows, more) = self
            .walk_rows(&prefix, MAX_SCANNED, None, token, "subscriptions")
            .await?;
        if more {
            // Named rather than silently truncated. DIVERGENCE, `accepted`: past
            // [`MAX_SCANNED`] subscriptions on one topic the counts are
            // approximate, a duplicate check can miss, and — the one that
            // matters — the fan-out delivers to the same PREFIX of the key range
            // on every publish while the rest of the subscribers never receive
            // anything. The publish path cannot tell a truncated list from a
            // complete one (a bounded read has no other shape to answer), so
            // this line is the whole of the signal and it is SAMPLED, like every
            // other log this feature repeats per publish: a topic at the ceiling
            // must not also be a log flood.
            if let Some(suppressed) = TRUNCATED_SCAN.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    topic,
                    scanned = rows.len(),
                    "topic has more subscriptions than one scan reads; publishes deliver to the \
                     first {} only",
                    MAX_SCANNED
                );
            }
        }
        Ok(records_of(&rows))
    }

    /// [`Registry::subscriptions_of`], through a short-lived cache — the read
    /// every `Publish` starts with. See the module header on why the freshness
    /// is a TTL and not a version.
    ///
    /// The entry is keyed by CREDENTIAL as well as by topic, for the queue
    /// cache's reason: the key space is per-tenant on the broker side, so two
    /// tenants have different subscriptions under one topic name.
    pub async fn subscriptions_cached(
        &self,
        topic: &str,
        token: Option<&str>,
    ) -> Result<Arc<Vec<SubscriptionRecord>>> {
        let key = (queen::CredentialKey::of(token), topic.to_string());
        {
            let cache = self.subscription_cache().lock().unwrap();
            if let Some(entry) = cache
                .get(&key)
                .filter(|entry| entry.at.elapsed() < self.ttl())
            {
                return Ok(Arc::clone(&entry.records));
            }
        }
        let records = Arc::new(self.subscriptions_of(topic, token).await?);
        if !self.ttl().is_zero() {
            let mut cache = self.subscription_cache().lock().unwrap();
            // The same bound and the same eviction as the queue cache: a
            // deployment with more topics than this has a memory ceiling, not a
            // slow leak.
            if cache.len() >= MAX_CACHED_TOPICS && !cache.contains_key(&key) {
                if let Some(coldest) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.at)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&coldest);
                }
            }
            cache.insert(
                key,
                crate::registry::CachedSubscriptions {
                    records: Arc::clone(&records),
                    at: std::time::Instant::now(),
                },
            );
        }
        Ok(records)
    }

    /// Forget one topic's subscription list under EVERY credential.
    ///
    /// Called by every action that changes a subscription, so that the instance
    /// which made the change publishes against it on the very next request. The
    /// cost of being wrong about which credential owns the topic is one extra
    /// scan; the cost of NOT doing it is a `Subscribe` followed by a `Publish`
    /// that skips the new subscriber, which is the exact sequence every
    /// framework performs at start-up.
    pub fn forget_subscriptions(&self, topic: &str) {
        self.subscription_cache()
            .lock()
            .unwrap()
            .retain(|(_, name), _| name != topic);
    }

    /// Delete every subscription of one topic. Answers how many went.
    ///
    /// Batched, because a topic with a hundred subscribers is a hundred keys and
    /// one call per key is a hundred round trips inside one `DeleteTopic`. NOT
    /// atomic with the topic's own delete: the store has no transaction across
    /// this many keys, so the ORDER carries the guarantee instead — see
    /// [`super::admin::delete_topic`].
    pub async fn delete_topic_subscriptions(
        &self,
        topic: &str,
        token: Option<&str>,
    ) -> Result<usize> {
        let subscriptions = self.subscriptions_of(topic, token).await?;
        let mut deleted = 0;
        for chunk in subscriptions.chunks(queen::MAX_KV_OPS_PER_CALL) {
            let ops: Vec<KvOp> = chunk
                .iter()
                .map(|s| KvOp::delete(NS, &Registry::key_subscription(topic, &s.id), None))
                .collect();
            for answer in self.api.kv(&ops, token).await? {
                deleted += usize::from(answer.applied());
            }
        }
        Ok(deleted)
    }

    /// Read, apply, write under a CAS; on a lost CAS re-apply onto the WINNER
    /// and write once more. Two attempts and no more, for
    /// [`Registry::set_attributes`]'s reason: a third is an unbounded loop under
    /// exactly the load that produced the contention.
    async fn mutate_topic<F>(
        &self,
        name: &str,
        token: Option<&str>,
        change: F,
    ) -> RegistryResult<TopicRecord>
    where
        F: Fn(&mut TopicRecord) -> RegistryResult<()>,
    {
        let mut current = self
            .topic(name, token)
            .await?
            .ok_or_else(|| missing_topic(name))?;
        for _ in 0..2 {
            let mut next = current.clone();
            change(&mut next)?;
            let value = next.to_value();
            guard_size("Attributes", "topic's attributes and tags", &value)?;
            let key = Registry::key_topic(name);
            let ops = [KvOp::put_expecting(NS, &key, value, next.version)];
            let answer = one(self.api.kv(&ops, token).await?)?;
            match settle_topic(&next, answer) {
                Ok(stored) => return Ok(stored),
                // An ABSENCE, not a competitor: the topic was deleted between
                // the read and the write, and re-applying would resurrect it.
                Err(winner) if winner.version == 0 => return Err(missing_topic(name)),
                Err(winner) => current = *winner,
            }
        }
        Err(refuse(
            ErrorKind::ServiceUnavailable,
            format!("Concurrent updates to topic {name}; please retry"),
        ))
    }
}

// ------------------------------------------------------------------ helpers

/// Turn one write's answer into a [`TopicCas`].
fn settle_topic(wrote: &TopicRecord, answer: KvAnswer) -> TopicCas {
    if answer.applied() {
        return Ok(TopicRecord {
            version: answer.version,
            ..wrote.clone()
        });
    }
    Err(Box::new(TopicRecord::from_value(
        &wrote.name,
        &answer.value,
        answer.version,
    )))
}

fn records_of(rows: &[KvRow]) -> Vec<SubscriptionRecord> {
    rows.iter()
        .filter_map(|row| {
            let (topic, id) = Registry::subscription_of_key(&row.key)?;
            Some(SubscriptionRecord::from_value(
                &topic,
                &id,
                &row.value,
                row.version,
            ))
        })
        .collect()
}

/// The one sentence a missing topic gets, from every entry point, so a client
/// cannot tell which call could not find it.
pub(crate) fn missing_topic(name: &str) -> RegistryError {
    RegistryError::Refused(super::not_found(format!("Topic does not exist: {name}")))
}

pub(crate) fn missing_subscription(topic: &str, id: &str) -> RegistryError {
    RegistryError::Refused(super::not_found(format!(
        "Subscription does not exist: {topic}:{id}"
    )))
}

/// Refuse a record the store would refuse anyway. `Policy`, `DeliveryPolicy`
/// and `FilterPolicy` are unbounded client-supplied documents, so this is
/// reachable from a request rather than only from a defect — and a 400 from the
/// broker would surface as a server fault (`InvalidParameterValue`, "upstream
/// status 400", SQS's own code inside an SNS answer) instead of the client
/// error it is.
///
/// EVERY write that carries a client document goes through it — both creates,
/// both mutations — because the ceiling belongs to the store and not to one
/// path, and the first write is where the largest document usually arrives.
fn guard_size(member: &str, what: &str, value: &Value) -> RegistryResult<()> {
    let bytes = serde_json::to_string(value).map_or(usize::MAX, |s| s.len());
    if bytes > queen::MAX_KV_VALUE_BYTES {
        return Err(RegistryError::Refused(super::invalid(
            member,
            format!(
                "the {what} are {bytes} bytes, over the {} the registry stores",
                queen::MAX_KV_VALUE_BYTES
            ),
        )));
    }
    Ok(())
}

/// `NextToken`: the last KEY of a page, base64. URL-safe and unpadded because it
/// travels in a query string on the Query protocol, where `+` is a space and `=`
/// is a separator.
///
/// The KEY and not the name, which is where this differs from the queue
/// listing's token: a subscription is addressed by TWO components, so a
/// name-only cursor cannot name one. Decoding is checked against the prefix
/// being walked ([`cursor_after`]), so a token cannot be edited into a read
/// outside it.
pub fn encode_cursor(key: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key)
}

/// The exclusive `after` a `NextToken` names, refused when it is not one this
/// facade minted FOR THIS LISTING.
///
/// `InvalidParameter` rather than a silent restart from the top: a client that
/// paged past a bad token would loop over page one for ever.
fn cursor_after(next_token: Option<&str>, prefix: &str) -> RegistryResult<Option<String>> {
    use base64::Engine;
    let Some(cursor) = next_token.filter(|c| !c.is_empty()) else {
        return Ok(None);
    };
    let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|key| key.starts_with(prefix));
    match key {
        Some(key) => Ok(Some(key)),
        None => Err(RegistryError::Refused(super::invalid(
            "NextToken",
            "the token is not one this endpoint issued for this listing",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::QueenApi;
    use crate::registry::Naming;
    use std::sync::Arc;
    use std::time::Duration;

    fn registry(api: &Arc<FakeQueen>) -> Registry {
        Registry::with_ttl(api.clone(), Duration::ZERO)
    }

    fn naming() -> Naming {
        Naming::new("queen-1", "000000000000")
    }

    fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn topic(name: &str, attributes: &[(&str, &str)]) -> TopicRecord {
        TopicRecord {
            name: name.to_string(),
            attributes: attrs(attributes),
            tags: BTreeMap::new(),
            fifo: super::super::is_fifo_topic(name),
            created_ms: 1_787_011_200_000,
            arn: naming().topic_arn(name),
            version: 0,
        }
    }

    fn subscription(topic: &str, id: &str, endpoint: &str) -> SubscriptionRecord {
        SubscriptionRecord {
            topic: topic.to_string(),
            id: id.to_string(),
            protocol: "sqs".to_string(),
            endpoint: endpoint.to_string(),
            owner: "000000000000".to_string(),
            attributes: BTreeMap::new(),
            created_ms: 1_787_011_200_000,
            arn: naming().subscription_arn(topic, id),
            version: 0,
        }
    }

    /// A record survives the store: everything a later read needs is in the
    /// value, and everything derivable from the KEY is derived rather than
    /// stored twice.
    #[tokio::test]
    async fn a_topic_round_trips_through_the_store() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        let wrote = topic("events", &[("DisplayName", "Events")]);
        let stored = reg
            .create_topic(&wrote, None)
            .await
            .expect("the store answered")
            .expect("the key was free");
        assert!(stored.version > 0, "a stored record carries its version");

        let read = reg
            .topic("events", None)
            .await
            .expect("the store answered")
            .expect("the topic is there");
        assert_eq!(read.attributes, wrote.attributes);
        assert_eq!(read.arn, "arn:aws:sns:queen-1:000000000000:events");
        assert_eq!(read.created_ms, wrote.created_ms);
        assert!(!read.fifo);
        assert_eq!(read.version, stored.version);
        // A name nobody created is `None` and never an error: the two are
        // different answers and only one of them is `NotFound`.
        assert_eq!(reg.topic("absent", None).await.expect("answered"), None);
    }

    /// The `.fifo` fact is DERIVED, so a row cannot disagree with its own name.
    #[tokio::test]
    async fn a_fifo_topic_is_declared_by_its_key() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("orders.fifo", &[("FifoTopic", "true")]), None)
            .await
            .expect("answered")
            .expect("free");
        let read = reg.topic("orders.fifo", None).await.unwrap().unwrap();
        assert!(read.fifo);
        assert_eq!(
            read.attributes.get("FifoTopic").map(String::as_str),
            Some("true")
        );
    }

    /// The second create loses and is handed the WINNER, which is what
    /// `CreateTopic`'s idempotency compares against without a second read.
    #[tokio::test]
    async fn a_lost_create_answers_the_winners_record() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[("DisplayName", "First")]), None)
            .await
            .expect("answered")
            .expect("free");
        let winner = reg
            .create_topic(&topic("events", &[("DisplayName", "Second")]), None)
            .await
            .expect("answered")
            .expect_err("the key was taken");
        assert_eq!(
            winner.attributes.get("DisplayName").map(String::as_str),
            Some("First"),
            "the loser is handed what is actually stored"
        );
        assert!(winner.version > 0);
    }

    /// A mutation MERGES and keeps its version moving, and a mutation of a topic
    /// that is not there is `NotFound` rather than a resurrection.
    #[tokio::test]
    async fn setting_an_attribute_merges_onto_what_is_stored() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[("DisplayName", "Events")]), None)
            .await
            .expect("answered")
            .expect("free");
        let policy: BTreeMap<String, Option<String>> =
            [("Policy".to_string(), Some("{}".to_string()))]
                .into_iter()
                .collect();
        let updated = reg
            .set_topic_attributes("events", &policy, None)
            .await
            .expect("the change landed");
        assert_eq!(
            updated.attributes.get("DisplayName").map(String::as_str),
            Some("Events"),
            "an attribute nobody named is untouched"
        );
        assert_eq!(
            updated.attributes.get("Policy").map(String::as_str),
            Some("{}")
        );

        // A change of `None` REMOVES, which is SNS's only way to unset one.
        let cleared: BTreeMap<String, Option<String>> =
            [("DisplayName".to_string(), None)].into_iter().collect();
        let updated = reg
            .set_topic_attributes("events", &cleared, None)
            .await
            .expect("the change landed");
        assert_eq!(updated.attributes.get("DisplayName"), None);
        assert_eq!(
            updated.attributes.get("Policy").map(String::as_str),
            Some("{}")
        );

        let e = reg
            .set_topic_attributes("absent", &policy, None)
            .await
            .expect_err("no such topic");
        assert_eq!(e.kind(), Some(ErrorKind::NotFound));
    }

    #[tokio::test]
    async fn tags_are_merged_and_removed_key_by_key() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[]), None)
            .await
            .expect("answered")
            .expect("free");
        reg.tag_topic(
            "events",
            &attrs(&[("team", "billing"), ("env", "prod")]),
            None,
        )
        .await
        .expect("tagged");
        reg.tag_topic("events", &attrs(&[("env", "stage")]), None)
            .await
            .expect("retagged");
        let read = reg.topic("events", None).await.unwrap().unwrap();
        assert_eq!(read.tags.get("team").map(String::as_str), Some("billing"));
        assert_eq!(read.tags.get("env").map(String::as_str), Some("stage"));

        // Untagging a key that is not there is a success, because AWS's untag is
        // idempotent and a provisioner re-runs.
        reg.untag_topic("events", &["env".to_string(), "absent".to_string()], None)
            .await
            .expect("untagged");
        let read = reg.topic("events", None).await.unwrap().unwrap();
        assert_eq!(read.tags.keys().collect::<Vec<_>>(), vec!["team"]);
    }

    /// A subscription is keyed by its topic AND its id, and the prefix read is
    /// what makes a per-topic listing possible without an index.
    #[tokio::test]
    async fn subscriptions_are_listed_by_their_topics_prefix() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for (topic_name, id) in [("events", "a"), ("events", "b"), ("events-2", "c")] {
            assert!(reg
                .create_subscription(
                    &subscription(topic_name, id, "arn:aws:sqs:queen-1:000000000000:orders"),
                    None
                )
                .await
                .expect("answered"));
        }
        let of_events = reg.subscriptions_of("events", None).await.expect("scanned");
        assert_eq!(of_events.len(), 2, "{of_events:?}");
        // THE property the prefix rests on: a topic whose name is another's
        // prefix cannot read its subscriptions.
        let of_two = reg
            .subscriptions_of("events-2", None)
            .await
            .expect("scanned");
        assert_eq!(of_two.len(), 1);
        assert_eq!(of_two[0].id, "c");

        // ...and the unscoped listing sees all three.
        let all = reg
            .list_subscriptions(None, 100, None, None)
            .await
            .expect("listed");
        assert_eq!(all.subscriptions.len(), 3);
        assert_eq!(all.next_token, None, "a complete page has no cursor");
    }

    /// The paging contract, on both listings: a full page carries a cursor, the
    /// next call continues from it exclusively, and the last page carries none.
    #[tokio::test]
    async fn a_listing_pages_and_the_cursor_is_exclusive() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for name in ["a", "b", "c"] {
            reg.create_topic(&topic(name, &[]), None)
                .await
                .expect("answered")
                .expect("free");
        }
        let first = reg.list_topics(2, None, None).await.expect("listed");
        assert_eq!(
            first
                .topics
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        let cursor = first.next_token.expect("a full page carries a cursor");
        let second = reg
            .list_topics(2, Some(&cursor), None)
            .await
            .expect("listed");
        assert_eq!(
            second
                .topics
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c"],
            "the cursor is exclusive"
        );
        assert_eq!(second.next_token, None, "the last page ends the listing");
    }

    /// A cursor is checked against the prefix it was minted for. A token from
    /// another listing — or one a client edited — is refused, never served as a
    /// read outside its own key space.
    #[tokio::test]
    async fn a_cursor_from_another_listing_is_refused() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[]), None)
            .await
            .expect("answered")
            .expect("free");
        let foreign = encode_cursor(&Registry::key_queue("orders"));
        let e = reg
            .list_topics(10, Some(&foreign), None)
            .await
            .expect_err("refused");
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameter));
        // Not base64 at all, and base64 of something that is not a key.
        for bad in ["!!!!", &encode_cursor("qs:s:events:a")] {
            assert_eq!(
                reg.list_topics(10, Some(bad), None)
                    .await
                    .expect_err("refused")
                    .kind(),
                Some(ErrorKind::InvalidParameter),
                "{bad}"
            );
        }
        // ...and a subscription cursor is refused by the TOPIC listing and
        // accepted by its own.
        assert!(reg
            .list_subscriptions(None, 10, Some(&encode_cursor("qs:s:events:a")), None)
            .await
            .is_ok());
    }

    /// The cascade `DeleteTopic` runs, and the count it answers.
    #[tokio::test]
    async fn deleting_a_topics_subscriptions_removes_exactly_that_topics() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        for (topic_name, id) in [("events", "a"), ("events", "b"), ("other", "c")] {
            reg.create_subscription(
                &subscription(topic_name, id, "arn:aws:sqs:queen-1:000000000000:orders"),
                None,
            )
            .await
            .expect("answered");
        }
        assert_eq!(
            reg.delete_topic_subscriptions("events", None)
                .await
                .expect("deleted"),
            2
        );
        assert!(reg
            .subscriptions_of("events", None)
            .await
            .expect("scanned")
            .is_empty());
        assert_eq!(reg.subscriptions_of("other", None).await.unwrap().len(), 1);
        // Idempotent: a second cascade deletes nothing and does not fail.
        assert_eq!(
            reg.delete_topic_subscriptions("events", None)
                .await
                .expect("deleted"),
            0
        );
    }

    /// A subscription's attributes merge, and a change of `None` REMOVES —
    /// which is the only way a `FilterPolicy` is cleared.
    #[tokio::test]
    async fn a_subscription_attribute_can_be_set_and_cleared() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_subscription(
            &subscription("events", "a", "arn:aws:sqs:queen-1:000000000000:orders"),
            None,
        )
        .await
        .expect("answered");

        let changes: BTreeMap<String, Option<String>> = [
            (
                ATTR_RAW_MESSAGE_DELIVERY.to_string(),
                Some("true".to_string()),
            ),
            (
                ATTR_FILTER_POLICY.to_string(),
                Some(r#"{"kind":["order"]}"#.to_string()),
            ),
        ]
        .into_iter()
        .collect();
        let updated = reg
            .set_subscription_attributes("events", "a", &changes, None)
            .await
            .expect("the change landed");
        assert!(updated.raw_message_delivery());
        assert_eq!(
            updated.filter_policy(),
            Some(serde_json::json!({"kind": ["order"]}))
        );
        assert_eq!(updated.filter_scope(), FILTER_SCOPE_DEFAULT);

        let cleared: BTreeMap<String, Option<String>> = [(ATTR_FILTER_POLICY.to_string(), None)]
            .into_iter()
            .collect();
        let updated = reg
            .set_subscription_attributes("events", "a", &cleared, None)
            .await
            .expect("the change landed");
        assert_eq!(updated.filter_policy(), None);
        assert!(
            updated.raw_message_delivery(),
            "clearing one attribute leaves the others"
        );

        // A subscription that is not there is NotFound, not a resurrection.
        assert_eq!(
            reg.set_subscription_attributes("events", "gone", &cleared, None)
                .await
                .expect_err("no such subscription")
                .kind(),
            Some(ErrorKind::NotFound)
        );
    }

    /// The derived reads, including the one that must not fail a publish: a
    /// stored policy that no longer parses is `None`, never an error.
    #[test]
    fn the_derived_subscription_fields_come_from_the_attribute_map() {
        let mut record = subscription("events", "a", "arn:aws:sqs:queen-1:000000000000:orders");
        assert!(!record.raw_message_delivery());
        assert_eq!(record.filter_policy(), None);
        assert_eq!(record.filter_scope(), "MessageAttributes");

        record
            .attributes
            .insert(ATTR_RAW_MESSAGE_DELIVERY.to_string(), "TRUE".to_string());
        record.attributes.insert(
            ATTR_FILTER_POLICY_SCOPE.to_string(),
            "MessageBody".to_string(),
        );
        record
            .attributes
            .insert(ATTR_FILTER_POLICY.to_string(), "not json".to_string());
        assert!(record.raw_message_delivery());
        assert_eq!(record.filter_scope(), "MessageBody");
        assert_eq!(record.filter_policy(), None, "unparseable is not a policy");
        // A JSON scalar is not a policy either.
        record
            .attributes
            .insert(ATTR_FILTER_POLICY.to_string(), "7".to_string());
        assert_eq!(record.filter_policy(), None);
    }

    /// A row this facade cannot read is SKIPPED and never invented into a name a
    /// client would then address.
    #[tokio::test]
    async fn a_foreign_key_under_our_prefix_is_skipped() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[]), None)
            .await
            .expect("answered")
            .expect("free");
        // A subscription key with no separator: under the prefix, not one of
        // ours.
        api.kv(
            &[KvOp::put(NS, "qs:s:malformed", serde_json::json!({}))],
            None,
        )
        .await
        .expect("written");
        let page = reg
            .list_subscriptions(None, 100, None, None)
            .await
            .expect("listed");
        assert!(page.subscriptions.is_empty(), "{page:?}");
    }

    /// A topic whose documents do not fit the store is a CLIENT error, refused
    /// before the write rather than surfacing as the broker's 400.
    #[tokio::test]
    async fn an_oversized_topic_is_refused_as_the_clients_error() {
        let api = FakeQueen::empty();
        let reg = registry(&api);
        reg.create_topic(&topic("events", &[]), None)
            .await
            .expect("answered")
            .expect("free");
        let huge: BTreeMap<String, Option<String>> = [(
            "Policy".to_string(),
            Some("x".repeat(crate::queen::MAX_KV_VALUE_BYTES)),
        )]
        .into_iter()
        .collect();
        let e = reg
            .set_topic_attributes("events", &huge, None)
            .await
            .expect_err("refused");
        assert_eq!(e.kind(), Some(ErrorKind::InvalidParameter));
    }
}
