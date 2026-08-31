//! The SNS administrative surface: topics, subscriptions, and their tags.
//!
//! CONTRACT. One function per action, protocol-blind, answering the payload both
//! codecs render — [`crate::actions`]'s contract, and these live in [`super`]
//! rather than beside the queue actions only because SNS is a different service
//! with a different vocabulary, a different namespace and a different error
//! catalog.
//!
//! The rules that are decisions rather than translation:
//!
//!   * **`CreateTopic` is idempotent unless the request CONTRADICTS the topic
//!     that is there.** The comparison is one-directional and its existing side
//!     is the topic's EFFECTIVE attributes — the same two rules the queue side
//!     arrived at the hard way (`compat/M0_SMOKE.md` D1), for the same reason:
//!     what `GetTopicAttributes` answers must be what `CreateTopic` accepts
//!     back.
//!   * **`Subscribe` is idempotent per (topic, protocol, endpoint)** and never
//!     rewrites an existing subscription's attributes. See [`subscribe`].
//!   * **A subscription's endpoint must be a queue THIS deployment knows.** AWS
//!     accepts an endpoint it cannot see, because cross-account subscriptions are
//!     legal there; here one deployment is one account, so an endpoint nobody can
//!     resolve is a configuration mistake, and refusing it at `Subscribe` is the
//!     difference between an error a client can read and a topic that silently
//!     drops every publish. Classified `deliberate`.
//!   * **Tags are not attributes.** `CreateTopic` on an existing topic neither
//!     compares nor applies them; `TagResource` is the action that changes them.
//!     Same sentence as the queue side's.
//!   * **`DeleteTopic` deletes its subscriptions FIRST.** A crash between the two
//!     leaves a topic with no subscribers, which a repeat of the same call
//!     repairs; the other order leaves subscription keys no topic owns, which
//!     `ListSubscriptions` would answer for ever.
//!
//! ## What v0 does not do
//!
//! `Publish`/`PublishBatch` are the other half of M4 and live in
//! [`super::publish`]; every action here that changes a subscription clears that
//! path's cache before it answers.
//! `ConfirmSubscription` is answered and cannot succeed — see the function.
//! `AddPermission`/`RemovePermission` are shared with SQS and stay in
//! [`crate::actions`], accepted and never enforced.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::actions::queues::{naming, param_list, param_map, param_text, require_text};
use crate::actions::{queen_error, Ctx};
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::registry::Naming;

use super::registry::{
    SubscriptionRecord, TopicRecord, ATTR_FILTER_POLICY, ATTR_FILTER_POLICY_SCOPE,
    ATTR_RAW_MESSAGE_DELIVERY, FILTER_SCOPE_DEFAULT, PAGE,
};
use super::{
    invalid, is_fifo_topic, not_found, validate_topic_name, ATTR_FIFO_TOPIC, MAX_TAGS, PROTOCOL_SQS,
};

// ------------------------------------------------------- the attribute catalogs

/// Topic attributes a client may set at any time.
///
/// The four documents in it — `Policy`, `DeliveryPolicy`, `ArchivePolicy` and
/// the KMS key — are STORED AND NEVER ENFORCED, PLAN_QUEEN_SQS.md's first
/// non-goal, and they are here for the queue catalog's reason: Terraform and
/// MassTransit set them unconditionally, and an attribute the facade refused to
/// store would fail an apply over a document nothing was going to read anyway.
///
/// The CloudWatch feedback attributes (`SQSSuccessFeedbackRoleArn` and its seven
/// relatives) are deliberately NOT here. They configure delivery logging into a
/// service this facade does not have, and accepting them would be the one
/// outcome the plan forbids: a client told its logging is configured when
/// nothing will ever write a log.
const TOPIC_MUTABLE: &[&str] = &[
    "DisplayName",
    "Policy",
    "DeliveryPolicy",
    "ArchivePolicy",
    "KmsMasterKeyId",
    "SignatureVersion",
    "TracingConfig",
    "ContentBasedDeduplication",
    "FifoThroughputScope",
];

/// Fixed at `CreateTopic`, because it is the name's own declaration.
const TOPIC_CREATE_ONLY: &[&str] = &[ATTR_FIFO_TOPIC];

/// Subscription attributes, at `Subscribe` and at `SetSubscriptionAttributes`
/// alike — SNS has no create-only subscription attribute.
///
/// `RedrivePolicy` and `DeliveryPolicy` are accepted and stored: they describe a
/// delivery ladder this facade does not run (a queue subscription's delivery is
/// one push inside the publish transaction), and refusing them would break the
/// provisioners that set them.
const SUBSCRIPTION_MUTABLE: &[&str] = &[
    ATTR_RAW_MESSAGE_DELIVERY,
    ATTR_FILTER_POLICY,
    ATTR_FILTER_POLICY_SCOPE,
    "DeliveryPolicy",
    "RedrivePolicy",
    "SubscriptionRoleArn",
];

/// The two scopes a filter policy can be matched in.
const FILTER_SCOPES: &[&str] = &[FILTER_SCOPE_DEFAULT, "MessageBody"];

/// Which call is asking. The two differ in exactly one thing: whether the
/// create-only attributes are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum When {
    Create,
    Set,
}

// -------------------------------------------------------------------- topics

/// `CreateTopic`. Answers `{"TopicArn": …}`.
///
/// IDEMPOTENT, and the exception is the interesting half: AWS answers the
/// existing topic's ARN when the requester already owns a topic of that name,
/// and `InvalidParameter` — *"Topic already exists with different attributes"* —
/// when the request contradicts it. Both are encoded here; the second is the one
/// the differential lane has to confirm, because AWS documents the idempotency
/// and not the refusal.
///
/// The comparison is [`first_conflict`]'s: one-directional, against the topic's
/// EFFECTIVE attributes. Tags are neither compared nor applied.
pub async fn create_topic(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let name = require_text(params, "Name")?.to_string();
    let attributes = param_map(params, "Attributes")?;
    let tags = param_map(params, "Tags")?;
    validate_topic_name(&name, &attributes)?;
    validate_topic_attributes(
        &attributes,
        When::Create,
        is_fifo_topic(&name),
        "Attributes",
    )?;
    validate_tags(&tags)?;

    let naming = naming(&ctx.facade.config);
    let record = TopicRecord {
        name: name.clone(),
        attributes,
        tags,
        fifo: is_fifo_topic(&name),
        created_ms: crate::obs::now_epoch_ms(),
        arn: naming.topic_arn(&name),
        version: 0,
    };
    match ctx
        .facade
        .registry
        .create_topic(&record, ctx.token())
        .await?
    {
        Ok(stored) => Ok(json!({ "TopicArn": stored.arn })),
        // A winner with no version is a store that said "the key is taken" and
        // did not answer with what is under it. There is nothing to compare, and
        // answering the ARN would be a promise that the attributes in this
        // request are the ones in force. Same guard as the queue side's.
        Err(winner) if winner.version == 0 => Err(SqsError::with(
            ErrorKind::InvalidParameter,
            format!("Invalid parameter: Attributes Reason: Topic already exists: {name}"),
        )),
        Err(winner) => existing_or_conflict(*winner, &record.attributes, &naming),
    }
}

/// `DeleteTopic`. IDEMPOTENT, which AWS documents in as many words — deleting a
/// topic that is not there is not an error — so this action reads no existence
/// and answers no `NotFound`.
///
/// The subscriptions go FIRST; see the module header on why the order carries
/// the guarantee that a crash cannot leave behind.
pub async fn delete_topic(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let name = topic_name_of(ctx, params, "TopicArn")?;
    let registry = &ctx.facade.registry;
    registry
        .delete_topic_subscriptions(&name, ctx.token())
        .await
        .map_err(store)?;
    registry
        .delete_topic(&name, ctx.token())
        .await
        .map_err(store)?;
    registry.forget_subscriptions(&name);
    Ok(Value::Null)
}

/// `ListTopics`, paginated with the opaque `NextToken` SNS's own paginators
/// follow. There is no `MaxResults` in this action: the page size is AWS's 100,
/// fixed, and a client's paginator is written against it.
pub async fn list_topics(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let page = ctx
        .facade
        .registry
        .list_topics(PAGE, param_text(params, "NextToken"), ctx.token())
        .await?;
    let naming = naming(&ctx.facade.config);
    let topics: Vec<Value> = page
        .topics
        .iter()
        .map(|topic| json!({ "TopicArn": arn_of(topic, &naming) }))
        .collect();
    // The member is written even when it is empty — SNS answers `<Topics/>`
    // rather than omitting it, and its paginator reads the member.
    let mut answer = json!({ "Topics": topics });
    if let Some(next) = page.next_token {
        answer["NextToken"] = Value::String(next);
    }
    Ok(answer)
}

/// `GetTopicAttributes`.
///
/// The three subscription counts cost ONE prefix read of the topic's own key
/// range, and they are computed rather than omitted because AWS answers them on
/// every topic and a client reading an absence would read it as zero.
/// `SubscriptionsPending` is structurally zero here: every subscription this
/// facade can create is same-account SQS, which is confirmed at `Subscribe`.
pub async fn get_topic_attributes(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let record = topic_of(ctx, params, "TopicArn").await?;
    let confirmed = ctx
        .facade
        .registry
        .subscriptions_of(&record.name, ctx.token())
        .await
        .map_err(store)?
        .len();
    let naming = naming(&ctx.facade.config);
    let mut attributes = effective_attributes(&record);
    attributes.insert("TopicArn".to_string(), arn_of(&record, &naming));
    attributes.insert("Owner".to_string(), ctx.facade.config.account.clone());
    attributes.insert("SubscriptionsConfirmed".to_string(), confirmed.to_string());
    attributes.insert("SubscriptionsPending".to_string(), "0".to_string());
    attributes.insert("SubscriptionsDeleted".to_string(), "0".to_string());
    Ok(json!({ "Attributes": attributes }))
}

/// `SetTopicAttributes`. ONE attribute per call, named by two scalar parameters
/// — the shape only SNS uses.
///
/// An omitted `AttributeValue` REMOVES the attribute, which is SNS's only way to
/// unset one. What the topic then reports for it is the default
/// [`effective_attributes`] supplies, exactly as if it had never been set.
pub async fn set_topic_attributes(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let record = topic_of(ctx, params, "TopicArn").await?;
    let (name, value) = attribute_pair(params)?;
    check_topic_attribute_name(&name, When::Set, "AttributeName")?;
    // The VALUE is checked only when there is one: an absent value is the
    // removal above, and running a boolean check over the empty string would
    // refuse the one call that takes `ContentBasedDeduplication` back off.
    if let Some(value) = &value {
        check_topic_attribute_value(&name, value, record.fifo, "AttributeName")?;
    }
    ctx.facade
        .registry
        .set_topic_attributes(
            &record.name,
            &[(name, value)].into_iter().collect(),
            ctx.token(),
        )
        .await?;
    Ok(Value::Null)
}

// ------------------------------------------------------------- subscriptions

/// `Subscribe`. Answers `{"SubscriptionArn": …}`.
///
/// Four refusals, all `InvalidParameter` and each naming its member: a protocol
/// outside v0's scope, an endpoint that is not a queue ARN of this deployment's,
/// an endpoint whose queue does not exist, and a FIFO/standard mismatch between
/// the topic and the queue (AWS's rule, in both directions).
///
/// IDEMPOTENT per (topic, protocol, endpoint): subscribing a queue that is
/// already subscribed answers the existing ARN. It does NOT apply the request's
/// attributes to that existing subscription — AWS's sentence is *"if the
/// requester already owns a subscription with the specified attributes, that
/// subscription's ARN is returned"* and is silent about the differing case, so
/// this facade changes nothing: `SetSubscriptionAttributes` is the action that
/// edits a live subscription, and a `Subscribe` that silently replaced a filter
/// policy would be a change nobody asked for. Flagged for the differential lane.
///
/// `ReturnSubscriptionArn` is accepted and cannot change the answer: it exists
/// so that a client can ask for the real ARN instead of the string `pending
/// confirmation`, and a same-account SQS subscription is confirmed the moment it
/// is created — so the real ARN is the only answer this action ever has, with
/// the flag or without it. That is AWS's own behaviour for a confirmed
/// subscription and not a shortcut.
pub async fn subscribe(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let topic = topic_of(ctx, params, "TopicArn").await?;
    let protocol = require_text(params, "Protocol")?;
    if protocol != PROTOCOL_SQS {
        return Err(invalid(
            "Protocol",
            format!(
                "this endpoint delivers to SQS queues only, so '{PROTOCOL_SQS}' is the one \
                 protocol it subscribes; HTTP/S, email, SMS and Lambda subscriptions are not \
                 implemented (queen-sqs milestone M6 covers HTTP/S)"
            ),
        ));
    }
    let endpoint = require_text(params, "Endpoint")?.to_string();
    let naming = naming(&ctx.facade.config);
    let queue = naming.name_of_arn(&endpoint).ok_or_else(|| {
        invalid(
            "Endpoint",
            "an sqs subscription's endpoint must be the ARN of a queue in this account and region",
        )
    })?;
    let known = ctx
        .facade
        .registry
        .queue(&queue, ctx.token())
        .await
        .map_err(store)?;
    if known.is_none() {
        return Err(invalid(
            "Endpoint",
            format!("the queue named by the endpoint does not exist: {queue}"),
        ));
    }
    // The pairing rule, and the two halves are NOT the same claim.
    //
    // FIFO topic to a standard queue is AWS's own refusal: ordering cannot
    // survive a queue that has no groups.
    //
    // Standard topic to a FIFO queue is THIS FACADE'S limit, and AWS permits it.
    // DIVERGENCE, `deliberate`: a standard topic's fan-out picks the target
    // queue's lane by hashing a fresh key across its width
    // ([`super::publish::push_for`]), and a FIFO queue's lanes are its
    // `MessageGroupId`s — so delivering there needs a decision about which group
    // id every message lands under (AWS's answer is one SNS chooses), and a
    // group id invented per message would put a FIFO consumer's ordering
    // guarantee in this facade's hands without saying so. It is refused at
    // `Subscribe`, where a client can read it, rather than silently at publish.
    // The fix, if the differential lane asks for it, is that decision — not a
    // relaxation here.
    if topic.fifo != crate::registry::is_fifo(&queue) {
        return Err(invalid(
            "Endpoint",
            match topic.fifo {
                true => "a FIFO topic can only be subscribed by a FIFO queue",
                false => {
                    "this endpoint does not deliver a standard topic to a FIFO queue: it has \
                          no MessageGroupId to place the message under"
                }
            },
        ));
    }
    let attributes = param_map(params, "Attributes")?;
    validate_subscription_attributes(&attributes, "Attributes")?;

    // THE IDENTITY IS THE RESOLVED DESTINATION, not the endpoint string. Two
    // ARNs that differ only in the partition segment — `arn:aws:` and
    // `arn:aws-cn:` — name ONE queue, because [`crate::registry::Naming::name_of_arn`]
    // accepts either deliberately, so comparing the strings would let one queue
    // be subscribed to one topic twice. The fan-out keys on the resolved name:
    // on a standard topic that is every message delivered twice, and on a FIFO
    // topic the two pushes share a (queue, partition, transactionId), which the
    // broker refuses as a duplicate — rolling back the WHOLE bundle, which this
    // facade answers as a success ([`super::publish`]). A topic that silently
    // delivers nothing, for ever, from one extra `Subscribe`.
    let existing = ctx
        .facade
        .registry
        .subscriptions_of(&topic.name, ctx.token())
        .await
        .map_err(store)?
        .into_iter()
        .find(|s| s.protocol == protocol && same_destination(&naming, s, &queue, &endpoint));
    if let Some(existing) = existing {
        return Ok(json!({ "SubscriptionArn": subscription_arn_of(&existing, &naming) }));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let record = SubscriptionRecord {
        arn: naming.subscription_arn(&topic.name, &id),
        topic: topic.name.clone(),
        id,
        protocol: protocol.to_string(),
        endpoint,
        owner: ctx.facade.config.account.clone(),
        attributes,
        created_ms: crate::obs::now_epoch_ms(),
        version: 0,
    };
    // The conditional write's ANSWER is read, not discarded: a `putIfAbsent`
    // that did not apply means the key was already there under an id this call
    // minted, so the record in force is somebody else's and the attributes the
    // caller asked for are not the ones a publish will apply. Answering the ARN
    // anyway would name a subscription this call never wrote. It is unreachable
    // from a fresh v4 uuid, which is exactly why the guard is cheap — see
    // [`super::registry::Registry::create_subscription`].
    let claimed = ctx
        .facade
        .registry
        .create_subscription(&record, ctx.token())
        .await?;
    settle_subscription(claimed, &record.arn)?;
    // The publish path reads this topic's subscriptions from a short-lived cache
    // ([`super::registry::Registry::subscriptions_cached`]). Clearing it here is
    // what makes subscribe-then-publish against ONE facade exact, which is the
    // sequence every framework performs at start-up.
    ctx.facade.registry.forget_subscriptions(&topic.name);
    Ok(json!({ "SubscriptionArn": record.arn }))
}

/// `Unsubscribe`.
///
/// `NotFound` for a well-formed `SubscriptionArn` that names nothing, which is
/// what AWS answers and NOT what its `DeleteTopic` answers — the two are
/// different actions and only one of them is documented idempotent. Flagged for
/// the differential lane, because a provisioner that deletes twice notices the
/// difference.
pub async fn unsubscribe(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let (topic, id) = subscription_of(ctx, params, "SubscriptionArn")?;
    let gone = ctx
        .facade
        .registry
        .delete_subscription(&topic, &id, ctx.token())
        .await
        .map_err(store)?;
    ctx.facade.registry.forget_subscriptions(&topic);
    match gone {
        true => Ok(Value::Null),
        false => Err(not_found("Subscription does not exist")),
    }
}

/// `ListSubscriptions`: every subscription in the account, paginated.
pub async fn list_subscriptions(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    listing(ctx, None, param_text(params, "NextToken")).await
}

/// `ListSubscriptionsByTopic`: one topic's, which is a prefix read and needs no
/// index ([`super::registry`]).
pub async fn list_subscriptions_by_topic(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    // The topic must EXIST, and that is not pedantry: an unknown ARN would
    // otherwise answer an empty list, which a client reads as "nothing is
    // subscribed" rather than "you asked about the wrong topic".
    let topic = topic_of(ctx, params, "TopicArn").await?;
    listing(ctx, Some(&topic.name), param_text(params, "NextToken")).await
}

/// `GetSubscriptionAttributes`.
pub async fn get_subscription_attributes(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let record = subscription_record(ctx, params, "SubscriptionArn").await?;
    let naming = naming(&ctx.facade.config);
    Ok(json!({ "Attributes": subscription_attributes(&record, &naming) }))
}

/// `SetSubscriptionAttributes`. One attribute per call, the same two-scalar
/// shape as [`set_topic_attributes`], and an omitted value REMOVES — which is
/// how a `FilterPolicy` is taken off a live subscription.
pub async fn set_subscription_attributes(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let (topic, id) = subscription_of(ctx, params, "SubscriptionArn")?;
    let (name, value) = attribute_pair(params)?;
    check_subscription_attribute_name(&name, "AttributeName")?;
    if let Some(value) = &value {
        check_subscription_attribute_value(&name, value, "AttributeName")?;
    }
    // An empty `FilterPolicy` is a REMOVAL and not an empty document: it is the
    // spelling SNS documents for taking a policy off, and storing `""` would
    // leave a subscription whose policy matches nothing.
    let value = value.filter(|v| !(name == ATTR_FILTER_POLICY && v.trim().is_empty()));
    ctx.facade
        .registry
        .set_subscription_attributes(
            &topic,
            &id,
            &[(name, value)].into_iter().collect(),
            ctx.token(),
        )
        .await?;
    // A `FilterPolicy` or a `RawMessageDelivery` the publish path is holding is
    // exactly what this call changed. See [`subscribe`].
    ctx.facade.registry.forget_subscriptions(&topic);
    Ok(Value::Null)
}

/// `ConfirmSubscription`, which in v0 cannot succeed and says so.
///
/// Every subscription this facade creates is a same-account SQS subscription,
/// and AWS confirms those AT `Subscribe`: `PendingConfirmation` is a state no
/// record here ever occupies, so no confirmation token is ever minted and every
/// token presented is one this endpoint did not issue. `InvalidParameter` is
/// AWS's own answer for a token it does not recognise; the message says why
/// there is nothing to confirm, rather than leaving a client to wonder which of
/// its tokens expired. The HTTP/S subscriptions that DO need the handshake are
/// M6 (PLAN_QUEEN_SQS.md), and this is where it lands.
pub async fn confirm_subscription(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    // The topic first, so that a wrong ARN is answered as a wrong ARN.
    topic_of(ctx, params, "TopicArn").await?;
    require_text(params, "Token")?;
    Err(invalid(
        "Token",
        "this endpoint issues no confirmation tokens: an SQS subscription in this account is \
         confirmed when it is created, so there is never anything to confirm",
    ))
}

// ---------------------------------------------------------------------- tags

/// `TagResource`. Answers an EMPTY RESULT and not a null one: SNS models an
/// (empty) output shape for this action, so the Query rendering writes a
/// `<TagResourceResult>` element — where `Unsubscribe`, `DeleteTopic` and the
/// two setters, which have no output shape at all, write none. `Value::Null`
/// against `json!({})` is how an action says which of the two it is
/// ([`crate::proto::render_ok`]).
pub async fn tag_resource(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let target = tag_target(ctx, params).await?;
    let tags = param_map(params, "Tags")?;
    // The cap is the store's, applied inside its compare-and-set over the
    // RESULTING tag set — see `Registry::tag_topic`. Checking the request here
    // would refuse the wrong calls and let the right ones through.
    ctx.facade
        .registry
        .tag_topic(&target.name, &tags, ctx.token())
        .await?;
    Ok(json!({}))
}

/// `UntagResource`.
pub async fn untag_resource(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let target = tag_target(ctx, params).await?;
    let keys = param_list(params, "TagKeys")?;
    ctx.facade
        .registry
        .untag_topic(&target.name, &keys, ctx.token())
        .await?;
    Ok(json!({}))
}

/// `ListTagsForResource`.
///
/// A LIST of `{Key, Value}` and not a map, which is where SNS and SQS differ:
/// `ListQueueTags` answers a map and this answers `<Tags><member><Key/>…`. The
/// shape is the action's, so it is built here.
pub async fn list_tags_for_resource(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let record = tag_target(ctx, params).await?;
    let tags: Vec<Value> = record
        .tags
        .iter()
        .map(|(key, value)| json!({"Key": key, "Value": value}))
        .collect();
    Ok(json!({ "Tags": tags }))
}

// ------------------------------------------------------------------ the parts

/// One page of subscriptions, in the shape both listings answer.
async fn listing(ctx: &Ctx, topic: Option<&str>, next_token: Option<&str>) -> SqsResult<Value> {
    let page = ctx
        .facade
        .registry
        .list_subscriptions(topic, PAGE, next_token, ctx.token())
        .await?;
    let naming = naming(&ctx.facade.config);
    let subscriptions: Vec<Value> = page
        .subscriptions
        .iter()
        .map(|s| {
            json!({
                "SubscriptionArn": subscription_arn_of(s, &naming),
                "Owner": s.owner,
                "Protocol": s.protocol,
                "Endpoint": s.endpoint,
                "TopicArn": naming.topic_arn(&s.topic),
            })
        })
        .collect();
    let mut answer = json!({ "Subscriptions": subscriptions });
    if let Some(next) = page.next_token {
        answer["NextToken"] = Value::String(next);
    }
    Ok(answer)
}

/// Every attribute a topic reports before the computed ones: what was stored,
/// over the defaults AWS reports for a topic that stored nothing.
///
/// TWO readers, and the second is the reason the defaults are here rather than
/// in the answer: [`first_conflict`] compares a `CreateTopic`'s attributes
/// against THIS map. On AWS every topic has a `DisplayName` (empty, if nobody
/// set one) and every FIFO topic reports `ContentBasedDeduplication`, so a
/// request that supplies AWS's own default against a topic created bare supplies
/// nothing that differs — and a client that reads a topic's attributes and hands
/// them back to `CreateTopic` must never be refused for it (`compat/M0_SMOKE.md`
/// D1, on the queue side, where that was a real defect).
/// TWO attributes AWS reports here and this does not, and the absence is the
/// honest answer rather than an oversight. DIVERGENCE, `deliberate`: `Policy`
/// is the topic's IAM document and `EffectiveDeliveryPolicy` is the HTTP retry
/// ladder, and this deployment evaluates neither — authorization is Queen's over
/// the SigV4 keypair (PLAN_QUEEN_SQS.md's first non-goal) and a queue
/// subscription's delivery is one push inside the publish transaction, with no
/// ladder to describe. Seeding AWS's default documents would answer a client
/// that its policy is in force, which is the one outcome the plan forbids; a
/// `Policy` a client SET is stored and answered, because that is what it asked
/// for. What is lost is a provisioner that reads `Policy` on a topic it never
/// set one on: it sees the key absent instead of AWS's generated default.
fn effective_attributes(record: &TopicRecord) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.insert("DisplayName".to_string(), String::new());
    if record.fifo {
        out.insert(ATTR_FIFO_TOPIC.to_string(), "true".to_string());
        out.insert("ContentBasedDeduplication".to_string(), "false".to_string());
    }
    out.extend(record.attributes.clone());
    out
}

/// Everything `GetSubscriptionAttributes` answers, and what `Subscribe`'s
/// idempotency would compare if AWS documented a comparison.
///
/// `PendingConfirmation` is `false` and `ConfirmationWasAuthenticated` is `true`
/// for every record: a same-account SQS subscription is created confirmed, by
/// the signature that created it. See [`confirm_subscription`].
fn subscription_attributes(
    record: &SubscriptionRecord,
    naming: &Naming,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    out.extend(record.attributes.clone());
    // AFTER the stored map, so the answer is the NORMALIZED boolean and not
    // whatever casing the client stored — `RawMessageDelivery` is read back by
    // provisioners that compare it against `true`.
    out.insert(
        ATTR_RAW_MESSAGE_DELIVERY.to_string(),
        record.raw_message_delivery().to_string(),
    );
    // The SCOPE is reported whenever there is a policy for it to apply to, set
    // or not — AWS's own behaviour, and the same absent-reads-as-unset trap the
    // topic side's D1 comment exists to prevent: a provisioner that sets a
    // filter policy and reads the subscription back to confirm the scope finds
    // no key, and either re-issues `SetSubscriptionAttributes` on every
    // reconcile or reports drift. It is NOT reported for a subscription with no
    // policy, which is also AWS's behaviour: there is no scope to be in.
    if record.has_filter_policy() {
        out.insert(
            ATTR_FILTER_POLICY_SCOPE.to_string(),
            record.filter_scope().to_string(),
        );
    }
    out.insert(
        "SubscriptionArn".to_string(),
        subscription_arn_of(record, naming),
    );
    out.insert("TopicArn".to_string(), naming.topic_arn(&record.topic));
    out.insert("Protocol".to_string(), record.protocol.clone());
    out.insert("Endpoint".to_string(), record.endpoint.clone());
    out.insert("Owner".to_string(), record.owner.clone());
    out.insert("PendingConfirmation".to_string(), "false".to_string());
    out.insert(
        "ConfirmationWasAuthenticated".to_string(),
        "true".to_string(),
    );
    out
}

/// A `CreateTopic` for a name that is taken: the existing topic's ARN, or the
/// refusal.
fn existing_or_conflict(
    existing: TopicRecord,
    supplied: &BTreeMap<String, String>,
    naming: &Naming,
) -> SqsResult<Value> {
    match first_conflict(&existing, supplied) {
        None => Ok(json!({ "TopicArn": arn_of(&existing, naming) })),
        Some(attribute) => Err(SqsError::with(
            ErrorKind::InvalidParameter,
            format!(
                "Invalid parameter: Attributes Reason: Topic already exists with a different \
                 value for attribute {attribute}"
            ),
        )),
    }
}

/// The first SUPPLIED attribute whose value is not the topic's current one, in
/// name order, or `None` when the request describes the topic that is there.
///
/// ONE-DIRECTIONAL: keys of the existing topic that the request does not name
/// are not consulted at all. An attribute the request does not supply cannot
/// differ from anything, which is what makes the create every framework performs
/// at start-up idempotent.
fn first_conflict(existing: &TopicRecord, supplied: &BTreeMap<String, String>) -> Option<String> {
    let current = effective_attributes(existing);
    supplied
        .iter()
        .find(|(name, value)| {
            !current
                .get(*name)
                .is_some_and(|current| same_attribute(name, value, current))
        })
        .map(|(name, _)| name.clone())
}

/// Whether a supplied topic attribute DESCRIBES the current one.
///
/// The two booleans are compared in the casing-insensitive way
/// [`check_topic_attribute_value`] accepts them, because the raw string is what
/// is stored: a topic created with `FifoTopic="TRUE"` must not refuse every
/// later `CreateTopic` that spells it `"true"`. Everything else is exact — the
/// documents this facade stores and does not read have no normal form it could
/// claim to know.
fn same_attribute(name: &str, supplied: &str, current: &str) -> bool {
    supplied == current
        || (matches!(name, ATTR_FIFO_TOPIC | "ContentBasedDeduplication")
            && supplied.eq_ignore_ascii_case(current))
}

/// The topic a request names, read from an ARN parameter. `NotFound` when it is
/// not there — SNS's 404, never SQS's 400.
async fn topic_of(ctx: &Ctx, params: &Value, member: &str) -> SqsResult<TopicRecord> {
    let name = topic_name_of(ctx, params, member)?;
    Ok(ctx
        .facade
        .registry
        .require_topic(&name, ctx.token())
        .await?)
}

/// The topic NAME inside an ARN parameter, without asking the store whether it
/// exists — which is what the idempotent `DeleteTopic` wants.
///
/// An ARN this deployment did not mint is `InvalidParameter` and the client's own
/// string is never echoed back: it is unbounded input on its way into this
/// facade's log. AWS distinguishes a malformed ARN (`InvalidParameter`) from one
/// in another account (`AuthorizationError`); one deployment is one account here,
/// so the distinction has nothing to describe.
fn topic_name_of(ctx: &Ctx, params: &Value, member: &str) -> SqsResult<String> {
    let arn = require_text(params, member)?;
    naming(&ctx.facade.config)
        .topic_of_arn(arn)
        .ok_or_else(|| invalid(member, "not an ARN this endpoint issued"))
}

/// The `(topic, id)` a `SubscriptionArn` parameter names.
fn subscription_of(ctx: &Ctx, params: &Value, member: &str) -> SqsResult<(String, String)> {
    let arn = require_text(params, member)?;
    naming(&ctx.facade.config)
        .subscription_of_arn(arn)
        .ok_or_else(|| invalid(member, "not an ARN this endpoint issued"))
}

/// The subscription record a `SubscriptionArn` parameter names.
async fn subscription_record(
    ctx: &Ctx,
    params: &Value,
    member: &str,
) -> SqsResult<SubscriptionRecord> {
    let (topic, id) = subscription_of(ctx, params, member)?;
    ctx.facade
        .registry
        .subscription(&topic, &id, ctx.token())
        .await
        .map_err(store)?
        .ok_or_else(|| not_found("Subscription does not exist"))
}

/// The topic the three tag actions address, refused with the code THOSE actions
/// answer.
///
/// `ResourceNotFound` and not `NotFound`: the tag trio was modelled in 2020 with
/// the modern shape, and an SDK's exception mapping is generated from it
/// ([`crate::error`]).
async fn tag_target(ctx: &Ctx, params: &Value) -> SqsResult<TopicRecord> {
    let arn = require_text(params, "ResourceArn")?;
    let name = naming(&ctx.facade.config)
        .topic_of_arn(arn)
        .ok_or_else(|| invalid("ResourceArn", "not a topic ARN this endpoint issued"))?;
    ctx.facade
        .registry
        .topic(&name, ctx.token())
        .await
        .map_err(store)?
        .ok_or_else(|| SqsError::new(ErrorKind::ResourceNotFound))
}

/// Whether an existing subscription addresses the same destination as the one
/// being created — [`subscribe`]'s idempotency key.
///
/// For `sqs` the destination is the QUEUE the endpoint resolves to, because that
/// is what the fan-out pushes to and two spellings of one queue's ARN are one
/// destination. For any other protocol (M6's HTTP/S) the endpoint string is the
/// whole of the destination and there is nothing to resolve it through.
fn same_destination(
    naming: &Naming,
    existing: &SubscriptionRecord,
    queue: &str,
    endpoint: &str,
) -> bool {
    match existing.protocol == PROTOCOL_SQS {
        true => naming.name_of_arn(&existing.endpoint).as_deref() == Some(queue),
        false => existing.endpoint == endpoint,
    }
}

/// What a `Subscribe` answers for the conditional write it just made.
///
/// A lost `putIfAbsent` is `ServiceUnavailable` — the one code in the catalog
/// that means "this may work if you send it again" — and never the ARN: the
/// record under that key is not the one this call built.
fn settle_subscription(applied: bool, arn: &str) -> SqsResult<()> {
    match applied {
        true => Ok(()),
        false => Err(SqsError::with(
            ErrorKind::ServiceUnavailable,
            format!("The subscription {arn} was already claimed; please retry"),
        )),
    }
}

/// The ARN a record pinned at create, or — for a record written before this
/// facade stored one — this deployment's own. Never empty: it is the only thing
/// a client can address a topic by.
///
/// `pub(super)` because the publish path writes it into every notification's
/// `TopicArn` ([`super::publish`]): the field every SNS-to-SQS consumer routes
/// on must be the same string every administrative read answers.
pub(super) fn arn_of(record: &TopicRecord, naming: &Naming) -> String {
    match record.arn.is_empty() {
        true => naming.topic_arn(&record.name),
        false => record.arn.clone(),
    }
}

fn subscription_arn_of(record: &SubscriptionRecord, naming: &Naming) -> String {
    match record.arn.is_empty() {
        true => naming.subscription_arn(&record.topic, &record.id),
        false => record.arn.clone(),
    }
}

/// The `AttributeName` / `AttributeValue` pair SNS's two setters take.
///
/// THE UN-LIFT. `AttributeName` is a LIST parameter everywhere else in these two
/// APIs, so the Query codec lifts a lone one into `AttributeNames: ["…"]` when no
/// `AttributeValue` accompanies it (`proto::query::LIFTS`) — and an omitted value
/// is exactly how SNS removes an attribute. Reading the one-element list back
/// here is what keeps that removal working WITHOUT teaching the codec an action
/// name, which is the one thing [`crate::proto`]'s contract forbids it.
fn attribute_pair(params: &Value) -> SqsResult<(String, Option<String>)> {
    let name = match param_text(params, "AttributeName") {
        Some(name) => name.to_string(),
        None => match param_list(params, "AttributeNames")?.as_slice() {
            [only] => only.clone(),
            _ => return Err(super::missing("AttributeName")),
        },
    };
    if name.is_empty() {
        return Err(super::missing("AttributeName"));
    }
    Ok((
        name,
        param_text(params, "AttributeValue").map(str::to_string),
    ))
}

/// The topic attribute catalog and the two values it checks.
///
/// An unknown name and an immutable one are both `InvalidParameter`: SNS has no
/// `InvalidAttributeName`, which is SQS's code, and inventing one would be
/// inventing a client behaviour ([`crate::error`]'s rule).
///
/// Only the BOOLEANS are validated. `Policy`, `DeliveryPolicy` and
/// `ArchivePolicy` are stored verbatim and never enforced, and validating a
/// document this facade does not read would refuse documents AWS accepts.
fn validate_topic_attributes(
    attributes: &BTreeMap<String, String>,
    when: When,
    fifo: bool,
    member: &str,
) -> SqsResult<()> {
    for (name, value) in attributes {
        check_topic_attribute_name(name, when, member)?;
        check_topic_attribute_value(name, value, fifo, member)?;
    }
    Ok(())
}

fn check_topic_attribute_name(name: &str, when: When, member: &str) -> SqsResult<()> {
    let settable = TOPIC_MUTABLE.contains(&name)
        || (when == When::Create && TOPIC_CREATE_ONLY.contains(&name));
    match settable {
        true => Ok(()),
        false => Err(invalid(
            member,
            match TOPIC_CREATE_ONLY.contains(&name) {
                true => format!("{name} is set when the topic is created and cannot be changed"),
                false => format!("unknown attribute {name}"),
            },
        )),
    }
}

fn check_topic_attribute_value(name: &str, value: &str, fifo: bool, member: &str) -> SqsResult<()> {
    if matches!(name, ATTR_FIFO_TOPIC | "ContentBasedDeduplication") && !is_boolean(value) {
        return Err(invalid(member, format!("{name} must be true or false")));
    }
    if name == "ContentBasedDeduplication" && !fifo {
        return Err(invalid(
            member,
            "ContentBasedDeduplication is only supported for FIFO topics",
        ));
    }
    Ok(())
}

/// The subscription attribute catalog, its two enumerations and the one document
/// this facade DOES read.
///
/// `FilterPolicy` is validated because the publish path applies it — the same
/// rule the queue side follows for `RedrivePolicy`: validate what you act on,
/// store what you do not.
fn validate_subscription_attributes(
    attributes: &BTreeMap<String, String>,
    member: &str,
) -> SqsResult<()> {
    for (name, value) in attributes {
        check_subscription_attribute_name(name, member)?;
        check_subscription_attribute_value(name, value, member)?;
    }
    Ok(())
}

fn check_subscription_attribute_name(name: &str, member: &str) -> SqsResult<()> {
    match SUBSCRIPTION_MUTABLE.contains(&name) {
        true => Ok(()),
        false => Err(invalid(member, format!("unknown attribute {name}"))),
    }
}

fn check_subscription_attribute_value(name: &str, value: &str, member: &str) -> SqsResult<()> {
    if name == ATTR_RAW_MESSAGE_DELIVERY && !is_boolean(value) {
        return Err(invalid(member, "RawMessageDelivery must be true or false"));
    }
    if name == ATTR_FILTER_POLICY_SCOPE && !FILTER_SCOPES.contains(&value) {
        return Err(invalid(
            member,
            "FilterPolicyScope must be MessageAttributes or MessageBody",
        ));
    }
    // An EMPTY policy is the removal SNS documents, and it is legal here for
    // that reason; anything else must be a GRAMMAR THIS FACADE EVALUATES. The
    // check is the publish path's own ([`super::filter::validate`]) and not a
    // shape test, because the alternative is the one failure a client cannot see
    // from the outside: a policy stored, never understood, and a subscription
    // that quietly stops receiving.
    if name == ATTR_FILTER_POLICY && !value.trim().is_empty() {
        let policy = serde_json::from_str::<Value>(value)
            .map_err(|_| invalid(member, "FilterPolicy must be a JSON object"))?;
        super::filter::validate(&policy)?;
    }
    Ok(())
}

fn is_boolean(value: &str) -> bool {
    value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false")
}

/// The cap on the tags a `CreateTopic` carries. `TagResource`'s own check is the
/// store's, because there it applies to a set this request only adds to.
fn validate_tags(tags: &BTreeMap<String, String>) -> SqsResult<()> {
    match tags.len() > MAX_TAGS {
        true => Err(SqsError::new(ErrorKind::TagLimitExceeded)),
        false => Ok(()),
    }
}

/// A broker failure, through the one mapping
/// ([`crate::error::SqsError::from_queen`]).
fn store(e: crate::queen::Error) -> SqsError {
    queen_error(&e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::testing::Rig;

    const TOPIC: &str = "arn:aws:sns:queen-1:000000000000:events";
    const FIFO_TOPIC: &str = "arn:aws:sns:queen-1:000000000000:events.fifo";
    const ORDERS: &str = "arn:aws:sqs:queen-1:000000000000:orders";
    const ORDERS_FIFO: &str = "arn:aws:sqs:queen-1:000000000000:orders.fifo";

    /// A rig with the two queues every subscription case needs.
    async fn rig() -> Rig {
        Rig::new(&[("orders", &[]), ("orders.fifo", &[])]).await
    }

    async fn create(rig: &Rig, name: &str, attributes: Value) -> SqsResult<Value> {
        let mut params = json!({ "Name": name });
        if !attributes.is_null() {
            params["Attributes"] = attributes;
        }
        create_topic(&rig.ctx, &params).await
    }

    async fn subscribe_queue(rig: &Rig, topic: &str, endpoint: &str) -> SqsResult<Value> {
        subscribe(
            &rig.ctx,
            &json!({"TopicArn": topic, "Protocol": "sqs", "Endpoint": endpoint}),
        )
        .await
    }

    fn arn(answer: &Value, member: &str) -> String {
        answer[member]
            .as_str()
            .unwrap_or_else(|| panic!("no {member} in {answer}"))
            .to_string()
    }

    fn attribute(answer: &Value, name: &str) -> Option<String> {
        answer["Attributes"].get(name)?.as_str().map(str::to_string)
    }

    // ------------------------------------------------------------- create

    /// The ARN is minted from the name, and a second create of the same name is
    /// the SAME topic — which is what every framework does at start-up.
    #[tokio::test]
    async fn creating_a_topic_answers_its_arn_and_is_idempotent() {
        let rig = rig().await;
        let first = create(&rig, "events", Value::Null).await.expect("created");
        assert_eq!(arn(&first, "TopicArn"), TOPIC);
        let again = create(&rig, "events", Value::Null).await.expect("created");
        assert_eq!(arn(&again, "TopicArn"), TOPIC);
        // ...and the topic is one topic, not two.
        let listed = list_topics(&rig.ctx, &json!({})).await.expect("listed");
        assert_eq!(listed["Topics"].as_array().map(Vec::len), Some(1));
    }

    /// The comparison behind the idempotency, in all four of its cases.
    #[tokio::test]
    async fn a_re_create_wins_the_topic_unless_it_contradicts_it() {
        let rig = rig().await;
        create(&rig, "events", json!({"DisplayName": "Events"}))
            .await
            .expect("created");

        // Supplying the same value is the same topic...
        create(&rig, "events", json!({"DisplayName": "Events"}))
            .await
            .expect("the request describes the topic that is there");
        // ...and so is supplying NOTHING, which is the create every framework
        // performs at start-up: an attribute the request does not name cannot
        // differ from anything.
        create(&rig, "events", Value::Null)
            .await
            .expect("an unattributed re-create is idempotent");
        // A value the topic does not have at all IS a conflict — the direction
        // is request-to-topic, not the reverse.
        assert_eq!(
            create(&rig, "events", json!({"Policy": "{}"}))
                .await
                .expect_err("refused")
                .kind,
            ErrorKind::InvalidParameter
        );
        // ...and a DIFFERENT value is the refusal, which names the attribute.
        let e = create(&rig, "events", json!({"DisplayName": "Other"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert_eq!(e.kind.http_status(), 400);
        assert!(e.message.contains("DisplayName"), "{}", e.message);
    }

    /// The D1 lesson, on the SNS side: what `GetTopicAttributes` answers for a
    /// settable attribute is exactly what `CreateTopic` accepts back. A topic
    /// created bare reports `DisplayName=""`, and handing that back must not be a
    /// conflict.
    #[tokio::test]
    async fn what_get_topic_attributes_answers_is_accepted_back_by_create() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        let attributes = read["Attributes"].as_object().expect("a map").clone();
        for (name, value) in &attributes {
            // Only the settable half round-trips; the computed ones are not
            // parameters of any create.
            if !TOPIC_MUTABLE.contains(&name.as_str()) {
                continue;
            }
            let mut one = serde_json::Map::new();
            one.insert(name.clone(), value.clone());
            create(&rig, "events", Value::Object(one))
                .await
                .unwrap_or_else(|e| panic!("{name} was answered and refused back: {e}"));
        }
        assert_eq!(attribute(&read, "DisplayName").as_deref(), Some(""));
    }

    /// Tags are not attributes: a re-create neither compares them nor applies
    /// them, and `TagResource` is the action that changes them.
    #[tokio::test]
    async fn a_re_create_does_not_touch_the_tags() {
        let rig = rig().await;
        create_topic(
            &rig.ctx,
            &json!({"Name": "events", "Tags": {"team": "billing"}}),
        )
        .await
        .expect("created");
        create_topic(
            &rig.ctx,
            &json!({"Name": "events", "Tags": {"team": "someone-else"}}),
        )
        .await
        .expect("a differing tag is not a conflict");
        let tags = list_tags_for_resource(&rig.ctx, &json!({"ResourceArn": TOPIC}))
            .await
            .expect("listed");
        assert_eq!(tags["Tags"], json!([{"Key": "team", "Value": "billing"}]));
    }

    /// Every refusal a `CreateTopic` can carry, each of them `InvalidParameter`
    /// — SNS has no `InvalidAttributeName`, which is SQS's code.
    #[tokio::test]
    async fn a_create_that_breaks_a_rule_is_refused_before_anything_is_stored() {
        let rig = rig().await;
        for (name, attributes) in [
            // The name charset...
            ("with space", Value::Null),
            (&"e".repeat(257), Value::Null),
            // ...the `.fifo` agreement, in both directions...
            ("events", json!({"FifoTopic": "true"})),
            ("events.fifo", Value::Null),
            // ...an attribute nobody may set...
            ("events", json!({"Owner": "someone"})),
            (
                "events",
                json!({"SQSSuccessFeedbackRoleArn": "arn:aws:iam::0:role/r"}),
            ),
            // ...a FIFO-only attribute on a standard topic...
            ("events", json!({"ContentBasedDeduplication": "true"})),
            // ...and a boolean that is not one.
            ("events.fifo", json!({"FifoTopic": "yes"})),
        ] {
            let e = create(&rig, name, attributes.clone())
                .await
                .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{name} {attributes}");
        }
        // Nothing was stored by any of them.
        assert_eq!(
            list_topics(&rig.ctx, &json!({})).await.expect("listed"),
            json!({"Topics": []})
        );
    }

    // ----------------------------------------------------------- attributes

    #[tokio::test]
    async fn a_topic_reports_its_attributes_and_its_subscription_count() {
        let rig = rig().await;
        create(&rig, "events", json!({"DisplayName": "Events"}))
            .await
            .expect("created");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "TopicArn").as_deref(), Some(TOPIC));
        assert_eq!(
            attribute(&read, "Owner").as_deref(),
            Some(crate::config::DEFAULT_ACCOUNT)
        );
        assert_eq!(attribute(&read, "DisplayName").as_deref(), Some("Events"));
        assert_eq!(
            attribute(&read, "SubscriptionsConfirmed").as_deref(),
            Some("0")
        );
        assert_eq!(
            attribute(&read, "SubscriptionsPending").as_deref(),
            Some("0")
        );
        assert_eq!(
            attribute(&read, "SubscriptionsDeleted").as_deref(),
            Some("0")
        );
        // A standard topic reports no FIFO attributes at all.
        assert_eq!(attribute(&read, "FifoTopic"), None);

        subscribe_queue(&rig, TOPIC, ORDERS)
            .await
            .expect("subscribed");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(
            attribute(&read, "SubscriptionsConfirmed").as_deref(),
            Some("1")
        );
    }

    #[tokio::test]
    async fn a_fifo_topic_reports_the_two_attributes_that_declare_it() {
        let rig = rig().await;
        create(&rig, "events.fifo", json!({"FifoTopic": "true"}))
            .await
            .expect("created");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": FIFO_TOPIC}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "FifoTopic").as_deref(), Some("true"));
        assert_eq!(
            attribute(&read, "ContentBasedDeduplication").as_deref(),
            Some("false"),
            "AWS reports it on every FIFO topic, set or not"
        );
    }

    /// The two attributes AWS answers on every topic and this deployment does
    /// not: `Policy` and `EffectiveDeliveryPolicy`. DIVERGENCE, `deliberate` —
    /// neither is evaluated here, and answering a default document would tell a
    /// client its policy is in force. A policy a client SET is stored and
    /// answered, because that is what it asked for.
    #[tokio::test]
    async fn a_topic_reports_no_policy_it_does_not_enforce() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "Policy"), None);
        assert_eq!(attribute(&read, "EffectiveDeliveryPolicy"), None);

        // What a client sets IS answered back, verbatim.
        set_topic_attributes(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "AttributeName": "Policy",
                    "AttributeValue": r#"{"Version":"2012-10-17"}"#}),
        )
        .await
        .expect("set");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(
            attribute(&read, "Policy").as_deref(),
            Some(r#"{"Version":"2012-10-17"}"#)
        );
    }

    /// The setter's two shapes: a value that lands, and an OMITTED value that
    /// removes — which is SNS's only way to unset an attribute.
    #[tokio::test]
    async fn setting_an_attribute_and_removing_it_are_the_same_action() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        set_topic_attributes(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "AttributeName": "DisplayName", "AttributeValue": "Events"}),
        )
        .await
        .expect("set");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "DisplayName").as_deref(), Some("Events"));

        // No AttributeValue at all, which the Query codec delivers as a
        // one-element `AttributeNames` list.
        set_topic_attributes(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "AttributeNames": ["DisplayName"]}),
        )
        .await
        .expect("removed");
        let read = get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("read");
        assert_eq!(
            attribute(&read, "DisplayName").as_deref(),
            Some(""),
            "the default is what a removed attribute reports"
        );
    }

    #[tokio::test]
    async fn an_unsettable_topic_attribute_is_refused() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        for (name, value) in [("FifoTopic", "true"), ("Nonsense", "1")] {
            let e = set_topic_attributes(
                &rig.ctx,
                &json!({"TopicArn": TOPIC, "AttributeName": name, "AttributeValue": value}),
            )
            .await
            .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{name}");
        }
    }

    // ---------------------------------------------------------------- delete

    /// `DeleteTopic` is idempotent AND it takes the subscriptions with it.
    #[tokio::test]
    async fn deleting_a_topic_takes_its_subscriptions_and_repeats_cleanly() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        subscribe_queue(&rig, TOPIC, ORDERS)
            .await
            .expect("subscribed");
        assert_eq!(
            list_subscriptions(&rig.ctx, &json!({}))
                .await
                .expect("listed")["Subscriptions"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );

        delete_topic(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("deleted");
        assert_eq!(
            list_subscriptions(&rig.ctx, &json!({}))
                .await
                .expect("listed")["Subscriptions"],
            json!([]),
            "a deleted topic leaves no subscription behind"
        );
        // Idempotent, which AWS documents in as many words.
        delete_topic(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("deleting a topic that is gone is a success");
        // ...and the topic really is gone.
        assert_eq!(
            get_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC}))
                .await
                .expect_err("gone")
                .kind,
            ErrorKind::NotFound
        );
    }

    // ------------------------------------------------------------- subscribe

    #[tokio::test]
    async fn subscribing_a_queue_answers_an_arn_under_the_topics_own() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let answer = subscribe_queue(&rig, TOPIC, ORDERS)
            .await
            .expect("subscribed");
        let subscription = arn(&answer, "SubscriptionArn");
        assert!(
            subscription.starts_with(&format!("{TOPIC}:")),
            "{subscription}"
        );

        let read = get_subscription_attributes(
            &rig.ctx,
            &json!({"SubscriptionArn": subscription.clone()}),
        )
        .await
        .expect("read");
        assert_eq!(attribute(&read, "TopicArn").as_deref(), Some(TOPIC));
        assert_eq!(attribute(&read, "Endpoint").as_deref(), Some(ORDERS));
        assert_eq!(attribute(&read, "Protocol").as_deref(), Some("sqs"));
        assert_eq!(
            attribute(&read, "RawMessageDelivery").as_deref(),
            Some("false")
        );
        assert_eq!(
            attribute(&read, "PendingConfirmation").as_deref(),
            Some("false")
        );
        assert_eq!(
            attribute(&read, "ConfirmationWasAuthenticated").as_deref(),
            Some("true")
        );
        assert_eq!(
            attribute(&read, "SubscriptionArn").as_deref(),
            Some(subscription.as_str())
        );
    }

    /// Subscribing twice is ONE subscription, and the second call does not
    /// rewrite the first's attributes.
    #[tokio::test]
    async fn subscribing_the_same_queue_twice_answers_the_same_arn() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let first = arn(
            &subscribe(
                &rig.ctx,
                &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                        "Attributes": {"RawMessageDelivery": "true"}}),
            )
            .await
            .expect("subscribed"),
            "SubscriptionArn",
        );
        let again = arn(
            &subscribe(
                &rig.ctx,
                &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                        "Attributes": {"RawMessageDelivery": "false"}}),
            )
            .await
            .expect("subscribed"),
            "SubscriptionArn",
        );
        assert_eq!(first, again);
        let read = get_subscription_attributes(&rig.ctx, &json!({"SubscriptionArn": first}))
            .await
            .expect("read");
        assert_eq!(
            attribute(&read, "RawMessageDelivery").as_deref(),
            Some("true"),
            "a re-subscribe does not rewrite a live subscription"
        );
        assert_eq!(
            list_subscriptions_by_topic(&rig.ctx, &json!({"TopicArn": TOPIC}))
                .await
                .expect("listed")["Subscriptions"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    /// The idempotency key is the QUEUE, not the endpoint string: two ARNs of
    /// one queue that differ only in their partition segment are one
    /// subscription.
    ///
    /// A second record for one queue is not a cosmetic duplicate. Every publish
    /// would push to that queue twice inside one transaction, and on a FIFO
    /// topic the two pushes share a (queue, partition, transactionId), which the
    /// broker refuses as a duplicate and rolls the whole fan-out back — answered
    /// to the publisher as a success. See [`subscribe`].
    #[tokio::test]
    async fn one_queue_is_one_subscription_whatever_the_arns_partition_says() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let first = arn(
            &subscribe_queue(&rig, TOPIC, ORDERS)
                .await
                .expect("subscribed"),
            "SubscriptionArn",
        );
        // The same queue, spelled with another AWS partition — which
        // `name_of_arn` resolves deliberately.
        let again = arn(
            &subscribe_queue(&rig, TOPIC, "arn:aws-cn:sqs:queen-1:000000000000:orders")
                .await
                .expect("subscribed"),
            "SubscriptionArn",
        );
        assert_eq!(first, again);
        assert_eq!(
            list_subscriptions_by_topic(&rig.ctx, &json!({"TopicArn": TOPIC}))
                .await
                .expect("listed")["Subscriptions"]
                .as_array()
                .map(Vec::len),
            Some(1),
            "one queue, one subscription"
        );
    }

    /// A lost `putIfAbsent` never answers an ARN: the record under that key is
    /// not the one this call built, so its attributes are not the ones a publish
    /// would apply.
    #[test]
    fn a_subscription_this_call_did_not_write_is_not_answered_as_written() {
        assert!(settle_subscription(true, "arn:aws:sns:queen-1:0:events:id").is_ok());
        let e = settle_subscription(false, "arn:aws:sns:queen-1:0:events:id").expect_err("refused");
        assert_eq!(e.kind, ErrorKind::ServiceUnavailable);
        // The one code in the catalog that means "this may work if you send it
        // again", which is exactly what a colliding id is.
        assert_eq!(e.kind.http_status(), 503);
    }

    /// Every way a `Subscribe` is refused, each naming its own member.
    #[tokio::test]
    async fn the_subscribe_refusals_name_what_is_wrong() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        create(&rig, "events.fifo", json!({"FifoTopic": "true"}))
            .await
            .expect("created");

        // A protocol outside v0's scope, which says so by name.
        let e = subscribe(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "Protocol": "https", "Endpoint": "https://example.test/hook"}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("sqs"), "{}", e.message);
        assert!(e.message.contains("M6"), "{}", e.message);

        // An endpoint that is not a queue ARN of ours, and one whose queue does
        // not exist.
        for endpoint in [
            "https://example.test/hook",
            "arn:aws:sqs:queen-1:999999999999:orders",
            "arn:aws:sqs:queen-1:000000000000:nope",
        ] {
            let e = subscribe_queue(&rig, TOPIC, endpoint)
                .await
                .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{endpoint}");
        }

        // The pairing rule, in both directions — and the two refusals do not
        // claim the same thing. A FIFO topic to a standard queue is AWS's own
        // rule; a standard topic to a FIFO queue is this facade's limit, and its
        // sentence names what is missing rather than attributing the refusal to
        // AWS.
        assert_eq!(
            subscribe_queue(&rig, FIFO_TOPIC, ORDERS)
                .await
                .expect_err("refused")
                .kind,
            ErrorKind::InvalidParameter
        );
        let e = subscribe_queue(&rig, TOPIC, ORDERS_FIFO)
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("MessageGroupId"), "{}", e.message);
        // ...and the pairing that IS allowed.
        subscribe_queue(&rig, FIFO_TOPIC, ORDERS_FIFO)
            .await
            .expect("a FIFO topic fans out to a FIFO queue");

        // A topic that is not there is SNS's 404, before anything else is read.
        let e = subscribe_queue(&rig, "arn:aws:sns:queen-1:000000000000:absent", ORDERS)
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::NotFound);
        assert_eq!(e.kind.http_status(), 404);
    }

    /// `ReturnSubscriptionArn` is honoured in the only way an auto-confirmed
    /// subscription can honour it: the real ARN, whichever value it carries.
    #[tokio::test]
    async fn return_subscription_arn_never_changes_the_answer() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let answer = subscribe(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                    "ReturnSubscriptionArn": "false"}),
        )
        .await
        .expect("subscribed");
        let subscription = arn(&answer, "SubscriptionArn");
        assert!(subscription.starts_with(TOPIC), "{subscription}");
        assert_ne!(subscription, "pending confirmation");
    }

    #[tokio::test]
    async fn a_subscription_attribute_is_validated_before_it_is_stored() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        for attributes in [
            json!({"RawMessageDelivery": "yes"}),
            json!({"FilterPolicyScope": "MessageHeaders"}),
            json!({"FilterPolicy": "not json"}),
            json!({"FilterPolicy": "[1,2]"}),
            json!({"Nonsense": "1"}),
        ] {
            let e = subscribe(
                &rig.ctx,
                &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                        "Attributes": attributes.clone()}),
            )
            .await
            .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{attributes}");
        }
    }

    /// The two mutable attributes the plan names, set and cleared through the
    /// action a client actually calls.
    #[tokio::test]
    async fn raw_delivery_and_the_filter_policy_are_mutable() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let subscription = arn(
            &subscribe_queue(&rig, TOPIC, ORDERS)
                .await
                .expect("subscribed"),
            "SubscriptionArn",
        );
        for (name, value) in [
            ("RawMessageDelivery", "true"),
            ("FilterPolicy", r#"{"kind":["order"]}"#),
            ("FilterPolicyScope", "MessageBody"),
        ] {
            set_subscription_attributes(
                &rig.ctx,
                &json!({"SubscriptionArn": subscription.clone(),
                        "AttributeName": name, "AttributeValue": value}),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
        let read = get_subscription_attributes(
            &rig.ctx,
            &json!({"SubscriptionArn": subscription.clone()}),
        )
        .await
        .expect("read");
        assert_eq!(
            attribute(&read, "RawMessageDelivery").as_deref(),
            Some("true")
        );
        assert_eq!(
            attribute(&read, "FilterPolicy").as_deref(),
            Some(r#"{"kind":["order"]}"#)
        );
        assert_eq!(
            attribute(&read, "FilterPolicyScope").as_deref(),
            Some("MessageBody")
        );

        // The documented removal: an EMPTY value takes the policy off.
        set_subscription_attributes(
            &rig.ctx,
            &json!({"SubscriptionArn": subscription.clone(),
                    "AttributeName": "FilterPolicy", "AttributeValue": ""}),
        )
        .await
        .expect("cleared");
        let read = get_subscription_attributes(&rig.ctx, &json!({"SubscriptionArn": subscription}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "FilterPolicy"), None);
        // The scope SET explicitly stays set, because removing it was not what
        // the request asked for — `SetSubscriptionAttributes` names one
        // attribute and touches no other.
        assert_eq!(
            attribute(&read, "FilterPolicyScope").as_deref(),
            Some("MessageBody")
        );
    }

    /// A subscription that carries a policy reports the SCOPE it is matched
    /// under whether or not anybody set one — the same absent-reads-as-unset
    /// trap the topic side's D1 comment exists to prevent. A provisioner that
    /// sets a policy and reads the subscription back to confirm the scope must
    /// find the default there, not a hole.
    #[tokio::test]
    async fn a_filter_policy_reports_the_scope_it_defaults_to() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let subscription = arn(
            &subscribe(
                &rig.ctx,
                &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                        "Attributes": {"FilterPolicy": r#"{"kind":["order"]}"#}}),
            )
            .await
            .expect("subscribed"),
            "SubscriptionArn",
        );
        let read = get_subscription_attributes(
            &rig.ctx,
            &json!({"SubscriptionArn": subscription.clone()}),
        )
        .await
        .expect("read");
        assert_eq!(
            attribute(&read, "FilterPolicyScope").as_deref(),
            Some(FILTER_SCOPE_DEFAULT)
        );
        // A subscription with NO policy reports no scope: there is nothing for
        // one to be the scope of.
        create(&rig, "other", Value::Null).await.expect("created");
        let bare = arn(
            &subscribe_queue(&rig, "arn:aws:sns:queen-1:000000000000:other", ORDERS)
                .await
                .unwrap_or_else(|e| panic!("{e}")),
            "SubscriptionArn",
        );
        let read = get_subscription_attributes(&rig.ctx, &json!({"SubscriptionArn": bare}))
            .await
            .expect("read");
        assert_eq!(attribute(&read, "FilterPolicyScope"), None);
    }

    /// A document the store cannot hold is the CLIENT's error, refused before
    /// anything is written — on the first write as well as on a later one. A
    /// `FilterPolicy` is unbounded client input and SNS's own ceiling for one is
    /// above this store's, so the refusal has to be here rather than a 400 from
    /// the broker rendered as a server fault.
    #[tokio::test]
    async fn an_oversized_document_is_refused_by_the_action_that_carries_it() {
        let rig = rig().await;
        let huge = format!(r#"{{"kind":["{}"]}}"#, "x".repeat(70_000));

        // At `Subscribe`, which writes the record for the first time.
        create(&rig, "events", Value::Null).await.expect("created");
        let e = subscribe(
            &rig.ctx,
            &json!({"TopicArn": TOPIC, "Protocol": "sqs", "Endpoint": ORDERS,
                    "Attributes": {"FilterPolicy": huge.clone()}}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert_eq!(e.kind.http_status(), 400);

        // At `SetSubscriptionAttributes`, on a live one.
        let subscription = arn(
            &subscribe_queue(&rig, TOPIC, ORDERS)
                .await
                .expect("subscribed"),
            "SubscriptionArn",
        );
        assert_eq!(
            set_subscription_attributes(
                &rig.ctx,
                &json!({"SubscriptionArn": subscription, "AttributeName": "FilterPolicy",
                        "AttributeValue": huge.clone()}),
            )
            .await
            .expect_err("refused")
            .kind,
            ErrorKind::InvalidParameter
        );

        // And at `CreateTopic`, whose `Policy` is the same unbounded input.
        assert_eq!(
            create(&rig, "wide", json!({"Policy": "x".repeat(70_000)}))
                .await
                .expect_err("refused")
                .kind,
            ErrorKind::InvalidParameter
        );
    }

    #[tokio::test]
    async fn unsubscribing_removes_exactly_one_and_then_says_so() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let subscription = arn(
            &subscribe_queue(&rig, TOPIC, ORDERS)
                .await
                .expect("subscribed"),
            "SubscriptionArn",
        );
        unsubscribe(&rig.ctx, &json!({"SubscriptionArn": subscription.clone()}))
            .await
            .expect("unsubscribed");
        assert_eq!(
            list_subscriptions(&rig.ctx, &json!({}))
                .await
                .expect("listed")["Subscriptions"],
            json!([])
        );
        // A second unsubscribe is `NotFound` — Unsubscribe is not DeleteTopic.
        let e = unsubscribe(&rig.ctx, &json!({"SubscriptionArn": subscription}))
            .await
            .expect_err("gone");
        assert_eq!(e.kind, ErrorKind::NotFound);
        // ...and an ARN this endpoint never issued is a parameter refusal.
        let e = unsubscribe(
            &rig.ctx,
            &json!({"SubscriptionArn": "arn:aws:sns:other:0:events:x"}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
    }

    // ------------------------------------------------------------- listings

    #[tokio::test]
    async fn the_two_subscription_listings_differ_by_their_scope() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        create(&rig, "audit", Value::Null).await.expect("created");
        subscribe_queue(&rig, TOPIC, ORDERS)
            .await
            .expect("subscribed");
        subscribe_queue(
            &rig,
            "arn:aws:sns:queen-1:000000000000:audit",
            "arn:aws:sqs:queen-1:000000000000:orders",
        )
        .await
        .expect("subscribed");

        let all = list_subscriptions(&rig.ctx, &json!({}))
            .await
            .expect("listed");
        assert_eq!(all["Subscriptions"].as_array().map(Vec::len), Some(2));
        let one = list_subscriptions_by_topic(&rig.ctx, &json!({"TopicArn": TOPIC}))
            .await
            .expect("listed");
        let subscriptions = one["Subscriptions"].as_array().expect("a list");
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0]["TopicArn"], TOPIC);
        assert_eq!(subscriptions[0]["Endpoint"], ORDERS);
        assert_eq!(subscriptions[0]["Protocol"], "sqs");
        assert_eq!(subscriptions[0]["Owner"], crate::config::DEFAULT_ACCOUNT);

        // A listing of a topic that is not there is a 404 and never an empty
        // list, which a client would read as "nothing is subscribed".
        assert_eq!(
            list_subscriptions_by_topic(
                &rig.ctx,
                &json!({"TopicArn": "arn:aws:sns:queen-1:000000000000:absent"})
            )
            .await
            .expect_err("no such topic")
            .kind,
            ErrorKind::NotFound
        );
    }

    /// The paging contract over the action: a page of a hundred and one carries
    /// a cursor, the next call continues from it, and no topic is answered
    /// twice.
    #[tokio::test]
    async fn a_topic_listing_pages_at_a_hundred() {
        let rig = rig().await;
        for i in 0..PAGE + 1 {
            create(&rig, &format!("topic-{i:03}"), Value::Null)
                .await
                .expect("created");
        }
        let first = list_topics(&rig.ctx, &json!({})).await.expect("listed");
        assert_eq!(first["Topics"].as_array().map(Vec::len), Some(PAGE));
        let cursor = first["NextToken"].as_str().expect("a cursor").to_string();
        let second = list_topics(&rig.ctx, &json!({ "NextToken": cursor }))
            .await
            .expect("listed");
        assert_eq!(second["Topics"].as_array().map(Vec::len), Some(1));
        assert_eq!(second.get("NextToken"), None, "the last page ends it");
        // No topic on both pages.
        let mut arns: Vec<&Value> = first["Topics"].as_array().unwrap().iter().collect();
        arns.extend(second["Topics"].as_array().unwrap());
        let unique: std::collections::BTreeSet<String> =
            arns.iter().map(|t| t["TopicArn"].to_string()).collect();
        assert_eq!(unique.len(), PAGE + 1);

        // An empty account still answers the member, which SNS writes even when
        // there is nothing in it.
        let empty = Rig::new(&[]).await;
        assert_eq!(
            list_topics(&empty.ctx, &json!({})).await.expect("listed"),
            json!({"Topics": []})
        );
    }

    // ------------------------------------------------------------------ tags

    #[tokio::test]
    async fn the_tag_trio_reads_and_writes_a_topics_tags() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        // An empty answer shape, not a null one: SNS models an output for these.
        assert_eq!(
            tag_resource(
                &rig.ctx,
                &json!({"ResourceArn": TOPIC, "Tags": {"team": "billing", "env": "prod"}})
            )
            .await
            .expect("tagged"),
            json!({})
        );
        let listed = list_tags_for_resource(&rig.ctx, &json!({"ResourceArn": TOPIC}))
            .await
            .expect("listed");
        assert_eq!(
            listed["Tags"],
            json!([{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "billing"}]),
            "a LIST of pairs, which is SNS's shape and not the queue map"
        );

        untag_resource(
            &rig.ctx,
            &json!({"ResourceArn": TOPIC, "TagKeys": ["env", "absent"]}),
        )
        .await
        .expect("untagged");
        let listed = list_tags_for_resource(&rig.ctx, &json!({"ResourceArn": TOPIC}))
            .await
            .expect("listed");
        assert_eq!(listed["Tags"], json!([{"Key": "team", "Value": "billing"}]));
    }

    /// The tag actions answer their OWN missing-resource code, which is not the
    /// one every other SNS action answers.
    #[tokio::test]
    async fn the_tag_trio_answers_resource_not_found() {
        let rig = rig().await;
        for params in [
            json!({"ResourceArn": TOPIC, "Tags": {"a": "b"}}),
            json!({"ResourceArn": TOPIC, "TagKeys": ["a"]}),
            json!({"ResourceArn": TOPIC}),
        ] {
            let answers = [
                tag_resource(&rig.ctx, &params).await,
                untag_resource(&rig.ctx, &params).await,
                list_tags_for_resource(&rig.ctx, &params).await,
            ];
            for answer in answers {
                let e = answer.expect_err("no such topic");
                assert_eq!(e.kind, ErrorKind::ResourceNotFound);
                assert_eq!(e.kind.http_status(), 404);
            }
        }
        // A resource ARN that is not a topic's at all is a parameter refusal.
        assert_eq!(
            list_tags_for_resource(&rig.ctx, &json!({"ResourceArn": ORDERS}))
                .await
                .expect_err("refused")
                .kind,
            ErrorKind::InvalidParameter
        );
    }

    /// The cap, and the case that makes it worth having: it is the RESULTING tag
    /// set that is capped, so a topic already at fifty refuses the fifty-first
    /// even when the request carries one tag — and the refusal happens before
    /// anything is stored.
    #[tokio::test]
    async fn more_than_fifty_tags_is_snss_own_limit_error() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let mut tags = serde_json::Map::new();
        for i in 0..=MAX_TAGS {
            tags.insert(format!("k{i}"), Value::String(i.to_string()));
        }
        let e = tag_resource(
            &rig.ctx,
            &json!({"ResourceArn": TOPIC, "Tags": Value::Object(tags.clone())}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::TagLimitExceeded);
        assert_eq!(e.kind.http_status(), 400);

        // Fifty land, and the one after them does not — added one call at a
        // time, which is the shape a request-only check would have missed.
        tags.remove(&format!("k{MAX_TAGS}"));
        tag_resource(
            &rig.ctx,
            &json!({"ResourceArn": TOPIC, "Tags": Value::Object(tags)}),
        )
        .await
        .expect("fifty is the cap, not one under it");
        let e = tag_resource(
            &rig.ctx,
            &json!({"ResourceArn": TOPIC, "Tags": {"one-too-many": "x"}}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::TagLimitExceeded);
        // Nothing was stored by the refused call.
        let listed = list_tags_for_resource(&rig.ctx, &json!({"ResourceArn": TOPIC}))
            .await
            .expect("listed");
        assert_eq!(listed["Tags"].as_array().map(Vec::len), Some(MAX_TAGS));
    }

    // --------------------------------------------------------------- confirm

    #[tokio::test]
    async fn confirm_subscription_cannot_succeed_and_explains_itself() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let e = confirm_subscription(&rig.ctx, &json!({"TopicArn": TOPIC, "Token": "abc"}))
            .await
            .expect_err("nothing to confirm");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("Token"), "{}", e.message);
        // A wrong topic is answered as a wrong topic, before the token.
        assert_eq!(
            confirm_subscription(
                &rig.ctx,
                &json!({"TopicArn": "arn:aws:sns:queen-1:000000000000:absent", "Token": "abc"})
            )
            .await
            .expect_err("no such topic")
            .kind,
            ErrorKind::NotFound
        );
    }

    /// Every action that names a topic answers SNS's 404 for one that is not
    /// there — one sentence, one code, from every entry point.
    #[tokio::test]
    async fn every_topic_action_answers_the_same_404() {
        let rig = rig().await;
        let absent = json!({"TopicArn": "arn:aws:sns:queen-1:000000000000:absent"});
        let answers = [
            get_topic_attributes(&rig.ctx, &absent).await,
            list_subscriptions_by_topic(&rig.ctx, &absent).await,
            set_topic_attributes(
                &rig.ctx,
                &json!({"TopicArn": "arn:aws:sns:queen-1:000000000000:absent",
                        "AttributeName": "DisplayName", "AttributeValue": "x"}),
            )
            .await,
        ];
        for answer in answers {
            let e = answer.expect_err("no such topic");
            assert_eq!(e.kind, ErrorKind::NotFound);
            assert_eq!(e.kind.http_status(), 404);
        }
    }

    /// A required parameter that is not there names itself, in every action that
    /// takes one.
    #[tokio::test]
    async fn a_missing_parameter_names_itself() {
        let rig = rig().await;
        create(&rig, "events", Value::Null).await.expect("created");
        let answers = [
            ("Name", create_topic(&rig.ctx, &json!({})).await),
            ("TopicArn", delete_topic(&rig.ctx, &json!({})).await),
            (
                "Protocol",
                subscribe(&rig.ctx, &json!({"TopicArn": TOPIC})).await,
            ),
            (
                "Endpoint",
                subscribe(&rig.ctx, &json!({"TopicArn": TOPIC, "Protocol": "sqs"})).await,
            ),
            ("SubscriptionArn", unsubscribe(&rig.ctx, &json!({})).await),
            ("ResourceArn", tag_resource(&rig.ctx, &json!({})).await),
            (
                "AttributeName",
                set_topic_attributes(&rig.ctx, &json!({"TopicArn": TOPIC})).await,
            ),
        ];
        for (member, answer) in answers {
            let e = answer.expect_err("refused");
            assert_eq!(e.kind, ErrorKind::MissingParameter, "{member}: {e}");
            assert!(e.message.contains(member), "{member}: {}", e.message);
        }
    }
}
