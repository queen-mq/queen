//! The action layer: one implementation per SQS/SNS verb, protocol-blind.
//!
//! CONTRACT. `dispatch` takes a decoded [`ProtoRequest`] and a [`Ctx`] and
//! answers a `serde_json::Value` payload or an [`SqsError`]. Nothing in this
//! subtree may look at [`crate::proto::Protocol`], write XML, or read a header:
//! the codecs are above it and Queen is below it, and an action that reached
//! either would be one that has to be tested twice.
//!
//! The set is CLOSED, like the error catalog and for the same reason: an action
//! this facade does not implement must answer `InvalidAction` rather than
//! something plausible, because "plausible" for a client that asked for
//! `AddPermission` means it believes its policy was applied. Where an action is
//! ACCEPTED AND NOT ENFORCED — `AddPermission`, `RemovePermission`, the `Policy`
//! attribute, the KMS attributes — the fact is documented on the variant, in the
//! docs, and in the answer's own shape, never in silence. That is the same
//! honesty as the kafka ACL answer.

use std::sync::Arc;

use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::proto::ProtoRequest;
use crate::sns;
use crate::Facade;

pub mod dlq;
pub mod fifo;
pub mod messages;
pub mod movetask;
pub mod queues;

/// The rig the action tests share. Test-only, and a module rather than a copy
/// per file so that every test drives the same facade over the same double.
#[cfg(test)]
pub mod testing;

/// Every action this facade answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    // ------------------------------------------------------------ queues (M0)
    CreateQueue,
    DeleteQueue,
    GetQueueUrl,
    ListQueues,
    GetQueueAttributes,
    SetQueueAttributes,
    // ----------------------------------------------------------- messages (M0)
    SendMessage,
    SendMessageBatch,
    ReceiveMessage,
    DeleteMessage,
    DeleteMessageBatch,
    // ---------------------------------------------------------- lifecycle (M1)
    ChangeMessageVisibility,
    ChangeMessageVisibilityBatch,
    PurgeQueue,
    ListQueueTags,
    TagQueue,
    UntagQueue,
    // --------------------------------------------------------------- dlq (M3)
    ListDeadLetterSourceQueues,
    StartMessageMoveTask,
    CancelMessageMoveTask,
    ListMessageMoveTasks,
    // --------------------------------------------------------------- sns (M4)
    CreateTopic,
    DeleteTopic,
    ListTopics,
    GetTopicAttributes,
    SetTopicAttributes,
    Subscribe,
    Unsubscribe,
    ConfirmSubscription,
    ListSubscriptions,
    ListSubscriptionsByTopic,
    GetSubscriptionAttributes,
    SetSubscriptionAttributes,
    /// SNS's own tag actions. They are NOT `TagQueue` under another name: the
    /// resource is named by an ARN rather than a URL, the answer is a LIST of
    /// pairs rather than a map, and a missing resource is
    /// [`ErrorKind::ResourceNotFound`] rather than a 400.
    TagResource,
    UntagResource,
    ListTagsForResource,
    Publish,
    PublishBatch,
    // ------------------------------------------------- accepted, NOT enforced
    /// Stored and answered, never enforced: authorization is Queen's, over the
    /// SigV4 keypair. Documented as a non-goal, loudly, rather than emulated.
    AddPermission,
    /// Likewise.
    RemovePermission,
}

impl Action {
    /// Every action, in the order the enum declares them. It is what
    /// [`Action::from_name`] scans and what the tests walk, so the set has ONE
    /// spelling: a variant that reached [`Action::name`] but not this list would
    /// be an action this facade implements and no client can name.
    pub const ALL: [Action; 40] = [
        Action::CreateQueue,
        Action::DeleteQueue,
        Action::GetQueueUrl,
        Action::ListQueues,
        Action::GetQueueAttributes,
        Action::SetQueueAttributes,
        Action::SendMessage,
        Action::SendMessageBatch,
        Action::ReceiveMessage,
        Action::DeleteMessage,
        Action::DeleteMessageBatch,
        Action::ChangeMessageVisibility,
        Action::ChangeMessageVisibilityBatch,
        Action::PurgeQueue,
        Action::ListQueueTags,
        Action::TagQueue,
        Action::UntagQueue,
        Action::ListDeadLetterSourceQueues,
        Action::StartMessageMoveTask,
        Action::CancelMessageMoveTask,
        Action::ListMessageMoveTasks,
        Action::CreateTopic,
        Action::DeleteTopic,
        Action::ListTopics,
        Action::GetTopicAttributes,
        Action::SetTopicAttributes,
        Action::Subscribe,
        Action::Unsubscribe,
        Action::ConfirmSubscription,
        Action::ListSubscriptions,
        Action::ListSubscriptionsByTopic,
        Action::GetSubscriptionAttributes,
        Action::SetSubscriptionAttributes,
        Action::TagResource,
        Action::UntagResource,
        Action::ListTagsForResource,
        Action::Publish,
        Action::PublishBatch,
        Action::AddPermission,
        Action::RemovePermission,
    ];

    /// The action a name denotes, or `None` — which is `InvalidAction` and never
    /// a guess.
    ///
    /// The scan is linear over [`Action::ALL`] rather than a second match on
    /// strings, because two matches are two places one action's spelling can
    /// live and exactly one of them would be updated. Thirty-five pointer
    /// comparisons per request is not a cost worth a source of truth.
    ///
    /// Case-SENSITIVE, as AWS is: `sendmessage` is not an action, and accepting
    /// it here would make this facade the only SQS endpoint on which a client's
    /// typo works — until the day that client is pointed at the real one.
    pub fn from_name(name: &str) -> Option<Action> {
        Action::ALL.into_iter().find(|a| a.name() == name)
    }

    /// The canonical spelling, which is what the XML envelope wraps a result in.
    pub fn name(self) -> &'static str {
        match self {
            Action::CreateQueue => "CreateQueue",
            Action::DeleteQueue => "DeleteQueue",
            Action::GetQueueUrl => "GetQueueUrl",
            Action::ListQueues => "ListQueues",
            Action::GetQueueAttributes => "GetQueueAttributes",
            Action::SetQueueAttributes => "SetQueueAttributes",
            Action::SendMessage => "SendMessage",
            Action::SendMessageBatch => "SendMessageBatch",
            Action::ReceiveMessage => "ReceiveMessage",
            Action::DeleteMessage => "DeleteMessage",
            Action::DeleteMessageBatch => "DeleteMessageBatch",
            Action::ChangeMessageVisibility => "ChangeMessageVisibility",
            Action::ChangeMessageVisibilityBatch => "ChangeMessageVisibilityBatch",
            Action::PurgeQueue => "PurgeQueue",
            Action::ListQueueTags => "ListQueueTags",
            Action::TagQueue => "TagQueue",
            Action::UntagQueue => "UntagQueue",
            Action::ListDeadLetterSourceQueues => "ListDeadLetterSourceQueues",
            Action::StartMessageMoveTask => "StartMessageMoveTask",
            Action::CancelMessageMoveTask => "CancelMessageMoveTask",
            Action::ListMessageMoveTasks => "ListMessageMoveTasks",
            Action::CreateTopic => "CreateTopic",
            Action::DeleteTopic => "DeleteTopic",
            Action::ListTopics => "ListTopics",
            Action::GetTopicAttributes => "GetTopicAttributes",
            Action::SetTopicAttributes => "SetTopicAttributes",
            Action::Subscribe => "Subscribe",
            Action::Unsubscribe => "Unsubscribe",
            Action::ConfirmSubscription => "ConfirmSubscription",
            Action::ListSubscriptions => "ListSubscriptions",
            Action::ListSubscriptionsByTopic => "ListSubscriptionsByTopic",
            Action::GetSubscriptionAttributes => "GetSubscriptionAttributes",
            Action::SetSubscriptionAttributes => "SetSubscriptionAttributes",
            Action::TagResource => "TagResource",
            Action::UntagResource => "UntagResource",
            Action::ListTagsForResource => "ListTagsForResource",
            Action::Publish => "Publish",
            Action::PublishBatch => "PublishBatch",
            Action::AddPermission => "AddPermission",
            Action::RemovePermission => "RemovePermission",
        }
    }

    /// Whether this action is one of SNS's. The two services share a listener
    /// and a signature scope but not an XML namespace or an action set.
    ///
    /// `AddPermission` and `RemovePermission` exist in BOTH services with the
    /// same name and are classified as SQS's here: this facade neither enforces
    /// nor stores them, so the only thing the answer's namespace decides is
    /// which of two empty envelopes the client parses — and the SQS one is what
    /// a Query request carrying no SNS version is asking for.
    pub fn is_sns(self) -> bool {
        matches!(
            self,
            Action::CreateTopic
                | Action::DeleteTopic
                | Action::ListTopics
                | Action::GetTopicAttributes
                | Action::SetTopicAttributes
                | Action::Subscribe
                | Action::Unsubscribe
                | Action::ConfirmSubscription
                | Action::ListSubscriptions
                | Action::ListSubscriptionsByTopic
                | Action::GetSubscriptionAttributes
                | Action::SetSubscriptionAttributes
                | Action::TagResource
                | Action::UntagResource
                | Action::ListTagsForResource
                | Action::Publish
                | Action::PublishBatch
        )
    }
}

/// Who is asking, resolved from the signature.
#[derive(Debug, Clone, Default)]
pub struct Principal {
    /// `None` only on an `auth=off` listener.
    pub access_key_id: Option<String>,
    /// The bearer every call to Queen for this request carries. It is the
    /// principal's own when the directory names one and the process default
    /// otherwise — and it is what makes one facade in front of many tenants
    /// possible without a credential ever reaching an action.
    pub queen_token: Option<String>,
}

/// Everything one action may know.
///
/// It carries no request bytes and no headers on purpose: an action that needed
/// them would be one the codec layer cannot fully decouple from. The single
/// exception is `host`, which is here because a queue URL must name the host the
/// CLIENT reached rather than the one this process bound.
pub struct Ctx {
    pub facade: Arc<Facade>,
    pub principal: Principal,
    /// The `Host` the request arrived on, for the queue URLs this answer mints.
    pub host: String,
    /// The id echoed in `ResponseMetadata` and in every error. One per request,
    /// and it is what ties a client's report to this facade's log line.
    pub request_id: String,
}

impl Ctx {
    /// The bearer this request's calls to Queen carry.
    pub fn token(&self) -> Option<&str> {
        self.principal.queen_token.as_deref()
    }
}

/// Route one decoded request to its implementation.
///
/// The name is resolved to an [`Action`] FIRST and the match is on the enum, so
/// the closed set is enforced in one place: an unknown name is `InvalidAction`
/// at the resolution above, and every known one has an arm the compiler checks.
/// Every action in the set is now implemented; an action a later milestone adds
/// must answer `InvalidAction` naming that milestone rather than a plausible
/// empty success, for the module header's reason — a client told `PurgeQueue`
/// succeeded believes its queue is empty.
pub async fn dispatch(ctx: &Ctx, request: &ProtoRequest) -> SqsResult<serde_json::Value> {
    let Some(action) = Action::from_name(&request.action) else {
        // The name is NOT echoed. It is unbounded client-controlled input that
        // would land in this facade's log and in the answer's body, and the
        // client already knows what it asked for.
        return Err(SqsError::new(ErrorKind::InvalidAction));
    };
    let params = &request.params;
    match action {
        // --------------------------------------------------------- queues (M0)
        Action::CreateQueue => queues::create_queue(ctx, params).await,
        Action::DeleteQueue => queues::delete_queue(ctx, params).await,
        Action::GetQueueUrl => queues::get_queue_url(ctx, params).await,
        Action::ListQueues => queues::list_queues(ctx, params).await,
        Action::GetQueueAttributes => queues::get_queue_attributes(ctx, params).await,
        Action::SetQueueAttributes => queues::set_queue_attributes(ctx, params).await,
        // ------------------------------------------------------- messages (M0)
        Action::SendMessage => messages::send_message(ctx, params).await,
        Action::SendMessageBatch => messages::send_message_batch(ctx, params).await,
        Action::ReceiveMessage => messages::receive_message(ctx, params).await,
        Action::DeleteMessage => messages::delete_message(ctx, params).await,
        Action::DeleteMessageBatch => messages::delete_message_batch(ctx, params).await,
        // ------------------------------------------------------ lifecycle (M1)
        Action::ChangeMessageVisibility => messages::change_message_visibility(ctx, params).await,
        Action::ChangeMessageVisibilityBatch => {
            messages::change_message_visibility_batch(ctx, params).await
        }
        Action::ListQueueTags => queues::list_queue_tags(ctx, params).await,
        Action::TagQueue => queues::tag_queue(ctx, params).await,
        Action::UntagQueue => queues::untag_queue(ctx, params).await,
        Action::PurgeQueue => queues::purge_queue(ctx, params).await,
        // ------------------------------------------------------------ dlq (M3)
        Action::ListDeadLetterSourceQueues => {
            dlq::list_dead_letter_source_queues(ctx, params).await
        }
        Action::StartMessageMoveTask => movetask::start_message_move_task(ctx, params).await,
        Action::CancelMessageMoveTask => movetask::cancel_message_move_task(ctx, params).await,
        Action::ListMessageMoveTasks => movetask::list_message_move_tasks(ctx, params).await,
        // ------------------------------------------------------------ sns (M4)
        Action::CreateTopic => sns::admin::create_topic(ctx, params).await,
        Action::DeleteTopic => sns::admin::delete_topic(ctx, params).await,
        Action::ListTopics => sns::admin::list_topics(ctx, params).await,
        Action::GetTopicAttributes => sns::admin::get_topic_attributes(ctx, params).await,
        Action::SetTopicAttributes => sns::admin::set_topic_attributes(ctx, params).await,
        Action::Subscribe => sns::admin::subscribe(ctx, params).await,
        Action::Unsubscribe => sns::admin::unsubscribe(ctx, params).await,
        Action::ConfirmSubscription => sns::admin::confirm_subscription(ctx, params).await,
        Action::ListSubscriptions => sns::admin::list_subscriptions(ctx, params).await,
        Action::ListSubscriptionsByTopic => {
            sns::admin::list_subscriptions_by_topic(ctx, params).await
        }
        Action::GetSubscriptionAttributes => {
            sns::admin::get_subscription_attributes(ctx, params).await
        }
        Action::SetSubscriptionAttributes => {
            sns::admin::set_subscription_attributes(ctx, params).await
        }
        Action::TagResource => sns::admin::tag_resource(ctx, params).await,
        Action::UntagResource => sns::admin::untag_resource(ctx, params).await,
        Action::ListTagsForResource => sns::admin::list_tags_for_resource(ctx, params).await,
        Action::Publish => sns::publish::publish(ctx, params).await,
        Action::PublishBatch => sns::publish::publish_batch(ctx, params).await,
        // ---------------------------------------------- accepted, NOT enforced
        Action::AddPermission | Action::RemovePermission => permission(ctx, params).await,
    }
}

/// `AddPermission` / `RemovePermission`: accepted, answered, never enforced.
///
/// The plan's first stated non-goal. Authorization here is QUEEN's, over the
/// SigV4 keypair — this facade has no principal model to apply an SQS policy to,
/// and emulating one would produce the single worst outcome available: a client
/// that is told its policy is in force when nothing reads it.
///
/// So the call is validated as far as SQS validates it (the queue must exist,
/// the label is required) and then does nothing, which is the honest half of
/// "accepted and not enforced". The other half is documentation, and it is loud.
async fn permission(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = queues::queue_of(ctx, params)?;
    ctx.facade.registry.require(&name, ctx.token()).await?;
    queues::require_text(params, "Label")?;
    Ok(serde_json::Value::Null)
}

/// The 10-entry cap every batch action shares, and the two errors either side of
/// it. AWS's own numbers: an empty batch and an over-long one are DIFFERENT
/// errors, and an SDK's batching helper branches on which.
pub const MAX_BATCH_ENTRIES: usize = 10;

/// Validate the entry list every batch action starts with: non-empty, within
/// [`MAX_BATCH_ENTRIES`], and with DISTINCT ids. Distinctness is not fussiness —
/// results are reported by the client's own `Id`, so two entries sharing one id
/// make the answer unreadable, which is why AWS gives it an error of its own.
///
/// One line, because the rule lives with the four batch actions that apply it
/// ([`messages::check_entry_ids`], which also owns the entry-id charset). Two
/// copies of a cap are two caps.
pub fn check_batch(ids: &[String]) -> SqsResult<()> {
    messages::check_entry_ids(ids)
}

/// The one place a broker failure becomes a client-visible error, so that no
/// action writes its own mapping. See [`SqsError::from_queen`].
pub fn queen_error(e: &crate::queen::Error) -> SqsError {
    SqsError::from_queen(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The set is closed and has one spelling. A variant missing from `ALL` is
    /// an action no client can name; two variants sharing a name are an action
    /// whose second implementation is unreachable.
    #[test]
    fn every_action_round_trips_through_its_own_name() {
        let mut seen: Vec<&str> = Vec::new();
        for action in Action::ALL {
            let name = action.name();
            assert_eq!(Action::from_name(name), Some(action), "{name}");
            assert!(!seen.contains(&name), "{name} is in the table twice");
            seen.push(name);
        }
    }

    /// Case-sensitivity is not pedantry: a facade that accepted `sendmessage`
    /// would be the only SQS endpoint on which that client works.
    #[test]
    fn an_unknown_or_miscased_name_is_not_an_action() {
        for name in [
            "",
            "sendmessage",
            "SendMessages",
            "AmazonSQS.SendMessage",
            " SendMessage",
        ] {
            assert_eq!(Action::from_name(name), None, "{name:?}");
        }
    }

    /// The SNS half of the set, counted: the two services share this listener
    /// and neither the namespace nor the action set.
    #[test]
    fn the_sns_actions_are_the_seventeen_of_them() {
        let sns: Vec<&str> = Action::ALL
            .into_iter()
            .filter(|a| a.is_sns())
            .map(Action::name)
            .collect();
        assert_eq!(sns.len(), 17, "{sns:?}");
        assert!(sns.contains(&"Publish"));
        assert!(!sns.contains(&"SendMessage"));
        // SNS's tag actions are SNS's; SQS's three are not, and the two sets
        // have no name in common.
        assert!(sns.contains(&"ListTagsForResource"));
        assert!(!sns.contains(&"ListQueueTags"));
        // Shared by both services, classified as SQS's — see `is_sns`.
        assert!(!Action::AddPermission.is_sns());
    }

    /// The batch rule has one owner. This is the delegation, proved on the two
    /// ends AWS gives different errors.
    #[test]
    fn the_batch_cap_is_the_message_layers_own() {
        assert!(check_batch(&["a".to_string(), "b".to_string()]).is_ok());
        assert_eq!(
            check_batch(&[]).expect_err("empty").kind,
            ErrorKind::EmptyBatchRequest
        );
        let eleven: Vec<String> = (0..=MAX_BATCH_ENTRIES).map(|i| i.to_string()).collect();
        assert_eq!(
            check_batch(&eleven).expect_err("too many").kind,
            ErrorKind::TooManyEntriesInBatchRequest
        );
        assert_eq!(
            check_batch(&["a".to_string(), "a".to_string()])
                .expect_err("not distinct")
                .kind,
            ErrorKind::BatchEntryIdsNotDistinct
        );
    }
}
