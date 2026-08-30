//! DeleteTopics (key 20), v1-v5 — `kafka-topics.sh --delete` against a Queen
//! queue.
//!
//! ## Say this part out loud
//!
//! This deletes a Queen queue **and everything in it**, from a Kafka client.
//! It is not a privilege escalation — the same bearer can already issue the same
//! `DELETE /api/v1/resources/queues/:queue` over HTTP, and the connection's
//! credential is the only thing that reaches Queen — but it is a new blast
//! radius on a facade that until M7 could only ever create. Two consequences are
//! designed rather than discovered:
//!
//! 1. A queue that native Queen producers share with Kafka clients can be
//!    deleted by the Kafka half. There is no facade-side guard that would not be
//!    a lie: the facade cannot tell a Kafka-created queue from a native one (the
//!    `__` rule is about broker-internal names, not about who made the queue).
//!    The mitigation is documentation on `/deploy/kafka` and the operator's own
//!    token scoping.
//! 2. Committed offsets under `qk:group:*:<topic>:*` are **not** removed with
//!    the topic and become orphans. Kafka has the same shape — offsets outlive a
//!    deleted topic until the group expires — and DeleteGroups is the tool for
//!    them. Stated in `compat/ERRORS.md` rather than half-fixed here.
//!
//! ## Why a missing topic is not an HTTP error
//!
//! The broker's route is idempotent by design: it answers 200 either way and
//! rewrites the body so `deleted` mirrors `existed`, precisely so a client that
//! trusts `deleted` is not told a queue it never had was removed
//! (server/src/handlers/queues.rs). So `{"deleted": false}` is the authoritative
//! "there was no such queue" and is what becomes UNKNOWN_TOPIC_OR_PARTITION —
//! Kafka's own answer for deleting a topic that is not there.
//!
//! ## Why an illegal name is UNKNOWN here and INVALID in CreateTopics
//!
//! This is not a name-validation surface, it is a "do you have this" surface.
//! [`super::metadata::not_a_topic_here`] is the rule already written for exactly
//! that distinction, and it is the one this handler uses.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{DeleteTopicsRequest, DeleteTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::create_topics::MAX_CREATES_PER_REQUEST;
use crate::handlers::metadata;
use crate::topic_record;
use crate::{queen, throttle, Facade};

/// One line per window when the per-request ceiling binds.
static DELETE_CAP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

pub async fn handle(
    facade: &Facade,
    req: &DeleteTopicsRequest,
    token: Option<&str>,
) -> DeleteTopicsResponse {
    let mut throttle_ms: Option<i32> = None;
    let mut deleted = 0usize;
    let mut deferred = 0usize;
    let mut results = Vec::with_capacity(req.topic_names.len());
    // The names whose queue actually went, for the config records swept after
    // the loop. See [`forget_the_records`].
    let mut forgettable: Vec<String> = Vec::new();

    // In request order and in sequence: each delete is one upstream call under
    // a ten-second budget on a connection that is muted until the whole response
    // is written (conn.rs), which is the same argument that bounds the create
    // path — and the same constant, because it is the same cost.
    //
    // v6 replaces `topic_names` with `topics[]` carrying a name OR a topic id,
    // and a topic id is a name this facade cannot resolve; the advertised
    // window stops at v5 so `topic_names` is always the field that is set.
    for name in &req.topic_names {
        // The name rule, in the one code this API may answer. See the header.
        if let Some(code) = metadata::not_a_topic_here(name.as_str()) {
            results.push(errored(
                name.clone(),
                code,
                "no such queue: the name is either reserved for a broker's internal topics \
                 (`__`) or not a legal Kafka topic name, so this facade has none"
                    .to_string(),
            ));
            continue;
        }

        if deleted >= MAX_CREATES_PER_REQUEST {
            deferred += 1;
            throttle_ms = throttle::longest(throttle_ms, Some(throttle::DEFAULT_MS));
            results.push(errored(
                name.clone(),
                // Retriable on purpose: the client asks again and the next
                // hundred go, so a request naming a thousand topics converges
                // over a few calls. DeleteTopics has no
                // THROTTLING_QUOTA_EXCEEDED version inside the advertised
                // window (v6 would, and v6 is out for the topic-id reason), so
                // REQUEST_TIMED_OUT is the retriable code available here.
                ResponseError::RequestTimedOut,
                format!(
                    "this request asked to delete more than {MAX_CREATES_PER_REQUEST} topics; \
                     the rest are answered with a retriable code and go as the client retries"
                ),
            ));
            continue;
        }
        deleted += 1;

        match facade.catalog.delete(name.as_str(), token).await {
            Ok(queen::Deleted { existed: true }) => {
                tracing::info!(
                    target: "kafka",
                    topic = name.as_str(),
                    "deleted a Queen queue for a Kafka client (DeleteTopics)"
                );
                forgettable.push(name.to_string());
                results.push(
                    DeletableTopicResult::default()
                        .with_name(Some(name.clone()))
                        .with_error_code(0)
                        .with_error_message(None),
                );
            }
            Ok(queen::Deleted { existed: false }) => results.push(errored(
                name.clone(),
                ResponseError::UnknownTopicOrPartition,
                "no such queue; nothing was deleted".to_string(),
            )),
            Err(e) => {
                tracing::error!(
                    target: "kafka",
                    topic = name.as_str(),
                    error = %e,
                    "DeleteTopics could not delete the queue"
                );
                throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                results.push(errored(name.clone(), failed(&e), e.to_string()));
            }
        }
    }

    if deferred > 0 {
        if let Some(suppressed) = DELETE_CAP.tick_now() {
            tracing::warn!(
                target: "kafka",
                deleted,
                deferred,
                suppressed,
                "one request asked to delete more topics than are deleted at a time; the rest \
                 were answered REQUEST_TIMED_OUT and go as the client retries"
            );
        }
    }

    forget_the_records(facade, &forgettable, token).await;

    DeleteTopicsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_responses(results)
}

/// Remove the config records of the queues this request actually deleted
/// ([`crate::topic_record`]).
///
/// A record outliving its queue is the one way a stale retention could be
/// reported: a topic recreated THROUGH this facade writes a fresh record, but a
/// topic recreated outside it would otherwise be described from the dead one.
/// The `qid` pin catches that too, and this is the cheaper half of the same
/// guarantee — it also keeps the store from accumulating a row per topic ever
/// created.
///
/// ONE KV call, after the loop, and never a reason to fail a delete that
/// already happened: the queue is gone either way and the record's own staleness
/// check is what stands behind a failure here.
async fn forget_the_records(facade: &Facade, deleted: &[String], token: Option<&str>) {
    if deleted.is_empty() {
        return;
    }
    if let Err(e) = topic_record::remove_many(facade.queen.as_ref(), deleted, token).await {
        tracing::warn!(
            target: "kafka",
            error = %e,
            topics = deleted.len(),
            "the queues were deleted but their config records were not; a topic recreated \
             outside this facade would be caught by the record's queue-id pin instead"
        );
    }
}

/// `error_message` is v5+ and the encoder drops it below that, which is exactly
/// why v5 is in the advertised window: it is where "there is no such queue" gets
/// to say so in words as well as in a code.
fn errored(name: TopicName, code: ResponseError, why: String) -> DeletableTopicResult {
    DeletableTopicResult::default()
        .with_name(Some(name))
        .with_error_code(code.code())
        .with_error_message(Some(StrBytes::from_string(why)))
}

/// The closest Kafka code for a failed call to Queen. Retriability first, the
/// same axis every other handler picks on.
fn failed(e: &queen::Error) -> ResponseError {
    match e {
        queen::Error::Transport(_) => ResponseError::RequestTimedOut,
        queen::Error::Status { code, .. } => match code {
            401 | 403 => ResponseError::TopicAuthorizationFailed,
            // The route answers 200 for a queue that is not there, so a 404
            // here is the route itself being absent — a facade pointed at
            // something that is not a Queen broker. Loud.
            404 => ResponseError::UnknownServerError,
            408 | 429 | 502..=504 => ResponseError::RequestTimedOut,
            _ => ResponseError::UnknownServerError,
        },
        queen::Error::Body(_) | queen::Error::Precondition { .. } => {
            ResponseError::UnknownServerError
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::alter_configs::tests::track;
    use crate::handlers::testing::facade_and_queen;
    use crate::queen::Error;

    fn request(names: &[&str]) -> DeleteTopicsRequest {
        DeleteTopicsRequest::default()
            .with_topic_names(
                names
                    .iter()
                    .map(|n| TopicName(StrBytes::from_string(n.to_string())))
                    .collect(),
            )
            .with_timeout_ms(30_000)
    }

    fn code(r: &DeleteTopicsResponse, name: &str) -> i16 {
        r.responses
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name))
            .unwrap_or_else(|| panic!("{name} is not in the answer"))
            .error_code
    }

    /// The queue's config record goes with the queue. A record that outlived
    /// its queue is the one way a stale retention could be reported, and this
    /// is the cheap half of that guarantee — the `qid` pin is the other half,
    /// for a queue dropped outside this facade.
    #[tokio::test]
    async fn a_delete_removes_the_record() {
        let (f, api) = facade_and_queen(&[("orders", 2), ("keep", 1)]);
        track(
            &api,
            "orders",
            &[("retentionEnabled", serde_json::json!(true))],
        );
        track(&api, "keep", &[]);

        let r = handle(&f, &request(&["orders"]), None).await;

        assert_eq!(code(&r, "orders"), 0);
        assert!(api
            .kv_get(
                crate::offsets::NAMESPACE,
                &crate::topic_record::key("orders")
            )
            .is_none());
        // ...and only that one.
        assert!(api
            .kv_get(crate::offsets::NAMESPACE, &crate::topic_record::key("keep"))
            .is_some());
    }

    /// A delete that found no queue removes no record: the name may belong to
    /// somebody's live topic that this request simply misspelled the case of,
    /// and DeleteTopics is idempotent on the queue but not licensed to sweep
    /// bookkeeping for a queue it did not delete.
    #[tokio::test]
    async fn a_delete_that_found_nothing_removes_no_record() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        handle(&f, &request(&["never-existed"]), None).await;
        assert!(api
            .kv_get(
                crate::offsets::NAMESPACE,
                &crate::topic_record::key("orders")
            )
            .is_some());
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a delete that found nothing still called the record store"
        );
    }

    #[tokio::test]
    async fn an_existing_topic_is_deleted() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(&["orders"]), None).await;

        assert_eq!(code(&r, "orders"), 0);
        assert_eq!(api.deleted(), ["orders"]);
        // ...and it is gone from the catalog the very next read serves, which
        // is what makes `--delete` then `--list` agree.
        assert!(f.catalog.list(None).await.unwrap().is_empty());
    }

    /// `deleted:false` is the route's authoritative "there was no such queue",
    /// and it is the ONLY thing that becomes UNKNOWN_TOPIC_OR_PARTITION — the
    /// call itself succeeded.
    #[tokio::test]
    async fn a_topic_that_is_not_there_is_unknown_and_not_an_error() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(&["never-existed"]), None).await;

        assert_eq!(
            code(&r, "never-existed"),
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(api.deleted(), ["never-existed"], "the call was still made");
    }

    /// Not a name-validation surface: a `__` name and an illegal one are both
    /// "this facade has no such topic", which is the code every client accepts
    /// on this API.
    #[tokio::test]
    async fn a_reserved_or_illegal_name_is_unknown_and_never_reaches_queen() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let names = ["__consumer_offsets", "has spaces", "..", "a/b"];
        let r = handle(&f, &request(&names), None).await;

        for name in names {
            assert_eq!(
                code(&r, name),
                ResponseError::UnknownTopicOrPartition.code(),
                "{name}"
            );
        }
        assert!(
            api.deleted().is_empty(),
            "a name this facade hides was sent upstream anyway"
        );
    }

    /// Order is the request's, and one topic's answer does not move another's.
    #[tokio::test]
    async fn results_line_up_with_the_request() {
        let (f, _) = facade_and_queen(&[("a", 1), ("c", 1)]);
        let r = handle(&f, &request(&["a", "b", "c", "__d"]), None).await;

        let names: Vec<&str> = r
            .responses
            .iter()
            .map(|t| t.name.as_ref().unwrap().as_str())
            .collect();
        assert_eq!(names, ["a", "b", "c", "__d"]);
        assert_eq!(r.responses[0].error_code, 0);
        assert_eq!(
            r.responses[1].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(r.responses[2].error_code, 0);
        assert_eq!(
            r.responses[3].error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }

    #[tokio::test]
    async fn queens_failures_map_to_codes_a_client_can_act_on() {
        for (e, want) in [
            (
                Error::status(401, "no"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(403, "no"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(500, "boom"),
                ResponseError::UnknownServerError,
            ),
            (
                Error::status(503, "draining"),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::Transport("refused".into()),
                ResponseError::RequestTimedOut,
            ),
        ] {
            let (f, api) = facade_and_queen(&[("orders", 2)]);
            api.fail_delete(e.clone());
            let r = handle(&f, &request(&["orders"]), None).await;
            assert_eq!(code(&r, "orders"), want.code(), "{e}");
        }
    }

    /// A rate cap carries its wait, and the code beside it is retriable —
    /// DeleteTopics has no THROTTLING_QUOTA_EXCEEDED version inside the
    /// advertised window.
    #[tokio::test]
    async fn a_rate_cap_carries_its_wait() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.fail_delete(Error::Status {
            code: 429,
            body: "rate_limited".into(),
            retry_after_ms: Some(4_000),
        });
        let r = handle(&f, &request(&["orders"]), None).await;

        assert_eq!(code(&r, "orders"), ResponseError::RequestTimedOut.code());
        assert_eq!(r.throttle_time_ms, 4_000);
    }

    #[tokio::test]
    async fn one_request_deletes_at_most_the_bound() {
        let queues: Vec<(String, i64)> = (0..MAX_CREATES_PER_REQUEST + 5)
            .map(|i| (format!("t{i}"), 1))
            .collect();
        let refs: Vec<(&str, i64)> = queues.iter().map(|(n, p)| (n.as_str(), *p)).collect();
        let (f, api) = facade_and_queen(&refs);
        let names: Vec<&str> = refs.iter().map(|(n, _)| *n).collect();
        let r = handle(&f, &request(&names), None).await;

        assert_eq!(api.deleted().len(), MAX_CREATES_PER_REQUEST);
        let refused: Vec<i16> = r
            .responses
            .iter()
            .skip(MAX_CREATES_PER_REQUEST)
            .map(|t| t.error_code)
            .collect();
        assert_eq!(refused.len(), 5);
        assert!(refused
            .iter()
            .all(|c| *c == ResponseError::RequestTimedOut.code()));
    }

    #[tokio::test]
    async fn the_connections_credential_is_what_deletes() {
        let (root, api) = facade_and_queen(&[("orders", 2)]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        handle(&f, &request(&["orders"]), Some("tenant-key")).await;

        let tokens = api.tokens.lock().unwrap().clone();
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some("tenant-key")),
            "a delete went out under the wrong credential: {tokens:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_request_touches_nothing() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(&[]), None).await;
        assert!(r.responses.is_empty());
        assert!(api.deleted().is_empty());
    }
}
