//! IncrementalAlterConfigs (key 44), v0-v1 — the DELTA form, and the one
//! `kafka-configs.sh --alter` actually sends.
//!
//! `ConfigCommand` has used `adminClient.incrementalAlterConfigs` since Kafka
//! 2.3 and 3.9's has no fallback to the deprecated key 33; every Terraform
//! provider and every modern AdminClient is the same. So this, and not
//! [`super::alter_configs`], is the key that decides whether
//! `--alter --add-config retention.ms=...` works against this facade.
//!
//! ## The difference from key 33, in one sentence
//!
//! AlterConfigs replaces a resource's whole configuration; this one names a
//! DELTA and everything it does not name is left exactly as it is. Here that
//! becomes `stored ∪ delta`, posted as one whole `/configure` bag — the merge is
//! [`crate::topic_record::merge`] and the completeness of `stored` is what makes
//! it lossless. Everything else — the untracked refusal, the broker refusal, the
//! ordering of the write and the record, cluster mode, the error taxonomy — is
//! shared with [`super::alter_configs`] and lives there.
//!
//! ## The four operations
//!
//! `AlterableConfig::config_operation` is SET(0), DELETE(1), APPEND(2) or
//! SUBTRACT(3). APPEND and SUBTRACT are legal only for LIST-typed configs, and
//! of the three keys this facade has only `cleanup.policy` is a list in Kafka —
//! so they are refused on the two scalars by name and computed on
//! `cleanup.policy`, whose value is always the single-element `[delete]`. That
//! makes `APPEND compact` the compaction refusal (which is where it should land)
//! and `SUBTRACT delete` an empty policy, which is refused for its own reason.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::incremental_alter_configs_request::AlterableConfig;
use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};

use crate::handlers::alter_configs::{
    broker, commit, context, other_resource, outcome_fields, topic_names, Context, Verdict,
    RESOURCE_BROKER, RESOURCE_TOPIC,
};
use crate::topic_config::{self, CLEANUP_DELETE, CLEANUP_POLICY};
use crate::topic_record;
use crate::{throttle, Facade};

/// Kafka's `AlterConfigOp.OpType`, the numbers the wire carries.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

pub async fn handle(
    facade: &Facade,
    req: &IncrementalAlterConfigsRequest,
    token: Option<&str>,
) -> IncrementalAlterConfigsResponse {
    let ctx = context(
        facade,
        &topic_names(
            req.resources
                .iter()
                .map(|r| (r.resource_type, r.resource_name.as_str())),
        ),
        token,
    )
    .await;
    let mut throttle_ms = ctx.throttle_ms;

    let mut responses = Vec::with_capacity(req.resources.len());
    for resource in &req.resources {
        let verdict = match resource.resource_type {
            RESOURCE_TOPIC => plan_topic(&ctx, resource.resource_name.as_str(), &resource.configs),
            RESOURCE_BROKER => broker(facade, resource.resource_name.as_str()),
            other => other_resource(other),
        };

        let outcome = match verdict {
            Verdict::Refuse(code, why) => Some((code, why)),
            Verdict::Unchanged => None,
            // `validate_only` (v0-v1), honoured the same way key 33 honours it.
            Verdict::Write { .. } if req.validate_only => None,
            Verdict::Write { qid, bag } => {
                let landed = commit(facade, resource.resource_name.as_str(), qid, bag, token).await;
                throttle_ms = throttle::longest(throttle_ms, landed.throttle_ms);
                landed.error
            }
        };
        let (code, why) = outcome_fields(outcome);
        responses.push(
            AlterConfigsResourceResponse::default()
                .with_error_code(code)
                .with_error_message(why)
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone()),
        );
    }

    IncrementalAlterConfigsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_responses(responses)
}

/// The delta, merged onto what the facade last applied.
fn plan_topic(ctx: &Context, topic: &str, configs: &[AlterableConfig]) -> Verdict {
    let (qid, stored) = match ctx.tracked(topic) {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };

    let mut delta: topic_config::Delta = Vec::new();
    for config in configs {
        match one(config) {
            Ok(mut entries) => delta.append(&mut entries),
            Err(refusal) => return refusal,
        }
    }

    // `merge` and not `absorb` onto an empty bag: this is the whole difference
    // between key 44 and key 33, and it is the reason a client may set
    // `retention.ms` here without losing whatever else the facade had applied.
    let desired = topic_record::merge(&stored, &delta);
    if desired == stored {
        Verdict::Unchanged
    } else {
        Verdict::Write { qid, bag: desired }
    }
}

/// What one `(name, operation, value)` triple contributes to the delta.
fn one(config: &AlterableConfig) -> Result<topic_config::Delta, Verdict> {
    let name = config.name.as_str();
    let value = config.value.as_ref().map(|v| v.as_str());
    let refuse = |why: String| Verdict::Refuse(ResponseError::InvalidConfig, why);

    match config.config_operation {
        OP_SET => topic_config::alter(name, value).map_err(refuse),
        // The request's `value` is IGNORED for a DELETE, which is what Kafka
        // does: the operation names the key to reset, not a value to remove.
        OP_DELETE => topic_config::reset(name).map_err(refuse),
        op @ (OP_APPEND | OP_SUBTRACT) => list_op(name, value, op).map_err(refuse),
        other => Err(Verdict::Refuse(
            ResponseError::InvalidRequest,
            format!(
                "`{other}` is not an AlterConfigOp: the operations are SET ({OP_SET}), DELETE \
                 ({OP_DELETE}), APPEND ({OP_APPEND}) and SUBTRACT ({OP_SUBTRACT})"
            ),
        )),
    }
}

/// APPEND and SUBTRACT, which are legal only on a LIST-typed config.
///
/// Of this facade's three keys only `cleanup.policy` is a list in Kafka, and its
/// value is always the single-element `[delete]` — there is no other policy and
/// no way to reach one. So the resulting list is computed and run through the
/// SAME rule a SET would use, which puts `APPEND compact` on the compaction
/// refusal (where an operator needs to meet it) rather than on a generic "not a
/// list" message.
fn list_op(name: &str, value: Option<&str>, op: i8) -> Result<topic_config::Delta, String> {
    let verb = if op == OP_APPEND {
        "append"
    } else {
        "subtract"
    };
    if name != CLEANUP_POLICY {
        return Err(format!(
            "config value {verb} is not allowed for config `{name}`: {verb} applies to \
             LIST-typed configs and `{name}` is not one. Set it outright instead"
        ));
    }
    // The current list, which is the only list there is.
    let mut policies: Vec<&str> = vec![CLEANUP_DELETE];
    let asked = value.unwrap_or_default().trim();
    if op == OP_APPEND {
        if !policies.iter().any(|p| p.eq_ignore_ascii_case(asked)) {
            policies.push(asked);
        }
    } else {
        policies.retain(|p| !p.eq_ignore_ascii_case(asked));
    }
    // ...and the result goes through the SET vocabulary, so an appended
    // `compact` meets the refusal that stops Kafka Connect at startup and an
    // emptied policy meets its own.
    topic_config::alter(CLEANUP_POLICY, Some(&policies.join(",")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::alter_configs::tests::track;
    use crate::handlers::testing::facade_and_queen;
    use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
    use kafka_protocol::protocol::StrBytes;
    use serde_json::json;

    fn op(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
        AlterableConfig::default()
            .with_name(StrBytes::from_string(name.to_string()))
            .with_config_operation(operation)
            .with_value(value.map(|v| StrBytes::from_string(v.to_string())))
    }

    fn resource(kind: i8, name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
        AlterConfigsResource::default()
            .with_resource_type(kind)
            .with_resource_name(StrBytes::from_string(name.to_string()))
            .with_configs(configs)
    }

    fn request(resources: Vec<AlterConfigsResource>) -> IncrementalAlterConfigsRequest {
        IncrementalAlterConfigsRequest::default().with_resources(resources)
    }

    fn message(r: &AlterConfigsResourceResponse) -> String {
        r.error_message
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_default()
    }

    /// THE acceptance for key 44: `kafka-configs.sh --alter --add-config
    /// retention.ms=604800000` posts the whole merged bag, and it names no key
    /// the delta did not touch.
    #[tokio::test]
    async fn a_set_merges_onto_the_record_and_leaves_other_keys_alone() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[("dedupWindowSeconds", json!(86_400))]);

        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", OP_SET, Some("604800000"))],
            )]),
            None,
        )
        .await;

        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        // The key the delta never named is STILL THERE, which is the whole
        // point: a partial `/configure` would have reset it to 3600.
        assert_eq!(
            api.configured(),
            [(
                "orders".to_string(),
                json!({
                    "dedupWindowSeconds": 86_400,
                    "retentionEnabled": true,
                    "retentionSeconds": 604_800
                })
            )]
        );
    }

    /// DELETE resets the key to Queen's own default by dropping it out of the
    /// bag, and leaves everything else alone. This is the third step of the
    /// round trip: describe, alter, delete-config, describe again.
    #[tokio::test]
    async fn a_delete_drops_the_key_and_keeps_the_rest() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("dedupWindowSeconds", json!(86_400)),
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(604_800)),
            ],
        );

        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                // The value is ignored for a DELETE, exactly as Kafka ignores
                // it, and this sends one to prove it.
                vec![op("retention.ms", OP_DELETE, Some("whatever"))],
            )]),
            None,
        )
        .await;

        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert_eq!(
            api.configured(),
            [("orders".to_string(), json!({"dedupWindowSeconds": 86_400}))]
        );
    }

    /// APPEND and SUBTRACT: refused by name on the two scalars, computed on the
    /// one list — and `APPEND compact` lands on the compaction refusal, which
    /// is the message an operator needs to read.
    #[tokio::test]
    async fn append_and_subtract_are_list_operations() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);

        for (name, operation) in [
            ("retention.ms", OP_APPEND),
            ("retention.ms", OP_SUBTRACT),
            ("min.insync.replicas", OP_APPEND),
        ] {
            let r = handle(
                &f,
                &request(vec![resource(
                    RESOURCE_TOPIC,
                    "orders",
                    vec![op(name, operation, Some("1000"))],
                )]),
                None,
            )
            .await;
            assert_eq!(
                r.responses[0].error_code,
                ResponseError::InvalidConfig.code(),
                "{name} op {operation}"
            );
            let why = message(&r.responses[0]);
            assert!(why.contains(name) && why.contains("LIST-typed"), "{why}");
        }

        // `APPEND compact` on the one list config: the compaction refusal.
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("cleanup.policy", OP_APPEND, Some("compact"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        assert!(message(&r.responses[0]).contains("compaction"));

        // `APPEND delete` computes the list it already is: a no-op.
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("cleanup.policy", OP_APPEND, Some("delete"))],
            )]),
            None,
        )
        .await;
        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));

        // `SUBTRACT delete` empties it, and a topic with no cleanup policy is
        // not a thing this facade will have.
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("cleanup.policy", OP_SUBTRACT, Some("delete"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        assert!(message(&r.responses[0]).contains("cannot be emptied"));

        assert!(
            api.configured().is_empty(),
            "a refused list operation reached Queen"
        );
    }

    /// An operation number that is not one of the four is INVALID_REQUEST and
    /// names it, rather than being silently treated as a SET.
    #[tokio::test]
    async fn an_unknown_operation_is_invalid_request() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", 9, Some("-1"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidRequest.code()
        );
        assert!(message(&r.responses[0]).contains('9'));
        assert!(api.configured().is_empty());
    }

    /// `validate_only` runs everything and writes nothing.
    #[tokio::test]
    async fn validate_only_writes_nothing() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", OP_SET, Some("604800000"))],
            )])
            .with_validate_only(true),
            None,
        )
        .await;
        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert!(api.configured().is_empty());
        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
                .unwrap()["set"],
            json!({})
        );
    }

    /// The untracked refusal reaches this API too, and it is the sentence
    /// `kafka-configs.sh` prints. Key 44 is the one a person actually meets it
    /// through.
    #[tokio::test]
    async fn an_untracked_topic_is_refused_by_name() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", OP_SET, Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        let why = message(&r.responses[0]);
        assert!(
            why.contains("orders") && why.contains("rewrites every config column"),
            "{why}"
        );
        assert!(api.configured().is_empty());
    }

    /// The broker and the unknown-topic rules are the shared ones, asserted
    /// here so that a change to either cannot land on only one of the two APIs.
    #[tokio::test]
    async fn the_shared_refusals_are_the_same_on_this_api() {
        let (f, _) = facade_and_queen(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![
                resource(
                    RESOURCE_BROKER,
                    "",
                    vec![op("num.partitions", OP_SET, Some("8"))],
                ),
                resource(
                    RESOURCE_TOPIC,
                    "nope",
                    vec![op("retention.ms", OP_SET, Some("-1"))],
                ),
                resource(8, "whatever", Vec::new()),
            ]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code(),
            "broker"
        );
        assert!(message(&r.responses[0]).contains("QUEEN_KAFKA_*"));
        assert_eq!(
            r.responses[1].error_code,
            ResponseError::UnknownTopicOrPartition.code(),
            "unknown topic"
        );
        assert_eq!(
            r.responses[2].error_code,
            ResponseError::InvalidRequest.code(),
            "BROKER_LOGGER"
        );
    }

    /// A delta that computes to what is already stored writes nothing, which
    /// keeps a re-run of the same `--alter` from re-posting a bag over a
    /// configuration somebody changed in the Queen console.
    #[tokio::test]
    async fn a_delta_that_changes_nothing_makes_no_call() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(604_800)),
            ],
        );
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![
                    op("retention.ms", OP_SET, Some("604800000")),
                    op("cleanup.policy", OP_SET, Some("delete")),
                    op("min.insync.replicas", OP_SET, Some("1")),
                ],
            )]),
            None,
        )
        .await;
        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert!(api.configured().is_empty());
    }

    /// The whole round trip in one test, in the order `kafka-configs.sh` walks
    /// it: an auto-created topic tracks an EMPTY bag, a SET lands the retention,
    /// and a DELETE puts it back to Queen's default.
    #[tokio::test]
    async fn the_round_trip_is_set_then_delete() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);

        let set = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", OP_SET, Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(set.responses[0].error_code, 0);
        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
                .unwrap()["set"],
            json!({"retentionEnabled": true, "retentionSeconds": 604_800})
        );

        let cleared = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![op("retention.ms", OP_DELETE, None)],
            )]),
            None,
        )
        .await;
        assert_eq!(cleared.responses[0].error_code, 0);
        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
                .unwrap()["set"],
            json!({})
        );
    }
}
