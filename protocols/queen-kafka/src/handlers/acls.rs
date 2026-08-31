//! DescribeAcls (29), CreateAcls (30), DeleteAcls (31), v1-v3 — the ACL family,
//! answered the way an Apache Kafka broker with no authorizer answers it.
//!
//! ## One sentence, three APIs
//!
//! Every request here is answered `SECURITY_DISABLED (54)` with the oracle's own
//! message for that API — [`NO_AUTHORIZER_ON_THE_BROKER`] for describe,
//! [`NO_AUTHORIZER`] for create and delete, because Apache Kafka really does use
//! two different sentences and the acceptance bar for this family is a byte-for-
//! byte match. Queen has no ACL model: authorization is Queen's own, over
//! a bearer token (401/403 become TOPIC_AUTHORIZATION_FAILED where they arise),
//! and there is no principal/resource/operation table for a client to read or
//! write. So there is nothing to describe, nothing to create and nothing to
//! delete, at any version, for any filter.
//!
//! ## Why the refusal is IN the protocol rather than a missing key
//!
//! Not advertising the three keys would also be a refusal — `conn::dispatch`
//! closes on an un-advertised key and every client turns that into
//! `UnsupportedVersionException`. Advertising them and answering 54 is better
//! for two measured reasons, and one reason often given for it is NOT true and
//! is not claimed here:
//!
//!   1. **It is the oracle's own answer.** `AclApis.handleDescribeAcls`,
//!      `handleCreateAcls` and `handleDeleteAcls` in Apache Kafka match on the
//!      configured authorizer and, for `None`, build the response from
//!      `Errors.SECURITY_DISABLED` (describe) or from
//!      `request.getErrorResponse(throttleMs, new SecurityDisabledException(...))`
//!      (create and delete). The differential rig's oracle runs `apache/kafka:3.9.1`
//!      with no `authorizer.class.name`, so this family is required to produce
//!      ZERO divergence — code, message and per-element shapes all identical
//!      (`compat/differential/admin_acls.go`).
//!   2. **The sentence an operator reads is the true one.** Against a broker
//!      that does not advertise the key, `kafka-acls.sh --list` prints "the
//!      broker does not support DESCRIBE_ACLS", which sends them off to upgrade
//!      a broker. Against this, it prints the same line a real Kafka with
//!      security off prints.
//!
//! The claim NOT made: that an admin UI crashes without it. Checked —
//! kafbat/kafka-ui probes `describeAcls(AclBindingFilter.ANY)` once at connect
//! and treats `SecurityDisabledException`, `InvalidRequestException` and
//! `UnsupportedVersionException` identically, hiding the ACL tab either way. The
//! gain is the oracle match and the message, not a crash avoided.
//!
//! ## The shapes, and the one that is easy to get wrong
//!
//! DescribeAcls carries the error at the TOP level with an empty `resources`.
//! CreateAcls and DeleteAcls carry it PER ELEMENT — one result per creation, one
//! result per filter — because Kafka's `getErrorResponse` maps over the request.
//! A top-level-only error on those two would decode in a Java client as "the
//! call succeeded and returned nothing", which is the opposite of the answer.
//! An empty `creations`/`filters` list therefore answers an empty result list
//! and no error at all, which is again what mapping over an empty list does.
//!
//! ## What this module does not do
//!
//! It reads no state, writes none, and makes no Queen call — the handlers do not
//! even take a [`crate::Facade`]. Nothing about the request's filter changes the
//! answer, which is exactly what a real broker does with no authorizer. In
//! cluster mode every node answers identically because there is nothing to own.
//! The SASL gate is unchanged: these dispatch after it, so an unauthenticated
//! connection still cannot reach them.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::create_acls_response::AclCreationResult;
use kafka_protocol::messages::delete_acls_response::DeleteAclsFilterResult;
use kafka_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse,
};
use kafka_protocol::protocol::StrBytes;

/// Apache Kafka's own sentence for the WRITE half of the family, byte for byte.
///
/// `AclApis.handleCreateAcls` and `handleDeleteAcls` raise
/// `new SecurityDisabledException("No Authorizer is configured.")` and hand it
/// to `request.getErrorResponse`, which puts `ApiError.message()` on the wire.
/// Copying it is deliberate: the differential scenario diffs this string against
/// the oracle, so a drift in either direction fails a test rather than reaching
/// an operator as two different explanations of the same broker.
///
/// It is also narrow, and the narrowness is the point: it says there is no ACL
/// model to query, not that nothing is authorized here. The facade
/// authenticates (SASL/PLAIN carries a Queen bearer, [`crate::sasl`]) and Queen
/// authorizes. The fuller explanation belongs in the docs, not on this wire,
/// where a longer sentence would cost the zero-divergence acceptance.
pub const NO_AUTHORIZER: &str = "No Authorizer is configured.";

/// ...and DescribeAcls' own, which is a DIFFERENT sentence in the same class.
///
/// Measured off `apache/kafka:3.9.1` rather than assumed: `handleDescribeAcls`
/// does not go through `SecurityDisabledException` at all, it builds the
/// response by hand and calls `.setErrorMessage("No Authorizer is configured on
/// the broker")` — four extra words and no full stop. The two literals really
/// are different in the oracle, so the facade carries both; collapsing them onto
/// one constant reads tidier and is wrong on the wire, which is precisely what
/// the seven `describe.*.message` keys of `compat/differential/admin_acls.go`
/// catch.
pub const NO_AUTHORIZER_ON_THE_BROKER: &str = "No Authorizer is configured on the broker";

/// One line per window when an ACL command reaches this facade, so an operator
/// who is wondering why `kafka-acls.sh` refuses can see that the request did
/// arrive and was answered rather than dropped.
static ACL_ATTEMPT: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

fn message() -> Option<StrBytes> {
    Some(StrBytes::from_static_str(NO_AUTHORIZER))
}

fn describe_message() -> Option<StrBytes> {
    Some(StrBytes::from_static_str(NO_AUTHORIZER_ON_THE_BROKER))
}

fn code() -> i16 {
    ResponseError::SecurityDisabled.code()
}

fn note(api: &'static str, elements: usize) {
    if let Some(suppressed) = ACL_ATTEMPT.tick_now() {
        tracing::info!(
            target: "kafka",
            api,
            elements,
            suppressed,
            "an ACL command reached this facade and was answered SECURITY_DISABLED: Queen has no \
             ACL model, and authorization here is Queen's own over the connection's bearer"
        );
    }
}

/// DescribeAcls: the error is top level and `resources` is empty.
///
/// The parameter is named for what happens to it. All seven filter fields —
/// `resource_type_filter`, `resource_name_filter`, `pattern_type_filter`,
/// `principal_filter`, `host_filter`, `operation`, `permission_type` — are read
/// off the wire by the decoder and then ignored, because there is nothing to
/// filter and no filter that could change the answer. That is what a real
/// broker with no authorizer does too.
pub fn describe(_filter: &DescribeAclsRequest) -> DescribeAclsResponse {
    note("DescribeAcls", 1);
    DescribeAclsResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(code())
        .with_error_message(describe_message())
        .with_resources(Vec::new())
}

/// CreateAcls: one result per creation, each carrying the code and the message.
pub fn create(req: &CreateAclsRequest) -> CreateAclsResponse {
    note("CreateAcls", req.creations.len());
    let results = req
        .creations
        .iter()
        .map(|_| {
            AclCreationResult::default()
                .with_error_code(code())
                .with_error_message(message())
        })
        .collect();
    CreateAclsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(results)
}

/// DeleteAcls: one result per filter, each with the code, the message and no
/// matching ACLs — nothing was matched because nothing exists to match.
pub fn delete(req: &DeleteAclsRequest) -> DeleteAclsResponse {
    note("DeleteAcls", req.filters.len());
    let results = req
        .filters
        .iter()
        .map(|_| {
            DeleteAclsFilterResult::default()
                .with_error_code(code())
                .with_error_message(message())
                .with_matching_acls(Vec::new())
        })
        .collect();
    DeleteAclsResponse::default()
        .with_throttle_time_ms(0)
        .with_filter_results(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::create_acls_request::AclCreation;
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

    /// The whole advertised window. Nothing in it varies — every field of all
    /// three schemas is `1-3` — so a version is a version of the ENCODING here
    /// and of nothing else, and the tests walk it to pin that.
    const WINDOW: [i16; 3] = [1, 2, 3];

    /// A creation the way `kafka-acls.sh --add --allow-principal User:alice
    /// --operation Read --topic orders` composes one.
    fn creation(resource: &'static str) -> AclCreation {
        AclCreation::default()
            .with_resource_type(2) // TOPIC
            .with_resource_name(StrBytes::from_static_str(resource))
            .with_resource_pattern_type(3) // LITERAL
            .with_principal(StrBytes::from_static_str("User:alice"))
            .with_host(StrBytes::from_static_str("*"))
            .with_operation(3) // READ
            .with_permission_type(3) // ALLOW
    }

    fn filter(resource: &'static str) -> DeleteAclsFilter {
        DeleteAclsFilter::default()
            .with_resource_type_filter(2)
            .with_resource_name_filter(Some(StrBytes::from_static_str(resource)))
            .with_pattern_type_filter(3)
            .with_principal_filter(Some(StrBytes::from_static_str("User:alice")))
            .with_host_filter(Some(StrBytes::from_static_str("*")))
            .with_operation(3)
            .with_permission_type(3)
    }

    #[test]
    fn describe_acls_is_security_disabled_at_every_version() {
        // The answer does not depend on the version, so the walk is over the
        // ENCODING: every version in the window has to round-trip the body this
        // handler builds, or a client reads a truncated frame.
        for version in WINDOW {
            let body = describe(&DescribeAclsRequest::default());
            assert_eq!(body.error_code, 54, "v{version}");
            // The describe literal, NOT the create/delete one. The oracle uses
            // two different sentences here and this assertion is the pin that
            // keeps them apart.
            assert_eq!(
                body.error_message.as_ref().map(|s| s.as_str()),
                Some(NO_AUTHORIZER_ON_THE_BROKER),
                "v{version}"
            );
            assert!(body.resources.is_empty(), "v{version}");
            assert_eq!(body.throttle_time_ms, 0, "v{version}");
            assert!(encodes(&body, version), "v{version} did not encode");
        }
    }

    #[test]
    fn create_acls_answers_one_result_per_creation() {
        for creations in [0usize, 1, 3] {
            let req = CreateAclsRequest::default()
                .with_creations((0..creations).map(|_| creation("orders")).collect());
            let body = create(&req);
            assert_eq!(body.results.len(), creations);
            for r in &body.results {
                assert_eq!(r.error_code, 54);
                assert_eq!(
                    r.error_message.as_ref().map(|s| s.as_str()),
                    Some(NO_AUTHORIZER)
                );
            }
            // The empty request is the interesting one: Kafka maps over the
            // creations, so no creations means no results AND no error, and
            // there is no top-level error field on this response to put one in
            // even if we wanted to.
            for version in WINDOW {
                assert!(encodes(&body, version), "v{version} with {creations}");
            }
        }
    }

    #[test]
    fn delete_acls_answers_one_result_per_filter() {
        for filters in [0usize, 1, 2] {
            let req = DeleteAclsRequest::default()
                .with_filters((0..filters).map(|_| filter("orders")).collect());
            let body = delete(&req);
            assert_eq!(body.filter_results.len(), filters);
            for r in &body.filter_results {
                assert_eq!(r.error_code, 54);
                assert_eq!(
                    r.error_message.as_ref().map(|s| s.as_str()),
                    Some(NO_AUTHORIZER)
                );
                assert!(
                    r.matching_acls.is_empty(),
                    "a filter matched something in a broker with no ACLs"
                );
            }
            for version in WINDOW {
                assert!(encodes(&body, version), "v{version} with {filters}");
            }
        }
    }

    /// Two filters with nothing in common, one identical answer. This is the
    /// property that makes the module stateless rather than merely empty: a
    /// client cannot learn anything about this broker by varying its filter.
    #[test]
    fn the_filter_fields_do_not_change_the_answer() {
        let wide = DescribeAclsRequest::default()
            .with_resource_type_filter(1) // ANY
            .with_pattern_type_filter(1)
            .with_operation(1)
            .with_permission_type(1);
        let narrow = DescribeAclsRequest::default()
            .with_resource_type_filter(2) // TOPIC
            .with_resource_name_filter(Some(StrBytes::from_static_str("orders")))
            .with_pattern_type_filter(3)
            .with_principal_filter(Some(StrBytes::from_static_str("User:alice")))
            .with_host_filter(Some(StrBytes::from_static_str("10.0.0.1")))
            .with_operation(3)
            .with_permission_type(3);
        assert_eq!(describe(&wide), describe(&narrow));

        // ...and the same across the two per-element APIs, where "the same"
        // means the same COUNT as well as the same content.
        let one = CreateAclsRequest::default().with_creations(vec![creation("orders")]);
        let other = CreateAclsRequest::default().with_creations(vec![creation("payments")]);
        assert_eq!(create(&one), create(&other));

        let one = DeleteAclsRequest::default().with_filters(vec![filter("orders")]);
        let other = DeleteAclsRequest::default().with_filters(vec![filter("payments")]);
        assert_eq!(delete(&one), delete(&other));
    }

    /// No Queen call is made, and the pin is stronger than the assertion: these
    /// handlers do not TAKE a facade, so there is nothing to reach Queen
    /// through. The fake is built and asserted untouched anyway, because the day
    /// somebody threads a facade in here for a "quick lookup" is the day this
    /// family stops being answerable while Queen is down.
    #[tokio::test]
    async fn no_queen_call_is_made() {
        let (_facade, api) = crate::handlers::testing::facade_and_queen(&[("orders", 2)]);
        describe(&DescribeAclsRequest::default());
        create(&CreateAclsRequest::default().with_creations(vec![creation("orders")]));
        delete(&DeleteAclsRequest::default().with_filters(vec![filter("orders")]));
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "an ACL request reached Queen's KV"
        );
        assert_eq!(api.list_count(), 0, "an ACL request read the queue catalog");
    }

    /// Every node answers the same, and there is no ownership question because
    /// there is no state to own. Built at two different nodes of the same
    /// cluster so that the day one of these grows a cluster-view read, the
    /// divergence shows up here rather than as two operators comparing outputs.
    #[tokio::test]
    async fn every_node_answers_the_same() {
        const THREE: [(i32, &str, u16); 3] = [
            (1, "kafka-1.example.com", 9092),
            (2, "kafka-2.example.com", 9092),
            (3, "kafka-3.example.com", 9092),
        ];
        let (_one, _) = crate::handlers::testing::clustered(&[("orders", 2)], &THREE, 1);
        let at_one = (
            describe(&DescribeAclsRequest::default()),
            create(&CreateAclsRequest::default().with_creations(vec![creation("orders")])),
            delete(&DeleteAclsRequest::default().with_filters(vec![filter("orders")])),
        );
        let (_two, _) = crate::handlers::testing::clustered(&[("orders", 2)], &THREE, 2);
        let at_two = (
            describe(&DescribeAclsRequest::default()),
            create(&CreateAclsRequest::default().with_creations(vec![creation("orders")])),
            delete(&DeleteAclsRequest::default().with_filters(vec![filter("orders")])),
        );
        assert_eq!(at_one, at_two);
    }

    /// The body encodes at `version` without error, which is what "the whole
    /// window is real" means for a response whose fields never vary.
    fn encodes<T: kafka_protocol::protocol::Encodable>(body: &T, version: i16) -> bool {
        let mut out = bytes::BytesMut::new();
        body.encode(&mut out, version).is_ok()
    }
}
