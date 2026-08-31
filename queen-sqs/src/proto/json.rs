//! AWS JSON 1.0: what every SDK major since late 2023 speaks.
//!
//! CONTRACT. The action name is the part of `X-Amz-Target` after the dot
//! (`AmazonSQS.ReceiveMessage`), the body IS the parameter object, and the
//! answer is a bare JSON object with no envelope of any kind. There is no
//! `…Response`/`…Result` wrapper here — that is the Query protocol's shape, and
//! emitting it would break every modern SDK.
//!
//! Errors are `{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"…"}`
//! with the HTTP status from the catalog, plus `x-amzn-query-error:
//! <QueryCode>;Sender` — a compatibility header for SDK majors that speak JSON
//! against a service model still generated from the Query protocol, and one they
//! branch on when the `__type` is not one they know.
//!
//! ## Which targets are accepted
//!
//! `AmazonSQS.<Action>` is the one every client sends. SNS has no JSON 1.0
//! protocol of its own — it is Query-only — but its target prefix is accepted
//! anyway and mapped to the bare action name, for one reason: the codec's job is
//! to name the action, and the ACTION LAYER is the only place that may decide an
//! action is not available. A refusal written here would be an `InvalidAction`
//! that no log line can distinguish from a typo. Any other dotted prefix is
//! treated the same way, on the same argument.

use serde_json::Value;

use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::proto::{
    common_headers, mirror_tags, ProtoRequest, ProtoResponse, Protocol, HEADER_QUERY_ERROR,
};

/// The prefix every SQS target carries.
pub const TARGET_PREFIX_SQS: &str = "AmazonSQS.";
/// SNS's own service name. A target naming it is accepted and reduced to the
/// action, per the module header — SNS clients speak Query and never send it.
pub const TARGET_PREFIX_SNS: &str = "AmazonSimpleNotificationService.";

/// The `__type` namespace an SQS error is reported under. SNS's half of the
/// catalog carries its own, and the ERROR is what says which
/// ([`crate::error::ErrorKind::json_namespace`]) — a codec that decided it would
/// be a codec that knows what an action is.
pub const ERROR_NAMESPACE: &str = crate::error::JSON_NAMESPACE_SQS;

/// Content type of both requests and answers.
pub const CONTENT_TYPE: &str = "application/x-amz-json-1.0";

/// Decode `X-Amz-Target` plus a JSON body.
pub fn parse(target: &str, body: &[u8]) -> SqsResult<ProtoRequest> {
    let action = action_of_target(target)?;
    let mut params = parse_params(body)?;
    mirror_tags(&mut params);
    Ok(ProtoRequest {
        protocol: Protocol::Json,
        action,
        params,
        version: None,
    })
}

/// The action a target names.
pub fn action_of_target(target: &str) -> SqsResult<String> {
    let target = target.trim();
    let name = target
        .strip_prefix(TARGET_PREFIX_SQS)
        .or_else(|| target.strip_prefix(TARGET_PREFIX_SNS))
        .unwrap_or_else(|| target.rsplit('.').next().unwrap_or(target))
        .trim();
    if name.is_empty() {
        return Err(SqsError::with(
            ErrorKind::InvalidAction,
            "The X-Amz-Target header names no action.",
        ));
    }
    Ok(name.to_string())
}

/// The body, which IS the parameter object.
///
/// An EMPTY body is an empty object and not an error: `ListQueues` with no
/// arguments is a legal call and more than one SDK sends nothing at all for it.
fn parse_params(body: &[u8]) -> SqsResult<Value> {
    let trimmed = body.trim_ascii();
    if trimmed.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    match serde_json::from_slice::<Value>(trimmed) {
        Ok(Value::Object(map)) => Ok(Value::Object(map)),
        // `null` is what a client sends for "no parameters" when its serializer
        // will not write an empty object.
        Ok(Value::Null) => Ok(Value::Object(serde_json::Map::new())),
        Ok(_) => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            "The request body must be a JSON object.",
        )),
        Err(e) => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!("The request body is not valid JSON: {e}."),
        )),
    }
}

pub fn render_ok(request_id: &str, payload: Value) -> ProtoResponse {
    ProtoResponse {
        status: 200,
        content_type: CONTENT_TYPE,
        // An action with no output shape returns `Null`; JSON 1.0 has no way to
        // write "nothing" but an empty object, and every SDK expects one.
        body: match payload {
            Value::Null => "{}".to_string(),
            payload => payload.to_string(),
        },
        headers: common_headers(request_id, None),
    }
}

pub fn render_error(error: &SqsError, request_id: &str) -> ProtoResponse {
    let body = serde_json::json!({
        "__type": format!("{}{}", error.kind.json_namespace(), error.kind.json_type()),
        "message": error.message,
    });
    let mut headers = common_headers(request_id, error.retry_after_ms);
    headers.push((
        HEADER_QUERY_ERROR.to_string(),
        format!(
            "{};{}",
            error.kind.query_code(),
            error.kind.fault().as_str()
        ),
    ));
    ProtoResponse {
        status: error.kind.http_status(),
        content_type: CONTENT_TYPE,
        body: body.to_string(),
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{header, HEADER_REQUEST_ID, HEADER_RETRY_AFTER};

    #[test]
    fn the_action_is_what_follows_the_service_name() {
        assert_eq!(
            action_of_target("AmazonSQS.SendMessageBatch").unwrap(),
            "SendMessageBatch"
        );
        // Whitespace some HTTP stacks leave behind.
        assert_eq!(
            action_of_target("  AmazonSQS.ListQueues ").unwrap(),
            "ListQueues"
        );
        // SNS's prefix is reduced, not refused: the action layer decides.
        assert_eq!(
            action_of_target("AmazonSimpleNotificationService.Publish").unwrap(),
            "Publish"
        );
        // An unknown prefix, and a bare name.
        assert_eq!(
            action_of_target("SomeOtherService.Publish").unwrap(),
            "Publish"
        );
        assert_eq!(action_of_target("ListQueues").unwrap(), "ListQueues");
    }

    #[test]
    fn a_target_that_names_nothing_is_invalid_action() {
        for target in ["", "   ", "AmazonSQS.", "AmazonSQS. "] {
            let e = action_of_target(target).expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidAction, "{target:?}");
        }
    }

    #[test]
    fn an_empty_body_is_an_empty_parameter_object() {
        for body in [&b""[..], b"  \n", b"{}", b"null"] {
            let request = parse("AmazonSQS.ListQueues", body).expect("decoded");
            assert_eq!(request.params, serde_json::json!({}), "{body:?}");
            assert_eq!(request.protocol, Protocol::Json);
            // The JSON protocol carries no version; the namespace question is
            // the Query renderer's alone.
            assert!(request.version.is_none());
        }
    }

    #[test]
    fn the_body_is_the_parameter_tree_verbatim_unicode_included() {
        let request = parse(
            "AmazonSQS.SendMessage",
            r#"{"QueueUrl":"http://h/000000000000/orders","MessageBody":"héllo 🐝 <b>&</b>",
                "MessageAttributes":{"trace":{"DataType":"String","StringValue":"→"}}}"#
                .as_bytes(),
        )
        .expect("decoded");
        assert_eq!(request.params["MessageBody"], "héllo 🐝 <b>&</b>");
        assert_eq!(
            request.params["MessageAttributes"]["trace"]["StringValue"],
            "→"
        );
    }

    #[test]
    fn a_body_that_is_not_an_object_is_refused() {
        for body in [&b"[1,2]"[..], b"\"hello\"", b"7", b"{oops"] {
            let e = parse("AmazonSQS.SendMessage", body).expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameterValue, "{body:?}");
        }
    }

    #[test]
    fn create_queue_tags_arrive_under_both_spellings() {
        let request = parse(
            "AmazonSQS.CreateQueue",
            br#"{"QueueName":"orders","tags":{"env":"prod"}}"#,
        )
        .expect("decoded");
        assert_eq!(request.params["tags"]["env"], "prod");
        assert_eq!(request.params["Tags"]["env"], "prod");
    }

    #[test]
    fn an_answer_is_a_bare_object_with_no_envelope() {
        let answer = render_ok("rid-7", serde_json::json!({"QueueUrl": "http://h/q"}));
        assert_eq!(answer.status, 200);
        assert_eq!(answer.content_type, CONTENT_TYPE);
        assert_eq!(answer.body, r#"{"QueueUrl":"http://h/q"}"#);
        assert!(!answer.body.contains("Response"));
        assert_eq!(header(&answer.headers, HEADER_REQUEST_ID), Some("rid-7"));
    }

    /// `DeleteMessage` has no output shape at all. JSON 1.0 cannot write
    /// "nothing", and an SDK handed an empty BODY raises a parse error.
    #[test]
    fn an_action_with_no_output_answers_an_empty_object() {
        assert_eq!(render_ok("rid", serde_json::Value::Null).body, "{}");
    }

    /// The golden. Every byte here is read by a client: the `__type` decides
    /// which exception is raised, the header decides what an older SDK major
    /// does, and the status decides whether either is consulted at all.
    #[test]
    fn the_error_golden() {
        let answer = render_error(&SqsError::new(ErrorKind::QueueDoesNotExist), "rid-9");
        assert_eq!(answer.status, 400);
        assert_eq!(answer.content_type, CONTENT_TYPE);
        assert_eq!(
            answer.body,
            r#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}"#
        );
        assert_eq!(
            header(&answer.headers, HEADER_QUERY_ERROR),
            Some("AWS.SimpleQueueService.NonExistentQueue;Sender")
        );
        assert_eq!(header(&answer.headers, HEADER_REQUEST_ID), Some("rid-9"));
    }

    /// An SNS error rendered over this codec carries SNS's namespace and SNS's
    /// status. It is reachable: `AmazonSimpleNotificationService.<Action>` is
    /// accepted here, so a client that speaks JSON to the SNS action set gets a
    /// `__type` its own model knows.
    #[test]
    fn an_sns_error_is_reported_under_snss_own_namespace() {
        let answer = render_error(&SqsError::new(ErrorKind::NotFound), "rid");
        assert_eq!(answer.status, 404);
        assert!(
            answer.body.contains("com.amazonaws.sns#NotFound"),
            "{}",
            answer.body
        );
        assert!(!answer.body.contains(ERROR_NAMESPACE), "{}", answer.body);
    }

    #[test]
    fn a_receiver_fault_says_so_in_the_compatibility_header() {
        let answer = render_error(&SqsError::new(ErrorKind::ServiceUnavailable), "rid");
        assert_eq!(answer.status, 503);
        assert_eq!(
            header(&answer.headers, HEADER_QUERY_ERROR),
            Some("ServiceUnavailable;Receiver")
        );
    }

    #[test]
    fn a_throttle_carries_the_backoff_a_client_must_honour() {
        let error = SqsError::new(ErrorKind::RequestThrottled).retry_after(Some(1200));
        let answer = render_error(&error, "rid");
        assert_eq!(answer.status, 403);
        assert_eq!(header(&answer.headers, HEADER_RETRY_AFTER), Some("2"));
        assert!(answer.body.contains("com.amazonaws.sqs#RequestThrottled"));
    }

    /// A message is client-influenced text and it lands in a JSON string: the
    /// serializer escapes it, and this is the test that says so out loud.
    #[test]
    fn a_message_is_escaped_by_the_serializer() {
        let error = SqsError::with(
            ErrorKind::InvalidParameterValue,
            "bad \"quoted\" \\ value\n",
        );
        let answer = render_error(&error, "rid");
        assert!(answer.body.contains(r#"bad \"quoted\" \\ value\n"#));
        // ...and it still parses back to exactly what went in.
        let back: serde_json::Value = serde_json::from_str(&answer.body).expect("valid JSON");
        assert_eq!(back["message"], "bad \"quoted\" \\ value\n");
    }
}
