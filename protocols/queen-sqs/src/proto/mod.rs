//! Two wire protocols, one action layer.
//!
//! CONTRACT. `sniff` turns an HTTP request into a [`ProtoRequest`] — an action
//! NAME and a parameter tree — without knowing what any action is. Everything
//! below the action layer speaks `serde_json::Value` and nothing else; the codec
//! is chosen once, here, and remembered on the request so the answer goes back
//! in the protocol it came in.
//!
//! Both protocols are live on ONE listener because both are live in the field:
//!
//!   * **AWS JSON 1.0** — `Content-Type: application/x-amz-json-1.0` and
//!     `X-Amz-Target: AmazonSQS.<Action>`. Every SDK major since late 2023.
//!   * **Query/XML** — a form-encoded body with `Action=…&Version=2012-11-05`,
//!     answered in XML. Older SDK majors, async-aws (so all of Symfony
//!     Messenger) — and ALL of SNS, which never moved to JSON at all.
//!
//! Sniffing is by `X-Amz-Target` first and the form's `Action=` second, in that
//! order, because a JSON request never carries `Action=` while some clients send
//! a Query request with a JSON-ish content type. The Content-Type header is read
//! by NOTHING here: it is the header clients get wrong, and both protocols are
//! unambiguous without it.
//!
//! THE PARAMETER TREE IS THE HARD PART, and it is the Query side's: SQS's form
//! encoding flattens structure into indexed keys —
//! `MessageAttribute.1.Name=x&MessageAttribute.1.Value.StringValue=y` and
//! `Entries.member.2.Id=…` (SNS uses `.member.`, SQS mostly does not). `query.rs`
//! rebuilds the tree those keys describe, so that one action implementation
//! reads the same `Value` whichever protocol delivered it.
//!
//! ## Which shape is canonical
//!
//! **The JSON protocol's.** A Query request is decoded INTO the JSON shape, not
//! the other way round: `Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=30`
//! becomes `{"Attributes":{"VisibilityTimeout":"30"}}`, which is what a modern
//! SDK posts verbatim. The single exception in AWS's own model is `tags` on
//! `CreateQueue` (lower-cased there and nowhere else); rather than choose a
//! spelling and make every action remember which, [`mirror_tags`] puts BOTH
//! spellings on the params object, from both protocols.
//!
//! ## Namespaces, and why two of the renderers take a version
//!
//! An XML answer must carry the namespace of the API that was asked, and SNS's
//! is not SQS's. The selector is [`ProtoRequest::version`] — nothing else in the
//! request distinguishes them, and it is deliberately NOT the action name,
//! because a codec that had to recognise the SNS action set would be a codec
//! that knows what an action is. [`render_ok`] and [`render_error`] answer in
//! SQS's namespace; `_versioned` takes the request's version and is what the SNS
//! milestone calls.

pub mod json;
pub mod query;
pub mod xml;

use crate::error::{ErrorKind, SqsError, SqsResult};

/// The header that decides the protocol.
pub const HEADER_TARGET: &str = "x-amz-target";

/// The request id, echoed on every answer of either protocol and in every
/// error. It is what ties a client's report to this facade's log line.
pub const HEADER_REQUEST_ID: &str = "x-amzn-requestid";

/// The compatibility header on a JSON error: `<QueryCode>;<Fault>`.
pub const HEADER_QUERY_ERROR: &str = "x-amzn-query-error";

/// The backoff, in whole seconds, on a throttled answer.
pub const HEADER_RETRY_AFTER: &str = "retry-after";

/// How the listener hands this module the request's QUERY STRING, which a
/// presigned GET carries its parameters in and which `sniff` would otherwise
/// never see.
///
/// It is not a real header and cannot be forged into one: `:` is not a legal
/// character in an HTTP/1.1 header name, so hyper rejects any request that tries
/// to send it and the only writer is the listener itself. Absent, the body is
/// the whole request, which is what every SDK actually posts.
pub const HEADER_QUERY_STRING: &str = ":query";

/// Which codec a request arrived in — and therefore which one its answer, and
/// its errors, must use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Json,
    Query,
}

/// One request, decoded down to what the action layer reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtoRequest {
    pub protocol: Protocol,
    /// The action NAME as it arrived, unvalidated: mapping it onto
    /// [`crate::actions::Action`] is the dispatcher's job, and an unknown one is
    /// `InvalidAction` rather than a parse failure here.
    pub action: String,
    /// The parameters, as a JSON object — the SAME shape from both protocols.
    pub params: serde_json::Value,
    /// The Query protocol's `Version` (`2012-11-05` for SQS, `2010-03-31` for
    /// SNS), which is also how the XML renderer picks its namespace.
    pub version: Option<String>,
}

/// One answer, ready to write.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtoResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
    /// Everything else the answer must carry: `x-amzn-requestid` on both
    /// protocols, and `x-amzn-query-error` on a JSON error, which some SDK
    /// majors read when they speak JSON to a Query-era service model.
    pub headers: Vec<(String, String)>,
}

/// Decide the protocol and decode the request.
pub fn sniff(headers: &[(String, String)], body: &[u8]) -> SqsResult<ProtoRequest> {
    if let Some(target) = header(headers, HEADER_TARGET) {
        if !target.trim().is_empty() {
            return json::parse(target, body);
        }
    }
    query::parse(body, header(headers, HEADER_QUERY_STRING).unwrap_or(""))
}

/// The protocol a request is in, without decoding it and without failing.
///
/// The answer path needs this before `sniff` can succeed: a request refused at
/// the SIGNATURE has no decoded body, and an error still has to go back in a
/// shape its client can read. A request with no target is assumed to be Query,
/// which is also what an unparseable one is.
pub fn protocol_of(headers: &[(String, String)]) -> Protocol {
    match header(headers, HEADER_TARGET) {
        Some(target) if !target.trim().is_empty() => Protocol::Json,
        _ => Protocol::Query,
    }
}

/// The API version a request names, without decoding it and without failing.
///
/// The XML renderer picks its NAMESPACE from this, and the answer path needs it
/// before `sniff` has run: a request refused at its SIZE or at its SIGNATURE has
/// no decoded body, and an SNS client answered in SQS's namespace is reading a
/// document from a service it did not address. A JSON request has no namespace
/// at all, so it has no version either.
pub fn version_of(headers: &[(String, String)], body: &[u8]) -> Option<String> {
    match protocol_of(headers) {
        Protocol::Json => None,
        Protocol::Query => {
            query::version_of(body, header(headers, HEADER_QUERY_STRING).unwrap_or(""))
        }
    }
}

/// Render a successful answer. `action` is needed because the XML rendering
/// wraps every result in `<{Action}Response><{Action}Result>`, which the JSON
/// one does not.
///
/// A `payload` of `Value::Null` is an action with NO OUTPUT SHAPE
/// (`DeleteMessage`, `SetQueueAttributes`): JSON answers `{}` and the XML
/// answer omits the `<{Action}Result>` element entirely, which is what AWS
/// writes. An action that has a result shape and nothing to put in it — an empty
/// `ReceiveMessage` — returns an empty OBJECT, and gets the empty element.
pub fn render_ok(
    protocol: Protocol,
    action: &str,
    request_id: &str,
    payload: serde_json::Value,
) -> ProtoResponse {
    render_ok_versioned(protocol, action, None, request_id, payload)
}

/// [`render_ok`] against a named API version, which is how an SNS answer gets
/// SNS's namespace and its `<member>`-wrapped lists.
pub fn render_ok_versioned(
    protocol: Protocol,
    action: &str,
    version: Option<&str>,
    request_id: &str,
    payload: serde_json::Value,
) -> ProtoResponse {
    match protocol {
        Protocol::Json => json::render_ok(request_id, payload),
        Protocol::Query => query::render_ok(action, version, request_id, payload),
    }
}

/// Render an error in the protocol it must go back in. The two renderings of one
/// [`crate::error::ErrorKind`] use DIFFERENT code strings, which is the whole
/// reason the catalog carries both.
pub fn render_error(protocol: Protocol, error: &SqsError, request_id: &str) -> ProtoResponse {
    render_error_versioned(protocol, None, error, request_id)
}

/// [`render_error`] against a named API version.
pub fn render_error_versioned(
    protocol: Protocol,
    version: Option<&str>,
    error: &SqsError,
    request_id: &str,
) -> ProtoResponse {
    match protocol {
        Protocol::Json => json::render_error(error, request_id),
        Protocol::Query => query::render_error(version, error, request_id),
    }
}

/// Case-insensitive header lookup, first match wins.
pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// AWS's model spells `CreateQueue`'s tag map `tags` and every other tag map
/// `Tags`. Both spellings are put on the params object rather than one being
/// chosen, so that no action has to remember which of its verbs is the odd one
/// and no request loses its tags to a capital letter.
pub(crate) fn mirror_tags(params: &mut serde_json::Value) {
    let Some(object) = params.as_object_mut() else {
        return;
    };
    match (object.get("Tags").cloned(), object.get("tags").cloned()) {
        (Some(upper), None) => {
            object.insert("tags".to_string(), upper);
        }
        (None, Some(lower)) => {
            object.insert("Tags".to_string(), lower);
        }
        _ => {}
    }
}

/// The two headers every answer carries, plus the backoff when there is one.
pub(crate) fn common_headers(
    request_id: &str,
    retry_after_ms: Option<i64>,
) -> Vec<(String, String)> {
    let mut headers = vec![(HEADER_REQUEST_ID.to_string(), request_id.to_string())];
    if let Some(ms) = retry_after_ms {
        // Whole seconds, rounded UP and never zero: a `Retry-After: 0` is an
        // instruction to hammer.
        let seconds = (ms.max(1) + 999) / 1000;
        headers.push((HEADER_RETRY_AFTER.to_string(), seconds.to_string()));
    }
    headers
}

/// The refusal both codecs give a request that names no action at all.
pub(crate) fn no_action() -> SqsError {
    SqsError::with(
        ErrorKind::InvalidAction,
        "The request names no action: it carries neither an X-Amz-Target header nor an Action \
         parameter.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The precedence rule, exercised the only way it can go wrong: a request
    /// that could be read as either. A client that posts a form body AND a
    /// target is speaking JSON — the target is the deliberate signal — and the
    /// body is then read as JSON and REFUSED, which is the answer that says so.
    /// The alternative, falling back to the form, would serve a `SendMessage`
    /// to a client that asked for a `ReceiveMessage`.
    #[test]
    fn a_target_wins_over_a_form_body() {
        let e = sniff(
            &headers(&[("X-Amz-Target", "AmazonSQS.ReceiveMessage")]),
            b"Action=SendMessage&QueueUrl=http://h/q",
        )
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);

        // ...and with a body its target agrees with, the target names the
        // action whatever else is in the request.
        let request = sniff(
            &headers(&[
                ("X-Amz-Target", "AmazonSQS.ReceiveMessage"),
                ("Content-Type", query::CONTENT_TYPE),
            ]),
            br#"{"QueueUrl":"http://h/q","MaxNumberOfMessages":10}"#,
        )
        .expect("decoded");
        assert_eq!(request.protocol, Protocol::Json);
        assert_eq!(request.action, "ReceiveMessage");
        assert_eq!(request.params["MaxNumberOfMessages"], 10);
    }

    #[test]
    fn header_lookup_ignores_case_and_takes_the_first() {
        let h = headers(&[
            ("x-AMZ-target", "AmazonSQS.ListQueues"),
            ("X-Amz-Target", "AmazonSQS.X"),
        ]);
        assert_eq!(header(&h, HEADER_TARGET), Some("AmazonSQS.ListQueues"));
        assert_eq!(sniff(&h, b"").expect("decoded").action, "ListQueues");
        assert_eq!(protocol_of(&h), Protocol::Json);
    }

    #[test]
    fn no_target_is_the_query_protocol() {
        let request = sniff(
            &headers(&[("Content-Type", query::CONTENT_TYPE)]),
            b"Action=GetQueueUrl&Version=2012-11-05&QueueName=orders",
        )
        .expect("decoded");
        assert_eq!(request.protocol, Protocol::Query);
        assert_eq!(request.action, "GetQueueUrl");
        assert_eq!(request.version.as_deref(), Some("2012-11-05"));
        assert_eq!(request.params, serde_json::json!({"QueueName": "orders"}));
    }

    /// An EMPTY target is not a JSON request. Some proxies add the header with
    /// no value, and reading that as "the JSON protocol, action ''" would turn
    /// a perfectly good Query request into an InvalidAction.
    #[test]
    fn an_empty_target_falls_through_to_the_form_body() {
        let request = sniff(
            &headers(&[("X-Amz-Target", "  ")]),
            b"Action=ListQueues&Version=2012-11-05",
        )
        .expect("decoded");
        assert_eq!(request.protocol, Protocol::Query);
        assert_eq!(request.action, "ListQueues");
        assert_eq!(
            protocol_of(&headers(&[("X-Amz-Target", "  ")])),
            Protocol::Query
        );
    }

    #[test]
    fn a_request_that_names_no_action_is_invalid_action() {
        let e = sniff(&headers(&[]), b"QueueUrl=http://h/q").expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidAction);
        assert_eq!(e.kind.http_status(), 400);
    }

    /// A presigned GET puts everything in the query string and posts nothing.
    #[test]
    fn the_query_string_is_read_when_the_body_is_empty() {
        let request = sniff(
            &headers(&[(HEADER_QUERY_STRING, "Action=ListQueues&Version=2012-11-05")]),
            b"",
        )
        .expect("decoded");
        assert_eq!(request.action, "ListQueues");
        assert_eq!(request.version.as_deref(), Some("2012-11-05"));
    }

    /// One error, both renderings, side by side: different codes, different
    /// bodies, ONE status.
    #[test]
    fn an_error_goes_back_in_the_protocol_it_came_in() {
        let error = SqsError::new(ErrorKind::QueueDoesNotExist);
        let json = render_error(Protocol::Json, &error, "rid-1");
        let query = render_error(Protocol::Query, &error, "rid-1");

        assert_eq!(json.status, 400);
        assert_eq!(query.status, 400);
        assert_eq!(json.content_type, json::CONTENT_TYPE);
        assert_eq!(query.content_type, query::RESPONSE_CONTENT_TYPE);
        assert!(json.body.contains("com.amazonaws.sqs#QueueDoesNotExist"));
        assert!(!json.body.contains("AWS.SimpleQueueService"));
        assert!(query
            .body
            .contains("AWS.SimpleQueueService.NonExistentQueue"));
        assert!(!query.body.contains("com.amazonaws.sqs#"));
        for answer in [&json, &query] {
            assert_eq!(
                header(&answer.headers, HEADER_REQUEST_ID),
                Some("rid-1"),
                "every answer is traceable"
            );
        }
    }

    #[test]
    fn a_backoff_becomes_whole_seconds_rounded_up() {
        assert_eq!(common_headers("r", None).len(), 1);
        let h = common_headers("r", Some(2500));
        assert_eq!(header(&h, HEADER_RETRY_AFTER), Some("3"));
        // Never zero, however small the backoff.
        let h = common_headers("r", Some(1));
        assert_eq!(header(&h, HEADER_RETRY_AFTER), Some("1"));
    }

    #[test]
    fn tags_carry_both_of_the_spellings_aws_uses() {
        let mut lower = serde_json::json!({"QueueName": "q", "tags": {"env": "prod"}});
        mirror_tags(&mut lower);
        assert_eq!(lower["Tags"], serde_json::json!({"env": "prod"}));
        assert_eq!(lower["tags"], serde_json::json!({"env": "prod"}));

        let mut upper = serde_json::json!({"Tags": {"env": "prod"}});
        mirror_tags(&mut upper);
        assert_eq!(upper["tags"], serde_json::json!({"env": "prod"}));

        // A request with neither grows neither.
        let mut none = serde_json::json!({"QueueName": "q"});
        mirror_tags(&mut none);
        assert_eq!(none, serde_json::json!({"QueueName": "q"}));
    }
}
