//! The Query protocol: a form body in, XML out.
//!
//! CONTRACT. `parse` turns `application/x-www-form-urlencoded` pairs into the
//! SAME parameter tree [`super::json`] produces, so no action implementation
//! ever learns which protocol it is serving. This is the half of the facade
//! that keeps async-aws — and therefore all of Symfony Messenger — and every SNS
//! client working, because SNS never moved off it.
//!
//! THE FLATTENING, which is the entire difficulty. SQS's form encoding writes a
//! structure as indexed dotted keys, 1-BASED:
//!
//! ```text
//!   Action=SendMessageBatch
//!   SendMessageBatchRequestEntry.1.Id=a
//!   SendMessageBatchRequestEntry.1.MessageBody=hello
//!   SendMessageBatchRequestEntry.1.MessageAttribute.1.Name=trace
//!   SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType=String
//!   SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue=abc
//!   Attribute.1.Name=VisibilityTimeout        (SQS)
//!   Attributes.entry.1.key=VisibilityTimeout  (some older clients)
//!   Entries.member.2.Id=…                     (SNS, and some SQS clients)
//! ```
//!
//! So the rebuilder must treat a purely numeric segment as a LIST INDEX, and the
//! literal segments `member` and `entry` as transparent — a client that writes
//! `.member.` and one that does not are describing the same list, and an action
//! that had to know which is which would be one action per SDK.
//!
//! Indices are 1-based and MAY BE SPARSE or out of order in a legal request; the
//! rebuilt list is dense and in index order. What must never happen is a silent
//! reindexing that moves an entry: `BatchResultErrorEntry` reports failures by
//! the client's own `Id`, so an entry read into the wrong slot is a failure
//! reported against the wrong message.
//!
//! ## The lift onto the canonical shape
//!
//! Rebuilding the tree is not enough: `Attribute.1.Name/.Value` describes a LIST
//! OF PAIRS and the canonical (JSON) shape is a MAP, `SendMessageBatchRequestEntry`
//! is spelled `Entries` there, and `AttributeName.1` is `AttributeNames`. Those
//! are per-parameter facts, not syntax, so they are a TABLE ([`LIFTS`]) and not
//! a rule — a rule that guessed would eventually guess a batch entry into a map.
//! The table is applied at every depth, because a batch entry carries message
//! attributes of its own.
//!
//! ONE CONSEQUENCE THE ACTION LAYER MUST KNOW: a form carries only strings, so
//! `DelaySeconds` arrives as `"5"` here and as `5` from a JSON client. Every
//! numeric parameter must be read as string-or-number. That is the single place
//! the two protocols do not converge, and it converges no further without
//! knowing which parameters are numbers — which is action knowledge.
//!
//! ## The answer
//!
//! The XML shape of a result is also per-member fact: a list is FLATTENED in SQS
//! (`<QueueUrl>` repeated, never `<QueueUrls>`) and `<member>`-wrapped in SNS, a
//! map is `<Attribute><Name/><Value/></Attribute>` in SQS and
//! `<entry><key/><value/></entry>` in SNS, and the element a batch result
//! repeats is named after the ACTION. [`list_element`] and [`map_member`] carry
//! those names, and they carry only the ones that have been read off a real
//! answer — a member with no row renders under its own name, which is visible
//! and wrong rather than plausible and wrong.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::proto::xml::{self, Xml};
use crate::proto::{common_headers, mirror_tags, no_action, ProtoRequest, ProtoResponse, Protocol};

/// Content type the Query protocol arrives with.
pub const CONTENT_TYPE: &str = "application/x-www-form-urlencoded";
/// What its answers are.
pub const RESPONSE_CONTENT_TYPE: &str = "text/xml";

/// The SQS API version its clients send.
pub const VERSION_SQS: &str = "2012-11-05";
/// SNS's, which is a different XML namespace and a different action set.
pub const VERSION_SNS: &str = "2010-03-31";

/// The segments that are structural noise rather than field names.
pub const TRANSPARENT_SEGMENTS: [&str; 2] = ["member", "entry"];

/// How many dotted segments one parameter key may carry.
///
/// THIS IS A STACK BOUND, not a style rule. [`insert`] descends one frame per
/// segment and [`Node::into_value`] and [`lift`] walk the same depth again, so
/// without a cap a ten-kilobyte form body — `a.a.a.…=1`, five thousand segments,
/// which fits inside [`crate::MAX_BODY_BYTES`] with room to spare — overflows
/// the worker's stack and aborts the PROCESS. On a stateless fleet that is every
/// other tenant's requests killed by one request from any client the listener
/// accepts at all.
///
/// The deepest key SQS itself defines is
/// `SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringListValue.1`
/// — seven segments once `member`/`entry` are dropped — so this is four times
/// the deepest legal request and no conforming client can reach it.
pub const MAX_KEY_SEGMENTS: usize = 32;

/// Decode a form body (and, for a GET, its query string) into a request.
pub fn parse(body: &[u8], query: &str) -> SqsResult<ProtoRequest> {
    // The query string first and the body second, so that a parameter sent in
    // both is the BODY's: a presigned GET carries the action in the URL, and a
    // POST that also has a query string is the odd one.
    let mut pairs = Vec::new();
    collect(query.as_bytes(), &mut pairs);
    collect(body, &mut pairs);

    let mut action = None;
    let mut version = None;
    let mut parameters = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        match key.as_str() {
            "Action" => action = Some(value),
            "Version" => version = Some(value),
            // The signature's own parameters are not the action's. A presigned
            // request carries them in the same namespace as everything else.
            _ if is_signature_parameter(&key) => {}
            _ => parameters.push((key, value)),
        }
    }

    let action = action
        .map(|action| action.trim().to_string())
        .filter(|action| !action.is_empty())
        .ok_or_else(no_action)?;

    let mut params = flatten(&parameters)?;
    mirror_tags(&mut params);
    Ok(ProtoRequest {
        protocol: Protocol::Query,
        action,
        params,
        version,
    })
}

/// Rebuild the tree that a flat pair list describes, in the canonical shape.
///
/// Exposed on its own because it is where every protocol-compatibility bug will
/// live, and it is testable without a request: a pair list in, a `Value` out.
///
/// The one way it fails is a key deeper than [`MAX_KEY_SEGMENTS`], which is
/// REFUSED rather than dropped: dropping it would answer 200 to a request this
/// facade did not read, and the only sender of such a key is one probing for the
/// stack the cap exists to protect.
pub fn flatten(pairs: &[(String, String)]) -> SqsResult<Value> {
    let mut root = Node::empty();
    for (key, value) in pairs {
        let segments: Vec<&str> = key
            .split('.')
            .filter(|segment| !segment.is_empty() && !TRANSPARENT_SEGMENTS.contains(segment))
            .collect();
        if segments.is_empty() {
            continue;
        }
        if segments.len() > MAX_KEY_SEGMENTS {
            return Err(too_deep(key));
        }
        insert(&mut root, &segments, value);
    }
    Ok(lift(root.into_value()))
}

/// The refusal for a key with more structure in it than SQS has.
///
/// It names the cap and not the key: the key is client bytes and this message
/// travels back out to a log line.
fn too_deep(key: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!(
            "A request parameter name carries more than {MAX_KEY_SEGMENTS} dotted segments \
             ({} of them); no SQS or SNS parameter is nested that deeply.",
            key.split('.')
                .filter(|s| !s.is_empty() && !TRANSPARENT_SEGMENTS.contains(s))
                .count()
        ),
    )
}

// ---------------------------------------------------------------- decoding

/// The tree under construction. A list is keyed by INDEX rather than pushed,
/// because indices arrive in any order and may be sparse; the map's own
/// ordering is what makes the finished list index-ordered.
enum Node {
    Leaf(String),
    Object(BTreeMap<String, Node>),
    List(BTreeMap<u64, Node>),
}

impl Node {
    /// A node whose kind is not decided yet. It is born an object and becomes a
    /// list on the first numeric segment, which is legal precisely because it
    /// is still empty.
    fn empty() -> Node {
        Node::Object(BTreeMap::new())
    }

    fn is_empty(&self) -> bool {
        match self {
            Node::Leaf(_) => false,
            Node::Object(fields) => fields.is_empty(),
            Node::List(items) => items.is_empty(),
        }
    }

    /// STRUCTURE WINS OVER A SCALAR: `A=1` followed by `A.B=2` keeps the tree
    /// and drops the scalar, because a client that wrote both described one
    /// structure and any other resolution invents a value. A conflict between
    /// two non-empty containers drops the later write for the same reason.
    fn as_object(&mut self) -> Option<&mut BTreeMap<String, Node>> {
        if !matches!(self, Node::Object(_)) && (self.is_empty() || matches!(self, Node::Leaf(_))) {
            *self = Node::empty();
        }
        match self {
            Node::Object(fields) => Some(fields),
            _ => None,
        }
    }

    fn as_list(&mut self) -> Option<&mut BTreeMap<u64, Node>> {
        if !matches!(self, Node::List(_)) && (self.is_empty() || matches!(self, Node::Leaf(_))) {
            *self = Node::List(BTreeMap::new());
        }
        match self {
            Node::List(items) => Some(items),
            _ => None,
        }
    }

    fn into_value(self) -> Value {
        match self {
            Node::Leaf(text) => Value::String(text),
            Node::Object(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(name, node)| (name, node.into_value()))
                    .collect(),
            ),
            Node::List(items) => Value::Array(items.into_values().map(Node::into_value).collect()),
        }
    }
}

fn insert(node: &mut Node, segments: &[&str], value: &str) {
    let Some((segment, rest)) = segments.split_first() else {
        // The last segment of a key: a later value for the same key replaces an
        // earlier one, and neither replaces a subtree.
        if !node.is_empty() && !matches!(node, Node::Leaf(_)) {
            return;
        }
        *node = Node::Leaf(value.to_string());
        return;
    };
    // A segment that is entirely digits is an index. One too long for a u64 is
    // not: it is a name, and treating it as an index would be the one way this
    // function could be made to allocate on a hostile key.
    match segment.parse::<u64>() {
        Ok(index) => {
            if let Some(items) = node.as_list() {
                insert(items.entry(index).or_insert_with(Node::empty), rest, value);
            }
        }
        Err(_) => {
            if let Some(fields) = node.as_object() {
                insert(
                    fields
                        .entry((*segment).to_string())
                        .or_insert_with(Node::empty),
                    rest,
                    value,
                );
            }
        }
    }
}

/// How a Query parameter reaches its canonical name.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lift {
    /// A list of name/value pairs that is a MAP in the canonical shape.
    Map,
    /// A list, which the canonical shape spells in the plural — and which a
    /// client that sent exactly one of, unindexed, still means as a list.
    List,
}

/// The table. Left: what a Query client writes. Right: what the JSON protocol —
/// and therefore every action in this facade — reads.
const LIFTS: &[(&str, &str, Lift)] = &[
    // SQS queue attributes: `Attribute.1.Name` / `.Value`.
    ("Attribute", "Attributes", Lift::Map),
    // ...and SNS's spelling of the same thing, `Attributes.entry.1.key`.
    ("Attributes", "Attributes", Lift::Map),
    ("AttributeName", "AttributeNames", Lift::List),
    (
        "MessageSystemAttributeName",
        "MessageSystemAttributeNames",
        Lift::List,
    ),
    ("MessageAttribute", "MessageAttributes", Lift::Map),
    ("MessageAttributes", "MessageAttributes", Lift::Map),
    (
        "MessageSystemAttribute",
        "MessageSystemAttributes",
        Lift::Map,
    ),
    ("MessageAttributeName", "MessageAttributeNames", Lift::List),
    ("Tag", "Tags", Lift::Map),
    ("Tags", "Tags", Lift::Map),
    ("TagKey", "TagKeys", Lift::List),
    // The three batch actions each spell their entry list after themselves, and
    // all three are `Entries` in the canonical shape.
    ("SendMessageBatchRequestEntry", "Entries", Lift::List),
    ("DeleteMessageBatchRequestEntry", "Entries", Lift::List),
    (
        "ChangeMessageVisibilityBatchRequestEntry",
        "Entries",
        Lift::List,
    ),
    (
        "PublishBatchRequestEntries",
        "PublishBatchRequestEntries",
        Lift::List,
    ),
    // AddPermission, which is accepted and never enforced.
    ("AWSAccountId", "AWSAccountIds", Lift::List),
    ("ActionName", "Actions", Lift::List),
];

/// What a pair list's name half may be called.
const NAME_KEYS: [&str; 3] = ["Name", "Key", "key"];
/// ...and its value half.
const VALUE_KEYS: [&str; 2] = ["Value", "value"];

fn lift_of(name: &str) -> Option<(&'static str, Lift)> {
    LIFTS
        .iter()
        .find(|(query, _, _)| *query == name)
        .map(|(_, json, shape)| (*json, *shape))
}

/// SNS's `SetTopicAttributes` and `SetSubscriptionAttributes` are the only two
/// actions in either service that name an attribute as two SCALARS,
/// `AttributeName=DisplayName&AttributeValue=Events`. Lifting the name half into
/// `AttributeNames: ["DisplayName"]` there would leave the action with no
/// `AttributeName` at all and a value with nothing to attach it to.
///
/// The discriminator is the sibling: SQS writes the indexed `AttributeName.1`
/// and never a top-level scalar `AttributeValue` (its own value half arrives as
/// `Attribute.1.Value`, which lifts into `Attributes`), so a request carrying
/// BOTH as strings is the SNS pair and nothing else.
fn is_scalar_attribute_pair(fields: &Map<String, Value>) -> bool {
    fields.get("AttributeName").is_some_and(Value::is_string)
        && fields.get("AttributeValue").is_some_and(Value::is_string)
}

fn lift(value: Value) -> Value {
    match value {
        Value::Object(fields) => {
            let scalar_pair = is_scalar_attribute_pair(&fields);
            let mut out = Map::new();
            for (name, child) in fields {
                let child = lift(child);
                let (name, child) = match lift_of(&name) {
                    // The SNS scalar pair keeps the spelling it arrived in.
                    Some(_) if scalar_pair && name == "AttributeName" => (name, child),
                    Some((canonical, Lift::Map)) => (canonical.to_string(), as_map(child)),
                    Some((canonical, Lift::List)) => (canonical.to_string(), as_list(child)),
                    None => (name, child),
                };
                // A client that sent both spellings keeps the one it sent
                // first; nothing is overwritten by a lift.
                out.entry(name).or_insert(child);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(lift).collect()),
        other => other,
    }
}

/// A list of name/value objects becomes a map. Anything else is left exactly as
/// it arrived: a shape this does not recognise is a shape it must not reinvent.
fn as_map(value: Value) -> Value {
    let Value::Array(items) = value else {
        return value;
    };
    let named = items.iter().all(|item| {
        item.as_object().is_some_and(|fields| {
            NAME_KEYS
                .iter()
                .any(|key| fields.get(*key).and_then(Value::as_str).is_some())
        })
    });
    if !named {
        return Value::Array(items);
    }
    let mut map = Map::new();
    for item in items {
        let Some(fields) = item.as_object() else {
            continue;
        };
        let Some(name) = NAME_KEYS
            .iter()
            .find_map(|key| fields.get(*key))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let value = VALUE_KEYS
            .iter()
            .find_map(|key| fields.get(*key))
            .cloned()
            // A name with no value is what a client sends when it means the
            // empty string; the action layer validates, this one transcribes.
            .unwrap_or_else(|| Value::String(String::new()));
        map.insert(name.to_string(), value);
    }
    Value::Object(map)
}

/// A single unindexed value under a list parameter (`AttributeName=All`) is a
/// list of one. Clients that do this are rare and legal.
fn as_list(value: Value) -> Value {
    match value {
        Value::Array(_) => value,
        Value::Null => Value::Array(Vec::new()),
        single => Value::Array(vec![single]),
    }
}

/// How much of a body [`version_of`] reads.
///
/// It runs BEFORE the signature is checked, so it is work an unauthenticated
/// client can ask for, and a full parse of two megabytes of form per refused
/// request is work worth not doing. Every SDK writes `Action` and `Version` at
/// the head of the form — they are the envelope, not a parameter — so four
/// kilobytes is the whole of what carries the answer.
const VERSION_SCAN_BYTES: usize = 4_096;

/// The `Version` a form names, read without building the parameter tree and
/// without trusting the request.
///
/// This is what an ERROR raised before the body was decoded needs: an SNS client
/// whose signature was refused must still be answered in SNS's namespace, and by
/// then nothing else in the request says which API it addressed.
pub fn version_of(body: &[u8], query: &str) -> Option<String> {
    fn scan(bytes: &[u8]) -> Option<String> {
        // Cut at a pair boundary rather than mid-value, so a truncated window
        // cannot answer half of somebody's version string.
        let window = match bytes.len() > VERSION_SCAN_BYTES {
            false => bytes,
            true => match bytes[..VERSION_SCAN_BYTES].iter().rposition(|b| *b == b'&') {
                Some(last) => &bytes[..last],
                None => return None,
            },
        };
        form_urlencoded::parse(window)
            .find(|(key, _)| key == "Version")
            .map(|(_, value)| value.into_owned())
    }
    // The body wins over the query string, which is the precedence [`parse`]
    // applies to every other parameter.
    scan(body).or_else(|| scan(query.as_bytes()))
}

fn collect(bytes: &[u8], into: &mut Vec<(String, String)>) {
    for (key, value) in form_urlencoded::parse(bytes) {
        into.push((key.into_owned(), value.into_owned()));
    }
}

/// SigV4's own query parameters (`X-Amz-Signature` and friends) and SigV2's,
/// which some very old clients still send. `str::get` rather than a slice: a
/// key is client text and may not have a character boundary at byte six.
fn is_signature_parameter(key: &str) -> bool {
    const SIGV2: [&str; 7] = [
        "AWSAccessKeyId",
        "Signature",
        "SignatureMethod",
        "SignatureVersion",
        "SecurityToken",
        "Timestamp",
        "Expires",
    ];
    key.get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-amz-"))
        || SIGV2.contains(&key)
}

// ---------------------------------------------------------------- rendering

pub fn render_ok(
    action: &str,
    version: Option<&str>,
    request_id: &str,
    payload: Value,
) -> ProtoResponse {
    let sns = is_sns(version);
    let mut document = Xml::document(&format!("{action}Response"), namespace(sns));
    // A `null` payload is an action with NO OUTPUT SHAPE, and AWS writes no
    // `<…Result>` element for one. An action that HAS a result shape and
    // nothing to put in it sends an empty object, and gets an empty element.
    if !payload.is_null() {
        document.open(&format!("{action}Result"));
        if let Some(fields) = payload.as_object() {
            write_object(&mut document, action, sns, fields);
        }
        document.close();
    }
    document
        .open("ResponseMetadata")
        .leaf("RequestId", request_id)
        .close();
    ProtoResponse {
        status: 200,
        content_type: RESPONSE_CONTENT_TYPE,
        body: document.finish(),
        headers: common_headers(request_id, None),
    }
}

/// The `<ErrorResponse>` both services answer — and the ONE element they do not
/// share.
///
/// SQS writes an empty `<Detail/>` inside `<Error>` and SNS does not: its
/// `ErrorResponse` is `Type`, `Code`, `Message` and nothing else. Both goldens
/// are pinned ([`tests::the_error_golden`], [`tests::the_sns_error_golden`]),
/// because this is the one document every SDK's error mapper parses and an
/// element nobody writes is a divergence in it.
pub fn render_error(version: Option<&str>, error: &SqsError, request_id: &str) -> ProtoResponse {
    let sns = is_sns(version);
    let mut document = Xml::document("ErrorResponse", namespace(sns));
    document
        .open("Error")
        .leaf("Type", error.kind.fault().as_str())
        .leaf("Code", error.kind.query_code())
        .leaf("Message", &error.message);
    if !sns {
        document.empty("Detail");
    }
    document.close();
    document.leaf("RequestId", request_id);
    ProtoResponse {
        status: error.kind.http_status(),
        content_type: RESPONSE_CONTENT_TYPE,
        body: document.finish(),
        headers: common_headers(request_id, error.retry_after_ms),
    }
}

fn is_sns(version: Option<&str>) -> bool {
    version.is_some_and(|version| version.trim() == VERSION_SNS)
}

fn namespace(sns: bool) -> &'static str {
    if sns {
        xml::NS_SNS
    } else {
        xml::NS_SQS
    }
}

fn write_object(document: &mut Xml, action: &str, sns: bool, fields: &Map<String, Value>) {
    for (name, value) in fields {
        write_member(document, action, sns, name, value);
    }
}

fn write_member(document: &mut Xml, action: &str, sns: bool, name: &str, value: &Value) {
    match value {
        // An absent optional field is an absent element, never an empty one.
        Value::Null => {}
        Value::Bool(flag) => {
            document.leaf(name, if *flag { "true" } else { "false" });
        }
        Value::Number(number) => {
            document.leaf(name, &number.to_string());
        }
        Value::String(text) => {
            document.leaf(name, text);
        }
        Value::Array(items) => write_list(document, action, sns, name, items),
        Value::Object(fields) => match map_member(name) {
            Some(shape) => write_map(document, action, sns, name, shape, fields),
            None => {
                document.open(name);
                write_object(document, action, sns, fields);
                document.close();
            }
        },
    }
}

fn write_list(document: &mut Xml, action: &str, sns: bool, name: &str, items: &[Value]) {
    // The one member whose CONTENT this codec translates: a batch's per-entry
    // failure carries an error code, the action layer built it with the JSON
    // spelling (it may not look at the protocol — [`crate::actions`]'s
    // contract), and the two spellings differ for exactly the kinds a batch
    // reaches — `MessageNotInflight` is `AWS.SimpleQueueService.MessageNotInflight`
    // here. A client that branches on the code it gets from a whole-request
    // refusal must find the SAME string inside a `BatchResultErrorEntry`.
    let translated: Vec<Value>;
    let items = match name == FAILED_MEMBER {
        true => {
            translated = items.iter().map(|item| batch_error(sns, item)).collect();
            translated.as_slice()
        }
        false => items,
    };
    if sns {
        // SNS wraps every list: `<Topics><member>…</member></Topics>`.
        document.open(name);
        for item in items {
            write_member(document, action, sns, "member", item);
        }
        document.close();
        return;
    }
    // SQS flattens: the element repeats and the plural member name is never
    // written at all.
    let element = list_element(action, name);
    for item in items {
        write_member(document, action, sns, &element, item);
    }
}

fn write_map(
    document: &mut Xml,
    action: &str,
    sns: bool,
    name: &str,
    shape: (&str, &str, &str),
    fields: &Map<String, Value>,
) {
    let (entry, key, value) = shape;
    if sns {
        document.open(name);
        for (k, v) in fields {
            document.open("entry").leaf("key", k);
            write_member(document, action, sns, "value", v);
            document.close();
        }
        document.close();
        return;
    }
    for (k, v) in fields {
        document.open(entry).leaf(key, k);
        write_member(document, action, sns, value, v);
        document.close();
    }
}

/// The member every batch action reports its per-entry failures under, in both
/// protocols. [`write_list`] translates the `Code` inside it.
const FAILED_MEMBER: &str = "Failed";

/// One `BatchResultErrorEntry`, with its `Code` in THIS protocol's spelling.
///
/// A code the catalog does not know is left exactly as it came: this codec
/// transcribes, and a string it cannot place is one it must not rewrite.
fn batch_error(sns: bool, entry: &Value) -> Value {
    let Some(fields) = entry.as_object() else {
        return entry.clone();
    };
    let translated = fields
        .get("Code")
        .and_then(Value::as_str)
        .and_then(|code| crate::error::ErrorKind::of_json_type(code, sns))
        .map(|kind| kind.query_code());
    match translated {
        None => entry.clone(),
        Some(code) => {
            let mut fields = fields.clone();
            fields.insert("Code".to_string(), Value::String(code.to_string()));
            Value::Object(fields)
        }
    }
}

/// The element a SQS list repeats, by canonical member name. Only members whose
/// answer shape is known are here; see the module header on why the fallback is
/// the member's own name.
fn list_element(action: &str, member: &str) -> String {
    match member {
        // ListQueues, and ListDeadLetterSourceQueues, whose canonical member is
        // lower-cased in AWS's own model.
        "QueueUrls" | "queueUrls" => "QueueUrl".to_string(),
        "Messages" => "Message".to_string(),
        // Every batch action reports failures the same way...
        FAILED_MEMBER => "BatchResultErrorEntry".to_string(),
        // ...and its successes under an element named after itself.
        "Successful" => format!("{action}ResultEntry"),
        other => other.to_string(),
    }
}

/// The members that are MAPS rather than structures, and the three element
/// names each writes: the entry, its key and its value.
fn map_member(member: &str) -> Option<(&'static str, &'static str, &'static str)> {
    match member {
        "Attributes" => Some(("Attribute", "Name", "Value")),
        "MessageAttributes" => Some(("MessageAttribute", "Name", "Value")),
        "MessageSystemAttributes" => Some(("MessageSystemAttribute", "Name", "Value")),
        "Tags" => Some(("Tag", "Key", "Value")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::proto::{header, HEADER_REQUEST_ID, HEADER_RETRY_AFTER};
    use serde_json::json;

    fn pairs(flat: &[(&str, &str)]) -> Vec<(String, String)> {
        flat.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// [`flatten`] for the cases whose keys are all inside [`MAX_KEY_SEGMENTS`],
    /// which is every case but the one that tests the cap.
    fn tree(flat: &[(String, String)]) -> Value {
        flatten(flat).expect("every key here is inside the depth cap")
    }

    /// THE round trip, and the reason this module exists: the same request in
    /// both protocols reaches an action as the same tree. A batch with nested
    /// message attributes is the hardest shape SQS has.
    ///
    /// The one documented difference is visible here too — `DelaySeconds` is
    /// `"5"` and not `5`, because a form carries only strings.
    #[test]
    fn a_send_message_batch_decodes_into_the_json_protocols_own_shape() {
        let from_query = tree(&pairs(&[
            ("QueueUrl", "http://h/000000000000/orders"),
            ("SendMessageBatchRequestEntry.1.Id", "a"),
            ("SendMessageBatchRequestEntry.1.MessageBody", "hello"),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name",
                "trace",
            ),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType",
                "String",
            ),
            (
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue",
                "abc",
            ),
            ("SendMessageBatchRequestEntry.2.Id", "b"),
            ("SendMessageBatchRequestEntry.2.MessageBody", "world"),
            ("SendMessageBatchRequestEntry.2.DelaySeconds", "5"),
        ]));
        let from_json: Value = serde_json::from_str(
            r#"{"QueueUrl":"http://h/000000000000/orders",
                "Entries":[
                  {"Id":"a","MessageBody":"hello",
                   "MessageAttributes":{"trace":{"DataType":"String","StringValue":"abc"}}},
                  {"Id":"b","MessageBody":"world","DelaySeconds":"5"}]}"#,
        )
        .expect("valid JSON");
        assert_eq!(from_query, from_json);
    }

    #[test]
    fn queue_attributes_become_the_canonical_map() {
        let params = tree(&pairs(&[
            ("QueueUrl", "http://h/q"),
            ("Attribute.1.Name", "VisibilityTimeout"),
            ("Attribute.1.Value", "30"),
            ("Attribute.2.Name", "RedrivePolicy"),
            (
                "Attribute.2.Value",
                r#"{"maxReceiveCount":5,"deadLetterTargetArn":"arn:aws:sqs:queen-1:0:dlq"}"#,
            ),
        ]));
        assert_eq!(
            params,
            json!({
                "QueueUrl": "http://h/q",
                "Attributes": {
                    "VisibilityTimeout": "30",
                    "RedrivePolicy": r#"{"maxReceiveCount":5,"deadLetterTargetArn":"arn:aws:sqs:queen-1:0:dlq"}"#,
                }
            })
        );
    }

    #[test]
    fn attribute_names_become_a_list_indexed_or_not() {
        assert_eq!(
            tree(&pairs(&[
                ("AttributeName.1", "All"),
                ("MessageAttributeName.1", "trace"),
                ("MessageAttributeName.2", "tenant"),
            ])),
            json!({"AttributeNames": ["All"], "MessageAttributeNames": ["trace", "tenant"]})
        );
        // A client that sends exactly one, unindexed, still means a list.
        assert_eq!(
            tree(&pairs(&[("AttributeName", "All")])),
            json!({"AttributeNames": ["All"]})
        );
    }

    /// `member` and `entry` are the same list two clients spell differently.
    #[test]
    fn member_and_entry_are_transparent() {
        let sns = tree(&pairs(&[
            ("TopicArn", "arn:aws:sns:queen-1:0:events"),
            ("Attributes.entry.1.key", "DisplayName"),
            ("Attributes.entry.1.value", "Events"),
            ("MessageAttributes.entry.1.Name", "trace"),
            ("MessageAttributes.entry.1.Value.DataType", "String"),
            ("MessageAttributes.entry.1.Value.StringValue", "abc"),
        ]));
        assert_eq!(
            sns,
            json!({
                "TopicArn": "arn:aws:sns:queen-1:0:events",
                "Attributes": {"DisplayName": "Events"},
                "MessageAttributes": {"trace": {"DataType": "String", "StringValue": "abc"}}
            })
        );
        // ...and the `.member.` spelling of a batch reaches the same list as
        // the bare one.
        assert_eq!(
            tree(&pairs(&[
                ("SendMessageBatchRequestEntry.member.1.Id", "a"),
                ("SendMessageBatchRequestEntry.member.2.Id", "b"),
            ])),
            json!({"Entries": [{"Id": "a"}, {"Id": "b"}]})
        );
    }

    /// Sparse and out of order, which a legal request may be. The list is dense
    /// afterwards, and in INDEX order — never in arrival order, because a
    /// failure is reported against the entry's own `Id`.
    #[test]
    fn indices_are_sorted_and_densified_never_reordered() {
        let params = tree(&pairs(&[
            ("DeleteMessageBatchRequestEntry.7.Id", "seventh"),
            ("DeleteMessageBatchRequestEntry.2.Id", "second"),
            ("DeleteMessageBatchRequestEntry.7.ReceiptHandle", "h7"),
            ("DeleteMessageBatchRequestEntry.2.ReceiptHandle", "h2"),
        ]));
        assert_eq!(
            params,
            json!({"Entries": [
                {"Id": "second", "ReceiptHandle": "h2"},
                {"Id": "seventh", "ReceiptHandle": "h7"}
            ]})
        );
    }

    #[test]
    fn percent_and_plus_and_unicode_decode() {
        let params = tree(&pairs(&[]));
        assert_eq!(params, json!({}));
        let request = parse(
            "Action=SendMessage&MessageBody=h%C3%A9llo+%F0%9F%90%9D+%3Cb%3E%26%3C%2Fb%3E\
             &QueueUrl=http%3A%2F%2Fh%2F0%2Forders"
                .as_bytes(),
            "",
        )
        .expect("decoded");
        assert_eq!(request.params["MessageBody"], "héllo 🐝 <b>&</b>");
        assert_eq!(request.params["QueueUrl"], "http://h/0/orders");
    }

    #[test]
    fn the_signature_is_not_a_parameter() {
        let request = parse(
            b"Action=ListQueues&Version=2012-11-05&X-Amz-Signature=deadbeef\
              &X-Amz-Credential=akid%2F20260830%2Fqueen-1%2Fsqs%2Faws4_request\
              &x-amz-date=20260830T101500Z&AWSAccessKeyId=akid&Signature=old\
              &QueueNamePrefix=ord",
            "",
        )
        .expect("decoded");
        assert_eq!(request.params, json!({"QueueNamePrefix": "ord"}));
    }

    #[test]
    fn the_envelope_is_not_a_parameter_and_the_body_wins() {
        let request = parse(
            b"Action=ListQueues&Version=2012-11-05",
            "Action=GetQueueUrl",
        )
        .expect("decoded");
        assert_eq!(request.action, "ListQueues");
        assert_eq!(request.version.as_deref(), Some(VERSION_SQS));
        assert_eq!(request.params, json!({}));
        assert_eq!(request.protocol, Protocol::Query);
    }

    #[test]
    fn a_form_with_no_action_is_invalid_action() {
        for body in [
            &b""[..],
            b"QueueUrl=http://h/q",
            b"Action=&QueueUrl=x",
            b"Action=%20",
        ] {
            let e = parse(body, "").expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidAction, "{body:?}");
        }
    }

    /// A malformed key cannot lose a subtree, and cannot panic. Both halves
    /// matter: this runs on a listener, on bytes a client chose.
    #[test]
    fn structure_survives_a_conflicting_key() {
        assert_eq!(
            tree(&pairs(&[("A", "1"), ("A.B", "2")])),
            json!({"A": {"B": "2"}})
        );
        assert_eq!(
            tree(&pairs(&[("A.B", "2"), ("A", "1")])),
            json!({"A": {"B": "2"}})
        );
        // A later value for the same key replaces the earlier one.
        assert_eq!(tree(&pairs(&[("A", "1"), ("A", "2")])), json!({"A": "2"}));
        // An index no u64 can hold is a NAME, not an index.
        assert_eq!(
            tree(&pairs(&[("A.99999999999999999999.B", "x")])),
            json!({"A": {"99999999999999999999": {"B": "x"}}})
        );
        // Empty segments and a key that is nothing but separators.
        assert_eq!(tree(&pairs(&[("...", "x")])), json!({}));
        assert_eq!(tree(&pairs(&[("A..B", "x")])), json!({"A": {"B": "x"}}));
    }

    /// A form body a client can post in ten kilobytes used to take the PROCESS
    /// down: one stack frame per dotted segment, three walks of the same depth,
    /// no cap. The refusal is what a request over the cap gets now, and the
    /// number below is well past anything an SDK writes and well short of the
    /// ~700 segments that overflowed a worker's stack in a debug build.
    #[test]
    fn a_key_deeper_than_sqs_has_is_refused_rather_than_recursed_into() {
        let key = vec!["a"; MAX_KEY_SEGMENTS + 1].join(".");
        let e = flatten(&pairs(&[(key.as_str(), "1")])).expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);
        assert!(
            e.message.contains(&MAX_KEY_SEGMENTS.to_string()),
            "{}",
            e.message
        );
        // The whole request is refused, not just the key: the answer a client
        // gets must not look like one this facade acted on.
        let body = format!("Action=ListQueues&Version=2012-11-05&{key}=1");
        assert_eq!(
            parse(body.as_bytes(), "").expect_err("refused").kind,
            ErrorKind::InvalidParameterValue
        );
        // Exactly at the cap is a legal request, and `member`/`entry` do not
        // count against it because they are not structure.
        let at_cap = vec!["a"; MAX_KEY_SEGMENTS].join(".member.");
        assert!(flatten(&pairs(&[(at_cap.as_str(), "1")])).is_ok());
    }

    /// SNS's two setter actions name an attribute as two SCALARS. Lifting the
    /// name half into a list would leave `SetTopicAttributes` with no
    /// `AttributeName` at all and a value with nothing to attach it to.
    #[test]
    fn the_sns_scalar_attribute_pair_survives_the_list_lift() {
        assert_eq!(
            tree(&pairs(&[
                ("TopicArn", "arn:aws:sns:queen-1:0:events"),
                ("AttributeName", "DisplayName"),
                ("AttributeValue", "Events"),
            ])),
            json!({
                "TopicArn": "arn:aws:sns:queen-1:0:events",
                "AttributeName": "DisplayName",
                "AttributeValue": "Events"
            })
        );
        // ...and SQS's own spellings are untouched by the exception: the indexed
        // form is a list, and so is a lone unindexed name with no value beside
        // it.
        assert_eq!(
            tree(&pairs(&[("AttributeName.1", "All")])),
            json!({"AttributeNames": ["All"]})
        );
        assert_eq!(
            tree(&pairs(&[("AttributeName", "All")])),
            json!({"AttributeNames": ["All"]})
        );
    }

    /// The version an ERROR is rendered against, read off bytes nobody has
    /// verified yet — so it is bounded, and it cannot answer half a value.
    #[test]
    fn the_version_is_readable_without_decoding_the_request() {
        assert_eq!(
            version_of(b"Action=ListTopics&Version=2010-03-31&TopicArn=x", ""),
            Some(VERSION_SNS.to_string())
        );
        // A presigned GET carries it in the query string and posts nothing.
        assert_eq!(
            version_of(b"", "Action=ListQueues&Version=2012-11-05"),
            Some(VERSION_SQS.to_string())
        );
        // The body wins, which is `parse`'s own precedence.
        assert_eq!(
            version_of(b"Version=2010-03-31", "Version=2012-11-05"),
            Some(VERSION_SNS.to_string())
        );
        assert_eq!(version_of(b"Action=ListQueues", ""), None);
        // Past the scan window the answer is None rather than a truncated
        // string: the pair the window cuts through is never half-read.
        let padded = format!(
            "{}&Version=2010-03-31",
            "Filler=".to_string() + &"x".repeat(VERSION_SCAN_BYTES)
        );
        assert_eq!(version_of(padded.as_bytes(), ""), None);
    }

    #[test]
    fn a_send_message_answer_is_the_aws_envelope() {
        let answer = render_ok(
            "SendMessage",
            Some(VERSION_SQS),
            "rid-1",
            json!({"MessageId": "b7f6", "MD5OfMessageBody": "9a0b"}),
        );
        assert_eq!(answer.status, 200);
        assert_eq!(answer.content_type, RESPONSE_CONTENT_TYPE);
        assert_eq!(header(&answer.headers, HEADER_REQUEST_ID), Some("rid-1"));
        assert_eq!(
            answer.body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <SendMessageResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\n\
             \x20\x20<SendMessageResult>\n\
             \x20\x20\x20\x20<MD5OfMessageBody>9a0b</MD5OfMessageBody>\n\
             \x20\x20\x20\x20<MessageId>b7f6</MessageId>\n\
             \x20\x20</SendMessageResult>\n\
             \x20\x20<ResponseMetadata>\n\
             \x20\x20\x20\x20<RequestId>rid-1</RequestId>\n\
             \x20\x20</ResponseMetadata>\n\
             </SendMessageResponse>\n"
        );
    }

    /// The answer shape a client actually parses messages out of: a flattened
    /// list, two kinds of map inside each item, and a body that is client text.
    #[test]
    fn a_receive_answer_flattens_its_list_and_writes_both_attribute_maps() {
        let answer = render_ok(
            "ReceiveMessage",
            None,
            "rid",
            json!({"Messages": [{
                "MessageId": "m-1",
                "ReceiptHandle": "h-1",
                "Body": "a & b <c>",
                "Attributes": {"ApproximateReceiveCount": "2"},
                "MessageAttributes": {"trace": {"DataType": "String", "StringValue": "→"}}
            }]}),
        );
        let body = answer.body;
        assert!(body.contains("<Message>"), "{body}");
        assert!(!body.contains("<Messages>"), "the plural is never written");
        assert!(body.contains("<Body>a &amp; b &lt;c&gt;</Body>"), "{body}");
        assert!(
            body.contains(
                "<Attribute>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<Name>ApproximateReceiveCount</Name>\n\
             \x20\x20\x20\x20\x20\x20\x20\x20<Value>2</Value>"
            ),
            "{body}"
        );
        assert!(body.contains("<MessageAttribute>"), "{body}");
        assert!(body.contains("<Name>trace</Name>"), "{body}");
        assert!(body.contains("<DataType>String</DataType>"), "{body}");
        assert!(body.contains("<StringValue>→</StringValue>"), "{body}");
    }

    /// An empty poll is the commonest answer this facade gives. The result
    /// element is present and empty; no `<Message>` is written.
    #[test]
    fn an_empty_list_writes_an_empty_result() {
        let answer = render_ok("ReceiveMessage", None, "rid", json!({"Messages": []}));
        assert!(answer.body.contains("<ReceiveMessageResult>"));
        assert!(!answer.body.contains("<Message>"));
        // ...and an action with NO result shape writes no result element at all.
        let answer = render_ok("DeleteMessage", None, "rid", Value::Null);
        assert!(
            !answer.body.contains("DeleteMessageResult"),
            "{}",
            answer.body
        );
        assert!(answer.body.contains("<RequestId>rid</RequestId>"));
    }

    #[test]
    fn batch_results_take_the_actions_own_element_name() {
        let answer = render_ok(
            "SendMessageBatch",
            None,
            "rid",
            json!({
                "Successful": [{"Id": "a", "MessageId": "m-1", "MD5OfMessageBody": "9a0b"}],
                "Failed": [{"Id": "b", "SenderFault": true, "Code": "InvalidParameterValue",
                            "Message": "MessageBody is empty"}]
            }),
        );
        let body = answer.body;
        assert!(body.contains("<SendMessageBatchResultEntry>"), "{body}");
        assert!(body.contains("<BatchResultErrorEntry>"), "{body}");
        assert!(body.contains("<SenderFault>true</SenderFault>"), "{body}");
        assert!(!body.contains("<Successful>"));
        assert!(!body.contains("<Failed>"));
        // The same payload under another batch action names its own element.
        let other = render_ok(
            "DeleteMessageBatch",
            None,
            "rid",
            json!({"Successful": [{"Id": "a"}]}),
        );
        assert!(other.body.contains("<DeleteMessageBatchResultEntry>"));
    }

    #[test]
    fn a_list_of_scalars_repeats_the_singular_element() {
        let answer = render_ok(
            "ListQueues",
            None,
            "rid",
            json!({"QueueUrls": ["http://h/0/a", "http://h/0/b"]}),
        );
        assert!(answer.body.contains("<QueueUrl>http://h/0/a</QueueUrl>"));
        assert!(answer.body.contains("<QueueUrl>http://h/0/b</QueueUrl>"));
        assert!(!answer.body.contains("QueueUrls"));
    }

    /// SNS is the other namespace AND the other list shape. Both are driven by
    /// the request's version and by nothing else.
    #[test]
    fn sns_wraps_its_lists_and_writes_entry_maps() {
        let answer = render_ok(
            "ListTopics",
            Some(VERSION_SNS),
            "rid",
            json!({"Topics": [{"TopicArn": "arn:aws:sns:queen-1:0:events"}]}),
        );
        let body = answer.body;
        assert!(body.contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""));
        assert!(body.contains("<Topics>"), "{body}");
        assert!(body.contains("<member>"), "{body}");

        let attributes = render_ok(
            "GetTopicAttributes",
            Some(VERSION_SNS),
            "rid",
            json!({"Attributes": {"DisplayName": "Events"}}),
        );
        assert!(attributes.body.contains("<entry>"), "{}", attributes.body);
        assert!(attributes.body.contains("<key>DisplayName</key>"));
        assert!(attributes.body.contains("<value>Events</value>"));
        // The SQS spelling never appears in an SNS answer.
        assert!(!attributes.body.contains("<Attribute>"));
    }

    /// The golden, byte for byte: this is the document an SDK's XML error
    /// parser walks, and `<Detail/>` is in it because AWS writes one.
    #[test]
    fn the_error_golden() {
        let answer = render_error(
            Some(VERSION_SQS),
            &SqsError::new(ErrorKind::QueueDoesNotExist),
            "rid-9",
        );
        assert_eq!(answer.status, 400);
        assert_eq!(answer.content_type, RESPONSE_CONTENT_TYPE);
        assert_eq!(
            answer.body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\n\
             \x20\x20<Error>\n\
             \x20\x20\x20\x20<Type>Sender</Type>\n\
             \x20\x20\x20\x20<Code>AWS.SimpleQueueService.NonExistentQueue</Code>\n\
             \x20\x20\x20\x20<Message>The specified queue does not exist.</Message>\n\
             \x20\x20\x20\x20<Detail/>\n\
             \x20\x20</Error>\n\
             \x20\x20<RequestId>rid-9</RequestId>\n\
             </ErrorResponse>\n"
        );
    }

    /// SNS's own error document, byte for byte — and it has NO `<Detail/>`.
    /// SQS writes one and SNS does not, which is the only difference between
    /// the two renderings besides the namespace.
    #[test]
    fn the_sns_error_golden() {
        let answer = render_error(
            Some(VERSION_SNS),
            &SqsError::with(ErrorKind::NotFound, "Topic does not exist: events"),
            "rid-7",
        );
        assert_eq!(answer.status, 404);
        assert_eq!(
            answer.body,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">\n\
             \x20\x20<Error>\n\
             \x20\x20\x20\x20<Type>Sender</Type>\n\
             \x20\x20\x20\x20<Code>NotFound</Code>\n\
             \x20\x20\x20\x20<Message>Topic does not exist: events</Message>\n\
             \x20\x20</Error>\n\
             \x20\x20<RequestId>rid-7</RequestId>\n\
             </ErrorResponse>\n"
        );
    }

    /// A per-entry failure carries the code a WHOLE-REQUEST refusal would have
    /// in the same protocol. The action layer writes the JSON spelling (it may
    /// not look at the protocol), so the codec translates it — otherwise a
    /// Query client matching on AWS's prefixed string does not recognise its
    /// own error.
    #[test]
    fn a_batch_entry_carries_this_protocols_spelling_of_its_code() {
        let failed = json!({"Failed": [{
            "Id": "a",
            "SenderFault": true,
            "Code": ErrorKind::MessageNotInflight.json_type(),
            "Message": "gone",
        }]});
        let answer = render_ok(
            "ChangeMessageVisibilityBatch",
            Some(VERSION_SQS),
            "rid",
            failed.clone(),
        );
        assert!(
            answer
                .body
                .contains("<Code>AWS.SimpleQueueService.MessageNotInflight</Code>"),
            "{}",
            answer.body
        );
        assert!(answer.body.contains("<BatchResultErrorEntry>"));
        // ...and the SNS batch keeps SNS's own spelling of a shared code.
        let sns = render_ok(
            "PublishBatch",
            Some(VERSION_SNS),
            "rid",
            json!({"Failed": [{
                "Id": "a",
                "SenderFault": true,
                "Code": ErrorKind::SnsInvalidBatchEntryId.json_type(),
                "Message": "bad id",
            }]}),
        );
        assert!(
            sns.body.contains("<Code>InvalidBatchEntryId</Code>"),
            "{}",
            sns.body
        );
        // A code this catalog never minted is transcribed, never rewritten.
        let unknown = render_ok(
            "SendMessageBatch",
            Some(VERSION_SQS),
            "rid",
            json!({"Failed": [{"Id": "a", "Code": "SomethingElse"}]}),
        );
        assert!(unknown.body.contains("<Code>SomethingElse</Code>"));
    }

    #[test]
    fn an_error_message_is_escaped_and_a_throttle_carries_its_backoff() {
        let error = SqsError::with(ErrorKind::InvalidParameterValue, "queue <orders> & co")
            .retry_after(Some(3200));
        let answer = render_error(None, &error, "rid");
        assert!(answer
            .body
            .contains("<Message>queue &lt;orders&gt; &amp; co</Message>"));
        assert_eq!(header(&answer.headers, HEADER_RETRY_AFTER), Some("4"));
        assert_eq!(header(&answer.headers, HEADER_REQUEST_ID), Some("rid"));

        // A receiver fault says so where an SDK reads it.
        let answer = render_error(None, &SqsError::new(ErrorKind::InternalFailure), "rid");
        assert_eq!(answer.status, 500);
        assert!(answer.body.contains("<Type>Receiver</Type>"));
    }
}
