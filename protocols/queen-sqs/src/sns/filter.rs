//! `FilterPolicy`: the grammar SNS matches a publish against, and the one place
//! a subscription decides it wants a message.
//!
//! CONTRACT. Two functions and one document type:
//!
//!   * [`validate`] runs at `Subscribe` and `SetSubscriptionAttributes` and
//!     refuses a policy this engine cannot evaluate — `InvalidParameter`, named.
//!     That order is the whole safety property: a policy accepted here and not
//!     understood later is a subscription that silently stops receiving, which
//!     is the one failure a client cannot see from the outside.
//!   * [`matches`] runs at publish, over a `serde_json::Value` DOCUMENT. In
//!     `MessageBody` scope that document is the message parsed as JSON; in
//!     `MessageAttributes` scope it is [`document_of_attributes`]'s rendering of
//!     the publish's own attributes. ONE matcher for both scopes, because the
//!     grammar is one grammar — the scopes differ only in what the document is.
//!
//! ## What is implemented, and what is refused
//!
//! Implemented: an exact string, a number, an OR-list of either, `exists`,
//! `prefix`, `suffix`, `equals-ignore-case`, `anything-but` (a string, a list, or
//! a nested `prefix`/`suffix`/`equals-ignore-case` whose own argument is a string
//! or a list of them), `numeric` with `=`, `<`,
//! `<=`, `>`, `>=` and the two-bound range form, `$or` at the top level, nested
//! objects for the body scope, and a document value that is an ARRAY (a policy
//! leaf matches if ANY element matches, which is what makes `String.Array`
//! attributes and JSON list fields work).
//!
//! Refused at write time, each by name: `cidr` — which needs IP semantics this
//! facade does not have — and `$or` below the top level, which is where AWS's
//! own documentation stops. Everything else unknown is refused as unknown.
//! Refusing is the honest half of the contract above: a client is told at
//! `Subscribe`, in a message naming the operator, instead of finding out through
//! a queue that stays empty.
//!
//! ## Three rules that look like details and are not
//!
//! **A negated rule does not distribute over an array.** A document value that
//! is an array matches a positive rule when ANY element does, and an
//! `anything-but` when NO element is excluded. Reading the negation the first
//! way would deliver `["internal","order"]` to a subscription that asked for
//! anything but `internal` ([`match_rule`]).
//!
//! **An absent attribute matches nothing but `{"exists": false}`.** That is
//! AWS's rule and it is what makes a filter policy a whitelist: a subscription
//! that filters on `event` receives nothing that has no `event`.
//!
//! **A `Binary` message attribute is not in the document at all.** AWS ignores
//! binary attributes for filtering, and the only way to ignore one consistently
//! is for it to be absent — so `{"exists": false}` on a binary attribute is
//! TRUE here, which is the reading that keeps "ignored" from meaning two things.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::envelope::{AttributeValue, MessageAttribute};
use crate::error::SqsResult;

/// The operators this engine evaluates.
const EXISTS: &str = "exists";
const PREFIX: &str = "prefix";
const SUFFIX: &str = "suffix";
const EQUALS_IGNORE_CASE: &str = "equals-ignore-case";
const ANYTHING_BUT: &str = "anything-but";
const NUMERIC: &str = "numeric";
const OR: &str = "$or";
/// The one operator AWS has that this engine refuses rather than approximates.
const CIDR: &str = "cidr";

/// The comparisons `numeric` takes.
const NUMERIC_OPS: [&str; 5] = ["=", "<", "<=", ">", ">="];

/// How deep a policy may nest.
///
/// A STACK BOUND on client input, the same argument as
/// [`crate::proto::query::MAX_KEY_SEGMENTS`]: this walks a document a client
/// posted, and it walks it twice (validate, then match). AWS's own documented
/// ceiling is five levels, so this is past anything a conforming policy has and
/// short of anything that could reach the worker's stack.
pub const MAX_DEPTH: usize = 8;

/// The longest fragment of a client's own policy a refusal quotes back. A
/// key or an operator is short in every legal policy; the cap is for the one
/// that is not, because this message becomes a log line.
const MAX_QUOTED: usize = 64;

// ------------------------------------------------------------------ matching

/// Whether `policy` accepts `document`.
///
/// `None` for the document is a message whose BODY is not JSON, which
/// `MessageBody` scope answers "no match" for — AWS's own behaviour, and the
/// reason this takes an `Option` rather than making the caller invent an empty
/// object: an empty object would MATCH a policy whose only rule is
/// `{"exists": false}`, and a message that is not JSON has no attribute that is
/// absent, it has no attributes at all to reason about.
pub fn matches(policy: &Value, document: Option<&Value>) -> bool {
    let (Some(policy), Some(document)) = (policy.as_object(), document) else {
        return false;
    };
    match_object(policy, document, true, 1)
}

/// The document a `MessageAttributes`-scope policy is matched against.
///
/// One JSON object, so the matcher is the body scope's:
///
///   * `String` (and any custom `String.*` label) → the string, EXCEPT
///     `String.Array`, whose value is a JSON array in SNS's own encoding and is
///     served as that array so a policy leaf can match any element;
///   * `Number` → a JSON number, so `numeric` rules read it as one. A value that
///     does not parse stays a string, which then matches string rules and no
///     numeric one — better than dropping an attribute the sender set;
///   * `Binary` → absent. See the module header.
pub fn document_of_attributes(attributes: &BTreeMap<String, MessageAttribute>) -> Value {
    let mut out = Map::new();
    for (name, attribute) in attributes {
        let AttributeValue::String(text) = &attribute.value else {
            continue;
        };
        let value = if attribute.data_type == "String.Array" {
            serde_json::from_str::<Value>(text)
                .ok()
                .filter(Value::is_array)
                .unwrap_or_else(|| Value::String(text.clone()))
        } else if attribute.data_type.starts_with("Number") {
            text.trim()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map_or_else(|| Value::String(text.clone()), Value::Number)
        } else {
            Value::String(text.clone())
        };
        out.insert(name.clone(), value);
    }
    Value::Object(out)
}

/// Every key of a policy object must be satisfied — an AND — and `$or` is one
/// more conjunct whose own branches are an OR.
fn match_object(policy: &Map<String, Value>, document: &Value, top: bool, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return false;
    }
    for (key, rule) in policy {
        let ok = match (key.as_str(), rule) {
            (OR, Value::Array(branches)) if top => branches.iter().any(|branch| {
                branch
                    .as_object()
                    .is_some_and(|branch| match_object(branch, document, false, depth + 1))
            }),
            // A nested policy: the document must have an object (or an array of
            // them) under this key.
            (_, Value::Object(nested)) => match field(document, key) {
                Some(value) => match_any(value, |item| {
                    item.is_object() && match_object(nested, item, false, depth + 1)
                }),
                None => false,
            },
            (_, Value::Array(rules)) => match_rules(rules, field(document, key)),
            // Neither, which [`validate`] refused — a policy stored before this
            // engine existed could still be one, and it matches nothing.
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

/// One key's value in the document, treating JSON `null` as absence: SNS has no
/// null attribute, and a body field explicitly set to null is one no rule but
/// `{"exists": false}` should accept.
fn field<'a>(document: &'a Value, key: &str) -> Option<&'a Value> {
    document.get(key).filter(|value| !value.is_null())
}

/// An array in the document matches when ANY element does; a scalar matches on
/// its own. The one function both the nested descent and the leaf comparison
/// use, so `String.Array` and a JSON list behave identically.
fn match_any(value: &Value, mut test: impl FnMut(&Value) -> bool) -> bool {
    match value {
        Value::Array(items) => items.iter().any(&mut test),
        scalar => test(scalar),
    }
}

/// The OR across one key's rule list.
fn match_rules(rules: &[Value], value: Option<&Value>) -> bool {
    rules.iter().any(|rule| match_rule(rule, value))
}

fn match_rule(rule: &Value, value: Option<&Value>) -> bool {
    // `exists` is the only rule that reads ABSENCE, so it is answered before the
    // value is unwrapped and it never looks at what the value is.
    if let Some(want) = rule.get(EXISTS).and_then(Value::as_bool) {
        return value.is_some() == want;
    }
    let Some(value) = value else {
        return false;
    };
    // A NEGATION DOES NOT DISTRIBUTE OVER THE OR AN ARRAY VALUE IS. Every other
    // rule matches an array when ANY element matches; `anything-but` is the one
    // negated rule, and reading it the same way would deliver
    // `["internal","order"]` to a subscription that asked for anything but
    // `internal` — the exact failure the module header forbids, since the
    // element it excluded IS in the value. So the exclusion is tested against
    // every element and the rule holds only when NONE of them is excluded.
    if let Some(excluded) = rule.get(ANYTHING_BUT) {
        return !match_any(value, |item| match_anything_but(excluded, item));
    }
    match_any(value, |item| match_scalar(rule, item))
}

fn match_scalar(rule: &Value, value: &Value) -> bool {
    match rule {
        Value::String(want) => value.as_str() == Some(want.as_str()),
        Value::Number(want) => match (want.as_f64(), value.as_f64()) {
            (Some(want), Some(got)) => want == got,
            _ => false,
        },
        Value::Object(fields) => match_operator(fields, value),
        _ => false,
    }
}

/// The operators, over ONE element of the document value.
///
/// `anything-but` is deliberately NOT here: it is answered in [`match_rule`],
/// which is the only place that sees the whole value and therefore the only
/// place that can fold a negation over an array correctly. A rule that reaches
/// this function naming it matches nothing, which is the safe direction for a
/// stored policy this engine cannot read the way it was written.
fn match_operator(rule: &Map<String, Value>, value: &Value) -> bool {
    for name in [PREFIX, SUFFIX, EQUALS_IGNORE_CASE] {
        if let Some(want) = rule.get(name) {
            return match_string_operator(name, want, value);
        }
    }
    if let Some(Value::Array(terms)) = rule.get(NUMERIC) {
        return match_numeric(terms, value);
    }
    false
}

/// One of the three string operators against ONE argument.
///
/// The argument must be a string here: the list form belongs to `anything-but`
/// alone ([`validate_anything_but`]) and is expanded by its caller, so a list
/// that reached a positive operator is a policy [`validate`] refused.
fn match_string_operator(name: &str, want: &Value, value: &Value) -> bool {
    let (Some(want), Some(got)) = (want.as_str(), value.as_str()) else {
        return false;
    };
    match name {
        PREFIX => got.starts_with(want),
        SUFFIX => got.ends_with(want),
        EQUALS_IGNORE_CASE => got.eq_ignore_ascii_case(want),
        _ => false,
    }
}

/// What an `anything-but` EXCLUDES, for one element of the value. The rule
/// matches when this does not — and the value is always there, because
/// [`match_rule`] unwrapped it, so an `anything-but` never matches an absent
/// attribute (AWS's rule, and the whitelist property the module header states).
fn match_anything_but(excluded: &Value, value: &Value) -> bool {
    match excluded {
        Value::Array(items) => items.iter().any(|item| match_scalar(item, value)),
        // The nested operator forms. Their argument may be a LIST, which AWS
        // takes here and nowhere else, and a list is an OR over its terms: the
        // exclusion holds when ANY term matches. See [`validate_anything_but`].
        Value::Object(fields) => match fields.iter().collect::<Vec<_>>()[..] {
            [(name, Value::Array(terms))] => terms
                .iter()
                .any(|term| match_string_operator(name, term, value)),
            _ => match_scalar(excluded, value),
        },
        scalar => match_scalar(scalar, value),
    }
}

/// `["=", 5]`, `[">", 0]`, or the two-bound range `[">", 0, "<=", 100]`. The
/// value must be a NUMBER: a numeric rule against a `String` attribute matches
/// nothing, which is AWS's rule and the reason `Number` attributes become JSON
/// numbers in [`document_of_attributes`].
fn match_numeric(terms: &[Value], value: &Value) -> bool {
    let Some(got) = value.as_f64() else {
        return false;
    };
    terms.chunks(2).all(|pair| match pair {
        [Value::String(op), bound] => match bound.as_f64() {
            Some(bound) => compare(op, got, bound),
            None => false,
        },
        _ => false,
    })
}

fn compare(op: &str, got: f64, bound: f64) -> bool {
    match op {
        "=" => got == bound,
        "<" => got < bound,
        "<=" => got <= bound,
        ">" => got > bound,
        ">=" => got >= bound,
        _ => false,
    }
}

// ---------------------------------------------------------------- validation

/// Whether a policy document is one this engine can evaluate.
///
/// SCOPE-INDEPENDENT on purpose. `SetSubscriptionAttributes` sets ONE attribute
/// per call, so a request that sets `FilterPolicy` does not carry the
/// `FilterPolicyScope` it will be matched under — and a validation that needed
/// the scope would either refuse a legal call or read a scope the same request
/// is about to change. Nested objects are therefore legal here whatever the
/// scope: under `MessageAttributes` an attribute is never an object, so a nested
/// key simply never matches — which is what AWS does with one too.
pub fn validate(policy: &Value) -> SqsResult<()> {
    let object = policy
        .as_object()
        .ok_or_else(|| super::invalid("FilterPolicy", "the policy must be a JSON object"))?;
    validate_object(object, true, 1)
}

fn validate_object(policy: &Map<String, Value>, top: bool, depth: usize) -> SqsResult<()> {
    if depth > MAX_DEPTH {
        return Err(super::invalid(
            "FilterPolicy",
            format!("the policy nests more than {MAX_DEPTH} levels deep"),
        ));
    }
    for (key, rule) in policy {
        if key == OR {
            if !top {
                return Err(super::invalid(
                    "FilterPolicy",
                    "$or is evaluated at the top level of a policy only",
                ));
            }
            let branches = rule.as_array().filter(|b| !b.is_empty()).ok_or_else(|| {
                super::invalid("FilterPolicy", "$or takes a non-empty list of policies")
            })?;
            for branch in branches {
                let branch = branch.as_object().ok_or_else(|| {
                    super::invalid("FilterPolicy", "every $or branch is a policy object")
                })?;
                validate_object(branch, false, depth + 1)?;
            }
            continue;
        }
        match rule {
            Value::Object(nested) => validate_object(nested, false, depth + 1)?,
            Value::Array(rules) if !rules.is_empty() => {
                for rule in rules {
                    validate_rule(key, rule)?;
                }
            }
            // An empty list can never match, so a client that wrote one wrote a
            // subscription that receives nothing — refused rather than stored.
            Value::Array(_) => {
                return Err(super::invalid(
                    "FilterPolicy",
                    format!(
                        "the rule list for {} is empty, which can never match",
                        quoted(key)
                    ),
                ))
            }
            _ => {
                return Err(super::invalid(
                    "FilterPolicy",
                    format!(
                        "{} must carry a list of match rules or a nested policy object",
                        quoted(key)
                    ),
                ))
            }
        }
    }
    Ok(())
}

fn validate_rule(key: &str, rule: &Value) -> SqsResult<()> {
    match rule {
        Value::String(_) | Value::Number(_) => Ok(()),
        Value::Object(fields) => validate_operator(key, fields),
        _ => Err(super::invalid(
            "FilterPolicy",
            format!(
                "a match rule for {} is a string, a number, or one of the match operators",
                quoted(key)
            ),
        )),
    }
}

fn validate_operator(key: &str, rule: &Map<String, Value>) -> SqsResult<()> {
    // ONE operator per rule object. Two would be a rule whose meaning depends on
    // which the evaluator happens to read first, and the evaluator here reads
    // them in a fixed order — which is exactly the kind of accident this refuses
    // rather than documents.
    let [(name, argument)] = rule.iter().collect::<Vec<_>>()[..] else {
        return Err(super::invalid(
            "FilterPolicy",
            format!(
                "a match rule for {} names exactly one match operator",
                quoted(key)
            ),
        ));
    };
    let ok =
        match name.as_str() {
            EXISTS => argument.is_boolean(),
            PREFIX | SUFFIX | EQUALS_IGNORE_CASE => argument.is_string(),
            ANYTHING_BUT => validate_anything_but(argument),
            NUMERIC => validate_numeric(argument),
            CIDR => return Err(super::invalid(
                "FilterPolicy",
                "cidr matching is not implemented by this endpoint; a policy that used it would \
                 have to be evaluated over IP ranges, and a subscription whose policy is not \
                 evaluated receives nothing without saying so",
            )),
            _ => {
                return Err(super::invalid(
                    "FilterPolicy",
                    format!("{} is not a match operator", quoted(name)),
                ))
            }
        };
    match ok {
        true => Ok(()),
        false => Err(super::invalid(
            "FilterPolicy",
            format!("the argument to {} is not one it takes", quoted(name)),
        )),
    }
}

fn validate_anything_but(argument: &Value) -> bool {
    match argument {
        Value::String(_) | Value::Number(_) => true,
        Value::Array(items) => {
            !items.is_empty()
                && items
                    .iter()
                    .all(|item| item.is_string() || item.is_number())
        }
        // The nested forms AWS documents, and only those.
        Value::Object(fields) => matches!(
            fields.iter().collect::<Vec<_>>()[..],
            [(name, argument)]
                if matches!(name.as_str(), PREFIX | SUFFIX | EQUALS_IGNORE_CASE)
                    && is_string_or_list(argument)
        ),
        _ => false,
    }
}

/// A nested operator's argument inside `anything-but`: one string, or the
/// non-empty LIST of strings AWS also takes there.
///
/// Accepted because a policy AWS stores must not be refused at `Subscribe` — a
/// client migrating `{"anything-but":{"prefix":["test-","dev-"]}}` cannot
/// subscribe at all otherwise — and because the list has one obvious reading,
/// the OR its terms already have everywhere else in this grammar
/// ([`match_anything_but`] applies it). The POSITIVE operators are left
/// string-only: there the OR is spelled by the rule list AWS's own grammar puts
/// around them, and widening them would invent a shape rather than accept one.
fn is_string_or_list(argument: &Value) -> bool {
    match argument {
        Value::String(_) => true,
        Value::Array(items) => !items.is_empty() && items.iter().all(Value::is_string),
        _ => false,
    }
}

/// `[op, number]` or `[op, number, op, number]` — a point comparison or a range,
/// and nothing between them: a three-element list is a client that lost a bound.
fn validate_numeric(argument: &Value) -> bool {
    let Some(terms) = argument.as_array() else {
        return false;
    };
    if terms.len() != 2 && terms.len() != 4 {
        return false;
    }
    terms.chunks(2).all(|pair| match pair {
        [Value::String(op), bound] => {
            NUMERIC_OPS.contains(&op.as_str()) && bound.as_f64().is_some()
        }
        _ => false,
    })
}

/// A fragment of the client's own policy, quoted and capped. See [`MAX_QUOTED`].
fn quoted(text: &str) -> String {
    match text.char_indices().nth(MAX_QUOTED) {
        None => format!("{text:?}"),
        Some((at, _)) => format!("{:?}...", &text[..at]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn json(text: &str) -> Value {
        serde_json::from_str(text).expect("valid JSON")
    }

    /// `matches` against a body-scope document written as JSON text.
    fn hit(policy: &str, document: &str) -> bool {
        let policy = json(policy);
        validate(&policy).unwrap_or_else(|e| panic!("{policy} is not a valid policy: {e}"));
        matches(&policy, Some(&json(document)))
    }

    fn attributes(pairs: &[(&str, &str, &str)]) -> BTreeMap<String, MessageAttribute> {
        pairs
            .iter()
            .map(|(name, data_type, value)| {
                (
                    (*name).to_string(),
                    MessageAttribute::string(*data_type, *value),
                )
            })
            .collect()
    }

    /// Walk one table of `(verdict, policy, document)`. Every matrix below
    /// shares it, so a case is three strings and a failure names all three.
    fn walk(cases: &[(bool, &str, &str)]) {
        for (want, policy, document) in cases {
            assert_eq!(
                hit(policy, document),
                *want,
                "policy {policy} against {document}"
            );
        }
    }

    /// An exact match, the OR a rule list is, and the AND the top level is.
    #[test]
    fn an_exact_rule_matches_a_value_and_a_list_is_an_or() {
        walk(&[
            (true, r#"{"kind":["order"]}"#, r#"{"kind":"order"}"#),
            (false, r#"{"kind":["order"]}"#, r#"{"kind":"refund"}"#),
            (
                true,
                r#"{"kind":["order","refund"]}"#,
                r#"{"kind":"refund"}"#,
            ),
            // Case matters, and a number is not its own decimal string — the two
            // types never match across.
            (false, r#"{"kind":["Order"]}"#, r#"{"kind":"order"}"#),
            (false, r#"{"n":["5"]}"#, r#"{"n":5}"#),
            (true, r#"{"n":[5]}"#, r#"{"n":5}"#),
            (true, r#"{"n":[5]}"#, r#"{"n":5.0}"#),
            // Every key must match.
            (
                true,
                r#"{"kind":["order"],"tier":["gold"]}"#,
                r#"{"kind":"order","tier":"gold"}"#,
            ),
            (
                false,
                r#"{"kind":["order"],"tier":["gold"]}"#,
                r#"{"kind":"order","tier":"silver"}"#,
            ),
            // An empty policy accepts everything, which is what "no rule to
            // apply" means.
            (true, r#"{}"#, r#"{"anything":1}"#),
        ]);
    }

    /// THE whitelist property: an absent key matches nothing but
    /// `{"exists": false}`, and an explicit null is an absence.
    #[test]
    fn an_absent_key_matches_nothing_but_exists_false() {
        walk(&[
            (false, r#"{"kind":["order"]}"#, r#"{"other":"order"}"#),
            (true, r#"{"kind":[{"exists":false}]}"#, r#"{"other":1}"#),
            (false, r#"{"kind":[{"exists":false}]}"#, r#"{"kind":"x"}"#),
            (true, r#"{"kind":[{"exists":true}]}"#, r#"{"kind":"x"}"#),
            (false, r#"{"kind":[{"exists":true}]}"#, r#"{"other":1}"#),
            (true, r#"{"kind":[{"exists":false}]}"#, r#"{"kind":null}"#),
            (false, r#"{"kind":[{"exists":true}]}"#, r#"{"kind":null}"#),
        ]);
    }

    #[test]
    fn the_string_operators_read_only_strings() {
        walk(&[
            (true, r#"{"k":[{"prefix":"ord"}]}"#, r#"{"k":"order"}"#),
            (false, r#"{"k":[{"prefix":"ord"}]}"#, r#"{"k":"reorder"}"#),
            (true, r#"{"k":[{"suffix":"der"}]}"#, r#"{"k":"order"}"#),
            (false, r#"{"k":[{"suffix":"der"}]}"#, r#"{"k":"orders"}"#),
            (
                true,
                r#"{"k":[{"equals-ignore-case":"ORDER"}]}"#,
                r#"{"k":"order"}"#,
            ),
            (
                false,
                r#"{"k":[{"equals-ignore-case":"ORDERS"}]}"#,
                r#"{"k":"order"}"#,
            ),
            // A number is not a string, so none of the three reads one.
            (false, r#"{"k":[{"prefix":"1"}]}"#, r#"{"k":12}"#),
        ]);
    }

    /// `anything-but`, in every argument shape — and the one thing it is NOT: a
    /// match on an absent attribute.
    #[test]
    fn anything_but_excludes_and_still_requires_the_attribute() {
        walk(&[
            (true, r#"{"k":[{"anything-but":"order"}]}"#, r#"{"k":"x"}"#),
            (
                false,
                r#"{"k":[{"anything-but":"order"}]}"#,
                r#"{"k":"order"}"#,
            ),
            (
                false,
                r#"{"k":[{"anything-but":["a","b"]}]}"#,
                r#"{"k":"b"}"#,
            ),
            (
                true,
                r#"{"k":[{"anything-but":["a","b"]}]}"#,
                r#"{"k":"c"}"#,
            ),
            (false, r#"{"k":[{"anything-but":[1,2]}]}"#, r#"{"k":2}"#),
            (true, r#"{"k":[{"anything-but":[1,2]}]}"#, r#"{"k":3}"#),
            (
                false,
                r#"{"k":[{"anything-but":{"prefix":"tmp-"}}]}"#,
                r#"{"k":"tmp-1"}"#,
            ),
            (
                true,
                r#"{"k":[{"anything-but":{"prefix":"tmp-"}}]}"#,
                r#"{"k":"live-1"}"#,
            ),
            (
                false,
                r#"{"k":[{"anything-but":{"suffix":"-tmp"}}]}"#,
                r#"{"k":"a-tmp"}"#,
            ),
            (
                false,
                r#"{"k":[{"anything-but":{"equals-ignore-case":"ORDER"}}]}"#,
                r#"{"k":"order"}"#,
            ),
            // The attribute must BE there: an absence is never "anything but".
            (false, r#"{"k":[{"anything-but":"order"}]}"#, r#"{"j":1}"#),
        ]);
    }

    /// THE negation rule: an array value is excluded when ANY of its elements
    /// is, where every positive rule matches when any element does.
    ///
    /// A subscriber that asked for anything but `internal` must not be handed a
    /// message tagged `["internal","order"]` — the tag it excluded is on the
    /// message, and OR-ing the elements the way a positive rule does would
    /// deliver it.
    #[test]
    fn anything_but_over_an_array_is_none_of_the_elements() {
        walk(&[
            (
                false,
                r#"{"kind":[{"anything-but":"internal"}]}"#,
                r#"{"kind":["internal","order"]}"#,
            ),
            (
                true,
                r#"{"kind":[{"anything-but":"internal"}]}"#,
                r#"{"kind":["order","refund"]}"#,
            ),
            // Every argument shape folds the same way.
            (
                false,
                r#"{"kind":[{"anything-but":["internal","secret"]}]}"#,
                r#"{"kind":["order","secret"]}"#,
            ),
            (
                false,
                r#"{"kind":[{"anything-but":{"prefix":"tmp-"}}]}"#,
                r#"{"kind":["live-1","tmp-2"]}"#,
            ),
            (
                true,
                r#"{"kind":[{"anything-but":{"prefix":"tmp-"}}]}"#,
                r#"{"kind":["live-1","live-2"]}"#,
            ),
            // An EMPTY array excludes nothing, so it is "anything but" — and it
            // is still a present attribute, which is what the rule requires.
            (true, r#"{"kind":[{"anything-but":"x"}]}"#, r#"{"kind":[]}"#),
            // The positive rules keep the OR: this is the asymmetry itself.
            (
                true,
                r#"{"kind":["order"]}"#,
                r#"{"kind":["internal","order"]}"#,
            ),
        ]);
    }

    /// The nested operators take a LIST, which AWS stores and this must not
    /// refuse — and the list is an OR over its terms.
    #[test]
    fn anything_but_takes_a_list_argument_for_its_nested_operators() {
        walk(&[
            (
                false,
                r#"{"src":[{"anything-but":{"prefix":["test-","dev-"]}}]}"#,
                r#"{"src":"dev-1"}"#,
            ),
            (
                true,
                r#"{"src":[{"anything-but":{"prefix":["test-","dev-"]}}]}"#,
                r#"{"src":"prod-1"}"#,
            ),
            (
                false,
                r#"{"src":[{"anything-but":{"suffix":["-test","-dev"]}}]}"#,
                r#"{"src":"a-test"}"#,
            ),
            (
                false,
                r#"{"src":[{"anything-but":{"equals-ignore-case":["A","B"]}}]}"#,
                r#"{"src":"b"}"#,
            ),
            // ...and the array value folds through it, one element at a time.
            (
                false,
                r#"{"src":[{"anything-but":{"prefix":["test-","dev-"]}}]}"#,
                r#"{"src":["prod-1","dev-2"]}"#,
            ),
        ]);
        // The POSITIVE operators are unchanged: a list there is still refused,
        // because AWS spells that OR with the rule list around them.
        assert!(validate(&json(r#"{"k":[{"prefix":["a","b"]}]}"#)).is_err());
        // An empty list excludes nothing and can only be a client's mistake.
        assert!(validate(&json(r#"{"k":[{"anything-but":{"prefix":[]}}]}"#)).is_err());
        assert!(validate(&json(r#"{"k":[{"anything-but":{"prefix":[1]}}]}"#)).is_err());
    }

    /// `numeric`, point and range — and the fact that it never reads a string.
    #[test]
    fn numeric_compares_numbers_and_only_numbers() {
        walk(&[
            (true, r#"{"n":[{"numeric":["=",5]}]}"#, r#"{"n":5}"#),
            (false, r#"{"n":[{"numeric":["=",5]}]}"#, r#"{"n":6}"#),
            (true, r#"{"n":[{"numeric":[">",5]}]}"#, r#"{"n":5.1}"#),
            (false, r#"{"n":[{"numeric":[">",5]}]}"#, r#"{"n":5}"#),
            (true, r#"{"n":[{"numeric":[">=",5]}]}"#, r#"{"n":5}"#),
            (true, r#"{"n":[{"numeric":["<",0]}]}"#, r#"{"n":-3}"#),
            (true, r#"{"n":[{"numeric":["<=",0]}]}"#, r#"{"n":0}"#),
            (
                true,
                r#"{"n":[{"numeric":[">",0,"<=",100]}]}"#,
                r#"{"n":100}"#,
            ),
            (
                false,
                r#"{"n":[{"numeric":[">",0,"<=",100]}]}"#,
                r#"{"n":101}"#,
            ),
            (
                false,
                r#"{"n":[{"numeric":[">",0,"<=",100]}]}"#,
                r#"{"n":0}"#,
            ),
            (false, r#"{"n":[{"numeric":["=",5]}]}"#, r#"{"n":"5"}"#),
        ]);
    }

    /// An ARRAY in the document matches when any element does, at a leaf and at
    /// an intermediate level — which is what makes `String.Array` attributes and
    /// JSON list fields work.
    #[test]
    fn an_array_in_the_document_matches_on_any_element() {
        walk(&[
            (
                true,
                r#"{"tags":["urgent"]}"#,
                r#"{"tags":["slow","urgent"]}"#,
            ),
            (false, r#"{"tags":["urgent"]}"#, r#"{"tags":["slow"]}"#),
            (
                true,
                r#"{"tags":[{"prefix":"ur"}]}"#,
                r#"{"tags":["slow","urgent"]}"#,
            ),
            (
                true,
                r#"{"lines":{"sku":["a"]}}"#,
                r#"{"lines":[{"sku":"b"},{"sku":"a"}]}"#,
            ),
            (
                false,
                r#"{"lines":{"sku":["a"]}}"#,
                r#"{"lines":[{"sku":"b"}]}"#,
            ),
        ]);
    }

    /// Nested objects, which are the body scope's whole point, and `$or`, which
    /// is one more conjunct beside its siblings.
    #[test]
    fn nested_policies_and_the_top_level_or() {
        walk(&[
            (
                true,
                r#"{"customer":{"tier":["gold"]}}"#,
                r#"{"customer":{"tier":"gold"}}"#,
            ),
            (
                false,
                r#"{"customer":{"tier":["gold"]}}"#,
                r#"{"customer":{"tier":"silver"}}"#,
            ),
            (
                false,
                r#"{"customer":{"tier":["gold"]}}"#,
                r#"{"customer":"gold"}"#,
            ),
            (
                false,
                r#"{"customer":{"tier":["gold"]}}"#,
                r#"{"other":{"tier":"gold"}}"#,
            ),
            (
                true,
                r#"{"a":{"b":{"c":["deep"]}}}"#,
                r#"{"a":{"b":{"c":"deep"}}}"#,
            ),
            (true, r#"{"$or":[{"k":["a"]},{"k":["b"]}]}"#, r#"{"k":"b"}"#),
            (
                false,
                r#"{"$or":[{"k":["a"]},{"k":["b"]}]}"#,
                r#"{"k":"c"}"#,
            ),
            (
                false,
                r#"{"tier":["gold"],"$or":[{"k":["a"]},{"k":["b"]}]}"#,
                r#"{"k":"a","tier":"silver"}"#,
            ),
            (
                true,
                r#"{"tier":["gold"],"$or":[{"k":["a"]},{"k":["b"]}]}"#,
                r#"{"k":"a","tier":"gold"}"#,
            ),
        ]);
    }

    /// A body that is not JSON has no document, and a body-scope policy over one
    /// matches nothing — AWS's own answer, and the reason `matches` takes an
    /// `Option` instead of an empty object.
    #[test]
    fn a_body_that_is_not_json_matches_no_policy() {
        let policy = json(r#"{"kind":[{"exists":false}]}"#);
        assert!(!matches(&policy, None));
        // ...where the same policy against an empty JSON object DOES match, so
        // the two really are different answers.
        assert!(matches(&policy, Some(&json("{}"))));
    }

    /// The attribute document, which is where the two scopes stop being the
    /// same: types decide what a rule can read.
    #[test]
    fn message_attributes_become_a_document_a_rule_can_read() {
        let mut map = attributes(&[
            ("kind", "String", "order"),
            ("n", "Number", "42"),
            ("labelled", "String.foo", "custom"),
            ("tags", "String.Array", r#"["a","b"]"#),
            ("notanumber", "Number", "not a number"),
        ]);
        map.insert(
            "blob".to_string(),
            MessageAttribute::binary("Binary", [1, 2, 3]),
        );
        let document = document_of_attributes(&map);

        assert!(matches(&json(r#"{"kind":["order"]}"#), Some(&document)));
        // A Number attribute is a NUMBER, so numeric rules read it...
        assert!(matches(
            &json(r#"{"n":[{"numeric":[">",41]}]}"#),
            Some(&document)
        ));
        // ...and the decimal string it arrived as does not match it.
        assert!(!matches(&json(r#"{"n":["42"]}"#), Some(&document)));
        // A custom String label is still a string.
        assert!(matches(
            &json(r#"{"labelled":["custom"]}"#),
            Some(&document)
        ));
        // String.Array is a list, and a leaf matches any element.
        assert!(matches(&json(r#"{"tags":["b"]}"#), Some(&document)));
        // A Number that does not parse stays readable as a string rather than
        // vanishing.
        assert!(matches(
            &json(r#"{"notanumber":["not a number"]}"#),
            Some(&document)
        ));
        // A Binary attribute is ABSENT, which is the only consistent reading of
        // "ignored for filtering".
        assert!(matches(
            &json(r#"{"blob":[{"exists":false}]}"#),
            Some(&document)
        ));
        assert!(!matches(
            &json(r#"{"blob":[{"exists":true}]}"#),
            Some(&document)
        ));
    }

    /// Every grammar this engine refuses, refused at WRITE time and named. The
    /// property under test is the safety one: nothing reaches the store that the
    /// matcher would silently answer "no" to for ever.
    #[test]
    fn a_policy_this_engine_cannot_evaluate_is_refused_and_named() {
        for (policy, expect) in [
            (r#"[]"#, "JSON object"),
            (r#""text""#, "JSON object"),
            (r#"{"k":"order"}"#, "list of match rules"),
            (r#"{"k":7}"#, "list of match rules"),
            (r#"{"k":[]}"#, "empty"),
            (r#"{"k":[true]}"#, "string, a number"),
            (r#"{"k":[["nested"]]}"#, "string, a number"),
            (r#"{"k":[{}]}"#, "exactly one match operator"),
            (
                r#"{"k":[{"prefix":"a","suffix":"b"}]}"#,
                "exactly one match operator",
            ),
            (r#"{"k":[{"nope":"a"}]}"#, "not a match operator"),
            (r#"{"k":[{"cidr":"10.0.0.0/8"}]}"#, "cidr"),
            (r#"{"k":[{"exists":"true"}]}"#, "argument to"),
            (r#"{"k":[{"prefix":7}]}"#, "argument to"),
            (r#"{"k":[{"anything-but":[]}]}"#, "argument to"),
            (
                r#"{"k":[{"anything-but":{"numeric":["=",1]}}]}"#,
                "argument to",
            ),
            (r#"{"k":[{"numeric":["=",1,"<"]}]}"#, "argument to"),
            (r#"{"k":[{"numeric":["~",1]}]}"#, "argument to"),
            (r#"{"k":[{"numeric":"= 1"}]}"#, "argument to"),
            (r#"{"$or":{}}"#, "non-empty list"),
            (r#"{"$or":[]}"#, "non-empty list"),
            (r#"{"$or":["k"]}"#, "branch is a policy"),
            (r#"{"a":{"$or":[{"k":["x"]}]}}"#, "top level"),
        ] {
            let e = validate(&json(policy)).expect_err(policy);
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{policy}");
            assert!(
                e.message.contains(expect),
                "{policy}: {} does not mention {expect}",
                e.message
            );
        }
    }

    /// A policy deeper than a client could plausibly write is refused rather
    /// than recursed into: this walks a document a client posted, on a listener.
    #[test]
    fn a_policy_deeper_than_the_cap_is_refused() {
        let mut policy = String::from(r#"["x"]"#);
        for _ in 0..=MAX_DEPTH {
            policy = format!(r#"{{"a":{policy}}}"#);
        }
        let e = validate(&json(&policy)).expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("levels deep"), "{}", e.message);
        // Exactly at the cap is legal, and it matches.
        let mut policy = String::from(r#"["x"]"#);
        let mut document = String::from(r#""x""#);
        for _ in 0..MAX_DEPTH {
            policy = format!(r#"{{"a":{policy}}}"#);
            document = format!(r#"{{"a":{document}}}"#);
        }
        assert!(validate(&json(&policy)).is_ok());
        assert!(matches(&json(&policy), Some(&json(&document))));
    }

    /// The refusal quotes the client's own key, capped: this message becomes a
    /// log line, and a policy key is client bytes.
    #[test]
    fn a_refusal_caps_what_it_quotes_back() {
        let key = "k".repeat(MAX_QUOTED * 2);
        let policy = json(&format!(r#"{{"{key}":"not a list"}}"#));
        let e = validate(&policy).expect_err("refused");
        assert!(e.message.contains("..."), "{}", e.message);
        assert!(
            e.message.len() < key.len() + 120,
            "the whole key survived: {}",
            e.message
        );
    }
}
