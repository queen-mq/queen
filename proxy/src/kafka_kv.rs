//! The Kafka facade's reserved KV key space, and the one rule the gateway
//! applies to it.
//!
//! # Why this exists
//!
//! `POST /api/v1/kv` is the ONLY route the Kafka facade has for a consumer
//! group's bookkeeping: committed offsets, the group index, the cluster fence,
//! transaction markers, the node registry and the topic-config records all live
//! in the KV store under one namespace (`queen-kafka`) and one key prefix
//! (`qk:`). `routes.rs` classifies that route `Gated(Feature::Kv, GatedOp::Mixed)`,
//! which is correct for the KV *product* and wrong for a Kafka consumer:
//!
//!   - the `kv` plan flag would make a tenant buy a feature it never asked for
//!     in order to run a Kafka client at all, and
//!   - the storage/monthly block would refuse an `OffsetFetch` — a READ — on a
//!     tenant that is over its quota, stranding every consumer at an offset it
//!     can never move past while the backlog it would drain keeps growing.
//!     That is the exact trap `routes.rs` spells out in the `/api/v1/fetch` arm
//!     and the reason `Mixed` exists at all (PLAN_KV_TIMERS.md §9.6); the
//!     facade's own offsets were simply on the wrong side of it.
//!
//! So a batch that addresses NOTHING BUT the facade's reserved space is
//! reclassified `RouteClass::Consume` by the gateway — the same authority the
//! fetch it serves already carries — and a batch that touches anything else
//! keeps `Gated(Kv, Mixed)` in every one of its five gates.
//!
//! # Why the gateway and not `classify()`
//!
//! `classify(method, path)` has no body, and this rule is a property of the
//! body. It could not live there even if the file were free to edit: the webdoc
//! route table is keyed by (path, method) and cannot express a body-conditional
//! rule, so publishing it there would publish a falsehood. `classify` is
//! therefore unchanged, and this module is what the gateway consults.
//!
//! # Fail closed
//!
//! Every uncertainty resolves to "not a Kafka batch", which means "keep today's
//! gating". An unreadable body, an empty batch, an op this proxy has not been
//! told about, one foreign key anywhere in the array — all `false`. The cost of
//! a false negative is that a Kafka client on a plan without `kv` sees the 403
//! it sees today; the cost of a false positive is a tenant slipping the KV
//! feature gate and the storage block with an arbitrary key. Those are not
//! comparable, and the asymmetry is the whole design of the predicate.

use crate::routes::{Feature, GatedOp, RouteClass};

/// The namespace the facade writes under. Fixed in
/// `queen-kafka/src/offsets.rs` (`NAMESPACE`), never client-chosen — which is
/// what makes it usable as half of an authorization decision.
const KAFKA_NS: &str = "queen-kafka";

/// The one literal that covers the facade's whole key space. Every family is
/// `qk:<what>:<...>`:
///
/// | prefix | what it holds | written by |
/// |---|---|---|
/// | `qk:group:`    | committed offsets      | `offsets.rs` |
/// | `qk:groups:`   | group existence index  | `offsets.rs` |
/// | `qk:fence:`    | cluster-mode fence     | `offsets.rs` |
/// | `qk:node:`     | cluster node registry  | `cluster.rs` |
/// | `qk:txn:`      | transaction markers    | `txn.rs` |
/// | `qk:topiccfg:` | topic config records   | `topic_record.rs` |
///
/// One prefix rather than six, deliberately: the families are the facade's
/// internal business and it has already added one since this rule was designed
/// (`qk:topiccfg:`). A per-family list would have silently degraded the day
/// that landed — the batch would have stopped being recognised and the tenant
/// would have gone back to needing the `kv` plan flag — whereas the shared
/// prefix keeps working and keeps the fail-closed direction for everything
/// that is not `qk:` at all.
const KAFKA_PREFIX: &str = "qk:";

/// The ops the facade emits, and the field each one carries its key in.
///
/// A CLOSED list, and this is the trap the shorthand "sniff `op.key`" walks
/// into: `getMany` (every `OffsetFetch`) carries `keys`, an ARRAY, and
/// `getPrefix` (every `ListGroups`, every all-topics fetch) carries `prefix`.
/// A sniffer that reads only `key` sees no foreign keys in either, and
/// "no keys are foreign" is not "this is a Kafka batch" — it would fail OPEN
/// on the two shapes the read path is made of.
///
/// Closed on the op NAME too, which is stricter than "does every key field
/// start with `qk:`". An op this proxy has not been told about is not
/// recognised even when its key is in the reserved space, so a future member of
/// the KV taxonomy cannot inherit the reclassification by accident; it falls
/// back to `Gated(Kv, Mixed)`, which is exactly today's behaviour and therefore
/// the safe direction. The facade's own set is
/// `queen-kafka/src/queen.rs`'s `KvOp` enum, which has these four variants and
/// no others (`get`, `incr` and `putIfAbsent` it never emits — the last
/// desugars to `put` + `expect:0`).
const KAFKA_OPS: [&str; 4] = ["put", "delete", "getMany", "getPrefix"];

/// Is this the route whose body decides its own class?
///
/// Keyed on the CLASS and not on `(method, path)`, so the predicate cannot
/// drift from `classify()`: `Gated(Kv, Mixed)` is produced by exactly one arm
/// of that function — `POST /api/v1/kv` and its trailing-slash spelling — and
/// nothing else in the file can produce it. Matching the path here instead
/// would be a second copy of that rule, free to disagree with the first.
pub fn is_kv_batch(class: RouteClass) -> bool {
    matches!(class, RouteClass::Gated(Feature::Kv, GatedOp::Mixed))
}

/// The class this request is actually gated by, once its body has been read.
///
/// The whole override in one place: `Gated(Kv, Mixed)` becomes `Consume` when
/// — and only when — the batch is the facade's own. Every other class is
/// returned untouched, so a caller that hands this function the wrong class
/// changes nothing.
pub fn effective_class(class: RouteClass, body: &[u8]) -> RouteClass {
    if is_kv_batch(class) && is_kafka_only_batch(body) {
        RouteClass::Consume
    } else {
        class
    }
}

/// Does EVERY op in this batch address the facade's own reserved key space?
///
/// See the module header for the fail-closed rule. In particular an **empty
/// batch is `false`**: "no op is foreign" must never become "this is a Kafka
/// batch", or an empty array would be a way to reach `Consume` on a route the
/// tenant's plan does not grant.
pub fn is_kafka_only_batch(body: &[u8]) -> bool {
    let root: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return false,
    };
    // Both roots the route accepts, and the two `mixed_batch_grows` already
    // reads: a bare array, or `{"operations":[...]}`. The facade always sends
    // the object form (`queen.rs`'s `KvBody`), but the sniffer must agree with
    // the route rather than with one of its clients.
    let ops = match &root {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(o) => match o.get("operations") {
            Some(serde_json::Value::Array(a)) => a,
            _ => return false,
        },
        _ => return false,
    };
    !ops.is_empty() && ops.iter().all(is_kafka_op)
}

/// One op, all three field shapes.
fn is_kafka_op(op: &serde_json::Value) -> bool {
    let Some(o) = op.as_object() else {
        return false;
    };
    // The namespace is half the identity of the key space and it is checked
    // first: `qk:group:g:orders:0` in SOMEBODY ELSE'S namespace is a different
    // row in `queen.kv` (the PK is `(tenant_id, namespace, key)`) and is not
    // the facade's bookkeeping in any sense.
    if o.get("ns").and_then(|v| v.as_str()) != Some(KAFKA_NS) {
        return false;
    }
    if !o
        .get("op")
        .and_then(|v| v.as_str())
        .is_some_and(|op| KAFKA_OPS.contains(&op))
    {
        return false;
    }
    // Exactly one of these is present on each of the four ops, but the check is
    // written as "every recognised field that IS present is ours, and at least
    // one was" rather than as a match on the op name. An op carrying two key
    // fields therefore has both read, and an op carrying none is foreign —
    // which is what closes the gap against a body that names a known op and
    // then addresses its keys some other way.
    let mut addressed = false;
    if let Some(v) = o.get("key") {
        let Some(s) = v.as_str() else { return false };
        if !s.starts_with(KAFKA_PREFIX) {
            return false;
        }
        addressed = true;
    }
    if let Some(v) = o.get("keys") {
        let Some(list) = v.as_array() else {
            return false;
        };
        // An empty list addresses nothing, so it cannot be evidence that this
        // is a Kafka batch — the same reason an empty batch is `false`.
        if list.is_empty() {
            return false;
        }
        for e in list {
            let Some(s) = e.as_str() else { return false };
            if !s.starts_with(KAFKA_PREFIX) {
                return false;
            }
        }
        addressed = true;
    }
    if let Some(v) = o.get("prefix") {
        let Some(s) = v.as_str() else { return false };
        // A prefix SHORTER than `qk:` (`q`, or `""`) would range over the
        // reserved space AND over foreign keys, so it is not a read of the
        // facade's own space and does not qualify. `starts_with` says exactly
        // that: `"q"` does not start with `"qk:"`.
        if !s.starts_with(KAFKA_PREFIX) {
            return false;
        }
        addressed = true;
    }
    addressed
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The §0.6 trap, which is the whole reason this function exists: the two
    /// READ shapes carry their keys somewhere other than `key`, and a sniffer
    /// that misses them fails open on every `OffsetFetch` and every
    /// `ListGroups`.
    #[test]
    fn every_op_shape_the_facade_emits_is_recognised() {
        for body in [
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":{"offset":7},"forever":true}]}"#.as_slice(),
            br#"{"operations":[{"op":"delete","ns":"queen-kafka","key":"qk:groups:g"}]}"#,
            br#"{"operations":[{"op":"getMany","ns":"queen-kafka","keys":["qk:group:g:orders:0","qk:group:g:orders:1"]}]}"#,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-kafka","prefix":"qk:groups:","limit":500}]}"#,
        ] {
            assert!(
                is_kafka_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        // and the bare-array root the route also accepts
        assert!(is_kafka_only_batch(
            br#"[{"op":"put","ns":"queen-kafka","key":"qk:txn:tx-1","value":{},"forever":true}]"#
        ));
    }

    /// A real batch is many ops, and the verdict is over ALL of them. One
    /// foreign key is enough to keep the whole batch on the KV gate — the same
    /// whole-batch posture `mixed_batch_grows` takes, for the same reason: a
    /// half-classified request is one nobody can reason about.
    #[test]
    fn one_foreign_key_in_a_batch_makes_it_foreign() {
        let mut ops: Vec<String> = (0..9)
            .map(|i| {
                format!(
                    r#"{{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:{i}","value":{{"offset":1}}}}"#
                )
            })
            .collect();
        // the same batch, without the intruder, IS a Kafka batch
        let clean = format!(r#"{{"operations":[{}]}}"#, ops.join(","));
        assert!(is_kafka_only_batch(clean.as_bytes()));

        ops.push(r#"{"op":"put","ns":"queen-kafka","key":"app:cache:x","value":1}"#.to_string());
        let dirty = format!(r#"{{"operations":[{}]}}"#, ops.join(","));
        assert!(!is_kafka_only_batch(dirty.as_bytes()));
    }

    /// The array case of the above: `getMany` is where a foreign key is
    /// easiest to hide, because the op looks entirely ordinary from outside.
    #[test]
    fn a_getmany_with_one_foreign_element_is_foreign() {
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-kafka","keys":["qk:group:g:orders:0","secrets:root"]}]}"#
        ));
        // an empty list addresses nothing and is not evidence of anything
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-kafka","keys":[]}]}"#
        ));
        // a non-string element is not a key this proxy can judge
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-kafka","keys":["qk:group:g:orders:0",7]}]}"#
        ));
    }

    /// The namespace is half the identity. `queen.kv`'s PK is
    /// `(tenant_id, namespace, key)`, so the same spelling in another namespace
    /// is a different row and is nobody's bookkeeping but the caller's.
    #[test]
    fn a_foreign_namespace_is_foreign() {
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"put","ns":"other","key":"qk:group:g:orders:0","value":1}]}"#
        ));
        // ...including a namespace that merely looks like it
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"put","ns":"queen-kafka2","key":"qk:group:g:orders:0","value":1}]}"#
        ));
        // ...and a missing or non-string one
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"put","key":"qk:group:g:orders:0","value":1}]}"#
        ));
        assert!(!is_kafka_only_batch(
            br#"{"operations":[{"op":"put","ns":7,"key":"qk:group:g:orders:0","value":1}]}"#
        ));
    }

    /// UNREADABLE IS NOT KAFKA, and neither is EMPTY. The second half is the
    /// one worth writing down: "every op is ours" is vacuously true of no ops,
    /// and an empty array must not be a way to reach `Consume` on a route the
    /// plan does not grant.
    #[test]
    fn an_unreadable_or_empty_batch_is_not_a_kafka_batch() {
        for body in [
            b"not json".as_slice(),
            b"",
            b"[]",
            br#"{"operations":[]}"#,
            br#"{"operations":{}}"#,
            br#"{"nope":1}"#,
            b"42",
            b"null",
            br#""qk:group:g""#,
        ] {
            assert!(
                !is_kafka_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// Fail-closed against a shape this proxy has not been told about: an op
    /// with no key field at all, an op whose key field is not a string, and an
    /// op NAME outside the facade's closed set even when its key is in the
    /// reserved space. All three keep the batch on `Gated(Kv, Mixed)`, which is
    /// exactly today's behaviour.
    #[test]
    fn an_op_with_no_recognised_key_field_is_foreign() {
        for body in [
            br#"{"operations":[{"op":"put","ns":"queen-kafka","value":1}]}"#.as_slice(),
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":7}]}"#,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-kafka","prefix":null}]}"#,
            br#"{"operations":[{"ns":"queen-kafka","key":"qk:group:g:orders:0"}]}"#,
            // a known key space under an op the facade does not emit
            br#"{"operations":[{"op":"incr","ns":"queen-kafka","key":"qk:group:g:orders:0","by":1}]}"#,
            br#"{"operations":[{"op":"somethingNew","ns":"queen-kafka","key":"qk:group:g:orders:0"}]}"#,
            // a non-object element
            br#"{"operations":["qk:group:g:orders:0"]}"#,
        ] {
            assert!(
                !is_kafka_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// One literal covers the facade's whole space — including
    /// `qk:topiccfg:`, which did not exist when this rule was written and is
    /// the reason the prefix is shared rather than enumerated per family.
    #[test]
    fn all_six_qk_spaces_are_recognised() {
        for key in [
            "qk:group:g:orders:0",
            "qk:groups:g",
            "qk:fence:g",
            "qk:txn:tx-1",
            "qk:node:n1",
            "qk:topiccfg:orders",
        ] {
            let body =
                format!(r#"{{"operations":[{{"op":"put","ns":"queen-kafka","key":"{key}"}}]}}"#);
            assert!(is_kafka_only_batch(body.as_bytes()), "{key}");
        }
        // and a key that merely starts with the same two letters does not
        for key in ["qk", "qkgroup:g", "q", "", "QK:group:g"] {
            let body =
                format!(r#"{{"operations":[{{"op":"put","ns":"queen-kafka","key":"{key}"}}]}}"#);
            assert!(!is_kafka_only_batch(body.as_bytes()), "{key}");
        }
    }

    /// A `getPrefix` that would range over foreign keys as well as ours is not
    /// a read of the facade's own space, however much of that space it covers.
    #[test]
    fn a_prefix_wider_than_the_reserved_space_is_foreign() {
        for prefix in ["q", "", "a"] {
            let body = format!(
                r#"{{"operations":[{{"op":"getPrefix","ns":"queen-kafka","prefix":"{prefix}","limit":10}}]}}"#
            );
            assert!(!is_kafka_only_batch(body.as_bytes()), "{prefix:?}");
        }
        // the exact boundary is a read of the whole reserved space and nothing
        // else, so it qualifies
        assert!(is_kafka_only_batch(
            br#"{"operations":[{"op":"getPrefix","ns":"queen-kafka","prefix":"qk:","limit":10}]}"#
        ));
    }

    /// The route predicate is the CLASS, so it fires on exactly the arm of
    /// `classify` that produces it and on nothing else — no second copy of the
    /// path rule to drift.
    #[test]
    fn only_the_kv_batch_class_asks_its_body() {
        use axum::http::Method;
        assert!(is_kv_batch(crate::routes::classify(
            &Method::POST,
            "/api/v1/kv"
        )));
        assert!(is_kv_batch(crate::routes::classify(
            &Method::POST,
            "/api/v1/kv/"
        )));
        // the other two `Mixed` families are not this one
        assert!(!is_kv_batch(crate::routes::classify(
            &Method::POST,
            "/api/v1/timers"
        )));
        assert!(!is_kv_batch(crate::routes::classify(
            &Method::POST,
            "/streams/v1/cycle"
        )));
        // nor are the single-key kv routes, which are already split by verb
        for (m, p) in [
            (Method::GET, "/api/v1/kv/queen-kafka/qk:group:g"),
            (Method::PUT, "/api/v1/kv/queen-kafka/qk:group:g"),
            (Method::DELETE, "/api/v1/kv/queen-kafka/qk:group:g"),
            (Method::GET, "/api/v1/kv"),
            (Method::POST, "/api/v1/push"),
            (Method::POST, "/api/v1/fetch"),
        ] {
            assert!(!is_kv_batch(crate::routes::classify(&m, p)), "{m} {p}");
        }
    }
}
