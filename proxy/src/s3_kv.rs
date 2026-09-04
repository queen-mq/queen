//! The S3 sink's reserved KV key space, and the one rule the gateway applies
//! to it.
//!
//! # Why this exists
//!
//! `POST /api/v1/kv` is where the sink keeps the only durable state it has: the
//! per-queue window commit pointer, the checkpoint that names the last object
//! it uploaded, the per-partition positions cache and the instance registry
//! (PLAN_S3_SINK.md §4.3, §4.5, §6.6). All of it lives under one namespace
//! (`queen-s3`) and one key prefix (`s3:`). `routes.rs` classifies that route
//! `Gated(Feature::Kv, GatedOp::Mixed)`, which is correct for the KV *product*
//! and wrong for the sink:
//!
//!   - the storage/monthly block would refuse the window COMMIT of a tenant
//!     that is over its quota. The sink cannot advance, so it re-uploads and
//!     re-commits the same window for ever while its lag grows without bound —
//!     and the block was supposed to stop the tenant GROWING, which a commit
//!     pointer of a few hundred bytes does not do. That is the §9.6 trap
//!     `routes.rs` already spells out in the `/api/v1/fetch` arm, on the one
//!     write the read path cannot proceed without.
//!   - the sink is a CONSUMER: everything it does is `fetch` +
//!     `partitions/changed` + this bookkeeping. Making it also require the `kv`
//!     product's authority is asking a consume-scoped credential to carry a
//!     grant it has no other use for.
//!
//! So a batch that addresses NOTHING BUT the sink's reserved space is
//! reclassified `RouteClass::Consume` by the gateway — the same authority the
//! fetch and the discovery call beside it already carry — and a batch that
//! touches anything else keeps `Gated(Kv, Mixed)` in every one of its gates.
//!
//! This is `kafka_kv.rs`'s rule, one namespace over, and it is a SECOND FILE
//! rather than a parameter of the first on purpose: the two key spaces are
//! owned by two different products with two different op sets, and a shared
//! predicate would mean a change made for one silently widens the carve-out of
//! the other. Duplication is the cheaper failure here.
//!
//! # Why the gateway and not `classify()`
//!
//! `classify(method, path)` has no body, and this rule is a property of the
//! body. Nor could the webdoc route table carry it: that table is keyed by
//! (path, method), so stating a body-conditional rule there would publish a
//! falsehood. `classify` is therefore unchanged, and this module is what the
//! gateway consults.
//!
//! # Fail closed
//!
//! Every uncertainty resolves to "not a sink batch", which means "keep today's
//! gating". An unreadable body, an empty batch, an op this proxy has not been
//! told about, one foreign key anywhere in the array — all `false`. The cost of
//! a false negative is that the sink of a quota-blocked tenant stalls loudly,
//! which is what happens today; the cost of a false positive is a tenant
//! slipping the KV feature gate and the storage block with an arbitrary key.
//! Those are not comparable, and the asymmetry is the whole design of the
//! predicate.

use crate::routes::{Feature, GatedOp, RouteClass};

/// The namespace the sink writes under. Fixed in the sink binary, never
/// client-chosen — which is what makes it usable as half of an authorization
/// decision.
const S3_NS: &str = "queen-s3";

/// The one literal that covers the sink's whole key space. Every key the
/// connector writes is `s3:<sink>:<escaped queue>:<what>`
/// (connectors/queen-s3/src/lease.rs and driver.rs), and `<what>` is one of:
///
/// | suffix | what it holds |
/// |---|---|
/// | `:intent`     | the in-flight window intent, written before the upload |
/// | `:committed`  | the committed window pointer per (sink, queue) |
/// | `:lease`      | the queue-ownership lease, also the fence of every commit batch |
///
/// The per-partition position cache lives in the bucket, not here. The example
/// keys in the tests below are illustrative shapes that exercise the prefix rule;
/// only the `s3:` prefix and the namespace are load-bearing.
///
/// One prefix rather than four, deliberately, and for the reason `kafka_kv.rs`
/// learned the hard way: a per-family list degrades silently the day the sink
/// adds a fifth — the batch stops being recognised and the tenant is back on
/// the gate — whereas the shared prefix keeps working and keeps the fail-closed
/// direction for everything that is not `s3:` at all.
const S3_PREFIX: &str = "s3:";

/// The ops the sink emits, and the field each one carries its key in.
///
/// A CLOSED list. `get`/`getMany`/`getPrefix` are the recovery reads of §4.5,
/// `put` (with `expect`) is the window commit's CAS, `putIfAbsent` is how an
/// instance claims a queue, `delete` retires an intent. `incr` is absent
/// because the sink never counts anything in KV, and its absence is what stops
/// a future member of the KV taxonomy inheriting this reclassification by
/// accident: an unrecognised op falls back to `Gated(Kv, Mixed)`, which is
/// exactly today's behaviour and therefore the safe direction.
///
/// The list is over op NAMES only. `expect`, `required`, `ttlSeconds`,
/// `forever` and `value` are ordinary fields of these ops and are neither
/// required nor forbidden here — they say nothing about WHICH ROWS the batch
/// addresses, which is the only question this file answers.
const S3_OPS: [&str; 6] = [
    "get",
    "getMany",
    "getPrefix",
    "put",
    "putIfAbsent",
    "delete",
];

/// Is this the route whose body decides its own class?
///
/// Keyed on the CLASS and not on `(method, path)`, so the predicate cannot
/// drift from `classify()`: `Gated(Kv, Mixed)` is produced by exactly one arm
/// of that function and nothing else in the file can produce it.
pub fn is_kv_batch(class: RouteClass) -> bool {
    matches!(class, RouteClass::Gated(Feature::Kv, GatedOp::Mixed))
}

/// The class this request is actually gated by, once its body has been read.
///
/// `Gated(Kv, Mixed)` becomes `Consume` when — and only when — the batch is the
/// sink's own. Every other class is returned untouched, so a caller that hands
/// this function the wrong class changes nothing, and chaining it after
/// `kafka_kv::effective_class` is safe in either order: a batch cannot be both,
/// since each demands that EVERY op sit in its own namespace.
pub fn effective_class(class: RouteClass, body: &[u8]) -> RouteClass {
    if is_kv_batch(class) && is_sink_only_batch(body) {
        RouteClass::Consume
    } else {
        class
    }
}

/// Does EVERY op in this batch address the sink's own reserved key space?
///
/// See the module header for the fail-closed rule. In particular an **empty
/// batch is `false`**: "no op is foreign" must never become "this is a sink
/// batch", or an empty array would be a way to reach `Consume` on a route the
/// tenant's plan does not grant.
pub fn is_sink_only_batch(body: &[u8]) -> bool {
    let root: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return false,
    };
    // Both roots the route accepts: a bare array, or `{"operations":[...]}`.
    // The sniffer must agree with the ROUTE rather than with one of its
    // clients, whatever shape the sink itself happens to send.
    let ops = match &root {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(o) => match o.get("operations") {
            Some(serde_json::Value::Array(a)) => a,
            _ => return false,
        },
        _ => return false,
    };
    !ops.is_empty() && ops.iter().all(is_sink_op)
}

/// One op, all three field shapes.
fn is_sink_op(op: &serde_json::Value) -> bool {
    let Some(o) = op.as_object() else {
        return false;
    };
    // The namespace is half the identity of the key space and it is checked
    // first: `s3:commit:orders` in SOMEBODY ELSE'S namespace is a different row
    // in the KV store (the PK is `(tenant_id, namespace, key)`) and is not the
    // sink's bookkeeping in any sense.
    if o.get("ns").and_then(|v| v.as_str()) != Some(S3_NS) {
        return false;
    }
    if !o
        .get("op")
        .and_then(|v| v.as_str())
        .is_some_and(|op| S3_OPS.contains(&op))
    {
        return false;
    }
    // Exactly one of these is present on each op, but the check is written as
    // "every recognised field that IS present is ours, and at least one was"
    // rather than as a match on the op name. An op carrying two key fields
    // therefore has both read, and an op carrying none is foreign — which
    // closes the gap against a body that names a known op and then addresses
    // its keys some other way. `getMany` carries `keys` (an ARRAY) and
    // `getPrefix` carries `prefix`; a sniffer that read only `key` would see no
    // foreign keys in either and fail OPEN on both of the recovery shapes.
    let mut addressed = false;
    if let Some(v) = o.get("key") {
        let Some(s) = v.as_str() else { return false };
        if !s.starts_with(S3_PREFIX) {
            return false;
        }
        addressed = true;
    }
    if let Some(v) = o.get("keys") {
        let Some(list) = v.as_array() else {
            return false;
        };
        // An empty list addresses nothing, so it cannot be evidence that this
        // is a sink batch — the same reason an empty batch is `false`.
        if list.is_empty() {
            return false;
        }
        for e in list {
            let Some(s) = e.as_str() else { return false };
            if !s.starts_with(S3_PREFIX) {
                return false;
            }
        }
        addressed = true;
    }
    if let Some(v) = o.get("prefix") {
        let Some(s) = v.as_str() else { return false };
        // A prefix SHORTER than `s3:` (`s`, or `""`) would range over the
        // reserved space AND over foreign keys, so it is not a read of the
        // sink's own space and does not qualify. `starts_with` says exactly
        // that: `"s"` does not start with `"s3:"`.
        if !s.starts_with(S3_PREFIX) {
            return false;
        }
        addressed = true;
    }
    addressed
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shape the sink emits, including the two READ shapes that carry
    /// their keys somewhere other than `key` — the trap this predicate exists
    /// to walk around — and the CAS fields the commit protocol rides on.
    #[test]
    fn every_op_shape_the_sink_emits_is_recognised() {
        for body in [
            // the window commit: a CAS put with a required precondition
            br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:sink-a:orders","value":{"tEnd":"2026-09-04T10:00:00Z"},"expect":41,"required":true,"forever":true}]}"#.as_slice(),
            // the intent, written before the upload and retired after it
            br#"{"operations":[{"op":"putIfAbsent","ns":"queen-s3","key":"s3:intent:sink-a:orders:41","value":{},"ttlSeconds":3600}]}"#,
            br#"{"operations":[{"op":"delete","ns":"queen-s3","key":"s3:intent:sink-a:orders:41","expect":3}]}"#,
            // the three recovery reads of §4.5
            br#"{"operations":[{"op":"get","ns":"queen-s3","key":"s3:commit:sink-a:orders"}]}"#,
            br#"{"operations":[{"op":"getMany","ns":"queen-s3","keys":["s3:commit:sink-a:orders","s3:positions:sink-a:orders"]}]}"#,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-s3","prefix":"s3:instance:","limit":500}]}"#,
        ] {
            assert!(
                is_sink_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        // and the bare-array root the route also accepts
        assert!(is_sink_only_batch(
            br#"[{"op":"put","ns":"queen-s3","key":"s3:instance:a","value":{},"ttlSeconds":60}]"#
        ));
    }

    /// A real commit is many ops in one batch — the pointer, the checkpoint and
    /// the positions cache travel together, which is the whole reason the sink
    /// uses a batch — and the verdict is over ALL of them. One foreign key is
    /// enough to keep the whole batch on the KV gate: a half-classified request
    /// is one nobody can reason about.
    #[test]
    fn one_foreign_key_in_a_batch_makes_it_foreign() {
        let mut ops: Vec<String> = (0..9)
            .map(|i| {
                format!(
                    r#"{{"op":"put","ns":"queen-s3","key":"s3:positions:sink-a:orders:{i}","value":{{"offset":1}}}}"#
                )
            })
            .collect();
        let clean = format!(r#"{{"operations":[{}]}}"#, ops.join(","));
        assert!(is_sink_only_batch(clean.as_bytes()));

        ops.push(r#"{"op":"put","ns":"queen-s3","key":"app:cache:x","value":1}"#.to_string());
        let dirty = format!(r#"{{"operations":[{}]}}"#, ops.join(","));
        assert!(!is_sink_only_batch(dirty.as_bytes()));
    }

    /// The array case: `getMany` is where a foreign key is easiest to hide,
    /// because the op looks entirely ordinary from outside.
    #[test]
    fn a_getmany_with_one_foreign_element_is_foreign() {
        assert!(!is_sink_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-s3","keys":["s3:commit:a","secrets:root"]}]}"#
        ));
        // an empty list addresses nothing and is not evidence of anything
        assert!(!is_sink_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-s3","keys":[]}]}"#
        ));
        // a non-string element is not a key this proxy can judge
        assert!(!is_sink_only_batch(
            br#"{"operations":[{"op":"getMany","ns":"queen-s3","keys":["s3:commit:a",7]}]}"#
        ));
    }

    /// The namespace is half the identity: the KV primary key is
    /// `(tenant_id, namespace, key)`, so the same spelling in another namespace
    /// is a different row and is nobody's bookkeeping but the caller's.
    #[test]
    fn a_foreign_namespace_is_foreign() {
        for body in [
            br#"{"operations":[{"op":"put","ns":"other","key":"s3:commit:a","value":1}]}"#
                .as_slice(),
            // ...including one that merely looks like it
            br#"{"operations":[{"op":"put","ns":"queen-s3x","key":"s3:commit:a","value":1}]}"#,
            // ...and the NEIGHBOURING facade's, which has its own carve-out and
            // must not be reachable through this one
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"s3:commit:a","value":1}]}"#,
            // ...and a missing or non-string one
            br#"{"operations":[{"op":"put","key":"s3:commit:a","value":1}]}"#,
            br#"{"operations":[{"op":"put","ns":7,"key":"s3:commit:a","value":1}]}"#,
        ] {
            assert!(
                !is_sink_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// UNREADABLE IS NOT THE SINK, and neither is EMPTY. The second half is the
    /// one worth writing down: "every op is ours" is vacuously true of no ops,
    /// and an empty array must not be a way to reach `Consume` on a route the
    /// plan does not grant.
    #[test]
    fn an_unreadable_or_empty_batch_is_not_a_sink_batch() {
        for body in [
            b"not json".as_slice(),
            b"",
            b"[]",
            br#"{"operations":[]}"#,
            br#"{"operations":{}}"#,
            br#"{"nope":1}"#,
            b"42",
            b"null",
            br#""s3:commit:a""#,
        ] {
            assert!(
                !is_sink_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// Fail-closed against a shape this proxy has not been told about: an op
    /// with no key field at all, an op whose key field is not a string, and an
    /// op NAME outside the closed set even when its key is in the reserved
    /// space. All three keep the batch on `Gated(Kv, Mixed)`, which is exactly
    /// today's behaviour.
    #[test]
    fn an_op_with_no_recognised_key_field_is_foreign() {
        for body in [
            br#"{"operations":[{"op":"put","ns":"queen-s3","value":1}]}"#.as_slice(),
            br#"{"operations":[{"op":"put","ns":"queen-s3","key":7}]}"#,
            br#"{"operations":[{"op":"getPrefix","ns":"queen-s3","prefix":null}]}"#,
            br#"{"operations":[{"ns":"queen-s3","key":"s3:commit:a"}]}"#,
            // a known key space under an op the sink does not emit
            br#"{"operations":[{"op":"incr","ns":"queen-s3","key":"s3:commit:a","by":1}]}"#,
            br#"{"operations":[{"op":"somethingNew","ns":"queen-s3","key":"s3:commit:a"}]}"#,
            // a non-object element
            br#"{"operations":["s3:commit:a"]}"#,
        ] {
            assert!(
                !is_sink_only_batch(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// One literal covers the sink's whole space, and a key that merely starts
    /// with the same characters does not.
    #[test]
    fn every_s3_family_is_recognised_and_nothing_adjacent_is() {
        for key in [
            "s3:commit:sink-a:orders",
            "s3:intent:sink-a:orders:41",
            "s3:positions:sink-a:orders",
            "s3:instance:sink-a",
            // a family that does not exist yet is covered too, which is the
            // reason the prefix is shared rather than enumerated
            "s3:manifest:sink-a:orders:41",
        ] {
            let body =
                format!(r#"{{"operations":[{{"op":"put","ns":"queen-s3","key":"{key}"}}]}}"#);
            assert!(is_sink_only_batch(body.as_bytes()), "{key}");
        }
        for key in ["s3", "s3commit:a", "s", "", "S3:commit:a", "qk:group:g"] {
            let body =
                format!(r#"{{"operations":[{{"op":"put","ns":"queen-s3","key":"{key}"}}]}}"#);
            assert!(!is_sink_only_batch(body.as_bytes()), "{key}");
        }
    }

    /// A `getPrefix` that would range over foreign keys as well as ours is not
    /// a read of the sink's own space, however much of that space it covers.
    #[test]
    fn a_prefix_wider_than_the_reserved_space_is_foreign() {
        for prefix in ["s", "s3", "", "a"] {
            let body = format!(
                r#"{{"operations":[{{"op":"getPrefix","ns":"queen-s3","prefix":"{prefix}","limit":10}}]}}"#
            );
            assert!(!is_sink_only_batch(body.as_bytes()), "{prefix:?}");
        }
        // the exact boundary is a read of the whole reserved space and nothing
        // else, so it qualifies
        assert!(is_sink_only_batch(
            br#"{"operations":[{"op":"getPrefix","ns":"queen-s3","prefix":"s3:","limit":10}]}"#
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
        // nor are the routes the sink actually consumes through, which carry
        // their own class and must not be moved by a body
        for (m, p) in [
            (Method::GET, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::PUT, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::DELETE, "/api/v1/kv/queen-s3/s3:commit:a"),
            (Method::GET, "/api/v1/kv"),
            (Method::POST, "/api/v1/push"),
            (Method::POST, "/api/v1/fetch"),
            (Method::POST, "/api/v1/partitions/changed"),
        ] {
            assert!(!is_kv_batch(crate::routes::classify(&m, p)), "{m} {p}");
        }
    }

    /// The two carve-outs are disjoint and compose in either order: a sink
    /// batch is invisible to the facade's predicate and vice versa, so chaining
    /// them in the gateway cannot produce a class neither would grant alone.
    #[test]
    fn the_two_carve_outs_do_not_see_each_others_batches() {
        use axum::http::Method;
        let kv = crate::routes::classify(&Method::POST, "/api/v1/kv");
        let sink =
            br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:a","value":1}]}"#;
        let kafka =
            br#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g","value":1}]}"#;

        assert_eq!(effective_class(kv, sink), RouteClass::Consume);
        assert_eq!(
            crate::kafka_kv::effective_class(kv, sink),
            kv,
            "the facade's predicate must not claim a sink batch"
        );
        assert_eq!(
            effective_class(kv, kafka),
            kv,
            "and this one must not claim the facade's"
        );
        // ...and a batch mixing the two namespaces is neither, in both orders.
        let mixed = br#"{"operations":[{"op":"put","ns":"queen-s3","key":"s3:commit:a","value":1},{"op":"put","ns":"queen-kafka","key":"qk:group:g","value":1}]}"#;
        assert_eq!(
            effective_class(crate::kafka_kv::effective_class(kv, mixed), mixed),
            kv
        );
        assert_eq!(
            crate::kafka_kv::effective_class(effective_class(kv, mixed), mixed),
            kv
        );
    }
}
