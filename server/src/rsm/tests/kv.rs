//! WP-2.2 — KV on the RSM: the planner and apply against a real store (the
//! [`Cell`] harness: plan a cycle, apply its entry, take a durable point), the
//! semantic cases of `tests/kv_semantics.rs` (024 against Postgres) ported one
//! by one, the receiver's pass 1, the expiry index and the leader sweep, the
//! outcome codec, and I2's determinism.
//!
//! A call goes through [`call`] exactly as the facade makes it: validated by
//! `parse_ops`, planned + applied when it writes, then rendered off the store
//! at `max(wall, last_now)` — so every answer asserted here is the wire answer.

use serde_json::{json, Value};

use crate::rsm::effect::Effect;
use crate::rsm::entry::{
    decode_entry, encode_entry, Entry, KvGot, KvOpOutcome, KvOutcome, KvPrecondition, KvReason,
    KvWrite, Outcome,
};
use crate::rsm::planner::kv::{
    namespaces_of, page_of, parse_ops, precondition_detail, render_call, ts_jsonb, ts_list,
    KvInvalid, MAX_READ_BYTES,
};
use crate::rsm::planner::{KvCommand, KvOp, Plan};
use crate::rsm::store::rows::KvRow;
use crate::rsm::store::{Keyspace, Reads, Store, TypedReads};

use super::planner_harness::{rid, Cell, Cmd, BASE_US, TENANT};

const SEC: i64 = 1_000_000;
/// LMDB's default key ceiling, what the store reports.
const MAX_KEY: usize = 511;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ops_for(tenant: &str, v: &Value) -> Vec<KvOp> {
    parse_ops(v.as_array().expect("an op array"), tenant, false, MAX_KEY)
        .unwrap_or_else(|e| panic!("invalid ops {v}: {e:?}"))
}

fn kv_cmd(id: u64, v: Value) -> Cmd {
    Cmd::Kv(KvCommand {
        request_id: rid(id),
        tenant: TENANT.into(),
        ops: ops_for(TENANT, &v),
    })
}

enum Answer {
    Results(Vec<Value>),
    Precondition(Value),
}

/// One KV call of `tenant`, as the facade makes it.
fn call_as(cell: &mut Cell, tenant: &str, id: u64, v: Value) -> Answer {
    let ops = ops_for(tenant, &v);
    let pre: Vec<KvOpOutcome> = if ops.iter().any(KvOp::is_write) {
        let cmd = Cmd::Kv(KvCommand {
            request_id: rid(id),
            tenant: tenant.into(),
            ops: ops.clone(),
        });
        let cycle = cell.run(&[cmd]);
        let o = match cycle.outcome(0) {
            Outcome::Kv(o) => o,
            other => panic!("a kv call answered {other:?}"),
        };
        if let Some(f) = &o.failed {
            let detail = precondition_detail(&ops, f);
            return Answer::Precondition(serde_json::from_str(&detail).expect("detail JSON"));
        }
        assert_eq!(o.results.len(), ops.len(), "index-aligned (§6.4)");
        o.results
    } else {
        vec![KvOpOutcome::Deferred; ops.len()]
    };
    let wall = cell.now();
    let out = cell
        .node
        .store()
        .read(|r| {
            let now = wall.max(r.last_now_us()?);
            render_call(r, tenant, &ops, &pre, now)
        })
        .expect("render");
    Answer::Results(out)
}

fn results(cell: &mut Cell, id: u64, v: Value) -> Vec<Value> {
    match call_as(cell, TENANT, id, v) {
        Answer::Results(r) => r,
        Answer::Precondition(d) => panic!("unexpected lost precondition: {d}"),
    }
}

fn one(cell: &mut Cell, id: u64, op: Value) -> Value {
    results(cell, id, json!([op])).remove(0)
}

fn applied(r: &Value) -> bool {
    r["applied"] == Value::Bool(true)
}

fn reason(r: &Value) -> &str {
    r["reason"].as_str().unwrap_or("<none>")
}

fn version(r: &Value) -> u64 {
    r["version"].as_u64().unwrap_or(0)
}

fn keys_of(rows: &Value) -> Vec<String> {
    rows.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| r["key"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn row(cell: &Cell, ns: &str, key: &str) -> Option<KvRow> {
    cell.node
        .store()
        .read(|r| r.kv(TENANT, ns, key))
        .expect("read")
}

fn count(cell: &Cell, ks: Keyspace) -> u64 {
    cell.node.store().read(|r| r.count(ks)).expect("count")
}

fn refused(v: Value) -> KvInvalid {
    parse_ops(v.as_array().unwrap(), TENANT, false, MAX_KEY).expect_err("the call must be refused")
}

// ---------------------------------------------------------------------------
// The basics
// ---------------------------------------------------------------------------

#[test]
fn a_put_is_read_back_and_a_second_put_keeps_the_lineage() {
    let mut c = Cell::new("kv-put-get");
    let p1 = one(
        &mut c,
        1,
        json!({"op":"put","ns":"orders","key":"o/1","value":{"n":1},"forever":true}),
    );
    assert!(applied(&p1), "{p1}");
    assert_eq!(p1["op"], "put");
    assert_eq!(
        p1["value"],
        json!({"n":1}),
        "an applied put answers its own value"
    );
    let v1 = version(&p1);
    assert!(v1 > 0, "0 is the version of an absent key");

    let g = one(&mut c, 2, json!({"op":"get","ns":"orders","key":"o/1"}));
    assert_eq!(g["found"], true);
    assert_eq!(g["value"], json!({"n":1}));
    assert_eq!(version(&g), v1);
    assert_eq!(g["expiresAt"], Value::Null, "forever");
    assert!(g["updatedAt"].as_str().unwrap().ends_with("+00:00"));

    let born = row(&c, "orders", "o/1").unwrap().created_at_us;
    c.advance(5 * SEC);
    let p2 = one(
        &mut c,
        3,
        json!({"op":"put","ns":"orders","key":"o/1","value":{"n":2},"ttlSeconds":60}),
    );
    assert!(applied(&p2));
    let v2 = version(&p2);
    assert!(v2 != v1 && v2 > 0);
    let r = row(&c, "orders", "o/1").unwrap();
    assert_eq!(r.created_at_us, born, "a live row keeps its birthday");
    assert_eq!(r.version, v2);
    assert_eq!(r.expires_at_us, Some(r.updated_at_us + 60 * SEC));
    assert_eq!(count(&c, Keyspace::KvExpiry), 1, "the TTL'd row is indexed");

    // A put never inherits the expiry: to forever drops the index row.
    let p3 = one(
        &mut c,
        4,
        json!({"op":"put","ns":"orders","key":"o/1","value":null,"forever":true}),
    );
    assert!(applied(&p3));
    assert_eq!(p3["value"], Value::Null);
    assert_eq!(count(&c, Keyspace::KvExpiry), 0);
    let g = one(&mut c, 5, json!({"op":"get","ns":"orders","key":"o/1"}));
    assert_eq!(g["found"], true, "null is a legal value, not an absent key");
    assert_eq!(g["value"], Value::Null);

    // Plain delete: applied, answers the removed value and version.
    let d = one(&mut c, 6, json!({"op":"delete","ns":"orders","key":"o/1"}));
    assert!(applied(&d));
    assert_eq!(version(&d), version(&p3));
    assert!(row(&c, "orders", "o/1").is_none());
    let d2 = one(&mut c, 7, json!({"op":"delete","ns":"orders","key":"o/1"}));
    assert!(!applied(&d2) && reason(&d2) == "absent" && version(&d2) == 0);
}

#[test]
fn exactly_one_put_if_absent_wins_inside_one_cycle() {
    // 024's row lock, here: the planner is the one serial point, and the
    // overlay makes the second command of the SAME cycle see the first.
    let mut c = Cell::new("kv-race");
    let cmds: Vec<Cmd> = (0..8u64)
        .map(|i| {
            kv_cmd(
                100 + i,
                json!([{"op":"putIfAbsent","ns":"race","key":"order-9f1",
                         "value":{"holder":i},"ttlSeconds":60}]),
            )
        })
        .collect();
    let cycle = c.run(&cmds);
    let outs: Vec<KvWrite> = (0..8)
        .map(|i| match cycle.outcome(i) {
            Outcome::Kv(o) => match &o.results[0] {
                KvOpOutcome::Write(w) => w.clone(),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        })
        .collect();
    let winners: Vec<usize> = (0..8).filter(|i| outs[*i].applied).collect();
    assert_eq!(winners, vec![0], "exactly one winner, the first planned");
    for (i, l) in outs.iter().enumerate().skip(1) {
        assert_eq!(l.reason, Some(KvReason::Exists), "loser {i}");
        assert_eq!(
            l.version, outs[0].version,
            "the loser carries the winner's version"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(l.value.as_ref().expect("value")).unwrap(),
            json!({"holder":0}),
            "and the winner's value (§5.3: no second round trip)"
        );
    }
    // The losers wrote nothing and were not logged.
    assert!(matches!(cycle.plan(1), Ok(Plan::Empty(_))));
    assert_eq!(count(&c, Keyspace::Kv), 1);
}

#[test]
fn expect_positive_never_creates() {
    let mut c = Cell::new("kv-expect");
    let r = one(
        &mut c,
        1,
        json!({"op":"put","ns":"kvexp","key":"ghost","value":{"x":1},
               "ttlSeconds":60,"expect":90101}),
    );
    assert!(!applied(&r) && reason(&r) == "absent", "{r}");
    assert_eq!(version(&r), 0);
    assert_eq!(r["value"], Value::Null);
    assert!(
        row(&c, "kvexp", "ghost").is_none(),
        "§5.3: the pure-UPDATE branch creates nothing"
    );

    let d = one(
        &mut c,
        2,
        json!({"op":"delete","ns":"kvexp","key":"ghost2","expect":90101}),
    );
    assert!(!applied(&d) && reason(&d) == "absent", "{d}");
    assert_eq!(count(&c, Keyspace::Kv), 0);

    // delete expect:0 on an absent key: idempotent success, version 0.
    let d0 = one(
        &mut c,
        3,
        json!({"op":"delete","ns":"kvexp","key":"ghost3","expect":0}),
    );
    assert!(
        applied(&d0) && version(&d0) == 0 && d0["value"] == Value::Null,
        "{d0}"
    );
}

#[test]
fn an_expired_key_reads_as_absent_and_put_if_absent_resurrects_it() {
    let mut c = Cell::new("kv-expired");
    let w = one(
        &mut c,
        1,
        json!({"op":"put","ns":"kvexpiry","key":"k1","value":{"v":1},"ttlSeconds":1}),
    );
    assert!(applied(&w));
    let v0 = version(&w);
    let created0 = row(&c, "kvexpiry", "k1").unwrap().created_at_us;

    c.advance(10 * SEC);
    let g = one(&mut c, 2, json!({"op":"get","ns":"kvexpiry","key":"k1"}));
    assert_eq!(g["found"], false, "§5.7: expired is absent: {g}");
    assert!(
        row(&c, "kvexpiry", "k1").is_some(),
        "the physical row is still there: the predicate is under test, not the sweep"
    );

    let m = one(
        &mut c,
        3,
        json!({"op":"getMany","ns":"kvexpiry","keys":["k1","never-written"]}),
    );
    assert!(keys_of(&m["rows"]).is_empty(), "{m}");
    assert_eq!(
        m["missing"],
        json!(["k1", "never-written"]),
        "absence is a datum"
    );
    assert_eq!(m["truncated"], false);

    let p = one(
        &mut c,
        4,
        json!({"op":"getPrefix","ns":"kvexpiry","prefix":"k"}),
    );
    assert!(keys_of(&p["rows"]).is_empty(), "{p}");

    // A CAS against the dead lineage cannot win either.
    let cas = one(
        &mut c,
        5,
        json!({"op":"put","ns":"kvexpiry","key":"k1","value":1,"ttlSeconds":60,"expect":v0}),
    );
    assert!(!applied(&cas) && reason(&cas) == "absent", "{cas}");

    // Resurrection: expect:0 wins against an expired-but-unpruned row and
    // starts a NEW lineage.
    let r = one(
        &mut c,
        6,
        json!({"op":"putIfAbsent","ns":"kvexpiry","key":"k1","value":{"v":2},"ttlSeconds":60}),
    );
    assert!(applied(&r), "{r}");
    assert_eq!(r["op"], "putIfAbsent");
    assert!(
        version(&r) > v0,
        "a fresh version, never an old one re-issued"
    );
    let created1 = row(&c, "kvexpiry", "k1").unwrap().created_at_us;
    assert!(
        created1 > created0,
        "a resurrected lineage resets created_at"
    );
    // The dead row's index entry went with it.
    assert_eq!(count(&c, Keyspace::KvExpiry), 1);
}

// ---------------------------------------------------------------------------
// incr (§5.4)
// ---------------------------------------------------------------------------

#[test]
fn incr_max_rejects_without_consuming() {
    let mut c = Cell::new("kv-incr-max");
    let op = json!({"op":"incr","ns":"kvquota","key":"acme:2026081712","delta":1,"max":3,
                    "ttlSeconds":60});
    for want in 1..=3i64 {
        let r = one(&mut c, want as u64, op.clone());
        assert!(applied(&r), "incr #{want}: {r}");
        assert_eq!(r["value"].as_i64(), Some(want));
        assert_eq!(r["op"], "incr");
    }
    let over = one(&mut c, 10, op.clone());
    assert!(!applied(&over) && reason(&over) == "limit", "{over}");
    assert_eq!(
        over["value"].as_i64(),
        Some(3),
        "the CURRENT value, never the would-be one"
    );
    let g = one(
        &mut c,
        11,
        json!({"op":"get","ns":"kvquota","key":"acme:2026081712"}),
    );
    assert_eq!(
        g["value"].as_i64(),
        Some(3),
        "a refused incr consumes nothing"
    );
    assert_eq!(
        version(&g),
        version(&over),
        "the refusal answers the current version"
    );
}

#[test]
fn the_first_incr_over_max_is_refused_and_writes_nothing() {
    let mut c = Cell::new("kv-incr-first");
    let r = one(
        &mut c,
        1,
        json!({"op":"incr","ns":"kvfirst","key":"burst","delta":10,"max":5,"ttlSeconds":60}),
    );
    assert!(!applied(&r) && reason(&r) == "limit", "{r}");
    assert_eq!(
        r["value"].as_i64(),
        Some(0),
        "the effective value of an absent counter"
    );
    assert_eq!(version(&r), 0);
    let n = one(
        &mut c,
        2,
        json!({"op":"incr","ns":"kvfirst","key":"burst","delta":-10,"min":0,"ttlSeconds":60}),
    );
    assert!(!applied(&n) && reason(&n) == "limit", "{n}");
    assert_eq!(count(&c, Keyspace::Kv), 0);

    // Decimal arithmetic, trimmed like NUMERIC.
    let a = one(
        &mut c,
        3,
        json!({"op":"incr","ns":"kvfirst","key":"dec","delta":0.1,"forever":true}),
    );
    let b = one(
        &mut c,
        4,
        json!({"op":"incr","ns":"kvfirst","key":"dec","delta":0.2,"forever":true}),
    );
    assert!(applied(&a) && applied(&b));
    assert_eq!(b["value"], json!(0.3));
}

#[test]
fn incr_over_an_expired_non_numeric_row_restarts_and_a_live_one_is_type() {
    let mut c = Cell::new("kv-incr-type");
    one(
        &mut c,
        1,
        json!({"op":"put","ns":"kvtype","key":"c1","value":{"count":0},"ttlSeconds":1}),
    );
    c.advance(10 * SEC);
    let r = one(
        &mut c,
        2,
        json!({"op":"incr","ns":"kvtype","key":"c1","delta":1,"ttlSeconds":60}),
    );
    assert!(
        applied(&r),
        "an expired non-numeric row counts as zero: {r}"
    );
    assert_eq!(reason(&r), "<none>");
    assert_eq!(r["value"].as_i64(), Some(1));

    one(
        &mut c,
        3,
        json!({"op":"put","ns":"kvtype","key":"c2","value":{"count":0},"ttlSeconds":600}),
    );
    let t = one(
        &mut c,
        4,
        json!({"op":"incr","ns":"kvtype","key":"c2","delta":1,"ttlSeconds":60}),
    );
    assert!(!applied(&t) && reason(&t) == "type", "{t}");
    assert_eq!(t["value"].as_i64(), Some(0));
}

#[test]
fn the_ttl_of_incr_is_create_only() {
    let mut c = Cell::new("kv-incr-ttl");
    let a = one(
        &mut c,
        1,
        json!({"op":"incr","ns":"kvwindow","key":"w","delta":1,"ttlSeconds":60}),
    );
    assert!(applied(&a));
    let exp0 = row(&c, "kvwindow", "w").unwrap().expires_at_us;
    assert!(exp0.is_some());
    c.advance(SEC);
    let b = one(
        &mut c,
        2,
        json!({"op":"incr","ns":"kvwindow","key":"w","delta":1,"ttlSeconds":86400}),
    );
    assert_eq!(b["value"].as_i64(), Some(2));
    assert_eq!(
        row(&c, "kvwindow", "w").unwrap().expires_at_us,
        exp0,
        "the window never extends"
    );
    let f = one(
        &mut c,
        3,
        json!({"op":"incr","ns":"kvwindow","key":"w","delta":1,"forever":true}),
    );
    assert!(applied(&f));
    assert_eq!(
        row(&c, "kvwindow", "w").unwrap().expires_at_us,
        exp0,
        "nor becomes immortal"
    );
    // And once the window closes, the next incr opens a new one at 1.
    c.advance(120 * SEC);
    let n = one(
        &mut c,
        4,
        json!({"op":"incr","ns":"kvwindow","key":"w","delta":1,"ttlSeconds":60}),
    );
    assert_eq!(n["value"].as_i64(), Some(1), "{n}");
    assert!(row(&c, "kvwindow", "w").unwrap().expires_at_us > exp0);
}

// ---------------------------------------------------------------------------
// Reads (§5.5)
// ---------------------------------------------------------------------------

#[test]
fn prefix_metacharacters_are_literal_and_pages_come_back_in_byte_order() {
    let mut c = Cell::new("kv-prefix");
    let keys = ["a%b", "a%bc", "axb", "aXb", "a_b", "aQb", "ab", "a%"];
    let ops: Vec<Value> = keys
        .iter()
        .map(|k| json!({"op":"put","ns":"kvprefix","key":k,"value":{"k":k},"ttlSeconds":600}))
        .collect();
    let seeded = results(&mut c, 1, json!(ops));
    assert!(seeded.iter().all(applied));

    let p = one(
        &mut c,
        2,
        json!({"op":"getPrefix","ns":"kvprefix","prefix":"a%b"}),
    );
    assert_eq!(keys_of(&p["rows"]), vec!["a%b", "a%bc"], "'%' is a literal");
    let u = one(
        &mut c,
        3,
        json!({"op":"getPrefix","ns":"kvprefix","prefix":"a_b"}),
    );
    assert_eq!(keys_of(&u["rows"]), vec!["a_b"], "'_' is a literal");
    let all = one(
        &mut c,
        4,
        json!({"op":"getPrefix","ns":"kvprefix","prefix":"a"}),
    );
    let ordered = keys_of(&all["rows"]);
    let mut sorted = ordered.clone();
    sorted.sort();
    assert_eq!(ordered, sorted, "byte order (COLLATE \"C\")");
    assert_eq!(ordered.len(), keys.len());
    assert_eq!(all["truncated"], false);
    assert_eq!(all["nextAfter"], Value::Null);
    // keysOnly carries no value.
    let ko = one(
        &mut c,
        5,
        json!({"op":"getPrefix","ns":"kvprefix","prefix":"a","keysOnly":true,"limit":2}),
    );
    assert!(ko["rows"][0].get("value").is_none(), "{ko}");
    assert_eq!(ko["truncated"], true);
    assert_eq!(ko["nextAfter"], ko["rows"][1]["key"]);
}

#[test]
fn the_read_ceilings_clamp_and_tell_the_truth() {
    let mut c = Cell::new("kv-ceilings");
    for (n, chunk) in (0..1200).collect::<Vec<i32>>().chunks(200).enumerate() {
        let ops: Vec<Value> = chunk
            .iter()
            .map(|i| {
                json!({"op":"put","ns":"kvcap","key":format!("k{i:05}"),
                       "value":{"i":i},"ttlSeconds":600})
            })
            .collect();
        let r = results(&mut c, 100 + n as u64, json!(ops));
        assert!(r.iter().all(applied));
    }
    let big = one(
        &mut c,
        1,
        json!({"op":"getPrefix","ns":"kvcap","prefix":"k","limit":5000}),
    );
    let rows = keys_of(&big["rows"]);
    assert_eq!(rows.len(), 1000, "clamped, never refused");
    assert_eq!(big["truncated"], true);
    assert_eq!(big["nextAfter"].as_str(), rows.last().map(|s| s.as_str()));
    let def = one(
        &mut c,
        2,
        json!({"op":"getPrefix","ns":"kvcap","prefix":"k"}),
    );
    assert_eq!(keys_of(&def["rows"]).len(), 100, "the default limit");

    let p1 = one(
        &mut c,
        3,
        json!({"op":"getPrefix","ns":"kvcap","prefix":"k01","limit":3}),
    );
    let k1 = keys_of(&p1["rows"]);
    assert_eq!(k1, vec!["k01000", "k01001", "k01002"]);
    let p2 = one(
        &mut c,
        4,
        json!({"op":"getPrefix","ns":"kvcap","prefix":"k01","limit":3,"after":p1["nextAfter"]}),
    );
    assert_eq!(
        keys_of(&p2["rows"]),
        vec!["k01003", "k01004", "k01005"],
        "an exclusive cursor"
    );

    // The byte ceiling: 80 values of ~60 KiB are ~4.8 MiB > 4 MiB.
    let blob = "x".repeat(60 * 1024);
    for (n, chunk) in (0..80).collect::<Vec<i32>>().chunks(8).enumerate() {
        let ops: Vec<Value> = chunk
            .iter()
            .map(|i| {
                json!({"op":"put","ns":"kvbytes","key":format!("b{i:03}"),
                       "value":blob,"ttlSeconds":600})
            })
            .collect();
        assert!(results(&mut c, 200 + n as u64, json!(ops))
            .iter()
            .all(applied));
    }
    let fat = one(
        &mut c,
        5,
        json!({"op":"getPrefix","ns":"kvbytes","prefix":"b","limit":1000}),
    );
    let n = fat["rows"].as_array().unwrap().len();
    assert!(n < 80, "a ceiling in BYTES: {n} rows came back");
    assert_eq!(fat["truncated"], true);
    let bytes: i64 = fat["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["value"].to_string().len() as i64)
        .sum();
    assert!(
        bytes <= MAX_READ_BYTES + 65_536,
        "at most one straddling row over"
    );

    // ONE budget per call, spent in 024's apply order: the getMany sorts at
    // (ns, '') — before the getPrefix at (ns, 'b') — so it is served whole
    // and the page gets only what it left.
    let many: Vec<String> = (0..40).map(|i| format!("b{i:03}")).collect();
    let both = results(
        &mut c,
        6,
        json!([
            {"op":"getPrefix","ns":"kvbytes","prefix":"b","limit":1000},
            {"op":"getMany","ns":"kvbytes","keys":many},
        ]),
    );
    assert_eq!(both[1]["truncated"], false);
    assert_eq!(both[1]["rows"].as_array().unwrap().len(), 40);
    let shared = both[0]["rows"].as_array().unwrap().len();
    assert!(
        shared < n,
        "the page shares the call's budget: {shared} vs {n} alone"
    );
    assert_eq!(both[0]["truncated"], true);

    // And a getMany cut by the budget reports the cut keys as NEITHER rows
    // NOR missing.
    let all: Vec<String> = (0..80).map(|i| format!("b{i:03}")).collect();
    let cut = one(&mut c, 7, json!({"op":"getMany","ns":"kvbytes","keys":all}));
    assert_eq!(cut["truncated"], true);
    assert!(cut["rows"].as_array().unwrap().len() < 80);
    assert!(
        cut["missing"].as_array().unwrap().is_empty(),
        "cut keys are not missing"
    );
}

// ---------------------------------------------------------------------------
// CAS and required (§5.3, §6.1 point 5)
// ---------------------------------------------------------------------------

#[test]
fn the_loser_of_a_cas_gets_the_current_value_and_version() {
    let mut c = Cell::new("kv-cas");
    let a = one(
        &mut c,
        1,
        json!({"op":"put","ns":"kvcas","key":"cas","value":{"n":1},"ttlSeconds":600}),
    );
    let v1 = version(&a);
    let born = row(&c, "kvcas", "cas").unwrap().created_at_us;
    c.advance(SEC);
    let lost = one(
        &mut c,
        2,
        json!({"op":"put","ns":"kvcas","key":"cas","value":{"n":2},"ttlSeconds":600,
               "expect":v1 + 777}),
    );
    assert!(!applied(&lost) && reason(&lost) == "version", "{lost}");
    assert_eq!(lost["value"], json!({"n":1}));
    assert_eq!(version(&lost), v1);
    let won = one(
        &mut c,
        3,
        json!({"op":"put","ns":"kvcas","key":"cas","value":{"n":3},"ttlSeconds":600,"expect":v1}),
    );
    assert!(applied(&won));
    let v3 = version(&won);
    let r = row(&c, "kvcas", "cas").unwrap();
    assert_eq!(
        r.created_at_us, born,
        "a CAS is an UPDATE: the lineage keeps its birthday"
    );
    assert!(r.updated_at_us > born);

    let dl = one(
        &mut c,
        4,
        json!({"op":"delete","ns":"kvcas","key":"cas","expect":v1}),
    );
    assert!(!applied(&dl) && reason(&dl) == "version", "{dl}");
    assert_eq!(dl["value"], json!({"n":3}));
    assert_eq!(version(&dl), v3);
    let d0 = one(
        &mut c,
        5,
        json!({"op":"delete","ns":"kvcas","key":"cas","expect":0}),
    );
    assert!(!applied(&d0) && reason(&d0) == "exists", "{d0}");
    let ok = one(
        &mut c,
        6,
        json!({"op":"delete","ns":"kvcas","key":"cas","expect":v3}),
    );
    assert!(applied(&ok));
    assert_eq!(
        ok["value"],
        json!({"n":3}),
        "an applied delete answers what it removed"
    );
    assert_eq!(version(&ok), v3);
    assert!(row(&c, "kvcas", "cas").is_none());
    assert_eq!(count(&c, Keyspace::KvExpiry), 0, "and its index row");
}

#[test]
fn a_lost_required_precondition_aborts_the_whole_call() {
    let mut c = Cell::new("kv-required");
    let b = one(
        &mut c,
        1,
        json!({"op":"put","ns":"saga","key":"b","value":{"b":1},"forever":true}),
    );
    let vb = version(&b);
    let before = c.digest();
    match call_as(
        &mut c,
        TENANT,
        2,
        json!([
            {"op":"put","ns":"saga","key":"a","value":{"a":1},"forever":true},
            {"op":"putIfAbsent","ns":"saga","key":"b","value":{"b":2},"forever":true,
             "required":true},
        ]),
    ) {
        Answer::Precondition(d) => {
            assert_eq!(d["index"], 1);
            assert_eq!(d["op"], "putIfAbsent");
            assert_eq!(d["ns"], "saga");
            assert_eq!(d["key"], "b");
            assert_eq!(d["reason"], "exists");
            assert_eq!(d["version"].as_u64(), Some(vb));
            assert_eq!(
                d["value"],
                json!({"b":1}),
                "the winner's value rides in the DETAIL"
            );
        }
        Answer::Results(r) => panic!("a lost required precondition must abort: {r:?}"),
    }
    assert!(
        row(&c, "saga", "a").is_none(),
        "nothing of the call was written"
    );
    assert_eq!(before, c.digest(), "and nothing was logged");

    // A required write that WINS is an ordinary call.
    let r = results(
        &mut c,
        3,
        json!([{"op":"put","ns":"saga","key":"a","value":1,"forever":true,"required":true}]),
    );
    assert!(applied(&r[0]));
}

#[test]
fn a_get_before_a_write_of_the_same_key_sees_the_old_value() {
    // 024 applies in (ns, key, ordinal) order: a get sorted BEFORE the write
    // reads the old row, one sorted after reads the new.
    let mut c = Cell::new("kv-get-order");
    one(
        &mut c,
        1,
        json!({"op":"put","ns":"ord","key":"k","value":"v1","forever":true}),
    );
    let r = results(
        &mut c,
        2,
        json!([
            {"op":"get","ns":"ord","key":"k"},
            {"op":"put","ns":"ord","key":"k","value":"v2","forever":true},
        ]),
    );
    assert_eq!(r[0]["value"], "v1", "read before the write: {r:?}");
    assert!(applied(&r[1]));
    let r = results(
        &mut c,
        3,
        json!([
            {"op":"put","ns":"ord","key":"k","value":"v3","forever":true},
            {"op":"get","ns":"ord","key":"k"},
            {"op":"getMany","ns":"ord","keys":["k"]},
        ]),
    );
    assert_eq!(r[1]["value"], "v3", "read after the write: {r:?}");
    assert_eq!(r[2]["rows"][0]["value"], "v3", "multi-key reads run last");
    assert_eq!(r[1]["index"], 1);
    assert_eq!(r[2]["index"], 2);
}

// ---------------------------------------------------------------------------
// Pass 1 (the receiver)
// ---------------------------------------------------------------------------

#[test]
fn pass_one_refuses_what_024_refuses() {
    let cases: Vec<(&str, Value, u16, &str)> = vec![
        (
            "no expiry",
            json!([{"op":"put","ns":"n","key":"k","value":1}]),
            400,
            "kv_expiry_not_specified",
        ),
        (
            "two expiries",
            json!([{"op":"put","ns":"n","key":"k","value":1,"ttlSeconds":60,"forever":true}]),
            400,
            "kv_expiry_not_specified",
        ),
        (
            "putIfAbsent no expiry",
            json!([{"op":"putIfAbsent","ns":"n","key":"k","value":1}]),
            400,
            "kv_expiry_not_specified",
        ),
        (
            "incr no expiry",
            json!([{"op":"incr","ns":"n","key":"k","delta":1}]),
            400,
            "kv_expiry_not_specified",
        ),
        (
            "forever false is no declaration",
            json!([{"op":"put","ns":"n","key":"k","value":1,"forever":false}]),
            400,
            "kv_expiry_not_specified",
        ),
        (
            "ttl 0",
            json!([{"op":"put","ns":"n","key":"k","value":1,"ttlSeconds":0}]),
            400,
            "kv_bad_ttl",
        ),
        (
            "ttl negative",
            json!([{"op":"put","ns":"n","key":"k","value":1,"ttlSeconds":-5}]),
            400,
            "kv_bad_ttl",
        ),
        (
            "ttl fractional",
            json!([{"op":"put","ns":"n","key":"k","value":1,"ttlSeconds":1.5}]),
            400,
            "kv_bad_ttl",
        ),
        (
            "ttl string",
            json!([{"op":"put","ns":"n","key":"k","value":1,"ttlSeconds":"60"}]),
            400,
            "kv_bad_ttl",
        ),
        (
            "no value",
            json!([{"op":"put","ns":"n","key":"k","forever":true}]),
            400,
            "kv_bad_request",
        ),
        (
            "bad namespace",
            json!([{"op":"get","ns":"Orders","key":"k"}]),
            400,
            "kv_bad_namespace",
        ),
        (
            "no namespace",
            json!([{"op":"get","key":"k"}]),
            400,
            "kv_bad_namespace",
        ),
        (
            "empty key",
            json!([{"op":"get","ns":"n","key":""}]),
            400,
            "kv_bad_key",
        ),
        (
            "nul key",
            json!([{"op":"get","ns":"n","key":"a\u{0}b"}]),
            400,
            "kv_bad_key",
        ),
        (
            "key over 512",
            json!([{"op":"get","ns":"n","key":"k".repeat(513)}]),
            413,
            "kv_key_too_large",
        ),
        (
            "value over 64 KiB",
            json!([{"op":"put","ns":"n","key":"k","value":"x".repeat(70_000),"forever":true}]),
            413,
            "kv_value_too_large",
        ),
        (
            "putIfAbsent with expect",
            json!([{"op":"putIfAbsent","ns":"n","key":"k","value":1,"forever":true,"expect":42}]),
            400,
            "kv_bad_expect",
        ),
        (
            "expect null",
            json!([{"op":"put","ns":"n","key":"k","value":1,"forever":true,"expect":null}]),
            400,
            "kv_bad_expect",
        ),
        (
            "expect negative",
            json!([{"op":"delete","ns":"n","key":"k","expect":-1}]),
            400,
            "kv_bad_expect",
        ),
        (
            "incr with expect",
            json!([{"op":"incr","ns":"n","key":"k","delta":1,"forever":true,"expect":1}]),
            400,
            "kv_bad_request",
        ),
        (
            "incr without delta",
            json!([{"op":"incr","ns":"n","key":"k","forever":true}]),
            400,
            "kv_bad_request",
        ),
        (
            "incr string max",
            json!([{"op":"incr","ns":"n","key":"k","delta":1,"max":"3","forever":true}]),
            400,
            "kv_bad_request",
        ),
        (
            "required not bool",
            json!([{"op":"delete","ns":"n","key":"k","required":1}]),
            400,
            "kv_bad_request",
        ),
        (
            "unknown op",
            json!([{"op":"scan","ns":"n","key":"k"}]),
            400,
            "kv_unknown_op",
        ),
        ("not an object", json!([7]), 400, "kv_bad_request"),
        (
            "tenant field",
            json!([{"op":"get","ns":"n","key":"k","tenantId":"x"}]),
            400,
            "kv_tenant_not_an_input",
        ),
        (
            "empty prefix",
            json!([{"op":"getPrefix","ns":"n","prefix":""}]),
            400,
            "kv_prefix_required",
        ),
        (
            "getMany keys not array",
            json!([{"op":"getMany","ns":"n","keys":"k"}]),
            400,
            "kv_bad_request",
        ),
        (
            "one write per key",
            json!([
            {"op":"put","ns":"n","key":"k","value":1,"forever":true},
            {"op":"delete","ns":"n","key":"k"}]),
            400,
            "kv_duplicate_key_in_call",
        ),
    ];
    for (what, ops, status, want) in cases {
        let e = refused(ops);
        assert_eq!((e.status, e.reason), (status, want), "{what}: {}", e.detail);
    }

    // Reads are exempt from one-write-per-key.
    assert!(parse_ops(
        json!([
            {"op":"get","ns":"n","key":"k"},
            {"op":"put","ns":"n","key":"k","value":1,"forever":true},
            {"op":"getMany","ns":"n","keys":["k","k"]},
        ])
        .as_array()
        .unwrap(),
        TENANT,
        false,
        MAX_KEY,
    )
    .is_ok());

    // The budgets: ops (256 / 64 in the wire), keys (4096 / 256).
    let many: Vec<Value> = (0..257)
        .map(|i| json!({"op":"get","ns":"n","key":format!("k{i}")}))
        .collect();
    assert_eq!(refused(json!(many)).reason, "kv_too_many_ops");
    let wire: Vec<Value> = (0..65)
        .map(|i| json!({"op":"get","ns":"n","key":format!("k{i}")}))
        .collect();
    let e = parse_ops(&wire, TENANT, true, MAX_KEY).unwrap_err();
    assert_eq!(e.reason, "kv_too_many_ops");
    let keys: Vec<String> = (0..4097).map(|i| format!("k{i}")).collect();
    assert_eq!(
        refused(json!([{"op":"getMany","ns":"n","keys":keys}])).reason,
        "kv_too_many_keys"
    );
    let e = parse_ops(
        json!([{"op":"getPrefix","ns":"n","prefix":"k"}])
            .as_array()
            .unwrap(),
        TENANT,
        true,
        MAX_KEY,
    )
    .unwrap_err();
    assert_eq!(e.reason, "kv_get_prefix_not_allowed_in_transaction");

    // The store's own ceiling: a long tenant and namespace leave less room for
    // the key than 024's 512 bytes — refused at the receiver (413), never a
    // `KeyTooLong` inside apply.
    let tenant = "t".repeat(300);
    let e = parse_ops(
        json!([{"op":"get","ns":"n","key":"k".repeat(300)}])
            .as_array()
            .unwrap(),
        &tenant,
        false,
        MAX_KEY,
    )
    .unwrap_err();
    assert_eq!(
        (e.status, e.reason),
        (413, "kv_key_too_large"),
        "{}",
        e.detail
    );
    // Right at the ceiling it fits.
    let room = MAX_KEY - (tenant.len() + 2) - ("n".len() + 2);
    assert!(parse_ops(
        json!([{"op":"get","ns":"n","key":"k".repeat(room)}])
            .as_array()
            .unwrap(),
        &tenant,
        false,
        MAX_KEY,
    )
    .is_ok());
}

#[test]
fn a_key_at_the_store_ceiling_round_trips() {
    let mut c = Cell::new("kv-long-key");
    let room = MAX_KEY - (TENANT.len() + 2) - ("long".len() + 2);
    let key = "z".repeat(room);
    let w = one(
        &mut c,
        1,
        json!({"op":"put","ns":"long","key":key,"value":1,"ttlSeconds":5}),
    );
    assert!(applied(&w));
    let g = one(
        &mut c,
        2,
        json!({"op":"getPrefix","ns":"long","prefix":"z","after":"z".repeat(room + 40)}),
    );
    assert!(
        keys_of(&g["rows"]).is_empty(),
        "a cursor past every storable key: {g}"
    );
    let g = one(
        &mut c,
        3,
        json!({"op":"getPrefix","ns":"long","prefix":"zz"}),
    );
    assert_eq!(keys_of(&g["rows"]), vec![key.clone()]);
    c.advance(10 * SEC);
    let s = c.run(&[Cmd::KvSweep { id: 9, limit: 8 }]);
    assert!(s.logged);
    assert!(row(&c, "long", &key).is_none());
}

// ---------------------------------------------------------------------------
// The expiry index and the leader sweep (026)
// ---------------------------------------------------------------------------

#[test]
fn the_sweep_prunes_only_what_is_expired_and_the_index_stays_exact() {
    let mut c = Cell::new("kv-sweep");
    results(
        &mut c,
        1,
        json!([
            {"op":"put","ns":"sw","key":"a","value":1,"ttlSeconds":1},
            {"op":"put","ns":"sw","key":"b","value":2,"ttlSeconds":100},
            {"op":"put","ns":"sw","key":"c","value":3,"forever":true},
            {"op":"incr","ns":"sw","key":"n","delta":1,"ttlSeconds":1},
        ]),
    );
    assert_eq!(
        count(&c, Keyspace::KvExpiry),
        3,
        "three rows expire, one is forever"
    );

    // Nothing due yet: the sweep plans nothing and nothing is logged.
    let s0 = c.run(&[Cmd::KvSweep { id: 10, limit: 16 }]);
    assert!(!s0.logged);

    c.advance(2 * SEC);
    let s1 = c.run(&[Cmd::KvSweep { id: 11, limit: 16 }]);
    assert!(s1.logged);
    assert!(row(&c, "sw", "a").is_none() && row(&c, "sw", "n").is_none());
    assert!(row(&c, "sw", "b").is_some() && row(&c, "sw", "c").is_some());
    assert_eq!(count(&c, Keyspace::Kv), 2);
    assert_eq!(count(&c, Keyspace::KvExpiry), 1, "only b is indexed");

    // The limit bounds one step.
    let ops: Vec<Value> = (0..5)
        .map(|i| json!({"op":"put","ns":"sw","key":format!("x{i}"),"value":i,"ttlSeconds":1}))
        .collect();
    results(&mut c, 2, json!(ops));
    c.advance(2 * SEC);
    c.run(&[Cmd::KvSweep { id: 12, limit: 2 }]);
    assert_eq!(count(&c, Keyspace::Kv), 2 + 3);
    c.run(&[Cmd::KvSweep { id: 13, limit: 16 }]);
    assert_eq!(count(&c, Keyspace::Kv), 2);

    // A key rewritten EARLIER IN THE SAME CYCLE is judged against the
    // overlay: the sweep sees the new live version and leaves it alone.
    one(
        &mut c,
        3,
        json!({"op":"put","ns":"sw","key":"d","value":"old","ttlSeconds":1}),
    );
    c.advance(2 * SEC);
    let cycle = c.run(&[
        kv_cmd(
            4,
            json!([{"op":"put","ns":"sw","key":"d","value":"new","ttlSeconds":100}]),
        ),
        Cmd::KvSweep { id: 14, limit: 16 },
    ]);
    assert!(
        matches!(cycle.plan(1), Ok(Plan::Empty(_))),
        "nothing else was due"
    );
    let d = row(&c, "sw", "d").expect("the rewritten row survives the sweep");
    assert_eq!(
        serde_json::from_slice::<Value>(&d.value).unwrap(),
        json!("new")
    );
    assert_eq!(
        count(&c, Keyspace::KvExpiry),
        2,
        "b and the new d: the old entry went"
    );
}

#[test]
fn the_same_calls_produce_the_same_state_on_two_nodes() {
    // I2: apply is a pure function of the entries.
    let run = |tag: &str| {
        let mut c = Cell::new(tag);
        results(
            &mut c,
            1,
            json!([
                {"op":"put","ns":"d","key":"a","value":{"x":[1,2]},"ttlSeconds":5},
                {"op":"incr","ns":"d","key":"n","delta":2.5,"forever":true},
                {"op":"putIfAbsent","ns":"e","key":"z","value":null,"ttlSeconds":1},
            ]),
        );
        c.advance(3 * SEC);
        results(
            &mut c,
            2,
            json!([
                {"op":"delete","ns":"d","key":"a"},
                {"op":"incr","ns":"d","key":"n","delta":-1,"forever":true},
            ]),
        );
        c.run(&[Cmd::KvSweep { id: 50, limit: 8 }]);
        c.digest()
    };
    let a = run("kv-det-a");
    let b = run("kv-det-b");
    assert_eq!(
        a.whole,
        b.whole,
        "first difference: {:?}",
        a.first_difference(&b)
    );
}

// ---------------------------------------------------------------------------
// Tenancy, the console reads
// ---------------------------------------------------------------------------

#[test]
fn tenants_never_see_each_other_and_the_console_reads_count_every_row() {
    let mut c = Cell::new("kv-console");
    results(
        &mut c,
        1,
        json!([
            {"op":"put","ns":"a","key":"x","value":1,"forever":true},
            {"op":"put","ns":"a","key":"y","value":{"big":"yy"},"forever":true},
            {"op":"put","ns":"b","key":"z","value":3,"ttlSeconds":1},
        ]),
    );
    // Another tenant, the same names.
    match call_as(
        &mut c,
        "t2",
        2,
        json!([{"op":"put","ns":"a","key":"x","value":"theirs","forever":true}]),
    ) {
        Answer::Results(r) => assert!(applied(&r[0])),
        Answer::Precondition(d) => panic!("{d}"),
    }
    let g = one(&mut c, 3, json!({"op":"get","ns":"a","key":"x"}));
    assert_eq!(g["value"], 1, "the tenant is part of the key");

    c.advance(5 * SEC);
    let store = c.node.store();
    let now = c.now();
    let ns = store.read(|r| namespaces_of(r, TENANT)).unwrap();
    assert_eq!(
        ns,
        json!([{"namespace":"a","keys":2},{"namespace":"b","keys":1}]),
        "every row, the expired one included"
    );
    let ns2 = store.read(|r| namespaces_of(r, "t2")).unwrap();
    assert_eq!(ns2, json!([{"namespace":"a","keys":1}]));

    let page = store
        .read(|r| {
            page_of(
                r,
                TENANT,
                "b",
                "",
                None,
                100,
                false,
                true,
                now,
                MAX_READ_BYTES,
                ts_list,
            )
        })
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0]["expired"], true);
    let exp = page.rows[0]["expiresAt"].as_str().unwrap();
    assert!(
        exp.ends_with('Z') && exp.len() == "2026-09-22T10:15:30.123456Z".len(),
        "{exp}"
    );
    let hidden = store
        .read(|r| {
            page_of(
                r,
                TENANT,
                "b",
                "",
                None,
                100,
                false,
                false,
                now,
                MAX_READ_BYTES,
                ts_list,
            )
        })
        .unwrap();
    assert!(hidden.rows.is_empty());

    let p1 = store
        .read(|r| {
            page_of(
                r,
                TENANT,
                "a",
                "",
                None,
                1,
                false,
                true,
                now,
                MAX_READ_BYTES,
                ts_list,
            )
        })
        .unwrap();
    assert_eq!(keys_of(&Value::Array(p1.rows.clone())), vec!["x"]);
    assert!(p1.truncated);
    assert_eq!(p1.next_after.as_deref(), Some("x"));
    assert_eq!(p1.bytes, 1);
    let p2 = store
        .read(|r| {
            page_of(
                r,
                TENANT,
                "a",
                "",
                Some("x"),
                1,
                true,
                true,
                now,
                MAX_READ_BYTES,
                ts_list,
            )
        })
        .unwrap();
    assert_eq!(keys_of(&Value::Array(p2.rows.clone())), vec!["y"]);
    assert!(!p2.truncated && p2.next_after.is_none());
    assert!(p2.rows[0].get("value").is_none());
    assert_eq!(p2.bytes, 0, "keysOnly charges nothing");
}

// ---------------------------------------------------------------------------
// The outcome codec
// ---------------------------------------------------------------------------

#[test]
fn the_kv_outcome_round_trips_alone_and_inside_an_entry() {
    let o = Outcome::Kv(KvOutcome {
        results: vec![
            KvOpOutcome::Deferred,
            KvOpOutcome::Got(None),
            KvOpOutcome::Got(Some(KvGot {
                value: b"{\"a\":1}".to_vec(),
                version: 7,
                expires_at_us: Some(BASE_US + 5),
                updated_at_us: BASE_US,
            })),
            KvOpOutcome::Write(KvWrite {
                applied: true,
                reason: None,
                value: None,
                version: 9,
            }),
            KvOpOutcome::Write(KvWrite {
                applied: false,
                reason: Some(KvReason::Type),
                value: Some(b"0".to_vec()),
                version: 3,
            }),
        ],
        failed: None,
    });
    assert_eq!(Outcome::decode(&o.encode()).unwrap(), o);
    let f = Outcome::Kv(KvOutcome {
        results: Vec::new(),
        failed: Some(KvPrecondition {
            index: 2,
            reason: KvReason::Exists,
            version: 11,
            value: Some(b"null".to_vec()),
        }),
    });
    assert_eq!(Outcome::decode(&f.encode()).unwrap(), f);

    let mut e = Entry::new(BASE_US, 1, 40);
    e.add_command(
        rid(1),
        o,
        vec![
            Effect::KvPut {
                tenant: TENANT.into(),
                ns: "n".into(),
                key: "k".into(),
                value: b"1".to_vec(),
                version: 40,
                expires_at_us: None,
                created_at_us: BASE_US,
                updated_at_us: BASE_US,
            },
            Effect::KvDelete {
                tenant: TENANT.into(),
                ns: "n".into(),
                key: "j".into(),
            },
        ],
    )
    .unwrap();
    assert_eq!(e.kinds_version, 1, "a new TAG at catalogue version 1");
    let bytes = encode_entry(&e).expect("encode");
    assert_eq!(decode_entry(&bytes).unwrap(), e);
}

#[test]
fn timestamps_render_like_postgres() {
    // 2026-09-22T10:15:30.123400 UTC.
    let us = 1_790_072_130_123_400i64;
    assert_eq!(ts_jsonb(us), "2026-09-22T10:15:30.1234+00:00");
    assert_eq!(ts_jsonb(us - 123_400), "2026-09-22T10:15:30+00:00");
    assert_eq!(ts_list(us), "2026-09-22T10:15:30.123400Z");
}

/// Jepsen W4 (19 G1c cycles in a no-fault run): two calls planned into ONE
/// entry, each reading the key the other writes. Their reads are rendered at
/// their own positions (`kv_reads`): the first sees nothing of the second,
/// the second sees the first's write — a serial order, never a cycle. Read
/// after the entry had applied (the old path), both saw each other's write.
#[test]
fn a_writing_calls_reads_see_nothing_planned_after_it() {
    let mut c = Cell::new("kv-read-position");
    let (ida, idb) = (0x5734_0001u64, 0x5734_0002u64);
    let a = json!([
        {"op":"getMany","ns":"w4","keys":["y"]},
        {"op":"put","ns":"w4","key":"x","value":"a","forever":true},
    ]);
    let b = json!([
        {"op":"getMany","ns":"w4","keys":["x"]},
        {"op":"put","ns":"w4","key":"y","value":"b","forever":true},
    ]);
    let reads = crate::rsm::kv_reads::global();
    let ra = reads
        .register(rid(ida), TENANT, &ops_for(TENANT, &a))
        .expect("register a");
    let rb = reads
        .register(rid(idb), TENANT, &ops_for(TENANT, &b))
        .expect("register b");
    let cycle = c.run(&[kv_cmd(ida, a.clone()), kv_cmd(idb, b.clone())]);
    assert!(cycle.logged, "both calls write");
    let got_a = ra.take().expect("a rendered at its position").expect("a");
    let got_b = rb.take().expect("b rendered at its position").expect("b");
    assert_eq!(got_a[0]["rows"], json!([]), "a read b's later write: {got_a:?}");
    assert_eq!(got_a[0]["missing"], json!(["y"]));
    assert_eq!(got_b[0]["rows"][0]["value"], "a", "b reads a's write: {got_b:?}");
    assert!(applied(&got_a[1]) && applied(&got_b[1]));
    // Nothing is left waiting once the calls are done.
    drop((ra, rb));
    assert!(reads.call(&rid(ida)).is_none() && reads.call(&rid(idb)).is_none());
}

/// Jepsen W4 pause (G-single): a read-only call returned one key of a two-key
/// batch before the other had applied — the RAM keyspaces are read live, and
/// the read landed between the batch's two effects. A reader at the entry gate
/// (`EntryGate::whole`, what `kv_read` holds) that starts in the middle of the
/// entry waits for apply to finish it, and sees the batch whole.
#[test]
fn a_read_at_the_entry_gate_never_sees_half_a_batch() {
    use std::time::Duration;

    use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify};

    use super::apply::{cfg, seg_opts};

    let batch = |v: &str| {
        json!([
            {"op":"put","ns":"w4","key":"a","value":v,"forever":true},
            {"op":"put","ns":"w4","key":"b","value":v,"forever":true},
        ])
    };
    let mut c = Cell::new("kv-entry-gate");
    assert!(c.run(&[kv_cmd(0x6a7e_0001, batch("old"))]).logged);
    let entry = c
        .plan_entry(&[kv_cmd(0x6a7e_0002, batch("new"))], None)
        .expect("the second batch writes");
    assert!(entry.effects.len() >= 2, "two puts: {:?}", entry.effects);
    let store = c.node.store();
    let (mut a, _) = Applier::open(
        store,
        &c.node.seg_dir(),
        seg_opts(),
        cfg(),
        std::sync::Arc::new(NoNotify),
    )
    .expect("open applier");

    let (go, started) = std::sync::mpsc::channel::<()>();
    let (seen_tx, seen) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(move || {
            started.recv().expect("mid-entry");
            let _whole = store.entry_gate().whole();
            let rows = store
                .read(|r| Ok((r.kv(TENANT, "w4", "a")?, r.kv(TENANT, "w4", "b")?)))
                .expect("read");
            seen_tx.send(rows).expect("seen");
        });
        crate::rsm::faults::set_mid_entry_hook(Some(Box::new(move || {
            let _ = go.send(());
            // Ample for a reader that does not wait to read right here.
            std::thread::sleep(Duration::from_millis(200));
        })));
        let done = a.apply(&ApplyCommitted {
            index: 2,
            term: 1,
            entry,
        });
        crate::rsm::faults::set_mid_entry_hook(None);
        done.expect("apply");
    });
    let (ra, rb) = seen.recv().expect("the reader read");
    let (ra, rb) = (ra.expect("a"), rb.expect("b"));
    assert_eq!(
        ra.value, rb.value,
        "half a batch: a={:?} b={:?}",
        ra.value, rb.value
    );
    assert_eq!(
        ra.value,
        b"\"new\"".to_vec(),
        "the whole entry, after it applied"
    );
}
