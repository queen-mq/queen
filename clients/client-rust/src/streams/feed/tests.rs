//! In-process tests of the feed streams (no broker): the chain hash against
//! the JS SDK's, the chain's checks, and the engine's behaviours -- the
//! window, pattern, join, gather, table and event-time tests of the JS
//! feed-unit suite, run on the pipeline the way the runner drives it.

use std::sync::Arc;

use serde_json::{json, Value};

use super::env::Env;
use super::pipeline::Pipeline;
use super::*;
use crate::js;

fn queen() -> Queen {
    Queen::connect_to("http://127.0.0.1:9").unwrap()
}

fn feed() -> FeedStream {
    Stream::from_feed(&queen(), FeedSource::new().queues(["orders"]))
}

use crate::streams::Stream;

fn hash(s: FeedStream) -> String {
    s.to("out").chain_hash().unwrap()
}

fn one(_: &Value) -> Value {
    json!(1)
}

fn yes(_: &Value) -> bool {
    true
}

// ---------------------------------------------------------------- chain hash

#[test]
fn the_chain_hash_is_the_js_one() {
    // Each vector made by the JS SDK (`_compileFeed().chainHash`) for the same chain.
    assert_eq!(hash(feed()), "4f53cda18c2baa0c");
    assert_eq!(
        hash(
            feed()
                .where_("data.country", "=", json!("IT"))
                .map(|m, _| m.clone())
        ),
        "4f53cda18c2baa0c"
    );
    assert_eq!(
        hash(
            feed()
                .window_tumbling(60.0)
                .aggregate(FeedAggregate::new().field("count", one))
        ),
        "f44531ef6270fadf"
    );
    assert_eq!(
        hash(
            feed()
                .window_tumbling(60.0)
                .aggregate(FeedAggregate::new().field("count", one))
                .name("perMinute")
        ),
        "11cc47f117a86612"
    );
    assert_eq!(
        hash(
            feed()
                .window(
                    FeedWindow::tumbling(0.5)
                        .grace_period(1.5)
                        .allowed_lateness("5m")
                        .on_late("include")
                        .idle(30.0)
                )
                .aggregate(FeedAggregate::new().field("sum", one).field("avg", one))
        ),
        "1135d85b5989e1e2"
    );
    assert_eq!(
        hash(
            feed()
                .key_by(|m| m["x"].clone())
                .window_sliding(60.0, 30.0)
                .reduce(Some(json!({ "n": 0 })), |a, _| a)
        ),
        "dc4095953ebd69c9"
    );
    assert_eq!(
        hash(feed().window_session(10.0).reduce(None, |a, _| a)),
        "d50462e9937d209e"
    );
    assert_eq!(
        hash(
            feed()
                .window(
                    FeedWindow::cron("minute")
                        .event_time(|m| m["data"]["at"].clone())
                        .idle("1m")
                )
                .aggregate(FeedAggregate::new().field("n", one))
        ),
        "fd1aa387f9eec087"
    );
    assert_eq!(
        hash(feed().pattern(Pattern::new(yes, yes, "15m").on_timeout(one))),
        "3c3026b89c4f2374"
    );
    assert_eq!(
        hash(
            feed().pattern(
                Pattern::new(yes, yes, "1h")
                    .event_time(|m| m["data"]["at"].clone())
                    .allowed_lateness("20m")
                    .on_late("include")
                    .idle("30s")
                    .on_repeat("restart")
                    .name("unanswered")
            )
        ),
        "83d9e3b88e93c103"
    );
    assert_eq!(
        hash(feed().join(Join::new(yes, yes, "1h").select(|_, _| json!(1)))),
        "3d669bb912429427"
    );
    assert_eq!(
        hash(
            feed().join(
                Join::sides("15m")
                    .side_queue("orders", "orders")
                    .side_queue("payments", "payments")
                    .side_queue("shipments", "shipments")
                    .on_timeout_all(|_, _, _| json!(1))
            )
        ),
        "84b3e27e5b429151"
    );
    assert_eq!(
        hash(
            feed().gather(
                Gather::count("10m", |_, _| json!(2))
                    .key(|m, _| m.clone())
                    .on_repeat("latest")
            )
        ),
        "84e560ea574d65b8"
    );
    assert_eq!(
        hash(
            feed().join_table(
                TableJoin::queues(["settings", "settings", "more"], |m, _| m.clone())
                    .key(|r| r.clone())
            )
        ),
        "672498d1a9687118"
    );
    assert_eq!(
        hash(
            feed()
                .window(FeedWindow::tumbling(3600.0).event_time(|m| m["data"]["at"].clone()))
                .aggregate(FeedAggregate::new().field("pushes", one))
                .map(|v, _| v.clone())
                .pattern(Pattern::new(yes, yes, "90m"))
                .name("busy")
        ),
        "84e656a2a4b69685"
    );
    assert_eq!(
        hash(
            feed()
                .window(FeedWindow::tumbling(3600.0).event_time(|m| m["data"]["at"].clone()))
                .aggregate(FeedAggregate::new().field("pushes", one))
                .pattern(Pattern::new(yes, yes, "90m").feed_clock())
        ),
        "737dee8e88c2abfc"
    );
    assert_eq!(
        hash(
            feed()
                .window(
                    FeedWindow::tumbling(60.0)
                        .event_time(|m| m["data"]["at"].clone())
                        .idle("30s")
                )
                .aggregate(FeedAggregate::new().field("n", one))
                .window_tumbling(3600.0)
                .aggregate(FeedAggregate::new().field("n", |v: &Value| v["n"].clone()))
        ),
        "6428177274e3cd74"
    );
    assert_eq!(
        hash(
            feed()
                .where_("data", "=", json!({ "b": 2, "a": 1 }))
                .window_tumbling(60.0)
                .aggregate(FeedAggregate::new().field("n", one))
                .where_("data.n", "exists", None)
        ),
        "ef2bc3322fd22d47"
    );
    assert_eq!(
        hash(
            feed()
                .gather(
                    Gather::steps("10m")
                        .step("paid", |_, _| true)
                        .step("stocked", |_, _| true)
                        .key(|m, _| m.clone())
                        .on_complete(|_, _| json!(1))
                        .on_timeout(|_, _, _| json!(1))
                )
                .name("runs")
        ),
        "3cc9cc960d9bca8b"
    );
    assert_eq!(
        hash(
            feed()
                .key_by(|m| m.clone())
                .window(
                    FeedWindow::session(900.0)
                        .event_time(|m| m["data"]["at"].clone())
                        .allowed_lateness(1800.0)
                        .grace_period(2.0)
                )
                .aggregate(FeedAggregate::new().field("clicks", one))
        ),
        "58cdb0c3f1bc3ca8"
    );
    assert_eq!(
        hash(
            feed()
                .window_tumbling(60.0)
                .aggregate(FeedAggregate::new().field("count", one))
                .pattern(Pattern::new(yes, yes, "5m").on_match(|_, _| json!(1)))
        ),
        "0cb9abacc002dad3"
    );
    assert_eq!(
        hash(
            feed()
                .window(FeedWindow::tumbling(60.0).event_time(|_| json!(1)))
                .aggregate(FeedAggregate::new().field("n", one))
                .key_by(|_| json!("all"))
                .window_sliding(120.0, 60.0)
                .reduce(Some(json!(0)), |a, _| a)
        ),
        "7782afa66bbd1e6f"
    );
    assert_eq!(
        hash(
            feed().join(
                Join::sides("2d")
                    .side_queue("order", "orders")
                    .side("paid", |_, _| true)
                    .on_repeat("latest")
                    .event_time(|_| json!(1))
                    .idle("1m")
            )
        ),
        "8e36e41c4d119bfa"
    );
}

#[test]
fn the_chain_hash_says_which_steps_take_the_event_time_of_the_step_before() {
    let win = || {
        feed()
            .window(FeedWindow::tumbling(60.0).event_time(|m| m["data"]["at"].clone()))
            .aggregate(FeedAggregate::new().field("n", one))
    };
    assert_ne!(
        hash(win().pattern(Pattern::new(yes, yes, "1m"))),
        hash(win().pattern(Pattern::new(yes, yes, "1m").feed_clock())),
        "inherited event time vs the feed clock"
    );
    let plain = feed()
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().field("n", one));
    let off = feed()
        .window(FeedWindow::tumbling(60.0).feed_clock())
        .aggregate(FeedAggregate::new().field("n", one));
    assert_eq!(
        hash(plain),
        hash(off),
        "eventTime: false on a first step changes nothing"
    );
}

// --------------------------------------------------------------- the checks

fn compile_err(s: FeedStream) -> String {
    match s.compile() {
        Ok(_) => panic!("the chain compiled"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn names_steps_and_refuses_a_name_in_the_wrong_place_or_twice() {
    let s = feed()
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .name("a");
    assert_eq!(s.step_names().unwrap(), ["a"]);
    assert!(compile_err(feed().name("a")).contains("stateful step right before"));
    assert!(compile_err(s.clone().map(|v, _| v.clone()).name("b"))
        .contains("stateful step right before"));
    assert!(compile_err(
        s.clone()
            .window_tumbling(60.0)
            .aggregate(FeedAggregate::new().count("n"))
            .name("a")
            .to("x")
    )
    .contains("named \"a\""));
    assert!(compile_err(feed().name("bad name")).contains("a step name"));
    let unnamed = feed()
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("n"))
        .pattern(Pattern::new(yes, yes, "1m"));
    assert_eq!(unnamed.step_names().unwrap(), ["step1", "step2"]);
}

#[test]
fn refuses_a_chain_it_cannot_run() {
    assert!(
        compile_err(feed().window_tumbling(60.0).map(|m, _| m.clone())).contains("right after it")
    );
    assert!(compile_err(feed().window_tumbling(60.0))
        .contains("needs .reduce() or .aggregate() after it"));
    assert!(
        compile_err(feed().aggregate(FeedAggregate::new().count("n")))
            .contains("requires a preceding window")
    );
    assert!(
        compile_err(feed().to("x").map(|m, _| m.clone())).contains("must be the last in the chain")
    );
    assert!(
        compile_err(feed().join(Join::sides("1m").side_queue("a", "a"))).contains("at least two")
    );
    assert!(compile_err(
        feed().gather(
            Gather::steps("1m")
                .step_queue("a")
                .with_count(|_, _| json!(1))
        )
    )
    .contains("one of the two"));
    assert!(
        compile_err(feed().gather(Gather::steps("1m").without_steps())).contains("one of the two")
    );
    assert!(compile_err(feed().gather(Gather::steps("1m"))).contains("steps names at least one"));
    assert!(
        compile_err(feed().pattern(Pattern::new(yes, yes, "soon"))).contains("invalid duration")
    );
    assert!(
        compile_err(feed().pattern(Pattern::new(yes, yes, "1m").on_repeat("twice")))
            .contains("onRepeat")
    );
    assert!(compile_err(feed().where_("x", "~", json!(1))).contains("unknown op"));
    assert!(compile_err(
        feed()
            .window(FeedWindow::sliding(60.0, 7.0))
            .reduce(None, |a, _| a)
    )
    .contains("integer multiple"));
    assert!(compile_err(
        feed()
            .window(FeedWindow::cron("fortnight"))
            .reduce(None, |a, _| a)
    )
    .contains("every must be one of"));
    assert!(
        compile_err(feed().join_table(TableJoin::new("t*", |m, _| m.clone())))
            .contains("no patterns")
    );
    assert!(compile_err(feed().to(Vec::<String>::new())).contains("name at least one queue"));
}

fn pipeline_err(s: FeedStream) -> String {
    let c = s.to("out").compile().unwrap();
    match Pipeline::new(&c.steps) {
        Ok(_) => panic!("the pipeline was made"),
        Err(e) => e,
    }
}

#[test]
fn refuses_idle_lateness_and_on_late_on_the_feeds_clock() {
    let pattern = |p: Pattern| feed().pattern(p);
    assert!(
        pipeline_err(pattern(Pattern::new(yes, yes, "1m").idle("30s")))
            .contains("idle moves a watermark on")
    );
    assert!(
        pipeline_err(pattern(Pattern::new(yes, yes, "1m").allowed_lateness("1m")))
            .contains("allowedLateness and onLate are for event time")
    );
    assert!(
        pipeline_err(pattern(Pattern::new(yes, yes, "1m").on_late("include")))
            .contains("allowedLateness and onLate are for event time")
    );
    assert!(pipeline_err(
        feed()
            .window(FeedWindow::tumbling(60.0).idle(30.0))
            .aggregate(FeedAggregate::new().count("n"))
    )
    .contains("idle moves a watermark on"));
    let first = || {
        feed()
            .window(FeedWindow::tumbling(60.0).event_time(|m| m["data"]["at"].clone()))
            .aggregate(FeedAggregate::new().count("n"))
    };
    assert!(pipeline_err(
        first()
            .window(FeedWindow::tumbling(3600.0).idle(30.0))
            .aggregate(FeedAggregate::new().count("n"))
    )
    .contains("follows its watermark: put idle on the first step"));
    assert!(pipeline_err(
        first().pattern(
            Pattern::new(yes, yes, "1m")
                .event_time(|m| m["ts"].clone())
                .idle("30s")
        )
    )
    .contains("results of the step before it"));
    assert!(compile_err(
        feed().pattern(
            Pattern::new(yes, yes, "1m")
                .event_time(|m| m["data"]["at"].clone())
                .idle(0.0)
        )
    )
    .contains("idle is a duration above zero"));
    // A window on the feed's clock, as a pattern: lateness above zero and
    // onLate 'include' are for event time.
    let window = |w: FeedWindow| feed().window(w).aggregate(FeedAggregate::new().count("n"));
    assert!(
        pipeline_err(window(FeedWindow::tumbling(60.0).allowed_lateness(30.0)))
            .contains("allowedLateness and onLate are for event time: the tumbling window")
    );
    assert!(
        pipeline_err(window(FeedWindow::tumbling(60.0).on_late("include")))
            .contains("allowedLateness and onLate are for event time: the tumbling window")
    );
    assert!(
        pipeline_err(window(FeedWindow::session(60.0).allowed_lateness("1m")))
            .contains("allowedLateness and onLate are for event time: the session window")
    );
    assert!(pipeline_err(window(
        FeedWindow::sliding(60.0, 30.0).allowed_lateness(10.0)
    ))
    .contains("allowedLateness and onLate are for event time: the sliding window"));
    assert!(
        pipeline_err(window(FeedWindow::cron("minute").on_late("include")))
            .contains("allowedLateness and onLate are for event time: the cron window")
    );
    // The plain values pass: no lateness, late messages dropped.
    let plain = window(
        FeedWindow::tumbling(60.0)
            .allowed_lateness(0.0)
            .on_late("drop"),
    )
    .to("out")
    .compile()
    .unwrap();
    assert!(Pipeline::new(&plain.steps).is_ok());
}

// ------------------------------------------------------------- the engine

const T0: f64 = 1_793_955_600_000.0; // 2026-11-06T09:00:00.000Z
const H0: f64 = 1_759_312_800_000.0; // 2025-10-01T10:00:00.000Z

fn ev(min: f64, sec: f64) -> String {
    js::iso(H0 + min * 60_000.0 + sec * 1000.0)
}

/// The runner's way of driving a pipeline, without a broker: each position
/// closes what is due before and after its messages; a page's end moves the
/// clock to what the read resolved.
struct Sim {
    p: Pipeline,
    clock: f64,
    position: u64,
    offsets: std::collections::HashMap<(String, String), u64>,
    out: Vec<(String, Value, Value)>,
    errors: Vec<String>,
}

impl Sim {
    fn new(s: FeedStream) -> Self {
        let c = s.to("out").compile().unwrap();
        Self {
            p: Pipeline::new(&c.steps).unwrap(),
            clock: 0.0,
            position: 0,
            offsets: Default::default(),
            out: Vec::new(),
            errors: Vec::new(),
        }
    }

    fn collect(&mut self, envs: Vec<Env>) {
        for e in envs {
            self.out.push((
                e.key.clone(),
                e.value().cloned().unwrap_or(Value::Null),
                e.ctx.clone().unwrap_or(Value::Null),
            ));
        }
    }

    fn close(&mut self, t: f64) {
        while let Some((at, when)) = self.p.next_close(t) {
            let took = self.p.take(at, when, super::state::CLOSE_BATCH);
            if let Some(rel) = took.release {
                let cp = self.p.checkpoint();
                match self.p.run_release(at, &rel, when) {
                    Ok((envs, _)) => self.collect(envs),
                    Err(e) => {
                        self.p.restore(&cp);
                        self.errors.push(e);
                    }
                }
                continue;
            }
            for em in &took.emits {
                let cp = self.p.checkpoint();
                match self.p.run_emit(at, em, when) {
                    Ok(envs) => self.collect(envs),
                    Err(e) => {
                        self.p.restore(&cp);
                        self.errors.push(e);
                    }
                }
            }
        }
    }

    /// One transaction of the log, committed at feed time `t`.
    fn tx(&mut self, t: f64, msgs: &[(&str, &str, Value)]) {
        self.position += 1;
        let t = self.clock.max(t);
        self.close(t);
        self.clock = t;
        let mut entries = Vec::new();
        for (q, p, data) in msgs {
            let off = self
                .offsets
                .entry((q.to_string(), p.to_string()))
                .or_insert(0);
            *off += 1;
            entries.push(Arc::new(json!({
                "position": self.position, "kind": "push", "queue": q, "partition": p, "partitionId": format!("pid-{p}"),
                "offset": *off, "id": format!("m-{}-{}", self.position, off), "transactionId": format!("t-{}-{}", self.position, off),
                "data": data, "ts": js::iso(t), "createdAt": js::iso(t)
            })));
        }
        for e in &entries {
            let cp = self.p.checkpoint();
            match self.p.entry(e, t) {
                Ok(envs) => self.collect(envs),
                Err(err) => {
                    self.p.restore(&cp);
                    self.errors.push(err);
                }
            }
        }
        self.close(t);
    }

    fn push(&mut self, t: f64, q: &str, p: &str, data: Value) {
        self.tx(t, &[(q, p, data)]);
    }

    /// The feed's clock moves on (a read that resolved at `t`).
    fn at(&mut self, t: f64) {
        let t = self.clock.max(t);
        self.clock = t;
        self.close(t);
    }

    fn values(&self) -> Vec<Value> {
        self.out.iter().map(|o| o.1.clone()).collect()
    }
}

fn count() -> FeedAggregate {
    FeedAggregate::new().count("count")
}

#[test]
fn a_tumbling_window_counts_per_entity_across_queues_and_closes_when_the_clock_passes_its_end() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window_tumbling(60.0)
        .aggregate(
            FeedAggregate::new()
                .count("count")
                .field("sum", |v: &Value| v["total"].clone())
                .field("avg", |v: &Value| v["total"].clone()),
        )
        .map(|acc, ctx| {
            let mut o = acc.clone();
            o["key"] = ctx.unwrap()["key"].clone();
            o["window"] = ctx.unwrap()["windowKey"].clone();
            o
        });
    let mut sim = Sim::new(s);
    sim.push(T0 + 1000.0, "orders", "customer-1", json!({ "total": 10 }));
    sim.push(
        T0 + 2000.0,
        "payments",
        "customer-1",
        json!({ "total": 10 }),
    );
    sim.push(T0 + 3000.0, "orders", "customer-2", json!({ "total": 7 }));
    assert!(sim.out.is_empty(), "open until the minute is over");
    sim.push(T0 + 61_000.0, "orders", "customer-3", json!({ "total": 1 }));
    assert_eq!(
        sim.values(),
        [
            json!({ "count": 2, "sum": 20, "avg": 10, "key": "customer-1", "window": "2026-11-06T09:00:00.000Z" }),
            json!({ "count": 1, "sum": 7, "avg": 7, "key": "customer-2", "window": "2026-11-06T09:00:00.000Z" }),
        ],
        "no __avg_ running sums in the result"
    );
    assert_eq!(sim.out[0].0, "customer-1", "a result goes on with its key");
}

#[test]
fn sliding_windows_count_each_message_in_every_window_it_falls_in() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window_sliding(60.0, 30.0)
        .aggregate(count())
        .map(|acc, ctx| json!({ "start": &ctx.unwrap()["windowKey"].as_str().unwrap()[11..19], "count": acc["count"] }));
    let mut sim = Sim::new(s);
    sim.push(T0 + 10_000.0, "q", "k", json!({}));
    sim.push(T0 + 40_000.0, "q", "k", json!({}));
    sim.push(T0 + 200_000.0, "other", "x", json!({}));
    let mine: Vec<Value> = sim
        .out
        .iter()
        .filter(|o| o.0 == "k")
        .map(|o| o.1.clone())
        .collect();
    assert_eq!(
        mine,
        [
            json!({ "start": "08:59:30", "count": 1 }),
            json!({ "start": "09:00:00", "count": 2 }),
            json!({ "start": "09:00:30", "count": 1 }),
        ]
    );
}

#[test]
fn a_session_closes_past_the_gap_and_on_the_clock_and_exactly_gap_later_is_still_in() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window_session(10.0)
        .aggregate(count())
        .map(|acc, ctx| {
            let c = ctx.unwrap();
            json!({ "count": acc["count"], "from": js::num(c["windowStart"].as_f64().unwrap() - T0), "to": js::num(c["windowEnd"].as_f64().unwrap() - T0) })
        });
    let mut sim = Sim::new(s);
    sim.push(T0, "clicks", "u1", json!({}));
    sim.push(T0 + 5000.0, "clicks", "u1", json!({}));
    sim.push(T0 + 30_000.0, "clicks", "u1", json!({}));
    assert_eq!(
        sim.values(),
        [json!({ "count": 2, "from": 0, "to": 15000 })]
    );
    sim.at(T0 + 41_000.0);
    assert_eq!(
        sim.values()[1],
        json!({ "count": 1, "from": 30000, "to": 40000 })
    );

    // 0 and 10 are 10 s apart: one session; 21 is 11 s after 10: the next.
    for grace in [0.0, 2.0] {
        for event_time in [false, true] {
            let mut w = FeedWindow::session(10.0).grace_period(grace);
            if event_time {
                w = w.event_time(|m| m["data"]["at"].clone());
            }
            let s = Stream::from_feed(&queen(), FeedSource::new())
                .window(w)
                .aggregate(count())
                .map(|acc, ctx| json!({ "user": ctx.unwrap()["key"], "count": acc["count"] }));
            let mut sim = Sim::new(s);
            for sec in [0.0, 10.0, 21.0] {
                sim.push(
                    T0 + sec * 1000.0,
                    "clicks",
                    "u1",
                    json!({ "at": ev(0.0, sec) }),
                );
            }
            sim.push(
                T0 + 60_000.0,
                "clicks",
                "other",
                json!({ "at": ev(1.0, 0.0) }),
            );
            let u1: Vec<Value> = sim
                .values()
                .into_iter()
                .filter(|v| v["user"] == "u1")
                .collect();
            assert_eq!(
                u1,
                [
                    json!({ "user": "u1", "count": 2 }),
                    json!({ "user": "u1", "count": 1 })
                ],
                "grace {grace}, event time {event_time}"
            );
        }
    }
}

#[test]
fn cron_windows_follow_the_wall_clock_and_close_in_key_order_at_one_time() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .key_by(|m| m["data"]["country"].clone())
        .window_cron("minute")
        .aggregate(FeedAggregate::new().field("sum", |v: &Value| v["amount"].clone()))
        .map(|acc, ctx| {
            let c = ctx.unwrap();
            json!({ "country": c["key"], "minute": &c["windowKey"].as_str().unwrap()[11..16], "sum": acc["sum"] })
        });
    let mut sim = Sim::new(s);
    sim.push(
        T0 + 59_000.0,
        "sales",
        "s1",
        json!({ "country": "IT", "amount": 5 }),
    );
    sim.push(
        T0 + 61_000.0,
        "sales",
        "s2",
        json!({ "country": "IT", "amount": 7 }),
    );
    sim.push(
        T0 + 62_000.0,
        "sales",
        "s3",
        json!({ "country": "FR", "amount": 1 }),
    );
    sim.push(T0 + 125_000.0, "other", "x", json!({ "country": "XX" }));
    assert_eq!(
        sim.values(),
        [
            json!({ "country": "IT", "minute": "09:00", "sum": 5 }),
            json!({ "country": "FR", "minute": "09:01", "sum": 1 }),
            json!({ "country": "IT", "minute": "09:01", "sum": 7 }),
        ]
    );
}

#[test]
fn event_time_windows_close_on_the_watermark_and_drop_what_comes_too_late() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window(FeedWindow::tumbling(60.0).event_time(|m| m["data"]["at"].clone()).allowed_lateness(5.0))
        .aggregate(FeedAggregate::new().field("sum", |v: &Value| v["v"].clone()))
        .map(|acc, ctx| json!({ "minute": &ctx.unwrap()["windowKey"].as_str().unwrap()[11..16], "sum": acc["sum"] }));
    let mut sim = Sim::new(s);
    let at = |sec: f64| js::iso(T0 + sec * 1000.0);
    for (sec, v) in [(10.0, 1), (50.0, 2), (70.0, 3), (20.0, 4), (130.0, 5)] {
        sim.push(T0, "readings", "sensor", json!({ "at": at(sec), "v": v }));
    }
    assert_eq!(
        sim.values(),
        [
            json!({ "minute": "09:00", "sum": 3 }),
            json!({ "minute": "09:01", "sum": 3 })
        ]
    );
    assert_eq!(
        sim.p.late, 1,
        "the reading of 09:00:20 came after the watermark passed it"
    );
    // A window takes allowedLateness as a duration too.
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window(
            FeedWindow::tumbling(600.0)
                .event_time(|m| m["data"]["at"].clone())
                .allowed_lateness("5m"),
        )
        .aggregate(count());
    let mut sim = Sim::new(s);
    for min in [0.0, 1.0, 2.0, 30.0, 31.0, 90.0] {
        sim.push(T0, "r", "s", json!({ "at": ev(min, 0.0) }));
    }
    assert_eq!(sim.values(), [json!({ "count": 3 }), json!({ "count": 2 })]);
}

fn followups() -> Pattern {
    Pattern::new(
        |m| m["queue"] == "orders",
        |m| m["queue"] == "payments",
        "15m",
    )
    .on_match(|o, p| json!({ "order": o["data"]["id"], "paid": p["data"]["amount"] }))
    .on_timeout(|o| json!({ "order": o["data"]["id"], "remind": true }))
}

#[test]
fn an_order_then_its_payment_within_15_minutes_is_a_match_and_exactly_15_minutes_is_in_time() {
    let mut sim = Sim::new(Stream::from_feed(&queen(), FeedSource::new()).pattern(followups()));
    sim.push(T0, "orders", "order-1", json!({ "id": 1 }));
    sim.push(
        T0 + 60_000.0,
        "payments",
        "order-1",
        json!({ "amount": 30 }),
    );
    sim.push(T0 + 70_000.0, "orders", "order-3", json!({ "id": 3 }));
    sim.push(
        T0 + 970_000.0,
        "payments",
        "order-3",
        json!({ "amount": 5 }),
    );
    sim.push(
        T0 + 971_000.0,
        "payments",
        "order-9",
        json!({ "amount": 1 }),
    );
    assert_eq!(
        sim.values(),
        [
            json!({ "order": 1, "paid": 30 }),
            json!({ "order": 3, "paid": 5 })
        ]
    );
    assert_eq!(sim.out[0].0, "order-1");
}

#[test]
fn an_order_with_no_payment_in_time_sends_the_reminder_at_the_deadline() {
    let mut sim = Sim::new(Stream::from_feed(&queen(), FeedSource::new()).pattern(followups()));
    sim.push(T0, "orders", "order-2", json!({ "id": 2 }));
    sim.at(T0 + 900_000.0);
    assert!(sim.out.is_empty(), "not at 15 minutes yet");
    sim.at(T0 + 901_000.0);
    sim.push(
        T0 + 950_000.0,
        "payments",
        "order-2",
        json!({ "amount": 9 }),
    );
    assert_eq!(
        sim.values(),
        [json!({ "order": 2, "remind": true })],
        "a payment after the deadline is no match"
    );
}

#[test]
fn on_repeat_restart_counts_the_deadline_from_the_latest_start() {
    let mut sim = Sim::new(
        Stream::from_feed(&queen(), FeedSource::new()).pattern(followups().on_repeat("restart")),
    );
    sim.push(T0, "orders", "cart-1", json!({ "id": "a" }));
    sim.push(T0 + 600_000.0, "orders", "cart-1", json!({ "id": "b" }));
    sim.at(T0 + 1_000_000.0);
    assert!(sim.out.is_empty(), "the second start moved the deadline");
    sim.at(T0 + 1_501_000.0);
    assert_eq!(sim.values(), [json!({ "order": "b", "remind": true })]);
}

#[test]
fn a_join_pairs_two_messages_in_either_order_and_times_out_the_one_left_alone() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).join(
        Join::new(
            |m| m["queue"] == "orders",
            |m| m["queue"] == "shipments",
            "1h",
        )
        .select(|o, s| json!({ "order": o["data"]["id"], "carrier": s["data"]["carrier"] }))
        .on_timeout(|m, side| json!({ "unshipped": m["data"]["id"], "side": side })),
    );
    let mut sim = Sim::new(s);
    sim.push(T0, "shipments", "order-1", json!({ "carrier": "DHL" }));
    sim.push(T0 + 1000.0, "orders", "order-1", json!({ "id": 1 }));
    sim.push(T0 + 2000.0, "orders", "order-2", json!({ "id": 2 }));
    sim.at(T0 + 2000.0 + 3_601_000.0);
    assert_eq!(
        sim.values(),
        [
            json!({ "order": 1, "carrier": "DHL" }),
            json!({ "unshipped": 2, "side": "left" })
        ]
    );
}

#[test]
fn after_a_flat_map_the_key_sees_each_item_and_the_entity_can_be_a_field() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .flat_map(|m, _| {
            if m["queue"] == "sync" {
                Value::Array(
                    m["data"]["changes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|c| c.get("messageId").is_some())
                        .cloned()
                        .collect(),
                )
            } else {
                json!([m["data"]])
            }
        })
        .pattern(
            Pattern::new(
                |m| m["queue"] == "sync",
                |m| m["queue"] == "delivered",
                "1m",
            )
            .key(|m, v| {
                let v = v.unwrap();
                if m["queue"] == "sync" {
                    v["messageId"].clone()
                } else {
                    v["message"]["id"].clone()
                }
            })
            .on_match(|_, d| json!({ "id": d["data"]["message"]["id"] })),
        );
    let mut sim = Sim::new(s);
    sim.push(
        T0,
        "sync",
        "conv-1",
        json!({ "changes": [{ "messageId": "m1" }, { "messageId": "m2" }, { "other": 1 }] }),
    );
    sim.push(
        T0 + 10.0,
        "delivered",
        "x",
        json!({ "message": { "id": "m2" } }),
    );
    sim.push(T0 + 15.0, "other", "x", json!({}));
    sim.push(
        T0 + 20.0,
        "delivered",
        "x",
        json!({ "message": { "id": "m1" } }),
    );
    assert!(sim.errors.is_empty(), "{:?}", sim.errors);
    let mut ids: Vec<String> = sim
        .values()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, ["m1", "m2"]);

    let mut sim = Sim::new(Stream::from_feed(&queen(), FeedSource::new()).pattern(
        followups().key(|m, _| {
            m["data"]
                .get("id")
                .cloned()
                .unwrap_or_else(|| m["data"]["order"].clone())
        }),
    ));
    sim.push(T0, "orders", "p-a", json!({ "id": "X1" }));
    sim.push(
        T0 + 1000.0,
        "payments",
        "p-b",
        json!({ "order": "X1", "amount": 3 }),
    );
    assert_eq!(sim.values(), [json!({ "order": "X1", "paid": 3 })]);
    assert_eq!(sim.out[0].0, "X1");
}

#[test]
fn a_three_sided_join_pairs_the_sides_in_any_order_and_names_the_missing_ones() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).join(
        Join::sides("15m")
            .side_queue("orders", "orders")
            .side_queue("payments", "payments")
            .side_queue("shipments", "shipments")
            .select_all(|got, key| {
                let mut sides: Vec<String> = got.as_object().unwrap().keys().cloned().collect();
                sides.sort();
                json!({ "order": key, "sides": sides })
            })
            .on_timeout_all(|_, missing, key| json!({ "order": key, "missing": missing })),
    );
    let mut sim = Sim::new(s);
    sim.push(T0 + 1000.0, "shipments", "o1", json!({}));
    sim.push(T0 + 2000.0, "payments", "o1", json!({}));
    sim.push(T0 + 3000.0, "orders", "o2", json!({}));
    sim.push(T0 + 4000.0, "orders", "o1", json!({}));
    assert_eq!(
        sim.values(),
        [json!({ "order": "o1", "sides": ["orders", "payments", "shipments"] })]
    );
    sim.at(T0 + 3000.0 + 15.0 * 60_000.0 + 1.0);
    assert_eq!(
        sim.values()[1],
        json!({ "order": "o2", "missing": ["payments", "shipments"] })
    );
}

#[test]
fn a_gather_of_named_steps_completes_once_and_a_late_duplicate_opens_nothing() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .gather(
            Gather::steps("10m")
                .step("paid", |m, _| m["queue"] == "payments")
                .step("stocked", |m, _| m["queue"] == "stock")
                .key(|m, _| m["data"]["order"].clone())
                .on_complete(|_, key| json!({ "order": key, "ready": true }))
                .on_timeout(|_, missing, key| json!({ "order": key, "missing": missing })),
        )
        .name("runs");
    let mut sim = Sim::new(s);
    sim.push(T0 + 1000.0, "payments", "p", json!({ "order": "A" }));
    sim.push(T0 + 2000.0, "stock", "s", json!({ "order": "A" }));
    assert_eq!(sim.out.len(), 1);
    assert_eq!(
        sim.p.inspect("runs", "A")["value"]["done"],
        json!(true),
        "remembered until its deadline"
    );
    sim.push(T0 + 3000.0, "payments", "p", json!({ "order": "A" }));
    sim.at(T0 + 1000.0 + 10.0 * 60_000.0 + 1000.0);
    sim.push(
        T0 + 1000.0 + 10.0 * 60_000.0 + 1000.0,
        "stock",
        "s",
        json!({ "order": "B" }),
    );
    sim.at(T0 + 1000.0 + 10.0 * 60_000.0 + 2000.0 + 10.0 * 60_000.0);
    assert_eq!(
        sim.values(),
        [
            json!({ "order": "A", "ready": true }),
            json!({ "order": "B", "missing": ["paid"] })
        ]
    );
}

#[test]
fn a_gather_by_count_waits_for_distinct_parts_and_says_how_many_are_missing() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).gather(
        Gather::count("5m", |m, _| m["data"]["of"].clone())
            .key(|m, _| m["data"]["batch"].clone())
            .on_complete(|got, key| {
                let mut parts: Vec<i64> = got
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| p["data"]["n"].as_i64().unwrap())
                    .collect();
                parts.sort();
                json!({ "batch": key, "parts": parts })
            })
            .on_timeout(|_, missing, key| json!({ "batch": key, "missing": missing })),
    );
    let mut sim = Sim::new(s);
    // Two pushes with one transaction id are one part.
    sim.position += 1;
    let mk = |pos: u64, q: &str, p: &str, data: Value, tid: &str| {
        Arc::new(
            json!({ "position": pos, "queue": q, "partition": p, "offset": 1, "transactionId": tid, "data": data, "ts": js::iso(T0 + 1000.0) }),
        )
    };
    let t = T0 + 1000.0;
    let entries = [
        mk(
            1,
            "parts",
            "x",
            json!({ "batch": "b1", "of": 3, "n": 1 }),
            "b1-1",
        ),
        mk(
            2,
            "parts",
            "x",
            json!({ "batch": "b1", "of": 3, "n": 1 }),
            "b1-1",
        ),
        mk(
            3,
            "parts",
            "y",
            json!({ "batch": "b1", "of": 3, "n": 2 }),
            "b1-2",
        ),
        mk(
            4,
            "parts",
            "x",
            json!({ "batch": "b2", "of": 2, "n": 1 }),
            "b2-1",
        ),
        mk(
            5,
            "parts",
            "z",
            json!({ "batch": "b1", "of": 3, "n": 3 }),
            "b1-3",
        ),
    ];
    for e in &entries {
        let envs = sim.p.entry(e, t).unwrap();
        sim.collect(envs);
    }
    assert_eq!(
        sim.values(),
        [json!({ "batch": "b1", "parts": [1, 2, 3] })],
        "the duplicate counted once"
    );
    sim.at(T0 + 1000.0 + 5.0 * 60_000.0 + 1.0);
    assert_eq!(sim.values()[1], json!({ "batch": "b2", "missing": 1 }));
}

#[test]
fn a_message_sees_the_table_as_of_its_own_position() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).join_table(
        TableJoin::new("settings", |m, _| m["data"]["property"].clone())
            .select(|v, row, _| json!({ "id": v.unwrap()["id"], "currency": row.map(|r| r["currency"].clone()).unwrap_or(Value::Null) })),
    );
    let c = s.to("out").compile().unwrap();
    let mut p = Pipeline::new(&c.steps).unwrap();
    let mut pos = 0u64;
    let mut out = Vec::new();
    let mut tx = |p: &mut Pipeline, msgs: Vec<(&str, Value)>| {
        pos += 1;
        // A table row of the transaction counts before its other messages.
        let mut msgs = msgs;
        msgs.sort_by_key(|(q, _)| if *q == "settings" { 0 } else { 1 });
        for (q, data) in msgs {
            let e = Arc::new(
                json!({ "position": pos, "queue": q, "partition": if q == "settings" { "p1" } else { "x" }, "offset": pos, "data": data, "ts": js::iso(T0) }),
            );
            if q == "settings" {
                p.apply_row(&e).unwrap();
            } else {
                for env in p.entry(&e, T0).unwrap() {
                    out.push(env.value().cloned().unwrap());
                }
            }
        }
    };
    tx(&mut p, vec![("settings", json!({ "currency": "EUR" }))]);
    tx(
        &mut p,
        vec![("bookings", json!({ "id": 1, "property": "p1" }))],
    );
    tx(&mut p, vec![("settings", json!({ "currency": "CHF" }))]);
    tx(
        &mut p,
        vec![("bookings", json!({ "id": 2, "property": "p1" }))],
    );
    tx(
        &mut p,
        vec![
            ("bookings", json!({ "id": 3, "property": "p1" })),
            ("settings", json!({ "currency": "GBP" })),
        ],
    );
    tx(&mut p, vec![("settings", Value::Null)]);
    tx(
        &mut p,
        vec![("bookings", json!({ "id": 4, "property": "p1" }))],
    );
    tx(
        &mut p,
        vec![("bookings", json!({ "id": 5, "property": "p2" }))],
    );
    assert_eq!(
        out,
        [
            json!({ "id": 1, "currency": "EUR" }),
            json!({ "id": 2, "currency": "CHF" }),
            json!({ "id": 3, "currency": "GBP" }),
            json!({ "id": 4, "currency": null }),
            json!({ "id": 5, "currency": null }),
        ]
    );
    // missing: 'drop' drops a message with no row.
    let s = Stream::from_feed(&queen(), FeedSource::new()).join_table(
        TableJoin::new("settings", |m, _| m["data"]["property"].clone()).missing("drop"),
    );
    let c = s.to("out").compile().unwrap();
    let mut p = Pipeline::new(&c.steps).unwrap();
    let e = Arc::new(
        json!({ "position": 1, "queue": "bookings", "partition": "x", "data": { "property": "nope" }, "ts": js::iso(T0) }),
    );
    assert!(p.entry(&e, T0).unwrap().is_empty());
}

#[test]
fn a_windows_results_feed_a_second_window_and_a_pattern() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window_tumbling(60.0)
        .aggregate(count())
        .name("perKey")
        .key_by(|_| json!("all"))
        .window_tumbling(120.0)
        .aggregate(
            FeedAggregate::new()
                .field("total", |v: &Value| v["count"].clone())
                .count("keys"),
        )
        .name("total");
    let mut sim = Sim::new(s);
    sim.push(T0 + 1000.0, "orders", "c1", json!({}));
    sim.push(T0 + 1000.0, "orders", "c1", json!({}));
    sim.push(T0 + 1000.0, "orders", "c2", json!({}));
    sim.push(T0 + 61_000.0, "orders", "c1", json!({}));
    sim.push(T0 + 121_000.0, "orders", "c3", json!({}));
    assert_eq!(sim.values(), [json!({ "total": 3, "keys": 2 })]);
    assert_eq!(sim.out[0].0, "all");

    // A burst per tenant, then calm: a window's results are the pattern's messages.
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .window_tumbling(60.0)
        .aggregate(count())
        .pattern(
            Pattern::new(|m| m["data"]["count"].as_i64().unwrap_or(0) >= 2, |m| m["data"]["count"].as_i64().unwrap_or(0) < 2, "5m")
                .on_match(|s, c| json!({ "tenant": s["partition"], "from": s["data"]["count"], "to": c["data"]["count"] })),
        );
    let mut sim = Sim::new(s);
    sim.push(T0 + 6000.0, "clicks", "t1", json!({}));
    sim.push(T0 + 7000.0, "clicks", "t1", json!({}));
    sim.push(T0 + 61_000.0, "clicks", "t1", json!({}));
    sim.push(T0 + 121_000.0, "clicks", "x", json!({}));
    assert_eq!(
        sim.values(),
        [json!({ "tenant": "t1", "from": 2, "to": 1 })]
    );
}

fn unanswered(extra: impl Fn(Pattern) -> Pattern) -> FeedStream {
    let p = Pattern::new(|m| m["queue"] == "issues", |m| m["queue"] == "comments", "1h")
        .key(|m, _| json!(format!("{}#{}", m["partition"].as_str().unwrap(), m["data"]["n"])))
        .event_time(|m| m["data"]["at"].clone())
        .on_match(|s, t| json!({ "issue": format!("{}#{}", s["partition"].as_str().unwrap(), s["data"]["n"]), "answered": &t["data"]["at"].as_str().unwrap()[11..16] }))
        .on_timeout(|s| json!({ "issue": format!("{}#{}", s["partition"].as_str().unwrap(), s["data"]["n"]), "unanswered": true }));
    Stream::from_feed(&queen(), FeedSource::new())
        .pattern(extra(p))
        .name("unanswered")
}

#[test]
fn history_pushed_all_at_once_times_out_by_the_event_times() {
    let mut sim = Sim::new(unanswered(|p| p));
    let at = |m: f64| json!({ "at": ev(m, 0.0) });
    let push = |sim: &mut Sim, q: &str, r: &str, n: i64, min: f64| {
        let mut d = at(min);
        d["n"] = json!(n);
        sim.push(T0, q, r, d);
    };
    push(&mut sim, "issues", "r1", 1, 0.0);
    push(&mut sim, "issues", "r1", 2, 10.0);
    push(&mut sim, "issues", "r2", 1, 20.0);
    push(&mut sim, "comments", "r1", 1, 30.0);
    push(&mut sim, "comments", "r2", 1, 85.0); // 65 minutes after its issue
    push(&mut sim, "issues", "r3", 1, 120.0);
    assert_eq!(
        sim.values(),
        [
            json!({ "issue": "r1#1", "answered": "10:30" }),
            json!({ "issue": "r1#2", "unanswered": true }),
            json!({ "issue": "r2#1", "unanswered": true }),
        ]
    );
    let m = sim.p.step_metrics(sim.clock);
    assert_eq!(m[0]["clock"], json!("event"));
    assert_eq!(
        m[0]["watermark"],
        json!(ev(120.0, 0.0)),
        "the watermark is the latest event time"
    );
    assert_eq!(
        m[0]["open"],
        json!(1),
        "r3#1 waits: its hour has not passed"
    );
}

#[test]
fn a_message_out_of_order_by_less_than_the_lateness_takes_its_place() {
    let load = |sim: &mut Sim| {
        for (q, r, n, min) in [
            ("issues", "r1", 1, 0.0),
            ("issues", "r9", 1, 45.0),
            ("comments", "r1", 1, 30.0),
            ("issues", "r9", 2, 120.0),
        ] {
            sim.push(T0, q, r, json!({ "n": n, "at": ev(min, 0.0) }));
        }
    };
    let mut held = Sim::new(unanswered(|p| p.allowed_lateness("20m")));
    load(&mut held);
    assert_eq!(
        held.values(),
        [json!({ "issue": "r1#1", "answered": "10:30" })],
        "the comment waited for its place"
    );
    assert_eq!(held.p.late, 0);
    assert_eq!(
        held.p.step_metrics(held.clock)[0]["held"],
        json!(1),
        "r9#2 (12:00) waits for the watermark (11:40)"
    );

    let mut strict = Sim::new(unanswered(|p| p));
    load(&mut strict);
    assert_eq!(
        strict.values(),
        [
            json!({ "issue": "r1#1", "unanswered": true }),
            json!({ "issue": "r9#1", "unanswered": true })
        ],
        "without lateness the comment came after the watermark: dropped"
    );
    assert_eq!(strict.p.late, 1);

    let mut include = Sim::new(unanswered(|p| p.on_late("include")));
    load(&mut include);
    assert_eq!(
        include.values(),
        [
            json!({ "issue": "r1#1", "answered": "10:30" }),
            json!({ "issue": "r9#1", "unanswered": true })
        ],
        "onLate: 'include' takes the late comment at once"
    );
    assert_eq!(include.p.late, 1);
}

#[test]
fn a_join_and_a_gather_on_event_time_count_their_deadlines_on_event_time() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).join(
        Join::new(|m| m["queue"] == "orders", |m| m["queue"] == "payments", "15m")
            .event_time(|m| m["data"]["at"].clone())
            .allowed_lateness("5m")
            .select(|l, r| {
                let secs = (js::parse_date(r["data"]["at"].as_str().unwrap()).unwrap() - js::parse_date(l["data"]["at"].as_str().unwrap()).unwrap()) / 1000.0;
                json!({ "order": l["partition"], "paidAfterSec": js::num(secs) })
            })
            .on_timeout(|m, side| json!({ "order": m["partition"], "missing": if side == "left" { "payment" } else { "order" } })),
    );
    let mut sim = Sim::new(s);
    sim.push(T0, "payments", "o1", json!({ "at": ev(5.0, 0.0) }));
    sim.push(T0, "orders", "o1", json!({ "at": ev(0.0, 0.0) }));
    sim.push(T0, "orders", "o2", json!({ "at": ev(10.0, 0.0) }));
    sim.push(T0, "orders", "o3", json!({ "at": ev(40.0, 0.0) }));
    assert_eq!(
        sim.values(),
        [
            json!({ "order": "o1", "paidAfterSec": 300 }),
            json!({ "order": "o2", "missing": "payment" })
        ]
    );

    let s = Stream::from_feed(&queen(), FeedSource::new()).gather(
        Gather::steps("10m")
            .step_queue("a")
            .step_queue("b")
            .step_queue("c")
            .key(|m, _| json!(js::string_of(m["data"].get("run"))))
            .event_time(|m| m["data"]["at"].clone())
            .allowed_lateness("15m")
            .on_complete(|got, run| json!({ "run": run, "done": true, "first": &got["a"]["data"]["at"].as_str().unwrap()[11..16] }))
            .on_timeout(|_, missing, run| json!({ "run": run, "missing": missing })),
    );
    let mut sim = Sim::new(s);
    for (q, run, min) in [
        ("c", json!(1), 9.0),
        ("a", json!(1), 0.0),
        ("b", json!(1), 5.0),
        ("a", json!(2), 20.0),
        ("b", json!(2), 25.0),
        ("c", json!(2), 31.0),
        ("a", json!("zz"), 50.0),
    ] {
        sim.push(T0, q, "x", json!({ "run": run, "at": ev(min, 0.0) }));
    }
    assert_eq!(
        sim.values(),
        [
            json!({ "run": "1", "done": true, "first": "10:00" }),
            json!({ "run": "2", "missing": ["c"] })
        ]
    );
}

#[test]
fn a_session_on_event_time_takes_its_messages_in_event_time_order() {
    let s = Stream::from_feed(&queen(), FeedSource::new())
        .key_by(|m| m["data"]["user"].clone())
        .window(
            FeedWindow::session(900.0)
                .event_time(|m| m["data"]["at"].clone())
                .allowed_lateness(1800.0),
        )
        .aggregate(FeedAggregate::new().count("clicks"))
        .map(|v, ctx| {
            let c = ctx.unwrap();
            json!({
                "user": c["key"], "clicks": v["clicks"],
                "from": &js::iso(c["windowStart"].as_f64().unwrap())[11..16],
                "to": &js::iso(c["windowEnd"].as_f64().unwrap())[11..16]
            })
        });
    let mut sim = Sim::new(s);
    for min in [0.0, 20.0, 10.0] {
        sim.push(
            T0,
            "clicks",
            "p",
            json!({ "user": "u", "at": ev(min, 0.0) }),
        );
    }
    sim.push(
        T0,
        "clicks",
        "p",
        json!({ "user": "v", "at": ev(120.0, 0.0) }),
    );
    assert_eq!(
        sim.values(),
        [json!({ "user": "u", "clicks": 3, "from": "10:00", "to": "10:35" })]
    );
}

fn busy_hours(off: bool) -> FeedStream {
    let mut p = Pattern::new(
        |m| m["data"]["pushes"].as_i64().unwrap_or(0) >= 3,
        |m| m["data"]["pushes"].as_i64().unwrap_or(0) >= 3,
        "90m",
    )
    .on_match(
        |a, c| json!({ "repo": a["partition"], "busy": [a["data"]["hour"], c["data"]["hour"]] }),
    )
    .on_timeout(|a| json!({ "repo": a["partition"], "alone": a["data"]["hour"] }))
    .name("busy");
    if off {
        p = p.feed_clock();
    }
    Stream::from_feed(&queen(), FeedSource::new())
        .window(FeedWindow::tumbling(3600.0).event_time(|m| m["data"]["at"].clone()))
        .aggregate(FeedAggregate::new().count("pushes"))
        .map(|v, ctx| {
            let mut o = v.clone();
            o["hour"] = json!(&ctx.unwrap()["windowKey"].as_str().unwrap()[11..13]);
            o
        })
        .pattern(p)
}

#[test]
fn a_step_after_a_step_on_event_time_runs_on_the_results_event_times() {
    let load = |sim: &mut Sim| {
        for min in [
            5.0, 10.0, 15.0, 60.0, 90.0, 105.0, 180.0, 190.0, 200.0, 210.0,
        ] {
            sim.push(T0, "pushes", "r1", json!({ "at": ev(min, 0.0) }));
        }
        sim.push(T0, "pushes", "r2", json!({ "at": ev(310.0, 0.0) }));
    };
    let mut sim = Sim::new(busy_hours(false));
    load(&mut sim);
    assert_eq!(
        sim.values(),
        [
            json!({ "repo": "r1", "busy": ["10", "11"] }),
            json!({ "repo": "r1", "alone": "11" })
        ]
    );
    let steps = sim.p.step_metrics(sim.clock);
    assert_eq!(steps[0]["clock"], json!("event"));
    assert_eq!(steps[1]["clock"], json!("event"));
    assert_eq!(
        steps[1]["watermark"],
        json!(ev(310.0, 0.0)),
        "the watermark of the window step"
    );

    let mut sim = Sim::new(busy_hours(true));
    load(&mut sim);
    assert_eq!(
        sim.values(),
        [
            json!({ "repo": "r1", "busy": ["10", "11"] }),
            json!({ "repo": "r1", "busy": ["11", "13"] })
        ],
        "eventTime: false: the results came within 90 minutes of each other on the feed's clock"
    );
}

#[test]
fn with_idle_a_quiet_feed_moves_the_watermark_and_without_it_nothing_does() {
    let sums = |idle: Option<&str>| {
        let mut w = FeedWindow::tumbling(60.0).event_time(|m| m["data"]["at"].clone());
        if let Some(i) = idle {
            w = w.idle(i);
        }
        Stream::from_feed(&queen(), FeedSource::new())
            .window(w)
            .aggregate(FeedAggregate::new().field("sum", |v: &Value| v["v"].clone()))
            .map(|acc, ctx| json!({ "minute": &ctx.unwrap()["windowKey"].as_str().unwrap()[11..16], "sum": acc["sum"] }))
    };
    for idle in [None, Some("30s")] {
        let mut sim = Sim::new(sums(idle));
        sim.push(
            T0 + 1000.0,
            "readings",
            "s1",
            json!({ "at": ev(0.0, 10.0), "v": 1 }),
        );
        sim.push(
            T0 + 1000.0,
            "readings",
            "s1",
            json!({ "at": ev(0.0, 50.0), "v": 2 }),
        );
        // 35 s after the last message: the watermark moved 5 s past 10:00:50.
        sim.at(T0 + 36_000.0);
        assert!(sim.out.is_empty());
        // 41 s after: past 10:01:00, the minute closes (with idle).
        sim.at(T0 + 42_000.0);
        if idle.is_some() {
            assert_eq!(sim.values(), [json!({ "minute": "10:00", "sum": 3 })]);
            assert_eq!(
                sim.p.step_metrics(sim.clock)[0]["watermark"],
                json!(ev(1.0, 1.0)),
                "10:00:50 + 11 s"
            );
            assert!(sim.p.needs_feed_clock());
        } else {
            sim.at(T0 + 600_000.0);
            assert!(
                sim.out.is_empty(),
                "without idle only a message moves the watermark"
            );
            assert!(!sim.p.needs_feed_clock());
        }
    }
}

#[test]
fn a_finished_import_releases_what_it_held_at_the_pace_of_the_feeds_clock() {
    let mut sim = Sim::new(unanswered(|p| {
        p.within_ms_for_test("20s")
            .idle("30s")
            .allowed_lateness("10s")
    }));
    sim.push(
        T0 + 1000.0,
        "issues",
        "r1",
        json!({ "n": 1, "at": ev(0.0, 0.0) }),
    );
    sim.push(
        T0 + 1000.0,
        "issues",
        "r1",
        json!({ "n": 2, "at": ev(0.0, 5.0) }),
    );
    sim.push(
        T0 + 1000.0,
        "comments",
        "r1",
        json!({ "n": 1, "at": ev(0.0, 12.0) }),
    );
    assert!(sim.out.is_empty());
    assert_eq!(sim.p.step_metrics(sim.clock)[0]["held"], json!(2));
    sim.at(T0 + 1000.0 + 45_000.0);
    assert_eq!(
        sim.values(),
        [json!({ "issue": "r1#1", "answered": "10:00" })]
    );
    sim.at(T0 + 1000.0 + 54_000.0);
    assert_eq!(
        sim.values()[1],
        json!({ "issue": "r1#2", "unanswered": true })
    );
}

#[test]
fn a_message_a_pattern_ignores_needs_no_time_and_an_invalid_time_fails_its_unit() {
    let s = Stream::from_feed(&queen(), FeedSource::new()).pattern(
        Pattern::new(
            |m| m["queue"] == "issues",
            |m| m["queue"] == "comments",
            "1h",
        )
        .key(|m, _| {
            json!(format!(
                "{}#{}",
                m["partition"].as_str().unwrap(),
                m["data"]["n"]
            ))
        })
        .allowed_lateness("10m")
        .event_time(|m| m["data"]["at"].clone())
        .on_match(|s, _| json!({ "answered": s["data"]["n"] }))
        .on_timeout(|s| json!({ "unanswered": s["data"]["n"] })),
    );
    let mut sim = Sim::new(s);
    sim.push(T0, "issues", "r1", json!({ "n": 1, "at": ev(0.0, 0.0) }));
    sim.push(T0, "audit", "x", json!({ "who": "bot" }));
    sim.push(
        T0,
        "audit",
        "x",
        json!({ "at": ev(60.0 * 24.0 * 365.0, 0.0) }),
    );
    sim.push(T0, "comments", "r1", json!({ "n": 1, "at": ev(5.0, 0.0) }));
    sim.push(T0, "issues", "r2", json!({ "n": 1, "at": ev(30.0, 0.0) }));
    assert!(sim.errors.is_empty(), "{:?}", sim.errors);
    assert_eq!(sim.p.late, 0);
    assert_eq!(sim.values(), [json!({ "answered": 1 })]);
    // An issue with no time fails its unit (the runner's poison policy meets it).
    sim.push(T0, "issues", "r1", json!({ "n": 2 }));
    assert_eq!(sim.errors.len(), 1);
    assert!(
        sim.errors[0].contains("eventTime gave no time"),
        "{}",
        sim.errors[0]
    );
}

#[test]
fn a_live_read_lists_what_a_step_keeps_not_the_messages_it_holds_for_order() {
    let mut sim = Sim::new(unanswered(|p| p.allowed_lateness("1h")));
    sim.push(T0, "issues", "r1", json!({ "n": 1, "at": ev(0.0, 0.0) }));
    sim.push(T0, "issues", "r2", json!({ "n": 1, "at": ev(5.0, 0.0) }));
    assert_eq!(
        sim.p.list("unanswered", 100),
        json!({ "items": [], "truncated": false, "total": 0 })
    );
    assert_eq!(sim.p.step_metrics(sim.clock)[0]["held"], json!(2));
    sim.push(T0, "issues", "r3", json!({ "n": 1, "at": ev(62.0, 0.0) }));
    let list = sim.p.list("unanswered", 100);
    assert_eq!(list["total"], json!(1));
    assert_eq!(list["items"][0]["key"], json!("r1#1"));
    assert_eq!(sim.p.inspect("unanswered", "r1#1")["found"], json!(true));
    assert!(sim.p.inspect("nope", "x")["error"]
        .as_str()
        .unwrap()
        .contains("no step named"));
}

#[test]
fn many_windows_that_end_at_one_time_close_in_batches_in_key_order() {
    let c = feed()
        .window_tumbling(60.0)
        .aggregate(count())
        .name("perMinute")
        .to("counts")
        .compile()
        .unwrap();
    let mut p = Pipeline::new(&c.steps).unwrap();
    let n = 100_000;
    p.load_begin();
    let set: Vec<Value> = (0..n)
        .map(|i| json!([format!("w\u{1f}0\u{1f}k{i:07}"), { "key": format!("k{i}"), "start": 0, "end": 60000, "due": 60000, "acc": { "count": 1 } }]))
        .collect();
    p.load_piece(0, "window", &set, &[]);
    p.load_end(None);
    assert_eq!(p.open(), n);
    let mut order = Vec::with_capacity(n);
    assert!(p.advance_sync(60_000.0, &mut |_, em| {
        order.push(em.key.clone());
        true
    }));
    assert_eq!(order.len(), n);
    assert_eq!(p.open(), 0);
    assert!(
        order.iter().enumerate().all(|(i, k)| *k == format!("k{i}")),
        "the heap's order, across batches"
    );
}

#[test]
fn a_loaded_state_of_500000_entries_is_indexed_again_in_batches_with_the_runtime_turning() {
    let c = feed()
        .window_tumbling(60.0)
        .aggregate(count())
        .name("perMinute")
        .to("counts")
        .compile()
        .unwrap();
    let mut p = Pipeline::new(&c.steps).unwrap();
    let n = 500_000;
    p.load_begin();
    let set: Vec<Value> = (0..n)
        .map(|i| json!([format!("w\u{1f}0\u{1f}k{i:07}"), { "key": format!("k{i}"), "start": 0, "end": 60000, "due": 60000, "acc": { "count": 1 } }]))
        .collect();
    p.load_piece(0, "window", &set, &[]);
    // In batches: the cursor stops every REBUILD_BATCH entries.
    let mut probe = Pipeline::new(&c.steps).unwrap();
    probe.load_begin();
    probe.load_piece(0, "window", &set[..120_000], &[]);
    let mut cursor = super::pipeline::RebuildCursor::new(None);
    let mut calls = 1;
    while !probe.load_end_some(&mut cursor, super::state::REBUILD_BATCH) {
        calls += 1;
    }
    assert_eq!(calls, 3, "120,000 entries in batches of 50,000");
    assert_eq!(probe.open(), 120_000);
    // On a runtime of one thread, a task beside the load keeps running.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ticks = rt.block_on(async move {
        let p = std::sync::Mutex::new(p);
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let t2 = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            loop {
                t2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        });
        super::pipeline::load_end_in_batches(&p, None).await;
        ticker.abort();
        assert_eq!(p.lock().unwrap().open(), n);
        let mut order = Vec::new();
        assert!(p.lock().unwrap().advance_sync(60_000.0, &mut |_, em| {
            if order.len() < 3 {
                order.push(em.key.clone());
            }
            true
        }));
        assert_eq!(order, ["k0", "k1", "k2"], "the heap is whole");
        ticks.load(std::sync::atomic::Ordering::SeqCst)
    });
    assert!(
        ticks >= 9,
        "the other task ran between the batches ({ticks} times)"
    );
}

#[test]
fn a_pipelines_metrics_cost_the_same_with_200000_open_windows() {
    let c = feed()
        .window_tumbling(60.0)
        .aggregate(count())
        .to("counts")
        .compile()
        .unwrap();
    let mut p = Pipeline::new(&c.steps).unwrap();
    let n = 200_000;
    p.load_begin();
    let set: Vec<Value> = (0..n)
        .map(|i| json!([format!("w\u{1f}0\u{1f}k{i}"), { "key": format!("k{i}"), "start": 0, "end": 60000, "due": 60000, "acc": { "count": 1 } }]))
        .collect();
    p.load_piece(0, "window", &set, &[]);
    p.load_end(None);
    let t0 = std::time::Instant::now();
    let mut open = 0;
    for _ in 0..2000 {
        open = p.open();
        p.step_metrics(0.0);
    }
    assert_eq!(open, n);
    assert!(
        t0.elapsed().as_millis() < 300,
        "2,000 reads of the metrics took {:?}",
        t0.elapsed()
    );
}

#[test]
fn a_held_message_whose_unit_failed_leaves_nothing_behind_that_releases_the_next_one_early() {
    let run = |with_bad: bool| {
        let s = Stream::from_feed(&queen(), FeedSource::new())
            .flat_map(|m, _| m["data"]["items"].clone())
            .gather(
                Gather::steps("5m")
                    .step("a", |_, v| v.unwrap()["kind"] == "a")
                    .step("b", |_, v| -> std::result::Result<bool, String> {
                        let k = v.unwrap()["kind"].as_str().ok_or("no kind")?;
                        Ok(k.starts_with('b'))
                    })
                    .key(|_, v| v.unwrap()["id"].clone())
                    .event_time(|m| m["data"]["at"].clone())
                    .allowed_lateness("30m")
                    .on_complete(|_, key| json!({ "key": key, "done": true }))
                    .on_timeout(|_, missing, key| json!({ "key": key, "missing": missing })),
            );
        let mut sim = Sim::new(s);
        let items = if with_bad {
            json!([{ "kind": "a", "id": "k1" }, { "id": "bad" }])
        } else {
            json!([])
        };
        sim.tx(
            T0,
            &[
                ("ev", "p", json!({ "at": ev(0.0, 0.0), "items": items })),
                (
                    "ev",
                    "p",
                    json!({ "at": ev(20.0, 0.0), "items": [{ "kind": "a", "id": "k2" }] }),
                ),
            ],
        );
        sim.push(
            T0,
            "ev",
            "p",
            json!({ "at": ev(31.0, 0.0), "items": [{ "kind": "a", "id": "k3" }] }),
        );
        sim.push(
            T0,
            "ev",
            "p",
            json!({ "at": ev(10.0, 0.0), "items": [{ "kind": "b", "id": "k2" }] }),
        );
        sim.push(
            T0,
            "ev",
            "p",
            json!({ "at": ev(200.0, 0.0), "items": [{ "kind": "a", "id": "k9" }] }),
        );
        let k2: Vec<Value> = sim
            .values()
            .into_iter()
            .filter(|v| v["key"] == "k2")
            .collect();
        (k2, sim.errors.len())
    };
    let want = vec![
        json!({ "key": "k2", "missing": ["a"] }),
        json!({ "key": "k2", "missing": ["b"] }),
    ];
    assert_eq!(run(false).0, want);
    let (bad, errors) = run(true);
    assert_eq!(errors, 1);
    assert_eq!(bad, want);
}
