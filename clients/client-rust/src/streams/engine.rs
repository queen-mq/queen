//! The pure part of the streaming engine: bucketing, folding, closing.
//!
//! Nothing here does I/O. The runner loads state, calls in, and commits what
//! comes out — which is what makes window semantics testable without a broker.
//!
//! # When a window closes
//!
//! ```text
//! closed  ⇔  window_end + grace  ≤  clock
//! ```
//!
//! and `clock` is the whole difference between the two time modes:
//!
//! * **processing time** — the newest `createdAt` in the batch. Windows close
//!   because messages *arrived*.
//! * **event time** — the partition's watermark, which is
//!   `max(event_time) - allowed_lateness`. Windows close because time *in the
//!   data* moved on, so a replay produces the same windows it did live.

use std::collections::BTreeMap;

use serde_json::Value;

use queen_protocol::{
    parse_state_key, session_state_key, state_key_for, StateOp, WATERMARK_STATE_KEY,
};

use super::ops::{format_iso_ms, Record, Reducer, Window, WindowKind};
use crate::error::{Error, Result};

/// A record with its windowing decided.
#[derive(Debug, Clone)]
pub(crate) struct Envelope {
    pub record: Record,
    pub key: String,
    pub event_time_ms: i64,
    pub window_start: i64,
    pub window_end: i64,
    pub window_key: String,
}

/// A closed window's result.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Emit {
    pub key: String,
    pub window_start: i64,
    pub window_end: i64,
    pub window_key: String,
    pub value: Value,
}

#[derive(Debug, Default)]
pub(crate) struct Outcome {
    pub state_ops: Vec<StateOp>,
    pub emits: Vec<Emit>,
}

/// Monday 00:00 UTC alignment: 1 Jan 1970 was a Thursday, so weeks shift by 4
/// days.
const WEEK_EPOCH_OFFSET_MS: i64 = 4 * 86_400_000;

impl Window {
    /// Which window(s) a record belongs to.
    ///
    /// One envelope for tumbling and cron, `size / slide` for sliding, and for
    /// a session one envelope carrying only the timestamp — a session's bounds
    /// depend on what came before, so they cannot be decided here.
    pub(crate) fn annotate(&self, record: Record, key: String, ts: i64) -> Vec<Envelope> {
        match self.kind {
            WindowKind::Tumbling { seconds } => {
                let size = seconds * 1000;
                let start = floor_div(ts, size) * size;
                vec![Envelope {
                    record,
                    key,
                    event_time_ms: ts,
                    window_start: start,
                    window_end: start + size,
                    window_key: format_iso_ms(start),
                }]
            }
            WindowKind::Cron { every } => {
                let size = every.millis();
                let start = if matches!(every, super::ops::Every::Week) {
                    floor_div(ts - WEEK_EPOCH_OFFSET_MS, size) * size + WEEK_EPOCH_OFFSET_MS
                } else {
                    floor_div(ts, size) * size
                };
                vec![Envelope {
                    record,
                    key,
                    event_time_ms: ts,
                    window_start: start,
                    window_end: start + size,
                    window_key: format_iso_ms(start),
                }]
            }
            WindowKind::Sliding { size, slide } => {
                let size_ms = size * 1000;
                let slide_ms = slide * 1000;
                let latest = floor_div(ts, slide_ms) * slide_ms;
                let count = (size / slide).max(1);
                let mut out = Vec::with_capacity(count as usize);
                for i in 0..count {
                    let start = latest - i * slide_ms;
                    // Guard the edges: a window is only this event's if it
                    // actually covers the timestamp.
                    if ts < start || ts >= start + size_ms {
                        continue;
                    }
                    out.push(Envelope {
                        record: record.clone(),
                        key: key.clone(),
                        event_time_ms: ts,
                        window_start: start,
                        window_end: start + size_ms,
                        window_key: format_iso_ms(start),
                    });
                }
                out
            }
            WindowKind::Session { .. } => vec![Envelope {
                record,
                key,
                event_time_ms: ts,
                window_start: ts,
                window_end: ts,
                window_key: String::new(),
            }],
        }
    }
}

fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

/// One accumulator being built.
struct Acc {
    key: String,
    window_start: i64,
    window_end: i64,
    window_key: String,
    acc: Value,
    touched: bool,
    /// Already in the database, so closing it needs a delete.
    seeded: bool,
}

/// Fold a batch into the windows it belongs to, and close whatever the clock
/// has passed.
///
/// `loaded_state` must hold **every** row for the partition, not just the ones
/// this batch touches: a window that received no traffic in this batch still
/// has to be noticed as closed and flushed, or it would sit there until the
/// partition happened to get another message.
pub(crate) fn run_reduce(
    reducer: &Reducer,
    envelopes: &[Envelope],
    loaded_state: &BTreeMap<String, Value>,
    clock_ms: Option<i64>,
    operator_tag: &str,
    grace_ms: i64,
) -> Outcome {
    let mut accs: BTreeMap<String, Acc> = BTreeMap::new();

    for (state_key, value) in loaded_state {
        let Some(parts) = parse_state_key(state_key) else {
            continue;
        };
        // Reserved internal keys (the watermark) are not windows.
        if parts.operator_tag.starts_with("__") {
            continue;
        }
        // Rows belonging to a different window operator in the same query.
        if !operator_tag.is_empty() && parts.operator_tag != operator_tag {
            continue;
        }
        let ws = value
            .get("windowStart")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| super::ops::parse_iso_ms(&parts.window_key).unwrap_or(0));
        let we = value
            .get("windowEnd")
            .and_then(|v| v.as_i64())
            .unwrap_or(ws);
        accs.insert(
            state_key.clone(),
            Acc {
                key: parts.user_key,
                window_start: ws,
                window_end: we,
                window_key: parts.window_key,
                acc: value.get("acc").cloned().unwrap_or_else(|| value.clone()),
                touched: false,
                seeded: true,
            },
        );
    }

    let mut batch_max_start = i64::MIN;
    for env in envelopes {
        let state_key = state_key_for(operator_tag, &env.window_key, &env.key);
        let entry = accs.entry(state_key).or_insert_with(|| Acc {
            key: env.key.clone(),
            window_start: env.window_start,
            window_end: env.window_end,
            window_key: env.window_key.clone(),
            acc: reducer.initial(),
            touched: false,
            seeded: false,
        });
        let acc = std::mem::replace(&mut entry.acc, Value::Null);
        entry.acc = reducer.step(acc, &env.record);
        entry.touched = true;
        batch_max_start = batch_max_start.max(env.window_start);
    }

    // Without a clock, fall back to the newest window start in the batch. That
    // deliberately keeps the newest window open — closing it would drop events
    // still arriving for it.
    let clock = clock_ms.unwrap_or(batch_max_start);

    let mut out = Outcome::default();
    for (state_key, entry) in accs {
        let closed = entry.window_end + grace_ms <= clock;
        if closed {
            out.emits.push(Emit {
                key: entry.key,
                window_start: entry.window_start,
                window_end: entry.window_end,
                window_key: entry.window_key,
                value: entry.acc,
            });
            if entry.seeded {
                out.state_ops.push(StateOp::Delete { key: state_key });
            }
        } else if entry.touched {
            out.state_ops.push(StateOp::Upsert {
                key: state_key,
                value: serde_json::json!({
                    "acc": entry.acc,
                    "windowStart": entry.window_start,
                    "windowEnd": entry.window_end,
                }),
            });
        }
    }
    out
}

struct Session {
    acc: Value,
    start: Option<i64>,
    last_event: Option<i64>,
    dirty: bool,
    seeded: bool,
    closed: bool,
}

/// Advance per-key session windows.
///
/// A session extends while events keep arriving within `gap` of each other and
/// closes on silence. Passing an empty `envelopes` is the idle-flush case: it
/// closes whatever has gone quiet without consuming anything.
pub(crate) fn run_session(
    window: &Window,
    reducer: &Reducer,
    envelopes: &[Envelope],
    loaded_state: &BTreeMap<String, Value>,
    now_ms: i64,
) -> Outcome {
    let WindowKind::Session { gap } = window.kind else {
        return Outcome::default();
    };
    let gap_ms = gap * 1000;
    let tag = window.tag();
    let mut sessions: BTreeMap<String, Session> = BTreeMap::new();

    for (state_key, value) in loaded_state {
        let parts: Vec<&str> = state_key.split(queen_protocol::STATE_KEY_SEP).collect();
        if parts.len() != 3 || parts[0] != tag || parts[1] != "open" {
            continue;
        }
        sessions.insert(
            parts[2].to_string(),
            Session {
                acc: value
                    .get("acc")
                    .cloned()
                    .unwrap_or_else(|| reducer.initial()),
                start: value.get("sessionStart").and_then(|v| v.as_i64()),
                last_event: value.get("lastEventTime").and_then(|v| v.as_i64()),
                dirty: false,
                seeded: true,
                closed: false,
            },
        );
    }

    let mut out = Outcome::default();

    for env in envelopes {
        let ts = env.event_time_ms;
        let existing = sessions.remove(&env.key);
        let mut s = match existing {
            // A gap longer than the threshold ends the old session and starts
            // a new one; the old one emits right here.
            Some(s) if s.last_event.is_some_and(|last| last + gap_ms < ts) => {
                out.emits.push(build_session_emit(&env.key, &s, gap_ms));
                Session {
                    acc: reducer.initial(),
                    start: Some(ts),
                    last_event: Some(ts),
                    dirty: true,
                    // The row is about to be overwritten, so the emit above
                    // does not also need a delete.
                    seeded: false,
                    closed: false,
                }
            }
            Some(mut s) if s.last_event.is_some() => {
                // Extends. An out-of-order timestamp must not drag the session
                // end backwards.
                if s.last_event.is_some_and(|last| ts > last) {
                    s.last_event = Some(ts);
                }
                s.dirty = true;
                s
            }
            other => Session {
                acc: match &other {
                    Some(s) => s.acc.clone(),
                    None => reducer.initial(),
                },
                start: Some(ts),
                last_event: Some(ts),
                dirty: true,
                seeded: other.map(|s| s.seeded).unwrap_or(false),
                closed: false,
            },
        };
        let acc = std::mem::replace(&mut s.acc, Value::Null);
        s.acc = reducer.step(acc, &env.record);
        sessions.insert(env.key.clone(), s);
    }

    // Idle sweep: anything whose silence has already run out closes now.
    for (key, s) in sessions.iter_mut() {
        let Some(last) = s.last_event else { continue };
        if last + gap_ms + window.grace_ms() <= now_ms {
            out.emits.push(build_session_emit(key, s, gap_ms));
            s.closed = true;
        }
    }

    for (key, s) in sessions {
        let state_key = session_state_key(&tag, &key);
        if s.closed {
            if s.seeded {
                out.state_ops.push(StateOp::Delete { key: state_key });
            }
        } else if s.dirty {
            out.state_ops.push(StateOp::Upsert {
                key: state_key,
                value: serde_json::json!({
                    "acc": s.acc,
                    "sessionStart": s.start,
                    "lastEventTime": s.last_event,
                }),
            });
        }
    }

    out
}

fn build_session_emit(key: &str, s: &Session, gap_ms: i64) -> Emit {
    let start = s.start.unwrap_or(0);
    Emit {
        key: key.to_string(),
        window_start: start,
        window_end: s.last_event.unwrap_or(start) + gap_ms,
        window_key: format_iso_ms(start),
        value: s.acc.clone(),
    }
}

/// The watermark row, so a restart resumes where event time had reached.
pub(crate) fn watermark_op(watermark_ms: i64) -> StateOp {
    StateOp::Upsert {
        key: WATERMARK_STATE_KEY.to_string(),
        value: serde_json::json!({ "eventTimeMs": watermark_ms }),
    }
}

/// Read a watermark out of loaded state.
pub(crate) fn watermark_of(loaded_state: &BTreeMap<String, Value>) -> Option<i64> {
    loaded_state
        .get(WATERMARK_STATE_KEY)?
        .get("eventTimeMs")?
        .as_i64()
}

/// Reject user state keys that would collide with the engine's reserved space.
pub(crate) fn check_user_key(key: &str) -> Result<()> {
    if key.starts_with("__") {
        return Err(Error::Invalid(format!(
            "state keys starting with '__' are reserved for the engine (got '{key}')"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::ops::{AggregateField, AggregateKind, Every, Record};
    use std::sync::Arc;

    fn rec(n: f64) -> Record {
        Record {
            data: serde_json::json!({ "n": n }),
            message: None,
            ctx: None,
        }
    }

    fn counter() -> Reducer {
        Reducer::Aggregate {
            fields: vec![AggregateField {
                name: "count".into(),
                kind: AggregateKind::Count,
                extract: Arc::new(|_| None),
            }],
        }
    }

    fn summer() -> Reducer {
        Reducer::Aggregate {
            fields: vec![AggregateField {
                name: "sum".into(),
                kind: AggregateKind::Sum,
                extract: Arc::new(|r| r.number("n")),
            }],
        }
    }

    // ------------------------------------------------------------ bucketing

    #[test]
    fn tumbling_floors_to_the_window_size() {
        let w = Window::new(WindowKind::Tumbling { seconds: 60 });
        // 10:00:37 lands in the window starting at 10:00:00.
        let env = w.annotate(rec(1.0), "k".into(), 1_785_837_637_000);
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].window_start, 1_785_837_600_000);
        assert_eq!(env[0].window_end, 1_785_837_660_000);
        assert_eq!(env[0].window_key, "2026-08-04T10:00:00.000Z");
    }

    #[test]
    fn tumbling_boundaries_are_half_open() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        // Exactly on the boundary belongs to the NEW window, not the old one.
        let a = w.annotate(rec(1.0), "k".into(), 10_000);
        assert_eq!(a[0].window_start, 10_000);
        let b = w.annotate(rec(1.0), "k".into(), 9_999);
        assert_eq!(b[0].window_start, 0);
    }

    #[test]
    fn tumbling_handles_timestamps_before_the_epoch() {
        // Integer division truncates toward zero; flooring must not, or
        // negative timestamps land in the window after the one they belong to.
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let e = w.annotate(rec(1.0), "k".into(), -1);
        assert_eq!(e[0].window_start, -10_000);
        assert_eq!(e[0].window_end, 0);
    }

    #[test]
    fn sliding_fans_out_to_size_over_slide_windows() {
        let w = Window::new(WindowKind::Sliding {
            size: 60,
            slide: 10,
        });
        let envs = w.annotate(rec(1.0), "k".into(), 1_785_837_637_000);
        assert_eq!(envs.len(), 6, "60/10 windows should cover each event");
        // Every window must actually contain the timestamp.
        for e in &envs {
            assert!(e.window_start <= 1_785_837_637_000);
            assert!(1_785_837_637_000 < e.window_end);
            assert_eq!(e.window_end - e.window_start, 60_000);
        }
        // ...and they are distinct.
        let starts: std::collections::HashSet<i64> = envs.iter().map(|e| e.window_start).collect();
        assert_eq!(starts.len(), 6);
    }

    #[test]
    fn sliding_with_slide_equal_to_size_is_tumbling() {
        let w = Window::new(WindowKind::Sliding {
            size: 30,
            slide: 30,
        });
        let envs = w.annotate(rec(1.0), "k".into(), 45_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].window_start, 30_000);
    }

    #[test]
    fn cron_aligns_to_wall_clock_boundaries() {
        let minute = Window::new(WindowKind::Cron {
            every: Every::Minute,
        });
        let e = minute.annotate(rec(1.0), "k".into(), 1_785_837_637_123);
        assert_eq!(e[0].window_key, "2026-08-04T10:00:00.000Z");

        let hour = Window::new(WindowKind::Cron { every: Every::Hour });
        let e = hour.annotate(rec(1.0), "k".into(), 1_785_837_637_123);
        assert_eq!(e[0].window_key, "2026-08-04T10:00:00.000Z");

        let day = Window::new(WindowKind::Cron { every: Every::Day });
        let e = day.annotate(rec(1.0), "k".into(), 1_785_837_637_123);
        assert_eq!(e[0].window_key, "2026-08-04T00:00:00.000Z");
    }

    #[test]
    fn cron_weeks_start_on_monday_utc() {
        // 2026-08-04 is a Tuesday; its week starts Monday 2026-08-03.
        let w = Window::new(WindowKind::Cron { every: Every::Week });
        let e = w.annotate(rec(1.0), "k".into(), 1_785_837_637_123);
        assert_eq!(e[0].window_key, "2026-08-03T00:00:00.000Z");
    }

    // -------------------------------------------------------------- folding

    #[test]
    fn a_window_stays_open_until_the_clock_passes_its_end() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let envs = w.annotate(rec(1.0), "k".into(), 5_000);
        let state = BTreeMap::new();

        // Clock inside the window: nothing emits, the accumulator is saved.
        let out = run_reduce(&counter(), &envs, &state, Some(9_999), "tumb:10", 0);
        assert!(out.emits.is_empty());
        assert_eq!(out.state_ops.len(), 1);
        assert!(matches!(out.state_ops[0], StateOp::Upsert { .. }));

        // Clock at the window end: it closes.
        let out = run_reduce(&counter(), &envs, &state, Some(10_000), "tumb:10", 0);
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].value["count"], 1.0);
        // Never persisted, so nothing to delete.
        assert!(out.state_ops.is_empty());
    }

    #[test]
    fn grace_holds_a_window_open_past_its_end() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let envs = w.annotate(rec(1.0), "k".into(), 5_000);
        let state = BTreeMap::new();

        let out = run_reduce(&counter(), &envs, &state, Some(10_000), "tumb:10", 5_000);
        assert!(out.emits.is_empty(), "grace should keep it open");

        let out = run_reduce(&counter(), &envs, &state, Some(15_000), "tumb:10", 5_000);
        assert_eq!(out.emits.len(), 1);
    }

    #[test]
    fn a_seeded_window_closes_and_is_deleted() {
        let mut state = BTreeMap::new();
        state.insert(
            state_key_for("tumb:10", "1970-01-01T00:00:00.000Z", "k"),
            serde_json::json!({ "acc": { "count": 7.0 }, "windowStart": 0, "windowEnd": 10_000 }),
        );

        let out = run_reduce(&counter(), &[], &state, Some(10_000), "tumb:10", 0);
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].value["count"], 7.0);
        assert_eq!(out.state_ops.len(), 1);
        assert!(matches!(out.state_ops[0], StateOp::Delete { .. }));
    }

    #[test]
    fn a_quiet_windows_backlog_still_closes() {
        // The reason the runner loads ALL rows rather than just the batch's:
        // this window got no traffic this cycle but the clock has passed it.
        let mut state = BTreeMap::new();
        state.insert(
            state_key_for("tumb:10", "1970-01-01T00:00:00.000Z", "quiet"),
            serde_json::json!({ "acc": { "count": 3.0 }, "windowStart": 0, "windowEnd": 10_000 }),
        );
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let envs = w.annotate(rec(1.0), "busy".into(), 25_000);

        let out = run_reduce(&counter(), &envs, &state, Some(25_000), "tumb:10", 0);
        let keys: Vec<&str> = out.emits.iter().map(|e| e.key.as_str()).collect();
        assert!(
            keys.contains(&"quiet"),
            "the idle key never closed: {keys:?}"
        );
    }

    #[test]
    fn rows_from_another_operator_are_left_alone() {
        let mut state = BTreeMap::new();
        state.insert(
            state_key_for("slide:60:10", "1970-01-01T00:00:00.000Z", "k"),
            serde_json::json!({ "acc": { "count": 1.0 }, "windowStart": 0, "windowEnd": 60_000 }),
        );
        state.insert(
            WATERMARK_STATE_KEY.to_string(),
            serde_json::json!({ "eventTimeMs": 99_000 }),
        );

        let out = run_reduce(&counter(), &[], &state, Some(999_999), "tumb:10", 0);
        assert!(
            out.emits.is_empty() && out.state_ops.is_empty(),
            "a tumbling reducer must not close a sliding operator's windows, \
             nor treat the watermark as one"
        );
    }

    #[test]
    fn folding_accumulates_across_a_batch() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let mut envs = Vec::new();
        for n in [1.0, 2.0, 3.0] {
            envs.extend(w.annotate(rec(n), "k".into(), 1_000));
        }
        let out = run_reduce(
            &summer(),
            &envs,
            &BTreeMap::new(),
            Some(10_000),
            "tumb:10",
            0,
        );
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].value["sum"], 6.0);
    }

    #[test]
    fn separate_keys_accumulate_separately() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let mut envs = w.annotate(rec(1.0), "a".into(), 1_000);
        envs.extend(w.annotate(rec(10.0), "b".into(), 1_000));
        envs.extend(w.annotate(rec(1.0), "a".into(), 2_000));

        let out = run_reduce(
            &summer(),
            &envs,
            &BTreeMap::new(),
            Some(10_000),
            "tumb:10",
            0,
        );
        assert_eq!(out.emits.len(), 2);
        let by_key: BTreeMap<&str, f64> = out
            .emits
            .iter()
            .map(|e| (e.key.as_str(), e.value["sum"].as_f64().unwrap()))
            .collect();
        assert_eq!(by_key["a"], 2.0);
        assert_eq!(by_key["b"], 10.0);
    }

    #[test]
    fn a_seeded_accumulator_continues_rather_than_restarting() {
        let mut state = BTreeMap::new();
        state.insert(
            state_key_for("tumb:10", "1970-01-01T00:00:00.000Z", "k"),
            serde_json::json!({ "acc": { "sum": 100.0 }, "windowStart": 0, "windowEnd": 10_000 }),
        );
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let envs = w.annotate(rec(5.0), "k".into(), 1_000);

        let out = run_reduce(&summer(), &envs, &state, Some(10_000), "tumb:10", 0);
        assert_eq!(out.emits[0].value["sum"], 105.0, "state was not resumed");
    }

    #[test]
    fn without_a_clock_the_newest_window_stays_open() {
        let w = Window::new(WindowKind::Tumbling { seconds: 10 });
        let mut envs = w.annotate(rec(1.0), "k".into(), 1_000);
        envs.extend(w.annotate(rec(1.0), "k".into(), 15_000));

        let out = run_reduce(&counter(), &envs, &BTreeMap::new(), None, "tumb:10", 0);
        // The older window closes; the newest one must not, since more events
        // for it may still arrive.
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].window_start, 0);
    }

    // ------------------------------------------------------------- sessions

    fn session_window(gap: i64) -> Window {
        Window::new(WindowKind::Session { gap })
    }

    #[test]
    fn a_session_extends_while_events_keep_arriving() {
        let w = session_window(30);
        let envs: Vec<Envelope> = [0i64, 10_000, 20_000]
            .iter()
            .flat_map(|ts| w.annotate(rec(1.0), "u1".into(), *ts))
            .collect();

        // now_ms just after the last event: the session is still open.
        let out = run_session(&w, &counter(), &envs, &BTreeMap::new(), 21_000);
        assert!(out.emits.is_empty(), "session closed too early");
        assert_eq!(out.state_ops.len(), 1);
        match &out.state_ops[0] {
            StateOp::Upsert { key, value } => {
                assert!(key.contains("open"));
                assert_eq!(value["acc"]["count"], 3.0);
                assert_eq!(value["sessionStart"], 0);
                assert_eq!(value["lastEventTime"], 20_000);
            }
            _ => panic!("expected an upsert"),
        }
    }

    #[test]
    fn a_session_closes_after_the_gap_of_silence() {
        let w = session_window(30);
        let envs = w.annotate(rec(1.0), "u1".into(), 0);
        // 30s gap has elapsed by 30_000.
        let out = run_session(&w, &counter(), &envs, &BTreeMap::new(), 30_000);
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].key, "u1");
        assert_eq!(out.emits[0].window_start, 0);
        assert_eq!(out.emits[0].window_end, 30_000);
    }

    #[test]
    fn a_late_arriving_event_past_the_gap_starts_a_new_session() {
        let w = session_window(30);
        let mut envs = w.annotate(rec(1.0), "u1".into(), 0);
        // 60s later: the first session is over, a second begins.
        envs.extend(w.annotate(rec(1.0), "u1".into(), 60_000));

        let out = run_session(&w, &counter(), &envs, &BTreeMap::new(), 60_500);
        assert_eq!(out.emits.len(), 1, "the first session should have emitted");
        assert_eq!(out.emits[0].window_start, 0);
        assert_eq!(out.emits[0].value["count"], 1.0);
        // The second is still open.
        assert!(out
            .state_ops
            .iter()
            .any(|op| matches!(op, StateOp::Upsert { .. })));
    }

    #[test]
    fn a_seeded_session_resumes_and_deletes_on_close() {
        let w = session_window(30);
        let mut state = BTreeMap::new();
        state.insert(
            session_state_key("sess:30", "u1"),
            serde_json::json!({ "acc": { "count": 4.0 }, "sessionStart": 0, "lastEventTime": 1_000 }),
        );

        // Idle flush with no new events: silence has run out, so it closes.
        let out = run_session(&w, &counter(), &[], &state, 40_000);
        assert_eq!(out.emits.len(), 1);
        assert_eq!(out.emits[0].value["count"], 4.0);
        assert_eq!(out.state_ops.len(), 1);
        assert!(matches!(out.state_ops[0], StateOp::Delete { .. }));
    }

    #[test]
    fn sessions_are_per_key() {
        let w = session_window(30);
        let mut envs = w.annotate(rec(1.0), "u1".into(), 0);
        envs.extend(w.annotate(rec(1.0), "u2".into(), 0));
        envs.extend(w.annotate(rec(1.0), "u1".into(), 1_000));

        let out = run_session(&w, &counter(), &envs, &BTreeMap::new(), 40_000);
        assert_eq!(out.emits.len(), 2);
        let by_key: BTreeMap<&str, f64> = out
            .emits
            .iter()
            .map(|e| (e.key.as_str(), e.value["count"].as_f64().unwrap()))
            .collect();
        assert_eq!(by_key["u1"], 2.0);
        assert_eq!(by_key["u2"], 1.0);
    }

    #[test]
    fn an_out_of_order_timestamp_does_not_shorten_a_session() {
        let w = session_window(30);
        let mut envs = w.annotate(rec(1.0), "u1".into(), 20_000);
        // Arrives after, but its event time is earlier.
        envs.extend(w.annotate(rec(1.0), "u1".into(), 5_000));

        let out = run_session(&w, &counter(), &envs, &BTreeMap::new(), 21_000);
        assert!(out.emits.is_empty());
        match &out.state_ops[0] {
            StateOp::Upsert { value, .. } => {
                assert_eq!(
                    value["lastEventTime"], 20_000,
                    "the session end moved backwards"
                );
            }
            _ => panic!("expected an upsert"),
        }
    }

    // ------------------------------------------------------------ watermark

    #[test]
    fn watermarks_round_trip_through_state() {
        let op = watermark_op(12_345);
        let StateOp::Upsert { key, value } = op else {
            panic!("expected an upsert")
        };
        assert_eq!(key, WATERMARK_STATE_KEY);

        let mut state = BTreeMap::new();
        state.insert(key, value);
        assert_eq!(watermark_of(&state), Some(12_345));
        assert_eq!(watermark_of(&BTreeMap::new()), None);
    }

    #[test]
    fn reserved_key_prefixes_are_refused() {
        assert!(check_user_key("normal").is_ok());
        assert!(check_user_key("__wm__").is_err());
        assert!(check_user_key("__anything").is_err());
    }
}
