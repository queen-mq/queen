//! The pin PLAN_KV_TIMERS §1.8 asks for by name: **the fire's SQL text must contain
//! `log_timers_fire_v1` and must NOT contain `log_push_multi_v1`.**
//!
//! Wired from `src/db.rs` — see `src/tests_unit/README.md` for the three-line block. It
//! `include_str!`s `025_log_timers.sql`, so wire it at F3, together with that file.
//!
//! Why a text assertion is the right instrument here, and not a weaker substitute for a real
//! test. The property being defended is a **lock order**, and a lock order is invisible at
//! runtime until two specific transactions overlap on two specific rows. The concurrency test of
//! §15 can only fail when it happens to interleave; this fails at `cargo test`, deterministically,
//! on the shape of the code. §18.7 accepts that the proof of §2 is maintained by a header — this
//! file is the part of that proof a machine can check.
//!
//! What goes wrong without it (§2.4 C1): the obvious implementation of the fire reuses the fusion
//! multi-segment push to get base offsets and then deletes the timers in a second call. That is
//! `P → T`, against the wire's `T → P`. It is a real cycle, on the hottest path in the product,
//! and it is what a competent person writes first because `log_push_multi_v1` is right there and
//! already does the batching.
//!
//! GOTCHA: the SQL is `include_str!`-embedded. After editing `025_log_timers.sql` you must
//! `cargo build` before these assertions mean anything.

use super::*;

const FIRE_SQL_FILE: &str = include_str!("../../sql/procedures/025_log_timers.sql");

/// The file with every comment blanked out, newlines preserved.
///
/// This is not tidiness, it is correctness: §6 requires each SP to carry the lock-order block of
/// §2 in its header, and that block **names `queen.log_consumers`, `log_push_multi_v1` and
/// `partition_id`** in the very sentences that forbid them. Scanning the raw text would fail on
/// a file that is exactly right, and the natural "fix" would be to delete the header — the
/// documentation §18.7 says the whole proof rests on.
fn sql() -> &'static str {
    static STRIPPED: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    STRIPPED.get_or_init(|| {
        let mut out = String::with_capacity(FIRE_SQL_FILE.len());
        let b = FIRE_SQL_FILE.as_bytes();
        let mut i = 0;
        let (mut line_comment, mut block_comment) = (false, false);
        while i < b.len() {
            let two = if i + 1 < b.len() {
                &b[i..i + 2]
            } else {
                &b[i..i + 1]
            };
            if line_comment {
                if b[i] == b'\n' {
                    line_comment = false;
                    out.push('\n');
                }
                i += 1;
            } else if block_comment {
                if two == b"*/" {
                    block_comment = false;
                    i += 2;
                } else {
                    if b[i] == b'\n' {
                        out.push('\n');
                    }
                    i += 1;
                }
            } else if two == b"--" {
                line_comment = true;
                i += 2;
            } else if two == b"/*" {
                block_comment = true;
                i += 2;
            } else {
                out.push(b[i] as char);
                i += 1;
            }
        }
        out
    })
}

/// The body of one SP inside a procedures file: from its `CREATE ... FUNCTION queen.<name>` to
/// the next top-level statement. Crude on purpose — it must not need a SQL parser to stay true.
///
/// The name also appears in the header comment block and in the `DROP FUNCTION IF EXISTS` line
/// the claim carries (the boot-idempotency half of the DROP+CREATE convention, §6.2), so the
/// match is anchored on a line that actually starts with `CREATE` rather than on the first hit.
fn body_of(name: &str) -> &'static str {
    let head = format!("FUNCTION queen.{name}");
    let start = sql()
        .match_indices(&head)
        .map(|(i, _)| sql()[..i].rfind('\n').map(|p| p + 1).unwrap_or(0))
        .find(|&line| sql()[line..].starts_with("CREATE"))
        .unwrap_or_else(|| panic!("025_log_timers.sql does not CREATE queen.{name}"));
    let rest = &sql()[start..];
    let end = ["\nCREATE ", "\nDROP ", "\nCOMMENT ", "\nGRANT "]
        .iter()
        .filter_map(|m| rest.get(1..).and_then(|r| r.find(m)).map(|i| i + 1))
        .min()
        .unwrap_or(rest.len());
    &rest[..end]
}

// ---------------------------------------------------------------------------
// §1.8, literally.
// ---------------------------------------------------------------------------

#[test]
fn the_broker_side_fire_statement_names_only_the_fire() {
    assert!(
        TIMERS_FIRE_SQL.contains("queen.log_timers_fire_v1"),
        "the fire must go through its own SP: {}",
        TIMERS_FIRE_SQL
    );
    assert!(
        !TIMERS_FIRE_SQL.contains("log_push_multi_v1"),
        "§1.8/§2.4 C1: the fire must NOT reuse the fusion multi-push. That path takes the \
         partition pre-lock FIRST, which is P → T against the wire's T → P — a deadlock cycle \
         on the product's hottest path. The fire is one SP, one transaction: claim-verify (T), \
         provision (Q), pre-lock (P), push, then DELETE rows this transaction already holds."
    );
    assert!(
        !TIMERS_FIRE_SQL.contains("log_push_one_v1"),
        "the broker never calls the allocator directly for a fire — log_push_one_v1 is reached \
         from INSIDE log_timers_fire_v1, with pid and window already resolved (§6.2 point 6)"
    );
}

#[test]
fn the_fire_sp_pushes_through_the_single_allocator_code_path() {
    let body = body_of("log_timers_fire_v1");
    assert!(
        !body.contains("log_push_multi_v1"),
        "§2.4 C1: log_timers_fire_v1 must not delegate to the multi-segment push"
    );
    assert!(
        body.contains("log_push_one_v1"),
        "§6.2 point 6: the fire pushes per segment through queen.log_push_one_v1 — the same \
         single allocator code path the wire uses, with pid and window already resolved so it \
         does not re-resolve them (that nested lookup was the seg v2 CPU regression)"
    );
}

// ---------------------------------------------------------------------------
// The invariants of §2 that live in the same text and cost nothing to check here.
// ---------------------------------------------------------------------------

#[test]
fn the_fire_never_waits_on_a_timer_row() {
    // §2.4 C2, written in the plan as: **the fire NEVER takes a lock that waits in the
    // queen.log_timers space.** A bare `FOR UPDATE` on log_timers turns the fire into a waiter
    // behind, say, a cancel that is sitting inside a transaction wire for the length of a whole
    // bundle — and it waits while holding the lease on 199 other timers, a Maint slot and a
    // connection. With PARALLELISM=1 one slow cancel stops every fire on the broker.
    //
    // Note the asymmetry, which is deliberate and must NOT be "fixed": the fire's `FOR UPDATE`
    // on queen.log_partitions (the ascending set-based pre-lock) MUST wait. Only the timer
    // space is required to skip.
    let body = body_of("log_timers_fire_v1");
    for (i, _) in body.match_indices("FOR UPDATE") {
        // The statement this FOR UPDATE belongs to: back to the previous `;`, forward past the
        // locking clause.
        let stmt_start = body[..i].rfind(';').map(|p| p + 1).unwrap_or(0);
        let window_end = (i + 80).min(body.len());
        let stmt = &body[stmt_start..window_end];
        if !stmt.contains("log_timers") {
            continue; // partitions / queues: waiting there is the declared behaviour
        }
        assert!(
            stmt.contains("SKIP LOCKED"),
            "a FOR UPDATE on queen.log_timers without SKIP LOCKED makes the fire a waiter in \
             the T space (§2.4 C2). Statement:\n{stmt}"
        );
    }
}

#[test]
fn nothing_in_025_touches_the_consumer_space() {
    // §2.3 actors 6 and 15: the fire's sequence is T → Q → P and the timer DLQ's is
    // T → Q → P → leaf. Neither may reach queen.log_consumers — which is exactly why
    // log_timers_dlq_v1 writes queen.log_dlq directly instead of reusing log_dlq_head_v1
    // (that one demands a live lease on (partition, group) and advances a consumer cursor;
    // a poisoned timer has no lease, no group and no cursor).
    assert!(
        !sql().contains("queen.log_consumers"),
        "025 must never touch queen.log_consumers: it would extend the fire's lock sequence by \
         a whole space, for no reason (§6.2, log_timers_dlq_v1)"
    );
    assert!(
        !sql().contains("log_dlq_head_v1"),
        "§6.2: the timer DLQ writes queen.log_dlq directly; log_dlq_head_v1 requires a lease \
         and a consumer group that a timer does not have"
    );
}

#[test]
fn the_schedule_path_never_provisions_a_queue() {
    // §2.4 C6. If log_timers_apply_v1 provisioned queen.queues to "validate" the destination,
    // the wire would hold TWO statements writing queen.queues with different sets in one
    // transaction (timers at step 0b, pushes at step 1). Their per-statement ORDER BY does not
    // help — they are different statements, and two overlapping bundles wait on each other's
    // xid on the unique index. The queue is born at the FIRE, never at the schedule; that is
    // the whole point of keying by names (§1.2).
    let body = body_of("log_timers_apply_v1");
    assert!(
        !body.contains("queen.queues"),
        "§2.4 C6: log_timers_apply_v1 must not write or read-lock queen.queues"
    );
    assert!(
        !body.contains("queen.log_partitions"),
        "§2.3 actor 13: the standalone timer path is a singleton in the T space"
    );
}

#[test]
fn the_timers_table_carries_no_partition_id() {
    // §2.4 C5 and §1.2: a partition_id column would give queen.log_partition_dead_v1 a veto leg
    // to consult, making partition cleanup P → T against the wire's T → P — and it would make
    // that O(partitions) scan more expensive, the one dimension this plan refuses to touch.
    let ddl_start = sql()
        .find("CREATE TABLE IF NOT EXISTS queen.log_timers")
        .expect("025 must create queen.log_timers");
    let ddl = &sql()[ddl_start..];
    let ddl = &ddl[..ddl.find("\n);").map(|i| i + 2).unwrap_or(ddl.len())];
    assert!(
        !ddl.contains("partition_id"),
        "queen.log_timers is keyed by NAMES: it stores `partition`, never a partition_id \
         (§1.2, §2.4 C5). The DDL read:\n{ddl}"
    );
}

#[test]
fn the_fire_does_not_ask_for_a_full_window_dedup_probe() {
    // §6.2: the fire passes `p_verified = v_last`, not -1. `-1` means "no broker cache, probe
    // the whole retained log_txns window", and that probe happens AFTER the FOR UPDATE on the
    // destination partition row — i.e. inside the push serializer shared with normal producers,
    // not isolated in the Maint lane. The symptom is "push got worse" and the fire's telemetry
    // does not explain it, because the time is billed to the push.
    let body = body_of("log_timers_fire_v1");
    for (i, _) in body.match_indices("log_push_one_v1") {
        let args = &body[i..(i + 400).min(body.len())];
        assert!(
            !args.contains("-1"),
            "the fire must pass p_verified = v_last, never -1 (§6.2). Call site:\n{args}"
        );
    }
}
