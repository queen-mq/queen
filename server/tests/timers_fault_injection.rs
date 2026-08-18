//! Fault injection on the timer fire path (PLAN_KV_TIMERS.md §15 "Fault injection sui
//! timer", §12 failure modes; closes F3).
//!
//! What this file exists to prove is one sentence: **a timer is delivered exactly once,
//! and every way the fire can fail leaves the row in a state somebody can still act on.**
//! The plan buys that with two mechanisms that are invisible from the outside — the lease
//! (`claim_token` AND `claimed_until`, §3.3) and the symmetric release of rows a
//! non-delivered segment had locked (§6.2 point 3) — so each of those gets a scenario
//! here rather than a paragraph in a header.
//!
//! Every scenario travels in time through `p_now` instead of sleeping. That is not a
//! shortcut: §6.2 makes every timer SP take the instant as a PARAMETER precisely so the
//! broker never does arithmetic on a timestamp, and it makes a lease expiry a
//! deterministic input instead of a 30-second wait.
//!
//! ONE test function on purpose: `log_timers_claim_v1` selects by shard and has no name
//! filter, so two of these running in parallel would claim each other's rows. Sections are
//! banners in the output; the assertion message names the contract that broke.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-timers-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test timers_fault_injection -- --ignored --nocapture
//! ```

mod timers_support;

use timers_support::*;

const LEASE_MS: i32 = 30_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn timers_survive_every_declared_fault() {
    let rig = boot().await;
    let c = &rig.c;
    let t = TENANT_DEFAULT;
    reset_timers(c).await;

    // ========================================================================
    section("a broker killed between the claim and the fire delivers EXACTLY once");
    // §12: "il broker muore dopo la claim, prima del fuoco — le righe restano claimed;
    // alla scadenza del lease visible_at rientra nella finestra e un altro broker le
    // prende. Nessun duplicato: il fuoco non era mai committato."
    // ========================================================================
    let q_kill = unique("tfi-kill");
    let k_kill = unique("k-kill");
    seed(c, &Seed::new(t, &q_kill, &k_kill).delay_s(-1.0)).await;

    let first = claim(c, 0.0, LEASE_MS, 100, 100).await;
    assert_eq!(first, vec![k_kill.clone()], "the due timer is claimed once");
    let token1 = token_of(c, t, &q_kill, &k_kill).await;

    // The dead broker's claim is invisible to everyone for the length of the lease — that
    // is the whole job of visible_at being GENERATED from claimed_until (§3.3).
    assert!(
        claim(c, 0.0, LEASE_MS, 100, 100).await.is_empty(),
        "a leased row must not be claimable twice; two brokers firing the same timer is \
         the one thing the lease exists to prevent"
    );
    let d = due(c, 0.0, 2000).await;
    assert_eq!(
        d["due"].as_i64(),
        Some(0),
        "a leased row is not due: the probe reads visible_at, not deliver_at ({d})"
    );

    // The lease expires. Nothing failed, so nothing was spent.
    let second = claim(c, 31.0, LEASE_MS, 100, 100).await;
    assert_eq!(second, vec![k_kill.clone()], "the expired lease is reclaimable");
    let row = timer_row(c, t, &q_kill, &k_kill).await.expect("still pending");
    assert_eq!(
        row.attempts, 0,
        "a lease expiry is not a failure: it must NOT consume the DLQ budget (§4.5)"
    );
    assert!(row.last_error.is_none(), "and it must not invent an error");
    let token2 = row.claim_token.clone().expect("reclaimed");
    assert_ne!(
        token1, token2,
        "the reclaim must mint a NEW token — the token is the only thing that makes the \
         lease safe (§12)"
    );

    // The dead broker wakes up and fires with its stale token.
    let segs = vec![Seg::new(t, &q_kill, "Default")];
    let stale = fire(
        c,
        &segs,
        &[Framed::new(&k_kill, 1, &k_kill, &token1)],
        31.0,
    )
    .await
    .expect("a stale fire is a verdict, not an error");
    assert_eq!(seg_result(&stale, 0), "stale", "wrong token ⇒ stale");
    assert!(
        partition_state(c, t, &q_kill, "Default").await.is_none(),
        "a stale segment must not even provision the destination: verification comes \
         BEFORE provisioning in §6.2's canonical body"
    );

    // The live holder fires.
    let fired = fire(c, &segs, &[Framed::new(&k_kill, 1, &k_kill, &token2)], 31.0)
        .await
        .expect("fire");
    assert_eq!(seg_result(&fired, 0), "fired");
    assert!(
        timer_row(c, t, &q_kill, &k_kill).await.is_none(),
        "delete and push share one transaction, so a delivered timer has no row left \
         (§1.4) — there is no 'fired' state to reconcile after a crash"
    );
    let p = partition_state(c, t, &q_kill, "Default")
        .await
        .expect("provisioned at the fire");
    assert_eq!(
        (p.last_offset, p.segments, p.messages_in_segments),
        // last_offset is an allocator that starts at -1 (001_log_schema.sql:32), so one
        // delivered message leaves it at 0, in one segment.
        (0, 1, 1),
        "exactly one message"
    );

    // And the retry of a fire that already committed (the broker died before learning it
    // succeeded, §12 row 2) adds nothing.
    let again = fire(c, &segs, &[Framed::new(&k_kill, 1, &k_kill, &token2)], 31.0)
        .await
        .expect("re-fire");
    assert_eq!(
        seg_result(&again, 0),
        "stale",
        "the row is gone, so the segment cannot verify — and 'gone' must read as stale, \
         never as a reason to push again"
    );
    let p = partition_state(c, t, &q_kill, "Default").await.expect("partition");
    assert_eq!(p.messages_in_segments, 1, "still exactly one message: exactly-once");

    // ========================================================================
    section("a lease that expires is reclaimed, and reclaiming is not failing");
    // ========================================================================
    let q_lease = unique("tfi-lease");
    let k_lease = unique("k-lease");
    seed(c, &Seed::new(t, &q_lease, &k_lease).delay_s(-0.5)).await;
    assert_eq!(claim(c, 0.0, 1_000, 100, 100).await, vec![k_lease.clone()]);
    let row = timer_row(c, t, &q_lease, &k_lease).await.expect("pending");
    assert_eq!(
        row.lease_in_future,
        Some(true),
        "claimed_until is the lease; claim_token is the identity. Both, always (§3.3)"
    );
    assert!(row.visible_in_future, "visible_at follows claimed_until while claimed");
    assert!(
        claim(c, 0.5, 1_000, 100, 100).await.is_empty(),
        "still inside the lease"
    );
    assert_eq!(
        claim(c, 2.0, 1_000, 100, 100).await,
        vec![k_lease.clone()],
        "past the lease, and no leader was needed to notice"
    );
    let row = timer_row(c, t, &q_lease, &k_lease).await.expect("pending");
    assert_eq!(row.attempts, 0, "reclaim ≠ attempt");

    // ========================================================================
    section("cancel racing the claim answers too_late, which is a VERDICT (§4.3)");
    // ========================================================================
    let q_race = unique("tfi-race");
    let k_early = unique("k-early");
    seed(c, &Seed::new(t, &q_race, &k_early).delay_s(-0.5)).await;
    let r = apply(c, &serde_json::json!([cancel_op(&q_race, &k_early)]), t, None, 0.0)
        .await
        .expect("cancel before the claim");
    assert_eq!(
        op_status(&r, 0),
        (true, "cancelled".to_string()),
        "a cancel that wins is a plain cancel"
    );
    assert!(timer_row(c, t, &q_race, &k_early).await.is_none());

    let k_late = unique("k-late");
    seed(c, &Seed::new(t, &q_race, &k_late).delay_s(-0.5)).await;
    assert_eq!(claim(c, 0.0, LEASE_MS, 100, 100).await, vec![k_late.clone()]);
    let r = apply(c, &serde_json::json!([cancel_op(&q_race, &k_late)]), t, None, 0.0)
        .await
        .expect("a lost race must NOT raise: it is a verdict, and the SQL must not abort \
                 the caller's transaction over it");
    assert_eq!(
        op_status(&r, 0),
        (false, "too_late".to_string()),
        "the holder has already packed that payload and is about to commit it (§4.3)"
    );
    assert!(
        timer_row(c, t, &q_race, &k_late).await.is_some(),
        "too_late must not delete: 'is it sent?' has to keep exactly one authority"
    );

    // Same rule for a reschedule — ONE rule, not two (§12).
    let r = apply(
        c,
        &serde_json::json!([schedule_op(&q_race, &k_late, "Default", 60_000, "txn-rescheduled")]),
        t,
        Some("tester"),
        0.0,
    )
    .await
    .expect("reschedule on a claimed row");
    assert_eq!(
        op_status(&r, 0),
        (false, "too_late".to_string()),
        "granting it would deliver the OLD payload after the client believes it replaced it"
    );
    assert_eq!(
        timer_row(c, t, &q_race, &k_late).await.expect("row").txn,
        k_late,
        "and nothing of the claimed row may change"
    );

    // ========================================================================
    section("a STALE segment releases every row it locked (§6.2 point 3)");
    // The failure this prevents: 200 timers on one (tenant, queue, partition), one of them
    // gone, and all 200 invisible for a full lease because the aborted segment left their
    // claim standing. lateMs climbs in steps and no metric names the cause.
    // ========================================================================
    let q_st = unique("tfi-stale");
    let q_ok = unique("tfi-stale-ok");
    let (k1, k2, k3, k4) = (
        unique("k-s1"),
        unique("k-s2"),
        unique("k-s3"),
        unique("k-ok"),
    );
    for k in [&k1, &k2, &k3] {
        seed(c, &Seed::new(t, &q_st, k).partition("pstale").delay_s(-1.0)).await;
    }
    seed(c, &Seed::new(t, &q_ok, &k4).partition("p2").delay_s(-1.0)).await;

    let claimed = claim(c, 0.0, LEASE_MS, 100, 100).await;
    assert_eq!(claimed.len(), 4, "four due rows in one pass: {claimed:?}");
    let (t1, t2, t3, t4) = (
        token_of(c, t, &q_st, &k1).await,
        token_of(c, t, &q_st, &k2).await,
        token_of(c, t, &q_st, &k3).await,
        token_of(c, t, &q_ok, &k4).await,
    );

    // What makes the segment stale: one of its rows is no longer there — the shape another
    // broker's committed fire leaves behind.
    c.execute(
        "DELETE FROM queen.log_timers WHERE tenant_id = $1::text::uuid AND queue = $2 AND timer_key = $3",
        &[&t, &q_st, &k2],
    )
    .await
    .expect("row vanishes under the packer");

    let res = fire(
        c,
        &[Seg::new(t, &q_st, "pstale"), Seg::new(t, &q_ok, "p2")],
        &[
            Framed::new(&k1, 1, &k1, &t1),
            Framed::new(&k2, 1, &k2, &t2),
            Framed::new(&k3, 1, &k3, &t3),
            Framed::new(&k4, 2, &k4, &t4),
        ],
        0.0,
    )
    .await
    .expect("a mixed batch is not an error");
    assert_eq!(
        seg_result(&res, 0),
        "stale",
        "verification is all-or-nothing PER SEGMENT: the blob is already packed, so one \
         missing timer makes that blob wrong"
    );
    assert_eq!(seg_result(&res, 1), "fired", "and a healthy segment is unaffected");

    for k in [&k1, &k3] {
        let row = timer_row(c, t, &q_st, k)
            .await
            .unwrap_or_else(|| panic!("{k} must NOT be deleted by a stale segment"));
        assert!(
            row.claim_token.is_none(),
            "{k}: the stale branch must NULL the token of the rows it locked"
        );
        assert!(
            !row.visible_in_future,
            "{k}: and pull claimed_until back to now, or the row is invisible for a whole \
             lease it never earned"
        );
    }
    let mut back = claim(c, 0.0, LEASE_MS, 100, 100).await;
    back.sort();
    let mut want = vec![k1.clone(), k3.clone()];
    want.sort();
    assert_eq!(
        back, want,
        "immediately re-claimable — that is the observable form of 'released'"
    );
    assert!(
        partition_state(c, t, &q_st, "pstale").await.is_none(),
        "a stale segment writes nothing at all"
    );
    let p = partition_state(c, t, &q_ok, "p2").await.expect("the live segment");
    assert_eq!((p.messages_in_segments, p.segments), (1, 1));
    assert!(timer_row(c, t, &q_ok, &k4).await.is_none(), "delivered ⇒ deleted");

    // ========================================================================
    section("the fire does NOT dedup against the log — the ratified cost of p_verified = v_last");
    //
    // CONTRACT TENSION, RESOLVED IN FAVOUR OF THE PLAN'S DECISION, and pinned here so the
    // consequence is a written assertion instead of a production discovery.
    //
    // §6.2 ratifies `p_verified = v_last` for the fire (never -1), with measured hot-path
    // reasoning: -1 makes log_push_one_v1 unroll the whole retained `log_txns` window of the
    // destination partition, AFTER the FOR UPDATE on log_partitions, i.e. inside the push
    // serializer shared with ordinary producers — the work the 1.0.0 bloom front-dedup took
    // from 60% to 3.7%. The symptom would be "push got worse" with nothing in the fire's own
    // telemetry to explain it.
    //
    // But 003_log_push.sql:129-135 gates the dedup probe on `v_last > v_from` where
    // `v_from := GREATEST(p_verified, txns_start - 1)`. With p_verified = v_last the probe
    // NEVER runs, so `log_push_one_v1` can never hand this caller a `duplicate` — and
    // 025_log_timers.sql closes the one remaining route on purpose, raising
    // `QFIRE two live segments target the same (tenant, queue, partition)` (the stale-
    // last_offset case) rather than letting a packer bug reach it.
    //
    // So: a timer whose fixed `txn` is ALREADY in the log is appended a SECOND time. The
    // fixed txn is NOT the safety net for the fire; the net is delete-and-push in one
    // transaction, which the first section of this file proves. That is the whole exactly-once
    // story, and this assertion is what stops someone from believing there are two.
    //
    // `duplicate` therefore stays in the fire's taxonomy (§4.1/§12/§14.2) as defence in depth
    // for a SHARED allocator — 025_log_timers.sql implements the arm in full, and the arm
    // feeds the same release statement as the stale arm ("written once, used by BOTH") — but
    // it is unreachable from this caller and cannot be exercised end to end. The release
    // symmetry it shares is covered by the stale section immediately above; what is NOT
    // covered, and cannot be until §6.2 changes, is the duplicate arm's own row bookkeeping.
    //
    // ALICE: this is the one open item. Either p_verified stays v_last and `duplicate` is
    // documented as unreachable-by-construction in §4.1/§12/§14.2, or the fire probes the
    // fixed txn itself and pays the cost §6.2 rejected. Do not "fix" this test to make
    // `duplicate` appear — that would mean quietly reverting the hot-path decision.
    // ========================================================================
    let q_dup = unique("tfi-dup");
    let txn_dup = unique("txn-dup");
    ensure_queue(c, t, &q_dup, 3600).await;
    c.execute(
        "SELECT queen.log_push_one_v1($1, 'pdup', 1, decode(md5($2), 'hex'), -1,
                 convert_to('already-delivered','UTF8'), NULL, NULL, $3::text::uuid)",
        &[&q_dup, &txn_dup, &t],
    )
    .await
    .expect("the timer's txn is already in the log");

    let (kd1, kd2) = (unique("k-d1"), unique("k-d2"));
    seed(
        c,
        &Seed::new(t, &q_dup, &kd1).partition("pdup").delay_s(-1.0).txn(&txn_dup),
    )
    .await;
    seed(c, &Seed::new(t, &q_dup, &kd2).partition("pdup").delay_s(-1.0)).await;
    let claimed = claim(c, 0.0, LEASE_MS, 100, 100).await;
    assert_eq!(claimed.len(), 2, "both due: {claimed:?}");
    let (td1, td2) = (
        token_of(c, t, &q_dup, &kd1).await,
        token_of(c, t, &q_dup, &kd2).await,
    );
    let res = fire(
        c,
        &[Seg::new(t, &q_dup, "pdup")],
        &[Framed::new(&kd1, 1, &txn_dup, &td1), Framed::new(&kd2, 1, &kd2, &td2)],
        0.0,
    )
    .await
    .expect("the fire is a verdict either way");
    assert_eq!(
        seg_result(&res, 0),
        "fired",
        "with p_verified = v_last the window probe is skipped, so a txn already in the log is \
         NOT detected. If this ever reads `duplicate`, someone changed p_verified back to -1 \
         and put the retained-window unroll back inside the push serializer (§6.2)."
    );
    for (k, why) in [
        (&kd1, "delivered (a second copy) ⇒ deleted"),
        (&kd2, "delivered ⇒ deleted"),
    ] {
        assert!(
            timer_row(c, t, &q_dup, k).await.is_none(),
            "{why}: the whole live segment is pushed and its timers removed in one transaction"
        );
    }
    let p = partition_state(c, t, &q_dup, "pdup").await.expect("partition");
    assert_eq!(
        p.messages_in_segments, 3,
        "1 pre-existing + 2 fired: the duplicated txn IS appended again. The fire's guarantee \
         is delete+push atomicity, not log-wide deduplication"
    );

    // The packer bug that WOULD reach the duplicate arm is closed loudly rather than
    // silently, and that is itself a contract: two live segments on one destination would
    // hand the second a stale last_offset for p_verified.
    let (kd3, kd4) = (unique("k-d3"), unique("k-d4"));
    seed(c, &Seed::new(t, &q_dup, &kd3).partition("pdup2").delay_s(-1.0)).await;
    seed(c, &Seed::new(t, &q_dup, &kd4).partition("pdup2").delay_s(-1.0)).await;
    let _ = claim(c, 0.0, LEASE_MS, 100, 100).await;
    let (td3, td4) = (
        token_of(c, t, &q_dup, &kd3).await,
        token_of(c, t, &q_dup, &kd4).await,
    );
    let err = fire(
        c,
        &[Seg::new(t, &q_dup, "pdup2"), Seg::new(t, &q_dup, "pdup2")],
        &[
            Framed::new(&kd3, 1, &kd3, &td3),
            Framed::new(&kd4, 2, &kd4, &td4),
        ],
        0.0,
    )
    .await
    .expect_err("two live segments on one destination must be loud, never silently wrong");
    let msg = err
        .as_db_error()
        .map(|d| d.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("two live segments target the same"),
        "the packer bug must name itself; got: {msg}"
    );

    // Earlier sections deliberately leave RELEASED rows behind (that is what they
    // assert), and the claim has no name filter — so the counting scenarios below
    // start from an empty table.
    reset_timers(c).await;
    // ========================================================================
    section("one poisoned timer inside a batch of 200 (§7.6, §12)");
    // The poison is product-shaped, not synthetic: a destination queue name that cannot be
    // provisioned. queen.queues.name is VARCHAR(255) (schema.sql:23), so 300 characters
    // raise 22001 — class 22, which §7.6's classifier calls permanent.
    // ========================================================================
    let q_batch = unique("tfi-batch");
    let q_poison = format!("poison-{}", "x".repeat(300));
    let mut keys: Vec<String> = Vec::new();
    let mut segs: Vec<Seg> = Vec::new();
    let mut frames: Vec<Framed> = Vec::new();
    for i in 0..199 {
        let k = format!("k-b{i}-{}", unique("p"));
        let part = format!("p{i}");
        seed(c, &Seed::new(t, &q_batch, &k).partition(&part).delay_s(-1.0)).await;
        segs.push(Seg::new(t, &q_batch, &part));
        keys.push(k);
    }
    let k_poison = unique("k-poison");
    seed(c, &Seed::new(t, &q_poison, &k_poison).delay_s(-1.0)).await;
    segs.push(Seg::new(t, &q_poison, "Default"));

    // DRAIN, not one pass. §6.2 pins the per-shard floor at
    // `GREATEST(ceil(p_max_rows / n_shards), 8)`, which with max_rows=500 over 64 shards is
    // exactly 8 — and 200 keys hash across the shards Poisson(~3.1), so a shard holding 9+ due
    // rows is common. Asserting "all 200 in one pass" is therefore a ~30% flake, not a
    // property: measured on a rig, 40 trials returned 194..200 with 12 short. The behaviour is
    // correct — §7.2's loop takes the remainder on the next pass — so the test drains the same
    // way the sweeper does, and asserts on the FIXED POINT instead of on one iteration.
    let mut claimed: Vec<String> = Vec::new();
    for _ in 0..64 {
        let batch = claim(c, 0.0, 60_000, 500, 500).await;
        if batch.is_empty() {
            break;
        }
        claimed.extend(batch);
        if claimed.len() >= 200 {
            break;
        }
    }
    assert_eq!(
        claimed.len(),
        200,
        "every due row is claimed by a bounded drain; a short FIXED POINT means rows are \
         invisible, which is a real defect (the per-shard floor is not one)"
    );
    for (i, k) in keys.iter().enumerate() {
        let tok = token_of(c, t, &q_batch, k).await;
        frames.push(Framed::new(k, (i + 1) as i32, k, &tok));
    }
    let tok_poison = token_of(c, t, &q_poison, &k_poison).await;
    frames.push(Framed::new(&k_poison, 200, &k_poison, &tok_poison));

    let err = fire(c, &segs, &frames, 0.0)
        .await
        .expect_err("one poisoned segment fails the whole transaction — that IS the reason \
                     QUEEN_SWEEPER_ISOLATE_ON_PERMANENT exists");
    let code = sqlstate(&err);
    assert!(
        code.starts_with("22") || code.starts_with("23") || code == "P0001",
        "the poison must surface as a PERMANENT class the classifier can act on, got {code}"
    );
    assert_eq!(
        timer_keys(c, t, &q_batch).await.len(),
        199,
        "the failed transaction committed nothing: no timer was consumed"
    );
    assert!(
        partition_state(c, t, &q_batch, "p0").await.is_none(),
        "and no message escaped from the aborted batch"
    );

    // With isolation: one segment per call. 199 commit, one keeps failing.
    let mut fired = 0;
    for (i, k) in keys.iter().enumerate() {
        let tok = token_of(c, t, &q_batch, k).await;
        let r = fire(
            c,
            &[Seg::new(t, &q_batch, &format!("p{i}"))],
            &[Framed::new(k, 1, k, &tok)],
            0.0,
        )
        .await
        .unwrap_or_else(|e| panic!("healthy segment {i} must commit alone: {e}"));
        assert_eq!(seg_result(&r, 0), "fired");
        fired += 1;
    }
    assert_eq!(fired, 199, "the 199 healthy timers get through");
    assert!(
        timer_keys(c, t, &q_batch).await.is_empty(),
        "every delivered timer is gone"
    );
    assert!(
        fire(
            c,
            &[Seg::new(t, &q_poison, "Default")],
            &[Framed::new(&k_poison, 1, &k_poison, &tok_poison)],
            0.0,
        )
        .await
        .is_err(),
        "the poisoned one still fails, alone, where it can be counted"
    );

    // Only NOW does it cost budget: a permanent failure spends an attempt (§4.5).
    let r = fail(
        c,
        t,
        &q_poison,
        &k_poison,
        &tok_poison,
        1_000,
        "QPUSH destination queue name too long",
        true,
        5,
        0.0,
    )
    .await
    .expect("fail_v1");
    assert!(r.is_object() || r.is_array(), "fail returns the exhausted list ({r})");
    let row = timer_row(c, t, &q_poison, &k_poison).await.expect("still pending");
    assert_eq!(row.attempts, 1, "one permanent failure, one attempt");
    assert!(
        row.claim_token.is_none(),
        "a row in backoff is in NOBODY's hands — that is what keeps it cancellable (§4.1)"
    );
    assert!(row.visible_in_future, "and invisible until the backoff elapses");
    assert!(row.last_error.is_some(), "with the reason kept for the operator");

    // ========================================================================
    section("an exhausted timer is archived under ITS OWN partition, then deleted");
    // §4.5 + §6.2: consumer_group '__timer__' and offset -1, because a timer never had a
    // group and its offset is not a position.
    // ========================================================================
    let q_dlq = unique("tfi-dlq");
    let k_dead = unique("k-dead");
    seed(c, &Seed::new(t, &q_dlq, &k_dead).delay_s(-1.0).attempts(5)).await;
    dlq(
        c,
        t,
        &q_dlq,
        &k_dead,
        "{\"poisoned\":true}",
        "exhausted after 5 permanent failures",
        5,
        0.0,
    )
    .await
    .expect("dlq_v1");
    let rows = dlq_rows(c, t, &q_dlq).await;
    assert_eq!(rows.len(), 1, "one dead letter");
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1),
        ("__timer__", -1),
        "a synthetic group and a non-position offset, exactly as declared"
    );
    assert_eq!(rows[0].2.as_deref(), Some(k_dead.as_str()), "carrying the timer's txn");
    assert!(
        timer_row(c, t, &q_dlq, &k_dead).await.is_none(),
        "archived ⇒ removed"
    );
    assert!(
        partition_state(c, t, &q_dlq, "Default").await.is_some(),
        "the DLQ must PROVISION the destination: get_dlq_messages_v1 inner-joins \
         log_partitions and queen.queues, so a row on a missing partition is archived and \
         unfindable, which is worse than not archiving it (§6.2)"
    );

    // The race the guard closes: a reschedule landing between fail_v1 and dlq_v1 resets
    // attempts, so the archive must find nothing and the row must live (§6.2).
    let k_resched = unique("k-resched");
    seed(c, &Seed::new(t, &q_dlq, &k_resched).delay_s(-1.0).attempts(0)).await;
    dlq(
        c,
        t,
        &q_dlq,
        &k_resched,
        "{\"poisoned\":true}",
        "stale exhaustion verdict",
        5,
        0.0,
    )
    .await
    .expect("dlq_v1 on a rescheduled row is not an error");
    assert!(
        timer_row(c, t, &q_dlq, &k_resched).await.is_some(),
        "attempts is back to 0, so this row is a NEW timer under an old name and must not \
         be archived for the sins of the one it replaced"
    );
    assert_eq!(
        dlq_rows(c, t, &q_dlq).await.len(),
        1,
        "and nothing new was written to the DLQ"
    );

    // Earlier sections deliberately leave RELEASED rows behind (that is what they
    // assert), and the claim has no name filter — so the counting scenarios below
    // start from an empty table.
    reset_timers(c).await;
    // ========================================================================
    section("the database going away mid-fire consumes NOTHING (§12, transient)");
    // ========================================================================
    let q_db = unique("tfi-db");
    let k_db = unique("k-db");
    seed(c, &Seed::new(t, &q_db, &k_db).delay_s(-1.0)).await;
    assert_eq!(claim(c, 0.0, LEASE_MS, 100, 100).await, vec![k_db.clone()]);
    let tok_db = token_of(c, t, &q_db, &k_db).await;

    let doomed = second_connection().await;
    let pid: i32 = doomed
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    doomed.batch_execute("BEGIN").await.expect("begin");
    fire(
        &doomed,
        &[Seg::new(t, &q_db, "Default")],
        &[Framed::new(&k_db, 1, &k_db, &tok_db)],
        0.0,
    )
    .await
    .expect("the fire runs inside the doomed transaction");
    // The backend dies before the COMMIT: the whole fire — push AND delete — rolls back
    // together, which is the entire reason they share a transaction.
    c.execute("SELECT pg_terminate_backend($1::int)", &[&pid])
        .await
        .expect("kill the fire's backend");

    let row = timer_row(c, t, &q_db, &k_db)
        .await
        .expect("the timer is still pending: nothing was committed");
    assert_eq!(
        row.attempts, 0,
        "five minutes of database trouble must not send every timer in the system to the \
         DLQ: infrastructure failure is not product failure (§4.5)"
    );
    assert_eq!(
        row.claim_token.as_deref(),
        Some(tok_db.as_str()),
        "the claim was committed earlier and survives; only the fire rolled back"
    );
    assert!(
        partition_state(c, t, &q_db, "Default").await.is_none(),
        "and nothing was delivered"
    );

    // The broker then reports the transient failure WITHOUT spending an attempt.
    fail(
        c, t, &q_db, &k_db, &tok_db, 5_000,
        "08006 connection failure during fire", false, 5, 0.0,
    )
    .await
    .expect("fail_v1 transient");
    let row = timer_row(c, t, &q_db, &k_db).await.expect("pending");
    assert_eq!(row.attempts, 0, "count_attempt = false means the budget is untouched");
    assert!(row.claim_token.is_none(), "and the row is released into backoff");
    assert!(row.visible_in_future, "held back by the backoff, not by a lease");

    // ========================================================================
    section("cancel during a BACKOFF succeeds — claim_token is NULL on purpose (§4.1)");
    // Collapsing claimed_until and claim_token into one column would make a poisoned timer
    // uncancellable for the whole backoff: the user could not remove the broken thing.
    // ========================================================================
    assert!(
        claim(c, 0.0, LEASE_MS, 100, 100).await.is_empty(),
        "the backing-off row is invisible to the sweeper"
    );
    let r = apply(c, &serde_json::json!([cancel_op(&q_db, &k_db)]), t, None, 0.0)
        .await
        .expect("cancel during backoff");
    assert_eq!(
        op_status(&r, 0),
        (true, "cancelled".to_string()),
        "in backoff ≠ in someone's hands"
    );
    assert!(timer_row(c, t, &q_db, &k_db).await.is_none(), "and it is gone");

    // A cancel for something that is no longer pending is `absent` with ok:false — never
    // ok:true, because a caller that trusts the flag would read it as 'stopped in time'
    // (§4.4).
    let r = apply(c, &serde_json::json!([cancel_op(&q_db, &k_db)]), t, None, 0.0)
        .await
        .expect("second cancel");
    assert_eq!(
        op_status(&r, 0),
        (false, "absent".to_string()),
        "absent means 'no longer pending', which may mean ALREADY DELIVERED (§4.4)"
    );

    let _ = rig.broker.shutdown().await;
}
