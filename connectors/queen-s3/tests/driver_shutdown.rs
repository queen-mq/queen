//! SIGTERM (plan §6.7): stop fetching, finish the window that is worth
//! finishing, abandon the one that is not — and lose nothing either way.
//!
//! The rule the tests pin is not "the sink flushes on shutdown". It is that the
//! sink is allowed to abandon, because the intent makes the redo identical: what
//! must never happen is a half-window, a moved pointer with no object, or a
//! record that no restart will read again.

use queen_s3::driver::Stop;
use queen_s3::window::EngineState;

#[path = "driver_support.rs"]
mod support;
use support::*;

/// A window with records in it and a lag past the ten-second mark is finished:
/// closed, uploaded and committed, before the process exits.
#[tokio::test(start_paused = true)]
async fn a_window_worth_finishing_is_committed_before_the_exit() {
    let mut rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a", "b"]);
    let lease = rig.own("orders", "inst-a").await;
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());

    run_until(&mut d, 100, |d| d.engine().buffered_bytes() > 0).await;
    assert_eq!(d.engine().committed_k(), 0, "nothing has closed yet");

    rig.shutdown.trigger();
    let stop = run_until(&mut d, 100, |_| false).await;
    assert_eq!(stop, Some(Stop::Drained));
    assert_eq!(
        d.engine().committed_k(),
        1,
        "the buffered window is finished rather than thrown away"
    );
    assert_manifests_match_objects(&rig.store);
    let shipped = all_rows(&rig.store);
    assert!(!shipped.is_empty());
    for row in &shipped {
        assert!(
            expected.contains(&(row.partition.clone(), row.offset)),
            "the lake holds only records the log holds: {row:?}"
        );
    }

    // …and the rest is not lost: a fresh process picks it up at the pointer.
    // The same instance comes back: a stopped process's lease is still its own
    // until the TTL passes, which is exactly what a restart-in-place looks like.
    rig.rearm_shutdown();
    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until_quiet(&mut second, 600, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_windows_tile(&manifests(&rig.store));
}

/// A window that is small AND young is abandoned: nothing is written, the
/// pointer does not move, and the next start reads the same records again.
#[tokio::test(start_paused = true)]
async fn a_young_window_is_abandoned_and_nothing_is_lost() {
    let mut rig = Rig::new(test_cfg());
    // Nothing is "worth finishing" here: a megabyte of buffer, or ten seconds
    // of lag, and the second window will have neither.
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:05:00.000000Z"),
            &["{\"n\":1}"; 3],
        );
    }
    rig.queen.set_safe_time(t("2026-09-04T10:10:00.000000Z"));
    let lease = rig.own("orders", "inst-a").await;
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    let after_first = data_keys(&rig.store);

    // A handful of new records, and a clock only five seconds past the commit.
    let mut expected: Vec<(String, i64)> = Vec::new();
    for p in ["a", "b"] {
        for o in 0..3 {
            expected.push((p.to_string(), o));
        }
        let base = rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:10:03.000000Z"),
            &["{\"n\":2}"; 3],
        );
        for i in 0..3 {
            expected.push((p.to_string(), base + i));
        }
    }
    // Five seconds past the last commit: under the ten-second rule, and far
    // under `max_window`, so nothing but the drain could close this window.
    rig.queen.set_safe_time(t("2026-09-04T10:10:05.000000Z"));
    run_until(&mut d, 100, |d| d.engine().buffered_bytes() > 0).await;

    rig.shutdown.trigger();
    let stop = run_until(&mut d, 100, |_| false).await;
    assert_eq!(stop, Some(Stop::Drained));
    assert_eq!(
        d.engine().committed_k(),
        1,
        "a small, young window is abandoned rather than closed early"
    );
    assert_eq!(
        data_keys(&rig.store),
        after_first,
        "and nothing was written"
    );

    // The abandoned records are read again by the next process.
    // The same instance comes back: a stopped process's lease is still its own
    // until the TTL passes, which is exactly what a restart-in-place looks like.
    rig.rearm_shutdown();
    // The broker's clock moved on while the process was down, which is what
    // gives the abandoned records a window to land in.
    rig.queen.set_safe_time(t("2026-09-04T10:20:00.000000Z"));
    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until(&mut second, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut second, 400, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_windows_tile(&manifests(&rig.store));
    assert_manifests_match_objects(&rig.store);
}

/// A shutdown that arrives with a window already closed does not abandon it:
/// the upload and the commit are pending actions, and the drain runs them.
/// This is the reason the embedded supervisor's grace is thirty seconds.
#[tokio::test(start_paused = true)]
async fn a_window_already_in_flight_is_finished_not_dropped() {
    let mut rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a"]);
    let lease = rig.own("orders", "inst-a").await;
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());

    run_until(&mut d, 400, |d| {
        matches!(d.engine().state(), EngineState::Upload(_))
    })
    .await;
    assert_eq!(
        d.engine().state(),
        EngineState::Upload(1),
        "the intent is durable and the object is not up yet"
    );

    rig.shutdown.trigger();
    let stop = run_until(&mut d, 100, |_| false).await;
    assert_eq!(stop, Some(Stop::Drained));
    assert_eq!(
        d.engine().committed_k(),
        1,
        "an intent that is already durable is always finished: the redo would be identical, \
         and finishing it costs one upload"
    );
    assert_manifests_match_objects(&rig.store);

    // The same instance comes back: a stopped process's lease is still its own
    // until the TTL passes, which is exactly what a restart-in-place looks like.
    rig.rearm_shutdown();
    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until_quiet(&mut second, 600, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&manifests(&rig.store));
}
