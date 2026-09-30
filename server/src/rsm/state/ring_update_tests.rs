use super::*;

/// Original replace implementation, independent of the in-place fast path.
fn replace_row(r: &mut PlanRing, pid: Pid, at: Option<i64>, now: i64) {
    if let Some(old) = r.at.remove(&pid) {
        if !r.ready.remove(&(old, pid)) {
            r.deferred.remove(&(old, pid));
        }
    }
    if let Some(at) = at {
        r.at.insert(pid, at);
        if at <= now {
            r.ready.insert((at, pid));
        } else {
            r.deferred.insert((at, pid));
        }
    }
}

#[test]
fn in_place_pending_updates_match_replacement_across_clock_changes() {
    let mut actual = PlanRing::default();
    let mut expected = PlanRing::default();
    let mut seed = 7u64;
    for step in 0..20000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let pid = (seed >> 32) % 128;
        let at = match seed % 4 {
            0 => None,
            1 => actual.at.get(&pid).copied(),
            _ => Some(((seed >> 16) % 64) as i64),
        };
        let now = ((seed >> 48) % 64) as i64;
        actual.set(pid, at, now);
        replace_row(&mut expected, pid, at, now);
        assert_eq!(actual.at, expected.at, "step {step}");
        assert_eq!(actual.ready, expected.ready, "step {step}");
        assert_eq!(actual.deferred, expected.deferred, "step {step}");
    }
}

#[test]
fn an_unchanged_pending_timestamp_can_move_both_ways_across_the_clock() {
    let mut ring = PlanRing::default();
    ring.set(1, Some(100), 0);
    assert_eq!(ring.deferred.len(), 1);
    ring.set(1, Some(100), 100);
    assert!(ring.deferred.is_empty());
    assert_eq!(ring.ready.len(), 1);
    ring.set(1, Some(100), 99);
    assert!(ring.ready.is_empty());
    assert_eq!(ring.deferred.len(), 1);
    ring.set(1, None, 99);
    assert!(ring.at.is_empty());
    assert!(ring.deferred.is_empty());
}

#[test]
fn router_views_keep_all_sampled_rows_inline() {
    let key = ("t".into(), "q".into(), "g".into());
    for ready in [0, 1, 8, 100] {
        let mut rings = PlanRings::new(0, 100);
        let mut rows: Vec<_> = (0..ready).map(|pid| (pid, 1)).collect();
        rows.extend((100..200).map(|pid| (pid, 200)));
        rings.keep_rows(&key, &rows);
        for now in [100, 200] {
            let view = rings.view(&key, now).unwrap();
            let expected = ready as usize + if now >= 200 { 100 } else { 0 };
            assert_eq!(view.left, expected);
            assert_eq!(view.rows.len(), expected.min(RING_VIEW_ROWS));
            assert!(
                !view.rows.spilled(),
                "the bounded router sample must not allocate"
            );
        }
    }
}
