//! THE RUNTIME KILL SWITCHES, and the three-rung ladder that decides what a
//! refused call is told (PLAN_KV_TIMERS.md §9.5, §12.1).
//!
//! ---------------------------------------------------------------------------
//! A KILL SWITCH IS NOT A FEATURE GATE, and this file is the reason the broker
//! has one and not the other
//!
//! There used to be a second, boot-time level here: `QUEEN_KV_ENABLED` and
//! `QUEEN_TIMERS_ENABLED` decided whether the routes were registered at all, and
//! a call to a surface whose flag was off got a 404. Both are GONE (Alice,
//! 2026-08-18, supersedes PLAN_KV_TIMERS.md §16 and §20.4). KV and timers are
//! not features one opts into, they are part of the engine, like push and pop —
//! and there is no `QUEEN_PUSH_ENABLED`. The existence of a boot flag IS the
//! claim that the thing is optional, so the flag had to go, not merely flip to
//! `true`.
//!
//! What stays is the other thing entirely, and the distinction is the whole
//! design:
//!
//!   * a GATE is turned on to try something out. It is read once, at boot; it
//!     decides whether a surface EXISTS; changing it is a rollout; and every
//!     cell in the fleet may legitimately have a different answer. That is the
//!     thing this broker no longer has.
//!   * a KILL SWITCH is turned off to stop something that is already running and
//!     is hurting. It is read on every call; the surface exists either way;
//!     flipping it takes effect now, on a cell an operator is holding at three
//!     in the morning; and it is expected to be flipped BACK. Same class as
//!     `maintenance_mode` and `pop_maintenance_mode` (`handlers/maintenance.rs`),
//!     which is why it wears the same shape: an in-process atomic authoritative
//!     on the hot path, plus a row in `queen.system_state` as a best-effort
//!     mirror for propagation and restart, and a fresh read on the GET.
//!
//! Hence the one answer a paused surface gives: **503** with `Retry-After` on
//! the routes (temporary, come back), and a PERMANENT refusal inside the
//! transaction wire (a bundle carries messages; retrying it in a loop against a
//! deliberately paused cell is a storm on the hot path). Nothing answers 404 for
//! being switched off any more, because on every cell that runs this binary the
//! surface is there.
//!
//! ---------------------------------------------------------------------------
//! WHY THE TWO TIMER SWITCHES ARE SEPARATE
//!
//! `timers_schedule_enabled` and `timers_fire_enabled` are distinct because the
//! two halves have OPPOSITE COSTS. Stopping the schedule is harmless and
//! instant: nothing has been promised. Stopping the fire accumulates promised
//! work, and a customer reads undelivered promised work as message loss. So the
//! ladder of §12.1 can pause the schedule (rung 8) and NOTHING may ever pause
//! the fire automatically — only an operator, through the third switch.
//!
//! ---------------------------------------------------------------------------
//! WHY THERE IS NO PER-QUEUE FLAG (§12.1)
//!
//! The KV has no queue. Timers have a destination queue, but a per-queue flag
//! would be a column on `queen.queues`, and `/configure` RESETS the columns
//! (confirmed defect, 2026-08-05): the flag would switch itself off at the first
//! reconfiguration, silently. A cell-wide switch that an operator can see is
//! better than a per-queue one that lies.
//!
//! ---------------------------------------------------------------------------
//! AN ABSENT ROW MEANS ON
//!
//! This is the difference between a kill switch and a feature flag, and getting
//! it backwards would mean every fresh cell boots with the feature dead and no
//! row to explain why. `db::get_system_flag` answers `false` for an absent row
//! because its callers ask "is maintenance ON?"; these ask "is the feature still
//! ON?", so they go through `db::get_system_flag_opt`, where `None` is "nobody
//! has ever touched this switch".

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::quota::{Quotas, Verdict};
#[cfg(test)]
use crate::quota::{Limits, Measure, TenantRow, TestKnobs};

pub struct Switches {
    kv_on: AtomicBool,
    timers_schedule_on: AtomicBool,
    timers_fire_on: AtomicBool,
    /// EPHEMERAL_QUEUES.md §3.8/M9 — the RAM class has NO boot flag either, for
    /// the reason the module header gives, so this is its only level. It is one
    /// switch and not four (push/pop/ack/admin): the halves have the SAME cost
    /// here, unlike the timer pair above. Pausing the fire accumulates promised
    /// work; pausing an ephemeral surface accumulates nothing, because nothing
    /// on it is promised past the incarnation (§1.2). An operator holding a cell
    /// whose memory is the problem wants one lever, not a decision tree.
    eph_on: AtomicBool,
}

impl Switches {
    /// The `queen.system_state` keys, spelled out because an operator types them
    /// into a `psql` prompt during an incident and a rename is a silent no-op.
    pub const KEY_KV: &'static str = "kv_enabled";
    pub const KEY_TIMERS_SCHEDULE: &'static str = "timers_schedule_enabled";
    pub const KEY_TIMERS_FIRE: &'static str = "timers_fire_enabled";
    pub const KEY_EPHEMERAL: &'static str = "ephemeral_enabled";

    /// EVERY SWITCH IS BORN ON, and takes no argument that could say otherwise.
    /// There is nothing at boot to seed them from: a kill switch that a cell
    /// could start life with in the off position would be a gate wearing this
    /// module's name. The only thing that turns one off is an operator, through
    /// `/api/v1/system/kv-timers` or the `queen.system_state` row, and `adopt`
    /// below is how a restart picks that decision back up.
    pub fn new() -> Arc<Switches> {
        Arc::new(Switches::fresh())
    }

    fn fresh() -> Switches {
        Switches {
            kv_on: AtomicBool::new(true),
            timers_schedule_on: AtomicBool::new(true),
            timers_fire_on: AtomicBool::new(true),
            eph_on: AtomicBool::new(true),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Switches {
        Switches::fresh()
    }

    /// Is the KV surface live? One level, because there is only one.
    pub fn kv_on(&self) -> bool {
        self.kv_on.load(Ordering::Relaxed)
    }
    pub fn timers_schedule_on(&self) -> bool {
        self.timers_schedule_on.load(Ordering::Relaxed)
    }
    /// The ONLY thing that may stop the fire, and only an operator may set it.
    /// No rung of the degradation ladder touches it (§12.1).
    pub fn fire_allowed(&self) -> bool {
        self.timers_fire_on.load(Ordering::Relaxed)
    }
    /// Is the ephemeral surface live? EPHEMERAL_QUEUES.md §3.8.
    pub fn eph_on(&self) -> bool {
        self.eph_on.load(Ordering::Relaxed)
    }

    pub fn set_kv(&self, on: bool) {
        self.flip(
            &self.kv_on,
            on,
            Self::KEY_KV,
            "kv_kill_switch",
            "every KV route answers 503 and the wire's kv array is refused permanently; \
             nothing already stored is affected",
        );
    }
    pub fn set_timers_schedule(&self, on: bool) {
        self.flip(
            &self.timers_schedule_on,
            on,
            Self::KEY_TIMERS_SCHEDULE,
            "timers_schedule",
            "no NEW timers are accepted; the ones already scheduled still fire, and cancels \
             are never blocked",
        );
    }
    pub fn set_timers_fire(&self, on: bool) {
        self.flip(
            &self.timers_fire_on,
            on,
            Self::KEY_TIMERS_FIRE,
            "timers_fire",
            "promised messages are NOT being delivered. Nothing is lost — deliverAt is 'no \
             earlier than' and the backlog drains oldest-first — but watch \
             queen_timers_due_backlog",
        );
    }

    /// EPHEMERAL_QUEUES.md §3.8. The `reason` is blunt on purpose: turning this
    /// off does not merely refuse new work, it strands every consumer parked on
    /// an ephemeral queue for the rest of its timeout, and the rings keep their
    /// memory until the idle collector reaches them.
    pub fn set_ephemeral(&self, on: bool) {
        self.flip(
            &self.eph_on,
            on,
            Self::KEY_EPHEMERAL,
            "ephemeral_kill_switch",
            "every /api/v1/ephemeral route answers 503; rings keep their contents and their \
             memory until the implicit collector reaches them, and nothing durable is affected",
        );
    }

    /// Adopt what the DB mirror says. `None` (no row) means ON — see the module
    /// header. An unknown key is IGNORED rather than fatal: the row is a mirror,
    /// and a typo in a mirror must not take a broker down.
    pub fn adopt(&self, key: &str, value: Option<bool>) {
        let on = value.unwrap_or(true);
        match key {
            Self::KEY_KV => self.set_kv(on),
            Self::KEY_TIMERS_SCHEDULE => self.set_timers_schedule(on),
            Self::KEY_TIMERS_FIRE => self.set_timers_fire(on),
            Self::KEY_EPHEMERAL => self.set_ephemeral(on),
            _ => {}
        }
    }

    /// On-change only, in the same vocabulary the sweeper's ladder uses (§14.4):
    /// a switch that has been off for an hour must not write an hour of lines,
    /// and flipping one must not wait for the next reporter tick.
    ///
    /// THIS IS THE ONLY PLACE THAT LOGS A SWITCH TRANSITION, deliberately. The
    /// sweeper reads `fire_allowed()` on every cycle and could log the same fact
    /// itself; two lines for one event is how a log stops being read, and this
    /// one is strictly better — it fires at the instant of the flip rather than
    /// up to a cycle later, and it is emitted on brokers that do not sweep at all.
    ///
    /// `reason` says what the operator has actually done to the cell, because the
    /// line will be read by whoever is holding the incident and not by whoever
    /// wrote the switch.
    fn flip(
        &self,
        cell: &AtomicBool,
        on: bool,
        key: &'static str,
        stage: &'static str,
        reason: &'static str,
    ) {
        let prev = cell.swap(on, Ordering::Relaxed);
        if prev == on {
            return;
        }
        if on {
            tracing::info!(
                target: "sweeper", stage, key,
                "degradation stage LEFT (operator kill switch back on)"
            );
        } else {
            tracing::warn!(
                target: "sweeper", stage, key, reason = %reason,
                "degradation stage ENTERED (operator kill switch)"
            );
        }
    }
}

// ===========================================================================
// The ladder
// ===========================================================================

/// The surfaces the ladder distinguishes. They are separate because they are
/// refused at different rungs: §9.5 makes reads unconditional on occupancy and
/// §9.6 makes a cancel unblockable, full stop — no rung refuses one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Surface {
    KvRead,
    KvWrite,
    TimerSchedule,
    TimerCancel,
    TimerRead,
    /// EPHEMERAL_QUEUES.md §1.6 — four surfaces and not one, for the same reason
    /// the five above are five: they are refused at DIFFERENT rungs. The push is
    /// the only one that can take a tenant over its byte allowance, and the pop
    /// and the ack are the only ways under it again — so refusing either on
    /// occupancy would lock a full tenant out of the one action that empties it,
    /// which is the kv "deletes are never blocked" rule (§9.5) applied to a class
    /// whose occupancy is RAM.
    EphPush,
    EphPop,
    EphAck,
    /// `configure` / `reset` / `delete` and the two status reads. On the grant,
    /// because declaring a queue is how a tenant consumes the object allowance;
    /// NOT on occupancy or the rate, because reset and delete FREE memory and a
    /// status read is the only way to see how much there is to free.
    EphAdmin,
}

/// Which feature a surface belongs to. Exists so the ONE rendering point below
/// can pick the family's error code without a second match per rung: a paused
/// timer schedule must not answer `kv_disabled`, and a full ephemeral tenant
/// must not answer `kv_quota_exceeded`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Kv,
    Timers,
    Ephemeral,
}

impl Family {
    fn of(s: Surface) -> Family {
        match s {
            Surface::KvRead | Surface::KvWrite => Family::Kv,
            Surface::TimerSchedule | Surface::TimerCancel | Surface::TimerRead => Family::Timers,
            Surface::EphPush | Surface::EphPop | Surface::EphAck | Surface::EphAdmin => {
                Family::Ephemeral
            }
        }
    }

    fn disabled_code(self) -> &'static str {
        match self {
            Family::Kv => "kv_disabled",
            Family::Timers => "timers_disabled",
            Family::Ephemeral => "ephemeral_disabled",
        }
    }

    /// The cell-condition code. `Verdict::http` renders `NoRoom` as
    /// `kv_unavailable` because it was written when the KV was the only caller;
    /// the STATUS (503) is the rung's property and stays, the NAME is the
    /// family's and is corrected here rather than by a second mapping table.
    fn unavailable_code(self) -> &'static str {
        match self {
            Family::Kv | Family::Timers => "kv_unavailable",
            Family::Ephemeral => "ephemeral_unavailable",
        }
    }
}

/// Where the call arrived. The same rung answers differently on the two, and
/// that is §12.1 stage 7: on `/api/v1/kv` a paused cell is 503 and the client
/// retries, but inside `/api/v1/transaction` the `kv` array is refused
/// PERMANENTLY, because a bundle that retries a paused KV forever is a client
/// spinning on the hot path of the product with messages in hand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Route,
    Wire,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    Allow,
    /// An operator's runtime switch.
    Paused,
    /// The quota gate spoke.
    Refused(Verdict),
}

/// A rendered HTTP answer: status, the code from the closed taxonomy of §9.5,
/// and the `Retry-After` where one is honest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Http {
    pub status: u16,
    pub code: &'static str,
    pub retry_after: Option<u32>,
}

impl Answer {
    /// `None` when the call may proceed.
    ///
    /// The surface is a parameter because the CODE names the family — a paused
    /// timer schedule must not answer `kv_disabled` — while the STATUS is a
    /// property of the rung alone.
    pub fn http(self, origin: Origin, surface: Surface) -> Option<Http> {
        let family = Family::of(surface);
        match self {
            Answer::Allow => None,
            Answer::Paused => Some(Http {
                status: if origin == Origin::Wire { 403 } else { 503 },
                code: family.disabled_code(),
                retry_after: if origin == Origin::Wire { None } else { Some(1) },
            }),
            Answer::Refused(v) => v.http().map(|(status, code)| Http {
                status,
                code: match v {
                    Verdict::NoRoom => family.unavailable_code(),
                    _ => code,
                },
                retry_after: v.retry_after(),
            }),
        }
    }
}

/// THE THREE RUNGS, IN ORDER, IN ONE PLACE.
///
/// The order is itself a contract: the answer names the OUTERMOST reason,
/// because that is the one the caller can act on — and because leaking "you are
/// over quota" past a switch an operator has pulled would tell a prober that the
/// tenant exists (§13.5, error hygiene).
///
/// There is no rung above these. The old first rung was the boot flag, and with
/// `QUEEN_KV_ENABLED` / `QUEEN_TIMERS_ENABLED` gone the surfaces exist on every
/// cell that runs this binary — so nothing here can answer "not on this cell".
///
/// `add_rows` / `add_bytes` are the upper bound this call could add, and are
/// ignored for every surface but `KvWrite`.
pub fn decide(
    sw: &Switches,
    q: &Quotas,
    tenant: &str,
    surface: Surface,
    add_rows: i64,
    add_bytes: i64,
) -> Answer {
    // RUNG 1 — the operator's runtime switch. The cancel and the timer reads are
    // NOT on it: §9.6 makes the cancel unblockable, and a peek that answered 503
    // would stop a caller finding out whether a timer it can no longer cancel is
    // still pending.
    let runtime_on = match surface {
        Surface::KvRead | Surface::KvWrite => sw.kv_on(),
        Surface::TimerSchedule => sw.timers_schedule_on(),
        Surface::TimerCancel | Surface::TimerRead => true,
        // The ephemeral family never passes through here — see
        // `decide_ephemeral`. Answering on rung 1 alone and calling it a decision
        // would be a ladder with its grant rung silently missing, so this arm
        // fails CLOSED and says so in a debug build rather than admitting a call
        // nobody checked.
        Surface::EphPush | Surface::EphPop | Surface::EphAck | Surface::EphAdmin => {
            debug_assert!(false, "ephemeral surfaces go through switches::decide_ephemeral");
            return Answer::Paused;
        }
    };
    if !runtime_on {
        return Answer::Paused;
    }

    // RUNGS 2 and 3 — the grant and the occupancy, both from the quota gate.
    let v = match surface {
        Surface::KvRead => q.check_kv_read(tenant),
        Surface::KvWrite => q.charge_kv_write(tenant, add_rows, add_bytes),
        Surface::TimerSchedule => q.charge_timers(tenant, add_rows.max(1)),
        Surface::TimerCancel => q.check_timers_cancel(tenant),
        Surface::TimerRead => Verdict::Allow,
        Surface::EphPush | Surface::EphPop | Surface::EphAck | Surface::EphAdmin => {
            unreachable!("returned on rung 1 above")
        }
    };
    if v == Verdict::Allow {
        Answer::Allow
    } else {
        Answer::Refused(v)
    }
}

/// THE SAME THREE RUNGS, FOR A FAMILY WHOSE THIRD ONE IS NOT IN A DATABASE
/// (EPHEMERAL_QUEUES.md §1.6, M7).
///
/// WHY THIS IS A SIBLING OF `decide` AND NOT AN ARM OF IT. `decide` reads its
/// occupancy from `Quotas`, which is a MEASUREMENT: the rollup counts rows in
/// Postgres, every broker re-reads the count, and the enforcer is the local delta
/// against it. The ephemeral engine has no such loop, because the broker IS the
/// meter — the bytes are in its own heap and `Ephemeral` holds them exactly. So
/// the third rung needs a different authority object, and the alternative
/// (threading it through `decide`'s signature) would edit the KV call site in
/// `handlers/data.rs`, i.e. the durable transaction wire, which §5.2 declares
/// untouched by this feature. One extra function is a cheaper price than an edit
/// to that file.
///
/// The ORDER is the contract, identically to `decide`: the answer names the
/// outermost reason, or a prober learns that a tenant exists from a quota message
/// that leaked past a switch an operator pulled (§13.5).
///
/// `add_msgs` is what the call would push, and is ignored on every surface but
/// `EphPush`. The BYTE occupancy is deliberately NOT tested here: it is charged
/// atomically inside `Ephemeral::push`, where the bytes are, and a second test
/// out here would be a majorant that can disagree with the enforcer — the one
/// failure mode a quota must not have.
pub fn decide_ephemeral(
    sw: &Switches,
    eph: &crate::ephemeral::Ephemeral,
    tenant: &str,
    surface: Surface,
    add_msgs: i64,
) -> Answer {
    // RUNG 1 — the operator's runtime switch, on all four surfaces including the
    // status reads. Unlike the timer cancel (§9.6) there is nothing here that a
    // pause could strand: the contents are already legally lost (§1.2), so no
    // surface has to stay open to let a tenant recover from the pause itself.
    if !sw.eph_on() {
        return Answer::Paused;
    }
    // RUNGS 2 and 3 — the grant, and (push only) the per-tenant message rate.
    // The pop and the ack are on the grant but on nothing else: they are the only
    // ways for a full tenant to get back under its allowance, and a gate that
    // refused them would leave it stuck until the ttl or the idle collector, i.e.
    // the exact self-defeating shape §9.5 forbids for kv deletes.
    let v = match surface {
        Surface::EphPush => eph.gate_push(tenant, add_msgs),
        Surface::EphPop | Surface::EphAck | Surface::EphAdmin => eph.gate_grant(tenant),
        _ => {
            debug_assert!(false, "non-ephemeral surface in decide_ephemeral");
            Verdict::NotGranted
        }
    };
    if v == Verdict::Allow {
        Answer::Allow
    } else {
        Answer::Refused(v)
    }
}

// ===========================================================================
// Tests (PLAN_KV_TIMERS §15). Written before this module.
// ===========================================================================

#[cfg(test)]
#[path = "tests_unit/switch_levels.rs"]
mod switch_levels_tests;
