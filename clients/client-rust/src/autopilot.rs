//! Pop autopilot, client side.
//!
//! The broker owns a controller that sizes a pop from state this client cannot
//! see: how many partitions of the (queue, group) are ready, how old their
//! oldest ready message is, at what rate messages are arriving. Two knobs are
//! under its control — `partitions` (the sweep width) and `batch` (the message
//! budget for the sweep).
//!
//! THE RULE, and it is the only one: an explicit user value is sacred.
//! Autopilot applies ONLY to the knobs the caller left unset, and it applies to
//! them one by one. A consumer that pins [`crate::QueueBuilder::partitions`] to
//! 1 and says nothing about batch keeps its single-partition claim forever and
//! lets the broker size the batch; the pinned dimension is never "adjusted", not
//! even towards a value the controller would consider better.
//!
//! The wire shape follows the conflation precedent: a client that is not
//! engaging autopilot sends the byte-identical request it sent before this
//! feature existed. `autopilot=true` is emitted only when at least one knob is
//! being left to the broker, never as `autopilot=false`, and the delegated knobs
//! are simply omitted. The emission itself lives in
//! [`queen_protocol::PopParams::to_pairs`], which both param sites of this SDK
//! already go through — unlike the other clients, there is only ever one copy of
//! it here.
//!
//! WHAT AN OLD BROKER DOES, and why there is no capability check. A broker older
//! than 1.2 ignores unknown query params: the request succeeds, and the omitted
//! knobs fall back to the SERVER-side defaults (batch 200, partitions 1) instead
//! of the old client-side ones. That is a sizing difference, not a correctness
//! one — nothing is lost, misordered or delivered twice — so unlike conflation
//! (which silently hands a last-value consumer a whole backlog, hence
//! [`crate::queue::CONFLATION_UNSUPPORTED`]) this degrades quietly and on
//! purpose. Callers who need the old numbers against an old broker set them
//! explicitly, or turn autopilot off.

use std::time::Duration;

/// The environment variable that disables pop autopilot for a whole process:
/// `QUEEN_SDK_POP_AUTOPILOT=off` restores the client-side defaults this SDK
/// applied before autopilot existed, byte for byte. It is read once, in
/// [`crate::Queen::connect`], so a single deployment can be rolled back without
/// touching code.
///
/// `off`, `false`, `0`, `no` and `disabled` all disable it (case-insensitive,
/// surrounding space ignored). Every other value, including the empty one,
/// leaves autopilot on.
pub const ENV_POP_AUTOPILOT: &str = "QUEEN_SDK_POP_AUTOPILOT";

/// Whether [`ENV_POP_AUTOPILOT`] asks for the pre-autopilot behaviour.
pub(crate) fn disabled_by_env() -> bool {
    disabled_by_value(&std::env::var(ENV_POP_AUTOPILOT).unwrap_or_default())
}

/// The vocabulary, split out from the lookup so it can be tested without
/// mutating a process-wide variable other tests are reading in parallel.
pub(crate) fn disabled_by_value(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "off" | "false" | "0" | "no" | "disabled"
    )
}

/// The client-side defaults this SDK applied before autopilot, and the ones it
/// applies again when autopilot is off.
pub(crate) const DEFAULT_BATCH: i32 = 1;
pub(crate) const DEFAULT_PARTITIONS: i32 = 1;

/// What travels for one pop's two sizing knobs.
///
/// `(autopilot, batch, partitions)`, where `None` means "this key does not
/// travel" — the dimension the broker is choosing.
pub(crate) struct Sizing {
    pub autopilot: Option<bool>,
    pub batch: Option<i32>,
    pub partitions: Option<i32>,
}

/// Resolve the sizing of one pop from the caller's (possibly absent) values.
///
/// `batch` and `partitions` are the USER's own: `None` means the setter was
/// never called, which is exactly what autopilot acts on. Note the case that
/// looks like an omission and is not — when both are set there is nothing left
/// for the controller to decide, so `autopilot=true` is NOT emitted and the
/// request is byte-identical to the one this SDK sent before autopilot existed.
pub(crate) fn sizing(batch: Option<i32>, partitions: Option<i32>, autopilot: bool) -> Sizing {
    if autopilot && !(batch.is_some() && partitions.is_some()) {
        return Sizing {
            autopilot: Some(true),
            batch,
            partitions,
        };
    }
    Sizing {
        autopilot: None,
        batch: Some(batch.unwrap_or(DEFAULT_BATCH)),
        partitions: Some(partitions.unwrap_or(DEFAULT_PARTITIONS)),
    }
}

/// The sleep the consume loop has always taken between two empty pops that are
/// NOT long-polling. A waiting pop already blocks on the broker, so it never
/// reaches here.
pub(crate) const EMPTY_POLL_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait after an empty pop: the broker's advice when it gave one,
/// the historical constant otherwise.
///
/// The advice is honoured as given, without a ceiling of this client's
/// invention. The sleep it feeds is inside a `tokio::select!`-able task that the
/// consumer's stop flag and cancel token both bound, so even an absurd value
/// cannot outlive a cancellation — which is the only property that has to hold
/// locally.
pub(crate) fn empty_poll_delay(echo: Option<&queen_protocol::AutopilotEcho>) -> Duration {
    match echo.and_then(|e| e.wait_ms) {
        Some(ms) if ms > 0 => Duration::from_millis(ms),
        _ => EMPTY_POLL_BACKOFF,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_value_is_sacred_per_dimension() {
        // Nothing set: both knobs go to the broker, neither travels.
        let s = sizing(None, None, true);
        assert_eq!(s.autopilot, Some(true));
        assert_eq!(s.batch, None);
        assert_eq!(s.partitions, None);

        // One pinned, one delegated — including the pin at 1, which used to be
        // indistinguishable from unset.
        let s = sizing(None, Some(1), true);
        assert_eq!(s.autopilot, Some(true));
        assert_eq!(s.batch, None);
        assert_eq!(s.partitions, Some(1));

        let s = sizing(Some(50), None, true);
        assert_eq!(s.autopilot, Some(true));
        assert_eq!(s.batch, Some(50));
        assert_eq!(s.partitions, None);
    }

    #[test]
    fn both_set_leaves_nothing_to_decide_so_the_flag_does_not_travel() {
        let s = sizing(Some(50), Some(4), true);
        assert_eq!(s.autopilot, None);
        assert_eq!(s.batch, Some(50));
        assert_eq!(s.partitions, Some(4));
    }

    #[test]
    fn off_restores_the_client_side_defaults() {
        let s = sizing(None, None, false);
        assert_eq!(s.autopilot, None);
        assert_eq!(s.batch, Some(DEFAULT_BATCH));
        assert_eq!(s.partitions, Some(DEFAULT_PARTITIONS));
    }

    #[test]
    fn the_environment_vocabulary() {
        for v in ["off", "OFF", " off ", "false", "0", "no", "disabled"] {
            assert!(disabled_by_value(v), "{v:?} should disable autopilot");
        }
        for v in ["", "on", "true", "1", "yes", "nonsense"] {
            assert!(!disabled_by_value(v), "{v:?} should leave autopilot on");
        }
    }

    #[test]
    fn the_pacing_advice_is_honoured_and_optional() {
        assert_eq!(empty_poll_delay(None), EMPTY_POLL_BACKOFF);
        let none = queen_protocol::AutopilotEcho {
            partitions: 1,
            batch: 1,
            wait_ms: None,
        };
        assert_eq!(empty_poll_delay(Some(&none)), EMPTY_POLL_BACKOFF);
        let advised = queen_protocol::AutopilotEcho {
            partitions: 1,
            batch: 1,
            wait_ms: Some(250),
        };
        assert_eq!(empty_poll_delay(Some(&advised)), Duration::from_millis(250));
    }
}
