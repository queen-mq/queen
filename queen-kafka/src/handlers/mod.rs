//! One module per Kafka API. A handler owns the semantics of its API and
//! nothing else: it takes the decoded request and returns the response body
//! together with the version that body must be ENCODED at (normally the request
//! version — see `api_versions` for the one case where it is not). The framing,
//! the headers and the dispatch live in `conn`.
//!
//! ## The error codes, in one table
//!
//! `compat/ERRORS.md` is the inventory: every non-zero code each API here can
//! put on the wire, where it comes from, and whether a client retries it. It is
//! the M6 error-code audit written down, and the thing to read before adding a
//! code — a code that reads as more precise but is not on the CLOSED set the
//! client accepts for that API ends the application rather than making it
//! recover. The rule that came out of the audit lives in
//! [`metadata::not_a_topic_here`].

pub mod api_versions;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod join_group;
pub mod leave_group;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_fetch;
pub mod produce;
pub mod sasl_authenticate;
pub mod sasl_handshake;
pub mod sync_group;

/// Fixtures shared by the handler tests. One place to build a [`crate::Facade`]
/// so that a new field on it does not have to be invented eleven times, and so
/// the group knobs the tests want (an instant join window — the clock is not
/// what these tests are about) are set once.
#[cfg(test)]
pub mod testing {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::coordinator::{Coordinator, GroupConfig};
    use crate::queen::testing::FakeQueen;
    use crate::queen::QueenApi;
    use crate::{Facade, Policy};

    /// A facade over a fake Queen holding `queues`, as `(name, live lanes)`.
    pub fn facade(queues: &[(&str, i64)]) -> Facade {
        with_queen(FakeQueen::with(queues))
    }

    /// The same, with the double kept so a test can inspect what was asked.
    pub fn facade_and_queen(queues: &[(&str, i64)]) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        (with_queen(Arc::clone(&api)), api)
    }

    fn with_queen(api: Arc<FakeQueen>) -> Facade {
        over(api as Arc<dyn QueenApi>, Policy::default())
    }

    /// The full constructor, for the tests that care about the listener policy
    /// or about the lanes — everything else takes the two helpers above.
    pub fn over(api: Arc<dyn QueenApi>, policy: Policy) -> Facade {
        Facade::new(
            "kafka.example.com".into(),
            9092,
            4,
            None,
            api,
            Coordinator::new(GroupConfig {
                // The join window is the coordinator's own subject and is
                // tested there, under a paused clock. A handler test asks a
                // different question — does this request become that answer —
                // and a three-second wait inside every one of them would be
                // three seconds of nothing.
                join_delay: Duration::ZERO,
                ..GroupConfig::default()
            }),
            policy,
        )
    }
}
