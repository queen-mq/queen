//! The process-wide rollback, `QUEEN_SDK_POP_AUTOPILOT`.
//!
//! Its own test binary on purpose. The switch is an environment variable read
//! at `Queen::connect`, and an environment is process-wide: setting it from one
//! of several tests running in parallel threads would change the wire shape
//! under the others. Cargo gives each integration file its own process, so this
//! file holds exactly one test and mutates the variable alone.
//!
//! The vocabulary itself ("off", "false", "0", ...) is unit-tested in
//! `src/autopilot.rs` against a plain string, where no process state is
//! involved. What is left to prove here is the PLUMBING: that the variable is
//! read at all, that it is read once at connect rather than per pop, and that an
//! explicit `.autopilot(..)` outranks it.

mod support;

use queen_mq::{Config, Queen, ENV_POP_AUTOPILOT};
use support::{FakeBroker, Reply};

const EMPTY_POP: &str = concat!(
    r#"{"success":true,"queue":"orders","partition":"","partitionId":"","leaseId":"","#,
    r#""consumerGroup":"workers","messages":[],"partitionsClaimed":0}"#,
);

fn query_of(broker: &FakeBroker) -> String {
    let hit = broker.hits().remove(0);
    hit.path
        .split_once('?')
        .map(|(_, q)| q.to_string())
        .unwrap_or_default()
}

const TAIL: &str = "wait=false&timeout=30000&consumerGroup=workers";

#[tokio::test]
async fn the_environment_switch_is_read_once_at_connect() {
    // 1. A client built while the variable is set sends the pre-autopilot
    //    request: the client-side defaults are back, byte for byte.
    std::env::set_var(ENV_POP_AUTOPILOT, "off");
    let broker = FakeBroker::start(vec![Reply::ok(EMPTY_POP)]).await;
    let rolled_back = Queen::connect(Config::new(broker.url())).expect("url");
    std::env::remove_var(ENV_POP_AUTOPILOT);

    rolled_back
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop()
        .await
        .expect("pop");
    assert_eq!(query_of(&broker), format!("batch=1&{TAIL}"));

    // 2. ...and it stays rolled back after the variable is gone: this is a
    //    deployment-level switch, not a per-request one.
    let broker2 = FakeBroker::start(vec![Reply::ok(EMPTY_POP)]).await;
    let live = Queen::connect(Config::new(broker2.url())).expect("url");
    rolled_back
        .queue("orders")
        .group("workers")
        .wait(false)
        .pop()
        .await
        .expect("pop");
    assert_eq!(
        query_of(&broker),
        format!("batch=1&{TAIL}"),
        "unsetting the variable must not move a client that already read it"
    );

    // 3. A client built after it was unset is back on autopilot.
    live.queue("orders")
        .group("workers")
        .wait(false)
        .pop()
        .await
        .expect("pop");
    assert_eq!(query_of(&broker2), format!("autopilot=true&{TAIL}"));

    // 4. An explicit .autopilot(true) outranks the environment: the variable is
    //    a default, not a lock.
    let broker3 = FakeBroker::start(vec![Reply::ok(EMPTY_POP)]).await;
    std::env::set_var(ENV_POP_AUTOPILOT, "off");
    let pinned_on = Queen::connect(Config::new(broker3.url())).expect("url");
    std::env::remove_var(ENV_POP_AUTOPILOT);

    pinned_on
        .queue("orders")
        .group("workers")
        .wait(false)
        .autopilot(true)
        .pop()
        .await
        .expect("pop");
    assert_eq!(query_of(&broker3), format!("autopilot=true&{TAIL}"));
}
