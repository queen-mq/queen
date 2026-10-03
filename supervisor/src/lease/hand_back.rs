//! Hand back the jobs a crashed worker had prefetched, without charging them
//! an attempt.
//!
//! A worker with prefetch holds a lease on a batch and runs its jobs one by
//! one. When it stops gracefully it hands the unstarted ones back itself: one
//! transaction completes each and pushes a copy that records the runs so far.
//! A worker that crashes (SIGKILL, out of memory, a PHP fatal error) cannot,
//! and the lease's expiry would deliver every unstarted job again with one
//! attempt more, so a job with `tries = 1` would fail without running.
//!
//! So the worker journals that transaction in the state directory, where this
//! service finds it by the worker's PID, and the service sends it once the
//! worker has exited while still holding the lease:
//!
//! ```text
//! hand-back-<pid>.plan   {"lease_id":"..","entries":[{"ack":{..},"unstarted":{..},"ran":{..}}, ..]}
//! hand-back-<pid>.state  {"lease_id":"..","entries":"cur-.."}
//! ```
//!
//! The plan is written once per batch, with each delivery's completed ACK and
//! the two copies its hand-back may push. The state holds one code per entry,
//! rewritten in place before each change of what the worker owes: `-` nothing,
//! `c` the ACK alone (a finished job whose ACK was deferred), `u` the ACK and
//! the unstarted copy, `r` the ACK and the copy that counts the run (the job
//! that was running, ahead of unstarted jobs in its partition lease). The
//! worker removes from the state a job whose ACK or release it is about to
//! send, so the journal never hands back a job the broker may have settled.
//!
//! Every ACK names the lease, and the broker refuses the whole transaction if
//! that lease is no longer this worker's: a hand-back is never sent after the
//! lease may have expired.

use serde::Deserialize;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

/// A batch of up to 1000 deliveries, each with two copies of its payload.
const MAX_PLAN_BYTES: u64 = 16 * 1024 * 1024;
/// One code per delivery and the lease ID: written by one write to one page.
const MAX_STATE_BYTES: u64 = 4096;

pub(super) struct Journal {
    plan: PathBuf,
    state: PathBuf,
}

#[derive(Deserialize)]
struct Plan {
    lease_id: String,
    entries: Vec<Entry>,
}

#[derive(Deserialize)]
struct Entry {
    ack: serde_json::Value,
    unstarted: serde_json::Value,
    ran: serde_json::Value,
}

#[derive(Deserialize)]
struct State {
    lease_id: String,
    entries: String,
}

/// The transaction to send, and the jobs it hands back: those that never
/// started, and the one that was running, with its run counted.
#[derive(Debug)]
pub(super) struct HandBack {
    pub(super) body: serde_json::Value,
    pub(super) unstarted: usize,
    pub(super) running: usize,
}

impl Journal {
    pub(super) fn of(directory: &Path, pid: u32) -> Self {
        Self {
            plan: directory.join(format!("hand-back-{pid}.plan")),
            state: directory.join(format!("hand-back-{pid}.state")),
        }
    }

    /// What the worker journaled for `lease_id`; None when it owes nothing.
    pub(super) fn transaction(&self, lease_id: &str) -> Result<Option<HandBack>, String> {
        let Some(state) = read(&self.state, MAX_STATE_BYTES)? else {
            return Ok(None);
        };
        // An empty state: a batch planned, nothing owed yet.
        if state.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        let state: State =
            serde_json::from_slice(&state).map_err(|_| "invalid hand-back state".to_owned())?;
        if state.lease_id != lease_id || state.entries.bytes().all(|code| code == b'-') {
            return Ok(None);
        }
        let plan = read(&self.plan, MAX_PLAN_BYTES)?
            .ok_or_else(|| "the hand-back plan is missing".to_owned())?;
        let plan: Plan =
            serde_json::from_slice(&plan).map_err(|_| "invalid hand-back plan".to_owned())?;
        if plan.lease_id != lease_id || plan.entries.len() != state.entries.len() {
            return Err("the hand-back plan does not match its state".into());
        }

        let mut operations = Vec::new();
        let (mut unstarted, mut running) = (0, 0);
        for (code, entry) in state.entries.bytes().zip(plan.entries) {
            let copy = match code {
                b'-' => continue,
                b'c' => None,
                b'u' => {
                    unstarted += 1;
                    Some(entry.unstarted)
                }
                b'r' => {
                    running += 1;
                    Some(entry.ran)
                }
                _ => return Err("invalid hand-back state".into()),
            };
            if !is_ack_of(&entry.ack, lease_id) {
                return Err("a hand-back ACK does not name the lease".into());
            }
            operations.push(entry.ack);
            if let Some(copy) = copy {
                if copy.get("type").and_then(serde_json::Value::as_str) != Some("push") {
                    return Err("a hand-back copy is not a push".into());
                }
                operations.push(copy);
            }
        }

        Ok(Some(HandBack {
            body: serde_json::json!({
                "operations": operations,
                "requiredLeases": [lease_id],
            }),
            unstarted,
            running,
        }))
    }

    pub(super) fn remove(&self) {
        let mut temporary = self.plan.clone().into_os_string();
        temporary.push(".tmp");
        for path in [&self.plan, &self.state, &PathBuf::from(temporary)] {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Remove every journal in the state directory: a new master has no worker
/// yet, and the workers of the one before died with it.
pub(super) fn remove_all(directory: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("hand-back-")
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// A completed ACK fenced by the lease: the broker refuses it, and the whole
/// transaction, once the lease is no longer the worker's.
fn is_ack_of(operation: &serde_json::Value, lease_id: &str) -> bool {
    let field = |name: &str| operation.get(name).and_then(serde_json::Value::as_str);
    field("type") == Some("ack")
        && field("status") == Some("completed")
        && field("leaseId") == Some(lease_id)
}

fn read(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "the hand-back journal cannot be read: {}",
                error.kind()
            ))
        }
    };
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("the hand-back journal cannot be read: {}", error.kind()))?;
    if bytes.len() as u64 > limit {
        return Err("the hand-back journal is too large".into());
    }
    Ok(Some(bytes))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn ack(index: usize, lease: &str) -> serde_json::Value {
        serde_json::json!({
            "type": "ack", "transactionId": format!("t{index}"), "partitionId": "p",
            "status": "completed", "consumerGroup": "laravel", "leaseId": lease,
        })
    }

    pub(crate) fn copy(index: usize, attempts: u32) -> serde_json::Value {
        serde_json::json!({"type": "push", "items": [{
            "queue": "default", "partition": "laravel-1", "transactionId": format!("c{index}"),
            "payload": {"job": "J", "_queen": {"attempts": attempts}},
        }]})
    }

    /// Journal `count` deliveries of `lease` and the codes in `state`.
    pub(crate) fn journal(directory: &Path, pid: u32, lease: &str, count: usize, state: &str) {
        let entries: Vec<_> = (0..count)
            .map(|index| serde_json::json!({"ack": ack(index, lease), "unstarted": copy(index, 0), "ran": copy(index, 1)}))
            .collect();
        std::fs::write(
            directory.join(format!("hand-back-{pid}.plan")),
            serde_json::json!({"lease_id": lease, "entries": entries}).to_string(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("hand-back-{pid}.state")),
            format!(
                "{}    \n",
                serde_json::json!({"lease_id": lease, "entries": state})
            ),
        )
        .unwrap();
    }

    fn directory(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!("qhb-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn the_state_picks_each_entrys_operations_in_order() {
        let directory = directory("pick");
        journal(&directory, 7, "L", 4, "cru-");

        let hand_back = Journal::of(&directory, 7)
            .transaction("L")
            .unwrap()
            .unwrap();
        assert_eq!((hand_back.unstarted, hand_back.running), (1, 1));
        assert_eq!(
            hand_back.body,
            serde_json::json!({
                "operations": [ack(0, "L"), ack(1, "L"), copy(1, 1), ack(2, "L"), copy(2, 0)],
                "requiredLeases": ["L"],
            })
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn nothing_is_handed_back_without_a_state_for_the_lease_or_with_nothing_owed() {
        let directory = directory("nothing");
        let journal_of = Journal::of(&directory, 7);
        assert!(journal_of.transaction("L").unwrap().is_none(), "no journal");

        journal(&directory, 7, "L", 2, "--");
        assert!(
            journal_of.transaction("L").unwrap().is_none(),
            "nothing owed"
        );

        std::fs::write(directory.join("hand-back-7.state"), "").unwrap();
        assert!(
            journal_of.transaction("L").unwrap().is_none(),
            "planned, nothing owed yet"
        );

        journal(&directory, 7, "earlier", 2, "uu");
        assert!(
            journal_of.transaction("L").unwrap().is_none(),
            "another lease"
        );

        std::fs::write(directory.join("hand-back-7.plan.tmp"), "{").unwrap();
        journal_of.remove();
        assert!(!directory.join("hand-back-7.plan").exists());
        assert!(!directory.join("hand-back-7.state").exists());
        assert!(!directory.join("hand-back-7.plan.tmp").exists());

        journal(&directory, 8, "L", 1, "u");
        std::fs::write(directory.join("lease.sock.keep"), "").unwrap();
        remove_all(&directory);
        let left: Vec<_> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(left, vec!["lease.sock.keep".to_owned()]);
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_journal_that_does_not_hold_together_is_refused() {
        let directory = directory("refused");
        let journal_of = Journal::of(&directory, 7);

        // A state for a new batch, the plan still the previous one.
        journal(&directory, 7, "L", 2, "uu");
        std::fs::write(
            directory.join("hand-back-7.state"),
            serde_json::json!({"lease_id": "L", "entries": "uuu"}).to_string(),
        )
        .unwrap();
        assert!(journal_of.transaction("L").is_err());

        journal(&directory, 7, "L", 2, "ux");
        assert!(journal_of.transaction("L").is_err(), "an unknown code");

        // A torn state: trailing bytes of a longer, earlier record.
        std::fs::write(
            directory.join("hand-back-7.state"),
            r#"{"lease_id":"L","entries":"uu"}"u"}"#,
        )
        .unwrap();
        assert!(journal_of.transaction("L").is_err());

        // An ACK that the broker would not fence by this lease.
        journal(&directory, 7, "L", 1, "u");
        let plan = serde_json::json!({"lease_id": "L", "entries": [
            {"ack": ack(0, "other"), "unstarted": copy(0, 0), "ran": copy(0, 1)},
        ]});
        std::fs::write(directory.join("hand-back-7.plan"), plan.to_string()).unwrap();
        assert!(journal_of.transaction("L").is_err());

        let _ = std::fs::remove_dir_all(&directory);
    }
}
