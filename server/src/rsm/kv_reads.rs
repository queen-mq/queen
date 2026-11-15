//! A KV call's reads, answered at the call's own position in the log.
//!
//! A call that writes is ONE command; its reads used to be evaluated by the
//! receiver against the node's applied state once the call's entry had
//! applied — by then later commands (of the same entry or of later ones) may
//! have applied too, so two calls could each read the other's write (Jepsen
//! W4: 19 G1c cycles in a no-fault run, below read-committed). The receiver
//! now registers the call here, under its request id, before submitting it;
//! apply renders the whole answer right after the LAST effect of that command,
//! before any later command's effect, and leaves it here for the receiver to
//! take. The read bytes never enter the log (D7).
//!
//! A transaction whose KV rider reads registers the same way (2026-11-15):
//! its reads, read off the state once its entry had applied, saw the writes
//! of commands planned after it (Jepsen W32: 141 G1c in a run with no
//! faults). Every such transaction logs a no-op last (`TxnCommand::reads`),
//! so it has a position even when nothing else of it is logged, and apply
//! renders its rider there as its receiver did (no ttl judgment: the
//! transaction committed, and says so).
//!
//! A call with a put that sets a ttl registers too, whether it reads or not
//! (2026-11-14): the expiry of ITS write can only be read from the row at its
//! own position — later, the row may be another write's — and the receiver
//! needs it to tell a write that landed already expired
//! ([`crate::rsm::planner::kv::render_answer`]).
//!
//! Process-wide: request ids are unique, and every node's state after a given
//! command is the same, so whichever apply thread in the process gets there
//! first may render it (an in-process test cluster runs several nodes).
//! A call answered from committed state alone (a request-id hit) has no entry
//! to render at: its receiver reads as before.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use crate::rsm::entry::RequestId;
use crate::rsm::planner::kv::KvOp;

struct Pending {
    tenant: Arc<str>,
    ops: Arc<Vec<KvOp>>,
    /// The answer goes to an HTTP client: stored values rendered raw.
    raw: bool,
    /// Apply rendered it (the answer may have been taken since).
    rendered: bool,
    answer: Option<Result<Vec<Value>, String>>,
}

/// The rendezvous between a KV call's receiver and apply.
#[derive(Default)]
pub struct KvReads {
    pending: Mutex<HashMap<RequestId, Pending>>,
    /// Registered calls not rendered yet: apply's one-load check per entry.
    waiting: AtomicUsize,
}

/// The process's rendezvous.
pub fn global() -> &'static KvReads {
    static R: OnceLock<KvReads> = OnceLock::new();
    R.get_or_init(KvReads::default)
}

/// A registered call; dropping it forgets the call, rendered or not.
pub struct Registration {
    id: RequestId,
}

impl Registration {
    /// The answer apply rendered at the call's position, once it has.
    pub fn take(&self) -> Option<Result<Vec<Value>, String>> {
        global()
            .pending
            .lock()
            .expect("kv reads")
            .get_mut(&self.id)
            .and_then(|p| p.answer.take())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut g = global().pending.lock().expect("kv reads");
        if let Some(p) = g.remove(&self.id) {
            if !p.rendered {
                global().waiting.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

impl KvReads {
    /// Register a call's reads before it is submitted. `None` when the id is
    /// already registered (a retry racing its original): that call reads at
    /// the receiver, as before.
    pub fn register(
        &self,
        id: RequestId,
        tenant: &str,
        ops: &[KvOp],
        raw: bool,
    ) -> Option<Registration> {
        let mut g = self.pending.lock().expect("kv reads");
        if g.contains_key(&id) {
            return None;
        }
        g.insert(
            id,
            Pending {
                tenant: Arc::from(tenant),
                ops: Arc::new(ops.to_vec()),
                raw,
                rendered: false,
                answer: None,
            },
        );
        self.waiting.fetch_add(1, Ordering::AcqRel);
        Some(Registration { id })
    }

    /// Apply: whether any call waits for its answer.
    pub fn any_waiting(&self) -> bool {
        self.waiting.load(Ordering::Acquire) > 0
    }

    /// Apply: the call `id`, if it still waits for its answer.
    pub fn call(&self, id: &RequestId) -> Option<(Arc<str>, Arc<Vec<KvOp>>, bool)> {
        let g = self.pending.lock().expect("kv reads");
        g.get(id)
            .filter(|p| !p.rendered)
            .map(|p| (p.tenant.clone(), p.ops.clone(), p.raw))
    }

    /// Apply: the call's answer, rendered at its position.
    pub fn answer(&self, id: &RequestId, answer: Result<Vec<Value>, String>) {
        let mut g = self.pending.lock().expect("kv reads");
        if let Some(p) = g.get_mut(id) {
            if !p.rendered {
                p.rendered = true;
                p.answer = Some(answer);
                self.waiting.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}
