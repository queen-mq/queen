use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Per-queue wake for long-poll pops: a parked empty pop waits on the queue's
/// Notify; a push to that queue wakes it (analogue of libqueen's
/// update_pop_backoff_tracker wake path). notify_waiters() only wakes tasks
/// already waiting, so callers must register (notified()) BEFORE their pop
/// attempt to avoid lost wakeups.
#[derive(Clone, Default)]
pub struct Notifier {
    map: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl Notifier {
    pub fn new() -> Notifier {
        Notifier { map: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn get(&self, key: &str) -> Arc<Notify> {
        let mut m = self.map.lock().unwrap();
        if let Some(n) = m.get(key) {
            return n.clone();
        }
        let n = Arc::new(Notify::new());
        m.insert(key.to_string(), n.clone());
        n
    }

    pub fn notify(&self, key: &str) {
        let n = {
            let m = self.map.lock().unwrap();
            m.get(key).cloned()
        };
        if let Some(n) = n {
            n.notify_waiters();
        }
    }
}
