//! Optional consumer observations. A task is not an operating-system process.
use crate::{
    error::{Error, Result},
    http::Opts,
    inner::Inner,
    queue::QueueBuilder,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Opt into dashboard reporting for one consume invocation. `None` is off.
#[derive(Clone, Debug)]
pub struct SupervisionConfig {
    pub group: String,
}
impl SupervisionConfig {
    pub fn new(group: impl Into<String>) -> Self {
        Self {
            group: group.into(),
        }
    }
}

fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn id() -> String {
    crate::uuid::uuidv7_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
#[derive(Default)]
struct Stats {
    running: usize,
    completed: u64,
    failed: u64,
    sequence: u64,
    last: Option<u64>,
    active: BTreeMap<u64, Instant>,
}
pub(crate) struct ConsumerSupervision {
    inner: Arc<Inner>,
    group: String,
    id: String,
    queue: Option<String>,
    namespace: Option<String>,
    task: Option<String>,
    consumer_group: String,
    desired: usize,
    started: Instant,
    started_epoch: u64,
    stats: Mutex<Stats>,
    done: tokio::sync::Notify,
}
impl ConsumerSupervision {
    pub(crate) fn new(builder: &QueueBuilder) -> Result<Option<Arc<Self>>> {
        let Some(config) = &builder.supervision else {
            return Ok(None);
        };
        let valid = !config.group.is_empty()
            && config.group.len() <= 255
            && config.group != "coordination"
            && config.group.as_bytes()[0].is_ascii_alphanumeric()
            && config
                .group
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
        if !valid {
            return Err(Error::Invalid(
                "supervision group must be a valid application/deployment name".into(),
            ));
        }
        if builder.concurrency == 0 || builder.concurrency > 4096 {
            return Err(Error::Invalid(
                "supervision requires concurrency between 1 and 4096".into(),
            ));
        }
        Ok(Some(Arc::new(Self {
            inner: builder.inner.clone(),
            group: config.group.clone(),
            id: id(),
            queue: builder.queue.clone().filter(|v| !v.is_empty()),
            namespace: builder.namespace.clone().filter(|v| !v.is_empty()),
            task: builder.task.clone().filter(|v| !v.is_empty()),
            consumer_group: builder
                .group
                .clone()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "__QUEUE_MODE__".into()),
            desired: builder.concurrency,
            started: Instant::now(),
            started_epoch: epoch(),
            stats: Mutex::new(Stats {
                running: builder.concurrency,
                ..Stats::default()
            }),
            done: tokio::sync::Notify::new(),
        })))
    }
    fn document(&self, state: &str) -> Value {
        let s = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        json!({
            "schema": "queen.consumer.status/v1", "instance_id": self.id, "engine": "rust", "execution_model": "async-tasks",
            "hostname": std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).ok(), "pid": std::process::id(),
            "state": state, "updated_at_epoch": epoch(), "started_at_epoch": self.started_epoch, "uptime_seconds": self.started.elapsed().as_secs(),
            "configuration": {"heartbeat_timeout": 30},
            "pool_status": [{"name": "consumer", "queue": self.queue, "namespace": self.namespace, "task": self.task,
                "consumer_group": self.consumer_group, "desired": self.desired, "running": s.running, "busy": s.active.len(),
                "completed": s.completed, "failed": s.failed, "last_completed_at_epoch": s.last,
                "oldest_inflight_seconds": s.active.values().map(|t| t.elapsed().as_secs()).max()}]
        })
    }
    async fn publish(&self, state: &str) -> Result<()> {
        let bytes = serde_json::to_vec(&self.document(state))?;
        if bytes.len() > 45_000 {
            return Err(Error::Invalid("consumer status exceeds one chunk".into()));
        }
        let write = id();
        let slot = format!("{}/{}", self.group, self.id);
        let body = json!({"operations": [
            {"op":"put","ns":"queen-supervisor","key":format!("{slot}/head"),"ttlSeconds":60,
                "value":{"format":"queen.supervisor.remote-status/v1","write":write,"chunks":1,"bytes":bytes.len()}},
            {"op":"put","ns":"queen-supervisor","key":format!("{slot}/chunk/0000"),"ttlSeconds":60,
                "value":{"write":write,"index":0,"data":queen_protocol::timers::base64_encode(&bytes)}}
        ]});
        let opts = Opts::default().timeout(Duration::from_secs(2));
        let response: Option<Value> = tokio::time::timeout(
            Duration::from_secs(2),
            self.inner.http.post_json("/api/v1/kv", &body, &opts),
        )
        .await
        .map_err(|_| Error::Network("consumer status publication timed out".into()))??;
        let applied = response
            .as_ref()
            .and_then(|v| v["results"].as_array())
            .is_some_and(|r| r.len() == 2 && r.iter().all(|v| v["applied"] == true));
        if !applied {
            return Err(Error::Decode(
                "consumer status publication was not applied".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut failing = false;
            loop {
                let stopped = this.stats.lock().unwrap_or_else(|e| e.into_inner()).running == 0;
                if this
                    .publish(if stopped { "stopped" } else { "running" })
                    .await
                    .is_err()
                {
                    if !failing {
                        tracing::warn!("Consumer status publication failed; consumption continues");
                    }
                    failing = true;
                } else {
                    failing = false;
                }
                if stopped {
                    break;
                }
                tokio::select! {
                    _ = this.done.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {},
                }
            }
        })
    }
    pub(crate) fn worker(self: &Arc<Self>) -> WorkerGuard {
        WorkerGuard(self.clone())
    }
    pub(crate) fn handler(self: &Arc<Self>) -> HandlerGuard {
        let mut stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        let id = stats.sequence;
        stats.sequence += 1;
        stats.active.insert(id, Instant::now());
        HandlerGuard {
            owner: self.clone(),
            id,
            success: false,
        }
    }
}
pub(crate) struct WorkerGuard(Arc<ConsumerSupervision>);
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let mut s = self.0.stats.lock().unwrap_or_else(|e| e.into_inner());
        s.running -= 1;
        if s.running == 0 {
            self.0.done.notify_one();
        }
    }
}
pub(crate) struct HandlerGuard {
    owner: Arc<ConsumerSupervision>,
    id: u64,
    success: bool,
}
impl HandlerGuard {
    pub(crate) fn finish(mut self, success: bool) {
        self.success = success;
    }
}
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        let mut s = self.owner.stats.lock().unwrap_or_else(|e| e.into_inner());
        s.active.remove(&self.id);
        s.last = Some(epoch());
        if self.success {
            s.completed += 1;
        } else {
            s.failed += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guards_observe_progress_and_task_exit_even_during_unwinding() {
        let queen = crate::Queen::connect_to("http://127.0.0.1:1").unwrap();
        let builder = queen
            .queue("orders")
            .concurrency(2)
            .supervision(Some(SupervisionConfig::new("billing")));
        let reporter = ConsumerSupervision::new(&builder).unwrap().unwrap();
        let other = ConsumerSupervision::new(&builder).unwrap().unwrap();
        assert_ne!(reporter.id, other.id);
        let worker = reporter.worker();
        let handler = reporter.handler();
        assert_eq!(reporter.document("running")["pool_status"][0]["busy"], 1);
        handler.finish(true);
        let failed = reporter.handler();
        drop(failed);
        drop(worker);
        let pool = reporter.document("running")["pool_status"][0].clone();
        assert_eq!(pool["running"], 1);
        assert_eq!(pool["busy"], 0);
        assert_eq!(pool["completed"], 1);
        assert_eq!(pool["failed"], 1);
        assert!(pool["oldest_inflight_seconds"].is_null());
        assert!(pool["last_completed_at_epoch"].is_u64());
    }
}
