//! Administrative and observability endpoints.
//!
//! These back dashboards and operator tooling rather than application traffic.
//! Most return the broker's JSON verbatim as [`serde_json::Value`]: the shapes
//! are report-shaped, they change with the dashboard, and typing them here
//! would turn every cosmetic addition into a client release. The endpoints
//! whose shape is part of the contract — DLQ, lease renewal, maintenance — are
//! typed.

use std::sync::Arc;

use queen_protocol::{
    DlqParams, DlqResponse, MaintenanceRequest, MaintenanceResponse, RenewLeaseRequest,
    RenewLeaseResponse, SeekRequest, SubscriptionRequest, TraceRequest, TraceResponse,
};

use crate::error::Result;
use crate::http::Opts;
use crate::inner::Inner;
use crate::queue::{encode_pairs, urlencode};

/// Administrative API. Obtained from [`crate::Queen::admin`].
pub struct Admin {
    inner: Arc<Inner>,
}

impl Admin {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    async fn get(&self, path: &str) -> Result<serde_json::Value> {
        let out: Option<serde_json::Value> =
            self.inner.http.get_json(path, &Opts::default()).await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    async fn post<B: serde::Serialize>(&self, path: &str, body: &B) -> Result<serde_json::Value> {
        let out: Option<serde_json::Value> = self
            .inner
            .http
            .post_json(path, body, &Opts::default())
            .await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    async fn delete(&self, path: &str) -> Result<serde_json::Value> {
        let out: Option<serde_json::Value> =
            self.inner.http.delete_json(path, &Opts::default()).await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    // ---------------------------------------------------------- resources

    pub async fn overview(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/resources/overview").await
    }

    pub async fn namespaces(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/resources/namespaces").await
    }

    pub async fn tasks(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/resources/tasks").await
    }

    pub async fn list_queues(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/resources/queues", params))
            .await
    }

    pub async fn queue(&self, name: &str) -> Result<serde_json::Value> {
        self.get(&format!("/api/v1/resources/queues/{}", urlencode(name)))
            .await
    }

    /// Per-partition backlog for one queue — the cheap sibling of
    /// [`Admin::queue`]: watermark arithmetic only, no segments, no
    /// timestamps. Shape: `{queue, group, pending, partitionsPending,
    /// conflation, effectivePending, partitions: [{partition, pending}]}`.
    /// `group: None` is queue-level pending under the same
    /// worst-cursor precedence the dashboard publishes; `Some(g)` is that
    /// group's own backlog per partition (a group with no cursor on a
    /// partition owes its whole retained range). Requires broker >= 1.0.4 —
    /// an older broker answers 404 `no_such_route`, so a caller that must run
    /// against both falls back to [`Admin::queue`] on that error.
    ///
    /// # Reading the depth of a conflating group (broker >= 1.1.0)
    ///
    /// `pending` is **log depth**: positions left to retire. For a group with
    /// [`crate::QueueBuilder::conflation`] on, that is not the amount of work
    /// left, because one pop of a partition retires its whole backlog and runs
    /// the handler once. The three fields that make the difference readable:
    ///
    /// * `partitionsPending` — partitions that owe anything at all. Useful for
    ///   every group; `queenctl` used to compute it client-side.
    /// * `conflation` — the group's stored policy, `false` for queue-level depth
    ///   and for a group that never registered.
    /// * `effectivePending` — **work depth**: handler invocations remaining.
    ///   Equal to `partitionsPending` when the group conflates and to `pending`
    ///   when it does not.
    ///
    /// So `pending: 4_000_000, effectivePending: 12` is a healthy conflating
    /// queue; the same two numbers on a non-conflating group are an incident.
    /// An older broker simply omits the three keys.
    pub async fn queue_depth(
        &self,
        name: &str,
        group: Option<&str>,
    ) -> Result<serde_json::Value> {
        let path = format!("/api/v1/resources/queues/{}/depth", urlencode(name));
        match group {
            Some(g) => self.get(&with_query(&path, &[("group", g.to_string())])).await,
            None => self.get(&path).await,
        }
    }

    /// # This broker does not route it
    ///
    /// `/api/v1/resources/partitions` is registered nowhere in the broker, so
    /// every call answers 404 `no_such_route` — the same trap as the JS SDK's
    /// `clearQueue` and `moveMessageToDlq`, which this client does not offer
    /// for exactly that reason. It is kept, and pinned by
    /// `the_partitions_resource_is_not_a_route_on_this_broker`, only because
    /// removing a public method is a breaking change; use
    /// [`Admin::queue`] for a single queue's partition counts.
    pub async fn partitions(&self, params: &[(&'static str, String)]) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/resources/partitions", params))
            .await
    }

    // NOTE: there is deliberately no `clear_queue` here. The JS SDK exposes one
    // that calls `DELETE /api/v1/queues/{name}/clear`, but this broker has no
    // such route — the call answers 404 `no_such_route`. Use
    // `QueueBuilder::delete` to drop the queue, or seek the consumer group
    // forward to skip its backlog.

    // ----------------------------------------------------------- messages

    pub async fn list_messages(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/messages", params)).await
    }

    pub async fn message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value> {
        self.get(&format!(
            "/api/v1/messages/{}/{}",
            urlencode(partition_id),
            urlencode(transaction_id)
        ))
        .await
    }

    pub async fn delete_message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value> {
        self.delete(&format!(
            "/api/v1/messages/{}/{}",
            urlencode(partition_id),
            urlencode(transaction_id)
        ))
        .await
    }

    /// Replay a **dead-lettered** message: re-push the DLQ snapshot and drop
    /// the DLQ row.
    ///
    /// This is not a generic "retry a failed message" — it only applies to a
    /// message already in the dead-letter queue. Calling it on a live message
    /// errors.
    pub async fn retry_message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value> {
        let path = format!(
            "/api/v1/messages/{}/{}/retry",
            urlencode(partition_id),
            urlencode(transaction_id)
        );
        let out: Option<serde_json::Value> =
            self.inner.http.post_empty(&path, &Opts::default()).await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    // NOTE: no `move_message_to_dlq` either. The JS SDK posts to
    // `/api/v1/messages/{pid}/{txn}/dlq`, which this broker does not route.
    // Dead-letter a message by acking it with `AckStatus::Dlq`, which does.

    /// Read the dead-letter queue.
    ///
    /// Only these four filters exist — the broker ignores any other query key,
    /// so there is deliberately no `partition`/`from`/`to` here even though the
    /// JS SDK sends them.
    pub async fn dlq(&self, params: DlqParams) -> Result<DlqResponse> {
        let path = format!("/api/v1/dlq?{}", encode_pairs(&params.to_pairs()));
        let out: Option<DlqResponse> = self.inner.http.get_json(&path, &Opts::default()).await?;
        Ok(out.unwrap_or_default())
    }

    // ------------------------------------------------------------- traces

    pub async fn record_trace(&self, trace: &TraceRequest) -> Result<TraceResponse> {
        let out: Option<TraceResponse> = self
            .inner
            .http
            .post_json("/api/v1/traces", trace, &Opts::default())
            .await?;
        Ok(out.unwrap_or_default())
    }

    pub async fn trace_names(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/traces/names", params)).await
    }

    pub async fn traces_by_name(
        &self,
        name: &str,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query(
            &format!("/api/v1/traces/by-name/{}", urlencode(name)),
            params,
        ))
        .await
    }

    pub async fn traces_for_message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value> {
        self.get(&format!(
            "/api/v1/traces/{}/{}",
            urlencode(partition_id),
            urlencode(transaction_id)
        ))
        .await
    }

    // --------------------------------------------------- consumer groups

    pub async fn list_consumer_groups(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/consumer-groups").await
    }

    pub async fn consumer_group(&self, name: &str) -> Result<serde_json::Value> {
        self.get(&format!("/api/v1/consumer-groups/{}", urlencode(name)))
            .await
    }

    pub async fn lagging_consumers(&self, min_lag_seconds: i64) -> Result<serde_json::Value> {
        self.get(&format!(
            "/api/v1/consumer-groups/lagging?minLagSeconds={min_lag_seconds}"
        ))
        .await
    }

    /// Delete a group's cursors everywhere.
    pub async fn delete_consumer_group(
        &self,
        group: &str,
        delete_metadata: bool,
    ) -> Result<serde_json::Value> {
        self.delete(&format!(
            "/api/v1/consumer-groups/{}?deleteMetadata={delete_metadata}",
            urlencode(group)
        ))
        .await
    }

    /// Delete a group's cursor for one queue only.
    pub async fn delete_consumer_group_for_queue(
        &self,
        group: &str,
        queue: &str,
        delete_metadata: bool,
    ) -> Result<serde_json::Value> {
        self.delete(&format!(
            "/api/v1/consumer-groups/{}/queues/{}?deleteMetadata={delete_metadata}",
            urlencode(group),
            urlencode(queue)
        ))
        .await
    }

    /// Move a group's cursor. This is how a replay is done — `subscription_from`
    /// on a pop only seeds a cursor that does not exist yet.
    pub async fn seek_consumer_group(
        &self,
        group: &str,
        queue: &str,
        seek: &SeekRequest,
    ) -> Result<serde_json::Value> {
        self.post(
            &format!(
                "/api/v1/consumer-groups/{}/queues/{}/seek",
                urlencode(group),
                urlencode(queue)
            ),
            seek,
        )
        .await
    }

    pub async fn set_subscription_timestamp(
        &self,
        group: &str,
        timestamp: impl Into<String>,
    ) -> Result<serde_json::Value> {
        self.post(
            &format!("/api/v1/consumer-groups/{}/subscription", urlencode(group)),
            &SubscriptionRequest {
                subscription_timestamp: timestamp.into(),
            },
        )
        .await
    }

    pub async fn refresh_consumer_stats(&self) -> Result<serde_json::Value> {
        let out: Option<serde_json::Value> = self
            .inner
            .http
            .post_empty("/api/v1/stats/refresh", &Opts::default())
            .await?;
        Ok(out.unwrap_or(serde_json::Value::Null))
    }

    // -------------------------------------------------------------- lease

    /// Extend a lease. Best-effort: always HTTP 200, so check
    /// [`RenewLeaseResponse::success`].
    pub async fn renew_lease(
        &self,
        lease_id: &str,
        seconds: Option<i32>,
    ) -> Result<RenewLeaseResponse> {
        let path = format!("/api/v1/lease/{}/extend", urlencode(lease_id));
        let out: Option<RenewLeaseResponse> = self
            .inner
            .http
            .post_json(&path, &RenewLeaseRequest { seconds }, &Opts::default())
            .await?;
        out.ok_or_else(|| crate::Error::Decode("lease extension returned an empty body".into()))
    }

    // ------------------------------------------------------------- status

    pub async fn status(&self, params: &[(&'static str, String)]) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/status", params)).await
    }

    pub async fn queue_stats(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/status/queues", params)).await
    }

    pub async fn queue_detail(
        &self,
        name: &str,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query(
            &format!("/api/v1/status/queues/{}", urlencode(name)),
            params,
        ))
        .await
    }

    pub async fn analytics(&self, params: &[(&'static str, String)]) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/status/analytics", params))
            .await
    }

    pub async fn system_metrics(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/analytics/system-metrics", params))
            .await
    }

    pub async fn worker_metrics(
        &self,
        params: &[(&'static str, String)],
    ) -> Result<serde_json::Value> {
        self.get(&with_query("/api/v1/analytics/worker-metrics", params))
            .await
    }

    pub async fn postgres_stats(&self) -> Result<serde_json::Value> {
        self.get("/api/v1/analytics/postgres-stats").await
    }

    // ------------------------------------------------------------- system

    pub async fn health(&self) -> Result<serde_json::Value> {
        self.get("/health").await
    }

    /// Prometheus exposition text, not JSON.
    pub async fn metrics(&self) -> Result<String> {
        self.inner.http.get_text("/metrics", &Opts::default()).await
    }

    /// Whether pushes are being diverted to the on-disk spool.
    pub async fn maintenance(&self) -> Result<MaintenanceResponse> {
        let out: Option<MaintenanceResponse> = self
            .inner
            .http
            .get_json("/api/v1/system/maintenance", &Opts::default())
            .await?;
        Ok(out.unwrap_or_default())
    }

    /// Divert every push to the spool. They replay when this is turned off.
    pub async fn set_maintenance(&self, enabled: bool) -> Result<MaintenanceResponse> {
        let out: Option<MaintenanceResponse> = self
            .inner
            .http
            .post_json(
                "/api/v1/system/maintenance",
                &MaintenanceRequest { enabled },
                &Opts::default(),
            )
            .await?;
        Ok(out.unwrap_or_default())
    }

    pub async fn pop_maintenance(&self) -> Result<MaintenanceResponse> {
        let out: Option<MaintenanceResponse> = self
            .inner
            .http
            .get_json("/api/v1/system/maintenance/pop", &Opts::default())
            .await?;
        Ok(out.unwrap_or_default())
    }

    /// Pause consumption. Pops answer 204 with `paused: true`, which the
    /// client surfaces as an empty poll rather than an error.
    pub async fn set_pop_maintenance(&self, enabled: bool) -> Result<MaintenanceResponse> {
        let out: Option<MaintenanceResponse> = self
            .inner
            .http
            .post_json(
                "/api/v1/system/maintenance/pop",
                &MaintenanceRequest { enabled },
                &Opts::default(),
            )
            .await?;
        Ok(out.unwrap_or_default())
    }
}

fn with_query(path: &str, params: &[(&'static str, String)]) -> String {
    if params.is_empty() {
        return path.to_string();
    }
    format!("{path}?{}", encode_pairs(params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_is_omitted_when_there_is_nothing_to_send() {
        assert_eq!(with_query("/api/v1/status", &[]), "/api/v1/status");
    }

    #[test]
    fn query_is_appended_and_encoded() {
        let params = vec![("search", "my queue".to_string()), ("limit", "10".into())];
        assert_eq!(
            with_query("/api/v1/resources/queues", &params),
            "/api/v1/resources/queues?search=my%20queue&limit=10"
        );
    }
}
