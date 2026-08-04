//! Queue configuration, DLQ, leases, subscriptions, traces and maintenance.

use serde::{Deserialize, Serialize};

// --------------------------------------------------------------- configure

/// Queue options accepted by `POST /api/v1/configure`.
///
/// Every field is optional and omitted when `None`: the stored procedure
/// `COALESCE`s each key to its own default, so sending a partial object is
/// well-defined and leaves the rest untouched. The documented default for each
/// field below is the *SQL* default, which is the one that actually applies.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,

    /// Default `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// Seconds a claim is held before it expires and the messages are
    /// redelivered. Default `300`.
    #[serde(rename = "leaseTime", default, skip_serializing_if = "Option::is_none")]
    pub lease_time: Option<i32>,

    /// Redeliveries before a message is dead-lettered. Default `3`.
    #[serde(
        rename = "retryLimit",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_limit: Option<i32>,

    /// Milliseconds. Default `1000`.
    #[serde(
        rename = "retryDelay",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_delay: Option<i32>,

    /// Cap on messages held in the queue. Default `0` (unbounded).
    #[serde(rename = "maxSize", default, skip_serializing_if = "Option::is_none")]
    pub max_size: Option<i32>,

    /// Seconds. Default `3600`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i32>,

    /// Default `true`. Note the asymmetry with the client-side defaults: the
    /// SDKs' own default option bags do not mention this key, so a queue
    /// created without it *does* get a DLQ.
    #[serde(
        rename = "deadLetterQueue",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub dead_letter_queue: Option<bool>,

    /// Default `true`.
    #[serde(
        rename = "dlqAfterMaxRetries",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub dlq_after_max_retries: Option<bool>,

    /// Seconds to hold a message before it becomes poppable. Default `0`.
    #[serde(
        rename = "delayedProcessing",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub delayed_processing: Option<i32>,

    /// Seconds the broker accumulates pushes before making them visible, which
    /// fattens batches at the cost of latency. Default `0`.
    #[serde(
        rename = "windowBuffer",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub window_buffer: Option<i32>,

    /// Default `0` (keep forever).
    #[serde(
        rename = "retentionSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retention_seconds: Option<i32>,

    /// Default `0` (keep forever).
    #[serde(
        rename = "completedRetentionSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_retention_seconds: Option<i32>,

    /// Default `false`. Retention only runs when this is on.
    #[serde(
        rename = "retentionEnabled",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retention_enabled: Option<bool>,

    /// At-rest payload encryption. Default `false`. Turning it on affects new
    /// messages only.
    #[serde(
        rename = "encryptionEnabled",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub encryption_enabled: Option<bool>,

    /// Default `0`.
    #[serde(
        rename = "maxWaitTimeSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub max_wait_time_seconds: Option<i32>,

    /// Milliseconds, clamped by the broker to `0..=60000`. Default `0`.
    #[serde(
        rename = "minPopWaitTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub min_pop_wait_time: Option<i32>,

    /// Window in which a repeated `transactionId` is treated as a duplicate.
    /// Default `3600`; clamped to `>= 0`.
    #[serde(
        rename = "dedupWindowSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub dedup_window_seconds: Option<i32>,
}

/// Body of `POST /api/v1/configure`.
///
/// The broker also accepts the options spread across the top level instead of
/// nested under `options`; this type always sends the nested form, which is
/// what every SDK does.
///
/// An empty string is a valid queue name — the broker deliberately allows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigureRequest {
    pub queue: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,

    pub options: QueueOptions,
}

impl ConfigureRequest {
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            namespace: None,
            task: None,
            options: QueueOptions::default(),
        }
    }

    pub fn options(mut self, options: QueueOptions) -> Self {
        self.options = options;
        self
    }
}

/// Echo of `configure_queue_v1`. Returned verbatim by the broker, so it carries
/// every option key back; the typed fields here are the ones worth branching
/// on and `rest` keeps the remainder rather than dropping it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigureResponse {
    #[serde(default)]
    pub configured: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

// --------------------------------------------------------------------- dlq

/// One dead-lettered message.
///
/// On an encryption-enabled queue the snapshot is stored verbatim at quarantine
/// time, so `data` is the `{encrypted, iv, authTag}` envelope rather than the
/// plaintext — the DLQ read path does not decrypt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(
        rename = "transactionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transaction_id: Option<String>,

    #[serde(
        rename = "partitionId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub partition_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    #[serde(default)]
    pub data: serde_json::Value,

    /// Why the message was dead-lettered.
    ///
    /// The wire key is `errorMessage`: `queen.get_dlq_messages_v1` projects the
    /// stored `error` column under that name, and the handler passes the SP's
    /// text straight through. Reading `error` instead — as this did — left the
    /// field permanently `None` while the reason sat unnoticed in `rest`. The
    /// alias keeps the plain key working for anything that renders a DLQ row
    /// from a different source.
    #[serde(
        rename = "errorMessage",
        alias = "error",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub error: Option<String>,

    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

/// Response of `GET /api/v1/dlq`.
///
/// # Only four filters actually apply
///
/// The broker collects exactly `queue` and `consumerGroup` from the query
/// string, then adds `limit` (default 100) and `offset` (default 0). The JS
/// SDK's DLQ builder also sends `partition`, `from` and `to` — those keys never
/// reach the stored procedure and are silently ignored, so a caller who relies
/// on them gets an unfiltered page back and no error. [`DlqParams`] therefore
/// exposes only the four that work.
///
/// `total` is the length of the returned page, not the size of the DLQ — it is
/// computed from `messages.len()` after the query, so it can never exceed
/// `limit` and cannot be used for pagination.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DlqResponse {
    #[serde(default)]
    pub messages: Vec<DlqMessage>,

    #[serde(default)]
    pub total: usize,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Query parameters honoured by `GET /api/v1/dlq`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DlqParams {
    pub queue: Option<String>,
    pub consumer_group: Option<String>,
    /// Default 100 broker-side.
    pub limit: Option<i32>,
    /// Default 0 broker-side.
    pub offset: Option<i32>,
}

impl DlqParams {
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        if let Some(v) = &self.queue {
            out.push(("queue", v.clone()));
        }
        if let Some(v) = &self.consumer_group {
            out.push(("consumerGroup", v.clone()));
        }
        if let Some(v) = self.limit {
            out.push(("limit", v.to_string()));
        }
        if let Some(v) = self.offset {
            out.push(("offset", v.to_string()));
        }
        out
    }
}

// ------------------------------------------------------------------- lease

/// Body of `POST /api/v1/lease/:leaseId/extend`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RenewLeaseRequest {
    /// Defaults to 60 broker-side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<i32>,
}

/// Response of a lease extension. Always HTTP 200 — renewal is best-effort, so
/// `success` is the signal.
///
/// The broker writes the same expiry under three different keys because three
/// SDKs each look for a different one. They are always equal; prefer
/// [`RenewLeaseResponse::expires_at`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenewLeaseResponse {
    #[serde(rename = "leaseId")]
    pub lease_id: String,

    pub success: bool,

    /// Number of partition claims the extension touched. A multi-partition pop
    /// shares one lease across several, so this can be greater than 1.
    pub renewed: i64,

    #[serde(rename = "newExpiresAt", default)]
    pub new_expires_at: Option<String>,

    #[serde(rename = "expiresAt", default)]
    pub expires_at_camel: Option<String>,

    #[serde(rename = "lease_expires_at", default)]
    pub lease_expires_at: Option<String>,
}

impl RenewLeaseResponse {
    /// The new expiry, whichever key it arrived under.
    pub fn expires_at(&self) -> Option<&str> {
        self.new_expires_at
            .as_deref()
            .or(self.expires_at_camel.as_deref())
            .or(self.lease_expires_at.as_deref())
    }
}

// ------------------------------------------------------- consumer groups

/// Body of `POST /api/v1/consumer-groups/:group/subscription`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    /// ISO-8601.
    #[serde(rename = "subscriptionTimestamp")]
    pub subscription_timestamp: String,
}

/// Body of `POST /api/v1/consumer-groups/:group/queues/:queue/seek`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SeekRequest {
    /// Move the cursor to this ISO-8601 instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    /// Symbolic destination, e.g. `"earliest"` / `"latest"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
}

// ------------------------------------------------------------------ traces

/// Body of `POST /api/v1/traces`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceRequest {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    #[serde(rename = "partitionId")]
    pub partition_id: String,

    #[serde(rename = "consumerGroup")]
    pub consumer_group: String,

    /// Free-form labels used to look the trace back up. `None` for an
    /// unlabelled event.
    #[serde(
        rename = "traceNames",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub trace_names: Option<Vec<String>>,

    /// `info`, `error`, `step`, … — free-form; SDKs default to `info`.
    #[serde(rename = "eventType")]
    pub event_type: String,

    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TraceResponse {
    #[serde(default)]
    pub success: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

// ------------------------------------------------------------- maintenance

/// Body of `POST /api/v1/system/maintenance` and `.../maintenance/pop`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceRequest {
    pub enabled: bool,
}

/// Reply from either maintenance route.
///
/// The request key is `enabled`, but the reply reports state under
/// `maintenanceMode` / `popMaintenanceMode` — and which of the two is present
/// depends on the route:
///
/// | route | `maintenanceMode` | `popMaintenanceMode` |
/// |---|---|---|
/// | `GET /system/maintenance`      | yes | yes |
/// | `POST /system/maintenance`     | yes | no  |
/// | `GET  /system/maintenance/pop` | no  | yes |
/// | `POST /system/maintenance/pop` | no  | yes |
///
/// Both are therefore `Option`; use [`MaintenanceResponse::push_paused`] and
/// [`MaintenanceResponse::pop_paused`] rather than reading the fields, so an
/// absent key does not read as `false`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceResponse {
    #[serde(
        rename = "maintenanceMode",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub maintenance_mode: Option<bool>,

    #[serde(
        rename = "popMaintenanceMode",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub pop_maintenance_mode: Option<bool>,

    /// Human-readable confirmation, present on the POST replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// How many messages are sitting in the spool. Push routes only.
    #[serde(
        rename = "bufferedMessages",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub buffered_messages: Option<i64>,

    #[serde(
        rename = "bufferHealthy",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub buffer_healthy: Option<bool>,

    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}

impl MaintenanceResponse {
    /// Whether pushes are being diverted to the spool. `None` when this reply
    /// did not report on it.
    pub fn push_paused(&self) -> Option<bool> {
        self.maintenance_mode
    }

    /// Whether pops are being answered as paused. `None` when this reply did
    /// not report on it.
    pub fn pop_paused(&self) -> Option<bool> {
        self.pop_maintenance_mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A DLQ row exactly as `queen.get_dlq_messages_v1` projects it.
    ///
    /// Copied key for key from the `jsonb_build_object` in
    /// `server/sql/procedures/010_log_admin.sql`. Deserializing a literal the
    /// crate itself invented would have passed while the field was reading the
    /// wrong key, which is how the reason went missing unnoticed.
    const DLQ_ROW_FROM_THE_SP: &str = r#"{
        "id": "0198f0aa-0000-7000-8000-000000000001",
        "transactionId": "0198f0aa-0000-7000-8000-000000000002",
        "partitionId": "0198f0aa-0000-7000-8000-000000000003",
        "queue": "orders",
        "partition": "customer-42",
        "consumerGroup": "fulfilment",
        "errorMessage": "handler returned an error: downstream 503",
        "retryCount": 3,
        "data": { "total": 19.99 },
        "producerSub": null,
        "createdAt": "2026-08-04T09:15:00.123Z",
        "failedAt": "2026-08-04T09:15:00.123Z"
    }"#;

    #[test]
    fn a_dlq_row_exposes_the_reason_the_broker_stored() {
        let row: DlqMessage = serde_json::from_str(DLQ_ROW_FROM_THE_SP).unwrap();
        assert_eq!(
            row.error.as_deref(),
            Some("handler returned an error: downstream 503"),
            "the dead-letter reason did not reach the typed field; it is probably \
             sitting in `rest` under a key this struct does not read: {:?}",
            row.rest
        );
        assert_eq!(row.queue.as_deref(), Some("orders"));
        assert_eq!(row.partition.as_deref(), Some("customer-42"));
        assert_eq!(row.data["total"], serde_json::json!(19.99));
        // Everything the struct does not name stays reachable rather than being
        // dropped on the floor.
        assert_eq!(row.rest.get("retryCount"), Some(&serde_json::json!(3)));
        assert_eq!(
            row.rest.get("consumerGroup"),
            Some(&serde_json::json!("fulfilment"))
        );
        assert!(
            !row.rest.contains_key("errorMessage"),
            "the reason was captured twice: once typed and once in `rest`"
        );
    }

    #[test]
    fn a_dlq_row_still_reads_a_plain_error_key() {
        // The alias exists so a row rendered from somewhere other than the SP
        // — a fixture, a proxy, an older broker — still resolves.
        let row: DlqMessage = serde_json::from_str(r#"{"data":{},"error":"plain key"}"#).unwrap();
        assert_eq!(row.error.as_deref(), Some("plain key"));
    }

    #[test]
    fn a_dlq_row_round_trips_through_the_wire_key() {
        let row: DlqMessage = serde_json::from_str(DLQ_ROW_FROM_THE_SP).unwrap();
        let s = serde_json::to_string(&row).unwrap();
        assert!(
            s.contains(r#""errorMessage":"#),
            "re-serializing must emit the key the broker sends, not the field name: {s}"
        );
        assert_eq!(serde_json::from_str::<DlqMessage>(&s).unwrap(), row);
    }

    #[test]
    fn the_dlq_envelope_parses_with_the_pagination_block_it_carries() {
        // `queen.get_dlq_messages_v1` wraps the rows in {messages, pagination}
        // (`server/sql/procedures/010_log_admin.sql:627-634`) and the handler
        // then injects `total` from `messages.len()`
        // (`server/src/handlers/messages.rs:570-573`). `pagination` is a real
        // key this struct does not model, so the unknown-field path is
        // exercised by the live wire and not only by hypothetical future
        // fields — and `total` really is the page length, which is why it can
        // never be used to drive pagination.
        let wire = format!(
            r#"{{"messages":[{DLQ_ROW_FROM_THE_SP}],"pagination":{{"limit":100,"offset":0}},"total":1}}"#
        );
        let got: DlqResponse =
            serde_json::from_str(&wire).expect("the DLQ envelope must deserialize");
        assert_eq!(got.total, 1);
        assert_eq!(got.messages.len(), 1);
        assert_eq!(
            got.messages[0].error.as_deref(),
            Some("handler returned an error: downstream 503"),
            "the reason has to survive the envelope too, not just a bare row"
        );
        assert!(got.error.is_none());
    }

    #[test]
    fn an_empty_dlq_reads_as_empty_rather_than_missing() {
        // The SP COALESCEs the aggregate to '[]' (010:628), so an empty DLQ is
        // an empty array with total 0 — never an absent `messages` key. Both
        // still have to land on the same value, because a client that reads a
        // missing array as an error reports a broken DLQ for a healthy queue.
        let full: DlqResponse = serde_json::from_str(
            r#"{"messages":[],"pagination":{"limit":100,"offset":0},"total":0}"#,
        )
        .unwrap();
        let bare: DlqResponse = serde_json::from_str("{}").unwrap();
        assert_eq!(full, bare);
        assert!(full.messages.is_empty() && full.total == 0);
    }

    #[test]
    fn a_dlq_row_tolerates_the_nulls_the_sql_projects() {
        // `producerSub` is a literal NULL in the projection (010:651) and the
        // remaining identity columns are nullable joins. Every one of them has
        // to read as absent rather than failing the row, or a single
        // unresolvable partition name takes the whole page down.
        let row: DlqMessage = serde_json::from_str(
            r#"{"id":null,"transactionId":null,"partitionId":null,"queue":null,"partition":null,"data":null,"errorMessage":null,"producerSub":null}"#,
        )
        .expect("a fully null row must still decode");
        assert!(row.id.is_none() && row.queue.is_none() && row.error.is_none());
        assert_eq!(row.data, serde_json::Value::Null);
    }

    #[test]
    fn a_dlq_row_from_a_newer_broker_keeps_what_it_does_not_model() {
        // Unknown keys land in `rest` rather than being dropped, which is what
        // made the missing `errorMessage` recoverable at all: the reason was
        // sitting there the whole time.
        let row: DlqMessage = serde_json::from_str(
            r#"{"data":{},"errorMessage":"boom","dlqReason":"budget-exhausted","attempt":4}"#,
        )
        .unwrap();
        assert_eq!(row.error.as_deref(), Some("boom"));
        assert_eq!(
            row.rest.get("dlqReason"),
            Some(&serde_json::json!("budget-exhausted"))
        );
    }

    #[test]
    fn empty_options_serialize_to_an_empty_object() {
        let req = ConfigureRequest::new("orders");
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"orders","options":{}}"#
        );
    }

    #[test]
    fn options_use_the_camel_case_keys_the_sql_reads() {
        let req = ConfigureRequest::new("orders").options(QueueOptions {
            lease_time: Some(60),
            retry_limit: Some(5),
            dedup_window_seconds: Some(0),
            encryption_enabled: Some(true),
            ..Default::default()
        });
        let s = serde_json::to_string(&req).unwrap();
        for key in [
            r#""leaseTime":60"#,
            r#""retryLimit":5"#,
            r#""dedupWindowSeconds":0"#,
            r#""encryptionEnabled":true"#,
        ] {
            assert!(s.contains(key), "missing {key} in {s}");
        }
    }

    #[test]
    fn an_empty_queue_name_is_valid() {
        let req = ConfigureRequest::new("");
        assert!(serde_json::to_string(&req)
            .unwrap()
            .contains(r#""queue":"""#));
    }

    #[test]
    fn configure_response_keeps_unmodelled_keys() {
        let got: ConfigureResponse =
            serde_json::from_str(r#"{"configured":true,"queue":"q","leaseTime":300}"#).unwrap();
        assert!(got.configured);
        assert_eq!(got.rest.get("leaseTime").unwrap(), &serde_json::json!(300));
    }

    #[test]
    fn dlq_params_expose_only_the_four_that_work() {
        let p = DlqParams {
            queue: Some("orders".into()),
            consumer_group: Some("g".into()),
            limit: Some(50),
            offset: Some(10),
        };
        assert_eq!(
            p.to_pairs(),
            vec![
                ("queue", "orders".to_string()),
                ("consumerGroup", "g".to_string()),
                ("limit", "50".to_string()),
                ("offset", "10".to_string()),
            ]
        );
    }

    #[test]
    fn renew_reads_the_expiry_under_any_of_the_three_keys() {
        let wire = r#"{"leaseId":"L1","success":true,"renewed":3,"newExpiresAt":"2026-08-04T10:05:00Z","expiresAt":"2026-08-04T10:05:00Z","lease_expires_at":"2026-08-04T10:05:00Z"}"#;
        let got: RenewLeaseResponse = serde_json::from_str(wire).unwrap();
        assert_eq!(got.renewed, 3);
        assert_eq!(got.expires_at(), Some("2026-08-04T10:05:00Z"));
    }

    #[test]
    fn renew_failure_carries_nulls_and_success_false() {
        let wire = r#"{"leaseId":"L1","success":false,"renewed":0,"newExpiresAt":null,"expiresAt":null,"lease_expires_at":null}"#;
        let got: RenewLeaseResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert_eq!(got.expires_at(), None);
    }

    #[test]
    fn maintenance_replies_report_only_their_own_route() {
        // GET /system/maintenance — both knobs.
        let both: MaintenanceResponse = serde_json::from_str(
            r#"{"bufferHealthy":true,"bufferedMessages":0,"maintenanceMode":true,"popMaintenanceMode":false}"#,
        )
        .unwrap();
        assert_eq!(both.push_paused(), Some(true));
        assert_eq!(both.pop_paused(), Some(false));
        assert_eq!(both.buffered_messages, Some(0));

        // POST /system/maintenance — push only; the pop knob is ABSENT, which
        // must not read as "pops are running".
        let push: MaintenanceResponse = serde_json::from_str(
            r#"{"bufferHealthy":true,"bufferedMessages":0,"maintenanceMode":true,"message":"Maintenance mode ENABLED. All PUSHes routing to file buffer."}"#,
        )
        .unwrap();
        assert_eq!(push.push_paused(), Some(true));
        assert_eq!(push.pop_paused(), None, "absent must not collapse to false");
        assert!(push.message.unwrap().contains("ENABLED"));

        // POST /system/maintenance/pop — pop only.
        let pop: MaintenanceResponse = serde_json::from_str(
            r#"{"message":"Pop maintenance mode ENABLED.","popMaintenanceMode":true}"#,
        )
        .unwrap();
        assert_eq!(pop.pop_paused(), Some(true));
        assert_eq!(pop.push_paused(), None);
    }

    #[test]
    fn maintenance_request_uses_enabled_not_the_reply_key() {
        // The asymmetry is real: you write `enabled` and read `maintenanceMode`.
        assert_eq!(
            serde_json::to_string(&MaintenanceRequest { enabled: true }).unwrap(),
            r#"{"enabled":true}"#
        );
    }

    #[test]
    fn trace_omits_names_when_unlabelled() {
        let req = TraceRequest {
            transaction_id: "t1".into(),
            partition_id: "p1".into(),
            consumer_group: "g".into(),
            trace_names: None,
            event_type: "info".into(),
            data: serde_json::json!({"step": 1}),
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(!s.contains("traceNames"), "{s}");
        assert!(s.contains(r#""eventType":"info""#), "{s}");
    }
}
