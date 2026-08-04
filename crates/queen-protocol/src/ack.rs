//! `POST /api/v1/ack` and `POST /api/v1/ack/batch` — commit or reject a claim.
//!
//! Both routes answer with the same top-level array, one [`AckResult`] per
//! acknowledgment in request order. A rejected ack (an expired lease, say) is
//! still an HTTP 200 with `success: false` on the item — the per-item flag is
//! the only reliable signal, so a client that checks only the status code will
//! silently lose messages.

use serde::{Deserialize, Serialize};

/// The four outcomes the broker's segment-ack procedure branches on.
///
/// The wire accepts more spellings than this enum has variants:
/// `completed`, `success`, `acked` and `ok` all normalize to
/// [`AckStatus::Completed`]; an **absent** status also means completed; and
/// *any* unrecognized string normalizes to [`AckStatus::Failed`] rather than
/// being rejected. [`AckStatus::parse`] reproduces that table exactly, so a
/// client can predict what the broker will do with a hand-written status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckStatus {
    /// Advance the cursor past this message.
    Completed,
    /// Nack. Redelivers until the queue's retry limit is reached, then
    /// dead-letters if the queue has a DLQ.
    Failed,
    /// Nack that explicitly asks for redelivery.
    Retry,
    /// Nack that dead-letters immediately, skipping the remaining retries.
    Dlq,
}

impl AckStatus {
    /// Normalize a wire status exactly as the broker's `normalize_ack_status`
    /// does, including the aliases and the "unknown means failed" fallback.
    pub fn parse(s: Option<&str>) -> Self {
        match s {
            None => Self::Completed,
            Some("completed") | Some("success") | Some("acked") | Some("ok") => Self::Completed,
            Some("retry") => Self::Retry,
            Some("dlq") => Self::Dlq,
            Some(_) => Self::Failed,
        }
    }

    /// The canonical spelling the broker normalizes to.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retry => "retry",
            Self::Dlq => "dlq",
        }
    }

    pub fn from_bool(ok: bool) -> Self {
        if ok {
            Self::Completed
        } else {
            Self::Failed
        }
    }
}

/// Body of `POST /api/v1/ack`.
///
/// `partition_id` is mandatory in practice even though the broker types it as
/// optional: a transaction id alone is not unique across partitions, so acking
/// without one can commit the cursor of a message the consumer never saw. Every
/// SDK refuses to send an ack without it, and so does this type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckRequest {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    #[serde(rename = "partitionId")]
    pub partition_id: String,

    pub status: AckStatus,

    #[serde(
        rename = "consumerGroup",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub consumer_group: Option<String>,

    /// The lease this ack redeems. Omitted for an `autoAck` delivery, which
    /// holds none.
    #[serde(rename = "leaseId", default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,

    /// Failure reason for a nack, recorded on the DLQ row when this nack
    /// exhausts the retry budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One entry of `acknowledgments` in a batch ack.
///
/// Note the asymmetry with [`AckRequest`]: there is no per-item
/// `consumerGroup`. The batch carries a single group for every item, so one
/// call cannot ack across two groups.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckBatchItem {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    #[serde(rename = "partitionId")]
    pub partition_id: String,

    pub status: AckStatus,

    #[serde(rename = "leaseId", default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Body of `POST /api/v1/ack/batch`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckBatchRequest {
    pub acknowledgments: Vec<AckBatchItem>,

    #[serde(
        rename = "consumerGroup",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub consumer_group: Option<String>,
}

/// One entry of the ack response array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AckResult {
    pub index: usize,

    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    pub success: bool,

    pub error: Option<String>,

    /// The lease was given back, so the partition is claimable again.
    #[serde(rename = "leaseReleased")]
    pub lease_released: bool,

    /// This ack dead-lettered the message.
    pub dlq: bool,

    /// The ack changed nothing — the cursor was already past this message.
    /// Usually a duplicate ack after a redelivery, and not an error.
    pub noop: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_aliases_match_the_broker_table() {
        for alias in ["completed", "success", "acked", "ok"] {
            assert_eq!(
                AckStatus::parse(Some(alias)),
                AckStatus::Completed,
                "{alias}"
            );
        }
        assert_eq!(AckStatus::parse(None), AckStatus::Completed);
        assert_eq!(AckStatus::parse(Some("retry")), AckStatus::Retry);
        assert_eq!(AckStatus::parse(Some("dlq")), AckStatus::Dlq);
        // Anything unrecognized is a nack, not a rejection.
        assert_eq!(AckStatus::parse(Some("nope")), AckStatus::Failed);
        assert_eq!(AckStatus::parse(Some("")), AckStatus::Failed);
        assert_eq!(AckStatus::parse(Some("COMPLETED")), AckStatus::Failed);
    }

    #[test]
    fn status_serializes_to_its_canonical_spelling() {
        assert_eq!(
            serde_json::to_string(&AckStatus::Completed).unwrap(),
            r#""completed""#
        );
        assert_eq!(serde_json::to_string(&AckStatus::Dlq).unwrap(), r#""dlq""#);
        assert_eq!(AckStatus::from_bool(false).as_str(), "failed");
    }

    #[test]
    fn single_ack_omits_absent_optionals() {
        let req = AckRequest {
            transaction_id: "t1".into(),
            partition_id: "p1".into(),
            status: AckStatus::Completed,
            consumer_group: None,
            lease_id: None,
            error: None,
        };
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"transactionId":"t1","partitionId":"p1","status":"completed"}"#
        );
    }

    #[test]
    fn batch_carries_one_group_for_every_item() {
        let req = AckBatchRequest {
            acknowledgments: vec![AckBatchItem {
                transaction_id: "t1".into(),
                partition_id: "p1".into(),
                status: AckStatus::Failed,
                lease_id: Some("L1".into()),
                error: Some("boom".into()),
            }],
            consumer_group: Some("g".into()),
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""consumerGroup":"g""#), "{s}");
        assert!(s.contains(r#""leaseId":"L1""#), "{s}");
        assert!(s.contains(r#""error":"boom""#), "{s}");
    }

    #[test]
    fn result_round_trips_the_rendered_wire() {
        let wire = r#"[{"index":0,"transactionId":"t1","success":true,"error":null,"leaseReleased":true,"dlq":false,"noop":false}]"#;
        let got: Vec<AckResult> = serde_json::from_str(wire).unwrap();
        assert!(got[0].success);
        assert!(got[0].lease_released);
        assert_eq!(serde_json::to_string(&got).unwrap(), wire);
    }

    #[test]
    fn an_ack_result_from_a_newer_broker_still_parses() {
        // The renderer (`server/src/handlers/data.rs:2938-2952`) always writes
        // all seven keys, so the only drift to guard against is a new one being
        // added. Failing the decode here would leave the caller unable to tell
        // a committed ack from a rejected one, which on a nack means silently
        // losing the retry.
        let wire = r#"[{"index":0,"transactionId":"t1","success":true,"error":null,"leaseReleased":true,"dlq":false,"noop":false,"offset":4211}]"#;
        let got: Vec<AckResult> =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert!(got[0].success);
    }

    #[test]
    fn a_noop_ack_is_not_a_failure() {
        // A duplicate ack after a redelivery comes back success:true with
        // noop:true. Treating `noop` as an error would make every redelivered
        // message look like a failed handoff.
        let wire = r#"{"index":0,"transactionId":"t1","success":true,"error":null,"leaseReleased":false,"dlq":false,"noop":true}"#;
        let got: AckResult = serde_json::from_str(wire).unwrap();
        assert!(got.success && got.noop);
        assert!(got.error.is_none());
    }

    #[test]
    fn a_rejected_ack_is_a_200_with_success_false() {
        let wire = r#"{"index":0,"transactionId":"t1","success":false,"error":"Invalid or expired lease","leaseReleased":false,"dlq":false,"noop":false}"#;
        let got: AckResult = serde_json::from_str(wire).unwrap();
        assert!(!got.success);
        assert_eq!(got.error.as_deref(), Some("Invalid or expired lease"));
    }
}
