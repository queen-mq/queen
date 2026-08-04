//! Shared state behind every builder, plus the ack primitives the consume loop
//! and the public API both go through.

use std::sync::Arc;

use queen_protocol::{AckBatchItem, AckBatchRequest, AckRequest, AckResult, AckStatus, Message};

use crate::buffer::BufferManager;
use crate::error::{Error, Result};
use crate::http::{HttpClient, Opts};

pub(crate) struct Inner {
    pub http: Arc<HttpClient>,
    pub buffers: Arc<BufferManager>,
}

impl Inner {
    /// Ack one message.
    ///
    /// The consumer group and the lease are taken from the message itself
    /// rather than from the caller. A message always knows which group claimed
    /// it, so this cannot ack the wrong group's cursor — the failure mode of
    /// every SDK that makes the caller pass the group separately.
    pub async fn ack_one(
        &self,
        message: &Message,
        status: AckStatus,
        error: Option<String>,
    ) -> Result<AckResult> {
        if message.partition_id.is_empty() {
            return Err(Error::Invalid(
                "cannot ack a message with no partitionId: a transactionId alone is not unique \
                 across partitions, so this could commit a cursor for a message you never saw"
                    .into(),
            ));
        }

        let req = AckRequest {
            transaction_id: message.transaction_id.clone(),
            partition_id: message.partition_id.clone(),
            status,
            consumer_group: non_empty(&message.consumer_group),
            lease_id: non_empty(&message.lease_id),
            error,
        };

        let results: Option<Vec<AckResult>> = self
            .http
            .post_json("/api/v1/ack", &req, &Opts::default())
            .await?;
        let mut results = parse_ack_results(results, 1)?;
        Ok(results.remove(0))
    }

    /// Ack several messages in one request.
    ///
    /// The batch endpoint carries a single consumer group, so every message
    /// must belong to the same one — mixing them would ack half of them against
    /// the wrong cursor. That is checked here rather than left to the broker.
    pub async fn ack_batch(
        &self,
        messages: &[Message],
        status: AckStatus,
        error: Option<String>,
    ) -> Result<Vec<AckResult>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let group = messages[0].consumer_group.clone();
        if let Some(odd) = messages.iter().find(|m| m.consumer_group != group) {
            return Err(Error::Invalid(format!(
                "cannot batch-ack across consumer groups: the batch carries one group, but got \
                 both '{group}' and '{}'",
                odd.consumer_group
            )));
        }
        if let Some(m) = messages.iter().find(|m| m.partition_id.is_empty()) {
            return Err(Error::Invalid(format!(
                "cannot ack message '{}' with no partitionId",
                m.transaction_id
            )));
        }

        let req = AckBatchRequest {
            acknowledgments: messages
                .iter()
                .map(|m| AckBatchItem {
                    transaction_id: m.transaction_id.clone(),
                    partition_id: m.partition_id.clone(),
                    status,
                    lease_id: non_empty(&m.lease_id),
                    error: error.clone(),
                })
                .collect(),
            consumer_group: non_empty(&group),
        };

        let results: Option<Vec<AckResult>> = self
            .http
            .post_json("/api/v1/ack/batch", &req, &Opts::default())
            .await?;
        parse_ack_results(results, messages.len())
    }
}

/// A rejected ack arrives as HTTP 200 with `success: false` on the item, so the
/// per-item flag is the only signal that the broker accepted it. A short or
/// misaligned array is refused outright rather than misattributed to the wrong
/// message.
fn parse_ack_results(results: Option<Vec<AckResult>>, expected: usize) -> Result<Vec<AckResult>> {
    let results = results.ok_or_else(|| Error::Decode("ack returned an empty body".into()))?;
    if results.len() != expected {
        return Err(Error::Decode(format!(
            "ack returned {} results for {expected} acknowledgments",
            results.len()
        )));
    }
    Ok(results)
}

fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(index: usize, success: bool) -> AckResult {
        AckResult {
            index,
            transaction_id: format!("t{index}"),
            success,
            error: None,
            lease_released: false,
            dlq: false,
            noop: false,
        }
    }

    #[test]
    fn a_misaligned_ack_response_is_refused() {
        // Silently zipping 1 result onto 2 acks would report the wrong message
        // as acknowledged.
        assert!(parse_ack_results(Some(vec![result(0, true)]), 2).is_err());
        assert!(parse_ack_results(Some(vec![]), 1).is_err());
        assert!(parse_ack_results(None, 1).is_err());
        assert!(parse_ack_results(Some(vec![result(0, true)]), 1).is_ok());
    }

    #[test]
    fn non_empty_maps_the_absent_sentinel() {
        // An autoAck delivery carries an empty leaseId; sending "" would be a
        // lookup for a lease that does not exist.
        assert_eq!(non_empty(""), None);
        assert_eq!(non_empty("L1"), Some("L1".to_string()));
    }
}
