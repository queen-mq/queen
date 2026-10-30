//! Kafka error codes, and the broker's answers mapped onto them.

#![allow(dead_code)]

use crate::core::{CallError, LocalResponse};

pub const NONE: i16 = 0;
pub const UNKNOWN_SERVER_ERROR: i16 = -1;
pub const OFFSET_OUT_OF_RANGE: i16 = 1;
pub const CORRUPT_MESSAGE: i16 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const LEADER_NOT_AVAILABLE: i16 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const REQUEST_TIMED_OUT: i16 = 7;
pub const MESSAGE_TOO_LARGE: i16 = 10;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const INVALID_GROUP_ID: i16 = 24;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
pub const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const ILLEGAL_SASL_STATE: i16 = 34;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const TOPIC_ALREADY_EXISTS: i16 = 36;
pub const INVALID_PARTITIONS: i16 = 37;
pub const INVALID_REPLICATION_FACTOR: i16 = 38;
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const INVALID_CONFIG: i16 = 40;
pub const INVALID_REQUEST: i16 = 42;
pub const POLICY_VIOLATION: i16 = 44;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
pub const KAFKA_STORAGE_ERROR: i16 = 56;
pub const SASL_AUTHENTICATION_FAILED: i16 = 58;
pub const NON_EMPTY_GROUP: i16 = 68;
pub const GROUP_ID_NOT_FOUND: i16 = 69;
pub const MEMBER_ID_REQUIRED: i16 = 79;
pub const GROUP_MAX_SIZE_REACHED: i16 = 81;
pub const INVALID_RECORD: i16 = 87;

/// A route that refused, as the error of a request a client may retry.
pub fn of_status(code: u16) -> i16 {
    match code {
        401 | 403 => TOPIC_AUTHORIZATION_FAILED,
        404 => UNKNOWN_TOPIC_OR_PARTITION,
        413 => MESSAGE_TOO_LARGE,
        400 | 422 => INVALID_REQUEST,
        // 429 (admission, quota), 503 (no leader, storage pressure), 504 and
        // anything else: retriable, and a retry is what the client needs.
        _ => REQUEST_TIMED_OUT,
    }
}

pub fn of_refusal(r: &LocalResponse) -> i16 {
    of_status(r.status)
}

/// For the group and offset APIs: anything but an auth refusal is the
/// coordinator not being able to answer now, which every client retries.
pub fn of_call_for_group(e: &CallError) -> i16 {
    match e {
        CallError::Status {
            code: 401 | 403, ..
        } => GROUP_AUTHORIZATION_FAILED,
        _ => COORDINATOR_NOT_AVAILABLE,
    }
}

/// For the admin APIs.
pub fn of_call(e: &CallError) -> i16 {
    match e {
        CallError::Status { code, .. } => of_status(*code),
        _ => REQUEST_TIMED_OUT,
    }
}
