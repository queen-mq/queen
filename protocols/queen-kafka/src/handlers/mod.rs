//! One module per family of Kafka requests. Each turns a decoded request into
//! broker operations and builds the answer; none keeps state of its own.

pub mod admin;
pub mod fetch;
pub mod group;
pub mod meta;
pub mod offsets;
pub mod produce;
pub mod sasl;

use kafka_protocol::protocol::StrBytes;

pub fn s(v: impl Into<String>) -> StrBytes {
    StrBytes::from_string(v.into())
}
