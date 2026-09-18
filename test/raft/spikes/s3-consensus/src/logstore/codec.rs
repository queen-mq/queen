//! Codec wrappers giving openraft's types the `codeq::Codec` impls `raft_log`
//! requires. Adapted from openraft `examples/log-wal/src/codec.rs`.
//!
//! MessagePack is self-delimiting, so a decoder stops at the end of one value
//! inside the WAL stream instead of reading to the end of it.

use std::any::type_name;
use std::cmp::Ordering;
use std::io;

use openraft::alias::VoteOf;
use openraft::vote::RaftVote;
use openraft::RaftTypeConfig;
use raft_log::codeq::Decode;
use raft_log::codeq::Encode;
use raft_log::codeq::OffsetWriter;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MsgPack<T>(pub T);

impl<T> MsgPack<T>
where
    T: Serialize,
{
    /// Bytes `encode` writes; `raft_log` bounds its payload cache with it.
    pub fn encoded_len(&self) -> u64 {
        match encode_msgpack(&self.0, io::sink()) {
            Ok(len) => len as u64,
            Err(e) => {
                tracing::warn!("logstore: cannot measure {}: {e}", type_name::<T>());
                0
            }
        }
    }
}

impl<T> Encode for MsgPack<T>
where
    T: Serialize,
{
    fn encode<W: io::Write>(&self, w: W) -> Result<usize, io::Error> {
        encode_msgpack(&self.0, w)
    }
}

impl<T> Decode for MsgPack<T>
where
    T: DeserializeOwned,
{
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        Ok(MsgPack(decode_msgpack(r)?))
    }
}

/// The vote needs its own wrapper: `raft_log::Types::Vote` requires
/// `PartialOrd`, which openraft's `RaftVote` does not have as a supertrait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgPackVote<C: RaftTypeConfig>(pub VoteOf<C>);

impl<C: RaftTypeConfig> PartialOrd for MsgPackVote<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        RaftVote::partial_cmp(&self.0, &other.0)
    }
}

impl<C: RaftTypeConfig> Encode for MsgPackVote<C> {
    fn encode<W: io::Write>(&self, w: W) -> Result<usize, io::Error> {
        encode_msgpack(&self.0, w)
    }
}

impl<C: RaftTypeConfig> Decode for MsgPackVote<C> {
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        Ok(MsgPackVote(decode_msgpack(r)?))
    }
}

fn encode_msgpack<T, W>(value: &T, mut w: W) -> Result<usize, io::Error>
where
    T: Serialize,
    W: io::Write,
{
    let mut offset_writer = OffsetWriter::new(&mut w);
    rmp_serde::encode::write_named(&mut offset_writer, value).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{e}; when:(encode {})", type_name::<T>()),
        )
    })?;
    Ok(offset_writer.offset())
}

fn decode_msgpack<T, R>(r: R) -> Result<T, io::Error>
where
    T: DeserializeOwned,
    R: io::Read,
{
    rmp_serde::decode::from_read(r).map_err(|e| {
        // An incomplete record at the WAL tail must keep its io error kind:
        // raft_log tells it apart from corruption by the kind.
        let kind = match &e {
            rmp_serde::decode::Error::InvalidMarkerRead(io_err) => io_err.kind(),
            rmp_serde::decode::Error::InvalidDataRead(io_err) => io_err.kind(),
            _ => io::ErrorKind::InvalidData,
        };
        io::Error::new(kind, format!("{e}; when:(decode {})", type_name::<T>()))
    })
}
