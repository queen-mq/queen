//! The bytes that cross the Raft RPC port, by what they carry.
//!
//! Each side counts what it moves: the server side in a layer around every
//! Raft route ([`super::network`]'s `serve`), the client side at each call
//! (`network::post_raw`, the append stream, the forward streams, the standby
//! link). So a node's `sent` is what it wrote and its `received` what it
//! read, and the cluster's `sent`, summed over its nodes, is what crossed the
//! wire.
//!
//! | channel | routes |
//! |---|---|
//! | replication | `/raft/v1/stream`, `/raft/v1/append` |
//! | forward | `/raft/v1/forward`, `/raft/v1/submit`: a follower's client commands |
//! | snapshot | `/raft/v1/snapshot` |
//! | link | `/link/v1/read`, `/link/v1/snapshot`: a standby cluster's reads |
//! | control | every other route: votes, the members view, membership, gathers, read index, a snapshot's basis |
//!
//! Bodies only: HTTP heads (a few hundred bytes a call) and TCP's own bytes
//! are not counted. A streamed body counts as its frames pass, so a snapshot
//! shows while it is sent, not when it ends.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::{Body, Bytes, HttpBody};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Channel {
    Replication,
    Forward,
    Snapshot,
    Link,
    Control,
}

impl Channel {
    /// Every channel, in the order [`totals`] answers them.
    pub(crate) const ALL: [Channel; 5] = [
        Channel::Replication,
        Channel::Forward,
        Channel::Snapshot,
        Channel::Link,
        Channel::Control,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Channel::Replication => "replication",
            Channel::Forward => "forward",
            Channel::Snapshot => "snapshot",
            Channel::Link => "link",
            Channel::Control => "control",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dir {
    Sent,
    Received,
}

// [channel][dir], flattened.
static BYTES: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];

fn slot(ch: Channel, dir: Dir) -> usize {
    ch as usize * 2 + dir as usize
}

/// Count `n` bytes this node moved on `ch`.
pub(crate) fn add(ch: Channel, dir: Dir, n: usize) {
    if n > 0 {
        BYTES[slot(ch, dir)].fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// The bytes counted since this process started: per channel, in
/// [`Channel::ALL`] order, `[sent, received]`.
pub(crate) fn totals() -> [[u64; 2]; 5] {
    let mut out = [[0u64; 2]; 5];
    for (i, ch) in Channel::ALL.into_iter().enumerate() {
        out[i] = [
            BYTES[slot(ch, Dir::Sent)].load(Ordering::Relaxed),
            BYTES[slot(ch, Dir::Received)].load(Ordering::Relaxed),
        ];
    }
    out
}

/// The channel of a Raft route, given its path or a URL to it.
pub(crate) fn channel_of(path_or_url: &str) -> Channel {
    let path = match path_or_url.split_once("://") {
        Some((_, rest)) => rest.find('/').map_or("", |i| &rest[i..]),
        None => path_or_url,
    };
    let path = path.split('?').next().unwrap_or_default();
    if path == super::stream::STREAM_PATH || path == "/raft/v1/append" {
        Channel::Replication
    } else if path == super::forward::ROUTE || path == "/raft/v1/submit" {
        Channel::Forward
    } else if path == "/raft/v1/snapshot" {
        Channel::Snapshot
    } else if path == crate::rsm::link::wire::READ_PATH
        || path == crate::rsm::link::wire::SNAPSHOT_PATH
    {
        Channel::Link
    } else {
        Channel::Control
    }
}

/// `body`, counted on `ch` as it is read. A body of known length is counted
/// whole, at once, and passed on untouched, so what goes out keeps its
/// `content-length`; a stream is counted frame by frame.
pub(crate) fn counted<B>(body: B, ch: Channel, dir: Dir) -> Body
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    use http_body_util::BodyExt;
    if let Some(n) = body.size_hint().exact() {
        add(ch, dir, n as usize);
        return Body::new(body);
    }
    Body::new(body.map_frame(move |frame| {
        if let Some(data) = frame.data_ref() {
            add(ch, dir, data.len());
        }
        frame
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn every_route_has_its_channel() {
        assert_eq!(channel_of("/raft/v1/stream"), Channel::Replication);
        assert_eq!(channel_of("/raft/v1/append"), Channel::Replication);
        assert_eq!(channel_of("/raft/v1/forward"), Channel::Forward);
        assert_eq!(channel_of("/raft/v1/submit"), Channel::Forward);
        assert_eq!(channel_of("/raft/v1/snapshot"), Channel::Snapshot);
        assert_eq!(channel_of("/raft/v1/snapshot/basis"), Channel::Control);
        assert_eq!(channel_of("/link/v1/read"), Channel::Link);
        assert_eq!(channel_of("/link/v1/snapshot"), Channel::Link);
        assert_eq!(channel_of("/raft/v1/vote"), Channel::Control);
        assert_eq!(channel_of("/raft/v1/local"), Channel::Control);
        // A URL, as the client side has it.
        assert_eq!(
            channel_of("http://10.0.0.7:7001/raft/v1/append"),
            Channel::Replication
        );
        assert_eq!(
            channel_of("http://q-1.q:7001/raft/v1/snapshot/basis"),
            Channel::Control
        );
        assert_eq!(channel_of("http://q-1.q:7001"), Channel::Control);
    }

    // The counters are process-wide and other tests move bytes on them at the
    // same time: each check only asks that its channel grew by at least what
    // it moved.

    #[tokio::test]
    async fn a_body_of_known_length_is_counted_whole_and_keeps_its_length() {
        let before = totals()[3][0];
        let body = counted(Body::from(vec![7u8; 1000]), Channel::Link, Dir::Sent);
        assert_eq!(body.size_hint().exact(), Some(1000));
        assert!(totals()[3][0] >= before + 1000);
        let got = body.collect().await.unwrap().to_bytes();
        assert_eq!(got.len(), 1000);
    }

    #[tokio::test]
    async fn a_stream_is_counted_as_its_frames_pass() {
        let before = totals()[3][1];
        let chunks = futures_util::stream::iter(
            [b"abc".as_slice(), b"defgh".as_slice()]
                .map(|c| Ok::<_, std::io::Error>(Bytes::from_static(c))),
        );
        let body = counted(Body::from_stream(chunks), Channel::Link, Dir::Received);
        assert_eq!(body.size_hint().exact(), None);
        let got = body.collect().await.unwrap().to_bytes();
        assert_eq!(&got[..], b"abcdefgh");
        assert!(totals()[3][1] >= before + 8);
    }
}
