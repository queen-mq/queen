//! Mutual HMAC handshake of PLAN_RAFT.md §9.2 / D12.
//!
//! Each side sends a fresh 32-byte nonce; each proves knowledge of
//! `QUEEN_RAFT_SECRET` with an HMAC over BOTH nonces, the cluster id and the
//! (from, to) node ids, so a transcript cannot be replayed at another node or
//! in the other direction. The session key derived from the same transcript
//! keys the per-frame MAC (D12) when TLS is not used.
//!
//! Wire: HELLO{nonce_c, cluster_id, from, to} -> HELLO_ACK{nonce_s, mac_s}
//!       -> AUTH{mac_c} -> AUTH_OK{}  (two round trips)

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::frame::{self, SessionKey, T_AUTH, T_AUTH_OK, T_HELLO, T_HELLO_ACK};

#[derive(Clone, Copy)]
pub struct Peers {
    pub cluster_id: u64,
    pub from: u64,
    pub to: u64,
}

fn mac_of(secret: &[u8], dom: u8, nc: &[u8; 32], ns: &[u8; 32], p: &Peers) -> Hmac<Sha256> {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(secret).unwrap();
    m.update(&[dom]);
    m.update(nc);
    m.update(ns);
    m.update(&p.cluster_id.to_le_bytes());
    m.update(&p.from.to_le_bytes());
    m.update(&p.to.to_le_bytes());
    m
}

fn tag(secret: &[u8], dom: u8, nc: &[u8; 32], ns: &[u8; 32], p: &Peers) -> [u8; 32] {
    let out = mac_of(secret, dom, nc, ns, p).finalize().into_bytes();
    let mut t = [0u8; 32];
    t.copy_from_slice(&out);
    t
}

/// Constant-time check of a peer's proof (hmac's own `verify_slice`, as
/// mesh.rs does at server/src/mesh.rs:935).
fn verify(secret: &[u8], dom: u8, nc: &[u8; 32], ns: &[u8; 32], p: &Peers, got: &[u8]) -> bool {
    mac_of(secret, dom, nc, ns, p).verify_slice(got).is_ok()
}

fn bad(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, what)
}

/// D12: "Multi-node mode refuses to start without a secret." pgless U19 was an
/// empty secret silently meaning *open mode*; `Hmac::new_from_slice` accepts a
/// zero-length key, so nothing below would have noticed. Both sides of the
/// handshake fail closed here, and `main.rs` refuses to start as well.
/// (Added 2026-09-18: the refutation of MEMO.md found that the memo claimed
/// fail-closed behaviour that no code implemented.)
pub const MIN_SECRET_LEN: usize = 16;

pub fn check_secret(secret: &[u8]) -> io::Result<()> {
    if secret.len() < MIN_SECRET_LEN {
        return Err(bad("QUEEN_RAFT_SECRET missing or too short"));
    }
    Ok(())
}

pub async fn client<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    r: &mut R,
    w: &mut W,
    secret: &[u8],
    p: Peers,
) -> io::Result<SessionKey> {
    check_secret(secret)?;
    let mut nc = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut nc);
    let mut body = Vec::with_capacity(32 + 24);
    body.extend_from_slice(&nc);
    body.extend_from_slice(&p.cluster_id.to_le_bytes());
    body.extend_from_slice(&p.from.to_le_bytes());
    body.extend_from_slice(&p.to.to_le_bytes());
    w.write_all(&frame::encode(T_HELLO, &body, None)).await?;
    w.flush().await?;

    let mut buf = Vec::new();
    let (ty, range) = frame::read_frame(r, &mut buf, None).await?;
    if ty != T_HELLO_ACK || range.len() != 64 {
        return Err(bad("handshake: hello_ack"));
    }
    let mut ns = [0u8; 32];
    ns.copy_from_slice(&buf[range.start..range.start + 32]);
    if !verify(
        secret,
        b'S',
        &nc,
        &ns,
        &p,
        &buf[range.start + 32..range.end],
    ) {
        return Err(bad("handshake: server mac"));
    }
    let mac_c = tag(secret, b'C', &nc, &ns, &p);
    w.write_all(&frame::encode(T_AUTH, &mac_c, None)).await?;
    w.flush().await?;
    let (ty, _) = frame::read_frame(r, &mut buf, None).await?;
    if ty != T_AUTH_OK {
        return Err(bad("handshake: auth_ok"));
    }
    Ok(tag(secret, b'K', &nc, &ns, &p))
}

pub async fn server<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    r: &mut R,
    w: &mut W,
    secret: &[u8],
    expect_cluster: u64,
    my_id: u64,
) -> io::Result<SessionKey> {
    check_secret(secret)?;
    let mut buf = Vec::new();
    let (ty, range) = frame::read_frame(r, &mut buf, None).await?;
    if ty != T_HELLO || range.len() != 56 {
        return Err(bad("handshake: hello"));
    }
    let b = &buf[range.clone()];
    let mut nc = [0u8; 32];
    nc.copy_from_slice(&b[0..32]);
    let p = Peers {
        cluster_id: u64::from_le_bytes(b[32..40].try_into().unwrap()),
        from: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        to: u64::from_le_bytes(b[48..56].try_into().unwrap()),
    };
    // I9: a node rejects RPCs that are not addressed to its own identity.
    if p.cluster_id != expect_cluster || p.to != my_id {
        return Err(bad("handshake: identity mismatch"));
    }
    let mut ns = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut ns);
    let mut ack = Vec::with_capacity(64);
    ack.extend_from_slice(&ns);
    ack.extend_from_slice(&tag(secret, b'S', &nc, &ns, &p));
    w.write_all(&frame::encode(T_HELLO_ACK, &ack, None)).await?;
    w.flush().await?;

    let (ty, range) = frame::read_frame(r, &mut buf, None).await?;
    if ty != T_AUTH || range.len() != 32 {
        return Err(bad("handshake: auth"));
    }
    if !verify(secret, b'C', &nc, &ns, &p, &buf[range.clone()]) {
        return Err(bad("handshake: client mac"));
    }
    w.write_all(&frame::encode(T_AUTH_OK, &[], None)).await?;
    w.flush().await?;
    Ok(tag(secret, b'K', &nc, &ns, &p))
}

/// HTTP variant: a pooled keep-alive connection cannot carry a per-connection
/// handshake (the pool hands out whichever connection is idle), so the HTTP
/// transport authenticates every request with a MAC over request id + body,
/// keyed directly by the shared secret.
pub fn request_mac(secret: &[u8], body: &[u8]) -> String {
    let out = request_mac_bin(secret, body);
    let mut s = String::with_capacity(32);
    for b in &out {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// The form the product would use, and the one the HTTP comparison should
/// have used: no allocation, and verified in constant time. `request_mac`
/// above (hex through `format!`, compared with `String ==`) is what the
/// measured HTTP+MAC rows ran; `bench` in main.rs prices the difference.
pub fn request_mac_bin(secret: &[u8], body: &[u8]) -> [u8; 16] {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(secret).unwrap();
    m.update(body);
    let out = m.finalize().into_bytes();
    let mut t = [0u8; 16];
    t.copy_from_slice(&out[..16]);
    t
}

pub fn verify_request_mac(secret: &[u8], body: &[u8], got: &[u8]) -> bool {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(secret).unwrap();
    m.update(body);
    m.verify_truncated_left(got).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    /// Runs the handshake over a real socket pair and returns both outcomes.
    async fn handshake(
        client_secret: &'static [u8],
        server_secret: &'static [u8],
        p: Peers,
        server_id: u64,
        server_cluster: u64,
    ) -> (io::Result<SessionKey>, io::Result<SessionKey>) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (mut r, mut w) = tokio::io::split(s);
            server(&mut r, &mut w, server_secret, server_cluster, server_id).await
        });
        // The client's socket halves must be DROPPED before awaiting the
        // server: when the client rejects the server's proof it simply walks
        // away, and a server still parked in `read_frame` only unblocks on the
        // EOF that closing the socket delivers.
        let c = {
            let s = TcpStream::connect(addr).await.unwrap();
            let (mut r, mut w) = tokio::io::split(s);
            client(&mut r, &mut w, client_secret, p).await
        };
        let s = tokio::time::timeout(std::time::Duration::from_secs(5), srv)
            .await
            .expect("server handshake did not finish within 5 s")
            .unwrap();
        (c, s)
    }

    #[tokio::test]
    async fn an_empty_or_short_secret_fails_closed() {
        // pgless U19: an unset secret must not mean "open mode". Both sides
        // refuse before a single frame is written.
        let (c, s) = handshake(b"", b"", P, 1, 42).await;
        assert_eq!(c.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(s.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert!(check_secret(b"").is_err());
        assert!(check_secret(b"short").is_err());
        assert!(check_secret(b"0123456789abcdef").is_ok());
    }

    #[tokio::test]
    async fn request_mac_binary_and_hex_agree_and_verify_constant_time() {
        let body = vec![1u8; 2800];
        let bin = request_mac_bin(b"0123456789abcdef", &body);
        assert_eq!(request_mac(b"0123456789abcdef", &body), hex(&bin));
        assert!(verify_request_mac(b"0123456789abcdef", &body, &bin));
        assert!(!verify_request_mac(b"0123456789abcdeg", &body, &bin));
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    const SEC: &[u8] = b"a-long-enough-spike-secret";

    const P: Peers = Peers {
        cluster_id: 42,
        from: 2,
        to: 1,
    };

    #[tokio::test]
    async fn matching_secret_agrees_on_a_session_key() {
        let (c, s) = handshake(SEC, SEC, P, 1, 42).await;
        assert_eq!(c.unwrap(), s.unwrap());
    }

    #[tokio::test]
    async fn wrong_secret_is_rejected_by_the_client_first() {
        // The server proves knowledge first, so a fake leader is caught before
        // the receiver reveals anything usable (§9.2, both nonces bound).
        let (c, s) = handshake(SEC, b"another-long-secret", P, 1, 42).await;
        assert_eq!(c.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert!(s.is_err());
    }

    #[tokio::test]
    async fn wrong_node_id_or_cluster_is_rejected_by_the_server() {
        // I9: a node rejects RPCs not addressed to its own identity.
        let (_, s) = handshake(SEC, SEC, P, 7, 42).await;
        assert_eq!(s.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        let (_, s) = handshake(SEC, SEC, P, 1, 43).await;
        assert_eq!(s.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn the_transcript_does_not_replay_in_the_other_direction() {
        // client and server tags differ by domain byte, so a recorded server
        // proof cannot be replayed as a client proof.
        let nc = [1u8; 32];
        let ns = [2u8; 32];
        assert_ne!(tag(SEC, b'S', &nc, &ns, &P), tag(SEC, b'C', &nc, &ns, &P));
        // and the key is neither of the two proofs
        let k = tag(SEC, b'K', &nc, &ns, &P);
        assert_ne!(k, tag(SEC, b'S', &nc, &ns, &P));
        assert_ne!(k, tag(SEC, b'C', &nc, &ns, &P));
    }

    #[tokio::test]
    async fn a_proof_for_another_peer_pair_does_not_transfer() {
        let nc = [1u8; 32];
        let ns = [2u8; 32];
        let other = Peers {
            cluster_id: 42,
            from: 2,
            to: 3,
        };
        assert_ne!(
            tag(SEC, b'C', &nc, &ns, &P),
            tag(SEC, b'C', &nc, &ns, &other)
        );
    }
}
