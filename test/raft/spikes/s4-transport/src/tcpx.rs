//! Transport (a): length-prefixed frames over a few persistent TCP
//! connections (PLAN_RAFT.md §9.2, §12.5, D12).
//!
//! Shape, both sides:
//!   - one READER task per socket. It only ever calls `frame::read_frame`
//!     with `read_exact` and hands whole frames to a channel. No frame is ever
//!     read inside a `select!` (cancellation safety — the pgless lane bug).
//!   - one WRITER task per socket, owning the write half, coalescing whatever
//!     is already queued into a single `write_all` (one syscall per burst).
//!   - work happens in a third task, so a slow handler cannot stall the reader.
//!
//! Requests are multiplexed by request id: several commands are in flight on
//! the same connection at once, which is the property HTTP/1.1 does not have.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::auth::{self, Peers};
use crate::frame::{self, MacCtx, DIR_C2S, DIR_S2C, T_CMD, T_OUTCOME, T_STATS_REQ, T_STATS_RESP};
use crate::net::{self, Counters};
use crate::wire;

pub const CLUSTER_ID: u64 = 0x5175_6565_6e00_0001;
pub const LEADER_ID: u64 = 1;
pub const RECEIVER_ID: u64 = 2;

type Pending = Arc<Mutex<HashMap<[u8; 16], oneshot::Sender<Vec<u8>>>>>;

/// Every RPC has a deadline (§0.3, I15, §9.2 "every request carries its
/// remaining budget"). Added 2026-09-18: the measured passes ran an
/// `rx.await` with no deadline at all, so a lost outcome hung its caller for
/// ever. The timer lives entirely on the receiver side, so no leader CPU
/// figure in RESULTS-vm.md is affected by it.
pub const DEFAULT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

struct Conn {
    /// (frame type, body): the writer task encodes, because with the revised
    /// per-frame MAC the sequence number must be assigned where frames are
    /// serialised.
    tx: mpsc::Sender<(u8, Vec<u8>)>,
    pending: Pending,
}

pub struct TcpClient {
    conns: Vec<Conn>,
    rr: AtomicU64,
    pub counters: Arc<Counters>,
    /// mean wall time of connect + handshake, microseconds
    pub handshake_us: u64,
}

impl TcpClient {
    pub async fn connect(
        addr: SocketAddr,
        n: usize,
        secret: &[u8],
        per_frame_mac: bool,
        tls: Option<Arc<rustls::ClientConfig>>,
        counters: Arc<Counters>,
    ) -> io::Result<Self> {
        let mut conns = Vec::with_capacity(n);
        let mut hs_total = 0u64;
        for _ in 0..n {
            let t0 = Instant::now();
            let io = net::connect(addr, tls.clone(), counters.clone()).await?;
            let (mut r, mut w) = tokio::io::split(io);
            let key = auth::client(
                &mut r,
                &mut w,
                secret,
                Peers {
                    cluster_id: CLUSTER_ID,
                    from: RECEIVER_ID,
                    to: LEADER_ID,
                },
            )
            .await?;
            hs_total += t0.elapsed().as_micros() as u64;
            let (mut send_mac, mut recv_mac) = if per_frame_mac {
                (
                    Some(MacCtx::new(&key, DIR_C2S)),
                    Some(MacCtx::new(&key, DIR_S2C)),
                )
            } else {
                (None, None)
            };
            let pending: Pending = Arc::new(Mutex::new(HashMap::new()));

            // reader task: whole frames only, resolved by request id
            let p2 = pending.clone();
            tokio::spawn(async move {
                let mut buf = Vec::with_capacity(64 * 1024);
                loop {
                    match frame::read_frame(&mut r, &mut buf, recv_mac.as_mut()).await {
                        Ok((ty, range)) => {
                            if ty == T_OUTCOME || ty == T_STATS_RESP {
                                let body = &buf[range];
                                if let Some(id) = wire::outcome_req_id(body) {
                                    let s = p2.lock().unwrap().remove(&id);
                                    if let Some(s) = s {
                                        let _ = s.send(body.to_vec());
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                // The connection is gone: drop every waiter's sender so that
                // in-flight requests fail NOW (D6 OutcomeUnknown, retryable)
                // instead of waiting out their deadline. Before 2026-09-18
                // this map kept the senders alive and every in-flight forward
                // on a dead socket hung for ever.
                p2.lock().unwrap().clear();
            });

            // writer task: encode (assigning the frame sequence) and coalesce
            // whatever is already queued into one write
            let (tx, mut rx) = mpsc::channel::<(u8, Vec<u8>)>(4096);
            tokio::spawn(async move {
                let mut out = Vec::with_capacity(256 * 1024);
                while let Some((ty, body)) = rx.recv().await {
                    out.clear();
                    frame::encode_into(&mut out, ty, &body, send_mac.as_mut());
                    while let Ok((ty, body)) = rx.try_recv() {
                        frame::encode_into(&mut out, ty, &body, send_mac.as_mut());
                        if out.len() > 512 * 1024 {
                            break;
                        }
                    }
                    if w.write_all(&out).await.is_err() {
                        break;
                    }
                    if w.flush().await.is_err() {
                        break;
                    }
                }
            });
            conns.push(Conn { tx, pending });
        }
        Ok(Self {
            conns,
            rr: AtomicU64::new(0),
            counters,
            handshake_us: hs_total / n as u64,
        })
    }

    async fn round_trip(
        &self,
        ty: u8,
        req_id: [u8; 16],
        body: &[u8],
        deadline: std::time::Duration,
    ) -> io::Result<Vec<u8>> {
        let i = (self.rr.fetch_add(1, Ordering::Relaxed) as usize) % self.conns.len();
        let c = &self.conns[i];
        let (tx, rx) = oneshot::channel();
        c.pending.lock().unwrap().insert(req_id, tx);
        if c.tx.send((ty, body.to_vec())).await.is_err() {
            c.pending.lock().unwrap().remove(&req_id);
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer gone"));
        }
        match tokio::time::timeout(deadline, rx).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed",
            )),
            Err(_) => {
                // I15/§9.2: the budget is spent. Drop the waiter so the map
                // cannot grow without bound, and tell the caller the outcome
                // is unknown (D6: the retry reuses the same request id).
                c.pending.lock().unwrap().remove(&req_id);
                Err(io::Error::new(io::ErrorKind::TimedOut, "deadline"))
            }
        }
    }

    pub async fn forward(&self, req_id: [u8; 16], body: &[u8]) -> io::Result<Vec<u8>> {
        self.round_trip(T_CMD, req_id, body, DEFAULT_DEADLINE).await
    }

    pub async fn stats(&self, req_id: [u8; 16]) -> io::Result<wire::Stats> {
        let b = self
            .round_trip(T_STATS_REQ, req_id, &req_id, DEFAULT_DEADLINE)
            .await?;
        wire::Stats::decode(&b).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "stats"))
    }
}

// ------------------------------------------------------------------ leader

pub async fn serve(
    addr: SocketAddr,
    secret: Vec<u8>,
    per_frame_mac: bool,
    tls: Option<Arc<rustls::ServerConfig>>,
    counters: Arc<Counters>,
) -> io::Result<()> {
    let l = TcpListener::bind(addr).await?;
    eprintln!(
        "[leader] tcp listening on {addr} mac={per_frame_mac} tls={}",
        tls.is_some()
    );
    loop {
        let (s, _peer) = l.accept().await?;
        let tls = tls.clone();
        let c = counters.clone();
        let secret = secret.clone();
        tokio::spawn(async move {
            let _g = net::OpenGuard(c.clone());
            if let Err(e) = serve_conn(s, secret, per_frame_mac, tls, c.clone()).await {
                eprintln!("[leader] conn ended: {e}");
            }
        });
    }
}

async fn serve_conn(
    s: tokio::net::TcpStream,
    secret: Vec<u8>,
    per_frame_mac: bool,
    tls: Option<Arc<rustls::ServerConfig>>,
    counters: Arc<Counters>,
) -> io::Result<()> {
    let io = net::accept_wrap(s, tls, counters.clone()).await?;
    let (mut r, mut w) = tokio::io::split(io);
    let key = auth::server(&mut r, &mut w, &secret, CLUSTER_ID, LEADER_ID).await?;
    let (mut send_mac, mut recv_mac) = if per_frame_mac {
        (
            Some(MacCtx::new(&key, DIR_S2C)),
            Some(MacCtx::new(&key, DIR_C2S)),
        )
    } else {
        (None, None)
    };

    let (out_tx, mut out_rx) = mpsc::channel::<(u8, Vec<u8>)>(4096);
    tokio::spawn(async move {
        let mut out = Vec::with_capacity(256 * 1024);
        while let Some((ty, body)) = out_rx.recv().await {
            out.clear();
            frame::encode_into(&mut out, ty, &body, send_mac.as_mut());
            while let Ok((ty, body)) = out_rx.try_recv() {
                frame::encode_into(&mut out, ty, &body, send_mac.as_mut());
                if out.len() > 512 * 1024 {
                    break;
                }
            }
            if w.write_all(&out).await.is_err() {
                break;
            }
            if w.flush().await.is_err() {
                break;
            }
        }
    });

    // work task: decoding and answering never happen on the reader task
    let (work_tx, mut work_rx) = mpsc::channel::<(u8, Vec<u8>)>(4096);
    let c2 = counters.clone();
    tokio::spawn(async move {
        while let Some((ty, body)) = work_rx.recv().await {
            let resp = handle(ty, &body, &c2);
            if let Some((rty, rbody)) = resp {
                if out_tx.send((rty, rbody)).await.is_err() {
                    break;
                }
            }
        }
    });

    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        let (ty, range) = frame::read_frame(&mut r, &mut buf, recv_mac.as_mut()).await?;
        if work_tx.send((ty, buf[range].to_vec())).await.is_err() {
            break;
        }
    }
    Ok(())
}

/// The synthetic leader-side work: the same for both transports.
pub fn handle(ty: u8, body: &[u8], c: &Counters) -> Option<(u8, Vec<u8>)> {
    match ty {
        T_CMD => {
            let p = wire::decode_command(body).ok()?;
            let n = c.cmds.fetch_add(1, Ordering::Relaxed) + 1;
            c.msgs.fetch_add(p.count as u64, Ordering::Relaxed);
            Some((
                T_OUTCOME,
                wire::encode_outcome(&p.req_id, n, p.count, n * 1000),
            ))
        }
        T_STATS_REQ => {
            let mut id = [0u8; 16];
            if body.len() >= 16 {
                id.copy_from_slice(&body[0..16]);
            }
            Some((T_STATS_RESP, stats_of(c).encode(&id)))
        }
        _ => None,
    }
}

pub fn stats_of(c: &Counters) -> wire::Stats {
    let (user_us, sys_us, rss_bytes) = net::rusage();
    let (rx_bytes, tx_bytes, conns, open, cmds, msgs) = c.snapshot();
    wire::Stats {
        user_us,
        sys_us,
        rss_bytes,
        rx_bytes,
        tx_bytes,
        conns,
        open,
        cmds,
        msgs,
    }
}
