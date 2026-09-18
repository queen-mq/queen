//! Spike S4 (PLAN_RAFT.md WP-0.6): forwarding transport for port 6634.
//!
//! Two processes, two transports behind one trait:
//!   leader   — the process that would hold the planner: decodes forwarded
//!              commands and answers outcomes.
//!   receiver — the process a client request lands on: mints a request id per
//!              command (D6), forwards at a fixed offered rate and measures
//!              the round trip.
//!
//! The payload is synthetic: `batch` messages of `payload` bytes plus a
//! 16-byte dedup hash each (D10), i.e. the bytes a push batch actually
//! forwards. No disk, no planner, no Raft: this measures the transport only.
//!
//! Usage (see README.md for the full matrix):
//!   s4 leader   --transport tcp --port 6734 --secret S [--mac 1] [--tls 1 --cert-out /tmp/c.der]
//!   s4 receiver --transport tcp --addr 127.0.0.1:6734 --secret S --rate 50000 \
//!               --secs 60 --warmup 5 [--mac 1] [--tls 1 --cert /tmp/c.der]

mod auth;
mod frame;
mod httpx;
mod net;
mod tcpx;
mod wire;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use net::Counters;

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The seam of §12.1/§12.5: everything above this trait is transport-blind.
trait Transport: Send + Sync + 'static {
    fn forward(self: Arc<Self>, req_id: [u8; 16], body: Vec<u8>) -> BoxFut<io::Result<Vec<u8>>>;
    fn stats(self: Arc<Self>, req_id: [u8; 16]) -> BoxFut<io::Result<wire::Stats>>;
    fn handshake_us(&self) -> u64;
    fn counters(&self) -> Arc<Counters>;
    fn name(&self) -> &'static str;
}

impl Transport for tcpx::TcpClient {
    fn forward(self: Arc<Self>, req_id: [u8; 16], body: Vec<u8>) -> BoxFut<io::Result<Vec<u8>>> {
        Box::pin(async move { tcpx::TcpClient::forward(&self, req_id, &body).await })
    }
    fn stats(self: Arc<Self>, req_id: [u8; 16]) -> BoxFut<io::Result<wire::Stats>> {
        Box::pin(async move { tcpx::TcpClient::stats(&self, req_id).await })
    }
    fn handshake_us(&self) -> u64 {
        self.handshake_us
    }
    fn counters(&self) -> Arc<Counters> {
        self.counters.clone()
    }
    fn name(&self) -> &'static str {
        "tcp"
    }
}

impl Transport for httpx::HttpClient {
    fn forward(self: Arc<Self>, req_id: [u8; 16], body: Vec<u8>) -> BoxFut<io::Result<Vec<u8>>> {
        Box::pin(async move { httpx::HttpClient::forward(&self, req_id, &body).await })
    }
    fn stats(self: Arc<Self>, req_id: [u8; 16]) -> BoxFut<io::Result<wire::Stats>> {
        Box::pin(async move { httpx::HttpClient::stats(&self, req_id).await })
    }
    fn handshake_us(&self) -> u64 {
        self.handshake_us
    }
    fn counters(&self) -> Arc<Counters> {
        self.counters.clone()
    }
    fn name(&self) -> &'static str {
        "http"
    }
}

// --------------------------------------------------------------------- args

struct Args(Vec<(String, String)>);

impl Args {
    fn parse() -> (String, Self) {
        let mut a: Vec<String> = std::env::args().skip(1).collect();
        if a.is_empty() {
            eprintln!("usage: s4 leader|receiver [--flag value ...]");
            std::process::exit(2);
        }
        let mode = a.remove(0);
        let mut v = Vec::new();
        let mut i = 0;
        while i < a.len() {
            let k = a[i].trim_start_matches("--").to_string();
            let val = a.get(i + 1).cloned().unwrap_or_default();
            v.push((k, val));
            i += 2;
        }
        (mode, Args(v))
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0
            .iter()
            .find(|(a, _)| a == k)
            .map(|(_, b)| b.clone())
            .unwrap_or_else(|| d.to_string())
    }
    fn n(&self, k: &str, d: u64) -> u64 {
        self.s(k, &d.to_string()).parse().unwrap_or(d)
    }
    fn b(&self, k: &str, d: bool) -> bool {
        self.n(k, if d { 1 } else { 0 }) != 0
    }
}

fn main() {
    let (mode, a) = Args::parse();
    net::install_crypto_provider();
    let threads = a.n("threads", 4) as usize;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    match mode.as_str() {
        "leader" => rt.block_on(leader(a)),
        "receiver" => rt.block_on(receiver(a)),
        "bench" => bench(a),
        m => {
            eprintln!("unknown mode {m}");
            std::process::exit(2);
        }
    }
}

// ------------------------------------------------------------------- leader

async fn leader(a: Args) {
    let port = a.n("port", 6734) as u16;
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let secret = require_secret(&a);
    let mac = a.b("mac", false);
    let tls = if a.b("tls", false) {
        Some(net::server_tls(&a.s("cert-out", "/tmp/s4-cert.der")))
    } else {
        None
    };
    let c = Arc::new(Counters::default());
    let r = match a.s("transport", "tcp").as_str() {
        "tcp" => tcpx::serve(addr, secret, mac, tls, c).await,
        "http" => httpx::serve(addr, secret, mac, tls, c).await,
        t => {
            eprintln!("unknown transport {t}");
            std::process::exit(2);
        }
    };
    if let Err(e) = r {
        eprintln!("[leader] fatal: {e}");
        std::process::exit(1);
    }
}

// ----------------------------------------------------------------- receiver

struct Sample {
    due_elapsed_us: u64,
    from_send_us: u32,
    from_due_us: u32,
    ok: bool,
}

async fn receiver(a: Args) {
    let transport = a.s("transport", "tcp");
    let addr: SocketAddr = a.s("addr", "127.0.0.1:6734").parse().expect("addr");
    let secret = require_secret(&a);
    let mac = a.b("mac", false);
    let rate_msgs = a.n("rate", 20_000);
    let batch = a.n("batch", 10) as usize;
    let payload = a.n("payload", 256) as usize;
    let secs = a.n("secs", 60);
    let warmup = a.n("warmup", 5);
    let conns = a.n("conns", 2) as usize;
    let pool = a.n("pool", 64) as usize;
    // Pacer shape (WP-0.6 refutation round, 2026-09-18). `timer` is what both
    // VM passes ran: `tokio::time::sleep` per command, whose ~1 ms timer
    // granularity releases commands in bursts of ~5 at 5 000 cmd/s, which the
    // framed writer then coalesces into one `write_all`. `spin` keeps the same
    // due times but lands on them within a few microseconds, so at most one
    // command is queued per write and the coalescing advantage disappears.
    let spin_pace = a.s("pace", "timer") == "spin";
    let tls_on = a.b("tls", false);
    let ctls = if tls_on {
        Some(net::client_tls(&a.s("cert", "/tmp/s4-cert.der")))
    } else {
        None
    };

    let counters = Arc::new(Counters::default());
    let t: Arc<dyn Transport> = match transport.as_str() {
        "tcp" => Arc::new(
            tcpx::TcpClient::connect(addr, conns, &secret, mac, ctls, counters.clone())
                .await
                .expect("tcp connect"),
        ),
        "http" => Arc::new(
            httpx::HttpClient::connect(addr, pool, &secret, mac, ctls, counters.clone())
                .await
                .expect("http connect"),
        ),
        x => {
            eprintln!("unknown transport {x}");
            std::process::exit(2);
        }
    };

    let cmd_rate = (rate_msgs as f64 / batch as f64).round() as u64;
    let period_ns = 1_000_000_000f64 / cmd_rate as f64;
    let total = cmd_rate * (secs + warmup);
    let template = wire::encode_command(
        &[0u8; 16],
        0,
        "acme",
        "orders",
        "7",
        batch,
        &vec![0x5au8; payload],
    );

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Sample>();
    let collector = tokio::spawn(async move {
        let mut v: Vec<Sample> = Vec::with_capacity(1 << 20);
        while let Some(s) = rx.recv().await {
            v.push(s);
        }
        v
    });

    // A run-unique request id prefix; the counter is the receiver's mint (D6).
    let mut prefix = [0u8; 8];
    {
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut prefix);
    }
    let seq = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let max_lag_us = Arc::new(AtomicU64::new(0));

    // snapshots taken at the warmup boundary and at the end of the run
    let start = Instant::now();
    let warm_at = start + Duration::from_secs(warmup);
    let end_at = warm_at + Duration::from_secs(secs);

    let snap_a: Arc<tokio::sync::Mutex<Option<Snapshot>>> = Arc::new(tokio::sync::Mutex::new(None));
    {
        let t = t.clone();
        let counters = counters.clone();
        let snap_a = snap_a.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(warm_at)).await;
            let s = snapshot(&t, &counters).await;
            *snap_a.lock().await = Some(s);
        });
    }

    for i in 0..total {
        let due = start + Duration::from_nanos((i as f64 * period_ns) as u64);
        if spin_pace {
            // coarse sleep while far away, then a cooperative busy wait that
            // still lets the spawned command tasks run on this worker
            loop {
                let now = Instant::now();
                if now >= due {
                    break;
                }
                let d = due - now;
                if d > Duration::from_micros(1500) {
                    tokio::time::sleep(d - Duration::from_micros(1000)).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        } else {
            let now = Instant::now();
            if due > now {
                tokio::time::sleep(due - now).await;
            }
        }
        let mut body = template.clone();
        let mut req_id = [0u8; 16];
        req_id[0..8].copy_from_slice(&prefix);
        req_id[8..16].copy_from_slice(&seq.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        body[wire::REQ_ID_OFF..wire::REQ_ID_OFF + 16].copy_from_slice(&req_id);
        let t = t.clone();
        let tx = tx.clone();
        let errors = errors.clone();
        let max_lag_us = max_lag_us.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let lag = t0.saturating_duration_since(due).as_micros() as u64;
            max_lag_us.fetch_max(lag, Ordering::Relaxed);
            let r = t.forward(req_id, body).await;
            let done = Instant::now();
            let ok = match &r {
                Ok(b) => wire::outcome_req_id(b) == Some(req_id),
                Err(e) => {
                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                        eprintln!("[receiver] forward error: {e}");
                    }
                    false
                }
            };
            let _ = tx.send(Sample {
                due_elapsed_us: due.saturating_duration_since(start).as_micros() as u64,
                from_send_us: done.duration_since(t0).as_micros() as u32,
                from_due_us: done.saturating_duration_since(due).as_micros() as u32,
                ok,
            });
        });
        if Instant::now() > end_at {
            break;
        }
    }

    // Snapshot B closes the measured window as soon as the pacer is done, so
    // the drain below does not dilute the achieved rate or the CPU deltas.
    let snap_b = snapshot(&t, &counters).await;
    // let the last in-flight commands land
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(tx);
    let samples = collector.await.unwrap();
    let snap_a = snap_a.lock().await.take().expect("warmup snapshot");

    let lo = warmup * 1_000_000;
    let hi = lo + secs * 1_000_000;
    let mut send_us: Vec<u32> = Vec::with_capacity(samples.len());
    let mut due_us: Vec<u32> = Vec::with_capacity(samples.len());
    let mut bad = 0u64;
    for s in &samples {
        if s.due_elapsed_us < lo || s.due_elapsed_us >= hi {
            continue;
        }
        if !s.ok {
            bad += 1;
            continue;
        }
        send_us.push(s.from_send_us);
        due_us.push(s.from_due_us);
    }
    send_us.sort_unstable();
    due_us.sort_unstable();

    let d = snap_b.minus(&snap_a);
    let wall_s = d.wall_us as f64 / 1e6;
    let msgs = d.leader_msgs.max(1);
    let json = format!(
        concat!(
            "{{\"transport\":\"{}\",\"tls\":{},\"mac\":{},\"rate_msgs\":{},\"batch\":{},\"payload\":{},",
            "\"secs\":{},\"conns_cfg\":{},\"host\":\"{}\",\"threads\":{},\"pace\":\"{}\",",
            "\"cmds_ok\":{},\"cmds_bad\":{},\"achieved_msgs_s\":{:.0},\"achieved_cmds_s\":{:.0},",
            "\"rt_send_p50_us\":{},\"rt_send_p90_us\":{},\"rt_send_p99_us\":{},\"rt_send_p999_us\":{},\"rt_send_max_us\":{},",
            "\"rt_due_p50_us\":{},\"rt_due_p99_us\":{},\"pace_lag_max_us\":{},\"handshake_us\":{},",
            "\"recv_cpu_us\":{},\"recv_cpu_cores\":{:.3},\"recv_cpu_us_per_msg\":{:.3},\"recv_rss_mb\":{:.1},",
            "\"leader_cpu_us\":{},\"leader_cpu_cores\":{:.3},\"leader_cpu_us_per_msg\":{:.3},\"leader_rss_mb\":{:.1},",
            "\"wire_bytes_per_msg\":{:.1},\"wire_up_bytes_per_msg\":{:.1},\"wire_down_bytes_per_msg\":{:.1},",
            "\"leader_conns_open\":{},\"leader_conns_opened\":{},\"recv_conns_opened\":{},\"payload_bytes_per_msg\":{}}}"
        ),
        t.name(),
        tls_on,
        mac,
        rate_msgs,
        batch,
        payload,
        secs,
        if transport == "tcp" { conns } else { pool },
        hostname(),
        a.n("threads", 4),
        if spin_pace { "spin" } else { "timer" },
        send_us.len(),
        bad,
        d.leader_msgs as f64 / wall_s,
        d.leader_cmds as f64 / wall_s,
        pct(&send_us, 0.50),
        pct(&send_us, 0.90),
        pct(&send_us, 0.99),
        pct(&send_us, 0.999),
        send_us.last().copied().unwrap_or(0),
        pct(&due_us, 0.50),
        pct(&due_us, 0.99),
        max_lag_us.load(Ordering::Relaxed),
        t.handshake_us(),
        d.recv_cpu_us,
        d.recv_cpu_us as f64 / d.wall_us as f64,
        d.recv_cpu_us as f64 / msgs as f64,
        snap_b.recv_rss as f64 / 1e6,
        d.leader_cpu_us,
        d.leader_cpu_us as f64 / d.wall_us as f64,
        d.leader_cpu_us as f64 / msgs as f64,
        snap_b.leader_rss as f64 / 1e6,
        (d.recv_tx + d.recv_rx) as f64 / msgs as f64,
        d.recv_tx as f64 / msgs as f64,
        d.recv_rx as f64 / msgs as f64,
        snap_b.leader_open,
        snap_b.leader_conns,
        snap_b.recv_conns,
        payload,
    );
    println!("RESULT {json}");
    eprintln!(
        "[receiver] {} tls={} mac={} rate={} -> p50 {} us  p99 {} us  leader {:.2} cores  recv {:.2} cores  {:.0} B/msg  conns {}",
        t.name(),
        tls_on,
        mac,
        rate_msgs,
        pct(&send_us, 0.50),
        pct(&send_us, 0.99),
        d.leader_cpu_us as f64 / d.wall_us as f64,
        d.recv_cpu_us as f64 / d.wall_us as f64,
        (d.recv_tx + d.recv_rx) as f64 / msgs as f64,
        snap_b.leader_open,
    );
}

/// D12 fail-closed (pgless U19): no default secret, and a short one is
/// refused. Added 2026-09-18 — `main.rs` used to default `--secret` to the
/// literal `"s4-spike-secret"` while MEMO.md claimed the spike implemented
/// fail-closed behaviour.
fn require_secret(a: &Args) -> Vec<u8> {
    let s = a.s("secret", "").into_bytes();
    if let Err(e) = auth::check_secret(&s) {
        eprintln!(
            "[s4] refusing to start: {e} (pass --secret, >= {} bytes)",
            auth::MIN_SECRET_LEN
        );
        std::process::exit(2);
    }
    s
}

// -------------------------------------------------------------------- bench
//
// Micro-benchmark of the per-command crypto the matrix could only measure
// end to end. Added for the 2026-09-18 refutation round to answer two
// questions with numbers: how much of the HTTP+MAC row is hex formatting
// rather than hashing, and what the revised (sequenced) frame MAC costs.

fn bench(a: Args) {
    let n = a.n("iters", 20_000) as usize;
    let rounds = a.n("rounds", 5) as usize;
    let body = vec![0x5au8; a.n("bytes", 2801) as usize];
    let secret = b"a-long-enough-spike-secret".to_vec();
    let key = [7u8; 32];

    // min of `rounds` means: a CPU that ramps or a neighbour that wakes can
    // only make an op look slower, never faster.
    let mut t = |label: &str, f: &mut dyn FnMut()| -> f64 {
        for _ in 0..n / 10 {
            f();
        }
        let mut best = f64::MAX;
        for _ in 0..rounds {
            let t0 = Instant::now();
            for _ in 0..n {
                f();
            }
            let ns = t0.elapsed().as_nanos() as f64 / n as f64;
            if ns < best {
                best = ns;
            }
        }
        println!("{label:40} {best:8.1} ns/op");
        best
    };

    println!(
        "# s4 bench  host={}  iters={n} x {rounds} rounds (min)  body={} B",
        hostname(),
        body.len()
    );
    let tag = auth::request_mac_bin(&secret, &body);
    let hex = auth::request_mac(&secret, &body);
    let hex2 = hex.clone();

    let hmac = t("hmac-sha256 over the body", &mut || {
        std::hint::black_box(auth::request_mac_bin(&secret, std::hint::black_box(&body)));
    });
    let hexing = t("hex of a 16 B tag (16x format!)", &mut || {
        std::hint::black_box(hexify(std::hint::black_box(&tag)));
    });
    let eq = t("String == on two 32-char hex tags", &mut || {
        std::hint::black_box(std::hint::black_box(&hex) == std::hint::black_box(&hex2));
    });
    let ct = t("hmac + constant-time verify", &mut || {
        std::hint::black_box(auth::verify_request_mac(
            &secret,
            std::hint::black_box(&body),
            &tag,
        ));
    });
    let fmac = t("frame encode + sequenced MAC", &mut || {
        let mut c = frame::MacCtx::new(&key, frame::DIR_C2S);
        let mut out = Vec::with_capacity(body.len() + 64);
        frame::encode_into(
            &mut out,
            frame::T_CMD,
            std::hint::black_box(&body),
            Some(&mut c),
        );
        std::hint::black_box(out);
    });
    let fplain = t("frame encode, no MAC", &mut || {
        let mut out = Vec::with_capacity(body.len() + 64);
        frame::encode_into(&mut out, frame::T_CMD, std::hint::black_box(&body), None);
        std::hint::black_box(out);
    });
    println!("# hmac throughput: {:.3} GB/s", body.len() as f64 / hmac);
    println!(
        "# the HTTP leader path per request = hmac {:.0} + hex {:.0} + String== {:.0} = {:.0} ns; \
constant-time binary = {:.0} ns; hex share {:.1} %",
        hmac,
        hexing,
        eq,
        hmac + hexing + eq,
        ct,
        100.0 * (hexing + eq) / (hmac + hexing + eq)
    );
    println!(
        "# sequenced frame MAC over {} B: {:.0} ns (encode alone {:.0} ns)",
        body.len(),
        fmac - fplain,
        fplain
    );
}

fn hexify(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
}

fn pct(v: &[u32], p: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i.min(v.len() - 1)]
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

struct Snapshot {
    at: Instant,
    recv_user: u64,
    recv_sys: u64,
    recv_rss: u64,
    recv_rx: u64,
    recv_tx: u64,
    recv_conns: u64,
    recv_open: u64,
    leader: wire::Stats,
    leader_rss: u64,
    leader_conns: u64,
    leader_open: u64,
}

struct Delta {
    wall_us: u64,
    recv_cpu_us: u64,
    leader_cpu_us: u64,
    recv_rx: u64,
    recv_tx: u64,
    leader_cmds: u64,
    leader_msgs: u64,
}

impl Snapshot {
    fn minus(&self, a: &Snapshot) -> Delta {
        Delta {
            wall_us: self.at.duration_since(a.at).as_micros() as u64,
            recv_cpu_us: (self.recv_user + self.recv_sys).saturating_sub(a.recv_user + a.recv_sys),
            leader_cpu_us: (self.leader.user_us + self.leader.sys_us)
                .saturating_sub(a.leader.user_us + a.leader.sys_us),
            recv_rx: self.recv_rx.saturating_sub(a.recv_rx),
            recv_tx: self.recv_tx.saturating_sub(a.recv_tx),
            leader_cmds: self.leader.cmds.saturating_sub(a.leader.cmds),
            leader_msgs: self.leader.msgs.saturating_sub(a.leader.msgs),
        }
    }
}

async fn snapshot(t: &Arc<dyn Transport>, c: &Arc<Counters>) -> Snapshot {
    let (u, s, rss) = net::rusage();
    let (rx, tx, conns, open, _, _) = c.snapshot();
    let leader = t.clone().stats([0xffu8; 16]).await.unwrap_or_default();
    Snapshot {
        at: Instant::now(),
        recv_user: u,
        recv_sys: s,
        recv_rss: rss,
        recv_rx: rx,
        recv_tx: tx,
        recv_conns: conns,
        recv_open: open,
        leader_rss: leader.rss_bytes,
        leader_conns: leader.conns,
        leader_open: leader.open,
        leader,
    }
}
