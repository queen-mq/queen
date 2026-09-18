//! The seven scenarios of WP-0.5.
//!
//! Every scenario boots real processes, prints what it measured, and leaves its
//! node logs under the run directory.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;

use crate::client::find_leader;
use crate::client::Conn;
use crate::cluster::Cluster;
use crate::cluster::NodeCfg;
use crate::types::NodeId;
use crate::util::stats;
use crate::wire::Req;
use crate::wire::WriteErr;

#[derive(Clone, Debug)]
pub struct Common {
    pub root: PathBuf,
    pub base_port: u16,
    pub fsync: String,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub lin_batch_ms: u64,
    pub durable_ms: u64,
    pub file_bytes: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub committed: String,
    pub wait_recovery: bool,
}

impl Common {
    fn cfg(&self, ordinal: u64, generation: u64) -> NodeCfg {
        NodeCfg {
            // §12.6: node_id = ordinal * 1000 + generation.
            id: ordinal * 1000 + generation,
            port: self.base_port + ordinal as u16,
            fsync: self.fsync.clone(),
            heartbeat_ms: self.heartbeat_ms,
            election_min_ms: self.election_min_ms,
            election_max_ms: self.election_max_ms,
            pre_vote: true,
            leader_restore: false,
            max_in_snapshot_log_to_keep: self.max_in_snapshot_log_to_keep,
            file_bytes: self.file_bytes,
            lin_batch_ms: self.lin_batch_ms,
            durable_ms: self.durable_ms,
            committed: self.committed.clone(),
            wait_recovery: self.wait_recovery,
        }
    }
}

/// Boot `n` voters and initialize the cluster.
async fn boot(common: &Common, n: u64) -> anyhow::Result<Cluster> {
    let _ = std::fs::remove_dir_all(&common.root);
    let mut cluster = Cluster::new(common.root.clone())?;
    for ordinal in 1..=n {
        cluster.start(common.cfg(ordinal, 1)).await?;
    }
    let members: Vec<(NodeId, String)> = cluster.addrs();
    let first = members[0].1.clone();
    let mut c = Conn::connect(&first).await?;
    c.unit(Req::Init {
        members: members.clone(),
    })
    .await?
    .map_err(anyhow::Error::msg)
    .context("initialize")?;
    let leader = find_leader(&members, Duration::from_secs(20))
        .await
        .context("no leader after init")?;
    println!("cluster up: {:?}, leader {}", members, leader.0);
    Ok(cluster)
}

/// Write, retrying what the plan calls retryable: `ForwardToLeader` (including
/// the `LeaseExpired` reason openraft returns when the leader's quorum-ack
/// lease is stale) and a broken connection. Re-resolves the leader each time.
async fn write_retry(
    addrs: &[(NodeId, String)],
    conn: &mut Conn,
    id: u64,
    bytes: Vec<u8>,
    within: Duration,
) -> anyhow::Result<(u64, u64)> {
    let deadline = Instant::now() + within;
    let mut last: String;
    loop {
        match conn.write(id, bytes.clone()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) => last = format!("{e:?}"),
            Err(e) => last = e.to_string(),
        }
        if Instant::now() > deadline {
            anyhow::bail!("write {id} never committed: {last}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Some((_, addr)) = find_leader(addrs, Duration::from_secs(5)).await {
            if let Ok(c) = Conn::connect(&addr).await {
                *conn = c;
            }
        }
    }
}

fn payload(bytes: usize, id: u64) -> Vec<u8> {
    let mut v = vec![0u8; bytes];
    v[..8.min(bytes)].copy_from_slice(&id.to_le_bytes()[..8.min(bytes)]);
    v
}

/// Scenario 1: commit latency with 64 KiB entries, `writers` proposals in
/// flight (D4 says one; more shows what pipelining would buy).
pub async fn s1_latency(
    common: &Common,
    rates: Vec<u64>,
    secs: u64,
    entry_bytes: usize,
    writers: usize,
) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!(
        "# scenario 1: commit latency, {entry_bytes} B entries, {writers} in flight, leader {leader_id}, fsync={}",
        common.fsync
    );

    {
        // Warm up: the first write of a term carries the leader's blank entry.
        let mut conn = Conn::connect(&leader_addr).await?;
        for i in 0..20u64 {
            let _ = conn.write(i, payload(entry_bytes, i)).await?;
        }
    }

    for rate in rates {
        let per_writer = (rate as f64 / writers as f64).max(1.0);
        let interval = Duration::from_secs_f64(1.0 / per_writer);
        let per_writer_total = (rate * secs) / writers as u64;
        // Never let a rate that the cluster cannot take run longer than twice
        // its nominal duration.
        let hard_stop = Instant::now() + Duration::from_secs(secs * 2 + 5);
        let start = Instant::now();
        let mut tasks = Vec::new();
        for w in 0..writers {
            let addr = leader_addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut conn = Conn::connect(&addr).await?;
                let mut service = Vec::new();
                let mut response = Vec::new();
                let mut errors = 0u64;
                for i in 0..per_writer_total {
                    let scheduled = start + interval.mul_f64(i as f64);
                    let now = Instant::now();
                    if now > hard_stop {
                        break;
                    }
                    if scheduled > now {
                        tokio::time::sleep(scheduled - now).await;
                    }
                    let t0 = Instant::now();
                    let id = 1_000_000 + (w as u64) * 10_000_000 + i;
                    let res = conn.write(id, payload(entry_bytes, id)).await?;
                    let done = Instant::now();
                    match res {
                        Ok(_) => {
                            service.push((done - t0).as_secs_f64() * 1000.0);
                            response.push((done - scheduled).as_secs_f64() * 1000.0);
                        }
                        Err(_) => errors += 1,
                    }
                }
                Ok::<(Vec<f64>, Vec<f64>, u64), anyhow::Error>((service, response, errors))
            }));
        }
        let mut service = Vec::new();
        let mut response = Vec::new();
        let mut errors = 0u64;
        for t in tasks {
            let (s, r, e) = t.await??;
            service.extend(s);
            response.extend(r);
            errors += e;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let done = service.len();
        let s = stats(service);
        let r = stats(response);
        println!(
            "rate={rate}/s offered, achieved={:.0}/s, committed={done}, errors={errors}\n  service  {s}\n  response {r}",
            done as f64 / elapsed
        );
    }

    cluster.shutdown_all().await;
    Ok(())
}

/// Offer `1/interval` writes per second for `secs` seconds on one connection,
/// recording every acknowledged write and its service time in ms.
async fn load_for(
    conn: &mut Conn,
    acked: &mut BTreeSet<u64>,
    next_id: &mut u64,
    entry_bytes: usize,
    interval: Duration,
    secs: f64,
) -> Vec<f64> {
    let start = Instant::now();
    let mut lat = Vec::new();
    while start.elapsed().as_secs_f64() < secs {
        let t0 = Instant::now();
        if let Ok(Ok(_)) = conn.write(*next_id, payload(entry_bytes, *next_id)).await {
            acked.insert(*next_id);
            lat.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        *next_id += 1;
        tokio::time::sleep(interval).await;
    }
    lat
}

/// Scenario 2: kill -9 the leader under load. With `stop_follower_secs > 0`, a
/// follower is SIGSTOPped for that long in the middle of the pre-kill load
/// first: the quorum must survive on the other two, and the frozen node must
/// catch up when it is resumed.
pub async fn s2_kill_leader(
    common: &Common,
    rate: u64,
    entry_bytes: usize,
    secs_before: u64,
    secs_after: u64,
    stop_follower_secs: u64,
) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!("# scenario 2: kill -9 leader {leader_id} under {rate}/s of {entry_bytes} B writes");

    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let mut conn = Conn::connect(&leader_addr).await?;
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut next_id = 1u64;

    // Load before the kill, optionally with a frozen follower in the middle.
    if stop_follower_secs == 0 {
        let a = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            secs_before as f64,
        )
        .await;
        println!("steady state before the kill: {}", stats(a));
    } else {
        let victim = addrs
            .iter()
            .map(|(id, _)| *id)
            .find(|id| *id != leader_id)
            .context("no follower")?;
        let half = (secs_before as f64 / 2.0).max(1.0);
        let a = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            half,
        )
        .await;
        println!("steady state before the freeze: {}", stats(a));

        cluster.signal(victim, "STOP")?;
        println!("SIGSTOPped follower {victim} for {stop_follower_secs} s");
        let f = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            stop_follower_secs as f64,
        )
        .await;
        println!("while follower {victim} is frozen: {}", stats(f));

        let leader_applied = conn.status().await?.last_applied.unwrap_or(0);
        cluster.signal(victim, "CONT")?;
        println!("SIGCONT {victim}; leader is at index {leader_applied}");
        let t_cont = Instant::now();
        let victim_addr = cluster.addr_of(victim).context("no addr")?;
        loop {
            if t_cont.elapsed() > Duration::from_secs(60) {
                println!("  follower {victim} did NOT catch up within 60 s");
                break;
            }
            let probe = tokio::time::timeout(Duration::from_millis(500), async {
                let mut c = Conn::connect(&victim_addr).await?;
                c.status().await
            })
            .await;
            if let Ok(Ok(s)) = probe {
                if s.last_applied.unwrap_or(0) >= leader_applied {
                    println!(
                        "  follower {victim} caught up to {:?} {:.0} ms after SIGCONT",
                        s.last_applied,
                        t_cont.elapsed().as_secs_f64() * 1000.0
                    );
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let b = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            half,
        )
        .await;
        println!("after the resume: {}", stats(b));
    }
    println!("before kill: {} acknowledged writes", acked.len());

    let t_kill = Instant::now();
    cluster.kill9(leader_id)?;
    println!("killed {leader_id} at t=0");

    // Time to a new leader, seen from a survivor.
    let survivors: Vec<(NodeId, String)> = addrs
        .iter()
        .filter(|(id, _)| *id != leader_id)
        .cloned()
        .collect();
    let mut new_leader = None;
    while t_kill.elapsed() < Duration::from_secs(30) {
        if let Some((id, addr)) = find_leader(&survivors, Duration::from_millis(50)).await {
            if id != leader_id {
                new_leader = Some((id, addr, t_kill.elapsed()));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (new_id, new_addr, t_leader) = new_leader.context("no new leader within 30 s")?;
    println!(
        "new leader {new_id} after {:.0} ms",
        t_leader.as_secs_f64() * 1000.0
    );

    // Time to the first committed write after the kill.
    let mut conn = Conn::connect(&new_addr).await?;
    let mut first_commit = None;
    while t_kill.elapsed() < Duration::from_secs(30) {
        match conn.write(next_id, payload(entry_bytes, next_id)).await {
            Ok(Ok(_)) => {
                acked.insert(next_id);
                first_commit = Some(t_kill.elapsed());
                next_id += 1;
                break;
            }
            Ok(Err(WriteErr::NotLeader { .. })) | Err(_) => {
                next_id += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
                if let Ok(c) = Conn::connect(&new_addr).await {
                    conn = c;
                }
            }
            Ok(Err(e)) => {
                println!("  write error after kill: {e:?}");
                next_id += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let first_commit = first_commit.context("no write committed after the kill")?;
    println!(
        "first committed write after {:.0} ms",
        first_commit.as_secs_f64() * 1000.0
    );

    // Keep writing for a while.
    let after = Instant::now();
    while after.elapsed() < Duration::from_secs(secs_after) {
        if let Ok(Ok(_)) = conn.write(next_id, payload(entry_bytes, next_id)).await {
            acked.insert(next_id);
        }
        next_id += 1;
        tokio::time::sleep(interval).await;
    }

    // No acknowledged write may be missing on any surviving node (I4).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut worst = 0usize;
    for (id, addr) in &survivors {
        let mut c = Conn::connect(addr).await?;
        let applied: BTreeSet<u64> = c.applied_ids().await?.into_iter().collect();
        let missing: Vec<u64> = acked.difference(&applied).cloned().collect();
        worst = worst.max(missing.len());
        println!(
            "node {id}: applied {} ids, acknowledged {}, missing {} {:?}",
            applied.len(),
            acked.len(),
            missing.len(),
            missing.iter().take(5).collect::<Vec<_>>()
        );
    }
    println!("VERDICT acknowledged-writes-missing={worst}");

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 3: `trigger().transfer_leader`, and what a transfer to an
/// unreachable target does (GH#2088).
pub async fn s3_transfer(common: &Common, rounds: usize) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!("# scenario 3: leadership transfer, leader {leader_id}, {rounds} healthy round(s)");

    let mut conn = Conn::connect(&leader_addr).await?;
    for i in 0..50u64 {
        let _ = conn.write(i, payload(1024, i)).await?;
    }

    // (a) transfer to a healthy follower, `rounds` times, moving on every time.
    let mut role_ms = Vec::new();
    let mut write_ms = Vec::new();
    for round in 1..=rounds {
        let (from, from_addr) = find_leader(&addrs, Duration::from_secs(10))
            .await
            .context("no leader")?;
        let target = addrs
            .iter()
            .map(|(id, _)| *id)
            .find(|id| *id != from)
            .unwrap();
        let mut conn = Conn::connect(&from_addr).await?;
        let t0 = Instant::now();
        conn.unit(Req::TriggerTransfer { to: target })
            .await?
            .map_err(anyhow::Error::msg)?;
        let mut moved_at = None;
        while t0.elapsed() < Duration::from_secs(20) {
            if let Some((id, _)) = find_leader(&addrs, Duration::from_millis(50)).await {
                if id == target {
                    moved_at = Some(t0.elapsed());
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        match moved_at {
            Some(d) => {
                role_ms.push(d.as_secs_f64() * 1000.0);
                println!(
                    "round {round}: transfer {from} -> {target}: OK, {target} reports is_leader after {:.0} ms",
                    d.as_secs_f64() * 1000.0
                )
            }
            None => println!(
                "round {round}: transfer {from} -> {target}: FAILED, no leadership change within 20 s"
            ),
        }
        // How long until the new leader actually takes a write (it must commit
        // the blank entry of its term first, I13).
        let new_addr = cluster.addr_of(target).unwrap();
        let mut conn = Conn::connect(&new_addr).await?;
        let t1 = Instant::now();
        let mut write_ok = None;
        let mut last_err = None;
        while t1.elapsed() < Duration::from_secs(10) {
            match conn
                .write(999_000 + round as u64, payload(1024, 999_000))
                .await?
            {
                Ok(_) => {
                    write_ok = Some(t0.elapsed());
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
        match write_ok {
            Some(d) => {
                write_ms.push(d.as_secs_f64() * 1000.0);
                println!(
                    "round {round}: first write committed on the new leader {:.0} ms after the trigger (last error before it: {:?})",
                    d.as_secs_f64() * 1000.0,
                    last_err
                )
            }
            None => println!("round {round}: the new leader never took a write: {last_err:?}"),
        }
    }
    println!("healthy transfers: role change {}", stats(role_ms));
    println!("healthy transfers: first write {}", stats(write_ms));

    // (b) transfer to a target that is down.
    let current = find_leader(&addrs, Duration::from_secs(5))
        .await
        .context("no leader")?;
    let dead = addrs
        .iter()
        .map(|(id, _)| *id)
        .find(|id| *id != current.0)
        .context("no other node")?;
    cluster.kill9(dead)?;
    println!(
        "killed {dead}; asking {} to transfer leadership to it",
        current.0
    );
    let alive: Vec<(NodeId, String)> = addrs
        .iter()
        .filter(|(id, _)| *id != dead)
        .cloned()
        .collect();
    let mut conn = Conn::connect(&current.1).await?;
    let t0 = Instant::now();
    let res = conn.unit(Req::TriggerTransfer { to: dead }).await?;
    println!("trigger returned {res:?}");

    let mut leaderless_from = None;
    let mut leaderless_until = None;
    let mut new_leader = None;
    let mut write_ok_at = None;
    while t0.elapsed() < Duration::from_secs(20) {
        match find_leader(&alive, Duration::from_millis(30)).await {
            None => {
                leaderless_from.get_or_insert(t0.elapsed());
            }
            Some((id, addr)) => {
                if leaderless_from.is_some() && leaderless_until.is_none() {
                    leaderless_until = Some(t0.elapsed());
                }
                if new_leader.is_none() {
                    new_leader = Some((id, t0.elapsed()));
                }
                if write_ok_at.is_none() {
                    if let Ok(mut c) = Conn::connect(&addr).await {
                        if let Ok(Ok(_)) = c.write(999_100, payload(64, 1)).await {
                            write_ok_at = Some((id, t0.elapsed()));
                            break;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!(
        "failed transfer: leaderless from {:?} ms to {:?} ms; a node reported is_leader again at {:?}; first committed write by (node, ms) {:?}",
        leaderless_from.map(|d| d.as_millis()),
        leaderless_until.map(|d| d.as_millis()),
        new_leader.map(|(id, d)| (id, d.as_millis())),
        write_ok_at.map(|(id, d)| (id, d.as_millis()))
    );

    cluster.shutdown_all().await;
    Ok(())
}

/// One rate point of scenario 4: offer `rate` linearizable reads per second for
/// `secs` seconds from a pool of connections, open loop. Returns achieved rate,
/// service time (what the read cost) and response time (service plus the
/// backlog the offered rate could not absorb).
async fn lin_rate_point(
    leader_addr: &str,
    mode: u8,
    rate: u64,
    secs: u64,
    workers: usize,
) -> anyhow::Result<(f64, crate::util::Stats, crate::util::Stats)> {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let total = rate * secs;
    let counter = Arc::new(AtomicU64::new(0));
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let start = Instant::now();
    let hard_stop = start + Duration::from_secs(secs * 2 + 5);
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let addr = leader_addr.to_string();
        let counter = counter.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = Conn::connect(&addr).await?;
            let mut service = Vec::new();
            let mut response = Vec::new();
            loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let scheduled = start + interval.mul_f64(i as f64);
                let now = Instant::now();
                if now > hard_stop {
                    break;
                }
                if scheduled > now {
                    tokio::time::sleep(scheduled - now).await;
                }
                let t0 = Instant::now();
                let r = conn.lin_read(mode).await?;
                let done = Instant::now();
                if r.is_ok() {
                    service.push((done - t0).as_secs_f64() * 1000.0);
                    response.push((done - scheduled).as_secs_f64() * 1000.0);
                }
            }
            Ok::<(Vec<f64>, Vec<f64>), anyhow::Error>((service, response))
        }));
    }
    let mut service = Vec::new();
    let mut response = Vec::new();
    for t in tasks {
        let (s, r) = t.await??;
        service.extend(s);
        response.extend(r);
    }
    let elapsed = start.elapsed().as_secs_f64();
    let achieved = service.len() as f64 / elapsed;
    Ok((achieved, stats(service), stats(response)))
}

/// Scenario 4: `ensure_linearizable`, one call per read vs a 2 ms batch.
pub async fn s4_linearizable(
    common: &Common,
    concurrency: Vec<usize>,
    rates: Vec<u64>,
    secs: u64,
) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!(
        "# scenario 4: linearizable reads on leader {leader_id} (batch window {} ms)",
        common.lin_batch_ms
    );

    {
        let mut conn = Conn::connect(&leader_addr).await?;
        for i in 0..50u64 {
            let _ = conn.write(i, payload(1024, i)).await?;
        }
    }

    // Rate-driven points: what a given read rate costs, single vs batched.
    for &rate in &rates {
        for mode in [0u8, 1, 2] {
            let (achieved, service, response) =
                lin_rate_point(&leader_addr, mode, rate, secs, 128).await?;
            println!(
                "rate={rate}/s offered mode={} achieved={achieved:.0}/s\n  service  {service}\n  response {response}",
                match mode {
                    0 => "single  ",
                    1 => "window  ",
                    _ => "coalesce",
                }
            );
        }
    }

    // The concurrency sweep is the closed-loop view; with rates given, the
    // rate points above are the measurement and this is skipped.
    let concurrency = if rates.is_empty() {
        concurrency
    } else {
        vec![]
    };
    for mode in [0u8, 1, 2] {
        for &c in &concurrency {
            let mut tasks = Vec::new();
            let stop = Instant::now() + Duration::from_secs(secs);
            for _ in 0..c {
                let addr = leader_addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut conn = Conn::connect(&addr).await?;
                    let mut lat = Vec::new();
                    while Instant::now() < stop {
                        let t0 = Instant::now();
                        let r = conn.lin_read(mode).await?;
                        if r.is_ok() {
                            lat.push(t0.elapsed().as_secs_f64() * 1000.0);
                        }
                    }
                    Ok::<Vec<f64>, anyhow::Error>(lat)
                }));
            }
            let mut all = Vec::new();
            for t in tasks {
                all.extend(t.await??);
            }
            let n = all.len();
            let s = stats(all);
            println!(
                "mode={} concurrency={c} reads/s={:.0}\n  {s}",
                match mode {
                    0 => "single  ",
                    1 => "window  ",
                    _ => "coalesce",
                },
                n as f64 / secs as f64
            );
        }
    }

    // Proof that the barrier really asks a quorum: with both followers dead a
    // linearizable read must not answer, while a local (stale) read still does.
    for (id, _) in addrs.iter().filter(|(id, _)| *id != leader_id) {
        cluster.kill9(*id)?;
    }
    let mut conn = Conn::connect(&leader_addr).await?;
    let t0 = Instant::now();
    let res = tokio::time::timeout(Duration::from_secs(5), conn.lin_read(0)).await;
    match res {
        Err(_) => println!("with both followers dead, a linearizable read did not answer within 5 s (ReadIndex waits for a quorum)"),
        Ok(Ok(Ok(v))) => println!("with both followers dead, a linearizable read ANSWERED {v:?} after {:.0} ms - the barrier did not ask a quorum", t0.elapsed().as_secs_f64() * 1000.0),
        Ok(Ok(Err(e))) => println!("with both followers dead, a linearizable read failed after {:.0} ms: {e}", t0.elapsed().as_secs_f64() * 1000.0),
        Ok(Err(e)) => println!("with both followers dead, the read connection broke: {e}"),
    }
    let mut conn = Conn::connect(&leader_addr).await?;
    match tokio::time::timeout(Duration::from_secs(2), conn.status()).await {
        Ok(Ok(s)) => println!(
            "the local (stale) read still answers: applied={:?}",
            s.last_applied
        ),
        other => println!("the local read did not answer: {other:?}"),
    }

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 5: a manifest snapshot streamed to an empty learner, and what a
/// killed transfer costs the second time (resume per file).
pub async fn s5_snapshot(
    common: &Common,
    total_bytes: u64,
    entry_bytes: usize,
    kill_after_ms: u64,
) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (_leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!(
        "# scenario 5: {} MiB snapshot to a wiped learner",
        total_bytes / (1024 * 1024)
    );

    let mut conn = Conn::connect(&leader_addr).await?;
    let n = total_bytes / entry_bytes as u64;
    let t_fill = Instant::now();
    for i in 0..n {
        write_retry(
            &addrs,
            &mut conn,
            i,
            payload(entry_bytes, i),
            Duration::from_secs(30),
        )
        .await?;
    }
    println!(
        "filled {} entries of {} B in {:.1} s ({:.1} MiB/s)",
        n,
        entry_bytes,
        t_fill.elapsed().as_secs_f64(),
        total_bytes as f64 / (1024.0 * 1024.0) / t_fill.elapsed().as_secs_f64()
    );

    let t_snap = Instant::now();
    conn.unit(Req::TriggerSnapshot)
        .await?
        .map_err(anyhow::Error::msg)?;
    let mut snapshot_index = None;
    while t_snap.elapsed() < Duration::from_secs(600) {
        let s = conn.status().await?;
        if s.snapshot_index.is_some() {
            snapshot_index = s.snapshot_index;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!(
        "snapshot built at index {:?} in {:.1} s (build cost is I8's declared exception)",
        snapshot_index,
        t_snap.elapsed().as_secs_f64()
    );

    // Purge the log so a new member can only be caught up by the snapshot.
    let last = conn.status().await?.last_applied.unwrap_or(0);
    let _ = conn.unit(Req::PurgeLog { upto: last }).await?;
    println!("purged log up to {last}: {:?}", conn.status().await?.purged);

    // A fresh voter-to-be joins as a learner with an empty directory (§12.6).
    let learner = common.cfg(4, 1);
    let learner_id = learner.id;
    cluster.start(learner).await?;
    let t_xfer = Instant::now();
    conn.unit(Req::AddLearner {
        id: learner_id,
        addr: cluster.addr_of(learner_id).unwrap(),
    })
    .await?
    .map_err(anyhow::Error::msg)?;

    let mut killed = false;
    let mut resumed_at = None;
    loop {
        if kill_after_ms > 0 && !killed && t_xfer.elapsed() > Duration::from_millis(kill_after_ms) {
            cluster.kill9(learner_id)?;
            killed = true;
            println!(
                "killed the learner {:.1} s into the transfer",
                t_xfer.elapsed().as_secs_f64()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
            cluster.restart(learner_id).await?;
            resumed_at = Some(t_xfer.elapsed());
            println!("learner restarted; the leader retries and the receiver keeps whole files");
        }
        let addr = cluster.addr_of(learner_id).unwrap();
        if let Ok(mut lc) = Conn::connect(&addr).await {
            if let Ok(s) = lc.status().await {
                if s.last_applied.unwrap_or(0) >= last {
                    let secs = t_xfer.elapsed().as_secs_f64();
                    println!(
                        "learner caught up at index {:?} in {:.1} s ({:.1} MiB/s of manifest)",
                        s.last_applied,
                        secs,
                        total_bytes as f64 / (1024.0 * 1024.0) / secs
                    );
                    break;
                }
            }
        }
        if t_xfer.elapsed() > Duration::from_secs(900) {
            println!("learner did not catch up within 900 s");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if let Some(d) = resumed_at {
        println!(
            "transfer was interrupted at {:.1} s; see n{learner_id}.log for 'N of M files needed'",
            d.as_secs_f64()
        );
    }

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 6: the wiped voter, done wrong and then right (D21, §12.6).
pub async fn s6_wiped_voter(common: &Common) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    let victim = addrs
        .iter()
        .map(|(id, _)| *id)
        .find(|id| *id != leader_id)
        .unwrap();
    println!("# scenario 6: wiped voter {victim} (leader {leader_id})");

    let mut conn = Conn::connect(&leader_addr).await?;
    for i in 0..200u64 {
        write_retry(
            &addrs,
            &mut conn,
            i,
            payload(4096, i),
            Duration::from_secs(20),
        )
        .await?;
    }
    let before = conn.status().await?;
    println!("before: {}", crate::server::describe(&before));

    // ---- the WRONG way: same id, empty disk ----
    cluster.stop(victim).await?;
    cluster.wipe(victim)?;
    cluster.restart(victim).await?;
    println!("restarted {victim} with an empty directory and the SAME id");
    tokio::time::sleep(Duration::from_secs(3)).await;
    for (id, addr) in &addrs {
        if let Ok(mut c) = Conn::connect(addr).await {
            match c.status().await {
                Ok(s) => println!("  wrong-way state: {}", crate::server::describe(&s)),
                Err(e) => println!("  node {id} does not answer: {e}"),
            }
        } else {
            println!("  node {id} is not listening (it may have panicked; see n{id}.log)");
        }
    }
    let l = find_leader(&addrs, Duration::from_secs(10)).await;
    println!("  leader after the wrong way: {l:?}");
    if let Some((_, a)) = &l {
        let mut c = Conn::connect(a).await?;
        let w = c.write(500_000, payload(4096, 500_000)).await?;
        println!("  write after the wrong way: {:?}", w.map(|(i, _)| i));
        let s = c.status().await?;
        println!("  leader sees: {}", crate::server::describe(&s));
    }

    // ---- the RIGHT way: remove, wipe, add as a learner with a new id, promote ----
    let leader = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    let mut conn = Conn::connect(&leader.1).await?;
    let keep: Vec<NodeId> = addrs
        .iter()
        .map(|(id, _)| *id)
        .filter(|id| *id != victim)
        .collect();
    println!("removing {victim} from the membership, keeping {keep:?}");
    conn.unit(Req::ChangeMembership {
        voters: keep.clone(),
    })
    .await?
    .map_err(anyhow::Error::msg)
    .context("change_membership remove")?;
    cluster.stop(victim).await?;
    cluster.wipe(victim)?;

    // A replaced disk gets the next generation (§12.6).
    let ordinal = victim / 1000;
    let replacement = common.cfg(ordinal, 2);
    let replacement_id = replacement.id;
    let mut replacement = replacement;
    replacement.port = common.base_port + 10 + ordinal as u16;
    cluster.start(replacement).await?;
    println!("started the replacement as node {replacement_id}");
    conn.unit(Req::AddLearner {
        id: replacement_id,
        addr: cluster.addr_of(replacement_id).unwrap(),
    })
    .await?
    .map_err(anyhow::Error::msg)
    .context("add_learner")?;

    let target = conn.status().await?.last_applied.unwrap_or(0);
    let t0 = Instant::now();
    loop {
        let mut c = Conn::connect(&cluster.addr_of(replacement_id).unwrap()).await?;
        let s = c.status().await?;
        if s.last_applied.unwrap_or(0) >= target {
            println!(
                "learner caught up in {:.0} ms",
                t0.elapsed().as_secs_f64() * 1000.0
            );
            break;
        }
        if t0.elapsed() > Duration::from_secs(60) {
            anyhow::bail!("learner did not catch up");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut voters = keep.clone();
    voters.push(replacement_id);
    conn.unit(Req::ChangeMembership {
        voters: voters.clone(),
    })
    .await?
    .map_err(anyhow::Error::msg)
    .context("promote")?;
    println!("promoted; voters are now {voters:?}");
    let w = conn.write(600_000, payload(4096, 600_000)).await?;
    println!("write after the right way: {:?}", w.map(|(i, _)| i));
    let s = conn.status().await?;
    println!("after: {}", crate::server::describe(&s));

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 7: restart every node with `enable_leader_restore = false`.
pub async fn s7_restart(common: &Common, entries: u64, kill9: bool) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    println!(
        "# scenario 7: restart all with enable_leader_restore=false (leader {leader_id}), stop={}, durable_ms={}, save_committed={}",
        if kill9 { "kill -9" } else { "graceful" },
        common.durable_ms,
        common.committed
    );

    let mut conn = Conn::connect(&leader_addr).await?;
    let mut acked = BTreeSet::new();
    for i in 0..entries {
        write_retry(
            &addrs,
            &mut conn,
            i,
            payload(4096, i),
            Duration::from_secs(20),
        )
        .await?;
        acked.insert(i);
    }
    let mut before = Vec::new();
    for (id, addr) in &addrs {
        let mut c = Conn::connect(addr).await?;
        let s = c.status().await?;
        println!("before: {}", crate::server::describe(&s));
        before.push((*id, s.applied_count, s.digest, s.last_applied));
    }

    for (id, _) in &addrs {
        if kill9 {
            cluster.kill9(*id)?;
        } else {
            cluster.stop(*id).await?;
        }
    }
    println!(
        "all three {}",
        if kill9 { "killed with -9" } else { "stopped" }
    );
    let t0 = Instant::now();
    for (id, _) in &addrs {
        cluster.restart(*id).await?;
    }
    println!(
        "right after the restarts, the nodes BELIEVE: {:?}",
        crate::client::believed_leader(&addrs).await
    );
    let leader = find_leader(&addrs, Duration::from_secs(30))
        .await
        .context("no leader after restart")?;
    println!(
        "a node reports is_leader again {:.0} ms after the restarts began: {}",
        t0.elapsed().as_secs_f64() * 1000.0,
        leader.0
    );

    let mut ok = true;
    for (id, addr) in &addrs {
        let mut c = Conn::connect(addr).await?;
        let s = c.status().await?;
        println!("after:  {}", crate::server::describe(&s));
        println!(
            "  reopened with: log last={:?} purged={:?} committed={:?} vote_term={:?}; sm applied={:?} count={} digest={:x}{}",
            s.reopen_last_log,
            s.reopen_purged,
            s.reopen_committed,
            s.reopen_vote_term,
            s.reopen_sm_applied,
            s.reopen_sm_applied_count,
            s.reopen_sm_digest,
            match s.recovery_ms {
                Some(ms) => format!("; wait_for_recovery {ms} ms"),
                None => String::new(),
            }
        );
        let (_, count, digest, applied) = before.iter().find(|(i, _, _, _)| i == id).unwrap();
        // Applied state may legitimately move FORWARD across the restart: a
        // follower that had committed but not applied an entry re-applies it
        // from the saved committed log id. It must never move backwards.
        if s.applied_count < *count || s.last_applied < *applied {
            println!(
                "  WENT BACKWARDS on {id}: applied_count {} -> {}, last_applied {:?} -> {:?}",
                count, s.applied_count, applied, s.last_applied
            );
            ok = false;
        } else if s.applied_count != *count || s.digest != *digest {
            println!(
                "  advanced on {id}: applied_count {} -> {}, digest {:x} -> {:x} (re-applied from saved committed)",
                count, s.applied_count, digest, s.digest
            );
        }
        let applied_ids: BTreeSet<u64> = c.applied_ids().await?.into_iter().collect();
        let missing: Vec<u64> = acked.difference(&applied_ids).cloned().collect();
        if !missing.is_empty() {
            println!(
                "  node {id} is missing {} acknowledged writes",
                missing.len()
            );
            ok = false;
        }
    }
    let mut conn = Conn::connect(&leader.1).await?;
    let mut w = conn.write(900_000, payload(4096, 900_000)).await?;
    let t1 = Instant::now();
    while w.is_err() && t1.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        w = conn.write(900_000, payload(4096, 900_000)).await?;
    }
    println!("write after the restart: {:?}", w.map(|(i, _)| i));
    println!("VERDICT applied-state-preserved={ok}");

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 8: `kill -9` a FOLLOWER under load, restart it, and check what its
/// raft log reopened with.
///
/// This is WP-0.3's bar (the one that eliminated fjall for D9) applied to the
/// Raft log instead of the store. Per round it records, from the leader, the
/// index the leader had counted as matched for the victim — i.e. an index the
/// victim had acknowledged and that may already be part of a commit quorum —
/// then kills the victim with -9, restarts it, and reads the state the process
/// found ON DISK before openraft could replicate anything into it
/// (`StatusResp::reopen_*`). Two things must hold every time:
///
///   * `reopen_last_log >= matched`: the log never loses an entry the victim
///     had acknowledged (that is what "the Raft log is the write-ahead log"
///     of §11.3 means);
///   * `reopen_vote_term >= vote_term before the kill`: `save_vote` never goes
///     backwards, which is what stops a second vote in one term.
///
/// It does NOT model dropped fsyncs: an fsync this harness asks for is one the
/// kernel really performs. Silently dropped writes need dm-flakey (§13.6) and
/// are still open.
#[allow(clippy::too_many_arguments)]
pub async fn s8_log_crash(
    common: &Common,
    rounds: usize,
    rate: u64,
    entry_bytes: usize,
    secs_per_round: f64,
    secs_down: f64,
    victim_dir: Option<PathBuf>,
    flakey_cmd: Option<PathBuf>,
) -> anyhow::Result<()> {
    // With a victim directory (a dm-flakey filesystem) the victim must be
    // known before boot, so it is fixed to ordinal 2 and leadership is moved
    // away if it happens to be the leader.
    let fixed_victim = victim_dir.as_ref().map(|_| 2 * 1000 + 1);
    let mut cluster = {
        let _ = std::fs::remove_dir_all(&common.root);
        let mut c = Cluster::new(common.root.clone())?;
        if let (Some(v), Some(d)) = (fixed_victim, victim_dir.as_ref()) {
            c.dir_override.insert(v, d.clone());
        }
        c
    };
    for ordinal in 1..=3u64 {
        cluster.start(common.cfg(ordinal, 1)).await?;
    }
    {
        let members = cluster.addrs();
        let mut c = Conn::connect(&members[0].1).await?;
        c.unit(Req::Init {
            members: members.clone(),
        })
        .await?
        .map_err(anyhow::Error::msg)
        .context("initialize")?;
    }
    let addrs = cluster.addrs();
    let (mut leader_id, mut leader_addr) = find_leader(&addrs, Duration::from_secs(20))
        .await
        .context("no leader")?;
    if fixed_victim == Some(leader_id) {
        let other = addrs
            .iter()
            .map(|(id, _)| *id)
            .find(|id| *id != leader_id)
            .context("no other node")?;
        let mut c = Conn::connect(&leader_addr).await?;
        c.unit(Req::TriggerTransfer { to: other })
            .await?
            .map_err(anyhow::Error::msg)?;
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(20) {
            if let Some((id, addr)) = find_leader(&addrs, Duration::from_secs(2)).await {
                if id != leader_id {
                    leader_id = id;
                    leader_addr = addr;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        println!("moved leadership off the victim; leader is now {leader_id}");
    }
    let victim = fixed_victim.unwrap_or(
        addrs
            .iter()
            .map(|(id, _)| *id)
            .find(|id| *id != leader_id)
            .context("no follower")?,
    );
    anyhow::ensure!(victim != leader_id, "the victim must not be the leader");
    let flakey = |verb: &str| -> anyhow::Result<()> {
        if let Some(cmd) = flakey_cmd.as_ref() {
            let st = std::process::Command::new(cmd)
                .arg(verb)
                .status()
                .with_context(|| format!("{} {verb}", cmd.display()))?;
            anyhow::ensure!(st.success(), "{} {verb} failed: {st}", cmd.display());
        }
        Ok(())
    };
    println!(
        "# scenario 8: kill -9 follower {victim} {rounds}x under {rate}/s of {entry_bytes} B writes (leader {leader_id}, save_committed={}{})",
        common.committed,
        if flakey_cmd.is_some() {
            ", DROPPED WRITES before every kill (dm-flakey)"
        } else {
            ""
        }
    );

    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let mut conn = Conn::connect(&leader_addr).await?;
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut next_id: u64 = 1;
    let mut below_matched = 0usize;
    let mut above_leader_count = 0usize;
    let mut unreadable = 0usize;
    let mut vote_reversions = 0usize;
    let mut reopen_failures = 0usize;
    let mut catchup_ms: Vec<f64> = Vec::new();

    for round in 1..=rounds {
        let lat = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            secs_per_round,
        )
        .await;

        // What the leader believes the victim has matched, and what the victim
        // itself says, immediately before the kill.
        let leader_status = conn.status().await?;
        let matched = leader_status
            .replication
            .as_ref()
            .and_then(|r| r.iter().find(|(id, _)| *id == victim).and_then(|(_, m)| *m));
        let (last_log_before, vote_term_before) = {
            let mut c = Conn::connect(&cluster.addr_of(victim).context("no addr")?).await?;
            let s = c.status().await?;
            (s.last_log_index, s.vote_term)
        };

        // With dm-flakey: from here every write the victim's filesystem issues
        // is silently thrown away, which is what a power loss does to
        // everything that was never fsynced.
        flakey("drop")?;
        cluster.kill9(victim)?;
        flakey("up")?;
        let acked_before_kill = acked.len();
        let down = load_for(
            &mut conn,
            &mut acked,
            &mut next_id,
            entry_bytes,
            interval,
            secs_down,
        )
        .await;

        let t_restart = Instant::now();
        cluster.restart(victim).await?;
        let victim_addr = cluster.addr_of(victim).context("no addr")?;
        let (reopened, verdict) = {
            let mut c = Conn::connect(&victim_addr).await?;
            let st = c.status().await?;
            let v = c.verify_log().await?;
            (st, v)
        };
        let ok_log = match (reopened.reopen_last_log, matched) {
            (Some(last), Some(m)) => last >= m,
            (None, Some(_)) => false,
            _ => true,
        };
        if !ok_log {
            below_matched += 1;
        }
        // The log must never claim more than the leader ever sent, and every
        // index it claims must be readable: that is what dropped writes would
        // break.
        let above_leader = match (reopened.reopen_last_log, leader_status.last_log_index) {
            (Some(last), Some(leader_last)) => last > leader_last,
            _ => false,
        };
        if above_leader {
            above_leader_count += 1;
        }
        let readable = match &verdict {
            Ok(v) => v.hole_at.is_none() && v.read == v.expected,
            Err(_) => false,
        };
        if !readable {
            unreadable += 1;
        }
        let ok_vote = reopened.reopen_vote_term.unwrap_or(0) >= vote_term_before;
        if !ok_vote {
            vote_reversions += 1;
        }
        if reopened.reopen_last_log.is_none() && last_log_before.is_some() {
            reopen_failures += 1;
        }

        // Catch up to whatever the leader has applied now.
        let target = conn.status().await?.last_applied.unwrap_or(0);
        let t_catch = Instant::now();
        let mut caught = None;
        while t_catch.elapsed() < Duration::from_secs(60) {
            let probe = tokio::time::timeout(Duration::from_millis(500), async {
                let mut c = Conn::connect(&victim_addr).await?;
                c.status().await
            })
            .await;
            if let Ok(Ok(st)) = probe {
                if st.last_applied.unwrap_or(0) >= target {
                    caught = Some(t_catch.elapsed().as_secs_f64() * 1000.0);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if let Some(ms) = caught {
            catchup_ms.push(ms);
        }

        println!(
            "round {round}: acked before kill={acked_before_kill} (p50 {:.2} ms, {} while it was down), leader matched={:?} victim last_log={:?} vote_term={}",
            stats(lat).p50,
            down.len(),
            matched,
            last_log_before,
            vote_term_before
        );
        println!(
            "  reopened: log last={:?} purged={:?} committed={:?} vote_term={:?}; sm applied={:?} count={} digest={:x}; restart->answer {:.0} ms; caught up {} ",
            reopened.reopen_last_log,
            reopened.reopen_purged,
            reopened.reopen_committed,
            reopened.reopen_vote_term,
            reopened.reopen_sm_applied,
            reopened.reopen_sm_applied_count,
            reopened.reopen_sm_digest,
            t_restart.elapsed().as_secs_f64() * 1000.0,
            match caught {
                Some(ms) => format!("in {ms:.0} ms"),
                None => "NEVER (60 s)".to_string(),
            }
        );
        println!(
            "  read-back of the reopened log: {}",
            match &verdict {
                Ok(v) => format!(
                    "purged={:?} last={:?} read {}/{} entries{}",
                    v.purged,
                    v.last,
                    v.read,
                    v.expected,
                    match v.hole_at {
                        Some(i) => format!(", FIRST HOLE AT {i}"),
                        None => String::new(),
                    }
                ),
                Err(e) => format!("FAILED: {e}"),
            }
        );
        println!(
            "  log>=matched: {}{}   log<=leader: {}   readable: {}   vote monotone: {}",
            if ok_log { "OK" } else { "BELOW" },
            if !ok_log && flakey_cmd.is_some() {
                " (expected with dropped writes: the leader re-sends)"
            } else {
                ""
            },
            if above_leader { "FAIL" } else { "OK" },
            if readable { "OK" } else { "FAIL" },
            if ok_vote { "OK" } else { "FAIL" }
        );
    }

    // Nothing acknowledged may be missing anywhere, and the victim's state must
    // equal the others'.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut worst_missing = 0usize;
    let mut digests: Vec<(NodeId, u64, u64, Option<u64>)> = Vec::new();
    for (id, addr) in &addrs {
        let mut c = Conn::connect(addr).await?;
        let st = c.status().await?;
        let applied: BTreeSet<u64> = c.applied_ids().await?.into_iter().collect();
        let missing: Vec<u64> = acked.difference(&applied).cloned().collect();
        worst_missing = worst_missing.max(missing.len());
        digests.push((*id, st.applied_count, st.digest, st.last_applied));
        println!(
            "node {id}: applied {} ids, acknowledged {}, missing {} {:?}; last_applied={:?} count={} digest={:x}",
            applied.len(),
            acked.len(),
            missing.len(),
            missing.iter().take(5).collect::<Vec<_>>(),
            st.last_applied,
            st.applied_count,
            st.digest
        );
    }
    let same_digest = digests
        .iter()
        .all(|(_, c, d, _)| *c == digests[0].1 && *d == digests[0].2);
    if !catchup_ms.is_empty() {
        println!("catch-up after restart: {}", stats(catchup_ms));
    }
    println!(
        "VERDICT rounds={rounds} dropped-writes={} log-reopened-below-matched={below_matched} log-above-leader={above_leader_count} unreadable-log={unreadable} vote-reversions={vote_reversions} empty-reopen={reopen_failures} acknowledged-writes-missing={worst_missing} digests-equal={same_digest}",
        flakey_cmd.is_some()
    );

    cluster.shutdown_all().await;
    Ok(())
}

/// Scenario 9: the one-way partition that the open openraft issue GH#2080 is
/// about — a leader whose quorum-ack lease expires while its heartbeats keep
/// renewing the followers' leases.
///
/// The exact topology of #2080 needs a "bridge" node, but the mechanism it
/// exploits is reachable with three nodes and a one-way link: both followers
/// keep PROCESSING the leader's `append_entries` (so their follower-side
/// leader lease is renewed and they reject vote requests) while their answers
/// never reach the leader (so its quorum-ack lease expires and it refuses
/// writes). If openraft is liveness-safe here, some node campaigns and the
/// cell recovers by itself. If it is not, the cell is write-dead until an
/// operator acts, which is what this scenario times for both operator actions:
/// heal the network, or kill the stuck leader.
pub async fn s9_one_way_partition(common: &Common, secs_blocked: u64) -> anyhow::Result<()> {
    let mut cluster = boot(common, 3).await?;
    let addrs = cluster.addrs();
    let (leader_id, leader_addr) = find_leader(&addrs, Duration::from_secs(10))
        .await
        .context("no leader")?;
    let followers: Vec<(NodeId, String)> = addrs
        .iter()
        .filter(|(id, _)| *id != leader_id)
        .cloned()
        .collect();
    println!(
        "# scenario 9: one-way partition, leader {leader_id}, followers {:?}, heartbeat {} ms, election {}-{} ms",
        followers.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        common.heartbeat_ms,
        common.election_min_ms,
        common.election_max_ms
    );

    let mut conn = Conn::connect(&leader_addr).await?;
    let mut next_id = 1u64;
    for _ in 0..20 {
        write_retry(
            &addrs,
            &mut conn,
            next_id,
            payload(4096, next_id),
            Duration::from_secs(10),
        )
        .await?;
        next_id += 1;
    }
    println!("warm: {} writes committed", next_id - 1);

    for phase in ["heal", "kill"] {
        // Cut the leader's inbound acks.
        for (fid, faddr) in &followers {
            let mut c = Conn::connect(faddr).await?;
            c.unit(Req::DropResponsesTo {
                peers: vec![leader_id],
            })
            .await?
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("blocking on {fid}"))?;
        }
        let t_block = Instant::now();
        println!("--- phase {phase}: both followers stopped answering leader {leader_id} at t=0");

        let mut committed_during = 0u64;
        let mut last_err = String::new();
        let mut election = None;
        while t_block.elapsed() < Duration::from_secs(secs_blocked) {
            let res = tokio::time::timeout(
                Duration::from_millis(900),
                conn.write(next_id, payload(4096, next_id)),
            )
            .await;
            match res {
                Ok(Ok(Ok(_))) => committed_during += 1,
                Ok(Ok(Err(e))) => last_err = format!("{e:?}"),
                Ok(Err(e)) => {
                    last_err = e.to_string();
                    if let Ok(c) = Conn::connect(&leader_addr).await {
                        conn = c;
                    }
                }
                Err(_) => last_err = "write did not answer within 900 ms".into(),
            }
            next_id += 1;

            let mut line = Vec::new();
            for (id, addr) in &addrs {
                if let Ok(Ok(Ok(st))) = tokio::time::timeout(Duration::from_millis(300), async {
                    let mut c = Conn::connect(addr).await?;
                    c.status().await
                })
                .await
                .map(|r| r.map(Ok::<_, anyhow::Error>))
                {
                    line.push(format!(
                        "{id}{}t{} lead={:?} acked={}ms",
                        if st.is_leader { "*" } else { "=" },
                        st.term,
                        st.current_leader,
                        st.quorum_acked_ms_ago
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into())
                    ));
                    if st.is_leader && st.id != leader_id && election.is_none() {
                        election = Some(t_block.elapsed());
                    }
                }
            }
            println!(
                "  t={:4.1}s  {}",
                t_block.elapsed().as_secs_f64(),
                line.join("  ")
            );
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        println!("  during the block: {committed_during} writes committed, last error: {last_err}");

        // What does a client actually SEE? D13 holds a request while no leader
        // is known and then answers 503; that only works if the write comes
        // back. Give one write 10 s of patience and report what it returns.
        let t_probe = Instant::now();
        let probe = tokio::time::timeout(
            Duration::from_secs(10),
            conn.write(next_id, payload(4096, next_id)),
        )
        .await;
        next_id += 1;
        println!(
            "  patience probe: after {:.1} s the write answered {}",
            t_probe.elapsed().as_secs_f64(),
            match &probe {
                Ok(Ok(Ok(v))) => format!("COMMITTED at index {}", v.0),
                Ok(Ok(Err(e))) => format!("{e:?}"),
                Ok(Err(e)) => format!("connection error: {e}"),
                Err(_) => "NOTHING (still waiting at 10 s)".to_string(),
            }
        );
        if probe.is_err() {
            if let Ok(c) = Conn::connect(&leader_addr).await {
                conn = c;
            }
        }
        match election {
            Some(d) => println!(
                "  a NEW LEADER appeared by itself after {:.1} s",
                d.as_secs_f64()
            ),
            None => {
                println!("  no node became leader while the fault lasted: the cell is WRITE-DEAD")
            }
        }

        // The operator acts.
        let t_op = Instant::now();
        if phase == "heal" {
            for (_, faddr) in &followers {
                let mut c = Conn::connect(faddr).await?;
                c.unit(Req::DropResponsesTo { peers: vec![] })
                    .await?
                    .map_err(anyhow::Error::msg)?;
            }
            println!("  operator action: healed the link");
        } else {
            cluster.kill9(leader_id)?;
            for (_, faddr) in &followers {
                let mut c = Conn::connect(faddr).await?;
                c.unit(Req::DropResponsesTo { peers: vec![] })
                    .await?
                    .map_err(anyhow::Error::msg)?;
            }
            println!("  operator action: kill -9 the stuck leader {leader_id}");
        }

        let mut where_ = if phase == "heal" {
            addrs.clone()
        } else {
            followers.clone()
        };
        where_.retain(|(id, _)| phase == "heal" || *id != leader_id);
        let mut recovered = None;
        while t_op.elapsed() < Duration::from_secs(30) {
            if let Some((_, addr)) = find_leader(&where_, Duration::from_millis(200)).await {
                if let Ok(mut c) = Conn::connect(&addr).await {
                    if let Ok(Ok(_)) = c.write(next_id, payload(4096, next_id)).await {
                        recovered = Some(t_op.elapsed());
                        next_id += 1;
                        conn = c;
                        break;
                    }
                }
            }
            next_id += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        match recovered {
            Some(d) => println!(
                "  VERDICT phase={phase} write-dead={:.1}s recovery-after-operator={:.0}ms",
                t_block.elapsed().as_secs_f64() - t_op.elapsed().as_secs_f64(),
                d.as_secs_f64() * 1000.0
            ),
            None => println!(
                "  VERDICT phase={phase} NO WRITE COMMITTED within 30 s of the operator action"
            ),
        }
        if phase == "kill" {
            break;
        }
        // Let the cell settle before the second phase.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    cluster.shutdown_all().await;
    Ok(())
}
