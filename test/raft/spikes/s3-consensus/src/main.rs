//! WP-0.5 spike S3: a 3-process openraft cluster over real TCP.
//!
//! `s3-consensus node ...` is one cluster member; every other subcommand is a
//! scenario that spawns members as child processes and measures something.
//! See README.md for what each scenario answers.

use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;

mod client;
mod cluster;
mod logstore;
mod net;
mod scenarios;
mod server;
mod sm;
mod types;
mod util;
mod wire;

use scenarios::Common;

#[derive(Parser)]
#[command(name = "s3-consensus", about = "PLAN_RAFT.md WP-0.5 consensus spike")]
struct Cli {
    /// Where scenario clusters put their data and logs.
    #[arg(long, default_value = "/private/tmp/s3-consensus-run", global = true)]
    root: PathBuf,
    /// First TCP port; node N listens on base + ordinal.
    #[arg(long, default_value_t = 26100, global = true)]
    base_port: u16,
    /// State machine durability: periodic (§11.4), batch or never.
    #[arg(long, default_value = "periodic", global = true)]
    fsync: String,
    #[arg(long, default_value_t = 100, global = true)]
    heartbeat_ms: u64,
    #[arg(long, default_value_t = 1000, global = true)]
    election_min_ms: u64,
    #[arg(long, default_value_t = 2000, global = true)]
    election_max_ms: u64,
    /// Window of the application's linearizable-read batcher (§9.4).
    #[arg(long, default_value_t = 2, global = true)]
    lin_batch_ms: u64,
    /// Interval of the periodic durable point (§11.4).
    #[arg(long, default_value_t = 1000, global = true)]
    durable_ms: u64,
    /// `save_committed` durability: none (openraft's example), buffered
    /// (`flush(false)`) or fsync (§12.3 as written).
    #[arg(long, default_value = "none", global = true)]
    committed: String,
    /// Start every node with `Raft::wait_for_recovery` and report how long it
    /// blocked (the alternative openraft offers to a durable `save_committed`).
    #[arg(long, action = clap::ArgAction::Set, default_value_t = false, global = true)]
    wait_recovery: bool,
    /// Bytes after which a state machine data file is sealed (§11.2).
    #[arg(long, default_value_t = 64 * 1024 * 1024, global = true)]
    file_bytes: u64,
    /// openraft `max_in_snapshot_log_to_keep`; 0 makes a purge really purge.
    #[arg(long, default_value_t = 1000, global = true)]
    keep_logs: u64,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run one cluster member (spawned by the scenarios).
    Node {
        #[arg(long)]
        id: u64,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        listen: String,
        #[arg(long, default_value = "periodic")]
        fsync: String,
        #[arg(long, default_value_t = 100)]
        heartbeat_ms: u64,
        #[arg(long, default_value_t = 1000)]
        election_min_ms: u64,
        #[arg(long, default_value_t = 2000)]
        election_max_ms: u64,
        #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
        pre_vote: bool,
        #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
        leader_restore: bool,
        #[arg(long, default_value_t = 1000)]
        max_in_snapshot_log_to_keep: u64,
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        file_bytes: u64,
        #[arg(long, default_value_t = 2)]
        lin_batch_ms: u64,
        #[arg(long, default_value_t = 1000)]
        durable_ms: u64,
        #[arg(long, default_value = "none")]
        committed: String,
        #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
        wait_recovery: bool,
    },
    /// 1: commit latency, 64 KiB entries, one write in flight (D4).
    Latency {
        #[arg(long, value_delimiter = ',', default_value = "200,500,1000")]
        rates: Vec<u64>,
        #[arg(long, default_value_t = 10)]
        secs: u64,
        #[arg(long, default_value_t = 65536)]
        entry_bytes: usize,
        /// Proposals in flight; D4 allows one.
        #[arg(long, default_value_t = 1)]
        writers: usize,
    },
    /// 2: kill -9 the leader under load.
    KillLeader {
        #[arg(long, default_value_t = 200)]
        rate: u64,
        #[arg(long, default_value_t = 16384)]
        entry_bytes: usize,
        #[arg(long, default_value_t = 5)]
        secs_before: u64,
        #[arg(long, default_value_t = 5)]
        secs_after: u64,
        /// SIGSTOP one follower for this long in the middle of the pre-kill
        /// load (0 = do not).
        #[arg(long, default_value_t = 0)]
        stop_follower_secs: u64,
    },
    /// 3: transfer_leader, healthy target and unreachable target.
    Transfer {
        /// How many healthy transfers before the unreachable-target case.
        #[arg(long, default_value_t = 1)]
        rounds: usize,
    },
    /// 4: ensure_linearizable, single vs batched.
    Linearizable {
        #[arg(long, value_delimiter = ',', default_value = "1,8,32")]
        concurrency: Vec<usize>,
        /// Offered read rates (open loop). Empty runs only the concurrency sweep.
        #[arg(long, value_delimiter = ',')]
        rates: Vec<u64>,
        #[arg(long, default_value_t = 5)]
        secs: u64,
    },
    /// 5: manifest snapshot to an empty learner, with resume.
    Snapshot {
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        total_bytes: u64,
        #[arg(long, default_value_t = 1024 * 1024)]
        entry_bytes: usize,
        /// Kill the learner this many ms into the transfer (0 = do not).
        #[arg(long, default_value_t = 0)]
        kill_after_ms: u64,
    },
    /// 6: the wiped voter, wrong way then right way.
    WipedVoter,
    /// 7: restart every node with enable_leader_restore=false.
    Restart {
        #[arg(long, default_value_t = 200)]
        entries: u64,
        /// `kill -9` the nodes instead of shutting them down gracefully.
        #[arg(long, action = clap::ArgAction::Set, default_value_t = false)]
        kill9: bool,
    },
    /// 8: `kill -9` a FOLLOWER under load, restart it, and check what its raft
    /// log reopened with (the qualification WP-0.3 demanded of the store, now
    /// applied to the log — see MEMO.md "Refutations").
    LogCrash {
        #[arg(long, default_value_t = 10)]
        rounds: usize,
        #[arg(long, default_value_t = 200)]
        rate: u64,
        #[arg(long, default_value_t = 65536)]
        entry_bytes: usize,
        /// Seconds of load before each kill.
        #[arg(long, default_value_t = 2.0)]
        secs_per_round: f64,
        /// Seconds of load with the victim down, before it is restarted.
        #[arg(long, default_value_t = 1.0)]
        secs_down: f64,
        /// Put the victim's data directory here instead of under --root (a
        /// dm-flakey mount). Fixes the victim to node 2001.
        #[arg(long)]
        victim_dir: Option<PathBuf>,
        /// A script called as `<cmd> drop` just before each kill and
        /// `<cmd> up` just after it: dropped unflushed writes (flaky-log.sh).
        #[arg(long)]
        flakey_cmd: Option<PathBuf>,
    },
    /// 9: one-way partition (the mechanism behind openraft GH#2080).
    OneWayPartition {
        /// How long the fault lasts before the operator acts.
        #[arg(long, default_value_t = 12)]
        secs_blocked: u64,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        let common = Common {
            root: cli.root.clone(),
            base_port: cli.base_port,
            fsync: cli.fsync.clone(),
            committed: cli.committed.clone(),
            wait_recovery: cli.wait_recovery,
            heartbeat_ms: cli.heartbeat_ms,
            election_min_ms: cli.election_min_ms,
            election_max_ms: cli.election_max_ms,
            lin_batch_ms: cli.lin_batch_ms,
            durable_ms: cli.durable_ms,
            file_bytes: cli.file_bytes,
            max_in_snapshot_log_to_keep: cli.keep_logs,
        };
        match cli.cmd {
            Cmd::Node {
                id,
                dir,
                listen,
                fsync,
                heartbeat_ms,
                election_min_ms,
                election_max_ms,
                pre_vote,
                leader_restore,
                max_in_snapshot_log_to_keep,
                file_bytes,
                lin_batch_ms,
                durable_ms,
                committed,
                wait_recovery,
            } => {
                server::run(server::NodeOpts {
                    id,
                    dir,
                    listen,
                    heartbeat_ms,
                    election_min_ms,
                    election_max_ms,
                    fsync: fsync.parse().map_err(anyhow::Error::msg)?,
                    file_bytes,
                    pre_vote,
                    leader_restore,
                    max_in_snapshot_log_to_keep,
                    lin_batch_ms,
                    durable_ms,
                    committed: committed.parse().map_err(anyhow::Error::msg)?,
                    wait_recovery,
                })
                .await
            }
            Cmd::Latency {
                rates,
                secs,
                entry_bytes,
                writers,
            } => scenarios::s1_latency(&common, rates, secs, entry_bytes, writers).await,
            Cmd::KillLeader {
                rate,
                entry_bytes,
                secs_before,
                secs_after,
                stop_follower_secs,
            } => {
                scenarios::s2_kill_leader(
                    &common,
                    rate,
                    entry_bytes,
                    secs_before,
                    secs_after,
                    stop_follower_secs,
                )
                .await
            }
            Cmd::Transfer { rounds } => scenarios::s3_transfer(&common, rounds).await,
            Cmd::Linearizable {
                concurrency,
                rates,
                secs,
            } => scenarios::s4_linearizable(&common, concurrency, rates, secs).await,
            Cmd::Snapshot {
                total_bytes,
                entry_bytes,
                kill_after_ms,
            } => scenarios::s5_snapshot(&common, total_bytes, entry_bytes, kill_after_ms).await,
            Cmd::WipedVoter => scenarios::s6_wiped_voter(&common).await,
            Cmd::Restart { entries, kill9 } => scenarios::s7_restart(&common, entries, kill9).await,
            Cmd::LogCrash {
                rounds,
                rate,
                entry_bytes,
                secs_per_round,
                secs_down,
                victim_dir,
                flakey_cmd,
            } => {
                scenarios::s8_log_crash(
                    &common,
                    rounds,
                    rate,
                    entry_bytes,
                    secs_per_round,
                    secs_down,
                    victim_dir,
                    flakey_cmd,
                )
                .await
            }
            Cmd::OneWayPartition { secs_blocked } => {
                scenarios::s9_one_way_partition(&common, secs_blocked).await
            }
        }
    })
}
