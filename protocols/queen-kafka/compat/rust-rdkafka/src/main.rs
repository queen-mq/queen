//! queen-kafka compat: **rdkafka (Rust)** — the `rdkafka` crate over a vendored
//! librdkafka, driven through `FutureProducer` and `StreamConsumer`.
//!
//! ```text
//!   compat [bootstrap] [runId]          # positional, like the librdkafka and java rows
//!   ./run.sh                            # same thing, reading the environment
//! ```
//!
//! ## What this row proves that the existing librdkafka row does not
//!
//! `compat/librdkafka` drives the same C library through kcat and through
//! confluent-kafka-python. This one drives it through the Rust binding, which
//! is a different surface with different defaults: `FutureProducer` returns one
//! future per record and reports the assigned partition and offset back to the
//! caller, and `StreamConsumer` is an async stream over the same rebalance
//! callbacks. If a facade behaviour only shows up when a client actually READS
//! the delivery report — an invented offset, a partition the broker moved — the
//! Java and kcat rows would not see it and this one does.
//!
//! It also carries a DIFFERENT librdkafka: 2.12.1, vendored and pinned by
//! rdkafka-sys 4.10.0, against the 2.15 the librdkafka row gets from Homebrew's
//! kcat. That is one version behind, not ahead, and it is the more useful side
//! to be on — the older client is the one still deployed, and two points on the
//! version curve are what tell you whether a negotiation result is a property of
//! the facade or of one client build.
//!
//! ## What is the CLIENT's fault, not the facade's
//!
//! Three things in the transcript look like facade defects and are not:
//!
//! * **zstd is downgraded to uncompressed.** librdkafka gates zstd PRODUCE on
//!   the broker advertising Fetch v10; queen-kafka caps Fetch at v6 on purpose
//!   (`versions.rs` — v7 introduces fetch sessions). librdkafka therefore logs
//!   `Broker does not support compression type zstd` and sends the batch
//!   uncompressed. The records land, byte-exact. Deliberate; see
//!   `PLAN_QUEEN_KAFKA.md` STATUS.
//! * **`enable.idempotence=true` WORKS since M7 F3.** InitProducerId (key 22)
//!   is advertised v0-4 and the per-(producer, topic-partition) sequence window
//!   is enforced. It used to be refused before a byte went out, because
//!   librdkafka reads ApiVersions and disables the feature by itself when the
//!   key is missing; `scenarios::idempotence` now measures the grant on the
//!   wire instead of recording the refusal.
//! * **A mysterious disconnect.** An out-of-window version on an ADVERTISED api
//!   key is answered by closing the connection, not by an error code
//!   (`compat/ERRORS.md`). If you change the config and connections start
//!   dropping, that is where to look first.
//!
//! ## What it needs
//!
//! A stack that is ALREADY RUNNING. Nothing here starts or stops a broker, a
//! facade or a Postgres; that is `rig.sh`'s job, or yours.

mod clients;
mod harness;
mod probe;
mod records;
mod scenarios;

use std::sync::{Arc, Mutex};

use clients::Env;
use harness::{failures, info, section};
use probe::Recorder;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let bootstrap = args
        .first()
        .cloned()
        .or_else(|| std::env::var("KAFKA_BOOTSTRAP").ok())
        .or_else(|| std::env::var("QUEEN_KAFKA_BOOTSTRAP").ok())
        .unwrap_or_else(|| "127.0.0.1:19092".to_string());
    let run = args
        .get(1)
        .cloned()
        .or_else(|| std::env::var("RUN_ID").ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs().to_string())
                .unwrap_or_else(|_| "0".into())
        });
    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "all".to_string());
    let partitions: i32 = std::env::var("QUEEN_KAFKA_PARTITIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    let env = Env {
        bootstrap,
        run,
        partitions,
        tls_bootstrap: std::env::var("QUEEN_KAFKA_TLS_BOOTSTRAP")
            .ok()
            .filter(|s| !s.is_empty()),
        sasl_token: std::env::var("QUEEN_KAFKA_SASL_TOKEN")
            .ok()
            .filter(|s| !s.is_empty()),
        tls_ca: std::env::var("QUEEN_KAFKA_TLS_CA")
            .ok()
            .filter(|s| !s.is_empty()),
    };

    // The crate version is the pin in Cargo.toml (cargo exposes no dependency
    // version at compile time). The librdkafka version beside it is NOT a
    // claim: it is read out of the C library that was actually linked.
    const RDKAFKA_CRATE: &str = "0.39.0";
    let (ver_num, ver_str) = rdkafka::util::get_rdkafka_version();
    println!("queen-kafka compat: rdkafka (Rust)");
    println!("  rdkafka crate   {RDKAFKA_CRATE} (cmake-build, vendored)");
    println!("  librdkafka      {ver_str} (0x{ver_num:08x}), as linked");
    println!("  bootstrap       {}", env.bootstrap);
    println!("  partitions      {}", env.partitions);
    println!("  runId           {}", env.run);
    println!("  scenario        {scenario}");
    match (&env.tls_bootstrap, &env.sasl_token) {
        (Some(b), Some(_)) => println!("  SASL_SSL        {b}"),
        _ => println!(
            "  SASL_SSL        skipped (QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset)"
        ),
    }

    let rec: Arc<Mutex<Recorder>> = Arc::new(Mutex::new(Recorder::default()));

    let want = |name: &str| scenario == "all" || scenario == name;

    if want("metadata") {
        scenarios::metadata(&env, &rec).await;
    }
    if want("roundtrip") {
        scenarios::roundtrip(&env, &rec).await;
    }
    if want("codecs") {
        scenarios::codecs(&env, &rec).await;
    }
    if want("resume") {
        scenarios::resume(&env, &rec).await;
    }
    if want("autocreate") {
        scenarios::autocreate(&env, &rec).await;
    }
    if want("offsets") {
        scenarios::offsets(&env, &rec).await;
    }
    if want("idempotence") {
        scenarios::idempotence(&env, &rec).await;
    }
    if scenario == "all" || scenario == "sasl" {
        scenarios::sasl_tls(&env, &rec).await;
    }

    scenarios::versions(&rec);

    section("result");
    let n = failures();
    info(format!("{} assertions", harness::checks()));
    if n == 0 {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL ({n})");
        std::process::exit(1);
    }
}
