//! The scenarios.
//!
//! Each one stands alone: it produces the corpus it needs, into a topic named
//! for itself and for the run id, so any scenario can be run by itself, twice,
//! or in any order, and two runs never share a topic or a group. Nothing here
//! starts or stops a stack.
//!
//! What is being proved, and what is deliberately NOT:
//!
//! * **Proved.** Metadata and auto-create; a 512-record produce across 8
//!   partitions with keys, headers and CreateTime, at `acks=all`, under every
//!   compression codec librdkafka has; a consumer GROUP that reads them back
//!   byte-exact and in per-partition order; an offset commit that a second
//!   consumer in the same group resumes from; the earliest/latest bounds and a
//!   seek.
//!   Idempotent produce is proved too since M7 F3: `idempotence()` below used
//!   to record the refusal and now measures the grant, off librdkafka's own
//!   protocol stream.
//! * **Not proved, on purpose.** Transactions (refused before InitProducerId is
//!   ever reached: FindCoordinator answers COORDINATOR_NOT_AVAILABLE for a
//!   TRANSACTION coordinator) and log compaction. CreateTopics, DeleteTopics,
//!   DescribeConfigs and the groups-admin trio ARE advertised since M7 F1/F2,
//!   but rdkafka's Rust AdminClient is not driven here — `compat/go`,
//!   `compat/sarama` and `compat/confluent-dotnet` cover that surface.
//!
//! Where a check fails for a reason that belongs to librdkafka rather than to
//! the facade, it says so on the line.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, RebalanceProtocol, StreamConsumer};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::clients::{self, Env};
use crate::harness::{check, check_eq, deadline, fail, info, ok, section, LONG, SHORT};
use crate::probe::{tpl_str, Probe, Recorder};
use crate::records::{self, Corpus};

type Rec = Arc<Mutex<Recorder>>;

// ---------------------------------------------------------------- produce ---

/// Produce a whole corpus and check every delivery report.
///
/// `send_result` rather than `send`: it hands back the delivery future without
/// awaiting, so all 512 records are in librdkafka's queue before the first
/// acknowledgement is waited on. That is what makes them batch, which is what
/// makes the compression lane mean anything.
async fn produce_corpus(
    producer: &FutureProducer<Probe>,
    corpus: &Corpus,
    label: &str,
) -> Option<BTreeMap<(i32, usize), i64>> {
    let mut futures = Vec::with_capacity(corpus.count);
    for seq in 0..corpus.count {
        let headers = records::owned_headers(seq);
        let rec = FutureRecord::to(&corpus.topic)
            .partition(corpus.partition(seq))
            .key(corpus.key(seq))
            .payload(corpus.value(seq))
            .timestamp(corpus.timestamp(seq))
            .headers(headers);
        match producer.send_result(rec) {
            Ok(f) => futures.push((seq, f)),
            Err((e, _)) => {
                fail(format!("{label}: enqueue seq={seq} refused: {e}"));
                return None;
            }
        }
    }

    let mut offsets: BTreeMap<(i32, usize), i64> = BTreeMap::new();
    let mut bad_partition = 0usize;
    let mut errors: Vec<String> = Vec::new();
    let mut nth: BTreeMap<i32, usize> = BTreeMap::new();
    for (seq, f) in futures {
        let outcome = deadline(90, &format!("{label}: delivery report seq={seq}"), f).await?;
        match outcome {
            Err(_canceled) => {
                errors.push(format!("seq={seq} delivery future cancelled"));
            }
            Ok(Err((e, _msg))) => {
                errors.push(format!("seq={seq} {e}"));
            }
            Ok(Ok(d)) => {
                if d.partition != corpus.partition(seq) {
                    bad_partition += 1;
                }
                let n = nth.entry(d.partition).or_default();
                offsets.insert((d.partition, *n), d.offset);
                *n += 1;
            }
        }
    }

    check_eq(
        offsets.len(),
        corpus.count,
        &format!("{label}: every record acknowledged"),
    );
    if !errors.is_empty() {
        fail(format!(
            "{label}: {} delivery errors, first 3: {:?}",
            errors.len(),
            &errors[..errors.len().min(3)]
        ));
    }
    check_eq(
        bad_partition,
        0,
        &format!("{label}: every ack names the partition it was sent to"),
    );

    if let Err(e) = producer.flush(SHORT) {
        fail(format!("{label}: flush: {e}"));
    }
    Some(offsets)
}

/// Drain a consumer until `want` records have arrived, the overall deadline
/// expires, or the stream goes quiet for `idle`.
///
/// A recv error does NOT end the loop. That is not leniency: a Kafka consumer
/// is a long-running poll and librdkafka surfaces retriable transport trouble
/// through the same stream as records — a coordinator moving, a connection
/// reset, a DNS answer that resolved to an address nothing is listening on.
/// An application that stopped on the first one would be broken. So errors are
/// COUNTED and PRINTED (the count is capped so a storm cannot bury the
/// transcript), and the judgement is made where it belongs: on whether the
/// records arrived before the deadline.
const MAX_ERRORS: usize = 50;

async fn drain(
    consumer: &StreamConsumer<Probe>,
    want: usize,
    overall: Duration,
    idle: Duration,
    label: &str,
) -> Vec<OwnedMessage> {
    let mut out = Vec::with_capacity(want);
    let mut errors: Vec<String> = Vec::new();
    let start = tokio::time::Instant::now();
    while out.len() < want {
        if start.elapsed() > overall {
            fail(format!(
                "{label}: overall deadline {overall:?} with {}/{want} consumed",
                out.len()
            ));
            break;
        }
        match tokio::time::timeout(idle, consumer.recv()).await {
            Err(_) => {
                // Quiet is a result: report it and stop rather than hang.
                info(format!(
                    "{label}: no record for {idle:?}, stopping at {}/{want}",
                    out.len()
                ));
                break;
            }
            Ok(Err(e)) => {
                let text = e.to_string();
                if !errors.contains(&text) {
                    errors.push(text);
                }
                if errors.len() >= MAX_ERRORS {
                    fail(format!(
                        "{label}: {MAX_ERRORS} distinct recv errors, giving up"
                    ));
                    break;
                }
            }
            Ok(Ok(m)) => out.push(m.detach()),
        }
    }
    for e in &errors {
        info(format!("{label}: recv reported (and recovered from): {e}"));
    }
    out
}

/// Byte-exact verification of a drained set against the corpus.
fn verify(msgs: &[OwnedMessage], corpus: &Corpus, label: &str) {
    let mut seen: BTreeMap<usize, usize> = BTreeMap::new();
    let mut per_partition: BTreeMap<i32, Vec<(i64, usize)>> = BTreeMap::new();
    let mut bad_key = Vec::new();
    let mut bad_value = Vec::new();
    let mut bad_headers = Vec::new();
    let mut bad_ts = Vec::new();
    let mut undecodable = 0usize;

    for m in msgs {
        let value = match m.payload() {
            Some(v) => v,
            None => {
                undecodable += 1;
                continue;
            }
        };
        let seq = match records::seq_of(value) {
            Some(s) if s < corpus.count => s,
            _ => {
                undecodable += 1;
                continue;
            }
        };
        *seen.entry(seq).or_default() += 1;
        per_partition
            .entry(m.partition())
            .or_default()
            .push((m.offset(), seq));

        if value != corpus.value(seq).as_slice() {
            bad_value.push(seq);
        }
        match m.key() {
            Some(k) if k == corpus.key(seq).as_slice() => {}
            other => bad_key.push(format!(
                "seq={seq} key={:?}",
                other.map(records::hex).unwrap_or_else(|| "<null>".into())
            )),
        }
        let got = records::read_headers(m.headers());
        if let Err(why) = records::headers_match(&got, seq) {
            if bad_headers.len() < 5 {
                bad_headers.push(format!("seq={seq}: {why}"));
            } else {
                bad_headers.push(String::new());
            }
        }
        if m.timestamp().to_millis() != Some(corpus.timestamp(seq)) {
            bad_ts.push(format!("seq={seq} ts={:?}", m.timestamp()));
        }
    }

    check_eq(undecodable, 0, &format!("{label}: every payload decodable"));
    check_eq(
        seen.len(),
        corpus.count,
        &format!("{label}: distinct records"),
    );
    let dups: usize = seen.values().filter(|c| **c > 1).count();
    if dups == 0 {
        ok(format!("{label}: no duplicates"));
    } else {
        info(format!(
            "{label}: {dups} record(s) delivered more than once (allowed only across a rebalance)"
        ));
    }

    if bad_value.is_empty() {
        ok(format!(
            "{label}: values byte-exact ({} bytes each, non-UTF-8)",
            corpus.value(0).len()
        ));
    } else {
        fail(format!(
            "{label}: {} values differ, first: {:?}",
            bad_value.len(),
            &bad_value[..bad_value.len().min(3)]
        ));
    }
    if bad_key.is_empty() {
        ok(format!("{label}: keys byte-exact"));
    } else {
        fail(format!(
            "{label}: {} keys differ, first: {:?}",
            bad_key.len(),
            &bad_key[..bad_key.len().min(3)]
        ));
    }
    let bad_headers: Vec<&String> = bad_headers.iter().filter(|s| !s.is_empty()).collect();
    if bad_headers.is_empty() {
        ok(format!(
            "{label}: headers byte-exact, in order, incl. empty value, NULL value and a repeated name"
        ));
    } else {
        fail(format!(
            "{label}: header mismatches, first: {:?}",
            &bad_headers[..bad_headers.len().min(3)]
        ));
    }
    if bad_ts.is_empty() {
        ok(format!("{label}: producer CreateTime round-tripped"));
    } else {
        fail(format!(
            "{label}: {} timestamps differ, first: {:?}",
            bad_ts.len(),
            &bad_ts[..bad_ts.len().min(3)]
        ));
    }

    // Per-partition order: offsets strictly increasing, and the sequence
    // numbers in the same order they were produced to that partition.
    let mut order_ok = true;
    let mut widths = 0;
    for (p, mut rows) in per_partition {
        widths += 1;
        let mut last_off = -1i64;
        for (i, (off, seq)) in rows.drain(..).enumerate() {
            if off <= last_off {
                order_ok = false;
                fail(format!(
                    "{label}: partition {p} offset went {last_off} -> {off}"
                ));
                break;
            }
            last_off = off;
            let want = corpus.seq_at(p, i);
            if seq != want {
                order_ok = false;
                fail(format!(
                    "{label}: partition {p} position {i} carries seq={seq}, produced seq={want}"
                ));
                break;
            }
        }
    }
    if order_ok {
        ok(format!(
            "{label}: per-partition order preserved across {widths} partitions"
        ));
    }
    check_eq(
        widths,
        corpus.partitions as usize,
        &format!("{label}: records landed on every partition"),
    );
}

// --------------------------------------------------------------- metadata ---

pub async fn metadata(env: &Env, rec: &Rec) {
    section("metadata: one broker, and who is allowed to auto-create");

    // A PRODUCER's metadata request always carries allow_auto_topic_creation=1
    // in librdkafka, so a bare Metadata naming an unknown topic creates it.
    let topic = env.topic("meta");
    let producer = clients::producer(env, Probe::new(rec, "meta-p"), &[]);
    let md = match producer.client().fetch_metadata(Some(&topic), SHORT) {
        Ok(md) => md,
        Err(e) => {
            fail(format!("fetch_metadata: {e}"));
            return;
        }
    };
    check_eq(md.brokers().len(), 1, "exactly one broker advertised");
    if let Some(b) = md.brokers().first() {
        ok(format!(
            "advertised broker id={} {}:{}",
            b.id(),
            b.host(),
            b.port()
        ));
    }
    match md.topics().iter().find(|t| t.name() == topic) {
        None => fail(format!("{topic} absent from metadata")),
        Some(t) => {
            check(
                t.error().is_none(),
                format!("{topic} carries no error code ({:?})", t.error()),
            );
            check_eq(
                t.partitions().len(),
                env.partitions as usize,
                "a producer's bare Metadata request alone auto-created it at QUEEN_KAFKA_DEFAULT_PARTITIONS",
            );
            let leaders: BTreeSet<i32> = t.partitions().iter().map(|p| p.leader()).collect();
            check_eq(
                leaders.len(),
                1,
                "one leader for every partition (the facade is one logical broker)",
            );
        }
    }

    // A CONSUMER's does not: librdkafka defaults `allow.auto.create.topics` to
    // false (Kafka's own client defaults it true), so it asks the broker NOT to
    // create, and queen-kafka answers UNKNOWN_TOPIC_OR_PARTITION — the correct
    // code for "does not exist and you told me not to make it"
    // (compat/ERRORS.md, Metadata table). This is the rdkafka-specific config
    // trap: a StreamConsumer subscribing to a topic no producer has written yet
    // waits forever unless this is turned on.
    let unasked = env.topic("meta-noauto");
    let strict = clients::consumer(env, Probe::new(rec, "meta-c"), &env.group("meta"), &[]);
    match strict.fetch_metadata(Some(&unasked), SHORT) {
        Ok(md) => match md.topics().iter().find(|t| t.name() == unasked) {
            Some(t) => {
                let e = format!("{:?}", t.error());
                check(
                    e.contains("UNKNOWN_TOPIC"),
                    format!("consumer default allow.auto.create.topics=false -> {e}"),
                );
                check_eq(t.partitions().len(), 0, "and nothing was created");
            }
            None => fail("the refused topic is missing from the response entirely"),
        },
        Err(e) => fail(format!("strict consumer fetch_metadata: {e}")),
    }

    // Turn it on and the same client gets the topic.
    let asked = env.topic("meta-auto");
    let lax = clients::consumer(
        env,
        Probe::new(rec, "meta-c2"),
        &env.group("meta2"),
        &[("allow.auto.create.topics", "true")],
    );
    match lax.fetch_metadata(Some(&asked), SHORT) {
        Ok(md) => match md.topics().iter().find(|t| t.name() == asked) {
            Some(t) => {
                check_eq(
                    t.partitions().len(),
                    env.partitions as usize,
                    "allow.auto.create.topics=true -> the consumer creates it too",
                );
            }
            None => fail("the requested topic is missing from the response entirely"),
        },
        Err(e) => fail(format!("lax consumer fetch_metadata: {e}")),
    }

    // `__`-prefixed names never exist anywhere, offsets included, and answer
    // UNKNOWN_TOPIC_OR_PARTITION rather than INVALID_TOPIC (compat/ERRORS.md).
    let reserved = format!("__rdk_rs_{}", env.run);
    match producer.client().fetch_metadata(Some(&reserved), SHORT) {
        Ok(md) => match md.topics().iter().find(|t| t.name() == reserved) {
            Some(t) => {
                let e = format!("{:?}", t.error());
                check(
                    e.contains("UNKNOWN_TOPIC"),
                    format!("a `__`-prefixed name answers {e} even to a producer that asked to create it"),
                );
                check_eq(t.partitions().len(), 0, "and was not auto-created");
            }
            None => info("`__`-prefixed name: absent from the topic list"),
        },
        Err(e) => info(format!("`__`-prefixed name: fetch_metadata: {e}")),
    }
}

// -------------------------------------------------------------- roundtrip ---

pub async fn roundtrip(env: &Env, rec: &Rec) {
    section("roundtrip: 512 records over 8 partitions, produced then group-consumed");
    let topic = env.topic("rt");
    let corpus = Corpus::new(&topic, env.partitions, 512);
    let producer = clients::producer(env, Probe::new(rec, "rt-p"), &[]);

    let Some(acked) = produce_corpus(&producer, &corpus, "produce").await else {
        return;
    };
    // A fresh topic: the n-th record on a partition must be acknowledged at
    // offset n. This is the assertion that would catch an offset the facade
    // invented rather than read back from Queen (C1: PushResult::offset).
    let mut off_ok = true;
    for ((p, n), off) in &acked {
        if *off != *n as i64 {
            off_ok = false;
            fail(format!(
                "produce: partition {p} record {n} acknowledged at offset {off}"
            ));
            break;
        }
    }
    if off_ok {
        ok("produce: acknowledged offsets are 0..63 per partition on a fresh topic");
    }

    let group = env.group("rt");
    let consumer = clients::consumer(env, Probe::new(rec, "rt-c"), &group, &[]);
    if let Err(e) = consumer.subscribe(&[&topic]) {
        fail(format!("subscribe: {e}"));
        return;
    }
    ok(format!("subscribed as group {group} (3s join delay is the facade's group.initial.rebalance.delay.ms)"));
    let msgs = drain(
        &consumer,
        corpus.count,
        Duration::from_secs(120),
        clients::POLL,
        "group consume",
    )
    .await;
    verify(&msgs, &corpus, "group consume");

    match consumer.assignment() {
        Ok(a) => {
            check_eq(
                a.count(),
                env.partitions as usize,
                "the single member was assigned every partition",
            );
        }
        Err(e) => fail(format!("assignment(): {e}")),
    }
    info(format!(
        "rebalance protocol negotiated: {}",
        match consumer.rebalance_protocol() {
            RebalanceProtocol::None => "None",
            RebalanceProtocol::Eager => "Eager",
            RebalanceProtocol::Cooperative => "Cooperative",
        }
    ));
    if let Ok(r) = rec.lock() {
        for line in r.rebalances.iter().filter(|l| l.starts_with("rt-c")) {
            info(format!("rebalance: {line}"));
        }
    }
}

// ----------------------------------------------------------------- codecs ---

pub async fn codecs(env: &Env, rec: &Rec) {
    section("compression: every codec this librdkafka build carries");
    // gzip/snappy/lz4 are unconditional in librdkafka; zstd is the `zstd`
    // cargo feature on rdkafka-sys, which this manifest enables.
    for codec in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let topic = env.topic(&format!("cod-{codec}"));
        let corpus = Corpus::new(&topic, env.partitions, 64);
        let producer = clients::producer(
            env,
            Probe::new(rec, &format!("cod-{codec}")),
            &[("compression.type", codec)],
        );
        let before = rec.lock().map(|r| r.lines.len()).unwrap_or(0);
        if produce_corpus(&producer, &corpus, &format!("{codec}: produce"))
            .await
            .is_none()
        {
            continue;
        }

        // Read back with an explicit assignment: no group, so no 3s join.
        let consumer = clients::consumer(
            env,
            Probe::new(rec, &format!("cod-{codec}-c")),
            &env.group(&format!("cod-{codec}")),
            &[],
        );
        let mut tpl = TopicPartitionList::new();
        for p in 0..env.partitions {
            tpl.add_partition_offset(&topic, p, Offset::Beginning).ok();
        }
        if let Err(e) = consumer.assign(&tpl) {
            fail(format!("{codec}: assign: {e}"));
            continue;
        }
        let msgs = drain(
            &consumer,
            corpus.count,
            Duration::from_secs(60),
            clients::POLL,
            &format!("{codec}: consume"),
        )
        .await;
        verify(&msgs, &corpus, codec);

        // Did librdkafka actually SEND this codec, or quietly downgrade?
        if let Ok(r) = rec.lock() {
            let notices: Vec<&String> = r.lines[before.min(r.lines.len())..]
                .iter()
                .filter(|l| l.contains("does not support compression type"))
                .collect();
            if let Some(n) = notices.first() {
                info(format!(
                    "{codec}: librdkafka downgraded the batch to uncompressed - {}",
                    n.trim()
                ));
                info(format!(
                    "{codec}: that is the CLIENT's Fetch-version gate, not a facade error; the records still landed (queen-kafka caps Fetch at v6 on purpose, versions.rs)"
                ));
            } else {
                ok(format!("{codec}: sent as {codec}, no downgrade notice"));
            }
        }
    }
}

// ----------------------------------------------------- commit and resume ---

pub async fn resume(env: &Env, rec: &Rec) {
    section(
        "commit + resume: a second consumer in the same group picks up where the first stopped",
    );
    let topic = env.topic("res");
    let corpus = Corpus::new(&topic, env.partitions, 200);
    let producer = clients::producer(env, Probe::new(rec, "res-p"), &[]);
    if produce_corpus(&producer, &corpus, "resume: produce")
        .await
        .is_none()
    {
        return;
    }
    drop(producer);

    let group = env.group("res");
    let first_batch = 80usize;

    // ---- member A
    let a = clients::consumer(env, Probe::new(rec, "res-A"), &group, &[]);
    if let Err(e) = a.subscribe(&[&topic]) {
        fail(format!("A subscribe: {e}"));
        return;
    }
    let got_a = drain(
        &a,
        first_batch,
        Duration::from_secs(90),
        clients::POLL,
        "A consume",
    )
    .await;
    check_eq(got_a.len(), first_batch, "A consumed its batch");

    // Commit exactly what A consumed: last offset + 1 per partition, Kafka's
    // definition of a committed position.
    let mut committed = TopicPartitionList::new();
    let mut high: BTreeMap<i32, i64> = BTreeMap::new();
    for m in &got_a {
        let e = high.entry(m.partition()).or_insert(-1);
        *e = (*e).max(m.offset());
    }
    for (p, off) in &high {
        committed
            .add_partition_offset(&topic, *p, Offset::Offset(off + 1))
            .ok();
    }
    match a.commit(&committed, CommitMode::Sync) {
        Ok(()) => ok(format!("A committed {}", tpl_str(&committed))),
        Err(e) => {
            fail(format!("A commit: {e}"));
            return;
        }
    }
    // Read it straight back: this is OffsetFetch against what OffsetCommit
    // just wrote (Queen KV today; the native cursor is C3).
    match a.committed(SHORT) {
        Ok(back) => {
            let want = committed.to_topic_map();
            let got = back.to_topic_map();
            let same = want.iter().all(|(k, v)| got.get(k) == Some(v));
            check(
                same,
                format!("A reads its own commit back: {}", tpl_str(&back)),
            );
        }
        Err(e) => fail(format!("A committed(): {e}")),
    }

    let seen_a: BTreeSet<usize> = got_a
        .iter()
        .filter_map(|m| m.payload().and_then(records::seq_of))
        .collect();
    drop(a);
    // Give the LeaveGroup time to land so B forms a fresh generation rather
    // than rebalancing with a ghost.
    tokio::time::sleep(Duration::from_secs(2)).await;
    ok("A closed (LeaveGroup sent)");

    // ---- member B, same group, brand new client
    let b = clients::consumer(env, Probe::new(rec, "res-B"), &group, &[]);
    if let Err(e) = b.subscribe(&[&topic]) {
        fail(format!("B subscribe: {e}"));
        return;
    }
    let remaining = corpus.count - seen_a.len();
    let got_b = drain(
        &b,
        remaining,
        Duration::from_secs(120),
        clients::POLL,
        "B consume",
    )
    .await;

    // The decisive check: B's FIRST record on each partition starts at the
    // offset A committed. Nothing before it is replayed.
    let mut firsts: BTreeMap<i32, i64> = BTreeMap::new();
    for m in &got_b {
        firsts.entry(m.partition()).or_insert(m.offset());
    }
    let mut resume_ok = true;
    for (p, want) in &high {
        match firsts.get(p) {
            None => info(format!(
                "B: nothing on partition {p} (A had committed through offset {want})"
            )),
            Some(first) if *first == want + 1 => {}
            Some(first) => {
                resume_ok = false;
                fail(format!(
                    "B resumed partition {p} at offset {first}, A committed {}",
                    want + 1
                ));
            }
        }
    }
    if resume_ok {
        ok("B resumed every partition at exactly the committed offset");
    }

    let seen_b: BTreeSet<usize> = got_b
        .iter()
        .filter_map(|m| m.payload().and_then(records::seq_of))
        .collect();
    let union: BTreeSet<usize> = seen_a.union(&seen_b).copied().collect();
    check_eq(union.len(), corpus.count, "A + B together saw every record");
    let overlap = seen_a.intersection(&seen_b).count();
    check_eq(overlap, 0, "no record was delivered to both A and B");
}

// ------------------------------------------------------------ auto-create ---

pub async fn autocreate(env: &Env, rec: &Rec) {
    section("auto-create: produce to a topic nobody has ever named");
    let topic = env.topic("auto");
    let corpus = Corpus::new(&topic, env.partitions, 24);
    let producer = clients::producer(env, Probe::new(rec, "auto"), &[]);
    let before = rec.lock().map(|r| r.lines.len()).unwrap_or(0);

    if produce_corpus(&producer, &corpus, "auto-create: produce")
        .await
        .is_none()
    {
        return;
    }
    match producer
        .client()
        .fetch_metadata(Some(&topic), SHORT)
        .map(|md| {
            md.topics()
                .iter()
                .find(|t| t.name() == topic)
                .map(|t| t.partitions().len())
        }) {
        Ok(Some(n)) => {
            check_eq(
                n,
                env.partitions as usize,
                "the new topic exists at the configured width",
            );
        }
        Ok(None) => fail("the new topic is absent from metadata after a successful produce"),
        Err(e) => fail(format!("fetch_metadata after auto-create: {e}")),
    }

    // The auto-create dance costs the client a retry or two; a retriable code
    // here is correct behaviour, a fatal one is not.
    if let Ok(r) = rec.lock() {
        let noisy: Vec<&String> = r.lines[before.min(r.lines.len())..]
            .iter()
            .filter(|l| {
                l.contains("UNKNOWN_TOPIC")
                    || l.contains("LEADER_NOT_AVAILABLE")
                    || l.contains("UnknownTopic")
            })
            .collect();
        if noisy.is_empty() {
            ok("no retriable error was needed: the first Metadata created it");
        } else {
            info(format!(
                "{} retriable notice(s) during the dance, e.g. {}",
                noisy.len(),
                noisy[0].trim()
            ));
        }
    }
}

// ---------------------------------------------------------------- offsets ---

pub async fn offsets(env: &Env, rec: &Rec) {
    section("offsets: earliest/latest bounds, assign-at-offset, seek");
    let topic = env.topic("off");
    let corpus = Corpus::new(&topic, env.partitions, 64); // 8 per partition
    let producer = clients::producer(env, Probe::new(rec, "off-p"), &[]);
    if produce_corpus(&producer, &corpus, "offsets: produce")
        .await
        .is_none()
    {
        return;
    }
    drop(producer);

    let consumer = clients::consumer(env, Probe::new(rec, "off-c"), &env.group("off"), &[]);
    let per = corpus.count as i64 / env.partitions as i64;

    let mut bounds_ok = true;
    for p in 0..env.partitions {
        match consumer.fetch_watermarks(&topic, p, SHORT) {
            Ok((low, high)) => {
                if low != 0 || high != per {
                    bounds_ok = false;
                    fail(format!(
                        "partition {p} watermarks ({low}, {high}), want (0, {per})"
                    ));
                }
            }
            Err(e) => {
                bounds_ok = false;
                fail(format!("fetch_watermarks({p}): {e}"));
            }
        }
    }
    if bounds_ok {
        ok(format!(
            "ListOffsets earliest/latest = (0, {per}) on all {} partitions",
            env.partitions
        ));
    }

    // Assign at an explicit offset.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(3)).ok();
    if let Err(e) = consumer.assign(&tpl) {
        fail(format!("assign at offset 3: {e}"));
        return;
    }
    match deadline(30, "recv after assign@3", consumer.recv()).await {
        Some(Ok(m)) => {
            check_eq(m.offset(), 3i64, "assign at offset 3 delivers offset 3");
            let seq = m.payload().and_then(records::seq_of);
            check_eq(
                seq,
                Some(corpus.seq_at(0, 3)),
                "and it is the 4th record produced to partition 0",
            );
        }
        Some(Err(e)) => fail(format!("recv after assign: {e}")),
        None => {}
    }

    // Seek back to the beginning on the same assignment.
    if let Err(e) = consumer.seek(&topic, 0, Offset::Beginning, SHORT) {
        fail(format!("seek to Beginning: {e}"));
    } else {
        match deadline(30, "recv after seek(Beginning)", consumer.recv()).await {
            Some(Ok(m)) => {
                check_eq(m.offset(), 0i64, "seek(Beginning) rewinds to offset 0");
            }
            Some(Err(e)) => fail(format!("recv after seek: {e}")),
            None => {}
        }
    }

    // Seek to the end: nothing further should arrive.
    if let Err(e) = consumer.seek(&topic, 0, Offset::End, SHORT) {
        fail(format!("seek to End: {e}"));
    } else {
        match tokio::time::timeout(Duration::from_secs(6), consumer.recv()).await {
            Err(_) => ok("seek(End) parks at the high watermark: nothing further arrives"),
            Ok(Ok(m)) => fail(format!("seek(End) still delivered offset {}", m.offset())),
            Ok(Err(e)) => fail(format!("recv after seek(End): {e}")),
        }
    }

    // ListOffsets by timestamp. queen-kafka answers a CONCRETE timestamp with
    // -1 and no error, on purpose (PLAN_QUEEN_KAFKA.md STATUS, "a concrete
    // ListOffsets timestamp answers -1 with no error"). Recorded, not asserted
    // as a failure - but it must not be an ERROR, because a client that gets
    // one stops.
    let mut ts_tpl = TopicPartitionList::new();
    ts_tpl
        .add_partition_offset(&topic, 0, Offset::Offset(records::TS_BASE))
        .ok();
    match consumer.offsets_for_times(ts_tpl, SHORT) {
        Ok(answer) => {
            ok(format!(
                "offsets_for_times returned without error: {}",
                tpl_str(&answer)
            ));
            let raw = answer
                .elements()
                .first()
                .and_then(|e| e.offset().to_raw())
                .unwrap_or(i64::MIN);
            if raw == -1 {
                info("-1 (OFFSET_INVALID) is the documented answer: queen-kafka does not index by timestamp");
            } else {
                info(format!("answered offset {raw}"));
            }
        }
        Err(e) => fail(format!(
            "offsets_for_times errored ({e}); a client that gets an error here stops"
        )),
    }
}

// ------------------------------------------------------------ idempotence ---

/// The idempotent producer, MEASURED.
///
/// This scenario used to assert the opposite. Idempotent produce needs
/// InitProducerId, and until M7 F3 the facade did not advertise key 22:
/// librdkafka read ApiVersions, saw the API missing, and failed the producer up
/// front with "Idempotent producer not supported by any of the 1 connected
/// broker(s)" — before a single byte of Produce went out. The check asserted
/// that refusal, because a silent success would have meant sequences nothing
/// enforced.
///
/// F3 advertises key 22 v0-4 and enforces the per-(producer, topic-partition)
/// sequence window (`protocols/queen-kafka/src/idempotent.rs`), so the assertion is
/// inverted. It is a MEASUREMENT and not just an inversion, because
/// `compat/CLIENT_MATRIX.md` carried this whole client family as "expected to
/// work, not re-measured": what is asserted here is the wire, from librdkafka's
/// own `debug=protocol` stream —
///
///   * an InitProducerId request was really SENT, and at a version inside the
///     advertised window (a client that quietly disabled the feature would
///     send none and still produce, which the offset check alone cannot tell
///     apart from success);
///   * the record landed and carries a real offset;
///   * a SECOND record on the same producer lands after it, which is the part
///     that exercises the sequence window rather than only the grant.
pub async fn idempotence(env: &Env, rec: &Rec) {
    section("enable.idempotence=true: works since M7 F3, measured on the wire");
    let topic = env.topic("idem");
    let producer = clients::producer(
        env,
        Probe::new(rec, "idem"),
        &[
            ("enable.idempotence", "true"),
            ("message.timeout.ms", "15000"),
        ],
    );

    let mut offsets = Vec::new();
    for seq in 0..2usize {
        let key = records::key_for(seq);
        let value = records::value_for(seq);
        let rc = deadline(
            60,
            "idempotent send",
            producer.send(
                FutureRecord::to(&topic)
                    .partition(0)
                    .key(&key)
                    .payload(&value),
                Duration::from_secs(20),
            ),
        )
        .await;
        match rc {
            Some(Ok(d)) => {
                ok(format!(
                    "idempotent send {} landed at {}:{}",
                    seq, d.partition, d.offset
                ));
                offsets.push(d.offset);
            }
            Some(Err((e, _))) => fail(format!(
                "idempotent send {seq} was refused: {e} - InitProducerId is advertised v0..=4 since M7 F3, so this is a regression and not the documented refusal"
            )),
            None => {}
        }
    }
    if offsets.len() == 2 {
        check(
            offsets[1] == offsets[0] + 1,
            format!(
                "the two sends are consecutive on the partition ({} then {}), so the sequence window advanced rather than rejecting or duplicating",
                offsets[0], offsets[1]
            ),
        );
    }

    // The wire, not the outcome: librdkafka disables idempotence by itself when
    // the broker does not advertise key 22, and a producer that did that would
    // still deliver both records above. Only the request proves the feature was
    // actually negotiated.
    if let Ok(r) = rec.lock() {
        match r.sent.get("InitProducerId") {
            Some(versions) => {
                let shown = versions
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                check(
                    versions.iter().all(|v| (0..=4).contains(v)),
                    format!(
                        "librdkafka SENT InitProducerId v{shown}, inside the advertised window 0..=4"
                    ),
                );
            }
            None => fail(
                "no InitProducerId request was sent at all - librdkafka disabled idempotence by itself, so the records above prove nothing about the feature",
            ),
        }
        for l in r
            .warnings
            .iter()
            .filter(|l| {
                l.contains("dempot") || l.contains("INIT_PRODUCER") || l.contains("InitProducerId")
            })
            .take(3)
        {
            info(format!("librdkafka said: {}", l.trim()));
        }
    }
}

// --------------------------------------------------------------- SASL/TLS ---

pub async fn sasl_tls(env: &Env, rec: &Rec) {
    section("SASL/PLAIN over TLS");
    let (Some(bootstrap), Some(token)) = (env.tls_bootstrap.clone(), env.sasl_token.clone()) else {
        info("QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset: lane skipped");
        return;
    };
    let tls_env = Env {
        bootstrap,
        ..env.clone()
    };
    let topic = tls_env.topic("tls");
    let corpus = Corpus::new(&topic, env.partitions, 64);

    let cfg = |c: &mut rdkafka::ClientConfig| {
        clients::with_sasl_ssl(c, &token, env.tls_ca.as_deref(), "rdkafka-rs");
    };

    // Producer over SASL_SSL.
    let producer = {
        let mut c = rdkafka::ClientConfig::new();
        c.set("bootstrap.servers", &tls_env.bootstrap)
            .set("debug", "protocol,security")
            .set("enable.idempotence", "false")
            .set("acks", "all")
            .set("message.timeout.ms", "60000")
            .set("linger.ms", "50")
            .set("client.id", format!("rdk-rs-tls-{}", env.run));
        c.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
        cfg(&mut c);
        match c.create_with_context::<Probe, FutureProducer<Probe>>(Probe::new(rec, "tls-p")) {
            Ok(p) => p,
            Err(e) => {
                fail(format!("SASL_SSL producer: {e}"));
                return;
            }
        }
    };
    if produce_corpus(&producer, &corpus, "tls: produce")
        .await
        .is_none()
    {
        return;
    }
    drop(producer);

    let group = tls_env.group("tls");
    let consumer = {
        let mut c = rdkafka::ClientConfig::new();
        c.set("bootstrap.servers", &tls_env.bootstrap)
            .set("debug", "protocol,security")
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .set("client.id", format!("rdk-rs-tls-c-{}", env.run));
        c.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
        cfg(&mut c);
        match c.create_with_context::<Probe, StreamConsumer<Probe>>(Probe::new(rec, "tls-c")) {
            Ok(x) => x,
            Err(e) => {
                fail(format!("SASL_SSL consumer: {e}"));
                return;
            }
        }
    };
    if let Err(e) = consumer.subscribe(&[&topic]) {
        fail(format!("tls subscribe: {e}"));
        return;
    }
    let msgs = drain(
        &consumer,
        corpus.count,
        Duration::from_secs(120),
        clients::POLL,
        "tls: consume",
    )
    .await;
    verify(&msgs, &corpus, "tls");
    drop(consumer);

    // A wrong password must be refused, and refused FATALLY: a client that
    // retries a bad credential forever is worse than one that stops.
    section("SASL/PLAIN: the wrong password");
    let mut c = rdkafka::ClientConfig::new();
    c.set("bootstrap.servers", &tls_env.bootstrap)
        .set("debug", "protocol,security")
        .set("group.id", tls_env.group("tls-bad"))
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "10000");
    c.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
    clients::with_sasl_ssl(
        &mut c,
        "definitely-not-the-token",
        env.tls_ca.as_deref(),
        "rdkafka-rs",
    );
    let bad_rec: Rec = Arc::new(Mutex::new(Recorder::default()));
    match c.create_with_context::<Probe, StreamConsumer<Probe>>(Probe::new(&bad_rec, "tls-bad")) {
        Err(e) => ok(format!("wrong password refused at construction: {e}")),
        Ok(bad) => {
            // The call must RETURN. A client stuck retrying a bad credential is
            // the failure mode SASL_AUTHENTICATION_FAILED exists to prevent.
            match bad.fetch_metadata(Some(&topic), Duration::from_secs(15)) {
                Ok(_) => fail("a wrong SASL password read metadata anyway"),
                Err(e) => ok(format!("wrong password could not read metadata: {e}")),
            }
            // …and the reason it returned must be AUTHENTICATION, not a
            // timeout: `fetch_metadata`'s own error is whatever the LAST
            // connection attempt was, which on a dual-stack name is as likely
            // to be a refused ::1 connect as the refusal itself. The decisive
            // evidence is the authentication error librdkafka logged
            // underneath it — and the log callback runs on librdkafka's own
            // threads, so it is waited for rather than read once.
            //
            // The wait POLLS the consumer rather than sleeping. rdkafka sets
            // `log.queue=true` and routes librdkafka's log stream to the main
            // queue (rdkafka-0.39 src/client.rs:257), and nothing serves that
            // queue for a consumer that is never polled — so a client that only
            // ever called `fetch_metadata` has an empty log and the evidence
            // looks absent when it is merely unread. This is a property of the
            // Rust binding, not of the facade, and it is the sort of thing this
            // row exists to find.
            let mut refusal: Option<String> = None;
            for _ in 0..60 {
                let _ = tokio::time::timeout(Duration::from_millis(250), bad.recv()).await;
                refusal = bad_rec.lock().ok().and_then(|r| {
                    r.warnings
                        .iter()
                        .chain(r.lines.iter())
                        .find(|l| l.to_ascii_lowercase().contains("authenticat"))
                        .cloned()
                });
                if refusal.is_some() {
                    break;
                }
            }
            match refusal {
                Some(l) => {
                    ok("the refusal is an AUTHENTICATION failure, not a timeout");
                    info(format!("librdkafka said: {}", l.trim()));
                    check(
                        l.contains("401") || l.contains("bearer token"),
                        "and it carries the facade's own explanation of what the password must be",
                    );
                }
                None => {
                    fail("no SASL authentication error was logged: the wrong password failed for some other reason");
                    if let Ok(r) = bad_rec.lock() {
                        for l in r.warnings.iter().take(4) {
                            info(format!("  instead: {}", l.trim()));
                        }
                    }
                }
            }
        }
    }
}

// ------------------------------------------------------------ the version ---

/// The table the whole suite exists to be able to print honestly: the
/// `(api, version)` pairs this librdkafka actually put on the wire, read out of
/// its own `debug=protocol` stream rather than assumed from `versions.rs`.
pub fn versions(rec: &Rec) {
    section("negotiated API versions (from librdkafka's own debug=protocol stream)");
    let Ok(r) = rec.lock() else {
        fail("recorder poisoned");
        return;
    };
    if r.sent.is_empty() {
        fail("no protocol lines captured - is debug=protocol set and the log level Debug?");
        return;
    }
    println!("  {:<20} {:<12} answered", "API", "sent");
    for (api, vs) in &r.sent {
        let got = r
            .received
            .get(api)
            .map(|v| {
                v.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_else(|| "-".into());
        println!(
            "  {:<20} v{:<11} v{}",
            api,
            vs.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(","),
            got
        );
    }
    let downgrades: BTreeSet<String> = r
        .grep("does not support compression type")
        .iter()
        .map(|l| {
            l.rsplit_once("compression type")
                .map(|(_, t)| t.trim().to_string())
                .unwrap_or_default()
        })
        .collect();
    if downgrades.is_empty() {
        ok("no codec was silently downgraded by the client");
    } else {
        info(format!(
            "codec(s) librdkafka refused to SEND against this broker's advertised versions: {:?} - deliberate (Fetch is capped at v6, versions.rs); the batches went uncompressed and the records landed",
            downgrades
        ));
    }

    let warn_kinds: BTreeSet<String> = r
        .warnings
        .iter()
        .map(|w| w.chars().take(300).collect::<String>())
        .collect();
    if warn_kinds.is_empty() {
        ok("librdkafka logged nothing at WARNING or worse for the whole run");
    } else {
        info(format!(
            "{} WARNING+ line(s) from librdkafka:",
            warn_kinds.len()
        ));
        for w in warn_kinds.iter().take(12) {
            info(format!("  {}", w.trim()));
        }
    }
    let _ = LONG;
}
