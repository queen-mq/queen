// compat/transactions -- the M9 TRANSACTIONS acceptance suite, Java half.
//
// Every check here is driven by the real org.apache.kafka:kafka-clients, in
// java's single-file source mode against a directory of jars, exactly as
// compat/java and compat/java-matrix do. Nothing is asserted about the wire:
// what is asserted is what an application using the official client observes.
//
// The scenarios map onto scratchpad/design-transactions/DESIGN.md section 8.2:
//
//   s1  A2 + A3  commit visibility, abort invisibility, and the read_uncommitted
//                divergence the design predicts (a consumer sees LESS than Kafka
//                would show it, never more).
//   s2  A1       initTransactions() is fast. The F3 campaign measured a 20 s
//                hang here; it was FindCoordinator(key_type=1) answering the
//                retriable COORDINATOR_NOT_AVAILABLE and the client looping to
//                max.block.ms (DESIGN 1.6).
//   s3  A4       fencing by epoch bump, asserted by READING the partitions and
//                not by trusting the exception.
//   s4  A5       crash mid-transaction: SIGKILL, restart, nothing partial.
//   s6  A9       the stage caps, which have no Kafka analogue at all (DESIGN 5.3).
//   s7  A1-cluster the cluster-mode refusal, fatal and fast.
//   s8           the idempotent (non-transactional) producer still works.
//
// Run one scenario per JVM: the bootstrap differs between them (s6 and s7 have
// their own facades) and a fatal transactional producer poisons nothing that
// way.
//
//   env  QK_BOOTSTRAP   host:port of the facade under test
//        QK_RUN         a suffix that makes every topic and id unique
//        QK_RESTART_CMD an executable that SIGKILLs the facade and restarts it
//        QK_PARTITIONS  how many partitions the facade gives a new topic

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueenKafkaTxn {

    static final String BOOTSTRAP = env("QK_BOOTSTRAP", "127.0.0.1:32912");
    static final String RUN = env("QK_RUN", Long.toString(System.currentTimeMillis()));
    static final String RESTART = env("QK_RESTART_CMD", "");
    static int passed = 0;
    static int failed = 0;

    public static void main(String[] args) {
        String which = args.length > 0 ? args[0] : "s1";
        System.out.println("compat/transactions " + which + "  bootstrap=" + BOOTSTRAP + "  run=" + RUN);
        try {
            switch (which) {
                case "s1" -> s1CommitAndAbortVisibility();
                case "s2" -> s2InitTransactionsIsFast();
                case "s3" -> s3Fencing();
                case "s4" -> s4CrashMidTransaction();
                case "s6" -> s6Caps();
                case "s7" -> s7ClusterGate();
                case "s8" -> s8IdempotentStillWorks();
                default -> {
                    System.out.println("unknown scenario " + which);
                    System.exit(2);
                }
            }
        } catch (Throwable t) {
            failed++;
            System.out.println("  FAIL  the scenario threw before it could finish");
            t.printStackTrace(System.out);
        }
        System.out.println(which + ": " + passed + " passed, " + failed + " failed");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ------------------------------------------------------------------- s1

    // A2 and A3. One producer, three transactions: commit 1000, abort 1000,
    // commit 10. Both isolation levels are read after each, and the log end
    // offset is read from the broker rather than inferred.
    static void s1CommitAndAbortVisibility() throws Exception {
        String topic = "qkt-" + RUN + "-s1";
        String txnId = "qkt-" + RUN + "-s1";
        int parts = Integer.parseInt(env("QK_PARTITIONS", "4"));
        createTopic(topic, parts);

        long start = endOffsetSum(topic);
        check("a fresh topic starts at offset 0", start == 0, "log end offset " + start);

        try (Producer<String, String> p = txnProducer(txnId, 60_000)) {
            p.initTransactions();

            // --- transaction 1: 1000 records across every partition, committed.
            p.beginTransaction();
            List<Future<RecordMetadata>> acks = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                acks.add(p.send(new ProducerRecord<>(topic, i % parts, "k" + i, "v" + i)));
            }
            boolean allMinusOne = true;
            for (Future<RecordMetadata> f : acks) {
                if (f.get(60, TimeUnit.SECONDS).offset() != -1L) {
                    allMinusOne = false;
                }
            }
            // DESIGN 1.4: -1 is a first-class sentinel in this client. The
            // offset is not allocated until the commit, so the honest answer to
            // a staged record is "no offset", and RecordMetadata keeps -1
            // rather than adding a batch index to it.
            check("every staged send is acked with base_offset -1", allMinusOne, "");
            p.commitTransaction();

            int committed = drain(topic, "read_committed").total;
            int uncommitted = drain(topic, "read_uncommitted").total;
            check("read_committed sees the committed 1000", committed == 1000, "saw " + committed);
            check("read_uncommitted sees the committed 1000", uncommitted == 1000, "saw " + uncommitted);

            long afterCommit = endOffsetSum(topic);
            // DESIGN 0: N records advance the log by exactly N, where Kafka
            // advances it by N+1 because its commit marker consumes an offset.
            check("the log advanced by exactly 1000, with no commit marker",
                  afterCommit - start == 1000, "advanced by " + (afterCommit - start));
            Lag lag = drain(topic, "read_committed");
            check("a read_committed consumer reaches lag 0", lag.lag == 0, "lag " + lag.lag);

            // --- transaction 2: 1000 records, ABORTED.
            p.beginTransaction();
            for (int i = 0; i < 1000; i++) {
                p.send(new ProducerRecord<>(topic, i % parts, "a" + i, "aborted-" + i));
            }
            p.flush();
            // The open transaction is invisible at BOTH isolation levels, which
            // is the one place a consumer sees less than Kafka would show it
            // (DESIGN 2.1, classified deliberate in 8.3).
            int openUncommitted = drain(topic, "read_uncommitted").total;
            check("read_uncommitted sees nothing of an OPEN transaction (the documented divergence)",
                  openUncommitted == 1000, "saw " + openUncommitted + ", Kafka would show 2000");
            p.abortTransaction();

            // --- transaction 3: 10 records, committed.
            p.beginTransaction();
            for (int i = 0; i < 10; i++) {
                p.send(new ProducerRecord<>(topic, i % parts, "z" + i, "kept-" + i));
            }
            p.commitTransaction();

            Lag c2 = drain(topic, "read_committed");
            Lag u2 = drain(topic, "read_uncommitted");
            check("read_committed never sees an aborted record", c2.total == 1010, "saw " + c2.total);
            check("read_uncommitted never sees an aborted record either",
                  u2.total == 1010, "saw " + u2.total + ", Kafka would show 2010");
            check("no aborted key is readable at any isolation level",
                  !c2.keys.containsKey("a0") && !u2.keys.containsKey("a0"), "");

            long end = endOffsetSum(topic);
            // A3's number: the whole abort phase advanced the log by the 10
            // kept records and by nothing else, where Kafka would advance by
            // 1012 (1000 aborted, its marker, the 10, its marker).
            check("the abort phase advanced the log by exactly 10",
                  end - afterCommit == 10, "advanced by " + (end - afterCommit));
        }
    }

    // ------------------------------------------------------------------- s2

    // A1. The F3 campaign measured 20 000 ms here, which was max.block.ms
    // expiring while the client retried a RETRIABLE FindCoordinator refusal.
    // The measurement, not the outcome, is the check.
    static void s2InitTransactionsIsFast() {
        long[] took = new long[2];
        for (int i = 0; i < 2; i++) {
            String txnId = "qkt-" + RUN + "-s2-" + i;
            Properties props = producerProps(txnId, 60_000);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
            long t0 = System.nanoTime();
            try (Producer<String, String> p = new KafkaProducer<>(props)) {
                p.initTransactions();
                took[i] = (System.nanoTime() - t0) / 1_000_000L;
            }
        }
        check("initTransactions() returns in well under a second (cold JVM)",
              took[0] < 1000, took[0] + "ms, the F3 baseline was 20000ms");
        check("initTransactions() returns in well under a second (warm JVM)",
              took[1] < 1000, took[1] + "ms");
    }

    // ------------------------------------------------------------------- s3

    // A4. The second producer takes the id at epoch+1 and the first is fenced.
    // The decisive half is the second assertion: the partitions are READ, so a
    // fenced producer that had written its records and then been told it was
    // fenced would fail this even though its exception was right.
    static void s3Fencing() throws Exception {
        String topic = "qkt-" + RUN + "-s3";
        String txnId = "qkt-" + RUN + "-s3";
        createTopic(topic, 4);

        Producer<String, String> p1 = txnProducer(txnId, 60_000);
        p1.initTransactions();
        p1.beginTransaction();
        for (int i = 0; i < 100; i++) {
            p1.send(new ProducerRecord<>(topic, i % 4, "f" + i, "fenced-" + i)).get(30, TimeUnit.SECONDS);
        }

        Producer<String, String> p2 = txnProducer(txnId, 60_000);
        long t0 = System.nanoTime();
        p2.initTransactions();
        check("the second producer's initTransactions() succeeds",
              true, ((System.nanoTime() - t0) / 1_000_000L) + "ms");

        String seen = "no exception at all";
        try {
            p1.commitTransaction();
        } catch (Throwable t) {
            seen = t.getClass().getSimpleName();
        }
        check("the fenced producer's commitTransaction() raises ProducerFencedException",
              seen.equals("ProducerFencedException"), "raised " + seen);

        Lag after = drain(topic, "read_uncommitted");
        check("the topic contains ZERO of the fenced producer's records",
              after.total == 0, "read " + after.total);
        long end = endOffsetSum(topic);
        check("the log end offset never moved", end == 0, "log end offset " + end);

        // The second producer still owns the id and can commit.
        p2.beginTransaction();
        p2.send(new ProducerRecord<>(topic, 0, "w", "winner")).get(30, TimeUnit.SECONDS);
        p2.commitTransaction();
        Lag win = drain(topic, "read_committed");
        check("the winning producer commits normally afterwards", win.total == 1, "read " + win.total);

        closeQuietly(p1);
        closeQuietly(p2);
    }

    // ------------------------------------------------------------------- s4

    // A5. The stage is this process's and is deliberately lost on a restart
    // (DESIGN 4.8). What must be true is that nothing PARTIAL is in the log and
    // that the client is told, fatally, rather than being allowed to believe a
    // commit happened.
    static void s4CrashMidTransaction() throws Exception {
        if (RESTART.isEmpty()) {
            System.out.println("  SKIP  QK_RESTART_CMD is not set");
            return;
        }
        String topic = "qkt-" + RUN + "-s4";
        String txnId = "qkt-" + RUN + "-s4";
        createTopic(topic, 4);
        long before = endOffsetSum(topic);

        Producer<String, String> p = txnProducer(txnId, 60_000);
        p.initTransactions();
        p.beginTransaction();
        for (int i = 0; i < 500; i++) {
            p.send(new ProducerRecord<>(topic, i % 4, "c" + i, "crash-" + i)).get(30, TimeUnit.SECONDS);
        }

        restartFacade();

        String seen = "no exception at all";
        try {
            p.commitTransaction();
        } catch (Throwable t) {
            seen = t.getClass().getSimpleName();
        }
        // 48 = INVALID_TXN_STATE, which the Java transactional producer treats
        // as fatal. Any success here would be the one failure mode the design
        // says is unreachable: an application believing a commit that never
        // happened.
        check("commitTransaction() after a crash raises InvalidTxnStateException",
              seen.equals("InvalidTxnStateException"), "raised " + seen);
        closeQuietly(p);

        long after = endOffsetSum(topic);
        check("the log end offset is unchanged: nothing partial was written",
              after == before, "before " + before + ", after " + after);
        Lag drained = drain(topic, "read_uncommitted");
        check("not one record of the lost transaction is readable", drained.total == 0,
              "read " + drained.total);

        // The recovery path: a new producer on the same transactional.id takes
        // it at epoch+1 and works.
        try (Producer<String, String> p2 = txnProducer(txnId, 60_000)) {
            p2.initTransactions();
            p2.beginTransaction();
            p2.send(new ProducerRecord<>(topic, 0, "r", "recovered")).get(30, TimeUnit.SECONDS);
            p2.commitTransaction();
        }
        Lag rec = drain(topic, "read_committed");
        check("the same transactional.id re-inits and commits after the crash",
              rec.total == 1, "read " + rec.total);
    }

    // ------------------------------------------------------------------- s6

    // A9. Every cap here is a deviation with NO Kafka analogue (DESIGN 5.2):
    // records are appended as they arrive in Kafka, so a Kafka transaction has
    // no size. What is checked is that each cap surfaces as a NAMED client
    // exception rather than a hang or a closed connection, and that the
    // connection stays usable afterwards.
    static void s6Caps() throws Exception {
        String topic = "qkt-" + RUN + "-s6";
        createTopic(topic, 1);

        // --- the per-transaction byte cap. The facade under this scenario runs
        // at QUEEN_KAFKA_TXN_MAX_BYTES=65536, its floor.
        String big = "x".repeat(8192);
        try (Producer<String, String> p = txnProducer("qkt-" + RUN + "-s6-bytes", 60_000)) {
            p.initTransactions();
            p.beginTransaction();
            String seen = "no exception at all";
            for (int i = 0; i < 32 && seen.equals("no exception at all"); i++) {
                try {
                    p.send(new ProducerRecord<>(topic, 0, "b" + i, big)).get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    seen = root(e).getClass().getSimpleName();
                }
            }
            // 10 = MESSAGE_TOO_LARGE, which is on the closed set the Java
            // producer accepts on Produce and is not retriable: waiting will
            // not make an oversized transaction fit (DESIGN 5.2, 5.3).
            check("a transaction past QUEEN_KAFKA_TXN_MAX_BYTES raises RecordTooLargeException",
                  seen.equals("RecordTooLargeException"), "raised " + seen);
            String aborted = "clean";
            try {
                p.abortTransaction();
            } catch (Throwable t) {
                aborted = t.getClass().getSimpleName();
            }
            check("the producer can still abort after the cap refusal", aborted.equals("clean"), aborted);
            String next = "clean";
            try {
                p.beginTransaction();
                p.send(new ProducerRecord<>(topic, 0, "small", "ok")).get(30, TimeUnit.SECONDS);
                p.commitTransaction();
            } catch (Throwable t) {
                next = root(t).getClass().getSimpleName();
            }
            check("the connection stays sane: a small transaction commits next", next.equals("clean"), next);
        }

        // --- transaction.timeout.ms above QUEEN_KAFKA_TXN_MAX_TIMEOUT_MS.
        // 50 = INVALID_TRANSACTION_TIMEOUT, which is Kafka's own answer to
        // exactly this and the only cap in the table with a Kafka analogue.
        String tooLong = "no exception at all";
        Properties props = producerProps("qkt-" + RUN + "-s6-timeout", 1_000_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
        try (Producer<String, String> p = new KafkaProducer<>(props)) {
            p.initTransactions();
        } catch (Throwable t) {
            tooLong = describe(root(t));
        }
        // The Java client's InitProducerIdHandler turns every code it does not
        // name into KafkaException("Unexpected error in InitProducerIdResponse;
        // " + error.message()), so the NAMED thing is the message, not the
        // class, and it would be the same against a real broker answering 50.
        check("transaction.timeout.ms above the cap is refused with Kafka's own words for code 50",
              tooLong.contains("transaction timeout is larger") || tooLong.contains("InvalidTxnTimeout"),
              "raised " + tooLong);

        // --- the timeout sweep. A transaction left open past its own
        // transaction.timeout.ms has its stage dropped; the design says the
        // next request naming it is answered INVALID_TXN_STATE, and a SUCCESS
        // here would be a commit of records that were never written.
        String swept = "no exception at all";
        try (Producer<String, String> p = txnProducer("qkt-" + RUN + "-s6-sweep", 2_000)) {
            p.initTransactions();
            p.beginTransaction();
            p.send(new ProducerRecord<>(topic, 0, "s", "swept")).get(30, TimeUnit.SECONDS);
            long endBefore = endOffsetSum(topic);
            Thread.sleep(6_000);
            try {
                p.commitTransaction();
            } catch (Throwable t) {
                swept = root(t).getClass().getSimpleName();
            }
            long endAfter = endOffsetSum(topic);
            check("a transaction past transaction.timeout.ms is refused, not silently emptied",
                  swept.equals("InvalidTxnStateException"), "raised " + swept);
            check("and its records are not in the log", endAfter == endBefore,
                  "log end offset " + endBefore + " -> " + endAfter);
        } catch (Throwable t) {
            check("the swept producer closes without a second failure", false, root(t).toString());
        }

        // --- the per-transaction offset cap, MAX_TXN_OFFSETS = 62, which is
        // WIRE_KV_MAX_OPS (64) minus the fence and the group index (DESIGN 5.2).
        // This scenario's facade gives a new topic 70 partitions so a single
        // sendOffsetsToTransaction can exceed it.
        String wide = "qkt-" + RUN + "-s6-wide";
        createTopic(wide, 70);
        int wideParts = partitionsOf(wide);
        if (wideParts <= 62) {
            System.out.println("  SKIP  the offsets cap needs a topic wider than 62; got " + wideParts);
        } else {
            String group = "qkt-" + RUN + "-s6-group";
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            for (int i = 0; i < wideParts; i++) {
                offsets.put(new TopicPartition(wide, i), new OffsetAndMetadata(1L));
            }
            String seen = "no exception at all";
            try (Consumer<String, String> c = consumer("read_committed", group);
                 Producer<String, String> p = txnProducer("qkt-" + RUN + "-s6-offsets", 60_000)) {
                List<TopicPartition> tps = new ArrayList<>(offsets.keySet());
                c.assign(tps);
                p.initTransactions();
                p.beginTransaction();
                p.send(new ProducerRecord<>(topic, 0, "o", "offsets")).get(30, TimeUnit.SECONDS);
                try {
                    p.sendOffsetsToTransaction(offsets, c.groupMetadata());
                    p.commitTransaction();
                } catch (Throwable t) {
                    seen = describe(root(t));
                }
            }
            // 28 = INVALID_COMMIT_OFFSET_SIZE, the code offsets.rs already uses
            // for a commit this facade cannot store. The Java client's
            // TxnOffsetCommitHandler wraps a code it does not name in a
            // KafkaException carrying that code's message, so the message is
            // what is asserted.
            check("more than 62 offsets in one transaction is refused with Kafka's words for code 28",
                  seen.contains("committing offset data size") || seen.contains("InvalidCommitOffsetSize"),
                  "raised " + seen);
        }
    }

    // ------------------------------------------------------------------- s7

    // The cluster gate (DESIGN 4.6). A producer sends Produce to the partition
    // LEADER and EndTxn to the transaction COORDINATOR; in cluster mode those
    // are different nodes, so a stage would land on one facade and the commit
    // on another. The refusal is on CONFIGURATION and it must be FATAL, so the
    // client stops instead of looping to max.block.ms.
    static void s7ClusterGate() {
        String txnId = "qkt-" + RUN + "-s7";
        Properties props = producerProps(txnId, 60_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
        String seen = "no exception at all";
        long took;
        long t0 = System.nanoTime();
        try (Producer<String, String> p = new KafkaProducer<>(props)) {
            p.initTransactions();
            took = (System.nanoTime() - t0) / 1_000_000L;
        } catch (Throwable t) {
            took = (System.nanoTime() - t0) / 1_000_000L;
            seen = root(t).getClass().getSimpleName();
        }
        // 53 = TRANSACTIONAL_ID_AUTHORIZATION_FAILED, chosen because it is
        // fatal in the Java client: the same code the produce path gives a
        // transactional id, so a user meets ONE message about transactions.
        check("a clustered facade refuses initTransactions() with TransactionalIdAuthorizationException",
              seen.equals("TransactionalIdAuthorizationException"), "raised " + seen);
        check("and it refuses in well under a second, not at max.block.ms",
              took < 1000, took + "ms");
    }

    // ------------------------------------------------------------------- s8

    // The M8/F3 regression: an idempotent, NON-transactional producer is
    // untouched by any of this and still gets real, contiguous offsets.
    static void s8IdempotentStillWorks() throws Exception {
        String topic = "qkt-" + RUN + "-s8";
        createTopic(topic, 4);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "40000");

        Map<Integer, List<Long>> byPartition = new HashMap<>();
        try (Producer<String, String> p = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                RecordMetadata md = p.send(new ProducerRecord<>(topic, i % 4, "i" + i, "v" + i))
                                     .get(40, TimeUnit.SECONDS);
                byPartition.computeIfAbsent(md.partition(), k -> new ArrayList<>()).add(md.offset());
            }
        }
        boolean real = byPartition.values().stream().flatMap(List::stream).allMatch(o -> o >= 0);
        check("an idempotent producer still gets REAL offsets, not the staged -1", real, "");
        boolean contiguous = true;
        for (List<Long> offs : byPartition.values()) {
            for (int i = 1; i < offs.size(); i++) {
                if (offs.get(i) != offs.get(i - 1) + 1) {
                    contiguous = false;
                }
            }
        }
        check("its offsets are contiguous per partition", contiguous, "");
        Lag read = drain(topic, "read_committed");
        check("and a read_committed consumer reads all 100", read.total == 100, "read " + read.total);
    }

    // ------------------------------------------------------------- machinery

    record Lag(int total, long lag, Map<String, Integer> keys) { }

    /// Read a topic whole at one isolation level: assign every partition, seek
    /// to the beginning, and poll until three consecutive polls are empty.
    /// `lag` is the sum over partitions of end offset minus position, which is
    /// what a read_committed consumer against real Kafka cannot drive to zero
    /// (its last offset is a commit marker it never delivers).
    static Lag drain(String topic, String isolation) {
        Map<String, Integer> keys = new HashMap<>();
        int total = 0;
        long lag = 0;
        try (Consumer<String, String> c = consumer(isolation, null)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic, Duration.ofSeconds(30))) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            int empty = 0;
            while (empty < 3) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(1500));
                if (recs.isEmpty()) {
                    empty++;
                    continue;
                }
                empty = 0;
                for (ConsumerRecord<String, String> r : recs) {
                    total++;
                    keys.merge(r.key(), 1, Integer::sum);
                }
            }
            Map<TopicPartition, Long> ends = c.endOffsets(tps, Duration.ofSeconds(30));
            for (TopicPartition tp : tps) {
                lag += ends.getOrDefault(tp, 0L) - c.position(tp);
            }
        }
        return new Lag(total, lag, keys);
    }

    static long endOffsetSum(String topic) {
        try (Consumer<String, String> c = consumer("read_uncommitted", null)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic, Duration.ofSeconds(30))) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            long sum = 0;
            for (Long v : c.endOffsets(tps, Duration.ofSeconds(30)).values()) {
                sum += v;
            }
            return sum;
        }
    }

    static int partitionsOf(String topic) {
        try (Consumer<String, String> c = consumer("read_uncommitted", null)) {
            return c.partitionsFor(topic, Duration.ofSeconds(30)).size();
        }
    }

    static void createTopic(String topic, int parts) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        try (Admin a = Admin.create(props)) {
            a.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(topic, parts, (short) 1)))
             .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Auto-creation on the first Metadata request is the fallback, and
            // it is what a producer would trigger anyway. A topic that already
            // exists lands here too.
            System.out.println("  note  createTopics(" + topic + ", " + parts + "): " + root(e));
        }
    }

    static Producer<String, String> txnProducer(String txnId, int timeoutMs) {
        return new KafkaProducer<>(producerProps(txnId, timeoutMs));
    }

    static Properties producerProps(String txnId, int timeoutMs) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Integer.toString(timeoutMs));
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        // One record per batch. The Sender SPLITS a multi-record batch that is
        // answered MESSAGE_TOO_LARGE and retries the halves, which would turn
        // the byte-cap check into a retry storm instead of a client exception.
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, "1");
        return p;
    }

    static Consumer<String, String> consumer(String isolation, String group) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group == null ? "qkt-" + RUN + "-reader" : group);
        // The classic protocol: the facade advertises the classic group APIs
        // and not ConsumerGroupHeartbeat (KIP-848), and a 4.x client defaults
        // to negotiating the new one.
        p.put("group.protocol", "classic");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        return new KafkaConsumer<>(p);
    }

    static void restartFacade() throws Exception {
        Process proc = new ProcessBuilder(RESTART).inheritIO().start();
        int code = proc.waitFor();
        if (code != 0) {
            throw new IllegalStateException("QK_RESTART_CMD exited " + code);
        }
    }

    /// The class AND the message. Several of the codes this facade emits are
    /// ones the Java client does not name in its handlers, so it wraps them in
    /// a plain KafkaException carrying the error's own message: the message is
    /// then the only place the answer is legible, and a check that looked only
    /// at the class would call a correct refusal a failure.
    static String describe(Throwable t) {
        return t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
    }

    static Throwable root(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        return r;
    }

    static void closeQuietly(Producer<String, String> p) {
        try {
            p.close(Duration.ofSeconds(5));
        } catch (Throwable ignored) {
            // A fatal transactional producer throws from close(); the check
            // that matters already ran.
        }
    }

    static void check(String what, boolean ok, String detail) {
        if (ok) {
            passed++;
            System.out.println("  PASS  " + what + (detail.isEmpty() ? "" : "  [" + detail + "]"));
        } else {
            failed++;
            System.out.println("  FAIL  " + what + "  [" + detail + "]");
        }
    }

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? fallback : v;
    }
}
