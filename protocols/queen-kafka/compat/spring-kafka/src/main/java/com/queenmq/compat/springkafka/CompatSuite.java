package com.queenmq.compat.springkafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootVersion;
import org.springframework.context.ApplicationContext;
import org.springframework.core.SpringVersion;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Every phase of the compat bar, in order.  Each phase prints a "=== " header and one
 * "  ok" / "  FAIL" line per assertion; the process exit code is the failure count.
 */
@Component
public class CompatSuite {

    private static final byte[] BIN_HEADER = new byte[]{0x00, (byte) 0xFF, 0x7F, (byte) 0x80, 0x0A, 0x0D};
    private static final byte[] BIN_TAIL = new byte[]{0x00, 0x01, (byte) 0xFF, (byte) 0xFE};
    private static final int MAIN_COUNT = 512;
    private static final int CODEC_COUNT = 128;
    private static final int RESUME_FIRST = 200;
    private static final int RESUME_SECOND = 100;

    private final Support.Check check = new Support.Check();
    private Support.LogCapture capture;

    private final CompatConfig config;
    private final Recorder recorder;
    private final KafkaListenerEndpointRegistry registry;
    private final Listeners.SeekListener seekListener;

    private final KafkaTemplate<byte[], byte[]> noneTemplate;
    private final List<String> codecs = List.of("lz4", "zstd", "snappy", "gzip");
    private final ApplicationContext ctx;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;
    @Value("${queen.compat.run-id}")
    private String runId;
    @Value("${queen.compat.topic.main}")
    private String mainTopic;
    @Value("${queen.compat.topic.resume}")
    private String resumeTopic;
    @Value("${queen.compat.topic.autocreate}")
    private String autoCreateTopic;
    @Value("${queen.compat.topic.lz4}")
    private String lz4Topic;
    @Value("${queen.compat.topic.zstd}")
    private String zstdTopic;
    @Value("${queen.compat.topic.snappy}")
    private String snappyTopic;
    @Value("${queen.compat.topic.gzip}")
    private String gzipTopic;
    @Value("${queen.compat.topic.tls}")
    private String tlsTopic;
    @Value("${queen.compat.group.main}")
    private String mainGroup;
    @Value("${queen.compat.group.resume}")
    private String resumeGroup;
    @Value("${queen.compat.group.probe}")
    private String probeGroup;
    @Value("${queen.compat.group.tls}")
    private String tlsGroup;

    /** seq -> where the producer said it landed. */
    private final Map<Integer, long[]> mainSent = new TreeMap<>();

    public CompatSuite(CompatConfig config,
                       Recorder recorder,
                       KafkaListenerEndpointRegistry registry,
                       Listeners.SeekListener seekListener,
                       ApplicationContext ctx) {
        this.config = config;
        this.recorder = recorder;
        this.registry = registry;
        this.seekListener = seekListener;
        this.ctx = ctx;
        this.noneTemplate = config.template("none");
    }

    // ------------------------------------------------------------------ run

    public int run() {
        this.capture = Support.LogCapture.install();

        phaseVersions();
        phaseProduceMain();
        phaseApiVersions();
        phaseCompression();
        phaseConsumeGroup();
        phaseCooperativeRebalance();
        phaseConsumeCodecs();
        phaseCommitAndResume();
        phaseOffsetsAndSeek();
        phaseAutoCreate();
        phaseKafkaAdmin();
        phaseIdempotenceOn();
        phaseSaslTls();

        System.out.println();
        System.out.println("=== summary");
        System.out.println("  assertions passed: " + check.passes());
        System.out.println("  assertions failed: " + check.failures());
        if (check.failures() > 0) {
            System.out.println();
            System.out.println("=== last 120 kafka-clients DEBUG lines (diagnostics)");
            for (String line : capture.tail(120)) {
                System.out.println("  | " + line);
            }
        }
        System.out.println();
        if (check.failures() == 0) {
            System.out.println("RESULT: PASS");
            return 0;
        }
        System.out.println("RESULT: FAIL (" + check.failures() + ")");
        return 1;
    }

    // --------------------------------------------------------- 1. versions

    private void phaseVersions() {
        check.section("versions and configuration");
        check.note("spring-boot     " + SpringBootVersion.getVersion());
        check.note("spring-core     " + SpringVersion.getVersion());
        check.note("spring-kafka    " + implVersion(KafkaTemplate.class));
        check.note("kafka-clients   " + AppInfoParser.getVersion() + " (commit " + AppInfoParser.getCommitId() + ")");
        check.note("jvm             " + System.getProperty("java.version") + " / " + System.getProperty("java.vendor"));
        check.note("bootstrap       " + bootstrap);
        check.note("run id          " + runId);
        check.note("producer config " + config.producerProps("none"));
        check.note("consumer config " + config.consumerProps("earliest"));
        check.ok(AppInfoParser.getVersion() != null && !AppInfoParser.getVersion().isBlank(),
                "kafka-clients version resolved");
        // Prove Boot's own auto-configuration is intact and in play.
        for (String bean : List.of("kafkaTemplate", "kafkaProducerFactory", "kafkaConsumerFactory",
                "kafkaListenerContainerFactory", "kafkaAdmin")) {
            check.ok(ctx.containsBean(bean), "Boot auto-configured bean present: " + bean);
        }
    }

    private static String implVersion(Class<?> c) {
        Package p = c.getPackage();
        String v = p == null ? null : p.getImplementationVersion();
        return v == null ? "(unknown - running from exploded classes)" : v;
    }

    // ----------------------------------------------- 2. produce main topic

    private void phaseProduceMain() {
        check.section("produce " + MAIN_COUNT + " records to a topic that does not exist yet (" + mainTopic + ")");
        List<CompletableFuture<SendResult<byte[], byte[]>>> futures = new ArrayList<>();
        for (int i = 0; i < MAIN_COUNT; i++) {
            futures.add(noneTemplate.send(record(mainTopic, i, "main")));
        }
        noneTemplate.flush();

        int failed = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                SendResult<byte[], byte[]> r = futures.get(i).get(60, TimeUnit.SECONDS);
                mainSent.put(i, new long[]{r.getRecordMetadata().partition(), r.getRecordMetadata().offset()});
            } catch (Exception e) {
                if (failed++ < 3) {
                    check.note("send seq=" + i + " failed: " + rootCause(e));
                }
            }
        }
        check.eq(MAIN_COUNT, mainSent.size(), "all sends acknowledged with metadata");

        Set<Long> partitions = mainSent.values().stream().map(a -> a[0]).collect(Collectors.toCollection(TreeSet::new));
        check.note("partitions written: " + partitions);
        check.ok(partitions.size() >= 4, "records spread over at least 4 partitions (" + partitions.size() + ")");

        // Offsets the broker handed back must be dense and start at 0 on every partition.
        Map<Long, List<Long>> byPartition = new TreeMap<>();
        for (long[] pa : mainSent.values()) {
            byPartition.computeIfAbsent(pa[0], k -> new ArrayList<>()).add(pa[1]);
        }
        boolean dense = true;
        for (Map.Entry<Long, List<Long>> e : byPartition.entrySet()) {
            List<Long> offsets = new ArrayList<>(e.getValue());
            offsets.sort(Comparator.naturalOrder());
            for (int i = 0; i < offsets.size(); i++) {
                if (offsets.get(i) != i) {
                    dense = false;
                    break;
                }
            }
        }
        check.ok(dense, "broker-assigned offsets are 0..n-1 and gapless on every partition");
    }

    // ----------------------------------------------- 3. negotiated versions

    private void phaseApiVersions() {
        check.section("API versions actually negotiated (read out of NetworkClient's own DEBUG stream)");
        List<String> lines = capture.negotiated();
        check.ok(!lines.isEmpty(), "captured the ApiVersions table the client negotiated");

        Set<String> supported = new java.util.TreeSet<>();
        List<String> unsupported = new ArrayList<>();
        for (String line : lines) {
            int open = line.indexOf('(');
            int close = line.lastIndexOf(')');
            String body = (open >= 0 && close > open) ? line.substring(open + 1, close) : line;
            for (String entry : body.split(", (?=[A-Za-z])")) {
                String e = entry.trim().replaceAll("\\)\\.?$", "");
                if (e.isEmpty()) {
                    continue;
                }
                if (e.endsWith("UNSUPPORTED")) {
                    unsupported.add(e.substring(0, e.indexOf(':')).trim());
                } else {
                    supported.add(e);
                }
            }
        }
        for (String s : supported) {
            System.out.println("  |  " + s);
        }
        check.note("keys the facade does NOT advertise: " + unsupported.size()
                + " (deliberate - see PLAN_QUEEN_KAFKA.md STATUS)");
        for (String notable : List.of("InitProducerId", "CreateTopics", "DeleteTopics", "DescribeConfigs",
                "DescribeCluster", "ListGroups", "DescribeGroups", "OffsetDelete", "OffsetForLeaderEpoch",
                "ConsumerGroupHeartbeat", "GetTelemetrySubscriptions")) {
            check.note("  unsupported, as designed: " + notable + " -> "
                    + (unsupported.stream().anyMatch(u -> u.startsWith(notable + "(")) ? "confirmed absent" : "PRESENT"));
        }
        check.ok(capture.saw("apiversions-downgrade"),
                "client's first ApiVersions (v4) was refused with UNSUPPORTED_VERSION and it retried at v3");
    }

    // ------------------------------------------------------ 4. compression

    private void phaseCompression() {
        check.section("produce with each compression codec kafka-clients ships");
        Map<String, String> topics = Map.of("lz4", lz4Topic, "zstd", zstdTopic, "snappy", snappyTopic, "gzip", gzipTopic);
        for (String codec : codecs) {
            String topic = topics.get(codec);
            KafkaTemplate<byte[], byte[]> template = config.template(codec);
            List<CompletableFuture<SendResult<byte[], byte[]>>> futures = new ArrayList<>();
            for (int i = 0; i < CODEC_COUNT; i++) {
                futures.add(template.send(record(topic, i, codec)));
            }
            template.flush();
            int acked = 0;
            String error = null;
            for (CompletableFuture<SendResult<byte[], byte[]>> f : futures) {
                try {
                    f.get(60, TimeUnit.SECONDS);
                    acked++;
                } catch (Exception ex) {
                    if (error == null) {
                        error = rootCause(ex);
                    }
                }
            }
            if (error != null) {
                check.note(codec + " first error: " + error);
            }
            check.eq(CODEC_COUNT, acked, "compression.type=" + codec + " produced " + CODEC_COUNT + " records");
        }
    }

    // ----------------------------------------- 5. consume via a group, c=4

    private void phaseConsumeGroup() {
        check.section("@KafkaListener consumer group, concurrency=4, over " + mainTopic);
        MessageListenerContainer container = registry.getListenerContainer("main");
        check.ok(container instanceof ConcurrentMessageListenerContainer, "container is a ConcurrentMessageListenerContainer");
        container.start();

        boolean got = Support.await("all " + MAIN_COUNT + " records delivered", 120_000,
                () -> distinct(recorder.lane("main")).size() >= MAIN_COUNT);
        // let any late duplicates settle so the dedupe assertion is honest
        Support.awaitQuiescence(() -> recorder.count("main"), 2_000, 15_000);

        List<Recorder.Got> raw = recorder.lane("main");
        Map<String, Recorder.Got> uniq = distinct(raw);
        check.ok(got, "listener received every record before the deadline");
        check.eq(MAIN_COUNT, uniq.size(), "distinct (partition,offset) delivered");
        check.note("raw deliveries " + raw.size() + " (" + (raw.size() - uniq.size())
                + " redelivered across rebalance - at-least-once is the contract)");

        if (container instanceof ConcurrentMessageListenerContainer<?, ?> c) {
            check.eq(4, c.getContainers().size(), "concurrency spawned 4 child containers / 4 consumers");
            Collection<TopicPartition> assigned = c.getAssignedPartitions();
            check.note("assigned partitions: " + (assigned == null ? "null" : new TreeSet<>(
                    assigned.stream().map(TopicPartition::toString).collect(Collectors.toList()))));
            check.ok(assigned != null && assigned.size() >= 4,
                    "group assignment covers the topic (" + (assigned == null ? 0 : assigned.size()) + " partitions)");
        }

        Set<String> threads = raw.stream().map(g -> g.thread).collect(Collectors.toCollection(TreeSet::new));
        check.note("listener threads that received records: " + threads);
        check.ok(threads.size() >= 2, "work was spread over more than one consumer thread (" + threads.size() + ")");

        List<String> rebalances = recorder.rebalanceEvents();
        check.ok(rebalances.stream().anyMatch(s -> s.contains("ASSIGNED")),
                "container rebalance listener fired onPartitionsAssigned");
        check.ok(capture.saw("group-coordinator-found"),
                "FindCoordinator resolved a group coordinator");
        for (String r : rebalances) {
            check.note("rebalance: " + r);
        }

        // byte-exact round trip + per-partition order
        verifyRoundTrip(uniq.values(), MAIN_COUNT, "main");
    }

    private void verifyRoundTrip(Collection<Recorder.Got> got, int expected, String tag) {
        int badKey = 0, badValue = 0, badHeader = 0, badSeq = 0;
        Map<Integer, List<Recorder.Got>> byPartition = new TreeMap<>();
        Set<Integer> seenSeq = new TreeSet<>();
        for (Recorder.Got g : got) {
            int seq = seqOf(g.value);
            if (seq < 0) {
                badSeq++;
                continue;
            }
            seenSeq.add(seq);
            if (!Recorder.bytesEqual(keyFor(seq), g.key)) {
                badKey++;
            }
            if (!Recorder.bytesEqual(payload(seq, tag), g.value)) {
                badValue++;
            }
            if (!Recorder.bytesEqual(String.valueOf(seq).getBytes(StandardCharsets.UTF_8), g.headers.get("seq"))
                    || !Recorder.bytesEqual(BIN_HEADER, g.headers.get("bin"))
                    || !Recorder.bytesEqual(tag.getBytes(StandardCharsets.UTF_8), g.headers.get("tag"))
                    || g.headers.size() != 3) {
                badHeader++;
            }
            byPartition.computeIfAbsent(g.partition, k -> new ArrayList<>()).add(g);
        }
        check.eq(expected, seenSeq.size(), "[" + tag + "] every produced sequence number came back exactly once");
        check.eq(0, badSeq, "[" + tag + "] every payload parsed");
        check.eq(0, badKey, "[" + tag + "] keys byte-identical");
        check.eq(0, badValue, "[" + tag + "] payloads byte-identical (incl. the 0x00/0xFF binary tail)");
        check.eq(0, badHeader, "[" + tag + "] all 3 headers byte-identical (incl. the binary one)");

        int orderBreaks = 0;
        for (Map.Entry<Integer, List<Recorder.Got>> e : byPartition.entrySet()) {
            List<Recorder.Got> list = new ArrayList<>(e.getValue());
            list.sort(Comparator.comparingLong(g -> g.offset));
            long prevOffset = -1;
            int prevSeq = -1;
            for (Recorder.Got g : list) {
                if (g.offset <= prevOffset) {
                    orderBreaks++;
                }
                int seq = seqOf(g.value);
                // producers write monotonically increasing seq, so within one partition
                // the seq order must follow the offset order.
                if (seq < prevSeq) {
                    orderBreaks++;
                }
                prevOffset = g.offset;
                prevSeq = seq;
            }
        }
        check.eq(0, orderBreaks, "[" + tag + "] per-partition order preserved (offset and produce order agree)");

        if ("main".equals(tag)) {
            int mismatched = 0;
            for (Recorder.Got g : got) {
                int seq = seqOf(g.value);
                long[] sent = mainSent.get(seq);
                if (sent == null || sent[0] != g.partition || sent[1] != g.offset) {
                    mismatched++;
                }
            }
            check.eq(0, mismatched, "[main] consumed (partition,offset) equals the producer's own RecordMetadata");
        }
    }

    // ------------------------------- 5b. cooperative incremental rebalancing

    private void phaseCooperativeRebalance() {
        check.section("cooperative-sticky rebalance protocol (Spring's recommended assignor)");
        MessageListenerContainer container = registry.getListenerContainer("coop");
        container.start();
        boolean got = Support.await("cooperative group drains " + mainTopic, 120_000,
                () -> distinct(recorder.lane("coop")).size() >= MAIN_COUNT);
        Support.awaitQuiescence(() -> recorder.count("coop"), 2_000, 15_000);
        container.stop();
        Map<String, Recorder.Got> uniq = distinct(recorder.lane("coop"));
        check.ok(got, "CooperativeStickyAssignor group formed and drained the topic");
        check.eq(MAIN_COUNT, uniq.size(), "distinct records under the cooperative protocol");
        check.note("the facade logs this group's protocol as \"cooperative-sticky\"; the eager "
                + "groups above negotiate \"range\"");
    }

    // ------------------------------------------------ 6. consume the codecs

    private void phaseConsumeCodecs() {
        check.section("read back every compression codec");
        MessageListenerContainer container = registry.getListenerContainer("codec");
        container.start();
        int expected = CODEC_COUNT * codecs.size();
        boolean got = Support.await("all codec records", 120_000,
                () -> distinct(recorder.lane("codec")).size() >= expected);
        Support.awaitQuiescence(() -> recorder.count("codec"), 2_000, 15_000);
        container.stop();

        Map<String, Recorder.Got> uniq = distinct(recorder.lane("codec"));
        check.ok(got, "codec listener drained all four topics before the deadline");
        check.eq(expected, uniq.size(), "distinct records across lz4/zstd/snappy/gzip");

        Map<String, String> topicToCodec = Map.of(lz4Topic, "lz4", zstdTopic, "zstd",
                snappyTopic, "snappy", gzipTopic, "gzip");
        Map<String, List<Recorder.Got>> perTopic = uniq.values().stream()
                .collect(Collectors.groupingBy(g -> g.topic));
        for (Map.Entry<String, String> e : topicToCodec.entrySet()) {
            List<Recorder.Got> list = perTopic.getOrDefault(e.getKey(), List.of());
            check.eq(CODEC_COUNT, list.size(), "codec " + e.getValue() + " round-tripped " + CODEC_COUNT + " records");
            verifyRoundTrip(list, CODEC_COUNT, e.getValue());
        }
    }

    // --------------------------------------- 7. commit, stop, resume anew

    private void phaseCommitAndResume() {
        check.section("offset commit, stop, and a NEW consumer instance in the same group");
        produce(noneTemplate, resumeTopic, 0, RESUME_FIRST, "resume");
        check.pass("produced " + RESUME_FIRST + " records to " + resumeTopic);

        MessageListenerContainer a = registry.getListenerContainer("resumeA");
        a.start();
        boolean firstPass = Support.await("resumeA drains " + RESUME_FIRST, 120_000,
                () -> distinct(recorder.lane("resumeA")).size() >= RESUME_FIRST);
        Support.awaitQuiescence(() -> recorder.count("resumeA"), 2_000, 10_000);
        check.ok(firstPass, "first consumer instance drained the backlog");
        check.eq(RESUME_FIRST, distinct(recorder.lane("resumeA")).size(), "resumeA distinct records");

        a.stop();
        Support.await("resumeA stopped", 30_000, () -> !a.isRunning());
        check.ok(!a.isRunning(), "first consumer instance stopped (container closed its consumer)");

        // Read the committed offsets back with a raw consumer: this is OffsetFetch on the wire.
        Map<TopicPartition, Long> committed = committedOffsets(resumeGroup, resumeTopic);
        long committedTotal = committed.values().stream().mapToLong(Long::longValue).sum();
        check.note("committed offsets after resumeA: " + committed);
        check.eq(RESUME_FIRST, committedTotal, "committed offsets sum to everything resumeA consumed");

        produce(noneTemplate, resumeTopic, RESUME_FIRST, RESUME_SECOND, "resume");
        check.pass("produced " + RESUME_SECOND + " more records while nobody was consuming");

        MessageListenerContainer b = registry.getListenerContainer("resumeB");
        b.start();
        boolean secondPass = Support.await("resumeB drains " + RESUME_SECOND, 120_000,
                () -> distinct(recorder.lane("resumeB")).size() >= RESUME_SECOND);
        Support.awaitQuiescence(() -> recorder.count("resumeB"), 3_000, 15_000);
        b.stop();

        Map<String, Recorder.Got> uniqB = distinct(recorder.lane("resumeB"));
        check.ok(secondPass, "second consumer instance received the new records");
        Set<Integer> seqsB = uniqB.values().stream().map(g -> seqOf(g.value))
                .collect(Collectors.toCollection(TreeSet::new));
        check.eq(RESUME_SECOND, seqsB.size(), "resumeB saw exactly the records produced after the commit");
        long replayed = seqsB.stream().filter(s -> s < RESUME_FIRST).count();
        check.eq(0, replayed, "resumeB replayed nothing that resumeA had committed");
        long missing = 0;
        for (int i = RESUME_FIRST; i < RESUME_FIRST + RESUME_SECOND; i++) {
            if (!seqsB.contains(i)) {
                missing++;
            }
        }
        check.eq(0, missing, "resumeB lost nothing (resume is exact, no gap at the committed offset)");
    }

    // -------------------------------------------------- 8. offsets + seek

    private void phaseOffsetsAndSeek() {
        check.section("ListOffsets (earliest/latest), position, and an explicit seek");
        try (Consumer<byte[], byte[]> c = rawConsumer(probeGroup + "-offsets")) {
            List<PartitionInfo> parts = c.partitionsFor(mainTopic, Duration.ofSeconds(20));
            check.ok(parts != null && !parts.isEmpty(), "Metadata returned partitions for " + mainTopic
                    + " (" + (parts == null ? 0 : parts.size()) + ")");
            List<TopicPartition> tps = parts.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());

            Map<TopicPartition, Long> begin = c.beginningOffsets(tps, Duration.ofSeconds(20));
            Map<TopicPartition, Long> end = c.endOffsets(tps, Duration.ofSeconds(20));
            long span = 0;
            for (TopicPartition tp : tps) {
                span += end.getOrDefault(tp, 0L) - begin.getOrDefault(tp, 0L);
            }
            check.note("beginning: " + new TreeMap<>(stringify(begin)));
            check.note("latest:    " + new TreeMap<>(stringify(end)));
            check.eq(MAIN_COUNT, span, "sum(latest - earliest) equals what was produced");
            check.ok(begin.values().stream().allMatch(v -> v == 0L), "earliest offset is 0 on every partition");

            // seek to a mid offset on the busiest partition and prove the first record
            // we get back is exactly the one at that offset.
            TopicPartition busiest = tps.stream()
                    .max(Comparator.comparingLong(tp -> end.getOrDefault(tp, 0L))).orElseThrow();
            long target = end.get(busiest) / 2;
            c.assign(List.of(busiest));
            c.seek(busiest, target);
            check.eq(target, c.position(busiest, Duration.ofSeconds(20)), "position() after seek()");
            ConsumerRecord<byte[], byte[]> first = pollOne(c, 30_000);
            check.ok(first != null, "poll after seek returned a record");
            if (first != null) {
                check.eq(target, first.offset(), "first record after seek is the one at the requested offset");
                int seq = seqOf(first.value());
                long[] sent = mainSent.get(seq);
                check.ok(sent != null && sent[0] == busiest.partition() && sent[1] == target,
                        "record at the sought offset is the one the producer put there (seq=" + seq + ")");
            }

            c.seekToBeginning(List.of(busiest));
            check.eq(0L, c.position(busiest, Duration.ofSeconds(20)), "seekToBeginning()");
            c.seekToEnd(List.of(busiest));
            check.eq(end.get(busiest), c.position(busiest, Duration.ofSeconds(20)), "seekToEnd()");
        } catch (Exception e) {
            check.fail("offsets/seek probe threw: " + rootCause(e));
        }

        check.section("Spring ConsumerSeekAware: seekToBeginning in a fresh auto.offset.reset=latest group");
        seekListener.setSeekToBeginning(true);
        MessageListenerContainer seek = registry.getListenerContainer("seek");
        seek.start();
        boolean got = Support.await("seek listener rewinds and drains", 120_000,
                () -> distinct(recorder.lane("seek")).size() >= MAIN_COUNT);
        Support.awaitQuiescence(() -> recorder.count("seek"), 2_000, 15_000);
        seek.stop();
        int n = distinct(recorder.lane("seek")).size();
        check.ok(got, "ConsumerSeekAware.seekToBeginning rewound a latest-reset group");
        check.eq(MAIN_COUNT, n, "seek listener received the whole topic (would be 0 without the seek)");
    }

    // ------------------------------------------------------ 9. auto-create

    private void phaseAutoCreate() {
        check.section("auto-create through the stock Boot-auto-configured KafkaTemplate");
        try {
            @SuppressWarnings("unchecked")
            KafkaTemplate<Object, Object> bootTemplate =
                    (KafkaTemplate<Object, Object>) ctx.getBean("kafkaTemplate", KafkaTemplate.class);
            SendResult<Object, Object> r = bootTemplate
                    .send(autoCreateTopic, "boot-key", "boot-value-" + runId)
                    .get(60, TimeUnit.SECONDS);
            check.pass("stock kafkaTemplate bean produced to a brand-new topic "
                    + autoCreateTopic + " -> partition " + r.getRecordMetadata().partition()
                    + " offset " + r.getRecordMetadata().offset());
        } catch (Exception e) {
            check.fail("stock kafkaTemplate send to a new topic failed: " + rootCause(e));
        }
        try (Consumer<byte[], byte[]> c = rawConsumer(probeGroup + "-autocreate")) {
            List<PartitionInfo> parts = c.partitionsFor(autoCreateTopic, Duration.ofSeconds(20));
            int count = parts == null ? 0 : parts.size();
            check.ok(count >= 1, "auto-created topic is visible in Metadata with " + count + " partitions");
            check.note("QUEEN_KAFKA_DEFAULT_PARTITIONS observed as " + count);
        } catch (Exception e) {
            check.fail("metadata for the auto-created topic threw: " + rootCause(e));
        }
    }

    // ---------------------------------------- 9b. Spring's KafkaAdmin surface

    /**
     * Boot always registers a {@code KafkaAdmin} bean.  It opens no AdminClient unless
     * the application asks it to, but three Spring conveniences DO ask: NewTopic beans,
     * {@code spring.kafka.listener.missing-topics-fatal=true} (which calls
     * describeTopics), and Micrometer observation on KafkaTemplate (which calls
     * clusterId()).  This phase says exactly which of them survive.
     *
     * <p>Since M7 F1/F2 all three do.  The facade's ApiVersions now carries
     * CreateTopics, DeleteTopics, DescribeConfigs, ListGroups, DescribeGroups and
     * DeleteGroups; the one key still absent is DescribeCluster (60), and no Spring
     * convenience needs it because {@code clusterId()} is answered from Metadata.
     */
    private void phaseKafkaAdmin() {
        check.section("Spring KafkaAdmin: which admin conveniences survive the facade's API surface");
        KafkaAdmin admin = ctx.getBean(KafkaAdmin.class);
        admin.setOperationTimeout(20);

        // describeTopics is Metadata under the hood, and Metadata IS advertised (0-9).
        try {
            Map<String, TopicDescription> described = admin.describeTopics(mainTopic);
            TopicDescription d = described.get(mainTopic);
            check.ok(d != null && d.partitions().size() == 8,
                    "KafkaAdmin.describeTopics works (" + (d == null ? "null" : d.partitions().size())
                            + " partitions) - so spring.kafka.listener.missing-topics-fatal=true is usable");
        } catch (Exception e) {
            check.fail("KafkaAdmin.describeTopics failed: " + rootCause(e));
        }

        // clusterId() is what Micrometer observation calls on every KafkaTemplate.
        try {
            String clusterId = admin.clusterId();
            check.ok(clusterId != null && !clusterId.isBlank(),
                    "KafkaAdmin.clusterId() returned \"" + clusterId + "\" - observation-enabled templates are usable");
        } catch (Exception e) {
            check.note("KafkaAdmin.clusterId() failed: " + rootCause(e)
                    + " -> do NOT enable Micrometer observation on KafkaTemplate");
        }

        // createOrModifyTopics is what a NewTopic bean triggers at startup, and
        // it is the ONE line in this suite that M7 F1 turned around. Until
        // CreateTopics (key 19) was advertised, this call threw
        // UnsupportedVersionException and a Boot app that declared a NewTopic
        // bean could not START; the assertion here was that it was refused.
        // It is inverted rather than deleted because the refusal is the thing
        // every Boot user was told to design around, so the file has to state
        // in one place that they no longer have to.
        //
        // Asserting the create SUCCEEDS is not enough on its own: a facade that
        // answered error_code 0 and made nothing would pass that, so the topic
        // is described afterwards and has to be there.
        //
        // The bean asks for 3 partitions and does NOT get 3, and that is the
        // documented behaviour rather than a defect. Queen has no declared
        // per-topic width: every client sees max(live lanes,
        // QUEEN_KAFKA_DEFAULT_PARTITIONS), so a create's num_partitions is
        // accepted, not acted on, and the next Metadata reports the facade's
        // width. Spring's own contract survives that -- createOrModifyTopics
        // only ever ADDS partitions, never removes them -- so the assertion is
        // "at least what the bean asked for", which is the promise a Boot app
        // actually depends on.
        String wanted = autoCreateTopic + "-explicit";
        try {
            admin.createOrModifyTopics(TopicBuilder.name(wanted).partitions(3).replicas(1).build());
            check.pass("KafkaAdmin.createOrModifyTopics works (M7 F1 advertises CreateTopics): " + wanted);
            TopicDescription made = admin.describeTopics(wanted).get(wanted);
            int width = made == null ? -1 : made.partitions().size();
            check.ok(made != null && width >= 3,
                    "the created topic exists and is describable with " + width + " partitions, "
                            + "at least the 3 the NewTopic bean asked for");
            check.note("the bean's partitions(3) is stored as this topic's own width floor: "
                    + "the width is max(live lanes, that floor) = " + width
                    + ". Before M7 the ask was discarded and the width was "
                    + "max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)");
            check.note("consequence: a Boot app MAY declare NewTopic beans now; auto-create on first "
                    + "Metadata still works and is still the simplest option");
        } catch (Exception e) {
            check.fail("KafkaAdmin.createOrModifyTopics failed: " + rootCause(e)
                    + " - CreateTopics is advertised (v2-6) since M7 F1, so this is a regression, not the documented refusal");
        }
    }

    // -------------------------------------- 10. the footgun that M7 F3 removed

    /**
     * The stock Boot default, run on purpose.  {@code enable.idempotence} has been
     * true by default in kafka-clients since 3.0 and Boot inherits it, so before
     * M7 F3 a Spring service with NO Kafka configuration at all could not send a
     * single record: the first send died about 400ms in with
     * {@code UnsupportedVersionException: The node does not support INIT_PRODUCER_ID}
     * and the producer stayed in FATAL_ERROR for the life of the bean.
     *
     * <p>That is why this phase used to assert a FAILURE.  It now asserts the send
     * lands and carries an offset, because "it does not throw" and "the record is
     * really there" are different claims and only the second one retires the
     * footgun.  The suite's own producers still run with
     * {@code enable.idempotence=false} (application.properties) so that the rest of
     * the file keeps measuring the non-idempotent path; this phase is the one that
     * measures the default.
     */
    private void phaseIdempotenceOn() {
        check.section("stock Boot default: enable.idempotence=true (works since M7 F3)");
        Map<String, Object> props = new HashMap<>(config.producerProps("none"));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-compat-idem-" + runId);
        ProducerFactory<byte[], byte[]> pf = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<byte[], byte[]> t = new KafkaTemplate<>(pf);
        String what;
        RecordMetadata md = null;
        try {
            md = t.send(record(mainTopic, 999_999, "idem")).get(45, TimeUnit.SECONDS).getRecordMetadata();
            what = md.topic() + "-" + md.partition() + "@" + md.offset();
        } catch (Exception e) {
            what = rootCause(e);
        } finally {
            try {
                t.destroy();
            } catch (Exception ignored) {
                // best effort: on the failure path the producer may already be fatal
            }
        }
        if (check.ok(md != null, "a producer at the STOCK Boot default (enable.idempotence=true) sends: " + what)) {
            check.ok(md.hasOffset() && md.offset() >= 0,
                    "the broker assigned it a real offset (" + md.offset() + "), so InitProducerId was granted "
                            + "and the sequence window accepted the batch");
        }
        check.note("no producer property is mandatory for Spring Boot any more; "
                + "enable.idempotence=false in application.properties is this suite's own choice, "
                + "kept so the other phases measure the non-idempotent path");
    }

    // ------------------------------------------------------ 11. SASL/TLS

    private void phaseSaslTls() {
        check.section("SASL/PLAIN over TLS");
        String tlsBootstrap = env("QUEEN_KAFKA_TLS_BOOTSTRAP");
        String token = env("QUEEN_KAFKA_SASL_TOKEN");
        String truststore = env("QUEEN_KAFKA_TRUSTSTORE");
        String truststorePassword = envOr("QUEEN_KAFKA_TRUSTSTORE_PASSWORD", "changeit");
        if (tlsBootstrap.isEmpty() || token.isEmpty() || truststore.isEmpty()) {
            check.note("skipped: set QUEEN_KAFKA_TLS_BOOTSTRAP, QUEEN_KAFKA_SASL_TOKEN and "
                    + "QUEEN_KAFKA_TRUSTSTORE to exercise the SASL_SSL listener");
            return;
        }

        Map<String, Object> secure = new LinkedHashMap<>();
        secure.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        secure.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        secure.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"spring\" password=\"" + token + "\";");
        secure.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        secure.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        secure.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        // Deliberately NOT disabling hostname verification: the rig certificate carries
        // an IP SAN for 127.0.0.1, so a host-side client can verify it properly.

        Map<String, Object> pProps = new LinkedHashMap<>(config.producerProps("none"));
        pProps.putAll(secure);
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, tlsBootstrap);
        pProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-compat-tls-" + runId);
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        KafkaTemplate<byte[], byte[]> tlsTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(pProps));
        int n = 32;
        try {
            List<CompletableFuture<SendResult<byte[], byte[]>>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                futures.add(tlsTemplate.send(record(tlsTopic, i, "tls")));
            }
            tlsTemplate.flush();
            for (CompletableFuture<SendResult<byte[], byte[]>> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            check.pass("produced " + n + " records over SASL_SSL with full hostname verification");
        } catch (Exception e) {
            check.fail("SASL_SSL produce failed: " + rootCause(e));
        } finally {
            try {
                tlsTemplate.destroy();
            } catch (Exception ignored) {
                // best effort
            }
        }

        Map<String, Object> cProps = new LinkedHashMap<>(config.consumerProps("earliest"));
        cProps.putAll(secure);
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tlsBootstrap);
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, tlsGroup);
        cProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "spring-compat-tls-c-" + runId);
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        Set<Integer> seen = new TreeSet<>();
        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(cProps)) {
            c.subscribe(List.of(tlsTopic));
            long deadline = System.nanoTime() + 90_000L * 1_000_000L;
            while (System.nanoTime() < deadline && seen.size() < n) {
                ConsumerRecords<byte[], byte[]> rs = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : rs) {
                    seen.add(seqOf(r.value()));
                }
            }
            c.commitSync(Duration.ofSeconds(20));
            check.eq(n, seen.size(), "consumed the SASL_SSL records back through a group and committed");
        } catch (Exception e) {
            check.fail("SASL_SSL consume failed: " + rootCause(e));
        }

        // A wrong password must be refused, not silently accepted.
        Map<String, Object> bad = new LinkedHashMap<>(pProps);
        bad.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"spring\" password=\"definitely-not-the-token\";");
        bad.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20000);
        bad.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-compat-tls-bad-" + runId);
        KafkaTemplate<byte[], byte[]> badTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(bad));
        try {
            badTemplate.send(record(tlsTopic, 1, "tls")).get(45, TimeUnit.SECONDS);
            check.fail("a WRONG SASL password was accepted");
        } catch (Exception e) {
            String cause = rootCause(e);
            check.ok(cause.toLowerCase().contains("authentication") || cause.contains("Sasl"),
                    "wrong SASL password refused: " + cause);
        } finally {
            try {
                badTemplate.destroy();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    // --------------------------------------------------------- helpers

    private void produce(KafkaTemplate<byte[], byte[]> template, String topic, int from, int count, String tag) {
        List<CompletableFuture<SendResult<byte[], byte[]>>> futures = new ArrayList<>();
        for (int i = from; i < from + count; i++) {
            futures.add(template.send(record(topic, i, tag)));
        }
        template.flush();
        for (CompletableFuture<SendResult<byte[], byte[]>> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                check.fail("produce to " + topic + " failed: " + rootCause(e));
                return;
            }
        }
    }

    private static ProducerRecord<byte[], byte[]> record(String topic, int seq, String tag) {
        ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(topic, null, keyFor(seq), payload(seq, tag));
        r.headers().add("seq", String.valueOf(seq).getBytes(StandardCharsets.UTF_8));
        r.headers().add("bin", BIN_HEADER);
        r.headers().add("tag", tag.getBytes(StandardCharsets.UTF_8));
        return r;
    }

    private static byte[] keyFor(int seq) {
        return ("k-" + (seq % 64)).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] payload(int seq, String tag) {
        byte[] head = ("seq=" + seq + ";tag=" + tag + ";pad=" + "queen".repeat(20)).getBytes(StandardCharsets.UTF_8);
        byte[] out = Arrays.copyOf(head, head.length + BIN_TAIL.length);
        System.arraycopy(BIN_TAIL, 0, out, head.length, BIN_TAIL.length);
        return out;
    }

    private static int seqOf(byte[] value) {
        if (value == null) {
            return -1;
        }
        String s = new String(value, 0, Math.min(value.length, 40), StandardCharsets.US_ASCII);
        if (!s.startsWith("seq=")) {
            return -1;
        }
        int semi = s.indexOf(';');
        try {
            return Integer.parseInt(s.substring(4, semi));
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private static Map<String, Recorder.Got> distinct(List<Recorder.Got> got) {
        Map<String, Recorder.Got> m = new LinkedHashMap<>();
        for (Recorder.Got g : got) {
            m.putIfAbsent(g.topic + "/" + g.partition + "/" + g.offset, g);
        }
        return m;
    }

    private Consumer<byte[], byte[]> rawConsumer(String groupId) {
        return config.consumerFactory("earliest").createConsumer(groupId, "probe-" + runId);
    }

    private Map<TopicPartition, Long> committedOffsets(String groupId, String topic) {
        Map<TopicPartition, Long> out = new LinkedHashMap<>();
        try (Consumer<byte[], byte[]> c = rawConsumer(groupId)) {
            List<PartitionInfo> parts = c.partitionsFor(topic, Duration.ofSeconds(20));
            Set<TopicPartition> tps = parts.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(tps, Duration.ofSeconds(20));
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committed.entrySet()) {
                if (e.getValue() != null) {
                    out.put(e.getKey(), e.getValue().offset());
                }
            }
        } catch (Exception e) {
            check.fail("reading committed offsets threw: " + rootCause(e));
        }
        return out;
    }

    private static ConsumerRecord<byte[], byte[]> pollOne(Consumer<byte[], byte[]> c, long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            ConsumerRecords<byte[], byte[]> rs = c.poll(Duration.ofMillis(500));
            for (ConsumerRecord<byte[], byte[]> r : rs) {
                return r;
            }
        }
        return null;
    }

    private static Map<String, Long> stringify(Map<TopicPartition, Long> m) {
        Map<String, Long> out = new HashMap<>();
        m.forEach((k, v) -> out.put(k.toString(), v));
        return out;
    }

    private static String env(String name) {
        String v = System.getenv(name);
        return v == null ? "" : v.trim();
    }

    private static String envOr(String name, String fallback) {
        String v = env(name);
        return v.isEmpty() ? fallback : v;
    }

    static String rootCause(Throwable t) {
        Throwable cur = t;
        Set<Throwable> seen = new HashSet<>();
        while (cur.getCause() != null && seen.add(cur)) {
            cur = cur.getCause();
        }
        return cur.getClass().getName() + ": " + cur.getMessage();
    }
}
