// The kafka-clients VERSION MATRIX row of the compat suite: the same body of
// tests run against SEVERAL org.apache.kafka:kafka-clients releases, so the
// question it answers is not "does Java work" (compat/java already answers that
// for 3.9.1) but "does the facade's advertised window still satisfy the client
// floors as kafka-clients moves".
//
// WHY THIS EXISTS AS ITS OWN ROW. Kafka 4.0 shipped KIP-896, which RAISED the
// minimum protocol versions a client is willing to speak — 4.x clients refuse
// brokers older than 2.1. The facade deliberately caps several APIs well below
// their newest schema version (versions.rs: Fetch at v6 because v7 is fetch
// SESSIONS, Metadata at v9 because v10 is topic ids, the group APIs one below
// group_instance_id). Those two facts point at each other, and the only way to
// know whether the caps sit above or below the new floors is to run a 4.x
// client against the facade and read the negotiation off the wire. That is
// section 1 of this file, and it is the reason the file exists.
//
// WHAT IT PROVES, in order:
//   1. negotiation — the ApiVersions handshake, including the v4-then-v3
//      fallback every 4.x client performs, and the version each API actually
//      settled on ("usable"), read out of the client's OWN NetworkClient debug
//      stream rather than assumed.
//   2. bulk produce — 512 records across 8 partitions, byte keys, BINARY values
//      carrying non-UTF-8 bytes, three headers each including an EMPTY value and
//      a NULL value (two things a naive header codec collapses into one).
//   3. compression — every codec this client can encode (gzip, snappy, lz4,
//      zstd), each into its own topic, produced and read back byte-exact.
//   4. group consume — subscribe, drain, and check count, no duplicates,
//      per-partition offsets contiguous from 0, per-partition ORDER equal to the
//      produce order, and byte-exact key/value/header round-trip.
//   5. commit and resume — commitSync, close, produce more, rejoin the SAME
//      group: the committed offset must beat auto.offset.reset=earliest.
//   6. auto-create — produce to a topic no one has ever named.
//   7. offsets and seek — beginningOffsets/endOffsets/position, seek to a middle
//      offset, seekToBeginning, seekToEnd.
//
// WHAT IS THE CLIENT'S FAULT, NOT THE FACADE'S:
//   * enable.idempotence defaults to TRUE since 3.0 and is LEFT ALONE here
//     since M7 F3: InitProducerId (key 22) is advertised and the per-partition
//     sequence window is enforced (queen-kafka/src/idempotent.rs). Before F3
//     every producer in this file had to set it false or die on its first send
//     inside TransactionManager.
//   * 4.x enables KIP-714 client telemetry by default. GetTelemetrySubscriptions
//     is not advertised, so the client logs an UnsupportedVersionException and
//     carries on. It is noise, not a failure, and it is the client's own
//     retry loop making it.
//   * ApiVersions is asked for at v4 first and answered errorCode=35 with a
//     v0-encoded body; the client then retries at v3. That round trip is KIP-511
//     working as designed on both sides, not a fault.
//
// Run it through run.sh, which acquires the jars and drives every version. To
// run one version by hand:
//
//   java -cp "<jars>/*" QueenKafkaMatrix.java <bootstrap> <runId>
//
// Security for the SASL_SSL lane comes from the environment, not argv:
// QK_SECURITY_PROTOCOL, QK_SASL_MECHANISM, QK_SASL_USERNAME, QK_SASL_PASSWORD,
// QK_TRUSTSTORE, QK_TRUSTSTORE_PASSWORD, QK_DISABLE_HOSTNAME_VERIFICATION.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class QueenKafkaMatrix {

    // ---- the shape of the run -------------------------------------------
    static final int PARTITIONS = 8;
    static final int BULK       = 512;   // the test bar's "at least 500"
    static final int RESUME_N   = 64;
    static final int CODEC_N    = 40;

    static String bootstrap;
    static String run;
    static int failures = 0;
    static int checks = 0;
    static Path debugLog;

    // ---- reporting -------------------------------------------------------
    static void section(String s) { System.out.println("\n=== " + s); }
    static void ok(String m)      { checks++; System.out.println("  ok   " + m); }
    static void info(String m)    { System.out.println("  ..   " + m); }
    static void fail(String m)    { checks++; failures++; System.out.println("  FAIL " + m); }
    static void check(boolean c, String m) { if (c) ok(m); else fail(m); }

    public static void main(String[] args) throws Exception {
        // slf4j-simple reads these once, lazily, when the first Kafka class asks
        // for a logger. Setting them here — before any org.apache.kafka type is
        // touched — is what lets this file capture its own negotiation without
        // depending on how it was launched.
        debugLog = Files.createTempFile("qk-java-matrix-", ".log");
        System.setProperty("org.slf4j.simpleLogger.logFile", debugLog.toString());
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.NetworkClient", "debug");

        bootstrap = args.length > 0 ? args[0] : "127.0.0.1:19092";
        run       = args.length > 1 ? args[1] : String.valueOf(System.currentTimeMillis());

        System.out.println("kafka-clients " + clientVersion() + " (commit " + clientCommit() + ")");
        System.out.println("bootstrap " + bootstrap + "   runId " + run
            + "   security " + env("QK_SECURITY_PROTOCOL", "PLAINTEXT"));
        System.out.println("java " + System.getProperty("java.version")
            + " on " + System.getProperty("os.arch"));

        stage("negotiation",      QueenKafkaMatrix::negotiation);
        stage("bulk produce",     QueenKafkaMatrix::bulkProduceAndConsume);
        stage("compression",      QueenKafkaMatrix::compression);
        stage("commit and resume",QueenKafkaMatrix::commitAndResume);
        stage("auto-create",      QueenKafkaMatrix::autoCreate);
        stage("offsets and seek", QueenKafkaMatrix::offsetsAndSeek);

        System.out.println("\n" + checks + " check(s) run");
        System.out.println("RESULT: " + (failures == 0 ? "PASS" : "FAIL (" + failures + ")"));
        try { Files.deleteIfExists(debugLog); } catch (IOException ignored) {}
        System.exit(failures == 0 ? 0 : 1);
    }

    interface Stage { void go() throws Exception; }

    // Every stage is fenced: a throw is one FAIL and the run continues, because
    // "the client died here" is a result the next stage should not hide.
    static void stage(String name, Stage s) {
        section(name);
        long t0 = System.currentTimeMillis();
        try {
            s.go();
        } catch (Throwable t) {
            fail(name + " threw after " + (System.currentTimeMillis() - t0) + "ms: " + t);
            t.printStackTrace(System.out);
        }
    }

    static String clientVersion() {
        try { return org.apache.kafka.common.utils.AppInfoParser.getVersion(); }
        catch (Throwable t) { return "unknown"; }
    }
    static String clientCommit() {
        try { return org.apache.kafka.common.utils.AppInfoParser.getCommitId(); }
        catch (Throwable t) { return "unknown"; }
    }

    static String env(String k, String dflt) {
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    // ---- config ----------------------------------------------------------

    // Anything the SASL_SSL lane needs. Absent from a plaintext run, so the
    // plaintext and TLS lanes are the same code path with the same assertions.
    static void security(Properties p) {
        String proto = env("QK_SECURITY_PROTOCOL", "");
        if (proto.isEmpty()) return;
        p.put("security.protocol", proto);
        if (proto.contains("SASL")) {
            String mech = env("QK_SASL_MECHANISM", "PLAIN");
            p.put("sasl.mechanism", mech);
            p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + env("QK_SASL_USERNAME", "queen") + "\" password=\""
                + env("QK_SASL_PASSWORD", "") + "\";");
        }
        if (proto.contains("SSL")) {
            String ts = env("QK_TRUSTSTORE", "");
            if (!ts.isEmpty()) {
                p.put("ssl.truststore.location", ts);
                p.put("ssl.truststore.type", ts.endsWith(".p12") ? "PKCS12" : "JKS");
                p.put("ssl.truststore.password", env("QK_TRUSTSTORE_PASSWORD", "changeit"));
            }
            if (!env("QK_DISABLE_HOSTNAME_VERIFICATION", "").isEmpty()) {
                p.put("ssl.endpoint.identification.algorithm", "");
            }
        }
    }

    static Properties producerProps(String acks, String compression) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, acks);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        // enable.idempotence is left at the client's OWN DEFAULT (true since
        // 3.0), and that is the M7 F3 acceptance: the whole scored suite below
        // runs on a producer configured exactly as a user would leave it. Until
        // F3 this line read `ENABLE_IDEMPOTENCE_CONFIG, "false"` and was
        // documented as "THE mandatory knob".
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        security(p);
        return p;
    }

    static Properties consumerProps(String groupId) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        if (groupId != null) c.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Inside the facade's window (6000..300000) or it answers
        // INVALID_SESSION_TIMEOUT (26).
        c.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        security(c);
        return c;
    }

    // ---- 1. negotiation --------------------------------------------------

    // The one section that reads the client's debug stream. Everything here is
    // what the CLIENT decided, not what the facade claims.
    static void negotiation() throws Exception {
        // Any call that forces a connection will do; partitionsFor is the
        // cheapest that also proves Metadata answered.
        List<PartitionInfo> parts;
        try (Producer<byte[], byte[]> p = new KafkaProducer<>(producerProps("all", "none"))) {
            parts = p.partitionsFor("neg-" + run);
        }
        check(parts.size() == PARTITIONS,
            "Metadata auto-created neg-" + run + " with " + parts.size() + " partitions");

        List<String> log = Files.readAllLines(debugLog, StandardCharsets.UTF_8);

        // KIP-511: a 4.x client opens with ApiVersions v4. The facade advertises
        // 0..=3, so it must answer UNSUPPORTED_VERSION (35) with a v0-encoded
        // body, and the client must then retry at v3. Both halves are asserted
        // because either one alone is a silently broken handshake.
        boolean askedV4 = log.stream().anyMatch(l ->
            l.contains("Sending API_VERSIONS request") && l.contains("apiVersion=4"));
        boolean refusedV4 = log.stream().anyMatch(l ->
            l.contains("Received API_VERSIONS response") && l.contains("apiVersion=4")
            && l.contains("errorCode=35"));
        boolean retriedV3 = log.stream().anyMatch(l ->
            l.contains("Sending API_VERSIONS request") && l.contains("apiVersion=3"));
        if (askedV4) {
            info("the client opened with ApiVersions v4 (KIP-511 probe)");
            check(refusedV4, "the facade answered v4 with errorCode=35 and a v0 body");
            check(retriedV3, "the client fell back to ApiVersions v3 and got the table");
        } else {
            info("the client opened at ApiVersions v3 or below; no v4 probe to check");
            check(retriedV3, "the client negotiated ApiVersions v3");
        }

        Map<String, int[]> table = parseApiVersions(log);   // name -> {min, max, usable}
        Set<String> unsupported = parseUnsupported(log);
        check(!table.isEmpty(), "the client recorded an API version table (" + table.size() + " advertised)");

        List<String> rows = new ArrayList<>(table.keySet());
        Collections.sort(rows);
        for (String r : rows) {
            int[] v = table.get(r);
            info(String.format("negotiated %-16s %d..%d  usable %d", r, v[0], v[1], v[2]));
        }

        // THE assertions this whole file exists for: the facade caps and the
        // client floors have to overlap, and the overlap has to be the cap.
        expectUsable(table, "Produce",         3, 9, 9);
        expectUsable(table, "Fetch",           4, 6, 6);   // v7 is fetch sessions
        expectUsable(table, "ListOffsets",     1, 5, 5);
        expectUsable(table, "Metadata",        0, 9, 9);   // v10 is topic ids
        expectUsable(table, "OffsetCommit",    2, 6, 6);
        expectUsable(table, "OffsetFetch",     1, 7, 7);
        expectUsable(table, "FindCoordinator", 0, 3, 3);
        expectUsable(table, "JoinGroup",       0, 4, 4);   // v5 is static membership
        expectUsable(table, "SyncGroup",       0, 2, 2);
        expectUsable(table, "Heartbeat",       0, 2, 2);
        expectUsable(table, "LeaveGroup",      0, 2, 2);
        expectUsable(table, "ApiVersions",     0, 3, 3);

        // M7 F3 moved InitProducerId out of the holes and into the window
        // above: key 22 is advertised 0..=4 and negotiated to v4 by this
        // client. It is asserted as USABLE rather than deleted, because the
        // whole point of the row is that a default producer negotiates it.
        expectUsable(table, "InitProducerId", 0, 4, 4);  // v5 is KIP-890 txn v2

        // M7 F1 and F2 turned the five deliberate holes below into rows. Until
        // they landed, this block asserted that CreateTopics, DeleteTopics,
        // DescribeConfigs, ListGroups and DescribeGroups were seen as
        // UNSUPPORTED — an AdminClient built on any of them could not open at
        // all. The assertion is INVERTED rather than deleted, because "the
        // client can see it" and "the client settled on the version the facade
        // caps at" are two different claims and the second one is what a
        // raised client floor (KIP-896) would break first.
        expectUsable(table, "CreateTopics",    2, 6, 6);   // v7 answers a topic id
        expectUsable(table, "DeleteTopics",    1, 5, 5);   // v6 names topics by id
        expectUsable(table, "DescribeConfigs", 1, 4, 4);   // the whole schema
        expectUsable(table, "ListGroups",      0, 4, 4);   // v5 is KIP-848's group_type
        expectUsable(table, "DescribeGroups",  0, 3, 3);   // v4 is group_instance_id
        expectUsable(table, "DeleteGroups",    0, 2, 2);   // the whole schema

        // ...and the other half of the inversion: none of them may still be
        // sitting in the client's UNSUPPORTED list. A client that recorded a
        // key as both negotiated and unsupported would mean the table above was
        // read from the wrong connection.
        for (String arrived : List.of("CreateTopics", "DeleteTopics", "DescribeConfigs",
                                      "ListGroups", "DescribeGroups", "DeleteGroups")) {
            check(!unsupported.contains(arrived),
                arrived + " is no longer in the client's unsupported list (M7 advertises it)");
        }

        // KIP-848 and KIP-714 are the two 4.x defaults that could have made the
        // facade unusable. Neither is advertised; both must be seen as absent.
        for (String modern : List.of("ConsumerGroupHeartbeat", "GetTelemetrySubscriptions")) {
            if (table.containsKey(modern)) {
                fail(modern + " was negotiated, which this facade cannot honour");
            } else {
                ok(modern + " is absent from the table (this client build knows it: "
                    + (unsupported.contains(modern) ? "yes" : "not in this client version") + ")");
            }
        }
    }

    static void expectUsable(Map<String, int[]> table, String api, int min, int max, int usable) {
        int[] v = table.get(api);
        if (v == null) { fail(api + " was not advertised to the client at all"); return; }
        check(v[0] == min && v[1] == max,
            api + " advertised " + v[0] + ".." + v[1] + " as versions.rs says (" + min + ".." + max + ")");
        check(v[2] == usable,
            api + " settled on v" + v[2] + ", the top of the advertised window");
    }

    // "Produce(0): 3 to 9 [usable: 9], Fetch(1): 4 to 6 [usable: 6], ..."
    static Map<String, int[]> parseApiVersions(List<String> log) {
        Map<String, int[]> out = new LinkedHashMap<>();
        for (String line : log) {
            int i = line.indexOf("API versions: (");
            if (i < 0) continue;
            for (String piece : line.substring(i).split(", ")) {
                java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("([A-Za-z]+)\\((\\d+)\\): (\\d+) to (\\d+) \\[usable: (\\d+)\\]")
                    .matcher(piece);
                if (m.find()) {
                    out.put(m.group(1), new int[]{
                        Integer.parseInt(m.group(3)),
                        Integer.parseInt(m.group(4)),
                        Integer.parseInt(m.group(5))});
                }
            }
            if (!out.isEmpty()) return out;
        }
        return out;
    }

    static Set<String> parseUnsupported(List<String> log) {
        Set<String> out = new TreeSet<>();
        for (String line : log) {
            int i = line.indexOf("API versions: (");
            if (i < 0) continue;
            java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("([A-Za-z]+)\\(\\d+\\): UNSUPPORTED").matcher(line.substring(i));
            while (m.find()) out.add(m.group(1));
            if (!out.isEmpty()) return out;
        }
        return out;
    }

    // ---- test data -------------------------------------------------------

    static byte[] key(int i)   { return String.format("k%04d", i).getBytes(StandardCharsets.UTF_8); }

    // A value that is NOT valid UTF-8 and contains a NUL, so "byte-exact" means
    // byte-exact rather than "survived a string round trip".
    static byte[] value(int i) {
        byte[] tag = ("v" + i + "|").getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[tag.length + 5];
        System.arraycopy(tag, 0, out, 0, tag.length);
        out[tag.length]     = (byte) 0x00;
        out[tag.length + 1] = (byte) 0xff;
        out[tag.length + 2] = (byte) 0xfe;
        out[tag.length + 3] = (byte) (i & 0xff);
        out[tag.length + 4] = (byte) 0x80;
        return out;
    }

    static List<Header> headers(int i) {
        return List.of(
            new RecordHeader("seq",   String.valueOf(i).getBytes(StandardCharsets.UTF_8)),
            new RecordHeader("empty", new byte[0]),      // empty value
            new RecordHeader("nullv", (byte[]) null));   // null value: NOT the same thing
    }

    static String describeHeaders(org.apache.kafka.common.header.Headers hs) {
        List<String> out = new ArrayList<>();
        for (Header h : hs) {
            out.add(h.key() + "=" + (h.value() == null ? "<null>"
                : "[" + h.value().length + "]" + new String(h.value(), StandardCharsets.UTF_8)));
        }
        return String.join(" ", out);
    }

    // ---- 2. bulk produce + 4. group consume ------------------------------

    static void bulkProduceAndConsume() throws Exception {
        String topic = "jm-bulk-" + run;
        String group = "jm-bulk-g-" + run;

        Map<Integer, List<Integer>> producedOrder = new TreeMap<>();   // partition -> seq in send order
        Map<Integer, Long> firstOffset = new TreeMap<>();
        long t0 = System.currentTimeMillis();
        List<java.util.concurrent.Future<RecordMetadata>> sent = new ArrayList<>();
        try (Producer<byte[], byte[]> p = new KafkaProducer<>(producerProps("all", "none"))) {
            for (int i = 0; i < BULK; i++) {
                int part = i % PARTITIONS;
                producedOrder.computeIfAbsent(part, k -> new ArrayList<>()).add(i);
                sent.add(p.send(new ProducerRecord<>(topic, part, key(i), value(i), headers(i))));
            }
            p.flush();
            for (int i = 0; i < BULK; i++) {
                RecordMetadata md = sent.get(i).get();
                firstOffset.putIfAbsent(md.partition(), md.offset());
                if (md.offset() < 0) { fail("record " + i + " came back with offset " + md.offset()); return; }
            }
        }
        long ms = System.currentTimeMillis() - t0;
        ok(BULK + " records acked across " + producedOrder.size() + " partitions in " + ms + "ms");
        check(producedOrder.size() == PARTITIONS, "the send spread over all " + PARTITIONS + " partitions");
        check(firstOffset.values().stream().allMatch(o -> o == 0L),
            "every partition started at offset 0 on a fresh topic " + firstOffset);

        // Drain it through a GROUP. subscribe() means JoinGroup/SyncGroup and a
        // 3s initial rebalance delay, which is the point.
        Map<Integer, List<Integer>> readOrder = new TreeMap<>();
        Map<Integer, List<Long>> readOffsets = new TreeMap<>();
        Map<String, byte[]> byKey = new HashMap<>();
        Map<String, String> headerByKey = new HashMap<>();
        int total = 0;
        long joinStart = System.currentTimeMillis();
        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(group))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 120_000;
            while (total < BULK && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
                if (total == 0 && recs.count() > 0) {
                    info("first records after " + (System.currentTimeMillis() - joinStart)
                        + "ms (includes the 3s group.initial.rebalance.delay.ms)");
                }
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    total++;
                    String k = new String(r.key(), StandardCharsets.UTF_8);
                    byKey.put(k, r.value());
                    headerByKey.put(k, describeHeaders(r.headers()));
                    readOrder.computeIfAbsent(r.partition(), x -> new ArrayList<>())
                        .add(Integer.parseInt(k.substring(1)));
                    readOffsets.computeIfAbsent(r.partition(), x -> new ArrayList<>()).add(r.offset());
                }
            }
            check(total == BULK, "the group read every record (" + total + "/" + BULK + ")");
            check(byKey.size() == BULK, "no duplicates and no losses (" + byKey.size() + " distinct keys)");

            // byte-exact, including the non-UTF-8 tail
            int mismatched = 0;
            for (int i = 0; i < BULK; i++) {
                byte[] got = byKey.get(new String(key(i), StandardCharsets.UTF_8));
                if (got == null || !Arrays.equals(got, value(i))) mismatched++;
            }
            check(mismatched == 0, "every value came back byte-exact including 0x00/0xff/0x80 ("
                + mismatched + " mismatched)");

            String h0 = headerByKey.get("k0000");
            info("headers on k0000: " + h0);
            check("seq=[1]0 empty=[0] nullv=<null>".equals(h0),
                "headers survive in order with EMPTY and NULL values kept apart");
            long badHeaders = headerByKey.values().stream()
                .filter(h -> !h.endsWith("empty=[0] nullv=<null>")).count();
            check(badHeaders == 0, "all " + BULK + " records kept their header triple (" + badHeaders + " wrong)");

            // per-partition order and offset contiguity
            int outOfOrder = 0, notContiguous = 0;
            for (int part : producedOrder.keySet()) {
                if (!producedOrder.get(part).equals(readOrder.get(part))) outOfOrder++;
                List<Long> offs = readOffsets.getOrDefault(part, List.of());
                for (int i = 0; i < offs.size(); i++) if (offs.get(i) != (long) i) { notContiguous++; break; }
            }
            check(outOfOrder == 0, "every partition came back in produce order (" + outOfOrder + " scrambled)");
            check(notContiguous == 0, "every partition's offsets are 0..n-1 contiguous ("
                + notContiguous + " gapped)");
            info("partition sizes " + readOffsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size(), (a,b)->a, TreeMap::new)));

            c.commitSync();
            ok("commitSync() returned");
            Set<TopicPartition> tps = new LinkedHashSet<>();
            for (int i = 0; i < PARTITIONS; i++) tps.add(new TopicPartition(topic, i));
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(tps);
            long sum = committed.values().stream().filter(Objects::nonNull)
                .mapToLong(OffsetAndMetadata::offset).sum();
            check(sum == BULK, "OffsetFetch reads back commits summing to " + BULK + " (got " + sum + ")");
        }
    }

    // ---- 3. compression --------------------------------------------------

    // One topic per codec, and assign() rather than subscribe() so the check is
    // about the codec and not about four more 3s rebalances.
    static void compression() throws Exception {
        for (String codec : List.of("gzip", "snappy", "lz4", "zstd")) {
            String topic = "jm-" + codec + "-" + run;
            long t0 = System.currentTimeMillis();
            try (Producer<byte[], byte[]> p = new KafkaProducer<>(producerProps("all", codec))) {
                List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
                for (int i = 0; i < CODEC_N; i++)
                    fs.add(p.send(new ProducerRecord<>(topic, i % PARTITIONS, key(i), value(i), headers(i))));
                p.flush();
                for (var f : fs) f.get();
            } catch (Throwable t) {
                fail(codec + " producer failed: " + t);
                continue;
            }
            ok(codec + " produced " + CODEC_N + " records in " + (System.currentTimeMillis() - t0) + "ms");

            Map<String, byte[]> got = new HashMap<>();
            try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(null))) {
                List<TopicPartition> tps = new ArrayList<>();
                for (int i = 0; i < PARTITIONS; i++) tps.add(new TopicPartition(topic, i));
                c.assign(tps);
                c.seekToBeginning(tps);
                long deadline = System.currentTimeMillis() + 60_000;
                while (got.size() < CODEC_N && System.currentTimeMillis() < deadline)
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500)))
                        got.put(new String(r.key(), StandardCharsets.UTF_8), r.value());
            }
            int bad = 0;
            for (int i = 0; i < CODEC_N; i++) {
                byte[] v = got.get(new String(key(i), StandardCharsets.UTF_8));
                if (v == null || !Arrays.equals(v, value(i))) bad++;
            }
            check(got.size() == CODEC_N && bad == 0,
                codec + " round-tripped all " + CODEC_N + " byte-exact (read " + got.size()
                + ", " + bad + " wrong)");
        }
    }

    // ---- 5. commit and resume -------------------------------------------

    static void commitAndResume() throws Exception {
        String topic = "jm-res-" + run;
        String group = "jm-res-g-" + run;

        produce(topic, 0, RESUME_N, "a");
        int first = drainAndCommit(topic, group, RESUME_N);
        check(first == RESUME_N, "the first member read " + RESUME_N + " and committed (" + first + ")");

        produce(topic, RESUME_N, RESUME_N, "b");

        // A NEW consumer object in the SAME group. The committed offset has to
        // beat auto.offset.reset=earliest, or the 'a' batch comes back.
        List<String> second = new ArrayList<>();
        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(group))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 90_000;
            while (second.size() < RESUME_N && System.currentTimeMillis() < deadline)
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500)))
                    second.add(new String(r.value(), StandardCharsets.UTF_8).substring(0, 1));
            // keep polling briefly to catch a WRONG redelivery of the first batch
            long extra = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < extra)
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500)))
                    second.add(new String(r.value(), StandardCharsets.UTF_8).substring(0, 1));
        }
        long replayed = second.stream().filter(s -> s.equals("a")).count();
        long fresh    = second.stream().filter(s -> s.equals("b")).count();
        info("the restarted member read " + second.size() + " records: " + fresh + " new, " + replayed + " replayed");
        check(replayed == 0, "the committed offset won over auto.offset.reset=earliest (0 replays)");
        check(fresh == RESUME_N, "it read exactly the " + RESUME_N + " new records");
    }

    static void produce(String topic, int from, int n, String tag) throws Exception {
        try (Producer<byte[], byte[]> p = new KafkaProducer<>(producerProps("all", "none"))) {
            List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
            for (int i = from; i < from + n; i++)
                fs.add(p.send(new ProducerRecord<>(topic, i % PARTITIONS, key(i),
                    (tag + i).getBytes(StandardCharsets.UTF_8))));
            p.flush();
            for (var f : fs) f.get();
        }
    }

    static int drainAndCommit(String topic, String group, int expect) {
        int seen = 0;
        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(group))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 90_000;
            while (seen < expect && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
                seen += recs.count();
                if (recs.count() > 0) c.commitSync();
            }
        }
        return seen;
    }

    // ---- 6. auto-create --------------------------------------------------

    static void autoCreate() throws Exception {
        String topic = "jm-auto-" + run;   // never named before this line
        RecordMetadata md;
        long t0 = System.currentTimeMillis();
        try (Producer<byte[], byte[]> p = new KafkaProducer<>(producerProps("all", "none"))) {
            md = p.send(new ProducerRecord<>(topic, 3, key(1), value(1), headers(1))).get();
        }
        check(md.offset() == 0 && md.partition() == 3,
            "a send to a topic that did not exist landed at p3/offset0 in "
            + (System.currentTimeMillis() - t0) + "ms");
        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(null))) {
            List<PartitionInfo> parts = c.partitionsFor(topic);
            check(parts.size() == PARTITIONS,
                "the auto-created topic has QUEEN_KAFKA_DEFAULT_PARTITIONS=" + PARTITIONS
                + " partitions (got " + parts.size() + ")");
        }
    }

    // ---- 7. offsets and seek --------------------------------------------

    static void offsetsAndSeek() throws Exception {
        String topic = "jm-seek-" + run;
        int n = 80;
        produce(topic, 0, n, "s");

        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) tps.add(new TopicPartition(topic, i));

        try (Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(null))) {
            // ListOffsets, the API a wrong error code kills the Java client on.
            Map<TopicPartition, Long> begin = c.beginningOffsets(tps);
            Map<TopicPartition, Long> end   = c.endOffsets(tps);
            long total = end.values().stream().mapToLong(Long::longValue).sum();
            info("beginningOffsets " + byPartition(begin));
            info("endOffsets       " + byPartition(end));
            check(total == n, "endOffsets sum to the " + n + " records produced (" + total + ")");
            check(begin.values().stream().allMatch(v -> v == 0L), "beginningOffsets are all 0");

            c.assign(tps);

            // seek to a middle offset on one partition and read the tail
            TopicPartition tp = tps.get(0);
            long endTp = end.get(tp);
            long mid = endTp / 2;
            c.seek(tp, mid);
            check(c.position(tp) == mid, "position() reports the seek target " + mid);
            List<Long> tail = new ArrayList<>();
            c.assign(List.of(tp));
            c.seek(tp, mid);
            long deadline = System.currentTimeMillis() + 30_000;
            while (tail.size() < endTp - mid && System.currentTimeMillis() < deadline)
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) tail.add(r.offset());
            check(tail.size() == endTp - mid && (tail.isEmpty() || tail.get(0) == mid),
                "seek(" + mid + ") returned exactly the tail " + tail.size() + "/" + (endTp - mid)
                + " starting at " + (tail.isEmpty() ? "-" : tail.get(0)));

            c.seekToEnd(List.of(tp));
            check(c.position(tp) == endTp, "seekToEnd lands on the high watermark " + endTp);
            c.seekToBeginning(List.of(tp));
            check(c.position(tp) == 0L, "seekToBeginning lands on 0");
        }
    }

    static Map<Integer, Long> byPartition(Map<TopicPartition, Long> in) {
        Map<Integer, Long> out = new TreeMap<>();
        in.forEach((k, v) -> out.put(k.partition(), v));
        return out;
    }
}
