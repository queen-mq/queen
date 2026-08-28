// The Java half of the M6 client matrix: the official org.apache.kafka
// kafka-clients against the compat rig (compat/rig.sh, 19092).
//
// Run it in single-file source mode, no build tool:
//
//   java -cp "<libdir>/*" \
//     -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
//     -Dorg.slf4j.simpleLogger.log.org.apache.kafka.clients.NetworkClient=debug \
//     QueenKafkaCompat.java <bootstrap> <runId>
//
// The NetworkClient debug line is deliberate: it prints "Recorded API versions
// for node …" with the version the client picked as usable for every API, which
// is the only honest record of what was negotiated.
//
// The Java consumer is the strict one — an error code outside the set it expects
// for a request is not retried, it is thrown out of poll() and kills the
// application. That is exactly why it is in the matrix.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class QueenKafkaCompat {
    static String bootstrap;
    static String run;
    static int failures = 0;

    static void ok(String msg) { System.out.println("  ok   " + msg); }
    static void info(String msg) { System.out.println("  ..   " + msg); }
    static void check(boolean cond, String msg) {
        if (cond) ok(msg);
        else { failures++; System.out.println("  FAIL " + msg); }
    }

    public static void main(String[] args) throws Exception {
        bootstrap = args.length > 0 ? args[0] : "127.0.0.1:19092";
        run = args.length > 1 ? args[1] : String.valueOf(System.currentTimeMillis());

        try {
            produceAndConsume();
        } catch (Throwable t) {
            failures++;
            System.out.println("  FAIL produceAndConsume threw: " + t);
            t.printStackTrace(System.out);
        }
        try {
            commitAndResume();
        } catch (Throwable t) {
            failures++;
            System.out.println("  FAIL commitAndResume threw: " + t);
            t.printStackTrace(System.out);
        }
        System.out.println("\nRESULT: " + (failures == 0 ? "PASS" : "FAIL (" + failures + " check(s))"));
        System.exit(failures == 0 ? 0 : 1);
    }

    static Properties producerProps(String acks, String compression) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, acks);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        // The facade implements no InitProducerId: the idempotent producer that
        // is the default since 3.0 has to be turned off, exactly as franz-go
        // needs DisableIdempotentWrite.
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        return p;
    }

    static Properties consumerProps(String groupId, boolean autoCommit) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        c.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        return c;
    }

    // Produce with acks=all and with gzip, then read every record back through a
    // consumer group: keys, values, headers and offsets.
    static void produceAndConsume() throws Exception {
        String topic = "java-rt-" + run;
        int n = 24;
        Map<String, String> want = new LinkedHashMap<>();
        try (Producer<String, String> p = new KafkaProducer<>(producerProps("all", "none"))) {
            for (int i = 0; i < n; i++) {
                List<Header> hs = List.of(
                    new RecordHeader("trace", ("t" + i).getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader("empty", new byte[0]),
                    new RecordHeader("nullv", null));
                ProducerRecord<String, String> rec = new ProducerRecord<>(
                    topic, i % 8, "k" + i, "v" + i, hs);
                RecordMetadata md = p.send(rec).get();
                if (i < 3) info("acks=all record " + i + " -> partition " + md.partition()
                    + " offset " + md.offset() + " timestamp " + md.timestamp());
                check(md.offset() >= 0, "record " + i + " got a real offset (" + md.offset() + ")");
                want.put("k" + i, "v" + i);
            }
        }
        try (Producer<String, String> p = new KafkaProducer<>(producerProps("all", "gzip"))) {
            for (int i = n; i < n + 8; i++) {
                RecordMetadata md = p.send(new ProducerRecord<>(topic, i % 8, "k" + i, "v" + i)).get();
                check(md.offset() >= 0, "gzip record " + i + " offset " + md.offset());
                want.put("k" + i, "v" + i);
            }
        }

        Map<String, String> got = new LinkedHashMap<>();
        Map<String, List<String>> headers = new LinkedHashMap<>();
        Set<Integer> parts = new TreeSet<>();
        try (Consumer<String, String> c = new KafkaConsumer<>(consumerProps("java-g-" + run, false))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 120000;
            while (got.size() < want.size() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : recs) {
                    got.put(r.key(), r.value());
                    parts.add(r.partition());
                    List<String> hs = new ArrayList<>();
                    for (Header h : r.headers())
                        hs.add(h.key() + "=" + (h.value() == null ? "<null>" : new String(h.value(), StandardCharsets.UTF_8)));
                    headers.put(r.key(), hs);
                }
            }
            check(got.size() == want.size(), "the group read every record (" + got.size() + "/" + want.size() + ")");
            check(got.equals(want), "every value matches its key");
            check(parts.size() == 8, "every partition was read " + parts);
            List<String> h0 = headers.getOrDefault("k0", List.of());
            info("headers on k0: " + h0);
            check(h0.equals(List.of("trace=t0", "empty=", "nullv=<null>")),
                "headers survive in order, empty and null values distinct: " + h0);
            c.commitSync();
            ok("commitSync() returned");
            Map<TopicPartition, OffsetAndMetadata> committed =
                c.committed(new HashSet<>(List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1))));
            info("committed after commitSync: " + committed);
            check(committed.get(new TopicPartition(topic, 0)) != null
                && committed.get(new TopicPartition(topic, 0)).offset() > 0,
                "OffsetFetch reads the commit back for partition 0");
        }

        // endOffsets / beginningOffsets go through ListOffsets, which is where a
        // wrong error code kills the Java client outright.
        try (Consumer<String, String> c = new KafkaConsumer<>(consumerProps("java-bounds-" + run, false))) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int i = 0; i < 8; i++) tps.add(new TopicPartition(topic, i));
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            Map<TopicPartition, Long> begin = c.beginningOffsets(tps);
            long total = end.values().stream().mapToLong(Long::longValue).sum();
            info("beginningOffsets " + new TreeMap<>(mapKeys(begin)) + " endOffsets " + new TreeMap<>(mapKeys(end)));
            check(total == want.size(), "endOffsets sum to the records produced (" + total + ")");
            check(begin.values().stream().allMatch(v -> v == 0L), "beginningOffsets are all 0");
        }
    }

    static Map<Integer, Long> mapKeys(Map<TopicPartition, Long> in) {
        Map<Integer, Long> out = new TreeMap<>();
        in.forEach((k, v) -> out.put(k.partition(), v));
        return out;
    }

    // A group that commits, closes, and comes back: the committed offset has to
    // win over auto.offset.reset=earliest.
    static void commitAndResume() throws Exception {
        String topic = "java-res-" + run;
        String group = "java-res-g-" + run;
        try (Producer<String, String> p = new KafkaProducer<>(producerProps("all", "none"))) {
            for (int i = 0; i < 16; i++) p.send(new ProducerRecord<>(topic, i % 8, "a" + i, "a" + i)).get();
        }
        int firstRead = drain(topic, group, 16, true);
        check(firstRead == 16, "first consumer read 16 and committed (" + firstRead + ")");

        try (Producer<String, String> p = new KafkaProducer<>(producerProps("all", "none"))) {
            for (int i = 0; i < 8; i++) p.send(new ProducerRecord<>(topic, i % 8, "b" + i, "b" + i)).get();
        }
        List<String> second = new ArrayList<>();
        try (Consumer<String, String> c = new KafkaConsumer<>(consumerProps(group, false))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 60000;
            while (second.size() < 8 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) second.add(r.key());
            }
            // Poll a little longer to catch a wrong redelivery of the first 16.
            long extra = System.currentTimeMillis() + 4000;
            while (System.currentTimeMillis() < extra) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) second.add(r.key());
            }
        }
        long replayed = second.stream().filter(k -> k.startsWith("a")).count();
        info("restarted consumer read " + second.size() + " records: " + second);
        check(replayed == 0, "the restarted consumer replayed nothing (" + replayed + " old records)");
        check(second.size() == 8, "the restarted consumer read exactly the 8 new records");
    }

    static int drain(String topic, String group, int expect, boolean commit) {
        int seen = 0;
        try (Consumer<String, String> c = new KafkaConsumer<>(consumerProps(group, false))) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 120000;
            while (seen < expect && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                seen += recs.count();
                if (commit && recs.count() > 0) c.commitSync();
            }
        }
        return seen;
    }
}
