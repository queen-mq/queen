// The kafka-clients configurations the facade deliberately does NOT support,
// run against it on purpose to find out HOW each one fails. Nothing here is a
// pass/fail assertion about the facade: what is being recorded is whether a
// user who hits one of these gets a fast, legible error or a hang.
//
// This is the 4.x-aware sibling of compat/java/QueenKafkaEdges.java. Kafka 4.0
// changed which of these a user reaches BY DEFAULT, which is why the same probes
// have to be re-run per client version rather than answered once:
//
//   1. the DEFAULT producer — enable.idempotence has been true since 3.0. This
//      is the single most likely first contact between a real Java app and the
//      facade, and since M7 F3 it is no longer an edge: InitProducerId (key 22)
//      is advertised and the sequence window is enforced, so the probe records a
//      SEND. It stays in this file because "the default producer works" is the
//      claim most worth re-measuring on every client version.
//   2. group.protocol=consumer — KIP-848, GA in 4.0 and explicitly a non-goal
//      here (ConsumerGroupHeartbeat is not advertised).
//   3. AdminClient — 4.x AdminClient over a broker with no CreateTopics,
//      DescribeCluster, ListGroups or DescribeConfigs. listTopics() rides
//      Metadata and should work; the rest should refuse fast.
//   4. KIP-714 client telemetry — on by default in 4.x. It must be NOISE, not a
//      failure: a client that cannot report metrics still has to produce.
//   5. a transactional producer — transactional.id set, which cannot work and
//      must say so before any record is accepted. MEASURED 2026-08-29, and the
//      measurement is the point: it still takes the whole of max.block.ms. The
//      client never reaches InitProducerId (which refuses in ~10 ms with
//      TRANSACTIONAL_ID_AUTHORIZATION_FAILED) because it asks FindCoordinator
//      for a TRANSACTION coordinator first, and the facade answers that
//      COORDINATOR_NOT_AVAILABLE — a RETRIABLE code the client loops on ~190
//      times. Advertising key 22 did not change this and was never going to.
//
// Run through run.sh, or by hand:
//   java -cp "<jars>/*" QueenKafkaEdges4x.java <bootstrap> <runId>
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class QueenKafkaEdges4x {
    static String bootstrap, run;

    static void section(String s) { System.out.println("\n=== " + s); }
    static void out(String s)     { System.out.println("  " + s); }

    // Every probe is time-boxed and its wall clock is printed, because "it
    // failed" and "it hung for two minutes and then failed" are different
    // answers for whoever is debugging a real app.
    static void probe(String name, Runnable body) {
        section(name);
        long t0 = System.currentTimeMillis();
        try {
            body.run();
            out("finished in " + (System.currentTimeMillis() - t0) + "ms with no throw");
        } catch (Throwable t) {
            Throwable root = t;
            while (root.getCause() != null && root.getCause() != root) root = root.getCause();
            out("after " + (System.currentTimeMillis() - t0) + "ms it failed with:");
            out("    " + t.getClass().getName() + ": " + t.getMessage());
            if (root != t) out("    root cause: " + root.getClass().getName() + ": " + root.getMessage());
        }
    }

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        bootstrap = args.length > 0 ? args[0] : "127.0.0.1:19092";
        run       = args.length > 1 ? args[1] : String.valueOf(System.currentTimeMillis());
        System.out.println("kafka-clients " + org.apache.kafka.common.utils.AppInfoParser.getVersion()
            + "   bootstrap " + bootstrap + "   runId " + run);

        probe("the DEFAULT producer (enable.idempotence left at its 3.0+ default of true)", () -> {
            Properties p = base();
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
            // Must stay >= linger.ms + request.timeout.ms or the producer refuses
            // to be CONSTRUCTED, which looks like a broker failure and is not.
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "40000");
            try (Producer<String, String> prod = new KafkaProducer<>(p)) {
                RecordMetadata md = prod.send(new ProducerRecord<>("jm-edge-" + run, 0, "k", "v"))
                    .get(40, TimeUnit.SECONDS);
                out("it SENT -> partition " + md.partition() + " offset " + md.offset()
                    + "  (idempotence ON, no overrides: M7 F3 advertises InitProducerId"
                    + " and enforces the sequence window)");
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        probe("a consumer asking for the KIP-848 group protocol (group.protocol=consumer)", () -> {
            Properties c = consumerBase("jm-edge-g848-" + run);
            c.put("group.protocol", "consumer");
            try (Consumer<String, String> con = new KafkaConsumer<>(c)) {
                con.subscribe(List.of("jm-edge-" + run));
                ConsumerRecords<String, String> recs = con.poll(Duration.ofSeconds(20));
                out("poll returned " + recs.count() + " record(s) with no error");
            }
        });

        probe("the DEFAULT consumer group protocol for this client version", () -> {
            Properties c = consumerBase("jm-edge-gdef-" + run);
            try (Consumer<String, String> con = new KafkaConsumer<>(c)) {
                con.subscribe(List.of("jm-edge-" + run));
                ConsumerRecords<String, String> recs = con.poll(Duration.ofSeconds(25));
                out("poll returned " + recs.count() + " record(s) — the default protocol works here");
            }
        });

        probe("AdminClient.listTopics() (rides Metadata, which IS advertised)", () -> {
            try (Admin a = Admin.create(adminBase())) {
                Set<String> names = a.listTopics().names().get(30, TimeUnit.SECONDS);
                out("listed " + names.size() + " topic(s); sample "
                    + names.stream().sorted().limit(4).toList());
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        probe("AdminClient.describeCluster() (DescribeCluster is NOT advertised)", () -> {
            try (Admin a = Admin.create(adminBase())) {
                DescribeClusterResult r = a.describeCluster();
                out("clusterId=" + r.clusterId().get(30, TimeUnit.SECONDS)
                    + " nodes=" + r.nodes().get(30, TimeUnit.SECONDS));
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        probe("AdminClient.createTopics() (CreateTopics IS advertised since M7 F1, v2-6)", () -> {
            try (Admin a = Admin.create(adminBase())) {
                a.createTopics(List.of(new NewTopic("jm-edge-ct-" + run, 4, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
                out("createTopics returned with no error");
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        probe("AdminClient.listConsumerGroups() (ListGroups IS advertised since M7 F2, v0-4)", () -> {
            try (Admin a = Admin.create(adminBase())) {
                var groups = a.listConsumerGroups().all().get(30, TimeUnit.SECONDS);
                out("listed " + groups.size() + " group(s)");
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        probe("a TRANSACTIONAL producer (transactional.id set)", () -> {
            Properties p = base();
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "jm-edge-txn-" + run);
            p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "40000");
            long t0 = System.nanoTime();
            try (Producer<String, String> prod = new KafkaProducer<>(p)) {
                prod.initTransactions();
                out("initTransactions() returned with no error");
            } finally {
                out("initTransactions() spent " + ((System.nanoTime() - t0) / 1_000_000) + "ms"
                    + " — see the header: the wait is FindCoordinator(TRANSACTION), not InitProducerId");
            }
        });

        probe("client telemetry (KIP-714) must be noise, not a failure", () -> {
            // enable.metrics.push defaults to true on 4.x clients. The probe is
            // whether a producer that CANNOT report telemetry still produces.
            Properties p = base();
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "40000");
            try (Producer<String, String> prod = new KafkaProducer<>(p)) {
                RecordMetadata md = prod.send(new ProducerRecord<>("jm-edge-" + run, 1, "t", "t"))
                    .get(40, TimeUnit.SECONDS);
                out("produced at p" + md.partition() + " offset " + md.offset()
                    + " with telemetry left at its default");
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        System.out.println("\n(none of the above is pass/fail: what matters is that every one ENDS, "
            + "fast, with something a human can read)");
    }

    static Properties base() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        security(p);
        return p;
    }

    static Properties adminBase() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "25000");
        security(p);
        return p;
    }

    static Properties consumerBase(String group) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        security(c);
        return c;
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? d : v;
    }

    static void security(Properties p) {
        String proto = env("QK_SECURITY_PROTOCOL", "");
        if (proto.isEmpty()) return;
        p.put("security.protocol", proto);
        if (proto.contains("SASL")) {
            p.put("sasl.mechanism", env("QK_SASL_MECHANISM", "PLAIN"));
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
            if (!env("QK_DISABLE_HOSTNAME_VERIFICATION", "").isEmpty())
                p.put("ssl.endpoint.identification.algorithm", "");
        }
    }
}
