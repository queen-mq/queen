/*
 * queen-kafka compatibility suite - Spring Boot 3.x + spring-kafka
 * ================================================================
 *
 * WHAT THIS PROVES
 *   That a stock Spring Boot service - Boot's own KafkaAutoConfiguration, KafkaTemplate,
 *   @KafkaListener on a ConcurrentMessageListenerContainer with concurrency > 1, the
 *   container's rebalance-listener callbacks, ConsumerSeekAware, and container
 *   stop/start through KafkaListenerEndpointRegistry - drives the queen-kafka facade
 *   with no framework-level workarounds.
 *
 *   Phases, in order:
 *     1  library versions actually on the classpath (Boot decides kafka-clients, not us)
 *     2  produce 512 records, keys + 3 headers (one binary), to a topic that does not
 *        exist yet, and check the RecordMetadata the broker hands back
 *     3  print the API versions kafka-clients NEGOTIATED, read out of NetworkClient's
 *        own DEBUG stream - never assumed
 *     4  produce with lz4, zstd, snappy and gzip
 *     5  consume through a group with concurrency=4 and verify count, per-partition
 *        order, byte-exact key/payload/header round trip, and that the broker-reported
 *        (partition, offset) matches what the producer was told
 *    5b  the same topic again through a CooperativeStickyAssignor group, so the
 *        incremental rebalance protocol is exercised and not only the eager one
 *     6  read all four compressed topics back
 *     7  commit, stop the container, produce more, start a DIFFERENT listener id in the
 *        SAME group: it must resume exactly at the committed offset
 *     8  ListOffsets earliest/latest, position(), seek(), seekToBeginning/End, plus
 *        Spring's ConsumerSeekAware rewinding an auto.offset.reset=latest group
 *     9  auto-create through the stock Boot-auto-configured kafkaTemplate bean
 *    9b  which Spring KafkaAdmin conveniences survive the facade's API surface
 *    10  the stock Boot producer default, enable.idempotence=true
 *    11  SASL/PLAIN over TLS (skipped unless the TLS env vars are set)
 *
 * WHAT USED TO BE THE CLIENT'S PROBLEM HERE, AND IS NOT ANY MORE
 *   - enable.idempotence: kafka-clients >= 3.0 turns it ON by default whenever acks=all
 *     and no conflicting config is present, and Spring Boot does not change that.
 *     Before M7 F3 that killed a stock Boot producer on its FIRST send, and
 *     spring.kafka.producer.properties.enable.idempotence=false was the one mandatory
 *     config change.  InitProducerId (key 22) is advertised v0-4 now and the sequence
 *     window is enforced in queen-kafka/src/idempotent.rs, so phase 10 runs a producer
 *     at the stock default and asserts the record lands with a real offset.  This
 *     suite still pins the flag off for its OTHER producers, on purpose, so the rest
 *     of the file keeps measuring the non-idempotent path.
 *   - AdminClient: Boot always registers a KafkaAdmin bean, but it opens no AdminClient
 *     unless the application asks it to.  Phase 9b measures which of the three Spring
 *     conveniences that DO ask still work.  Measured, not assumed:
 *         KafkaAdmin.describeTopics(...)   WORKS  (it is Metadata underneath, and
 *                                                 Metadata 0-9 is advertised), so
 *                                                 spring.kafka.listener.missing-topics-
 *                                                 fatal=true is usable
 *         KafkaAdmin.clusterId()           WORKS  (returns "queen"), so Micrometer
 *                                                 observation on KafkaTemplate is usable
 *         NewTopic beans                   WORK   since M7 F1 (CreateTopics v2-6); the
 *                                                 topic is created at the partition
 *                                                 count the bean asked for
 *     So a Boot service has no admin rule to follow any more: declare NewTopic beans or
 *     let queen-kafka auto-create on the first Metadata request, both work.  The one
 *     shape CreateTopics still refuses is cleanup.policy=compact.
 *   - Duplicates across a rebalance are at-least-once semantics, not loss or corruption:
 *     phase 5 asserts on distinct (partition, offset) and reports the redelivery count.
 *
 * RUNNING
 *   ./run.sh                          # 127.0.0.1:19092, timestamp run id
 *   QUEEN_KAFKA_BOOTSTRAP=127.0.0.1:31102 RUN_ID=abc ./run.sh
 *   java -jar target/queen-kafka-spring-compat.jar <bootstrap> <runId>
 *
 *   Nothing here starts or stops a broker; the stack is rig.sh's job.  Every topic and
 *   group name carries the run id so reruns never collide.  Every blocking wait has a
 *   deadline: a hang is a RESULT, printed as a FAIL, not an indefinite stall.
 *   Exit code is the number of failed assertions (0 = RESULT: PASS).
 */
package com.queenmq.compat.springkafka;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class SpringKafkaCompatApplication {

    public static void main(String[] args) {
        List<String> positional = new ArrayList<>();
        for (String a : args) {
            if (!a.startsWith("--")) {
                positional.add(a);
            }
        }
        String bootstrap = positional.size() > 0 ? positional.get(0)
                : firstNonEmpty(System.getenv("QUEEN_KAFKA_BOOTSTRAP"),
                                System.getenv("KAFKA_BOOTSTRAP"),
                                "127.0.0.1:19092");
        String runId = positional.size() > 1 ? positional.get(1)
                : firstNonEmpty(System.getenv("RUN_ID"), Long.toString(System.currentTimeMillis() / 1000L));

        String prefix = "spring-" + runId;
        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put("spring.kafka.bootstrap-servers", bootstrap);
        defaults.put("queen.compat.run-id", runId);
        defaults.put("queen.compat.topic.main", prefix + "-main");
        defaults.put("queen.compat.topic.resume", prefix + "-resume");
        defaults.put("queen.compat.topic.autocreate", prefix + "-autocreate");
        defaults.put("queen.compat.topic.lz4", prefix + "-lz4");
        defaults.put("queen.compat.topic.zstd", prefix + "-zstd");
        defaults.put("queen.compat.topic.snappy", prefix + "-snappy");
        defaults.put("queen.compat.topic.gzip", prefix + "-gzip");
        defaults.put("queen.compat.topic.tls", prefix + "-tls");
        defaults.put("queen.compat.group.main", prefix + "-g-main");
        defaults.put("queen.compat.group.codec", prefix + "-g-codec");
        defaults.put("queen.compat.group.resume", prefix + "-g-resume");
        defaults.put("queen.compat.group.seek", prefix + "-g-seek");
        defaults.put("queen.compat.group.coop", prefix + "-g-coop");
        defaults.put("queen.compat.group.probe", prefix + "-g-probe");
        defaults.put("queen.compat.group.tls", prefix + "-g-tls");

        System.out.println("=== queen-kafka compat: Spring Boot + spring-kafka");
        System.out.println("  bootstrap " + bootstrap);
        System.out.println("  run id    " + runId);

        SpringApplication app = new SpringApplication(SpringKafkaCompatApplication.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setDefaultProperties(defaults);

        int rc;
        ConfigurableApplicationContext ctx = null;
        try {
            ctx = app.run(args);
            rc = ctx.getBean(CompatSuite.class).run();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
            System.out.println();
            System.out.println("RESULT: FAIL (suite aborted: " + CompatSuite.rootCause(t) + ")");
            rc = 2;
        } finally {
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception ignored) {
                    // shutting down is best effort; the result is already decided
                }
            }
        }
        System.out.flush();
        System.exit(rc);
    }

    private static String firstNonEmpty(String... candidates) {
        for (String c : candidates) {
            if (c != null && !c.isBlank()) {
                return c.trim();
            }
        }
        return "";
    }
}
