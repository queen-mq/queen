package com.queenmq.compat.springkafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Producer / consumer plumbing for the phases that need byte-exact payloads or a
 * specific compression codec.
 *
 * <p>DELIBERATE SHAPE: the only Spring beans declared here are the two
 * {@link ConcurrentKafkaListenerContainerFactory}s that {@code @KafkaListener} looks up
 * by name.  The byte[] KafkaTemplates and ConsumerFactories are plain objects, NOT
 * beans, because Boot's own {@code KafkaAutoConfiguration} backs off
 * ({@code @ConditionalOnMissingBean(KafkaTemplate.class)} /
 * {@code @ConditionalOnMissingBean(ConsumerFactory.class)}) the moment an application
 * declares one of its own.  Keeping them out of the context means the stock Boot
 * {@code kafkaTemplate}, {@code kafkaProducerFactory}, {@code kafkaConsumerFactory},
 * {@code kafkaListenerContainerFactory} and {@code KafkaAdmin} beans all still exist and
 * are exercised (see the auto-create phase).  The container factories are named
 * {@code earliestFactory} / {@code latestFactory}, so Boot's
 * {@code @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")} does not
 * fire either.
 *
 * <p>There is no mandatory deviation from stock Boot any more.  {@code
 * enable.idempotence=false} used to be one: kafka-clients >= 3.0 turns idempotence ON
 * by default and, before M7 F3 advertised InitProducerId, a stock Boot producer died
 * on its first send.  The flag stays off here so the rest of the suite measures the
 * non-idempotent path; the idempotence phase in {@link CompatSuite} runs a producer at
 * the stock default and asserts it sends.
 */
@Configuration
public class CompatConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrap;

    @Value("${queen.compat.run-id}")
    private String runId;

    private final Map<String, KafkaTemplate<byte[], byte[]>> templates = new LinkedHashMap<>();
    private final List<DefaultKafkaProducerFactory<byte[], byte[]>> producerFactories = new ArrayList<>();

    @PostConstruct
    void buildTemplates() {
        for (String codec : List.of("none", "lz4", "zstd", "snappy", "gzip")) {
            DefaultKafkaProducerFactory<byte[], byte[]> pf =
                    new DefaultKafkaProducerFactory<>(producerProps(codec));
            producerFactories.add(pf);
            templates.put(codec, new KafkaTemplate<>(pf));
        }
    }

    @PreDestroy
    void closeTemplates() {
        for (DefaultKafkaProducerFactory<byte[], byte[]> pf : producerFactories) {
            try {
                pf.destroy();
            } catch (Exception ignored) {
                // best effort: the JVM is on its way out
            }
        }
    }

    public KafkaTemplate<byte[], byte[]> template(String codec) {
        KafkaTemplate<byte[], byte[]> t = templates.get(codec);
        if (t == null) {
            throw new IllegalArgumentException("no template for codec " + codec);
        }
        return t;
    }

    // ------------------------------------------------------------- producers

    public Map<String, Object> producerProps(String codec) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        // MANDATORY against queen-kafka: no InitProducerId on the facade.
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 25);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-compat-" + codec + "-" + runId);
        return p;
    }

    // ------------------------------------------------------------- consumers

    public Map<String, Object> consumerProps(String offsetReset) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        // Spring turns this off itself; stated here so the report can quote it.
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        // session.timeout.ms / heartbeat.interval.ms deliberately left at kafka-clients
        // defaults (45000 / 3000) - both inside the facade's 6000..300000 window.
        return p;
    }

    /**
     * The eager (RangeAssignor / StickyAssignor) default is what the other factories
     * use; this one asks for the cooperative incremental protocol instead, which is
     * what Spring's own documentation recommends and what most Boot services in the
     * wild are configured with.  The group protocol name on the wire changes from
     * "range" to "cooperative-sticky", so it is a genuinely different JoinGroup.
     */
    public Map<String, Object> cooperativeConsumerProps() {
        Map<String, Object> p = consumerProps("earliest");
        p.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                List.of(org.apache.kafka.clients.consumer.CooperativeStickyAssignor.class));
        return p;
    }

    public ConsumerFactory<byte[], byte[]> consumerFactory(String offsetReset) {
        return new DefaultKafkaConsumerFactory<>(consumerProps(offsetReset));
    }

    private ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory(
            Map<String, Object> consumerProps, Recorder recorder, String label) {
        ConcurrentKafkaListenerContainerFactory<byte[], byte[]> f = new ConcurrentKafkaListenerContainerFactory<>();
        f.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps));
        ContainerProperties props = f.getContainerProperties();
        props.setAckMode(ContainerProperties.AckMode.BATCH);
        props.setPollTimeout(500);
        // The framework pattern under test: a rebalance listener wired into the
        // container, not into a hand-rolled consumer loop.
        props.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                recorder.rebalance(label + " ASSIGNED " + partitions);
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                recorder.rebalance(label + " REVOKED-before-commit " + partitions);
            }

            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                recorder.rebalance(label + " REVOKED-after-commit " + partitions);
            }

            @Override
            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                recorder.rebalance(label + " LOST " + partitions);
            }
        });
        return f;
    }

    @Bean(name = "earliestFactory")
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> earliestFactory(Recorder recorder) {
        return factory(consumerProps("earliest"), recorder, "earliest");
    }

    @Bean(name = "latestFactory")
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> latestFactory(Recorder recorder) {
        return factory(consumerProps("latest"), recorder, "latest");
    }

    @Bean(name = "cooperativeFactory")
    public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> cooperativeFactory(Recorder recorder) {
        return factory(cooperativeConsumerProps(), recorder, "cooperative");
    }
}
