package com.queenmq.compat.springkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * The @KafkaListener surface under test.
 *
 * <p>Every container is {@code autoStartup="false"}: the suite starts and stops them
 * one phase at a time through {@code KafkaListenerEndpointRegistry}, which is also the
 * mechanism the restart-resume phase uses to prove a NEW consumer instance in the same
 * group picks up from the committed offset.
 *
 * <p>Topic and group names come from properties seeded in
 * {@link SpringKafkaCompatApplication#main} so every name carries the run id and
 * reruns never collide.
 */
public final class Listeners {

    private Listeners() {
    }

    /** Concurrency > 1: a ConcurrentMessageListenerContainer with 4 child consumers in one group. */
    @Component
    public static class MainListener {
        private final Recorder recorder;

        public MainListener(Recorder recorder) {
            this.recorder = recorder;
        }

        @KafkaListener(
                id = "main",
                topics = "${queen.compat.topic.main}",
                groupId = "${queen.compat.group.main}",
                containerFactory = "earliestFactory",
                concurrency = "4",
                autoStartup = "false")
        public void onMessage(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("main", record);
        }
    }

    /**
     * Cooperative incremental rebalancing (CooperativeStickyAssignor) - the protocol
     * Spring's own reference documentation recommends, and a different JoinGroup
     * protocol name on the wire than the eager default the other listeners use.
     */
    @Component
    public static class CooperativeListener {
        private final Recorder recorder;

        public CooperativeListener(Recorder recorder) {
            this.recorder = recorder;
        }

        @KafkaListener(
                id = "coop",
                topics = "${queen.compat.topic.main}",
                groupId = "${queen.compat.group.coop}",
                containerFactory = "cooperativeFactory",
                concurrency = "2",
                autoStartup = "false")
        public void onMessage(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("coop", record);
        }
    }

    /** Compression lane: one container over the four codec topics. */
    @Component
    public static class CodecListener {
        private final Recorder recorder;

        public CodecListener(Recorder recorder) {
            this.recorder = recorder;
        }

        @KafkaListener(
                id = "codec",
                topics = {"${queen.compat.topic.lz4}", "${queen.compat.topic.zstd}",
                        "${queen.compat.topic.snappy}", "${queen.compat.topic.gzip}"},
                groupId = "${queen.compat.group.codec}",
                containerFactory = "earliestFactory",
                concurrency = "2",
                autoStartup = "false")
        public void onMessage(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("codec", record);
        }
    }

    /**
     * Two DISTINCT listener ids sharing one group id.  "A" runs first and commits;
     * "A" is stopped; more records are produced; "B" starts as a brand new member of
     * the same group and must see only what "A" never consumed.
     */
    @Component
    public static class ResumeListeners {
        private final Recorder recorder;

        public ResumeListeners(Recorder recorder) {
            this.recorder = recorder;
        }

        @KafkaListener(
                id = "resumeA",
                topics = "${queen.compat.topic.resume}",
                groupId = "${queen.compat.group.resume}",
                containerFactory = "earliestFactory",
                concurrency = "1",
                autoStartup = "false")
        public void first(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("resumeA", record);
        }

        @KafkaListener(
                id = "resumeB",
                topics = "${queen.compat.topic.resume}",
                groupId = "${queen.compat.group.resume}",
                containerFactory = "earliestFactory",
                concurrency = "1",
                autoStartup = "false")
        public void second(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("resumeB", record);
        }
    }

    /**
     * ConsumerSeekAware in its own bean (the callbacks apply to every container of the
     * bean that implements it, so it must not share a class with the other listeners).
     *
     * <p>Runs with {@code auto.offset.reset=latest} in a fresh group: without the seek
     * it would receive nothing at all, so "it received everything" is a real proof that
     * the seek reached the broker and that Fetch honours an explicit offset.
     */
    @Component
    public static class SeekListener implements ConsumerSeekAware {
        private final Recorder recorder;
        private volatile boolean seekToBeginning = true;

        public SeekListener(Recorder recorder) {
            this.recorder = recorder;
        }

        public void setSeekToBeginning(boolean value) {
            this.seekToBeginning = value;
        }

        @KafkaListener(
                id = "seek",
                topics = "${queen.compat.topic.main}",
                groupId = "${queen.compat.group.seek}",
                containerFactory = "latestFactory",
                concurrency = "1",
                autoStartup = "false")
        public void onMessage(ConsumerRecord<byte[], byte[]> record) {
            recorder.record("seek", record);
        }

        @Override
        public void onPartitionsAssigned(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
                                         ConsumerSeekCallback callback) {
            recorder.rebalance("seek ASSIGNED " + assignments.keySet());
            if (seekToBeginning && !assignments.isEmpty()) {
                callback.seekToBeginning(assignments.keySet());
            }
        }
    }
}
