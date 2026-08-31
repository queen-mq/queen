package com.queenmq.compat.springkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything the listeners see, kept as immutable snapshots so the phases can
 * assert on them after the containers have stopped.
 *
 * <p>Records are bucketed by an arbitrary lane name so several @KafkaListener
 * methods can share one recorder bean without stepping on each other.
 */
@Component
public class Recorder {

    /** One consumed record, copied out of the (reused) ConsumerRecord. */
    public static final class Got {
        public final String topic;
        public final int partition;
        public final long offset;
        public final byte[] key;
        public final byte[] value;
        public final Map<String, byte[]> headers;
        public final String thread;
        public final long timestamp;

        Got(ConsumerRecord<byte[], byte[]> r) {
            this.topic = r.topic();
            this.partition = r.partition();
            this.offset = r.offset();
            this.key = r.key() == null ? null : r.key().clone();
            this.value = r.value() == null ? null : r.value().clone();
            this.timestamp = r.timestamp();
            Map<String, byte[]> h = new LinkedHashMap<>();
            for (Header header : r.headers()) {
                h.put(header.key(), header.value() == null ? null : header.value().clone());
            }
            this.headers = h;
            this.thread = Thread.currentThread().getName();
        }

        @Override
        public String toString() {
            return topic + "-" + partition + "@" + offset
                    + " key=" + (key == null ? "null" : new String(key, java.nio.charset.StandardCharsets.UTF_8))
                    + " valueLen=" + (value == null ? -1 : value.length)
                    + " headers=" + headers.keySet()
                    + " thread=" + thread;
        }
    }

    private final Map<String, List<Got>> lanes = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    private final List<String> rebalanceEvents = new CopyOnWriteArrayList<>();

    public void record(String lane, ConsumerRecord<byte[], byte[]> r) {
        lanes.computeIfAbsent(lane, k -> new CopyOnWriteArrayList<>()).add(new Got(r));
        counts.computeIfAbsent(lane, k -> new AtomicLong()).incrementAndGet();
    }

    public List<Got> lane(String lane) {
        return new ArrayList<>(lanes.getOrDefault(lane, List.of()));
    }

    public long count(String lane) {
        AtomicLong c = counts.get(lane);
        return c == null ? 0 : c.get();
    }

    public void clear(String lane) {
        lanes.remove(lane);
        counts.remove(lane);
    }

    public void rebalance(String event) {
        rebalanceEvents.add(event);
    }

    public List<String> rebalanceEvents() {
        return new ArrayList<>(rebalanceEvents);
    }

    public static boolean bytesEqual(byte[] a, byte[] b) {
        return Arrays.equals(a, b);
    }
}
