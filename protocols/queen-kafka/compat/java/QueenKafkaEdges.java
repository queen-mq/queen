// The two Java-client configurations the facade deliberately does NOT support,
// run to find out HOW they fail. A clear, immediate error is a pass; a hang or
// a silent wrong result is not.
//
//   1. the idempotent producer (the 3.x DEFAULT): InitProducerId is
//      unimplemented on purpose — no transactions, no EOS.
//   2. group.protocol=consumer (KIP-848): the new group protocol, explicitly a
//      non-goal, so ConsumerGroupHeartbeat is not advertised.
//
// Run it the same way as QueenKafkaCompat.java.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class QueenKafkaEdges {
    public static void main(String[] args) {
        String bootstrap = args.length > 0 ? args[0] : "127.0.0.1:19092";
        String run = args.length > 1 ? args[1] : String.valueOf(System.currentTimeMillis());

        System.out.println("=== the DEFAULT java producer (enable.idempotence left alone)");
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
        // Must stay >= linger.ms + request.timeout.ms (30s by default) or the
        // producer refuses to be CONSTRUCTED, which looks like a broker failure
        // and is not one.
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "40000");
        long t0 = System.currentTimeMillis();
        try (Producer<String, String> prod = new KafkaProducer<>(p)) {
            RecordMetadata md = prod.send(
                new ProducerRecord<>("java-edge-" + run, 0, "k", "v")).get(40, TimeUnit.SECONDS);
            System.out.println("  sent in " + (System.currentTimeMillis() - t0) + "ms -> partition "
                + md.partition() + " offset " + md.offset()
                + "  (the client fell back to a non-idempotent producer)");
        } catch (Throwable t) {
            System.out.println("  after " + (System.currentTimeMillis() - t0) + "ms it failed with: " + t);
        }

        System.out.println("\n=== a consumer asking for the KIP-848 group protocol");
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "java-edge-g-" + run);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put("group.protocol", "consumer");
        long t1 = System.currentTimeMillis();
        try (Consumer<String, String> con = new KafkaConsumer<>(c)) {
            con.subscribe(List.of("java-edge-" + run));
            ConsumerRecords<String, String> recs = con.poll(Duration.ofSeconds(20));
            System.out.println("  after " + (System.currentTimeMillis() - t1) + "ms poll returned "
                + recs.count() + " record(s) with no error");
        } catch (Throwable t) {
            System.out.println("  after " + (System.currentTimeMillis() - t1) + "ms it failed with: " + t);
        }
        System.out.println("\n(neither of these is a pass/fail: what matters is that both end, fast, "
            + "with something a human can read)");
    }
}
