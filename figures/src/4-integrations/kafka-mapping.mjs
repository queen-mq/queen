// Chapter 4.1: what a Kafka client sees, and what Queen stores.
export default {
  id: "kafka-mapping",
  height: 320,
  alt: "Left, what kcat sees: topic orders with 1,024 partitions; the key customer-123 hashes to partition 307; the consumer group billing commits offsets. Right, what Queen stores: the queue orders, the partition named 307, each record one message whose payload holds the key, value and timestamp in base64, and the Queen consumer group billing, whose position is the committed offset. HTTP clients read and write the same data.",
  draw(c) {
    c.text(20, 26, "what a Kafka client sees", { size: 17, weight: 600 });
    c.text(530, 26, "what Queen stores", { size: 17, weight: 600 });
    const rows = [
      ["topic orders, 1,024 partitions", "queue orders, created on first use"],
      ["key customer-123 → partition 307", "partition \"307\""],
      ["record: key, value, timestamp", "message { k, v, t } in base64"],
      ["offset 0, 1, 2…", "the partition's offsets"],
      ["group billing, committed offset", "consumer group billing, its position"],
    ];
    rows.forEach(([k, q], i) => {
      const y = 52 + i * 50;
      c.box(20, y, 400, 38, k, { size: 14.5, weight: 500, tone: "info" });
      c.arrow([[426, y + 19], [524, y + 19]], { tone: "muted", both: true });
      c.box(530, y, 450, 38, q, { size: 14.5, weight: 500, tone: i === 4 ? "accent" : "plain" });
    });
    c.text(500, 306, "HTTP clients push to and pop from the same queue, and join the same group.", { size: 14, anchor: "middle", tone: "muted", italic: true });
  },
};
