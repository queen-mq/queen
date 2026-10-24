// Chapter 1.3 (A simple interface, Wrap up): thin clients, the logic in the broker.
export default {
  id: "thin-clients",
  height: 360,
  alt: "Clients on the left: the JavaScript, Python, Go, Rust, PHP and C++ SDKs, curl, and Kafka clients. The SDKs and curl speak JSON over HTTP on port 6632, Kafka clients the Kafka protocol on port 9092. On the right, the broker holds all the logic: the order of each partition, leases, retries and the dead-letter queue, deduplication, timers, KV state, and the transactions that tie them together.",
  draw(c) {
    const clients = ["JavaScript", "Python", "Go", "Rust", "PHP", "C++", "curl"];
    clients.forEach((name, i) => {
      const y = 24 + i * 40;
      c.box(30, y, 150, 32, name, { size: 15, weight: 500, tone: name === "curl" ? "muted" : "plain" });
      c.line(180, y + 16, 200, y + 16, { tone: "line", width: 1.6 });
    });
    c.box(30, 312, 150, 32, "Kafka clients", { size: 15, weight: 500, tone: "info" });
    c.rect(200, 22, 6, 282, { tone: "none", fill: c.p.line, r: 3 });
    c.arrow([[214, 160], [444, 160]], { label: "JSON over HTTP, port 6632", tone: "ink", width: 2.2 });
    c.arrow([[184, 328], [444, 328]], { label: "Kafka protocol, port 9092", tone: "info" });
    c.rect(460, 20, 520, 324, { tone: "strong", r: 14 });
    c.text(720, 52, "the broker: where the logic lives", { size: 18, weight: 700, anchor: "middle" });
    const items = [
      "the order of each partition", "leases: one holder at a time", "retries, then the dead-letter queue",
      "deduplication by transactionId", "timers instead of cron", "KV state and locks",
      "transactions that tie them together",
    ];
    items.forEach((t, i) => {
      const col = i < 4 ? 0 : 1;
      const row = i < 4 ? i : i - 4;
      const x = 484 + col * 250;
      const y = 92 + row * 58;
      c.rect(x, y, 236, 44, { tone: i === 6 ? "accent" : "plain", r: 8 });
      c.text(x + 118, y + 22, t, { size: 13.5, anchor: "middle", weight: 500 });
    });
  },
};
