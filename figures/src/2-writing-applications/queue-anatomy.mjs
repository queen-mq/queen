// Chapter 2.2: a queue is a named set of partitions that share one configuration.
export default {
  id: "queue-anatomy",
  height: 330,
  alt: "The queue orders holds its options (leaseTime, retryLimit, retryDelay, dedupWindowSeconds, retention), shared by all its partitions. Its partitions are customer-42, customer-7 and customer-13, each an ordered list of messages at offsets 0, 1, 2 and so on. A push that names a new partition, customer-99, creates it.",
  draw(c) {
    c.rect(20, 20, 960, 290, { tone: "strong", r: 14 });
    c.text(44, 50, "queue  orders", { size: 19, weight: 700, mono: true });
    c.rect(40, 74, 250, 216, { tone: "accent", r: 10 });
    c.text(56, 98, "options, for every partition", { size: 14.5, weight: 600 });
    [["leaseTime", "60 s"], ["retryLimit", "3"], ["retryDelay", "1000 ms"], ["dedupWindowSeconds", "3600"], ["retentionEnabled", "false"]].forEach(([k, v], i) => {
      c.text(56, 128 + i * 32, k, { size: 14, mono: true });
      c.text(274, 128 + i * 32, v, { size: 13.5, anchor: "end", tone: "muted" });
    });
    const parts = [["customer-42", 5], ["customer-7", 3], ["customer-13", 6]];
    parts.forEach(([name, n], i) => {
      const y = 80 + i * 62;
      c.text(316, y + 20, name, { size: 14.5, mono: true, tone: "muted" });
      c.strip(450, y, Array.from({ length: n }, (_, j) => ({ label: String(j) })), { cellW: 44, cellH: 40, gap: 5, size: 14 });
    });
    const y = 80 + 3 * 62;
    c.text(316, y + 20, "customer-99", { size: 14.5, mono: true, tone: "accent", weight: 600 });
    c.strip(450, y, [{ label: "0", tone: "accent" }], { cellW: 44, cellH: 40, size: 14 });
    c.text(512, y + 20, "← created by the push that names it", { size: 14, tone: "accent" });
  },
};
