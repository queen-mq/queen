// Chapter 1.3 (Partitions): head-of-line blocking on hashed partitions, and one
// partition per entity. The slow message is customer B's, as in the Smartchat story.
export default {
  id: "kafka-vs-queen",
  height: 470,
  alt: "Left, a Kafka topic with three partitions: customers are hashed onto them, and in partition P1 a slow message of customer B sits at the head, so the messages of customers D and E behind it wait. Right, a Queen queue with one partition per customer, created by its first push: three workers each hold one partition, and customer B's slow message delays only customer B.",
  draw(c) {
    const titleY = 26;
    // ---------------------------------------------------------------- Kafka
    c.text(20, titleY, "Kafka: keys hashed onto 3 partitions", { size: 18, weight: 600 });
    const rows = [
      { name: "P0", cells: ["A", "C", "A", "F"] },
      { name: "P1", cells: ["D", "E", "D", "B"], slow: 3 },
      { name: "P2", cells: ["C", "F", "A", "C"] },
    ];
    rows.forEach((row, i) => {
      const y = 78 + i * 104;
      c.text(20, y + 22, row.name, { size: 16, weight: 600, tone: "muted", mono: true });
      const cells = row.cells.map((label, j) => ({
        label,
        tone: j === row.slow ? "danger" : row.slow !== undefined ? "muted" : "plain",
      }));
      const s = c.strip(62, y, cells, { cellW: 54, cellH: 44, gap: 5 });
      const consumerX = s.end + 52;
      c.arrow([[s.end + 6, y + 22], [consumerX - 4, y + 22]], { tone: "muted" });
      c.box(consumerX, y + 2, 92, 40, `consumer ${i + 1}`, { size: 14.5, weight: 500 });
      if (row.slow !== undefined) {
        c.text(62, y + 64, "B takes 40 s (an LLM call): D and E wait behind it", { size: 13.5, tone: "danger", italic: true });
      }
    });
    c.text(20, 420, "Head of the partition on the right. One slow message", { size: 14.5, tone: "muted" });
    c.text(20, 442, "holds up every key that hashes to the same partition.", { size: 14.5, tone: "muted" });

    c.line(500, 20, 500, 450, { tone: "grid", width: 1.4 });

    // ---------------------------------------------------------------- Queen
    c.text(530, titleY, "Queen: one partition per customer", { size: 18, weight: 600 });
    const customers = [
      { name: "customer-A", cells: ["", ""], worker: "w1" },
      { name: "customer-B", cells: ["", "B"], slow: 1, worker: "w2" },
      { name: "customer-C", cells: [""] },
      { name: "customer-D", cells: ["", "", ""], worker: "w3" },
      { name: "customer-E", cells: [""] },
      { name: "customer-F", cells: ["", ""] },
    ];
    customers.forEach((cu, i) => {
      const y = 64 + i * 56;
      c.text(530, y + 19, cu.name, { size: 14.5, mono: true, tone: "muted" });
      const cells = cu.cells.map((label, j) => ({ label: j === cu.slow ? "B" : "", tone: j === cu.slow ? "danger" : cu.slow !== undefined ? "muted" : "plain" }));
      const width = cells.length * 41 - 5;
      c.strip(780 - width, y, cells, { cellW: 36, cellH: 38, gap: 5 });
      if (cu.worker) {
        c.arrow([[786, y + 19], [868, y + 19]], { tone: "accent", width: 2 });
        c.box(872, y + 1, 70, 36, cu.worker, { tone: "accent", size: 15 });
      } else {
        c.text(800, y + 19, "free", { size: 13.5, tone: "faint", italic: true });
      }
    });
    c.text(530, 420, "Each customer is its own ordered partition, created by its", { size: 14.5, tone: "muted" });
    c.text(530, 442, "first push. B's slow message delays only customer B.", { size: 14.5, tone: "muted" });
  },
};
