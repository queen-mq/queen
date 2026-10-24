// Chapter 6.7: the bytes of one message record, as the xxd dump in the text shows them.
export default {
  id: "record-layout",
  height: 290,
  alt: "The layout of the message record in the dump: len, 4 bytes, value 110; checksum, 8 bytes, an xxh3 of everything after it; seq, 8 bytes, entry 11; partition, 8 bytes, 1; first offset, 8 bytes, 0; count, 4 bytes, 1; created at, 8 bytes, microseconds; kind, 1 byte, 0; hashes, 16 bytes per message; then the payload: one frame per message, with its length 45, flags, its 16-byte id, its transactionId order-8891 and its data.",
  draw(c) {
    const fields = [["len", 4, "110"], ["checksum", 8, "xxh3"], ["seq", 8, "11"], ["partition", 8, "1"], ["first offset", 8, "0"], ["count", 4, "1"], ["created at", 8, "µs"], ["kind", 1, "0"], ["hashes", 16, "1 × 16"]];
    const scale = 9.3;
    let x = 20;
    fields.forEach(([name, n, v], i) => {
      const w = Math.max(n * scale * 1.35, 42);
      c.rect(x, 50, w - 3, 54, { tone: i === 8 ? "accent" : "plain", r: 4 });
      c.text(x + (w - 3) / 2, 70, v, { size: 13.5, anchor: "middle", mono: true, weight: 600 });
      c.text(x + (w - 3) / 2, 90, `${n} B`, { size: 11.5, anchor: "middle", tone: "muted" });
      c.text(x + (w - 3) / 2, 122, name, { size: 12, anchor: "middle", tone: "muted" });
      x += w;
    });
    c.text(20, 30, "message record (offset 0x20 in the file)", { size: 15, weight: 600 });
    c.rect(x, 50, 980 - x, 54, { tone: "ok", r: 4 });
    c.text(x + (980 - x) / 2, 77, "payload", { size: 14, anchor: "middle", weight: 600 });
    c.text(20, 170, "payload: one frame per message", { size: 15, weight: 600 });
    const frame = [["length", "45"], ["flags", "0"], ["message id", "01a16d65-…"], ["txn len", "10"], ["transactionId", "order-8891"], ["data", "{\"orderId\":8891}"]];
    let fx = 20;
    frame.forEach(([n, v], i) => {
      const w = [90, 70, 190, 90, 190, 330][i];
      c.rect(fx, 190, w - 4, 54, { tone: i === 5 ? "ok" : "plain", r: 4 });
      c.text(fx + (w - 4) / 2, 210, v, { size: 13.5, anchor: "middle", mono: true, weight: 600 });
      c.text(fx + (w - 4) / 2, 262, n, { size: 12, anchor: "middle", tone: "muted" });
      fx += w;
    });
  },
};
