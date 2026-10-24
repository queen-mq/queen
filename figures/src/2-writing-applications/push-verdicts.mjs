// Chapter 2.3: one push, one answer per message.
export default {
  id: "push-verdicts",
  height: 250,
  alt: "One push carries three messages. The broker answers each one: the first is queued at offset 7; the second has a transactionId already stored in the dedup window, so it is a duplicate and nothing is written; the third is refused with an error and nothing is written.",
  draw(c) {
    c.rect(20, 30, 300, 190, { tone: "plain", r: 12 });
    c.text(40, 56, "one push, three messages", { size: 15, weight: 600, tone: "muted" });
    const msgs = [["order-1001", "new"], ["welcome-42", "sent before"], ["bad-payload", "refused"]];
    msgs.forEach(([id], i) => c.box(40, 74 + i * 46, 260, 36, id, { mono: true, size: 15, weight: 500 }));
    const verdicts = [
      ["queued", "stored at offset 7", "ok"],
      ["duplicate", "already in the dedup window: nothing written", "accent"],
      ["error", "refused: nothing written", "danger"],
    ];
    verdicts.forEach(([v, why, tone], i) => {
      const y = 92 + i * 46;
      c.arrow([[306, y], [470, y]], { tone: "muted" });
      c.tag(480, y, v, { tone, mono: true, size: 14.5 });
      c.text(600, y, why, { size: 14.5, tone: "muted" });
    });
    c.text(500, 236, "The JavaScript client throws on an error, never on a duplicate.", { size: 14.5, anchor: "middle", italic: true, tone: "muted" });
  },
};
