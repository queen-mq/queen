// Chapter 3.4: a conversation's burst waits windowBuffer, then goes as one batch.
export default {
  id: "chat-burst",
  height: 290,
  alt: "Partition conv-42 on a time axis. The guest sends Hi at 0 seconds, Is breakfast included at 0.4 and And is there parking at 0.9. With windowBuffer 2, the messages wait until the first is two seconds old; at 2 seconds the assistant receives all three in one batch and answers once. Another conversation, conv-7, is not held by conv-42.",
  draw(c) {
    const x0 = 170, x1 = 960;
    const T = (s) => x0 + (x1 - x0) * (s / 3);
    c.text(20, 72, "conv-42", { size: 15, mono: true, weight: 600 });
    [[0, "Hi!"], [0.45, "Is breakfast included?"], [0.95, "And is there parking?"]].forEach(([t, msg], i) => {
      c.circle(T(t), 72, 7, { fill: c.p.ink, stroke: "none" });
      c.text(T(t) - 6, i === 1 ? 24 : 46, msg, { size: 13.5, anchor: "start", tone: "muted" });
    });
    c.rect(T(0), 92, T(2) - T(0), 30, { tone: "accent", r: 6 });
    c.text((T(0) + T(2)) / 2, 107, "held: windowBuffer 2 s from the first message", { size: 14, anchor: "middle", weight: 500 });
    c.arrow([[T(2), 128], [T(2), 166]], { tone: "ok", width: 2.2 });
    c.box(T(2) - 130, 170, 260, 52, "one batch of 3", { sub: "one answer from the assistant", tone: "ok", size: 16 });
    c.text(20, 160, "conv-7", { size: 15, mono: true, tone: "muted" });
    c.circle(T(1.2), 160, 7, { fill: c.p.muted, stroke: "none" });
    c.text(T(1.2) + 14, 160, "its own burst, its own window", { size: 13, tone: "muted" });
    c.axis(x0, x1, 254, [0, 1, 2, 3].map((s) => ({ at: T(s), label: `${s} s` })), {});
  },
};
