// Chapter 4.4: what the supervisor is. A master on the Laravel host starts and stops
// ordinary queue:work processes; the workers talk to the broker; the master only reads
// the backlog. Status goes to a private state directory that the status command reads.
export default {
  id: "supervisor-topology",
  height: 400,
  alt: "On the Laravel host, the supervisor master (PHP or Rust) starts, stops and restarts ordinary php artisan queue:work processes, here four of them. Every few seconds the master reads the queue's backlog from the QueenMQ broker; the workers pop and ack jobs over HTTP themselves, so no job passes through the master. The master writes its status to a private state directory, which php artisan queen:supervisor status reads.",
  draw(c) {
    // The Laravel host.
    c.rect(20, 20, 600, 360, { tone: "plain", r: 14 });
    c.text(40, 46, "a Laravel host", { size: 15, weight: 700 });

    c.box(60, 70, 230, 70, "supervisor master", { sub: "PHP or Rust", tone: "accent", size: 16 });

    // State directory below the backlog line, and the status command that reads it.
    c.file(370, 150, 210, 62, "state directory", { sub: "status, control requests", mono: false });
    c.arrow([[294, 128], [366, 168]], { tone: "muted", label: "writes", labelAt: [322, 166] });
    c.tag(475, 246, "php artisan queen:supervisor status", { tone: "info", anchor: "middle", mono: true, size: 13 });
    c.arrow([[475, 232], [475, 216]], { tone: "info" });
    c.text(484, 228, "reads", { size: 13, tone: "info" });

    // Workers.
    for (let i = 0; i < 4; i++) c.box(60 + i * 132, 284, 118, 54, "queue:work", { mono: true, size: 14.5 });
    c.text(592, 316, "…", { size: 20, anchor: "middle", tone: "muted" });
    c.arrow([[175, 140], [175, 280]], { tone: "accent", width: 2, label: "starts, stops, restarts", left: false });

    // The broker.
    c.rect(700, 60, 270, 290, { tone: "strong", r: 14 });
    c.text(835, 92, "QueenMQ", { size: 17, weight: 700, anchor: "middle" });
    c.strip(735, 130, [{ label: "" }, { label: "" }, { label: "" }, { label: "" }, { label: "" }], { cellW: 36, cellH: 36, gap: 4 });
    c.text(835, 192, "queue default", { size: 13.5, anchor: "middle", tone: "muted", mono: true });
    c.text(835, 330, "no job passes through the master", { size: 13, anchor: "middle", tone: "muted", italic: true });

    c.arrow([[290, 96], [696, 96]], { tone: "muted", dash: true, label: "reads the backlog (every few seconds)" });
    c.arrow([[604, 311], [696, 290]], { tone: "strong", width: 2, both: true, label: "pop, ack (HTTP)", labelAt: [652, 268] });
  },
};
