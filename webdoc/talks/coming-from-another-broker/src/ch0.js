
  // ==== Opening ====

  // What a booking flow runs next to Kafka, and the chapter that removes each one.
  // The closing slide shows the same boxes again.
  const GLUE = [
    ["The broker", "Kafka, RabbitMQ, SQS…", "", "The broker itself. QueenMQ takes its place, and its Kafka port keeps Kafka clients working (the end)."],
    ["Retry topics or delay tricks", "and the dead letters", "2", "Chapter 2: every message gets a lease, an answer, retries and a dead-letter queue, in the broker."],
    ["An outbox and an idempotency table", "in Postgres, and a relay", "3", "Chapter 3: the ack, the state and the next event commit as one log entry."],
    ["A reminders table and a cron job", "scanning every minute", "4", "Chapter 4: a timer, scheduled in the step that wants it, fired as a message."],
    ["Redis", "for locks", "5", "Chapter 5: a lock, and a guard that keeps a frozen holder's commit out."],
    ["Reply queues, or HTTP calls", "for a price, now", "6", "Chapter 6: ephemeral queues, in memory, with a deadline."],
    ["A stream processor and a search index", "to ask what happened", "7", "Chapter 7: the feed, one log of everything, and streams on it."],
    ["Debezium and Kafka Connect", "to move rows in and out", "8", "Chapter 8: Postgres connectors that run in the broker."],
  ];
  const glueGrid = (el, onPick) => {
    const grid = h(`<div class="glue-grid"></div>`);
    GLUE.forEach(([name, what, ch], i) => {
      const b = h(`<button class="box pick" data-i="${i}"><span class="ch">${ch ? `ch ${ch}` : "all"}</span>${name}<small>${what}</small></button>`);
      b.addEventListener("click", () => onPick(i, b));
      grid.appendChild(b);
    });
    el.appendChild(grid);
    return grid;
  };

  // ---- 0.2 The plan: a row per chapter; a click jumps to it ----
  {
    const PLAN = [
      ["One booking = one partition", "order per key: a partition count, or a queue per key", "a partition per booking"],
      ["Work it, fail it, retry it", "retry topics or delay tricks, dead letters", "leases, answers, retries, DLQ"],
      ["One step = one commit", "an outbox, an idempotency table", "one transaction"],
      ["Waiting without cron", "a reminders table, a cron job", "timers"],
      ["Only one worker", "Redis locks", "a lock and a guard"],
      ["The booking page: now, not history", "reply queues, HTTP calls", "conflation, ephemeral queues"],
      ["The whole history, live", "a stream processor, a search index", "the feed and streams"],
      ["Postgres in, Postgres out", "Debezium and its runtime", "connectors in the broker"],
      ["In production", "", "a guest chat, a channel manager, tracing"],
    ];
    const el = $("#plan");
    el.innerHTML = `
      <table class="opts compare plan-table">
        <thead><tr><th></th><th>chapter</th><th>around a broker you build</th><th>in QueenMQ</th></tr></thead>
        <tbody>${PLAN.map(([title, kafka, queen], i) => `<tr data-go="${title}"><td>${i < 8 ? i + 1 : "→"}</td><td>${title}</td><td>${kafka}</td><td>${queen}</td></tr>`).join("")}</tbody>
      </table>`;
    $$("tr[data-go]", el).forEach((tr) => tr.addEventListener("click", () => {
      const i = slides.findIndex((s) => s.dataset.title === tr.dataset.go);
      if (i >= 0) { go(i); if (flow.matches) slides[i].scrollIntoView(); }
    }));
  }

  // ---- 0.3 Kafka (KRaft + ISR) and Queen (one raft log), three nodes each ----
  {
    const slide = $("#arch-kafka").closest(".slide");
    const kEl = $("#arch-kafka"), qEl = $("#arch-queen"), t = timers();
    let k, q;
    const kDraw = (hl = {}) => {
      const box = $(".nodes", kEl);
      box.innerHTML = "";
      for (const n of [1, 2, 3]) {
        const up = k.up[n];
        const parts = k.parts.map((p, i) => {
          if (!up) return "";
          const role = p.leader === n ? "leader" : p.isr.includes(n) ? "follower · ISR" : "follower";
          const lit = (hl.parts || []).some(([pi, pn]) => pi === i && pn === n);
          return `<span class="pt${p.leader === n ? " is-leader" : ""}${lit ? " hl" : ""}">P${i} · ${role}</span>`;
        }).join("");
        box.appendChild(h(`<div class="node${up ? "" : " down"}${(hl.nodes || []).includes(n) ? " hl" : ""}">
          <b>node-${n}</b><span>${up ? "broker + controller" : "down"}</span>
          ${up ? `<span class="tag">${k.controller === n ? "active controller" : "controller"}</span>` : ""}
          <div class="parts">${parts}</div></div>`));
      }
    };
    const qDraw = (hl = {}) => {
      const box = $(".nodes", qEl);
      box.innerHTML = "";
      for (const n of [1, 2, 3]) {
        const up = q.up[n], lead = q.leader === n;
        box.appendChild(h(`<div class="node${up ? "" : " down"}${(hl.nodes || []).includes(n) ? " hl" : ""}">
          <b>node-${n}</b><span>${!up ? "down" : lead ? "leader" : q.leader ? "follower" : "follower · electing"}</span>
          ${up ? `<span class="tag">${lead ? "orders every write" : "replicates, serves reads"}</span>` : ""}
          <div class="parts">${up ? `<span class="pt${(hl.log || []).includes(n) ? " hl" : ""}">the log · full copy</span>` : ""}</div></div>`));
      }
    };
    const say = (el, text) => { $(".arch-step", el).textContent = text; };
    const reset = () => {
      t.clear();
      k = { up: { 1: true, 2: true, 3: true }, controller: 1, parts: [{ leader: 1, isr: [1, 2, 3] }, { leader: 2, isr: [1, 2, 3] }, { leader: 3, isr: [1, 2, 3] }] };
      q = { up: { 1: true, 2: true, 3: true }, leader: 1 };
      kDraw(); qDraw();
      say(kEl, "Three brokers, three partitions, replication factor 3. Each partition has its leader on a different node.");
      say(qEl, "Three nodes, one raft group. The leader orders the writes; every node applies them to its own copy.");
      $('[data-act="fail"]', slide).disabled = false;
      $('[data-act="write"]', slide).disabled = false;
    };
    $('[data-act="write"]', slide).addEventListener("click", () => {
      t.clear();
      $('[data-act="write"]', slide).disabled = true;
      // Kafka: a record for P1.
      const p = k.parts[1], L = p.leader, F = p.isr.filter((n) => n !== L && k.up[n]);
      kDraw({ nodes: [L], parts: [[1, L]] });
      say(kEl, `1. The producer sends the record to P1's leader, node-${L}. The metadata told it where.`);
      t.after(1400, () => {
        kDraw({ nodes: [L, ...F], parts: [[1, L], ...F.map((n) => [1, n])] });
        say(kEl, `2. ${F.map((n) => `node-${n}`).join(" and ")} fetch the record from node-${L}.`);
      });
      t.after(2800, () => {
        kDraw({ nodes: [L], parts: [[1, L], ...F.map((n) => [1, n])] });
        say(kEl, `3. Every ISR replica has it: node-${L} answers (acks=all). By default Kafka leaves the fsync to the OS.`);
      });
      // Queen: the client talks to node-3.
      const QL = q.leader, others = [1, 2, 3].filter((n) => n !== QL && q.up[n]);
      qDraw({ nodes: [3, QL] });
      say(qEl, QL === 3 ? "1. The client sends to node-3, the leader." : `1. The client sends to node-3. node-3 prepares the command and passes it to the leader, node-${QL}.`);
      t.after(1400, () => {
        qDraw({ nodes: [QL, ...others], log: [QL, ...others] });
        say(qEl, `2. node-${QL} puts it in the next log entry and sends it to ${others.map((n) => `node-${n}`).join(" and ")}.`);
      });
      t.after(2800, () => {
        qDraw({ nodes: [3], log: [QL, ...others] });
        say(qEl, "3. A majority has written it and fsynced it: committed. node-3 answers its client from its own copy.");
        $('[data-act="write"]', slide).disabled = false;
      });
    });
    $('[data-act="fail"]', slide).addEventListener("click", () => {
      t.clear();
      $('[data-act="fail"]', slide).disabled = true;
      $('[data-act="write"]', slide).disabled = false;   // a write cut short by the crash can be sent again
      // Kafka: node-1 was the active controller and P0's leader.
      k.up[1] = false;
      k.controller = null;
      kDraw();
      say(kEl, "node-1 is gone: it was the active controller and P0's leader. Its session times out.");
      t.after(1500, () => {
        k.controller = 2;
        k.parts.forEach((p) => { p.isr = p.isr.filter((n) => n !== 1); if (p.leader === 1) p.leader = 2; });
        kDraw({ nodes: [2], parts: [[0, 2]] });
        say(kEl, "The two controllers left elect node-2. It gives P0 a new leader from the ISR, node-2, and drops node-1 from every ISR.");
      });
      // Queen: node-1 was the leader.
      q.up[1] = false;
      q.leader = null;
      qDraw();
      say(qEl, "node-1, the leader, is gone. The followers hear nothing for 1 to 2 seconds; the writes wait.");
      t.after(1500, () => {
        q.leader = 2;
        qDraw({ nodes: [2] });
        say(qEl, "They elect node-2. Committed writes were on a majority: nothing lost. With SIGTERM the handover takes a fraction of a second.");
      });
    });
    $('[data-act="reset"]', slide).addEventListener("click", reset);
    reset();
  }
