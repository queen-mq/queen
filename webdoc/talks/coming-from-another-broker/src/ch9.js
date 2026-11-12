
  // ==== In production ====

  // A flow diagram on a 6 × 3 grid: services, queues and outside systems, the arrows between
  // them, the Queen features as tags, and a message (the yellow dot) that walks a path.
  // Node ids name their cell: "B3" = row B, column 3.
  const FD_COL = 196, FD_ROW = 122;
  let fdSeq = 0;
  const flowDiagram = (el, spec) => {
    const uid = ++fdSeq;
    const card = el.closest(".prod-card");
    const caption = $(".fd-caption", card);
    // The legend sits on the eyebrow line, so the diagram keeps the whole card.
    el.closest(".slide").querySelector(".s-head").appendChild(h(`<div class="fd-legend">
      <span><i class="lg-svc"></i>service</span><span><i class="lg-q"></i>queue</span>
      <span><i class="lg-eph"></i>ephemeral queue</span><span><i class="lg-ext"></i>outside system</span></div>`));
    el.innerHTML = `<svg aria-hidden="true"><defs>
      <marker id="fd-a${uid}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" class="fd-arrowhead"/></marker>
      <marker id="fd-b${uid}" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" class="fd-arrowhead on"/></marker>
    </defs></svg>`;
    const svg = $("svg", el);
    const nodes = {};
    for (const [id, kind, name, sub, tags] of spec.nodes) {
      const col = +id[1], row = "ABC".indexOf(id[0]);
      const n = h(`<div class="fd-node ${kind}" style="left:${col * FD_COL}px;top:${row * FD_ROW}px">
        <b>${name.replace(/\./g, ".<wbr>")}</b>${sub ? `<small>${sub}</small>` : ""}${tags && tags.length ? `<div class="tags">${tags.map((x) => `<span>${x}</span>`).join("")}</div>` : ""}</div>`);
      el.appendChild(n);
      nodes[id] = n;
    }
    const token = h(`<span class="fd-token" hidden></span>`);
    el.appendChild(token);

    // Arrows need the laid-out nodes, so they are drawn the first time the slide is on screen.
    const edges = {};
    const boxOf = (n) => ({ x: n.offsetLeft, y: n.offsetTop, w: n.offsetWidth, h: n.offsetHeight });
    const pathFor = (a, b) => {
      const ca = { x: a.x + a.w / 2, y: a.y + a.h / 2 }, cb = { x: b.x + b.w / 2, y: b.y + b.h / 2 };
      const dx = cb.x - ca.x, dy = cb.y - ca.y;
      if (Math.abs(dx) > (a.w + b.w) / 2 - 4) {
        const s = Math.sign(dx);
        const x1 = s > 0 ? a.x + a.w : a.x, x2 = s > 0 ? b.x : b.x + b.w;
        const k = Math.max(24, Math.abs(x2 - x1) * 0.45);
        return `M${x1} ${ca.y} C${x1 + s * k} ${ca.y} ${x2 - s * k} ${cb.y} ${x2 - s * 3} ${cb.y}`;
      }
      const s = Math.sign(dy) || 1;
      const y1 = s > 0 ? a.y + a.h : a.y, y2 = s > 0 ? b.y : b.y + b.h;
      const k = Math.max(16, Math.abs(y2 - y1) * 0.45);
      return `M${ca.x} ${y1} C${ca.x} ${y1 + s * k} ${cb.x} ${y2 - s * k} ${cb.x} ${y2 - s * 3}`;
    };
    const side = (b, where) => ({
      top: { x: b.x + b.w / 2, y: b.y }, bottom: { x: b.x + b.w / 2, y: b.y + b.h },
      left: { x: b.x, y: b.y + b.h / 2 }, right: { x: b.x + b.w, y: b.y + b.h / 2 },
    })[where];
    const rounded = (pts, r = 10) => {
      let d = `M${pts[0][0]} ${pts[0][1]}`;
      for (let i = 1; i < pts.length - 1; i++) {
        const [x0, y0] = pts[i - 1], [x1, y1] = pts[i], [x2, y2] = pts[i + 1];
        const d1 = Math.hypot(x1 - x0, y1 - y0) || 1, d2 = Math.hypot(x2 - x1, y2 - y1) || 1;
        const r1 = Math.min(r, d1 / 2), r2 = Math.min(r, d2 / 2);
        d += ` L${x1 - ((x1 - x0) / d1) * r1} ${y1 - ((y1 - y0) / d1) * r1}`;
        d += ` Q${x1} ${y1} ${x1 + ((x2 - x1) / d2) * r2} ${y1 + ((y2 - y1) / d2) * r2}`;
      }
      const [xn, yn] = pts[pts.length - 1];
      return d + ` L${xn} ${yn}`;
    };
    // The free lanes of the grid: the middle of the gap under row r, right of column c.
    const gap = {
      y: (r) => {
        const bottoms = Object.entries(nodes).filter(([id]) => "ABC".indexOf(id[0]) === r).map(([, n]) => n.offsetTop + n.offsetHeight);
        return (Math.max(...bottoms) + (r + 1) * FD_ROW) / 2;
      },
      x: (c) => c * FD_COL + 160 + (FD_COL - 160) / 2,
    };
    const routed = (from, to, opt) => {
      const S = side(boxOf(nodes[from]), opt.from), E = side(boxOf(nodes[to]), opt.to);
      const end = { x: E.x, y: E.y + (opt.to === "bottom" ? 3 : opt.to === "top" ? -3 : 0) };
      return rounded([[S.x, S.y], ...opt.via(S, E, gap), [end.x, end.y]]);
    };
    let laid = false;
    const layout = () => {
      if (laid || !el.offsetWidth) return;
      laid = true;
      for (const [id, from, to, label, kind, opt] of spec.edges) {
        const p = document.createElementNS("http://www.w3.org/2000/svg", "path");
        p.setAttribute("d", opt ? routed(from, to, opt) : pathFor(boxOf(nodes[from]), boxOf(nodes[to])));
        p.setAttribute("class", `fd-edge ${kind || ""}`);
        p.setAttribute("marker-end", `url(#fd-a${uid})`);
        svg.appendChild(p);
        const len = p.getTotalLength();
        if (label) {
          const at = p.getPointAtLength(len * (opt?.labelAt ?? 0.5));
          el.appendChild(h(`<span class="fd-label" style="left:${at.x}px;top:${at.y}px">${label}</span>`));
        }
        edges[id] = { p, len, from, to };
      }
    };

    let play = null;
    const lit = (e, on) => {
      e.p.classList.toggle("on", on);
      e.p.setAttribute("marker-end", `url(#fd-${on ? "b" : "a"}${uid})`);
    };
    const clear = () => {
      Object.values(nodes).forEach((n) => n.classList.remove("on"));
      Object.values(edges).forEach((e) => lit(e, false));
      token.hidden = true;
    };
    const start = (name) => {
      layout();
      if (!laid) return;                           // not on screen yet: nothing to draw on
      clear();
      const pl = spec.plays[name];
      play = { steps: pl.steps, i: 0, t: -500 };
      nodes[edges[pl.steps[0][0]].from].classList.add("on");
      caption.textContent = pl.start;
      $$("[data-play]", card).forEach((b) => b.classList.toggle("primary", b.dataset.play === name));
    };
    $$("[data-play]", card).forEach((b) => b.addEventListener("click", () => start(b.dataset.play)));
    caption.textContent = spec.hint;
    tick(el, (dt) => {
      layout();
      if (!play) return;
      play.t += dt;
      if (play.t < 0) return;                      // a pause on the node it reached
      const [eid, say] = play.steps[play.i];
      const e = edges[eid];
      if (!e) { play = null; return; }
      const f = Math.min(1, play.t / 750);
      const at = e.p.getPointAtLength(e.len * f);
      token.hidden = false;
      nodes[e.from].classList.add("on");
      token.style.left = `${at.x}px`;
      token.style.top = `${at.y}px`;
      lit(e, true);
      if (f < 1) return;
      nodes[e.to].classList.add("on");
      caption.textContent = say;
      play.i++;
      play.t = -1700;
      if (play.i >= play.steps.length) play = null;
    });
  };

  // ---- 9.1 A guest chat ----
  flowDiagram($("#fd-chat"), {
    hint: "Each box is a real queue or service, with a simpler name. Press a button to follow a message.",
    nodes: [
      ["A0", "ext", "channel manager", "chats from the travel sites"],
      ["A1", "q", "chat.deliver", "", ["a group for the chat"]],
      ["A2", "svc", "integration", "channels in and out"],
      ["A3", "q", "incoming", "", ["partition = chat", "retries · DLQ"]],
      ["A4", "svc", "router", "the chat core", ["ack + push: one tx"]],
      ["A5", "q", "translate", "", ["its own group"]],
      ["B0", "eph", "chat.rpc", "", ["ephemeral", "replyTo inbox"]],
      ["B2", "q", "outgoing", "", ["a group per client"]],
      ["B3", "svc", "router", "the chat core", ["ack + push: one tx"]],
      ["B4", "q", "history", "", ["partition = chat"]],
      ["B5", "svc", "translator", "the AI service"],
      ["C1", "svc", "web app", "live, per client"],
      ["C2", "svc", "AI agent", "the AI service"],
      ["C3", "q", "agent", "", ["windowBuffer: a burst, one batch"]],
      ["C4", "q", "outgoing.email", "", ["email"]],
      ["C5", "ext", "email provider", "email out"],
    ],
    edges: [
      ["e1", "A0", "A1"], ["e2", "A1", "A2"], ["e3", "A2", "A3"], ["e4", "A3", "A4"], ["e5", "A4", "A5"],
      ["e6", "A5", "B5"], ["e7", "B5", "B4"], ["e8", "B4", "B3"], ["e9", "B3", "B2"], ["e10", "B2", "C1"],
      ["e11", "B3", "C3"], ["e12", "C3", "C2"], ["e13", "B3", "C4"], ["e14", "C4", "C5"],
      ["e15", "A2", "B0", "request", "eph"], ["e16", "B0", "A0", "answer", "eph"],
      ["e17", "C2", "A3", "its answer", "", {
        from: "top", to: "bottom", labelAt: 0.22,
        via: (S, E, g) => [[S.x, g.y(1)], [g.x(2), g.y(1)], [g.x(2), g.y(0)], [E.x, g.y(0)]],
      }],
    ],
    plays: {
      guest: {
        start: "A guest writes from a travel site. The channel manager publishes the message on chat.deliver.",
        steps: [
          ["e1", "chat.deliver: the chat reads it as its own consumer group."],
          ["e2", "The integration takes it…"],
          ["e3", "…and pushes it to incoming, in the partition of its chat: one chat, in order."],
          ["e4", "The router routes it."],
          ["e5", "One transaction: ack incoming, push translate."],
          ["e6", "The translator reads it in its own group."],
          ["e7", "It pushes the translated message to history."],
          ["e8", "The router stores it in the history."],
          ["e9", "One transaction again: ack history, push outgoing."],
          ["e10", "Each client reads outgoing with its own group: the web app shows the guest's message live."],
          ["e11", "The same router step also wakes the AI agent, on the agent queue."],
          ["e12", "The agent queue waits for the burst to end (windowBuffer) and hands it over as one batch."],
          ["e17", "The agent's answer is pushed to incoming as a new message…"],
          ["e4", "…and walks the same steps: router, translate, history, outgoing."],
        ],
      },
      ai: {
        start: "After the history, the router also wakes the AI agent.",
        steps: [
          ["e11", "The agent queue waits for the burst to end (windowBuffer) and hands it over as one batch."],
          ["e12", "The AI agent writes the answer."],
          ["e17", "The answer enters incoming as a new message…"],
          ["e4", "…and walks the same steps as the guest's message."],
        ],
      },
      rpc: {
        start: "The chat needs an answer from the channel manager, now.",
        steps: [
          ["e15", "It pushes a request on chat.rpc: in memory, with a private replyTo inbox."],
          ["e16", "The channel manager pops it and answers on the inbox. Nothing touched a disk."],
        ],
      },
    },
  });

  // ---- 9.2 A channel manager ----
  flowDiagram($("#fd-channel"), {
    hint: "Four lanes, one direction each. Press a button to follow one.",
    nodes: [
      ["A0", "ext", "writes", "APIs, operators"],
      ["A1", "q", "operations.in", "", ["partition = the state", "durable before the database"]],
      ["A2", "svc", "core", "one database tx + idempotency ledger", ["ack + push: one tx"]],
      ["A3", "q", "operations.out", "", ["a group per reader"]],
      ["A4", "q", "webhooks", "", ["retries on timers"]],
      ["A5", "ext", "subscribers", "webhooks out"],
      ["B1", "q", "sync.flush", "", ["partition = connection", "conflation"]],
      ["B2", "svc", "calendar sync", "reads the changes, pushes"],
      ["B3", "svc", "Gate", "egress rate limiter", ["kv.incr max = the limit", "answers: ephemeral"]],
      ["B4", "q", "chat.deliver", "", ["the chat bridge"]],
      ["B5", "ext", "guest chat", "the conversation facts"],
      ["C0", "ext", "hints, polls", "travel-site webhooks, schedules"],
      ["C1", "q", "sync.fetch", "", ["a key, not a payload", "conflation"]],
      ["C2", "svc", "fetch", "reads from the sites"],
      ["C3", "ext", "travel sites", "their APIs and limits"],
      ["C4", "q", "watches", "", ["fed by timers", "conflation"]],
      ["C5", "svc", "alerts", "watches, alerts"],
    ],
    edges: [
      ["e1", "A0", "A1"], ["e2", "A1", "A2"], ["e3", "A2", "A3"], ["e4", "A3", "A4"], ["e5", "A4", "A5"],
      ["e6", "A3", "B4"], ["e7", "B4", "B5"], ["e8", "A2", "B1"], ["e9", "B1", "B2"], ["e10", "B2", "B3"],
      ["e11", "B3", "C3"], ["e12", "C0", "C1"], ["e13", "C1", "C2"], ["e14", "C2", "B3"], ["e15", "C4", "C5"],
    ],
    plays: {
      op: {
        start: "An operator changes a price. The operation goes to the broker before the database is touched.",
        steps: [
          ["e1", "operations.in, in the partition of the state it writes: order only where it matters."],
          ["e2", "The core applies it in one database transaction, with the idempotency ledger."],
          ["e3", "One QueenMQ transaction: ack the operation, push the fact on operations.out."],
          ["e4", "Webhook delivery reads operations.out as its own group, and queues one delivery per target."],
          ["e5", "A failed call is retried on a QueenMQ timer, not an in-process scheduler."],
          ["e6", "The chat bridge reads the same lane as another group…"],
          ["e7", "…and hands the conversation facts to the guest chat."],
        ],
      },
      push: {
        start: "A calendar changed: a mark in the database, and a wake-up on sync.flush.",
        steps: [
          ["e8", "sync.flush, partition = the connection. Ten wake-ups waiting? Conflation runs the handler once."],
          ["e9", "The sync reads the changes and the calendar it should send."],
          ["e10", "It sends the push through Gate."],
          ["e11", "Gate admits it only if kv.incr stays under the site's limit, then calls the site. The answer comes back on an ephemeral queue."],
        ],
      },
      burst: {
        start: "Twenty hints for one connection: a travel site pings us twenty times.",
        steps: [
          ["e12", "Each hint is a key on sync.fetch, never a payload."],
          ["e13", "Conflation: one handler run for the burst, one read instead of twenty."],
          ["e14", "The read goes through Gate as well, paced under the site's limit."],
          ["e11", "Gate calls the site."],
        ],
      },
    },
  });

  // ---- 9.3 A tracing service ----
  flowDiagram($("#fd-tracing"), {
    hint: "One stateless process, any number of replicas. Press a button to follow a trace.",
    nodes: [
      ["A0", "ext", "SDKs", "our services, over HTTP"],
      ["A1", "svc", "tracing", "any replica, stateless"],
      ["A2", "q", "events", "", ["partition = trace id", "dedup by event key"]],
      ["A3", "svc", "stream", "session windows", ["one window per trace", "ack + state + push: one tx"]],
      ["A4", "q", "traces", "", ["one merged trace"]],
      ["B0", "ext", "audit, errors", "errors: a public key, a quota"],
      ["B1", "q", "audit<br>errors", ""],
      ["B5", "svc", "archiver", "in every replica", ["its own group", "minutes or megabytes"]],
      ["C2", "svc", "dashboard", "SQL over the files"],
      ["C5", "ext", "object storage", "Parquet files, by hour"],
    ],
    edges: [
      ["e1", "A0", "A1"], ["e2", "A1", "A2"], ["e3", "A2", "A3"], ["e4", "A3", "A4"], ["e5", "A4", "B5"],
      ["e6", "B0", "B1"], ["e7", "B1", "B5", "archived too"], ["e8", "B5", "C5"],
      ["e9", "C5", "C2", "the files"], ["e10", "A2", "C2", "the newest, by offset", "read"],
    ],
    plays: {
      trace: {
        start: "A service sends a span over HTTP.",
        steps: [
          ["e1", "Any replica of the tracing service takes it."],
          ["e2", "One push to events, partition = the trace id. A repeat with the same event key writes nothing."],
          ["e3", "The stream collects the trace's events in a session window."],
          ["e4", "When the session ends, one transaction acks the events, writes the window state and pushes the merged trace."],
          ["e5", "The archiver group pops traces, in every replica."],
          ["e8", "After a few minutes or megabytes: one Parquet file in object storage, then the ack."],
          ["e9", "The dashboard reads the files with SQL."],
        ],
      },
      read: {
        start: "The dashboard also needs the last minutes, which are not in a file yet.",
        steps: [
          ["e10", "It asks events which partitions changed (changedSince), and reads them by offset (fetch): no group, no lease."],
          ["e9", "Older traces come from the Parquet files."],
        ],
      },
    },
  });
