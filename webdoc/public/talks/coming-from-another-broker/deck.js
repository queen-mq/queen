(() => {

  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const html = document.documentElement;

  // ---- Theme, as the site: data-mode on <html> ----
  $("#mode").addEventListener("click", () => {
    const m = html.dataset.mode === "dark" ? "light" : "dark";
    html.dataset.mode = m;
    try { localStorage.setItem("qk-mode", m); } catch (e) {}
  });

  // ---- Deck ----
  const stage = $("#stage");
  const slides = $$(".slide");
  const flow = matchMedia("(max-width: 759px)");
  const prog = $("#prog");
  let cur = 0;

  // Each slide names its chapter; the first slide of a chapter starts a new group of the progress bar.
  slides.forEach((s, i) => {
    const b = document.createElement("button");
    b.setAttribute("aria-label", `Slide ${i + 1}: ${s.dataset.title}`);
    if (i > 0 && s.dataset.chapter !== slides[i - 1].dataset.chapter) b.classList.add("first");
    b.addEventListener("click", () => go(i));
    prog.appendChild(b);
  });

  function fit() {
    if (flow.matches) { stage.style.removeProperty("--s"); return; }
    stage.style.setProperty("--s", Math.min(innerWidth / 1280, innerHeight / 720));
  }
  function go(i) {
    cur = Math.max(0, Math.min(slides.length - 1, i));
    slides.forEach((s, j) => s.classList.toggle("on", j === cur));
    $$("button", prog).forEach((b, j) => {
      b.classList.toggle("on", j === cur);
      b.classList.toggle("done", j < cur);
    });
    $("#count").textContent = `${cur + 1} / ${slides.length}`;
    $("#chap").textContent = slides[cur].dataset.chapter;
    try { history.replaceState(null, "", "#" + (cur + 1)); } catch (e) {}
  }
  addEventListener("resize", fit);
  flow.addEventListener("change", fit);
  addEventListener("keydown", (e) => {
    if (flow.matches || e.metaKey || e.ctrlKey || e.altKey) return;
    const onButton = e.target instanceof HTMLButtonElement;
    if (["ArrowRight", "PageDown"].includes(e.key) || (e.key === " " && !onButton)) { e.preventDefault(); go(cur + 1); }
    else if (["ArrowLeft", "PageUp"].includes(e.key)) { e.preventDefault(); go(cur - 1); }
    else if (e.key === "Home") go(0);
    else if (e.key === "End") go(slides.length - 1);
    else if (e.key === "f" || e.key === "F") {
      if (document.fullscreenElement) document.exitFullscreen();
      else document.documentElement.requestFullscreen?.();
    }
  });
  let tx = null;
  addEventListener("touchstart", (e) => { tx = e.touches[0].clientX; }, { passive: true });
  addEventListener("touchend", (e) => {
    if (flow.matches || tx === null) return;
    const dx = e.changedTouches[0].clientX - tx;
    if (Math.abs(dx) > 60) go(cur + (dx < 0 ? 1 : -1));
    tx = null;
  });
  $("#prev").addEventListener("click", () => go(cur - 1));
  $("#next").addEventListener("click", () => go(cur + 1));
  const fromHash = () => go((parseInt(location.hash.slice(1), 10) || 1) - 1);
  addEventListener("hashchange", fromHash);
  fit();
  fromHash();

  // ---- A clock for the demos: it runs only while its demo is on screen ----
  const tickers = [];
  const visible = new Set();
  const io = new IntersectionObserver((entries) => {
    for (const e of entries) e.isIntersecting ? visible.add(e.target) : visible.delete(e.target);
  });
  const tick = (el, fn) => { tickers.push([el, fn]); io.observe(el); };
  let last = performance.now();
  requestAnimationFrame(function frame(now) {
    const dt = Math.min(100, now - last);
    last = now;
    for (const [el, fn] of tickers) if (visible.has(el)) fn(dt);
    requestAnimationFrame(frame);
  });

  // ---- Shared bits ----
  const ME = "booking-9137";
  const SLOW_B = "booking-4410";
  const h = (markup) => {
    const t = document.createElement("template");
    t.innerHTML = markup.trim();
    return t.content.firstElementChild;
  };
  const cell = (label, cls = "") => {
    const el = document.createElement("span");
    el.className = "cell " + cls;
    el.innerHTML = `<b>${label}</b><i></i>`;
    return el;
  };
  const laneRow = (name, cls = "") => {
    const row = document.createElement("div");
    row.className = "lane " + cls;
    row.innerHTML = `<div class="lane-name">${name}</div><div class="cells"></div><div class="more"></div>`;
    return row;
  };
  // Timeouts a demo can cancel all at once, on Reset.
  const timers = () => {
    let ids = [];
    return {
      after: (ms, fn) => ids.push(setTimeout(fn, ms)),
      clear: () => { ids.forEach(clearTimeout); ids = []; },
    };
  };
  // An answer line, as the broker's answers are shown in every demo.
  const answerRow = (left, mid, what = "", bad = false) =>
    h(`<div class="row"><span>${left}</span><span>${mid}</span>${what ? `<span class="what${bad ? " bad" : ""}">${what}</span>` : ""}</div>`);
  // A small segmented control; onPick(value) runs on every change.
  const segmented = (box, onPick) => {
    $$("button", box).forEach((b) => b.addEventListener("click", () => {
      $$("button", box).forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      onPick(b.dataset.v);
    }));
  };


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
  // ---- Cover figure (static) ----
  {
    const box = $("#cover-lanes");
    [[ME, 5, "me"], [SLOW_B, 3, ""], ["booking-2203", 6, ""], ["booking-7781", 2, ""]].forEach(([b, n, cls]) => {
      const row = laneRow(b, cls);
      for (let i = 0; i < n; i++) $(".cells", row).appendChild(cell(i, cls));
      box.appendChild(row);
    });
    const row = laneRow("booking-1408", "new");
    $(".cells", row).appendChild(cell(0, "ghost"));
    const h = document.createElement("span");
    h.className = "hint";
    h.textContent = "← created by its first push";
    $(".cells", row).appendChild(h);
    box.appendChild(row);
  }

  // ---- 1.1 + 1.2: the same bookings, Kafka partitions or Queen partitions ----
  const NORMAL = 650, SLOW = 6000, EVERY = 300;
  const START = [ME, SLOW_B, "booking-2203", "booking-7781", "booking-5120", "booking-3349", "booking-6012", "booking-8875"];
  const EXTRA = ["booking-1408", "booking-2950"];
  const KAFKA_P = {
    "booking-2203": 0, "booking-7781": 0, "booking-5120": 1, "booking-3349": 1,
    [ME]: 2, [SLOW_B]: 2, "booking-6012": 3, "booking-8875": 3, "booking-1408": 1, "booking-2950": 3,
  };

  class LaneSim {
    constructor(el) {
      this.el = el;
      this.kafka = el.dataset.sim === "kafka";
      el.dataset.mode = el.dataset.sim;
      el.innerHTML = `
        <div class="demo-head">
          <span class="mono">${this.kafka ? "topic bookings · 4 partitions · 1 consumer each" : "queue bookings · 1 partition per booking"}</span>
          <span class="legend"><i class="lg-me"></i>booking-9137 <i class="lg-slow"></i>slow message</span>
        </div>
        <div class="lanes"></div>
        <div class="sim-msg" aria-live="polite"></div>
        <div class="demo-foot">
          <div class="ro"><span class="ro-label">booking-9137 waits</span><span class="ro-num">0.0 s</span><span class="ro-note"></span></div>
          <div class="btns">
            <button class="btn" data-act="slow" aria-pressed="false">Make booking-4410 slow</button>
            <button class="btn" data-act="new">New booking</button>
            <button class="btn" data-act="reset">Reset</button>
          </div>
        </div>`;
      this.lanesEl = $(".lanes", el);
      this.ro = $(".ro", el);
      this.msg = $(".sim-msg", el);
      this.slowBtn = $('[data-act="slow"]', el);
      this.newBtn = $('[data-act="new"]', el);
      this.slowBtn.addEventListener("click", () => this.setSlow(!this.slow));
      this.newBtn.addEventListener("click", () => this.newBooking());
      $('[data-act="reset"]', el).addEventListener("click", () => this.reset());
      this.reset();
    }
    setSlow(on) {
      this.slow = on;
      // A booking-4410 message already in the worker's hands turns slow too.
      if (on && this.lanes) for (const l of this.lanes.values()) {
        const h = l.log[l.head];
        if (h && h.b === SLOW_B && h.start !== null) h.cost = SLOW;
      }
      this.slowBtn.setAttribute("aria-pressed", String(on));
      this.slowBtn.textContent = on ? "booking-4410 is slow" : "Make booking-4410 slow";
    }
    reset() {
      this.t = 0; this.next = 0; this.seq = 0; this.extra = 0; this.wait = 0;
      this.off = {};
      this.bookings = [...START];
      this.lanes = new Map();
      this.lanesEl.innerHTML = "";
      this.msg.textContent = "";
      this.newBtn.disabled = false;
      this.setSlow(false);
      if (this.kafka) for (let p = 0; p < 4; p++) this.lane("p" + p, "partition " + p, p === 2 ? "me" : "");
      else for (const b of this.bookings) this.lane(b, b, b === ME ? "me" : "");
      for (let i = 0; i < 120; i++) this.step(100); // warm up: some history in every lane
      this.render();
    }
    lane(key, name, cls) {
      let l = this.lanes.get(key);
      if (l) return l;
      const row = laneRow(name, cls);
      this.lanesEl.appendChild(row);
      // log: the partition's messages; head: the cursor, the first one not handled yet
      l = { row, cells: $(".cells", row), more: $(".more", row), log: [], head: 0, els: new Map() };
      this.lanes.set(key, l);
      return l;
    }
    keyOf(b) { return this.kafka ? "p" + KAFKA_P[b] : b; }
    pick() { return this.bookings[Math.floor(Math.random() * this.bookings.length)]; }
    push(b) {
      this.off[b] = (this.off[b] ?? -1) + 1;
      this.lane(this.keyOf(b), b).log.push({ id: ++this.seq, b, off: this.off[b], at: this.t, start: null, cost: 0, prog: 0 });
    }
    newBooking() {
      if (this.extra >= EXTRA.length) return;
      const b = EXTRA[this.extra++];
      this.bookings.push(b);
      if (this.kafka) {
        const p = KAFKA_P[b];
        const others = START.filter((x) => KAFKA_P[x] === p).join(" and ");
        this.say(`${b} → partition ${p}. No new partition: it now shares with ${others}.`);
      } else {
        const l = this.lane(b, b, "new");
        const h = document.createElement("span");
        h.className = "hint";
        h.textContent = "← created by the push that names it";
        l.row.querySelector(".more").before(h);
        l.cells.style.display = "none";
        setTimeout(() => { h.remove(); l.cells.style.display = ""; l.row.classList.remove("new"); }, 2600);
      }
      this.push(b);
      if (this.extra >= EXTRA.length) this.newBtn.disabled = true;
    }
    say(text) {
      this.msg.textContent = text;
      clearTimeout(this.sayT);
      this.sayT = setTimeout(() => { this.msg.textContent = ""; }, 5000);
    }
    step(dt) {
      this.t += dt;
      while (this.next <= this.t) {
        this.push(this.pick());
        this.next += EVERY * (0.6 + Math.random() * 0.8);
      }
      for (const l of this.lanes.values()) {
        const h = l.log[l.head];
        if (!h) continue;
        if (h.start === null) {
          h.start = this.t;
          h.cost = this.slow && h.b === SLOW_B ? SLOW : NORMAL * (0.7 + Math.random() * 0.6);
          if (h.b === ME) this.wait = h.start - h.at;
        }
        h.prog = (this.t - h.start) / h.cost;
        if (h.prog >= 1) l.head++;
        if (l.head > 60) { l.log.splice(0, 30); l.head -= 30; } // keep the log short
      }
    }
    myWait() {
      let w = null;
      for (const l of this.lanes.values())
        for (let i = l.head; i < l.log.length; i++) {
          const m = l.log[i];
          if (m.b === ME && m.start === null) w = Math.max(w ?? 0, this.t - m.at);
        }
      return w ?? this.wait;
    }
    render() {
      for (const l of this.lanes.values()) {
        const cap = Math.max(3, Math.floor((l.cells.clientWidth + 4) / 38)) || 12;
        // As many handled messages as fit before the cursor; the pending ones after it.
        const start = Math.max(0, Math.min(l.head, l.log.length - cap));
        const vis = l.log.slice(start, start + cap);
        const ids = new Set(vis.map((m) => m.id));
        for (const [id, el] of l.els) if (!ids.has(id)) { el.remove(); l.els.delete(id); }
        let prev = null;
        vis.forEach((m, j) => {
          let el = l.els.get(m.id);
          if (!el) {
            el = cell(this.kafka ? m.b.slice(-4) : m.off, m.b === ME ? "me" : "");
            l.els.set(m.id, el);
          }
          const want = prev ? prev.nextSibling : l.cells.firstChild;
          if (el !== want) l.cells.insertBefore(el, want);
          prev = el;
          const i = start + j;
          const busy = i === l.head && m.start !== null;
          el.classList.toggle("done", i < l.head);
          el.classList.toggle("busy", busy);
          el.classList.toggle("slow", busy && m.cost === SLOW);
          el.lastChild.style.width = busy ? Math.min(100, m.prog * 100) + "%" : "0";
        });
        const rest = l.log.length - (start + vis.length);
        l.more.textContent = rest > 0 ? "+" + rest : "";
      }
      const w = this.myWait() / 1000;
      const bad = w > 2;
      this.ro.classList.toggle("bad", bad);
      $(".ro-num", this.ro).textContent = w.toFixed(1) + " s";
      $(".ro-note", this.ro).textContent = this.kafka
        ? (bad ? "stuck behind booking-4410 in partition 2" : "shares partition 2 with booking-4410")
        : (this.slow ? "booking-4410 is slow in its own partition" : "its own partition");
    }
  }

  for (const el of $$(".sim")) {
    const s = new LaneSim(el);
    tick(el, (dt) => { s.step(dt); s.render(); });
  }

  // ---- 1.3 Dedup ----
  {
    const el = $("#dedup");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">partition booking-9137 · dedup window 3600 s</span></div>
      <div class="lanes"></div>
      <p class="sub-label">The broker's answer, per item</p>
      <div class="answers"></div>
      <div class="demo-foot">
        <span></span>
        <div class="btns">
          <button class="btn primary" data-act="push">Push</button>
          <button class="btn" data-act="same">Retry, same transactionId</button>
          <button class="btn" data-act="uuid">Retry, new UUID</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const lanes = $(".lanes", el), answers = $(".answers", el);
    let row, ids, n;
    const reset = () => {
      lanes.innerHTML = "";
      row = laneRow(ME, "me");
      lanes.appendChild(row);
      ids = new Set(); n = 0;
      answers.innerHTML = `<div class="empty">Nothing pushed yet.</div>`;
    };
    const answer = (status, id, what, bad) => {
      $(".empty", answers)?.remove();
      const r = document.createElement("div");
      r.className = "row";
      r.innerHTML = `<span>201</span><span>{ status: '<span class="st">${status}</span>', transactionId: '${id}' }</span><span class="what${bad ? " bad" : ""}">${what}</span>`;
      answers.prepend(r);
      while (answers.children.length > 3) answers.lastChild.remove();
    };
    const push = (kind) => {
      const id = kind === "uuid" ? crypto.randomUUID().slice(0, 8) + "-…" : "booking-9137-created";
      if (ids.has(id)) { answer("duplicate", id, "nothing written: the first attempt got through"); return; }
      ids.add(id);
      const second = kind === "uuid" && n > 0;
      $(".cells", row).appendChild(cell(n, second ? "bad" : "me"));
      answer("queued", id, second ? `offset ${n} written: booking-9137 is now created twice` : `offset ${n} written`, second);
      n++;
    };
    $('[data-act="push"]', el).addEventListener("click", () => push("first"));
    $('[data-act="same"]', el).addEventListener("click", () => push("same"));
    $('[data-act="uuid"]', el).addEventListener("click", () => push("uuid"));
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }


  // ==== Chapter 2 · Work it, fail it, retry it ====

  // Keeps a strip's cells in step with a partition: n messages, the group's cursor at head.
  // Cells are reused, so their entry animation runs once.
  const syncCells = (box, n, head, o = {}) => {
    while (box.children.length < n) {
      const i = box.children.length;
      box.appendChild(cell(o.label ? o.label(i) : i, o.me ? "me" : ""));
    }
    while (box.children.length > n) box.lastChild.remove();
    [...box.children].forEach((c, i) => {
      const at = i === head;
      c.classList.toggle("done", i < head);
      c.classList.toggle("busy", at && !!o.busy);
      c.classList.toggle("bad", at && !!o.bad);
      c.classList.toggle("ghost", !!o.ghost && o.ghost(i));
      c.lastChild.style.width = at && o.prog != null ? Math.min(100, o.prog * 100) + "%" : "0";
    });
  };

  // Where a cursor at k points: the middle of cell k, or just past the last cell. Read from the
  // laid-out cells, so it holds at any cell size; divided by the deck's scale.
  const markerX = (cells, k) => {
    const kids = cells.children;
    if (!kids.length) return 17;
    const base = cells.getBoundingClientRect();
    const scale = base.width / (cells.offsetWidth || 1) || 1;
    const c = kids[Math.min(k, kids.length - 1)].getBoundingClientRect();
    const mid = k < kids.length ? c.left + c.width / 2 : c.right + 4 + c.width / 2;
    return (mid - base.left) / scale;
  };

  // ---- 2.0 Cover figure ----
  {
    const box = $("#c2-lanes");
    for (const [b, n, head, cls] of [[ME, 6, 3, "me"], ["booking-2203", 8, 6, ""], [SLOW_B, 5, 3, ""]]) {
      const r = laneRow(b, cls);
      box.appendChild(r);
      syncCells($(".cells", r), n, head, { me: cls === "me", busy: true, prog: 0.55 });
    }
  }

  // ---- 2.1 Kafka: retry in place, or a retry topic ----
  {
    const el = $("#k-retry");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">topic bookings · partition 2 · group confirm</span></div>
      <div class="chips" data-r="main"></div>
      <div class="sys" data-r="side" hidden>
        <div class="box yours mono">bookings-retry<small>a topic, and a consumer you run</small></div>
        <span class="arrow">→</span>
        <div class="box yours mono">bookings-dlt<small>one more</small></div>
      </div>
      <div class="chips" data-r="retry"></div>
      <div class="demo-foot">
        <div class="ro"><span class="ro-label">partition 2</span><span class="ro-num"></span><span class="ro-note"></span></div>
        <div class="btns">
          <button class="btn" data-act="place">Retry in place</button>
          <button class="btn" data-act="topic">Send to a retry topic</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const main = $('[data-r="main"]', el), side = $('[data-r="side"]', el), retry = $('[data-r="retry"]', el);
    const ro = $(".ro", el), t = timers();
    const MSGS = [["4410", "created"], ["9137", "created"], ["4410", "paid"], ["9137", "paid"], ["4410", "confirmed"]];
    const chip = ([b, ev], cls = "") => h(`<span class="chip ${b === "9137" ? "me" : ""} ${cls}"><i></i>${b} ${ev}</span>`);
    const say = (num, note, bad) => {
      $(".ro-num", ro).textContent = num;
      $(".ro-note", ro).textContent = note;
      ro.classList.toggle("bad", !!bad);
    };
    const buttons = (on) => $$('[data-act="place"], [data-act="topic"]', el).forEach((b) => { b.disabled = !on; });
    const reset = () => {
      t.clear();
      main.innerHTML = ""; retry.innerHTML = ""; side.hidden = true;
      MSGS.forEach((m, i) => main.appendChild(chip(m, i === 0 ? "done" : i === 1 ? "bad" : "")));
      say("offset 1", "booking-9137 created fails: the card service is down.");
      buttons(true);
    };
    $('[data-act="place"]', el).addEventListener("click", () => {
      reset();
      buttons(false);
      const failing = main.children[1];
      let n = 1;
      const again = () => {
        n++;
        failing.lastChild.textContent = `9137 created · try ${n}`;
        say("blocked", `try ${n}. Three messages wait behind it, two of them booking-4410's.`, true);
        if (n < 8) t.after(1200, again);
        else { say("blocked", "…and so on, until somebody fixes the card service.", true); buttons(true); }
      };
      t.after(400, again);
    });
    $('[data-act="topic"]', el).addEventListener("click", () => {
      reset();
      buttons(false);
      const failing = main.children[1];
      failing.remove();
      side.hidden = false;
      retry.appendChild(chip(MSGS[1], "bad"));
      say("offset 2", "9137 created went to bookings-retry. The partition goes on.");
      const next = (k) => {
        const c = main.children[k];
        if (!c) {
          t.after(1600, () => {
            retry.firstChild.classList.remove("bad");
            retry.firstChild.classList.add("done");
            say("out of order", "9137 created comes last, from the retry topic.", true);
            buttons(true);
          });
          return;
        }
        c.classList.add("done");
        if (c.textContent.includes("9137 paid")) {
          c.classList.remove("done");
          c.classList.add("bad");
          say("out of order", "booking-9137: paid handled before created.", true);
        }
        t.after(900, () => next(k + 1));
      };
      t.after(900, () => next(1));
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 2.2 Queen: a lease, an answer, a retry, the dead letters ----
  {
    const el = $("#q-retry");
    const SPEED = 5, LEASE = 30, DELAY = 5, LIMIT = 5;
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queue bookings · group confirm</span><span class="speed">the clock runs ×5</span></div>
      <div class="lanes"></div>
      <div class="workers">
        <div class="worker" data-w="a"><b>worker-a</b><span></span></div>
        <div class="worker" data-w="b"><b>worker-b</b><span></span></div>
      </div>
      <div class="listbox"><span class="lb-title">Dead-letter queue · confirm</span><div class="lb-rows"></div></div>
      <div class="demo-foot">
        <div class="ro"><span class="ro-label"></span><span class="ro-num"></span><span class="ro-note"></span></div>
        <div class="btns">
          <button class="btn primary" data-act="ok">Complete</button>
          <button class="btn" data-act="fail">Fail</button>
          <button class="btn" data-act="kill">Kill worker-a</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const lanes = $(".lanes", el), ro = $(".ro", el), rows = $(".lb-rows", el);
    const wa = $('[data-w="a"]', el), wb = $('[data-w="b"]', el);
    const OTHERS = ["booking-2203", SLOW_B];
    const rowsOf = {};
    for (const b of [ME, ...OTHERS]) { rowsOf[b] = laneRow(b, b === ME ? "me" : ""); lanes.appendChild(rowsOf[b]); }
    let st;
    const reset = () => {
      st = {
        t: 0, aAlive: true,
        me: { n: 6, head: 3 }, phase: "held", holder: "a", until: 0, failures: 0, delivery: 1,
        others: { "booking-2203": { n: 7, head: 5, p: 0 }, [SLOW_B]: { n: 5, head: 3, p: 0.5 } },
        dlq: [], flash: null,
      };
      draw();
    };
    const nextMessage = () => {
      st.me.head++;
      if (st.me.head >= st.me.n - 1) st.me.n++;
      st.phase = "held"; st.holder = st.aAlive ? "a" : "b"; st.failures = 0; st.delivery = 1;
    };
    const step = (dt) => {
      st.t += (dt / 1000) * SPEED;
      for (const o of Object.values(st.others)) {
        o.p += (dt / 1000) * SPEED / 2.5;
        if (o.p >= 1) { o.p = 0; o.head++; if (o.head >= o.n - 1) o.n++; }
      }
      if (st.phase !== "held" && st.t >= st.until) {
        st.phase = "held"; st.holder = st.aAlive ? "a" : "b"; st.delivery++;
      }
      draw();
    };
    const draw = () => {
      const { me, phase } = st;
      const left = Math.max(0, Math.ceil(st.until - st.t));
      const p = phase === "back" ? 1 - (st.until - st.t) / DELAY : phase === "orphan" ? 1 - (st.until - st.t) / LEASE : null;
      syncCells($(".cells", rowsOf[ME]), me.n, me.head, { me: true, busy: phase !== "back", bad: phase === "back", prog: p });
      for (const b of OTHERS) {
        const o = st.others[b];
        syncCells($(".cells", rowsOf[b]), o.n, o.head, { busy: true, prog: o.p });
      }
      const holdsMe = (w) => phase === "held" && st.holder === w;
      const other = st.others["booking-2203"];
      wa.classList.toggle("off", !st.aAlive);
      wa.classList.toggle("holds", st.aAlive && holdsMe("a"));
      $("span", wa).textContent = !st.aAlive ? "killed" : holdsMe("a") ? `holds booking-9137 #${me.head} · handler running` : `holds ${SLOW_B} #${st.others[SLOW_B].head}`;
      wb.classList.toggle("holds", holdsMe("b"));
      $("span", wb).textContent = holdsMe("b") ? `holds booking-9137 #${me.head} · handler running` : `holds booking-2203 #${other.head}`;
      $(".ro-label", ro).textContent = `booking-9137 #${me.head} · delivery ${st.delivery}`;
      $(".ro-num", ro).textContent = `${st.failures} of ${LIMIT} failures`;
      let note = phase === "held" ? `with worker-${st.holder}: Complete or Fail it`
        : phase === "back" ? `failed: back in ${left} s, and only booking-9137 waits`
        : `worker-a is gone: the lease runs out in ${left} s`;
      let bad = false;
      if (st.flash && st.t < st.flash.until) { note = st.flash.text; bad = true; }
      $(".ro-note", ro).textContent = note;
      ro.classList.toggle("bad", bad);
      rows.innerHTML = st.dlq.length ? st.dlq.map((r) => `<span class="lb-row">${r}</span>`).join("") : `<span class="lb-empty">Empty.</span>`;
      $('[data-act="ok"]', el).disabled = phase !== "held";
      $('[data-act="fail"]', el).disabled = phase !== "held";
      $('[data-act="kill"]', el).disabled = !st.aAlive;
    };
    $('[data-act="ok"]', el).addEventListener("click", () => { st.flash = null; nextMessage(); draw(); });
    $('[data-act="fail"]', el).addEventListener("click", () => {
      st.failures++;
      if (st.failures >= LIMIT) {
        st.dlq.push(`booking-9137 #${st.me.head} · card service timeout · ${LIMIT} failures`);
        st.flash = { text: `#${st.me.head} went to the dead-letter queue. The booking moved on.`, until: st.t + 20 };
        nextMessage();
      } else {
        st.phase = "back"; st.until = st.t + DELAY;
      }
      draw();
    });
    $('[data-act="kill"]', el).addEventListener("click", () => {
      st.aAlive = false;
      if (st.phase === "held" && st.holder === "a") { st.phase = "orphan"; st.until = st.t + LEASE; }
      draw();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
    tick(el, step);
  }

  // ---- 2.3 One pop, many bookings ----
  {
    const el = $("#drain");
    const NAMES = [ME, SLOW_B, "booking-2203", "booking-7781", "booking-5120", "booking-3349", "booking-6012", "booking-8875"];
    const PENDING = [3, 1, 2, 2, 1, 3, 2, 1];
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queue bookings · group confirm · batch 100</span></div>
      <div class="lanes"></div>
      <p class="sim-msg"></p>
      <div class="demo-foot">
        <div class="ro"><span class="ro-label">round trips</span><span class="ro-num">0</span><span class="ro-note"></span></div>
        <div class="ctl-row">
          <span class="seg-label mono">.partitions(</span>
          <div class="seg"><button data-v="1" aria-pressed="true">1</button><button data-v="4" aria-pressed="false">4</button><button data-v="8" aria-pressed="false">8</button></div>
          <span class="seg-label mono">)</span>
          <button class="btn primary" data-act="pop">Pop</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const lanes = $(".lanes", el), msg = $(".sim-msg", el), ro = $(".ro", el);
    const rows = NAMES.map((b) => { const r = laneRow(b, b === ME ? "me" : ""); lanes.appendChild(r); return r; });
    let left, trips, per = 1, busy = false;
    const total = PENDING.reduce((a, b) => a + b, 0);
    const draw = (claimed = []) => {
      rows.forEach((r, i) => {
        r.classList.toggle("claimed", claimed.includes(i));
        syncCells($(".cells", r), 2 + PENDING[i], 2 + PENDING[i] - left[i], { me: NAMES[i] === ME, busy: claimed.includes(i) && left[i] > 0, prog: 0.6 });
      });
      $(".ro-num", ro).textContent = trips;
      $('[data-act="pop"]', el).disabled = busy || left.every((x) => x === 0);
    };
    const reset = () => {
      left = [...PENDING]; trips = 0; busy = false;
      msg.textContent = `${total} messages waiting in 8 bookings.`;
      $(".ro-note", ro).textContent = "";
      draw();
    };
    segmented($(".seg", el), (v) => { per = +v; reset(); });
    $('[data-act="pop"]', el).addEventListener("click", () => {
      const claimed = left.map((x, i) => (x > 0 ? i : -1)).filter((i) => i >= 0).slice(0, per);
      const got = claimed.reduce((a, i) => a + left[i], 0);
      trips++; busy = true;
      draw(claimed);
      msg.textContent = `This pop: ${claimed.length} booking${claimed.length > 1 ? "s" : ""}, ${got} message${got > 1 ? "s" : ""}, each booking in its order.`;
      setTimeout(() => {
        claimed.forEach((i) => { left[i] = 0; });
        busy = false;
        draw();
        if (left.every((x) => x === 0)) {
          msg.textContent = `All ${total} messages in ${trips} round trip${trips > 1 ? "s" : ""}.`;
          $(".ro-note", ro).textContent = per === 1 ? "try .partitions(8)" : "";
        }
      }, 650);
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 2.4 New groups, seek and retention ----
  {
    const el = $("#seek");
    // One message a day from 4 November, two on the 10th and 11th. Now: 11 Nov 16:00. Retention: 7 days.
    const MSGS = [[4, 9], [5, 9], [6, 9], [7, 9], [8, 9], [9, 9], [10, 9], [10, 15], [11, 9], [11, 15]];
    const N = MSGS.length;
    const FROM_10_NOV_10 = 7;   // the first message at or after 10 Nov 10:00 is 10 Nov 15:00
    const removed = (i) => MSGS[i][0] === 4 && MSGS[i][1] < 16;   // older than 4 Nov 16:00
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queue bookings · partition booking-2203</span><span class="speed">now: 11 Nov 16:00 · retention 7 days</span></div>
      <div class="lanes"></div>
      <div class="axis" style="margin-left:134px"></div>
      <div class="tracks"></div>
      <p class="sim-msg"></p>
      <div class="demo-foot">
        <span></span>
        <div class="btns">
          <button class="btn primary" data-act="audit">New group audit</button>
          <button class="btn" data-act="back">Seek confirm to 10 Nov</button>
          <button class="btn" data-act="end">Seek to the end</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const row = laneRow("booking-2203");
    $(".lanes", el).appendChild(row);
    const axis = $(".axis", el);
    MSGS.forEach(([d], i) => { if (i === 0 || MSGS[i - 1][0] !== d) axis.appendChild(h(`<span style="left:${i * 38 + 4}px">${d === 4 ? "4 Nov" : d}</span>`)); });
    const tracks = $(".tracks", el), msg = $(".sim-msg", el);
    const track = (name) => {
      const t = { name, at: N, p: 0, el: h(`<div class="track"><span class="track-name">${name}</span><div class="track-line"><span class="marker"></span></div></div>`) };
      tracks.appendChild(t.el);
      return t;
    };
    let groups;
    const confirm = () => groups[0];
    const draw = () => {
      syncCells($(".cells", row), N, -1, { ghost: removed, label: (i) => (removed(i) ? "" : i) });
      for (const g of groups) {
        const m = $(".marker", g.el);
        const x = markerX($(".cells", row), g.at);
        m.style.left = x + "px";
        m.classList.toggle("flip", x > 340 || x > $(".cells", row).offsetWidth * 0.6);
        m.textContent = g.at >= N ? `at ${g.at} · caught up` : `at ${g.at}`;
      }
      $('[data-act="audit"]', el).disabled = groups.length > 1;
    };
    const reset = () => {
      tracks.innerHTML = "";
      groups = [track("confirm")];
      msg.textContent = "The message of 4 Nov is gone: older than 7 days. The rest stay, read or not.";
      draw();
    };
    $('[data-act="audit"]', el).addEventListener("click", () => {
      const g = track("audit");
      g.at = FROM_10_NOV_10;
      groups.push(g);
      msg.textContent = "audit starts at 10 Nov 15:00, the first message after 10:00, and catches up. confirm does not move.";
      draw();
    });
    $('[data-act="back"]', el).addEventListener("click", () => {
      confirm().at = FROM_10_NOV_10; confirm().p = 0;
      msg.textContent = "confirm reads again from 10 Nov 15:00, the first message after 10:00.";
      draw();
    });
    $('[data-act="end"]', el).addEventListener("click", () => {
      const c = confirm();
      msg.textContent = c.at < N ? `confirm skips the ${N - c.at} it had not read yet.` : "confirm is already at the end.";
      c.at = N;
      draw();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
    tick(el, (dt) => {
      for (const g of groups) if (g.at < N) { g.p += dt; if (g.p >= 1400) { g.p = 0; g.at++; } }
      draw();
    });
  }

  // ==== Chapter 3 · One step = one commit ====

  // A small "what is written now" table, shared by the step demos.
  const stateTable = (rows) => h(`
    <table class="opts state">
      <thead><tr><th>where</th><th>now</th></tr></thead>
      <tbody>${rows.map(([k, label]) => `<tr><td>${label}</td><td class="v2" data-k="${k}">·</td></tr>`).join("")}</tbody>
    </table>`);
  const setState = (table, values) => {
    for (const [k, [v, bad]] of Object.entries(values)) {
      const td = $(`[data-k="${k}"]`, table);
      td.textContent = v;
      td.classList.toggle("bad", !!bad);
    }
  };

  // ---- 3.1 Kafka: three writes, three systems ----
  {
    const el = $("#k-tx");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">consumer · confirm booking-9137</span></div>
      <div class="steps"></div>
      <div class="state-wrap"></div>
      <p class="verdict"></p>
      <div class="demo-foot">
        <span></span>
        <div class="btns">
          <button class="btn" data-act="1">Crash after ①</button>
          <button class="btn" data-act="2">Crash after ②</button>
          <button class="btn" data-act="3">Offset first, then crash</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const STEPS = {
      db: ["①", "<code>UPDATE bookings SET status = 'confirmed'</code>", "Postgres"],
      mail: ["②", "<code>produce emails · booking-9137</code>", "Kafka"],
      off: ["③", "<code>commit offset 4</code>", "Kafka"],
    };
    const stepsEl = $(".steps", el), verdict = $(".verdict", el), t = timers();
    const ready = () => $$("[data-act]", el).forEach((b) => { b.disabled = false; });
    const table = stateTable([["db", "Postgres · booking-9137"], ["mail", "emails sent to the guest"], ["off", "offset 4 committed"]]);
    $(".state-wrap", el).appendChild(table);
    let s;
    const show = (order, crashAfter) => {
      stepsEl.innerHTML = "";
      order.forEach((k, i) => {
        const [n, what, sys] = STEPS[k];
        stepsEl.appendChild(h(`<div class="stp" data-s="${k}"><span class="n">${n}</span><span>${what}</span><span class="sys-name">${sys}</span></div>`));
        if (i === crashAfter) stepsEl.appendChild(h(`<div class="crash" hidden><span>crash</span></div>`));
      });
    };
    const paint = () => setState(table, {
      db: [s.db ? "confirmed" : "pending"],
      mail: [String(s.mail), s.mail > 1],
      off: [s.off ? "yes" : "no"],
    });
    const run = (order, crashAfter, onDone) => {
      let i = 0;
      const next = () => {
        if (i > crashAfter) { const c = $(".crash", stepsEl); if (c) c.hidden = false; return t.after(900, onDone); }
        const k = order[i++];
        $(`[data-s="${k}"]`, stepsEl).classList.add("done");
        if (k === "db") s.db = true;
        if (k === "mail") s.mail++;
        if (k === "off") s.off = true;
        paint();
        t.after(700, next);
      };
      t.after(300, next);
    };
    const replay = (order, then) => {
      $$(".stp", stepsEl).forEach((x) => x.classList.remove("done"));
      $(".crash", stepsEl)?.remove();
      verdict.textContent = "Restart. The offset was not committed, so the message comes back and the step runs again.";
      run(order, order.length - 1, then);
    };
    const go = (mode) => {
      reset();
      $$("[data-act]", el).forEach((b) => { if (b.dataset.act !== "reset") b.disabled = true; });
      const normal = ["db", "mail", "off"];
      if (mode === "1") {
        show(normal, 0);
        run(normal, 0, () => replay(normal, () => {
          verdict.textContent = "This time it ends well, because the UPDATE can run twice. Not every write can.";
          ready();
        }));
      } else if (mode === "2") {
        show(normal, 1);
        run(normal, 1, () => replay(normal, () => {
          verdict.classList.add("bad");
          verdict.textContent = "The guest gets two emails.";
          ready();
        }));
      } else {
        const first = ["off", "db", "mail"];
        show(first, 0);
        run(first, 0, () => {
          verdict.classList.add("bad");
          verdict.textContent = "Restart. The offset already moved: the message never comes back. The booking is never confirmed, and nobody knows.";
          ready();
        });
      }
    };
    const reset = () => {
      t.clear();
      s = { db: false, mail: 0, off: false };
      show(["db", "mail", "off"], -1);
      verdict.classList.remove("bad");
      verdict.textContent = "Pick where the worker crashes.";
      paint();
      $$("[data-act]", el).forEach((b) => { b.disabled = false; });
    };
    $$("[data-act]", el).forEach((b) => b.addEventListener("click", () => (b.dataset.act === "reset" ? reset() : go(b.dataset.act))));
    reset();
  }

  // ---- 3.2 Queen: one transaction ----
  {
    const el = $("#q-tx");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">worker-a · confirm booking-9137</span></div>
      <div class="commit-box">
        <span class="cb-title">queen.transaction() … .commit()</span>
        <div class="stp"><span class="n">ack</span><span><code>bookings</code> · booking-9137 #3</span><span class="sys-name">QueenMQ</span></div>
        <div class="stp"><span class="n">kv</span><span><code>bookings/booking-9137</code> = confirmed</span><span class="sys-name">QueenMQ</span></div>
        <div class="stp"><span class="n">push</span><span><code>emails</code> · mail-booking-9137</span><span class="sys-name">QueenMQ</span></div>
      </div>
      <div class="state-wrap"></div>
      <p class="verdict"></p>
      <div class="demo-foot">
        <span></span>
        <div class="btns">
          <button class="btn" data-act="before">Crash before the commit</button>
          <button class="btn" data-act="after">Crash after it</button>
          <button class="btn" data-act="lease">The lease ran out</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const box = $(".commit-box", el), verdict = $(".verdict", el), head = $(".demo-head .mono", el), t = timers();
    const ready = () => $$("[data-act]", el).forEach((b) => { b.disabled = false; });
    const table = stateTable([["kv", "bookings/booking-9137"], ["mail", "messages in emails"], ["ack", "booking-9137 #3"]]);
    $(".state-wrap", el).appendChild(table);
    let s;
    const paint = () => setState(table, {
      kv: [s.kv ? "confirmed" : "absent"],
      mail: [String(s.mail), s.mail > 1],
      ack: [s.ack ? "completed" : s.holder ? `leased to ${s.holder}` : "waiting"],
    });
    const commit = () => {
      box.classList.add("done");
      $$(".stp", box).forEach((x) => x.classList.add("done"));
      s.kv = true; s.mail++; s.ack = true;
      paint();
    };
    const clearBox = () => {
      box.classList.remove("done");
      $$(".stp", box).forEach((x) => x.classList.remove("done", "lost"));
    };
    const reset = () => {
      t.clear();
      s = { kv: false, mail: 0, ack: false, holder: "worker-a" };
      clearBox();
      head.textContent = "worker-a · confirm booking-9137";
      verdict.classList.remove("bad");
      verdict.textContent = "Pick where the worker crashes.";
      paint();
      $$("[data-act]", el).forEach((b) => { b.disabled = false; });
    };
    const go = (mode) => {
      reset();
      $$("[data-act]", el).forEach((b) => { if (b.dataset.act !== "reset") b.disabled = true; });
      if (mode === "before") {
        $$(".stp", box).forEach((x) => x.classList.add("lost"));
        verdict.textContent = "worker-a dies before the commit. Nothing is written.";
        s.holder = null; paint();
        t.after(1500, () => {
          clearBox();
          s.holder = "worker-b"; paint();
          head.textContent = "worker-b · confirm booking-9137";
          verdict.textContent = "The lease runs out, worker-b gets the message and runs the step again.";
          t.after(1300, () => {
            commit();
            verdict.textContent = "Committed once, whole. Nothing was ever half done.";
            ready();
          });
        });
      } else if (mode === "after") {
        t.after(500, () => {
          commit();
          verdict.textContent = "Committed. worker-a dies a moment later: too late to matter, all three are written.";
          ready();
        });
      } else {
        verdict.textContent = "worker-a is slow. Its lease runs out, and worker-b gets the message.";
        s.holder = "worker-b"; paint();
        t.after(1500, () => {
          $$(".stp", box).forEach((x) => x.classList.add("lost"));
          verdict.classList.add("bad");
          verdict.textContent = "worker-a commits late: success: false, reason 'rejected_ack'. Nothing of its call is written.";
          t.after(1800, () => {
            clearBox();
            head.textContent = "worker-b · confirm booking-9137";
            commit();
            verdict.classList.remove("bad");
            verdict.textContent = "worker-b's commit is the only one: one state, one email, one ack.";
            ready();
          });
        });
      }
    };
    $$("[data-act]", el).forEach((b) => b.addEventListener("click", () => (b.dataset.act === "reset" ? reset() : go(b.dataset.act))));
    reset();
  }

  // ---- 3.3 KV: versions and conditions ----
  {
    const el = $("#kv");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">kv · namespace bookings</span></div>
      <table class="opts">
        <thead><tr><th>key</th><th>value</th><th>version</th></tr></thead>
        <tbody><tr><td>booking-9137</td><td class="v2" data-k="value"></td><td class="v2" data-k="version"></td></tr></tbody>
      </table>
      <div class="workers">
        <div class="worker"><b>worker-a</b><span>read version 3</span></div>
        <div class="worker"><b>worker-b</b><span>read version 3</span></div>
      </div>
      <p class="sub-label">The answers</p>
      <div class="answers"></div>
      <div class="demo-foot">
        <span></span>
        <div class="btns">
          <button class="btn primary" data-act="a">worker-a: put, expect 3</button>
          <button class="btn" data-act="b">worker-b: put, expect 3</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const answers = $(".answers", el);
    let row;
    const paint = () => {
      $('[data-k="value"]', el).textContent = `{ status: '${row.status}' }`;
      $('[data-k="version"]', el).textContent = row.version;
    };
    const reset = () => {
      row = { status: "confirmed", version: 3 };
      answers.innerHTML = `<div class="empty">Both workers read version 3. Who writes first?</div>`;
      paint();
    };
    const put = (who, status) => {
      $(".empty", answers)?.remove();
      let r;
      if (row.version === 3) {
        row = { status, version: 4 };
        r = answerRow(who, `{ applied: true, version: 4 }`, `wrote '${status}'`);
      } else {
        r = answerRow(who, `{ applied: false, reason: 'version', version: ${row.version} }`,
          `nothing written: someone wrote since version 3. The answer has the current value.`, true);
      }
      answers.prepend(r);
      while (answers.children.length > 3) answers.lastChild.remove();
      paint();
    };
    $('[data-act="a"]', el).addEventListener("click", () => put("a", "checked-in"));
    $('[data-act="b"]', el).addEventListener("click", () => put("b", "cancelled"));
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }


  // ==== Chapter 4 · Waiting without cron ====

  const boxes = (items) => items.map((it) => (it === "→" ? `<span class="arrow">→</span>`
    : `<div class="box${it[2] ? " yours" : ""}">${it[0]}<small>${it[1]}</small></div>`)).join("");

  // ---- 4.1 Kafka: two ways to build "later" ----
  {
    const el = $("#k-later");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">what you run next to Kafka</span>
        <div class="seg"><button data-v="cron" aria-pressed="true">a table and a cron</button><button data-v="topics" aria-pressed="false">a topic per delay</button></div>
      </div>
      <div class="steps diagram"></div>
      <p class="verdict"></p>
      <div class="demo-foot"><span class="legend"><i class="lg-yours"></i>yours to build and run</span><span></span></div>`;
    const DIAGRAMS = {
      cron: {
        rows: [
          [["bookings", "Kafka topic"], "→", ["worker", "your consumer"], "→", ["INSERT reminder", "Postgres table", 1]],
          [["cron", "every minute", 1], "→", ["SELECT due, unpaid", "scans the table", 1], "→", ["payment-reminders", "Kafka topic"]],
        ],
        verdict: "Up to a minute late. A cancel is an UPDATE that someone must remember. One more database to run.",
      },
      topics: {
        rows: [
          [["bookings", "Kafka topic"], "→", ["worker", "your consumer"], "→", ["delay-15m", "a topic for this delay", 1]],
          [["delay-5m", "another topic", 1], ["delay-1h", "and another", 1], ["delay-24h", "and another", 1]],
          [["a consumer per topic", "waits until each is due", 1], "→", ["payment-reminders", "Kafka topic"]],
        ],
        verdict: "A new duration is a new topic. A sent message cannot be cancelled, so the last consumer checks the booking first.",
      },
    };
    const show = (k) => {
      const d = DIAGRAMS[k];
      $(".diagram", el).innerHTML = d.rows.map((r) => `<div class="sys">${boxes(r)}</div>`).join("");
      $(".verdict", el).textContent = d.verdict;
    };
    segmented($(".seg", el), show);
    show("cron");
  }

  // ---- 4.2 A timer: schedule, move, cancel, fire ----
  {
    const el = $("#timer");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">timers · queue payment-reminders</span></div>
      <div class="ro"><span class="ro-label">the broker's clock</span><span class="clock"></span></div>
      <table class="opts">
        <thead><tr><th>timer key</th><th>fires at</th><th>status</th></tr></thead>
        <tbody><tr><td>booking-9137</td><td class="v2" data-k="at"></td><td class="v2" data-k="st"></td></tr></tbody>
      </table>
      <div class="lanes"></div>
      <p class="sub-label">The answers</p>
      <div class="answers log"></div>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn primary" data-act="plus">+5 min</button>
          <button class="btn" data-act="move">Schedule again, +30 min</button>
          <button class="btn" data-act="pay">The guest pays</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const row = laneRow(ME, "me");
    $(".lanes", el).appendChild(row);
    const answers = $(".answers", el);
    const hhmm = (m) => `${10 + Math.floor(m / 60)}:${String(m % 60).padStart(2, "0")}`;
    let s;
    const pending = () => s.st === "scheduled" || s.st === "rescheduled";
    const say = (when, text) => {
      answers.prepend(answerRow(hhmm(when), text));
      while (answers.children.length > 3) answers.lastChild.remove();
    };
    const paint = () => {
      $(".clock", el).textContent = hhmm(s.now);
      $('[data-k="at"]', el).textContent = pending() ? hhmm(s.at) : "—";
      $('[data-k="st"]', el).textContent = s.st === "fired" ? "fired: the row is gone" : s.st === "cancelled" ? "cancelled: gone" : s.st;
      syncCells($(".cells", row), s.fired, -1, { me: true });
      $(".more", row).textContent = "";
    };
    const reset = () => {
      s = { now: 0, at: 15, st: "scheduled", fired: 0 };
      answers.innerHTML = "";
      say(0, "schedule() → { status: 'scheduled' }");
      paint();
    };
    $('[data-act="plus"]', el).addEventListener("click", () => {
      s.now += 5;
      if (pending() && s.now >= s.at) {
        s.st = "fired"; s.fired++;
        say(s.at, "fired: the broker pushed a message into payment-reminders");
      }
      paint();
    });
    $('[data-act="move"]', el).addEventListener("click", () => {
      const was = pending();
      s.at = s.now + 30;
      s.st = was ? "rescheduled" : "scheduled";
      say(s.now, `schedule() → { status: '${s.st}' }, now for ${hhmm(s.at)}`);
      paint();
    });
    $('[data-act="pay"]', el).addEventListener("click", () => {
      if (pending()) { s.st = "cancelled"; say(s.now, "cancel() → { status: 'cancelled' }: no reminder"); }
      else say(s.now, "cancel() → { ok: false, status: 'absent' }: nothing pending");
      paint();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 4.3 The payment and the timer, close together ----
  {
    const el = $("#race");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">booking-9137 · two endings</span></div>
      <div class="answers log"></div>
      <p class="verdict"></p>
<pre class="code"><span class="c">// the reminder's handler</span>
.<span class="f">consume</span>(<span class="k">async</span> (msg) <span class="k">=&gt;</span> {
  <span class="k">const</span> b = <span class="k">await</span> queen.kv.<span class="f">get</span>(<span class="s">'bookings'</span>, msg.data.id)
  <span class="k">if</span> (b.value?.status === <span class="s">'paid'</span>) <span class="k">return</span>  <span class="c">// paid since</span>
  <span class="k">await</span> <span class="f">sendReminder</span>(msg.data)
})</pre>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn primary" data-act="early">The guest pays at 10:14</button>
          <button class="btn" data-act="late">… at 10:15:00.2</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const log = $(".answers", el), verdict = $(".verdict", el), t = timers();
    const STORIES = {
      early: {
        rows: [
          ["10:00:00", "the booking step commits: a timer for 10:15"],
          ["10:14:00", "the payment step commits: paid, and the cancel answers 'cancelled'"],
          ["10:15:00", "nothing fires"],
        ],
        verdict: "No reminder: the cancel came first.",
      },
      late: {
        rows: [
          ["10:00:00", "the booking step commits: a timer for 10:15"],
          ["10:15:00.0", "the timer fires: a message waits in payment-reminders"],
          ["10:15:00.2", "the payment step commits: paid. The cancel answers 'absent'"],
          ["10:15:00.5", "the handler reads bookings/booking-9137: paid. It sends nothing"],
        ],
        verdict: "No reminder, because the handler checked the booking first.",
      },
    };
    const reset = () => {
      t.clear();
      log.innerHTML = `<div class="empty">Pick when the guest pays.</div>`;
      verdict.textContent = "";
      $$("[data-act]", el).forEach((b) => { b.disabled = false; });
    };
    const play = (k) => {
      reset();
      log.innerHTML = "";
      $$("[data-act]", el).forEach((b) => { if (b.dataset.act !== "reset") b.disabled = true; });
      const st = STORIES[k];
      st.rows.forEach(([when, what], i) => t.after(300 + i * 900, () => log.appendChild(answerRow(when, what))));
      t.after(300 + st.rows.length * 900, () => {
        verdict.textContent = st.verdict;
        $$("[data-act]", el).forEach((b) => { b.disabled = false; });
      });
    };
    $('[data-act="early"]', el).addEventListener("click", () => play("early"));
    $('[data-act="late"]', el).addEventListener("click", () => play("late"));
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 4.4 delayedProcessing and windowBuffer ----
  {
    const el = $("#delays");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queue room-updates · a short demo: seconds, not minutes</span></div>
      <p class="sub-label">delayedProcessing: 5</p>
      <div class="lanes" data-r="dp"></div>
      <p class="sim-msg" data-r="dp-msg">A pushed message waits 5 s before anyone can take it.</p>
      <div class="ctl-row" style="justify-content:space-between">
        <p class="sub-label">windowBuffer</p>
        <div class="seg"><button data-v="0" aria-pressed="false">0</button><button data-v="3" aria-pressed="true">3 s</button></div>
      </div>
      <div class="lanes" data-r="wb"></div>
      <div class="ro"><span class="ro-label">handed to the worker</span><span class="ro-num" data-r="calls">0 batches</span><span class="ro-note" data-r="wb-msg"></span></div>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn" data-act="push">Push one</button>
          <button class="btn primary" data-act="burst">Push a burst of 6</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const dpRow = laneRow("hotel-7"), wbRow = laneRow("hotel-8");
    $('[data-r="dp"]', el).appendChild(dpRow);
    $('[data-r="wb"]', el).appendChild(wbRow);
    let s, buffer = 3;
    segmented($(".seg", el), (v) => { buffer = +v; reset(); });
    // A message: { at: when it was pushed, done: handed over }. Times in ms on the demo clock.
    const reset = () => {
      s = { t: 0, dp: [], wb: [], batches: 0, burstAt: null };
      $(".cells", dpRow).innerHTML = "";
      $(".cells", wbRow).innerHTML = "";
      $('[data-r="wb-msg"]', el).textContent = buffer ? "A burst waits 3 s, then goes out as one batch." : "Each message goes out as it arrives.";
      paint();
    };
    const paint = () => {
      const strip = (row, list, waiting) => {
        const box = $(".cells", row);
        while (box.children.length < list.length) box.appendChild(cell(box.children.length));
        list.forEach((m, i) => {
          const c = box.children[i];
          const w = waiting(m);
          c.classList.toggle("ghost", w);
          c.classList.toggle("done", m.done);
          $("b", c).textContent = w ? Math.ceil(w / 1000) + "s" : i;
        });
      };
      strip(dpRow, s.dp, (m) => (m.done ? 0 : Math.max(0, 5000 - (s.t - m.at))));
      strip(wbRow, s.wb, (m) => (m.done ? 0 : buffer && s.burstAt !== null ? Math.max(0, buffer * 1000 - (s.t - s.burstAt)) : 0));
      $('[data-r="calls"]', el).textContent = `${s.batches} batch${s.batches === 1 ? "" : "es"}`;
    };
    $('[data-act="push"]', el).addEventListener("click", () => { s.dp.push({ at: s.t, done: false }); paint(); });
    $('[data-act="burst"]', el).addEventListener("click", () => {
      for (let i = 0; i < 6; i++) setTimeout(() => {
        if (s.burstAt === null || s.wb.every((m) => m.done)) s.burstAt = s.t;
        s.wb.push({ at: s.t, done: false });
        paint();
      }, i * 300);
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
    tick(el, (dt) => {
      s.t += dt;
      for (const m of s.dp) if (!m.done && s.t - m.at >= 5000 + 400) m.done = true;
      const open = s.wb.filter((m) => !m.done);
      if (open.length) {
        if (!buffer) {
          for (const m of open) if (s.t - m.at >= 200) { m.done = true; s.batches++; }
        } else if (s.t - s.burstAt >= buffer * 1000 + 200) {
          open.forEach((m) => { m.done = true; });
          s.batches++;
          s.burstAt = null;
        }
      }
      paint();
    });
  }

  // ==== Chapter 5 · Only one worker ====

  // A played timeline: rows of bars [from, to] and marks [at], seconds 0..60.
  // Each item shows up when the "now" line reaches it.
  const playedTimeline = (el, head, rows, verdictText, bad) => {
    const SPAN = 60, PLAY_MS = 9000;
    const pct = (s) => (s / SPAN) * 100;
    el.innerHTML = `
      <div class="demo-head"><span class="mono">${head}</span><span class="speed">60 seconds in 9</span></div>
      <div class="tl"></div>
      <div class="tl-axis">${[0, 10, 20, 30, 40, 50, 60].map((s) => `<span style="left:${pct(s)}%">${s} s</span>`).join("")}</div>
      <p class="verdict"></p>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn primary" data-act="play">Play</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const tl = $(".tl", el), verdict = $(".verdict", el);
    const items = [];
    for (const [name, parts] of rows) {
      const track = h(`<div class="tl-row"><span class="tl-name">${name}</span><div class="tl-track"></div></div>`);
      tl.appendChild(track);
      const box = $(".tl-track", track);
      for (const p of parts) {
        const node = p.to !== undefined
          ? h(`<span class="tl-bar ${p.cls || ""}" style="left:${pct(p.from)}%;width:${pct(p.to - p.from)}%">${p.label}</span>`)
          : h(`<span class="tl-mark ${p.cls || ""}" style="${p.side === "r" ? `right:${100 - pct(p.at)}%` : `left:${pct(p.at)}%`}">${p.label}</span>`);
        box.appendChild(node);
        items.push([p.from ?? p.at, node, p]);
      }
    }
    const overlay = h(`<div class="tl-overlay"></div>`);
    tl.appendChild(overlay);
    const now = h(`<span class="tl-now"></span>`);
    let t0 = null, at = 0;
    const paint = () => {
      for (const [from, node, p] of items) {
        node.hidden = at < from;
        if (p.to !== undefined) node.style.width = `${pct(Math.max(0, Math.min(at, p.to) - p.from))}%`;
      }
    };
    const reset = () => {
      t0 = null; at = 0;
      verdict.textContent = "";
      verdict.classList.remove("bad");
      now.remove();
      paint();
      $('[data-act="play"]', el).disabled = false;
    };
    $('[data-act="play"]', el).addEventListener("click", () => {
      reset();
      t0 = 0;
      $('[data-act="play"]', el).disabled = true;
      overlay.appendChild(now);
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
    tick(el, (dt) => {
      if (t0 === null) return;
      t0 += dt;
      at = Math.min(SPAN, (t0 / PLAY_MS) * SPAN);
      now.style.left = `${pct(at)}%`;
      paint();
      if (at >= SPAN) {
        t0 = null;
        verdict.textContent = verdictText;
        verdict.classList.toggle("bad", bad);
        $('[data-act="play"]', el).disabled = false;
      }
    });
  };

  // ---- 5.1 Kafka: a Redis lock and a GC pause ----
  playedTimeline($("#k-lock"), "a Redis lock · TTL 30 s · nightly report", [
    ["lock", [{ from: 0, to: 30, label: "worker-a", cls: "held" }, { from: 31, to: 60, label: "worker-b", cls: "held" }]],
    ["worker-a", [{ from: 0, to: 10, label: "works" }, { from: 10, to: 50, label: "frozen: GC pause", cls: "paused" }, { at: 51, label: "writes", cls: "bad" }]],
    ["worker-b", [{ at: 1, label: "waits for the lock" }, { from: 31, to: 37, label: "works" }, { at: 38, label: "writes", cls: "ok" }]],
    ["reports", [{ at: 38, label: "report", cls: "ok" }, { at: 60, side: "r", label: "report again", cls: "bad" }]],
  ], "Two reports for 11 November. The lock expired, and nobody told worker-a.", true);

  // ---- 5.2 Queen: the same night, with a guard ----
  playedTimeline($("#q-lock"), "queen.lock('nightly-report') · ttl 30 s", [
    ["lock", [{ from: 0, to: 30, label: "worker-a · token 41", cls: "held" }, { from: 31, to: 60, label: "worker-b · token 42", cls: "held" }]],
    ["worker-a", [{ from: 0, to: 10, label: "works" }, { from: 10, to: 48, label: "frozen: GC pause", cls: "paused" }, { at: 49, label: "rolled back", cls: "bad" }]],
    ["worker-b", [{ at: 1, label: "acquire → false" }, { from: 31, to: 37, label: "works" }, { at: 38, label: "commits", cls: "ok" }]],
    ["reports", [{ at: 38, label: "report", cls: "ok" }, { at: 60, side: "r", label: "nothing written" }]],
  ], "One report. worker-a's late commit rolled back whole (kv_precondition): its lock was gone.", false);

  // ==== Chapter 6 · The booking page: now, not history ====

  // A burst of availability values for hotel-7: 14 rooms down to 9.
  const BURST = [14, 13, 12, 11, 10, 9];

  // ---- 6.1 Kafka: every value is delivered; the reply topic ----
  {
    const el = $("#k-page");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">compacted topic availability · key hotel-7</span></div>
      <div class="lanes"></div>
      <div class="ro"><span class="ro-label">the screen redrew</span><span class="ro-num">0 times</span><span class="ro-note">to show one number</span></div>
      <p class="sub-label">A price, asked over Kafka</p>
      <div class="sys">
        <div class="box">booking page</div><span class="arrow">→</span>
        <div class="box mono">quote-requests<small>a topic</small></div><span class="arrow">→</span>
        <div class="box">pricing</div><span class="arrow">→</span>
        <div class="box mono">quote-replies<small>a topic</small></div><span class="arrow">→</span>
        <div class="box yours">a consumer that waits<small>matches the correlation id</small></div>
      </div>
      <div class="demo-foot"><span class="legend"><i class="lg-yours"></i>yours to build and run</span>
        <div class="btns"><button class="btn primary" data-act="burst">6 updates in a second</button><button class="btn" data-act="reset">Reset</button></div>
      </div>`;
    const row = laneRow("hotel-7");
    $(".lanes", el).appendChild(row);
    const ro = $(".ro", el), t = timers();
    let n, head, redraws;
    const draw = () => {
      syncCells($(".cells", row), n, head, { label: (i) => BURST[i % BURST.length], busy: true, prog: 0.5 });
      $(".ro-num", ro).textContent = `${redraws} time${redraws === 1 ? "" : "s"}`;
    };
    const reset = () => { t.clear(); n = 0; head = 0; redraws = 0; $(".cells", row).innerHTML = ""; draw(); };
    $('[data-act="burst"]', el).addEventListener("click", () => {
      reset();
      BURST.forEach((_, i) => t.after(i * 160, () => { n++; draw(); }));
      const handle = () => {
        if (head < n) { head++; redraws++; draw(); }
        if (head < BURST.length) t.after(450, handle);
      };
      t.after(300, handle);
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 6.2 Conflation: the newest value, and a group that still gets all ----
  {
    const el = $("#conf");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queue availability · partition hotel-7</span></div>
      <div class="lanes"></div>
      <div class="ro"><span class="ro-label">the screen shows</span><span class="ro-num">—</span><span class="ro-note"></span></div>
      <div class="demo-foot"><span class="legend"><i class="lg-fresh"></i>handled <i class="lg-skip"></i>skipped</span>
        <div class="btns">
          <button class="btn primary" data-act="burst">6 updates, fast</button>
          <button class="btn" data-act="one">One more update</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const screenRow = laneRow("screen"), auditRow = laneRow("audit");
    $(".lane-name", screenRow).innerHTML = "screen <small>conflation</small>";
    $(".lane-name", auditRow).innerHTML = "audit <small>every message</small>";
    $(".lanes", el).append(screenRow, auditRow);
    const ro = $(".ro", el);
    let s;
    const reset = () => {
      s = { t: 0, msgs: [], queue: [], screen: { at: 0, busy: 0, runs: 0, shows: null, handled: new Set() }, audit: { at: 0, busy: 0, runs: 0 } };
      $(".cells", screenRow).innerHTML = ""; $(".cells", auditRow).innerHTML = "";
      draw();
    };
    const push = (v, delay) => s.queue.push({ v, due: s.t + delay });
    const paint = (row, status) => {
      const box = $(".cells", row);
      while (box.children.length < s.msgs.length) box.appendChild(cell(s.msgs[box.children.length]));
      [...box.children].forEach((c, i) => {
        const st = status(i);
        c.classList.toggle("fresh", st === "handled");
        c.classList.toggle("done", st === "skipped");
        c.classList.toggle("busy", st === "busy");
      });
    };
    const draw = () => {
      const sc = s.screen, au = s.audit;
      paint(screenRow, (i) => (sc.handled.has(i) ? (sc.busy > 0 && i === sc.last ? "busy" : "handled") : i < sc.at ? "skipped" : "pending"));
      paint(auditRow, (i) => (i < au.at ? (au.busy > 0 && i === au.at - 1 ? "busy" : "handled") : "pending"));
      $(".ro-num", ro).textContent = sc.shows === null ? "—" : `${sc.shows} rooms`;
      $(".ro-note", ro).textContent = s.msgs.length
        ? `screen: ${sc.runs} handler run${sc.runs === 1 ? "" : "s"} for ${s.msgs.length} messages · audit: ${au.runs}`
        : "";
    };
    $('[data-act="burst"]', el).addEventListener("click", () => { reset(); BURST.forEach((v, i) => push(v, i * 150)); });
    $('[data-act="one"]', el).addEventListener("click", () => {
      const last = s.msgs.length ? s.msgs[s.msgs.length - 1] : 10;
      push(Math.max(1, last - 1), 0);
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
    tick(el, (dt) => {
      s.t += dt;
      while (s.queue.length && s.queue[0].due <= s.t) s.msgs.push(s.queue.shift().v);
      const sc = s.screen, au = s.audit;
      if (sc.busy > 0) sc.busy -= dt;
      if (sc.busy <= 0 && sc.at < s.msgs.length) {        // one pop: the newest, the rest skipped
        sc.last = s.msgs.length - 1;
        sc.handled.add(sc.last);
        sc.at = s.msgs.length;
        sc.shows = s.msgs[sc.last];
        sc.runs++; sc.busy = 700;
      }
      if (au.busy > 0) au.busy -= dt;
      if (au.busy <= 0 && au.at < s.msgs.length) { au.at++; au.runs++; au.busy = 300; }
      draw();
    });
  }

  // ---- 6.3 Request and reply with a deadline ----
  {
    const el = $("#reply");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">ephemeral queues · quotes and a private inbox</span></div>
      <div class="ctl-row">
        <span class="seg-label">the pricing service takes</span>
        <input type="range" min="40" max="900" step="10" value="120" aria-label="Pricing service time">
        <span class="mono" data-r="lat">120 ms</span>
        <div class="seg" style="margin-left:auto"><button data-v="up" aria-pressed="true">up</button><button data-v="down" aria-pressed="false">down</button></div>
      </div>
      <div class="answers log"></div>
      <p class="verdict"></p>
<pre class="code"><span class="k">const</span> { messages } = <span class="k">await</span> queen.ephemeral
  .<span class="f">pop</span>(<span class="s">'quotes'</span>, { group: <span class="s">'pricing'</span>, wait: <span class="n">true</span> })
<span class="k">for</span> (<span class="k">const</span> m <span class="k">of</span> messages)
  <span class="k">await</span> queen.ephemeral.<span class="f">push</span>(m.payload.replyTo,
    [{ room: m.payload.room, price: <span class="n">129</span> }])
<span class="k">await</span> queen.ephemeral.<span class="f">ack</span>(<span class="s">'quotes'</span>, messages, { group: <span class="s">'pricing'</span> })</pre>
      <div class="demo-foot"><span></span>
        <div class="btns"><button class="btn primary" data-act="ask">Ask for a price</button></div>
      </div>`;
    const range = $("input", el), lat = $('[data-r="lat"]', el), log = $(".answers", el), verdict = $(".verdict", el), t = timers();
    let up = true;
    range.addEventListener("input", () => { lat.textContent = `${range.value} ms`; });
    segmented($(".seg", el), (v) => { up = v === "up"; });
    const line = (when, what) => log.appendChild(answerRow(`${when} ms`, what));
    $('[data-act="ask"]', el).addEventListener("click", () => {
      t.clear();
      log.innerHTML = "";
      verdict.classList.remove("bad");
      verdict.textContent = "";
      const inbox = "inbox-" + Math.random().toString(16).slice(2, 6);
      const ms = +range.value;
      const answered = up && ms < 500;
      line(0, `push quotes { room, replyTo: '${inbox}' }; wait on it`);
      if (!up) t.after(250, () => line("…", "nobody pops quotes: pricing is down"));
      else t.after(Math.min(ms, 500) / 2, () => line("…", "a pricing instance pops it"));
      if (answered) {
        t.after(ms, () => {
          line(ms, `pricing pushes { price: 129 } to ${inbox}`);
          verdict.textContent = "The page shows €129.";
        });
      } else {
        t.after(500, () => {
          line(500, "the timeout ends the wait");
          verdict.textContent = "The page shows “ask us”.";
          verdict.classList.add("bad");
        });
        if (up) t.after(ms, () => line(ms, `the answer, ${ms - 500} ms late: nobody waits, nothing is kept`));
      }
    });
  }

  // ---- 6.4 A node stops, a node crashes ----
  {
    const el = $("#eph-nodes");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">ephemeral partitions and the messages in them</span></div>
      <div class="nodes"></div>
      <p class="verdict"></p>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn" data-act="stop">Stop node-2 (SIGTERM)</button>
          <button class="btn" data-act="crash">Crash node-3</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const nodesEl = $(".nodes", el), verdict = $(".verdict", el);
    const START = { 1: ["quotes · 2", "inbox-3f2a · 1"], 2: ["inbox-91c0 · 1", "quotes · 1"], 3: ["inbox-77de · 1", "inbox-a412 · 1", "typing · 3"] };
    let s;
    const draw = () => {
      nodesEl.innerHTML = "";
      for (const id of [1, 2, 3]) {
        const st = s.state[id];
        const node = h(`<div class="node ${st === "up" ? "" : "down"}"><b>node-${id}</b><span>${st === "up" ? "owns" : st === "stopped" ? "stopped: handed over" : "crashed"}</span><div class="chips"></div></div>`);
        for (const p of s.parts[id]) $(".chips", node).appendChild(h(`<span class="chip">${p}</span>`));
        nodesEl.appendChild(node);
      }
      $('[data-act="stop"]', el).disabled = s.state[2] !== "up";
      $('[data-act="crash"]', el).disabled = s.state[3] !== "up";
    };
    const reset = () => {
      s = { state: { 1: "up", 2: "up", 3: "up" }, parts: { 1: [...START[1]], 2: [...START[2]], 3: [...START[3]] } };
      verdict.classList.remove("bad");
      verdict.textContent = "Each partition lives on one node. The others forward to it.";
      draw();
    };
    $('[data-act="stop"]', el).addEventListener("click", () => {
      const moved = s.parts[2];
      s.parts[2] = [];
      s.state[2] = "stopped";
      const to = s.state[1] === "up" ? 1 : 3;
      s.parts[to].push(...moved);
      verdict.classList.remove("bad");
      verdict.textContent = `node-2 handed its partitions to node-${to} before it stopped. Nothing lost.`;
      draw();
    });
    $('[data-act="crash"]', el).addEventListener("click", () => {
      const lost = s.parts[3].reduce((a, p) => a + +p.split("· ")[1], 0);
      s.parts[3] = [];
      s.state[3] = "crashed";
      verdict.classList.add("bad");
      verdict.textContent = `node-3 crashed: its ${lost} messages are gone. Their callers time out and ask again.`;
      draw();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ==== Chapter 7 · The whole history, live ====

  // ---- 7.1 Kafka: find one booking in three topics ----
  {
    const el = $("#k-find");
    const TOPICS = {
      bookings: ["2203", "9137", "4410", "7781", "5120", "3349", "6012", "8875", "1408", "2950", "4410", "2203"],
      payments: ["4410", "7781", "2203", "9137", "5120", "3349", "8875", "6012"],
      emails: ["2203", "4410", "7781", "9137", "5120", "6012", "3349", "8875", "1408", "2950"],
    };
    el.innerHTML = `
      <div class="demo-head"><span class="mono">three topics, one partition each shown</span></div>
      <div class="lanes"></div>
      <div class="ro"><span class="ro-label">messages read</span><span class="ro-num">0</span><span class="ro-note"></span></div>
      <div class="demo-foot"><span></span>
        <div class="btns"><button class="btn primary" data-act="find">Find booking-9137</button><button class="btn" data-act="reset">Reset</button></div>
      </div>`;
    const rows = Object.entries(TOPICS).map(([name, ids]) => {
      const r = laneRow(name);
      ids.forEach((id) => $(".cells", r).appendChild(cell(id)));
      $(".lanes", el).appendChild(r);
      return r;
    });
    const ro = $(".ro", el), t = timers();
    const reset = () => {
      t.clear();
      $$(".cell", el).forEach((c) => c.classList.remove("me", "done", "busy"));
      $(".ro-num", ro).textContent = "0";
      $(".ro-note", ro).textContent = "and in production, every partition of every topic";
      $('[data-act="find"]', el).disabled = false;
    };
    $('[data-act="find"]', el).addEventListener("click", () => {
      reset();
      $('[data-act="find"]', el).disabled = true;
      const all = rows.flatMap((r) => [...$$(".cell", r)]);
      let read = 0, found = 0;
      all.forEach((c, i) => t.after(i * 90, () => {
        read++;
        const hit = c.textContent === "9137";
        c.classList.add(hit ? "me" : "done");
        if (hit) found++;
        $(".ro-num", ro).textContent = String(read);
        $(".ro-note", ro).textContent = `${found} found. Which came first, across topics? The logs cannot say.`;
        if (i === all.length - 1) $('[data-act="find"]', el).disabled = false;
      }));
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 7.2 The feed: ask it about one booking ----
  {
    const el = $("#feed");
    const FEED = [
      [1030, "bookings", "booking-2203", "created · DE · 2 nights"],
      [1031, "bookings", "booking-9137", "created · IT · 3 nights"],
      [1032, "availability", "hotel-7", "11 rooms"],
      [1033, "bookings", "booking-4410", "created · IT · 1 night"],
      [1034, "payments", "booking-2203", "paid · €180"],
      [1035, "payments", "booking-9137", "paid · €387"],
      [1035, "bookings", "booking-9137", "event: paid"],
      [1036, "emails", "booking-9137", "confirm-mail"],
      [1037, "availability", "hotel-7", "10 rooms"],
      [1038, "emails", "booking-2203", "confirm-mail"],
      [1039, "payment-reminders", "booking-4410", "fired: remind the guest"],
      [1040, "bookings", "booking-9137", "checked-in"],
    ];
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queen.feed({ partitions: [id], since: '1d', follow: false })</span></div>
      <div class="ctl-row">
        <div class="seg"><button data-v="booking-9137" aria-pressed="true">booking-9137</button><button data-v="booking-4410" aria-pressed="false">booking-4410</button><button data-v="booking-2203" aria-pressed="false">booking-2203</button><button data-v="" aria-pressed="false">everything</button></div>
      </div>
      <table class="opts feed-table">
        <thead><tr><th>position</th><th>queue</th><th>partition</th><th>data</th></tr></thead>
        <tbody></tbody>
      </table>
      <p class="sim-msg"></p>`;
    const body = $("tbody", el), msg = $(".sim-msg", el);
    const show = (id) => {
      const rows = FEED.filter((r) => !id || r[2] === id);
      const shown = rows.slice(0, 7);
      body.innerHTML = shown.map(([p, q, part, d]) => `<tr><td>${p}</td><td>${q}</td><td>${part}</td><td>${d}</td></tr>`).join("")
        + (rows.length > shown.length ? `<tr><td></td><td colspan="3" class="v2">… ${rows.length - shown.length} more</td></tr>` : "");
      const queues = new Set(rows.map((r) => r[1])).size;
      msg.textContent = id
        ? `${rows.length} entries from ${queues} queues, in the order they committed. One read, no index.`
        : `The whole tenant: ${rows.length} entries, every queue, one order.`;
    };
    segmented($(".seg", el), show);
    show("booking-9137");
  }

  // ---- 7.3 A join: bookings meet payments, in either order ----
  {
    const el = $("#join");
    // Minutes after 10:00 on the log's clock.
    const SCRIPT = [
      [0, "bookings", "booking-9137", "created"],
      [2, "bookings", SLOW_B, "created"],
      [6, "payments", "booking-9137", "paid · €387"],
      [20, "payments", "booking-2203", "paid · €180"],
      [25, "bookings", "booking-2203", "created"],
      [62, null, SLOW_B, "deadline"],
    ];
    const AMOUNT = { "booking-9137": 387, "booking-2203": 180 };
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queryId paid-bookings · within 1h</span></div>
      <div class="ro"><span class="ro-label">the log's clock</span><span class="clock">10:00</span></div>
      <div class="lanes"></div>
      <table class="opts">
        <thead><tr><th>key</th><th>booking</th><th>paid</th><th>deadline</th></tr></thead>
        <tbody></tbody>
      </table>
      <p class="sub-label">pushed to paid-bookings</p>
      <div class="answers log"></div>
      <div class="demo-foot"><span></span>
        <div class="btns"><button class="btn primary" data-act="play">Play</button><button class="btn" data-act="reset">Reset</button></div>
      </div>`;
    const rows = { bookings: laneRow("bookings"), payments: laneRow("payments") };
    $(".lanes", el).append(rows.bookings, rows.payments);
    const body = $("tbody", el), log = $(".answers", el), t = timers();
    const hhmm = (m) => `${10 + Math.floor(m / 60)}:${String(m % 60).padStart(2, "0")}`;
    let open;
    const draw = () => {
      body.innerHTML = Object.entries(open).map(([k, o]) =>
        `<tr><td>${k}</td><td class="v2">${o.booking ? "yes" : "—"}</td><td class="v2">${o.paid ? "yes" : "—"}</td><td class="v2">${hhmm(o.due)}</td></tr>`).join("")
        || `<tr><td colspan="4" class="v2">nothing open</td></tr>`;
    };
    const reset = () => {
      t.clear();
      open = {};
      for (const r of Object.values(rows)) $(".cells", r).innerHTML = "";
      log.innerHTML = `<div class="empty">Nothing yet.</div>`;
      $(".clock", el).textContent = "10:00";
      $('[data-act="play"]', el).disabled = false;
      draw();
    };
    const out = (m, what) => { $(".empty", log)?.remove(); log.appendChild(answerRow(hhmm(m), what)); };
    const apply = ([m, q, key, what]) => {
      $(".clock", el).textContent = hhmm(m);
      if (!q) {                                           // a deadline passes
        out(m, `onTimeout → { id: '${key}', missing: ['paid'] }`);
        delete open[key];
        return draw();
      }
      const side = q === "bookings" ? "booking" : "paid";
      $(".cells", rows[q]).appendChild(h(`<span class="chip${key === ME ? " me" : ""}">${key.slice(-4)} ${what}</span>`));
      const o = open[key] || (open[key] = { due: m + 60 });
      o[side] = true;
      if (o.booking && o.paid) {
        out(m, `select → { id: '${key}', amount: ${AMOUNT[key]} }`);
        delete open[key];
      }
      draw();
    };
    $('[data-act="play"]', el).addEventListener("click", () => {
      reset();
      $('[data-act="play"]', el).disabled = true;
      SCRIPT.forEach((e, i) => t.after(300 + i * 1300, () => apply(e)));
      t.after(300 + SCRIPT.length * 1300, () => { $('[data-act="play"]', el).disabled = false; });
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 7.4 The pattern: paid, or reminded ----
  {
    const el = $("#pattern");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">pattern · bookings, then payments · within 15m</span></div>
      <div class="ro"><span class="ro-label">the log's clock</span><span class="clock"></span></div>
      <table class="opts">
        <thead><tr><th>booking</th><th>started</th><th>deadline</th><th>state</th></tr></thead>
        <tbody></tbody>
      </table>
      <p class="sub-label">pushed to payment-followups</p>
      <div class="answers log"></div>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn primary" data-act="plus">+5 min</button>
          <button class="btn" data-act="pay">booking-9137 pays</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const body = $("tbody", el), log = $(".answers", el);
    const hhmm = (m) => `${10 + Math.floor(m / 60)}:${String(m % 60).padStart(2, "0")}`;
    let s;
    const draw = () => {
      $(".clock", el).textContent = hhmm(s.now);
      body.innerHTML = s.open.map((p) => `<tr><td>${p.id}</td><td>${hhmm(p.at)}</td><td>${hhmm(p.at + 15)}</td><td class="v2">${p.state}</td></tr>`).join("");
      $('[data-act="pay"]', el).disabled = s.open.find((p) => p.id === ME).state !== "waiting";
    };
    const reset = () => {
      s = { now: 0, open: [{ id: ME, at: 0, state: "waiting" }, { id: SLOW_B, at: 2, state: "waiting" }] };
      log.innerHTML = `<div class="empty">Nothing yet.</div>`;
      draw();
    };
    const out = (when, what) => { $(".empty", log)?.remove(); log.appendChild(answerRow(hhmm(when), what)); };
    $('[data-act="plus"]', el).addEventListener("click", () => {
      s.now += 5;
      for (const p of s.open) if (p.state === "waiting" && s.now >= p.at + 15) {
        p.state = "timed out";
        out(p.at + 15, `onTimeout → { remind: '${p.id}' }`);
      }
      draw();
    });
    $('[data-act="pay"]', el).addEventListener("click", () => {
      const p = s.open.find((x) => x.id === ME);
      p.state = "matched";
      out(s.now, `onMatch → { paid: '${ME}' }`);
      draw();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 7.5 Shards and a live view ----
  {
    const el = $("#view");
    const SHARDS = [["hotel-1", "hotel-5"], ["hotel-2", "hotel-6"], ["hotel-3", "hotel-7"], ["hotel-4", "hotel-8"]];
    el.innerHTML = `
      <div class="demo-head"><span class="mono">queryId per-minute · shards 4</span><span class="speed">updates per hotel, this minute</span></div>
      <div class="shards"></div>
      <div class="ctl-row">
        <span class="seg-label mono">.get('perMinute',</span>
        <div class="seg"><button data-v="hotel-7" aria-pressed="true">'hotel-7'</button><button data-v="hotel-2" aria-pressed="false">'hotel-2'</button></div>
        <span class="seg-label mono">)</span>
      </div>
      <div class="ro"><span class="ro-label">found: true · value</span><span class="ro-num"></span><span class="ro-note">read from another process, while the shards run</span></div>`;
    const shardsEl = $(".shards", el), ro = $(".ro", el);
    const counts = {};
    let pick = "hotel-7", acc = 0, sec = 0;
    SHARDS.forEach((hotels, i) => {
      hotels.forEach((x) => { counts[x] = 0; });
      shardsEl.appendChild(h(`<div class="node"><b>shard ${i}</b><span>its own position and lock</span>${hotels.map((x) => `<small class="mono" data-h="${x}"></small>`).join("")}</div>`));
    });
    const draw = () => {
      for (const x of Object.keys(counts)) $(`[data-h="${x}"]`, el).textContent = `${x} · ${counts[x]}`;
      $(".ro-num", ro).textContent = `{ count: ${counts[pick]} }`;
    };
    segmented($(".seg", el), (v) => { pick = v; draw(); });
    draw();
    tick(el, (dt) => {
      acc += dt; sec += dt;
      if (sec >= 60000) { sec = 0; for (const x of Object.keys(counts)) counts[x] = 0; }   // a new window
      if (acc >= 450) {
        acc = 0;
        const x = Object.keys(counts)[Math.floor(Math.random() * 8)];
        counts[x]++;
        draw();
      }
    });
  }

  // ==== Chapter 8 · Postgres in, Postgres out ====

  // ---- 8.1 Kafka: what moves a row ----
  {
    const el = $("#k-cdc");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">a row, from one table to another</span></div>
      <div class="flow">
        <div class="box">PostgreSQL<small>public.bookings · a replication slot</small></div>
        <span class="arrow">↓</span>
        <div class="box yours">Debezium<small>a connector, on a Kafka Connect cluster you run</small></div>
        <span class="arrow">↓</span>
        <div class="box">Kafka<small>a topic per table</small></div>
        <span class="arrow">↓</span>
        <div class="box yours">a JDBC sink connector<small>on the same Connect cluster, or a consumer you write</small></div>
        <span class="arrow">↓</span>
        <div class="box">PostgreSQL<small>public.bookings_report</small></div>
      </div>
      <div class="demo-foot"><span class="legend"><i class="lg-yours"></i>yours to run, besides Kafka</span><span></span></div>`;
  }

  // ---- 8.2 The source: SQL in, messages out ----
  {
    const el = $("#pg-src");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">psql, then queue bookings</span></div>
      <div class="term"><div class="term-lines"></div></div>
      <p class="sub-label">New messages</p>
      <div class="answers msgs"></div>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn" data-act="upd">UPDATE 9137</button>
          <button class="btn" data-act="ins">INSERT 9141</button>
          <button class="btn primary" data-act="tx">INSERT + DELETE, one transaction</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const term = $(".term-lines", el), msgs = $(".answers", el);
    let lsn, xid;
    const hex = (n) => `0/17F${n.toString(16).toUpperCase().padStart(4, "0")}`;
    const reset = () => {
      lsn = 0x85f0; xid = 812;
      term.innerHTML = `<div class="muted">app=# -- the source streams every committed change</div>`;
      msgs.innerHTML = `<div class="empty">No change yet.</div>`;
    };
    const sql = (text) => { term.appendChild(h(`<div>app=# ${text}</div>`)); while (term.children.length > 4) term.firstChild.remove(); };
    const emit = (list) => {
      $(".empty", msgs)?.remove();
      lsn += 0x240; xid++;
      for (const [op, key, rest] of list) {
        msgs.prepend(answerRow(op, `{ op: '${op}', key: { id: ${key} }, ${rest}, lsn: '${hex(lsn)}', xid: ${xid} }`));
      }
      while (msgs.children.length > 4) msgs.lastChild.remove();
    };
    $('[data-act="upd"]', el).addEventListener("click", () => {
      sql(`UPDATE bookings SET status = 'paid' WHERE id = 9137;`);
      emit([["u", 9137, "after: { id: 9137, status: 'paid', … }"]]);
    });
    $('[data-act="ins"]', el).addEventListener("click", () => {
      sql(`INSERT INTO bookings (id, status) VALUES (9141, 'new');`);
      emit([["c", 9141, "after: { id: 9141, status: 'new', … }"]]);
    });
    $('[data-act="tx"]', el).addEventListener("click", () => {
      sql(`BEGIN; INSERT … (9142, 'new'); DELETE … WHERE id = 4410; COMMIT;`);
      emit([["c", 9142, "after: { id: 9142, status: 'new', … }"], ["d", 4410, "after: null, before: { id: 4410 }"]]);
      msgs.prepend(h(`<div class="note-line">Same lsn, same xid: one QueenMQ transaction. No consumer sees one without the other.</div>`));
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 8.3 The sink: upsert, and a lost ack ----
  {
    const el = $("#pg-sink");
    el.innerHTML = `
      <div class="demo-head"><span class="mono">sink bookings-report · upsert</span></div>
      <p class="sub-label">Messages in the queue</p>
      <div class="answers">
        <div class="row"><span>#0</span><span>{ id: 7, customer: { name: 'Ada' }, total: '12.50', note: 'late' }</span></div>
        <div class="row"><span>#1</span><span>{ id: 7, total: 15 }</span></div>
      </div>
      <p class="sub-label">public.bookings_report</p>
      <table class="opts">
        <thead><tr><th>id</th><th>customer</th><th>total</th></tr></thead>
        <tbody></tbody>
      </table>
      <div class="listbox"><span class="lb-title">queen.sink_progress</span><span class="lb-row"></span></div>
      <p class="verdict"></p>
      <div class="demo-foot"><span></span>
        <div class="btns">
          <button class="btn primary" data-act="one">Apply #0</button>
          <button class="btn" data-act="two">Apply #1</button>
          <button class="btn" data-act="again">Lose the ack: #1 again</button>
          <button class="btn" data-act="reset">Reset</button>
        </div>
      </div>`;
    const body = $("tbody", el), prog = $(".lb-row", el), verdict = $(".verdict", el);
    let s;
    const draw = () => {
      body.innerHTML = s.row ? `<tr><td>7</td><td>{ "name": "Ada" }</td><td>${s.row.total}</td></tr>` : `<tr><td colspan="3" class="v2">empty</td></tr>`;
      prog.textContent = s.done < 0 ? "nothing yet" : `bookings · up to offset ${s.done}, written with the rows`;
      $('[data-act="one"]', el).disabled = s.done >= 0;
      $('[data-act="two"]', el).disabled = s.done !== 0;
      $('[data-act="again"]', el).disabled = s.done !== 1;
    };
    const reset = () => { s = { row: null, done: -1 }; verdict.textContent = "One Postgres transaction per batch: the rows and the progress row."; draw(); };
    $('[data-act="one"]', el).addEventListener("click", () => {
      s.row = { total: "12.50" }; s.done = 0;
      verdict.textContent = "Inserted. note has no column, so it is ignored.";
      draw();
    });
    $('[data-act="two"]', el).addEventListener("click", () => {
      s.row.total = "15.00"; s.done = 1;
      verdict.textContent = "Updated: total is 15.00, and customer keeps its value, as #1 does not name it.";
      draw();
    });
    $('[data-act="again"]', el).addEventListener("click", () => {
      verdict.textContent = "#1 comes back after a lost ack. Its offset is already in queen.sink_progress: skipped, nothing written twice.";
      draw();
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

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

  // ==== The end ====

  // ---- 9.1 The opening's boxes, removed one by one ----
  {
    const el = $("#removed");
    el.innerHTML = `<div class="demo-head"><span class="mono">next to your services, before and after</span></div>`;
    const grid = glueGrid(el, () => {});
    const verdict = h(`<p class="verdict">The same eight boxes as at the start.</p>`);
    const foot = h(`<div class="demo-foot"><span></span><div class="btns"><button class="btn primary" data-act="go">Remove them</button><button class="btn" data-act="reset">Reset</button></div></div>`);
    el.append(verdict, foot);
    const t = timers();
    const boxes = $$(".box", grid);
    const reset = () => {
      t.clear();
      boxes.forEach((b, i) => {
        b.classList.remove("struck", "on");
        b.innerHTML = `<span class="ch">${GLUE[i][2] ? `ch ${GLUE[i][2]}` : "all"}</span>${GLUE[i][0]}<small>${GLUE[i][1]}</small>`;
      });
      verdict.textContent = "The same eight boxes as at the start.";
      $('[data-act="go"]', el).disabled = false;
    };
    $('[data-act="go"]', el).addEventListener("click", () => {
      reset();
      $('[data-act="go"]', el).disabled = true;
      for (let i = 1; i < boxes.length; i++) t.after(i * 450, () => {
        boxes[i].classList.add("struck");
        verdict.textContent = GLUE[i][3];
      });
      t.after(boxes.length * 450 + 300, () => {
        boxes[0].classList.add("on");
        boxes[0].innerHTML = `<span class="ch">all</span>QueenMQ<small>one broker, plus the Postgres you already had</small>`;
        verdict.textContent = "Eight systems became one broker.";
        $('[data-act="go"]', el).disabled = false;
      });
    });
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }

  // ---- 9.2 Kafka clients and Queen clients on the same queue ----
  {
    const el = $("#port");
    const SERVICES = [["booking-api", "produces bookings"], ["confirm", "consumer group confirm"], ["analytics", "consumer group analytics"], ["emails", "consumer group emails"]];
    el.innerHTML = `
      <div class="demo-head"><span class="mono">topic bookings = queue bookings</span><span class="speed">click a service to move it</span></div>
      <div class="glue-grid"></div>
      <p class="verdict"></p>
      <div class="demo-foot"><span></span><div class="btns"><button class="btn" data-act="reset">Reset</button></div></div>`;
    const grid = $(".glue-grid", el), verdict = $(".verdict", el);
    let moved;
    const draw = () => {
      grid.innerHTML = "";
      SERVICES.forEach(([name, what], i) => {
        const b = h(`<button class="box pick ${moved[i] ? "on" : ""}"><span class="ch">${moved[i] ? "QueenMQ SDK" : "Kafka client"}</span>${name}<small>${what}</small></button>`);
        b.addEventListener("click", () => { moved[i] = !moved[i]; draw(); });
        grid.appendChild(b);
      });
      const n = moved.filter(Boolean).length;
      verdict.textContent = n === 0
        ? "Every service still speaks Kafka, now to QueenMQ's Kafka port."
        : n === SERVICES.length
          ? "Every service on the QueenMQ SDK. Nothing else had to change on the way."
          : `${n} moved, ${SERVICES.length - n} still on Kafka clients: all of them read and write the same queue.`;
    };
    const reset = () => { moved = SERVICES.map(() => false); draw(); };
    $('[data-act="reset"]', el).addEventListener("click", reset);
    reset();
  }
})();
