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

