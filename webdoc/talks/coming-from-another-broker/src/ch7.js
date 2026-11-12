
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
