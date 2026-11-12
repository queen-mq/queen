
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
