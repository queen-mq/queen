
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
