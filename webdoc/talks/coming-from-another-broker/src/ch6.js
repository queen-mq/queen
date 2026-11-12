
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
