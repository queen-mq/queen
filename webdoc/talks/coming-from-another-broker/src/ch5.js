
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
