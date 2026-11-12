
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

