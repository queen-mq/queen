
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
