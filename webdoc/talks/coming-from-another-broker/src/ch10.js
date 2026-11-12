
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
