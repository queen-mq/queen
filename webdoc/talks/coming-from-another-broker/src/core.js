
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

