// Before the first paint: the theme picked last in the talk, else the system's.
(function () {
  var m = null;
  try { m = localStorage.getItem("qk-mode"); } catch (e) {}
  if (!m) m = matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  document.documentElement.dataset.mode = m;
})();
