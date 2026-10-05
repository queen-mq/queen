/**
 * theme-toggle.client.ts — light, dark, system. Writes the choice to
 * localStorage ("ui-mode"); "system" removes it, so the OS decides.
 * BaseLayout's pre-paint script owns DOM application so view transitions,
 * OS changes, and cross-tab edits stay in sync.
 */

import { mount } from "@cloudflare/nimbus-docs/client";

declare global {
  interface Window {
    __nbApplyTheme?: () => void;
  }
}

function initThemeToggle(button: HTMLElement): () => void {
  const order = ["light", "dark", "system"] as const;

  function handleClick() {
    const current = button.getAttribute("data-nb-pref");
    const next = order[(order.indexOf(current as (typeof order)[number]) + 1) % order.length];
    try {
      if (next === "system") localStorage.removeItem("ui-mode");
      else localStorage.setItem("ui-mode", next);
    } catch {
      // Ignore storage errors (private mode / restricted contexts).
    }
    window.__nbApplyTheme?.();
  }

  window.__nbApplyTheme?.();
  button.addEventListener("click", handleClick);
  return () => button.removeEventListener("click", handleClick);
}

mount("[data-nb-theme-toggle]", initThemeToggle);
