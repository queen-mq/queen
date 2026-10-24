/**
 * The tabs of <SdkCode>: rendered on the server, wired here. Every block shares one
 * sync key, so a language picked in one block is picked in all of them, on this
 * page and the next.
 */
import { initTabs, mount } from "@cloudflare/nimbus-docs/client";

mount("[data-sdk]", (root) => {
  const checkTpl = root.querySelector<HTMLTemplateElement>("[data-sdk-icon-check]");
  const copyTpl = root.querySelector<HTMLTemplateElement>("[data-sdk-icon-copy]");
  const timers = new Set<number>();
  const listeners: Array<[HTMLButtonElement, EventListener]> = [];

  root.querySelectorAll<HTMLButtonElement>("[data-sdk-copy]").forEach((btn) => {
    const onCopy = async () => {
      try {
        await navigator.clipboard.writeText(btn.dataset.code ?? "");
      } catch {
        return;
      }
      if (checkTpl) btn.replaceChildren(checkTpl.content.cloneNode(true));
      const timer = window.setTimeout(() => {
        timers.delete(timer);
        if (copyTpl) btn.replaceChildren(copyTpl.content.cloneNode(true));
      }, 1500);
      timers.add(timer);
    };
    btn.addEventListener("click", onCopy);
    listeners.push([btn, onCopy as EventListener]);
  });

  const tabs = initTabs({
    container: root,
    tabSelector: "[data-sdk-tab]",
    panelSelector: "[data-sdk-panel]",
    rovingTabindex: true,
    sync: { key: "ui-synced-tabs__queen-sdk" },
  });

  return () => {
    for (const [btn, onCopy] of listeners) btn.removeEventListener("click", onCopy);
    for (const timer of timers) window.clearTimeout(timer);
    tabs.destroy();
  };
});
