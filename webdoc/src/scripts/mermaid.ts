let diagrams: HTMLPreElement[] = [];
const captured = new WeakSet<HTMLPreElement>();

// Full-screen expand dialog (lazy — only created when needed)
let dialog: HTMLDialogElement | null = null;

function uniqueMermaidId(): string {
	const random =
		globalThis.crypto?.randomUUID?.() ??
		`${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
	return `mermaid-${random}`;
}

function captureDiagramSource(diagram: HTMLPreElement): void {
	if (captured.has(diagram)) return;
	diagram.setAttribute("data-diagram", diagram.textContent as string);
	captured.add(diagram);
}

function showRenderError(diagram: HTMLPreElement): void {
	captureDiagramSource(diagram);
	diagram.textContent = "Diagram failed to render.";
	diagram.setAttribute("data-error", "true");
	diagram.setAttribute("data-processed", "true");
}

function getDialog(): HTMLDialogElement {
	if (dialog) return dialog;

	dialog = document.createElement("dialog");
	dialog.className = "mermaid-dialog";
	dialog.innerHTML = `
		<div class="mermaid-dialog-body"></div>
		<button class="mermaid-dialog-close" aria-label="Close">
			<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
				<line x1="18" y1="6" x2="6" y2="18"></line>
				<line x1="6" y1="6" x2="18" y2="18"></line>
			</svg>
		</button>
	`;
	document.body.appendChild(dialog);

	function closeWithAnimation() {
		if (!dialog || !dialog.open) return;
		dialog.classList.add("closing");
		dialog.addEventListener(
			"animationend",
			() => {
				dialog!.classList.remove("closing");
				dialog!.close();
				document.documentElement.style.overflow = "";
			},
			{ once: true },
		);
	}

	dialog.addEventListener("click", (e) => {
		if (e.target === dialog) closeWithAnimation();
	});
	dialog
		.querySelector(".mermaid-dialog-close")
		?.addEventListener("click", () => {
			closeWithAnimation();
		});
	// Handle Escape key — native dialog closes immediately,
	// so we intercept cancel to animate first
	dialog.addEventListener("cancel", (e) => {
		e.preventDefault();
		closeWithAnimation();
	});

	return dialog;
}

function openDiagram(container: HTMLElement) {
	const d = getDialog();
	const clone = container.cloneNode(true) as HTMLElement;

	// Remove the expand button from the clone
	clone.querySelector(".mermaid-expand")?.remove();

	// Let the SVG scale freely in the expanded view
	const svg = clone.querySelector("svg");
	if (svg) {
		svg.removeAttribute("style");
		svg.setAttribute("width", "100%");
		svg.setAttribute("height", "auto");
	}

	const body = d.querySelector(".mermaid-dialog-body");
	if (!body) return;
	body.replaceChildren(clone);

	// Close dialog when clicking Mermaid `click` links inside the expanded view.
	clone.addEventListener("click", (e) => {
		const target = e.target as Element;
		const anchor = target.closest("a");
		const clickable = target.closest(".clickable");
		if (anchor || clickable) {
			// Skip animation for link clicks — navigate immediately
			d.close();
			document.documentElement.style.overflow = "";
		}
	});

	document.documentElement.style.overflow = "hidden";
	d.showModal();
}

// Get computed font family from CSS variable
function getFontFamily(): string {
	const computedStyle = getComputedStyle(document.documentElement);
	const font = computedStyle.getPropertyValue("--nb-font-sans").trim();
	return font || "system-ui, -apple-system, sans-serif";
}

function getPageBackground(): string {
	const style = getComputedStyle(document.documentElement);
	const bg = style.getPropertyValue("--nb-background").trim();
	if (isMermaidSupportedColor(bg)) return bg;
	return document.documentElement.getAttribute("data-mode") === "dark"
		? "#0f0f0f"
		: "#ffffff";
}

function getThemeColor(name: string, fallback: string): string {
	const value = getComputedStyle(document.documentElement)
		.getPropertyValue(name)
		.trim();
	return isMermaidSupportedColor(value) ? value : fallback;
}

function isMermaidSupportedColor(value: string): boolean {
	return (
		/^#(?:[0-9a-f]{3,4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.test(value) ||
		/^rgba?\(\s*\d+(?:\.\d+)?%?\s*,\s*\d+(?:\.\d+)?%?\s*,\s*\d+(?:\.\d+)?%?(?:\s*,\s*(?:0|1|0?\.\d+|\d+(?:\.\d+)?%))?\s*\)$/i.test(value) ||
		/^hsla?\(\s*-?\d+(?:\.\d+)?(?:deg|rad|turn)?\s*,\s*\d+(?:\.\d+)?%\s*,\s*\d+(?:\.\d+)?%(?:\s*,\s*(?:0|1|0?\.\d+|\d+(?:\.\d+)?%))?\s*\)$/i.test(value)
	);
}

// Create wrapper container with expand affordance
function wrapDiagram(diagram: HTMLPreElement) {
	// Skip if already wrapped
	if (diagram.parentElement?.classList.contains("mermaid-container")) {
		return;
	}

	// Create container
	const container = document.createElement("div");
	container.className = "mermaid-container";

	// Wrap the diagram
	diagram.parentNode?.insertBefore(container, diagram);
	container.appendChild(diagram);

	// Add expand button
	const expandBtn = document.createElement("button");
	expandBtn.className = "mermaid-expand";
	expandBtn.setAttribute("aria-label", "Expand diagram");
	expandBtn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
		<polyline points="15 3 21 3 21 9"></polyline>
		<polyline points="9 21 3 21 3 15"></polyline>
		<line x1="21" y1="3" x2="14" y2="10"></line>
		<line x1="3" y1="21" x2="10" y2="14"></line>
	</svg>`;
	expandBtn.addEventListener("click", () => openDiagram(container));
	container.appendChild(expandBtn);
}

async function render() {
	diagrams.forEach(captureDiagramSource);

	let mermaid: typeof import("mermaid").default;
	try {
		// Dynamically import mermaid — the ~2.5 MB bundle is only fetched
		// on the pages that actually contain diagrams.
		({ default: mermaid } = await import("mermaid"));
	} catch (e) {
		diagrams.forEach(showRenderError);
		console.error("Mermaid load failed:", e);
		return;
	}

	const isLight =
		document.documentElement.getAttribute("data-mode") !== "dark";
	const fontFamily = getFontFamily();
	const pageBg = getPageBackground();
	const accent = getThemeColor("--nb-primary", isLight ? "#ff4801" : "#ff6524");

	const lightThemeVars = {
		fontFamily,
		primaryColor: "#ffffff",
		primaryBorderColor: accent,
		primaryTextColor: "#1d1d1d",
		secondaryColor: "#f3f4f6",
		secondaryBorderColor: "#9ca3af",
		secondaryTextColor: "#1d1d1d",
		tertiaryColor: "#f3f4f6",
		tertiaryBorderColor: "#9ca3af",
		tertiaryTextColor: "#1d1d1d",
		lineColor: accent,
		textColor: "#1d1d1d",
		mainBkg: "#ffffff",
		errorBkgColor: "#fef2f2",
		errorTextColor: "#7f1d1d",
		edgeLabelBackground: pageBg,
		labelBackground: pageBg,
	};

	const darkThemeVars = {
		fontFamily,
		primaryColor: "#1f1f1f",
		primaryBorderColor: accent,
		primaryTextColor: "#f2f2f2",
		secondaryColor: "#262626",
		secondaryBorderColor: "#6b7280",
		secondaryTextColor: "#f2f2f2",
		tertiaryColor: "#262626",
		tertiaryBorderColor: "#6b7280",
		tertiaryTextColor: "#f2f2f2",
		lineColor: accent,
		textColor: "#f2f2f2",
		mainBkg: "#1f1f1f",
		background: "#0f0f0f",
		errorBkgColor: "#3c0501",
		errorTextColor: "#ffefee",
		edgeLabelBackground: pageBg,
		labelBackground: pageBg,
	};

	const themeVariables = isLight ? lightThemeVars : darkThemeVars;

	try {
		// Initialize once before the loop — config is identical for all diagrams
		mermaid.initialize({
			startOnLoad: false,
			theme: "base",
			themeVariables,
			flowchart: {
				htmlLabels: true,
				useMaxWidth: true,
				curve: "linear",
			},
		});
	} catch (e) {
		diagrams.forEach(showRenderError);
		console.error("Mermaid initialize failed:", e);
		return;
	}

	for (const diagram of diagrams) {
		try {
			const def = diagram.getAttribute("data-diagram") as string;

			const { svg } = await mermaid.render(uniqueMermaidId(), def);
			diagram.innerHTML = svg;
			diagram.removeAttribute("data-error");

			wrapDiagram(diagram);
			diagram.setAttribute("data-processed", "true");
		} catch (e) {
			showRenderError(diagram);
			console.error("Mermaid render failed:", e);
		}
	}

}

const obs = new MutationObserver(() => {
	if (diagrams.length > 0) render();
});

obs.observe(document.documentElement, {
	attributes: true,
	attributeFilter: ["data-mode"],
});

function setup() {
	diagrams = Array.from(
		document.querySelectorAll<HTMLPreElement>("pre.mermaid"),
	);
	// Body was swapped by the router — any previous dialog is gone.
	dialog = null;
	if (diagrams.length > 0) render();
}

// Fires on initial load and after every client-side navigation.
document.addEventListener("astro:page-load", setup);
