/**
 * The code of the homepage's five-minute guide, read from the programs that run it.
 *
 * Each language has one program under `examples/tutorials/` that performs every
 * step of the guide against a real broker and checks the outcome
 * (`examples/tutorials/run.sh` runs them). The page shows the regions between
 * `guide:start(<step>)` and `guide:end`, verbatim, minus lines that end in
 * `guide:hide` (test bookkeeping inside a handler). So the homepage code is
 * never a transcription: it is the code that ran.
 *
 * A step that appears more than once in a file (Go and Rust put their imports
 * at the top and the rest in `main`) is the concatenation of its regions. A
 * region that goes missing fails the build, unless the step is listed in
 * `missing` for that language with the note the page shows instead.
 */

import js from "../../../examples/tutorials/js/00-five-minutes.mjs?raw";
import py from "../../../examples/tutorials/py/00_five_minutes.py?raw";
import go from "../../../examples/tutorials/go/five-minutes/main.go?raw";
import rust from "../../../examples/tutorials/rust/src/bin/00_five_minutes.rs?raw";
import php from "../../../examples/tutorials/php/00-five-minutes.php?raw";

export type GuideLangId = "js" | "py" | "go" | "rust" | "php";

export interface GuideLang {
  id: GuideLangId;
  /** The selector's label. */
  label: string;
  /** Shiki's language name. */
  lang: string;
  /** Path from the repository root, for the "the program behind it" link. */
  file: string;
  /** The command that adds the client, shown in step 1. */
  install: string;
  /** Steps this language has no code for, with the sentence shown instead. */
  missing?: Partial<Record<string, string>>;
  source: string;
}

export const GUIDE_LANGS: GuideLang[] = [
  {
    id: "js",
    label: "JavaScript",
    lang: "js",
    file: "examples/tutorials/js/00-five-minutes.mjs",
    install: "npm install queen-mq",
    source: js,
  },
  {
    id: "py",
    label: "Python",
    lang: "python",
    file: "examples/tutorials/py/00_five_minutes.py",
    install: "pip install queen-mq",
    source: py,
  },
  {
    id: "go",
    label: "Go",
    lang: "go",
    file: "examples/tutorials/go/five-minutes/main.go",
    install: "go get github.com/smartpricing/queen/clients/client-go/v2",
    source: go,
  },
  {
    id: "rust",
    label: "Rust",
    lang: "rust",
    file: "examples/tutorials/rust/src/bin/00_five_minutes.rs",
    install: "cargo add queen-mq serde_json\ncargo add tokio --features rt-multi-thread,macros",
    source: rust,
  },
  {
    id: "php",
    label: "PHP",
    lang: "php",
    file: "examples/tutorials/php/00-five-minutes.php",
    install: "composer require queen-mq/php-client",
    missing: {
      stream:
        "PHP has no streams client. A stream reads any queue, whoever pushes to it, so run this " +
        "step from JavaScript, Python, Go or Rust.",
    },
    source: php,
  },
];

const START = /guide:start\(([a-z][a-z-]*)\)\s*$/;
const END = /guide:end\s*$/;
const HIDE = /guide:hide\s*$/;

/** Strip the common indentation and the blank lines around a region. */
function tidy(lines: string[]): string {
  while (lines.length && !lines[0].trim()) lines.shift();
  while (lines.length && !lines[lines.length - 1].trim()) lines.pop();
  const indents = lines.filter((l) => l.trim()).map((l) => l.match(/^[ \t]*/)![0].length);
  const cut = indents.length ? Math.min(...indents) : 0;
  return lines.map((l) => l.slice(cut).replace(/\s+$/, "")).join("\n");
}

function regions(lang: GuideLang): Map<string, string> {
  const parts = new Map<string, string[]>();
  let open: string | null = null;
  let buffer: string[] = [];
  lang.source.split("\n").forEach((line, i) => {
    const start = line.match(START);
    if (start) {
      if (open) throw new Error(`${lang.file}:${i + 1}: guide:start(${start[1]}) inside guide:start(${open})`);
      open = start[1];
      buffer = [];
    } else if (END.test(line)) {
      if (!open) throw new Error(`${lang.file}:${i + 1}: guide:end without a guide:start`);
      parts.set(open, [...(parts.get(open) ?? []), tidy(buffer)]);
      open = null;
    } else if (open && !HIDE.test(line)) {
      buffer.push(line);
    }
  });
  if (open) throw new Error(`${lang.file}: guide:start(${open}) never closed`);
  return new Map([...parts].map(([id, chunks]) => [id, chunks.join("\n\n")]));
}

const BY_LANG = new Map(GUIDE_LANGS.map((l) => [l.id, regions(l)]));

/** Every code id a step of the guide shows. */
export const GUIDE_CODE_IDS = [
  "configure",
  "push",
  "consume",
  "consume-group",
  "transaction",
  "replay",
  "fetch",
  "lock",
  "stream",
  "worker",
  "caller",
];

for (const lang of GUIDE_LANGS) {
  for (const id of GUIDE_CODE_IDS) {
    const has = BY_LANG.get(lang.id)!.has(id);
    if (!has && !lang.missing?.[id]) throw new Error(`${lang.file}: no guide:start(${id}) region`);
  }
}

/** The code one language shows for one code id, or null with `missing` set. */
export function guideCode(lang: GuideLang, id: string): string | null {
  return BY_LANG.get(lang.id)!.get(id) ?? null;
}
