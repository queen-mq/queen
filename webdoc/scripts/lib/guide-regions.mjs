/**
 * The regions of the five-minute guide's programs, one implementation for
 * every reader of them.
 *
 * Each language has one program under `examples/tutorials/` that runs every
 * step of the guide (`examples/tutorials/run.sh` runs them). A step's code is
 * the text between `guide:start(<step>)` and `guide:end`, minus lines that end
 * in `guide:hide` (test bookkeeping inside a handler), with the common
 * indentation removed. A step that appears more than once in a file (Go and
 * Rust put their imports at the top and the rest in `main`) is its regions
 * joined by a blank line.
 *
 * The homepage (`src/lib/home-guide.ts`) and the README check
 * (`scripts/check-readme.mjs`) both read the programs through this module, so
 * they cannot disagree about what a step's code is.
 */

const START = /guide:start\(([a-z][a-z-]*)\)\s*$/;
const END = /guide:end\s*$/;
const HIDE = /guide:hide\s*$/;

/**
 * Strip the common indentation and the blank lines around a region.
 * @param {string[]} lines
 * @returns {string}
 */
function tidy(lines) {
  const kept = [...lines];
  while (kept.length && !kept[0].trim()) kept.shift();
  while (kept.length && !kept[kept.length - 1].trim()) kept.pop();
  const indents = kept.filter((l) => l.trim()).map((l) => (l.match(/^[ \t]*/) ?? [""])[0].length);
  const cut = indents.length ? Math.min(...indents) : 0;
  return kept.map((l) => l.slice(cut).replace(/\s+$/, "")).join("\n");
}

/**
 * Every step's code in one program, by step id.
 * @param {string} source the program's text
 * @param {string} file its path, for the error messages
 * @returns {Map<string, string>}
 */
export function guideRegions(source, file) {
  /** @type {Map<string, string[]>} */
  const parts = new Map();
  /** @type {string | null} */
  let open = null;
  /** @type {string[]} */
  let buffer = [];
  source.split("\n").forEach((line, i) => {
    const start = line.match(START);
    if (start) {
      if (open) throw new Error(`${file}:${i + 1}: guide:start(${start[1]}) inside guide:start(${open})`);
      open = start[1];
      buffer = [];
    } else if (END.test(line)) {
      if (!open) throw new Error(`${file}:${i + 1}: guide:end without a guide:start`);
      parts.set(open, [...(parts.get(open) ?? []), tidy(buffer)]);
      open = null;
    } else if (open && !HIDE.test(line)) {
      buffer.push(line);
    }
  });
  if (open) throw new Error(`${file}: guide:start(${open}) never closed`);
  return new Map([...parts].map(([id, chunks]) => [id, chunks.join("\n\n")]));
}
