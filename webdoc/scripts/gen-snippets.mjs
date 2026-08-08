/**
 * Extract documented code snippets from files the test harness executes.
 *
 * A snippet is a region of a real source file delimited by markers:
 *
 *   // docs:start(push-one-message)
 *   ...code...
 *   // docs:end
 *
 * Each region becomes a partial under src/content/partials/snippets/, carrying
 * a header that names the file it came from and the suite that runs it. Pages
 * include them with `<Render file="snippets/<id>" />`, so a published snippet
 * is never a transcription — it is the code that ran.
 *
 * Marker syntax is comment-flavour agnostic: anything ending in
 * `docs:start(<id>)` opens a region and anything ending in `docs:end` closes
 * it, so `//`, `#` and `--` all work.
 */

import { mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { OUT_DIR, REPO, cell, isCheck, repoPath } from "./lib/source.mjs";

const SNIPPET_DIR = join(OUT_DIR, "..", "snippets");

/**
 * Where to look, and what running that code proves. `suite` is the label the
 * snippet header shows; `command` is what a reader can run to see it pass.
 */
const SOURCES = [
  {
    dir: "clients/client-js/test-v2",
    lang: "js",
    suite: "JavaScript client suite",
    command: "test/run.sh --suite js",
  },
  {
    dir: "clients/client-py/tests",
    lang: "python",
    suite: "Python client suite",
    command: "test/run.sh --suite py",
  },
  {
    dir: "clients/client-go/tests",
    lang: "go",
    suite: "Go client suite",
    command: "test/run.sh --suite go",
  },
  {
    dir: "clients/client-cpp",
    lang: "cpp",
    suite: "C++ client suite",
    command: "test/run.sh --suite cpp",
  },
  {
    dir: "clients/client-rust/tests",
    lang: "rust",
    suite: "Rust client suite",
    command: "test/run.sh --suite rust-client",
  },
  // The embedded engine's end-to-end smoke: a real in-process broker against a
  // throwaway Postgres. Gated on QUEEN_EMBEDDED_TEST_PG, hence the explicit
  // command instead of a test/run.sh suite.
  {
    dir: "server/tests",
    lang: "rust",
    suite: "Embedded broker smoke",
    command:
      "QUEEN_EMBEDDED_TEST_PG=localhost:5464 cargo test --manifest-path server/Cargo.toml --test embedded_smoke -- --ignored",
  },
  // The complete programs behind the Full examples section. These are whole
  // files rather than regions: each one is marked from its first line to its
  // last, so what a reader copies is exactly what runs.
  {
    dir: "examples/full",
    lang: "js",
    suite: "Full examples",
    command: "examples/full/run.sh",
  },
];

const EXT_LANG = {
  ".js": "js",
  ".mjs": "js",
  ".ts": "ts",
  ".py": "python",
  ".go": "go",
  ".cpp": "cpp",
  ".hpp": "cpp",
  ".php": "php",
  ".rs": "rust",
  ".sh": "bash",
};

function walk(abs, out = []) {
  for (const name of readdirSync(abs)) {
    if (name === "node_modules" || name === "__pycache__" || name.startsWith(".")) continue;
    const p = join(abs, name);
    const st = statSync(p);
    if (st.isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
}

const START = /docs:start\(([a-z0-9][a-z0-9-]*)\)\s*$/;
const END = /docs:end\s*$/;

function extract(file, text) {
  const regions = [];
  const lines = text.split("\n");
  let open = null;
  lines.forEach((line, i) => {
    const s = line.match(START);
    if (s) {
      if (open) throw new Error(`${file}:${i + 1}: docs:start(${s[1]}) inside an open region`);
      open = { id: s[1], from: i + 1, body: [] };
      return;
    }
    if (END.test(line)) {
      if (!open) throw new Error(`${file}:${i + 1}: docs:end without a docs:start`);
      regions.push(open);
      open = null;
      return;
    }
    if (open) open.body.push(line);
  });
  if (open) throw new Error(`${file}: docs:start(${open.id}) never closed`);
  return regions;
}

/** Strip the common leading indentation so a snippet reads as top-level code. */
function dedent(lines) {
  const meaningful = lines.filter((l) => l.trim().length > 0);
  if (meaningful.length === 0) return lines;
  const indent = Math.min(...meaningful.map((l) => l.match(/^\s*/)[0].length));
  return lines.map((l) => l.slice(indent));
}

function main() {
  const check = isCheck();
  const found = new Map();

  for (const src of SOURCES) {
    let files;
    try {
      files = walk(repoPath(src.dir));
    } catch {
      continue; // a client directory that is not present in this checkout
    }
    for (const abs of files) {
      const rel = relative(REPO, abs);
      const ext = abs.slice(abs.lastIndexOf("."));
      const lang = EXT_LANG[ext] ?? src.lang;
      let text;
      try {
        text = readFileSync(abs, "utf8");
      } catch {
        continue; // binary or unreadable
      }
      if (!text.includes("docs:start(")) continue;
      for (const region of extract(rel, text)) {
        if (found.has(region.id)) {
          throw new Error(
            `duplicate snippet id "${region.id}": ${found.get(region.id).file} and ${rel}`,
          );
        }
        found.set(region.id, {
          id: region.id,
          file: rel,
          lang,
          suite: src.suite,
          command: src.command,
          body: dedent(region.body).join("\n").replace(/^\n+|\n+$/g, ""),
        });
      }
    }
  }

  const rendered = [...found.values()].sort((a, b) => a.id.localeCompare(b.id));
  const contents = new Map();
  for (const s of rendered) {
    contents.set(
      `${s.id}.mdx`,
      [
        "---",
        "params: []",
        `description: "Verified snippet extracted from ${s.file}."`,
        "---",
        "",
        `{/* GENERATED FILE. Do not edit by hand.`,
        `    Extracted from ${s.file} by webdoc/scripts/gen-snippets.mjs`,
        `    Regenerate with: pnpm --dir webdoc gen`,
        `*/}`,
        "",
        "```" + s.lang + ` title="${s.file}"`,
        s.body,
        "```",
        "",
      ].join("\n"),
    );
  }

  // An index partial, so a page can show the whole verified inventory.
  const index = [
    "| Snippet | Language | Source file | Suite |",
    "| --- | --- | --- | --- |",
    ...rendered.map(
      (s) => `| \`${s.id}\` | ${s.lang} | \`${cell(s.file)}\` | ${cell(s.suite)} |`,
    ),
  ].join("\n");
  contents.set(
    "index.mdx",
    [
      "---",
      "params: []",
      'description: "Inventory of every verified code snippet published on this site."',
      "---",
      "",
      "{/* GENERATED FILE. Do not edit by hand. */}",
      "",
      index,
      "",
    ].join("\n"),
  );

  if (check) {
    let drifted = false;
    for (const [name, content] of contents) {
      let current = "";
      try {
        current = readFileSync(join(SNIPPET_DIR, name), "utf8");
      } catch {
        /* missing counts as drift */
      }
      if (current !== content) drifted = true;
    }
    return { drifted, title: `${rendered.length} verified snippets`, file: SNIPPET_DIR };
  }

  // Rewrite the directory so a removed marker removes its partial.
  rmSync(SNIPPET_DIR, { recursive: true, force: true });
  mkdirSync(SNIPPET_DIR, { recursive: true });
  for (const [name, content] of contents) writeFileSync(join(SNIPPET_DIR, name), content, "utf8");
  return { written: true, drifted: false, title: `${rendered.length} verified snippets`, file: SNIPPET_DIR };
}

const result = main();
if (result.drifted) {
  console.error(`DRIFT: ${result.file} is behind its sources`);
  process.exit(1);
}
console.log(`${result.written ? "wrote" : "ok"}  ${result.title}`);
