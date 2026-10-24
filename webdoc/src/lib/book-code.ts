/**
 * The code on the documentation pages, in the six languages of the SDKs, read from
 * the programs that run it.
 *
 * Each language has three programs under `examples/tutorials/`: the homepage's
 * five-minute guide, the client guide (chapter 2 of the book: queues, push, pop,
 * acks, transactions, KV, locks, timers, ephemeral queues, streams) and the patterns
 * (chapter 3: work queue, pub/sub, request/reply, ...). Every program performs each
 * of its steps against a real broker and checks the outcome
 * (`examples/tutorials/run.sh` runs them all). A page names a step,
 * `<SdkCode id="push" />`, and shows that step's region in every language, cut out
 * by `scripts/lib/guide-regions.mjs`: the same cutter the book, the homepage and the
 * README check use. So the code on a page is never a transcription. It is the code
 * that ran.
 *
 * A step id is unique across a language's three programs. The JavaScript programs
 * are the reference: a page that names a step JavaScript lacks fails the build. A
 * step another language cannot express (PHP has no streams client) is listed in
 * that language's `missing`, with the sentence the page shows instead; any other
 * gap fails the build too.
 */

import { guideRegions } from "../../scripts/lib/guide-regions.mjs";

export type SdkId = "js" | "py" | "go" | "rust" | "php" | "cpp";

export interface SdkLang {
  id: SdkId;
  /** The tab's label. Tabs with the same label switch together on a page. */
  label: string;
  /** Shiki's language name. */
  lang: string;
  /** The three programs, from the repository root. */
  files: string[];
  /** The command that adds the client. */
  install: string;
  /** Steps this language has no code for, with the sentence shown instead. */
  missing: Record<string, string>;
}

const T = "../../../examples/tutorials/";

// Literal paths: Vite resolves the list at build time, and a program that does not
// exist yet is simply absent from the map (its steps then fail the check below).
const SOURCES = import.meta.glob(
  [
    "../../../examples/tutorials/js/00-five-minutes.mjs",
    "../../../examples/tutorials/js/book-client-guide.mjs",
    "../../../examples/tutorials/js/book-use-cases.mjs",
    "../../../examples/tutorials/py/00_five_minutes.py",
    "../../../examples/tutorials/py/book_client_guide.py",
    "../../../examples/tutorials/py/book_use_cases.py",
    "../../../examples/tutorials/go/five-minutes/main.go",
    "../../../examples/tutorials/go/book-client-guide/main.go",
    "../../../examples/tutorials/go/book-use-cases/main.go",
    "../../../examples/tutorials/rust/src/bin/00_five_minutes.rs",
    "../../../examples/tutorials/rust/src/bin/book_client_guide.rs",
    "../../../examples/tutorials/rust/src/bin/book_use_cases.rs",
    "../../../examples/tutorials/php/00-five-minutes.php",
    "../../../examples/tutorials/php/book-client-guide.php",
    "../../../examples/tutorials/php/book-use-cases.php",
    "../../../examples/tutorials/cpp/00-five-minutes.cpp",
    "../../../examples/tutorials/cpp/book-client-guide.cpp",
    "../../../examples/tutorials/cpp/book-use-cases.cpp",
  ],
  { query: "?raw", import: "default", eager: true },
) as Record<string, string>;

const NO_STREAMS = (lang: string) =>
  `${lang} has no streams client. A stream reads any queue, whoever pushes to it, so run this ` +
  "step from JavaScript, Python, Go or Rust.";

/** The steps that are streams: a language without a streams client lists them all. */
const STREAM_STEPS = ["stream", "stream-sliding", "stream-session", "stream-hourly", "stream-gate", "uc-trace", "uc-rate"];
const noStreams = (lang: string) => Object.fromEntries(STREAM_STEPS.map((id) => [id, NO_STREAMS(lang)]));

export const SDK_LANGS: SdkLang[] = [
  {
    id: "js",
    label: "JavaScript",
    lang: "js",
    files: ["js/00-five-minutes.mjs", "js/book-client-guide.mjs", "js/book-use-cases.mjs"],
    install: "npm install queen-mq",
    missing: {},
  },
  {
    id: "py",
    label: "Python",
    lang: "python",
    files: ["py/00_five_minutes.py", "py/book_client_guide.py", "py/book_use_cases.py"],
    install: "pip install queen-mq",
    missing: {},
  },
  {
    id: "go",
    label: "Go",
    lang: "go",
    files: ["go/five-minutes/main.go", "go/book-client-guide/main.go", "go/book-use-cases/main.go"],
    install: "go get github.com/smartpricing/queen/clients/client-go/v2",
    missing: {},
  },
  {
    id: "rust",
    label: "Rust",
    lang: "rust",
    files: ["rust/src/bin/00_five_minutes.rs", "rust/src/bin/book_client_guide.rs", "rust/src/bin/book_use_cases.rs"],
    install: "cargo add queen-mq serde_json\ncargo add tokio --features rt-multi-thread,macros",
    missing: {},
  },
  {
    id: "php",
    label: "PHP",
    lang: "php",
    files: ["php/00-five-minutes.php", "php/book-client-guide.php", "php/book-use-cases.php"],
    install: "composer require queen-mq/php-client",
    missing: noStreams("PHP"),
  },
  {
    id: "cpp",
    label: "C++",
    lang: "cpp",
    files: ["cpp/00-five-minutes.cpp", "cpp/book-client-guide.cpp", "cpp/book-use-cases.cpp"],
    install: "# header-only: clients/client-cpp/queen_client.hpp, with cpp-httplib and nlohmann/json",
    missing: noStreams("C++"),
  },
];

/** Every step of one language, by id, with the program it comes from. */
function regionsOf(lang: SdkLang): Map<string, { code: string; file: string }> {
  const all = new Map<string, { code: string; file: string }>();
  for (const rel of lang.files) {
    const source = SOURCES[T + rel];
    if (source === undefined) continue;
    const file = `examples/tutorials/${rel}`;
    for (const [id, code] of guideRegions(source, file)) {
      const seen = all.get(id);
      if (seen) throw new Error(`guide:start(${id}) is in both ${seen.file} and ${file}`);
      all.set(id, { code, file });
    }
  }
  return all;
}

const REGIONS = new Map(SDK_LANGS.map((l) => [l.id, regionsOf(l)]));

export interface SdkVariant {
  lang: SdkLang;
  /** The step's code, or null when the language lists the step in `missing`. */
  code: string | null;
  /** The program the code comes from, from the repository root. */
  file: string | null;
  /** The sentence shown instead of code. */
  note: string | null;
}

/**
 * One step in every language. Throws when JavaScript lacks the step, or when another
 * language lacks it without a `missing` note.
 */
export function sdkCode(id: string, where = "a page"): SdkVariant[] {
  return SDK_LANGS.map((lang) => {
    const found = REGIONS.get(lang.id)!.get(id);
    if (found) return { lang, code: found.code, file: found.file, note: null };
    const note = lang.missing[id];
    if (note) return { lang, code: null, file: null, note };
    // While a port is being written: QUEEN_DOCS_ALLOW_MISSING_SDK=1 shows a gap instead of failing.
    if (lang.id !== "js" && process.env.QUEEN_DOCS_ALLOW_MISSING_SDK === "1") {
      return { lang, code: null, file: null, note: `The ${lang.label} version of this step is not written yet.` };
    }
    throw new Error(
      `<SdkCode id="${id}"> on ${where}: no guide:start(${id}) region in ${lang.label} ` +
        `(${lang.files.map((f) => `examples/tutorials/${f}`).join(", ")}). ` +
        `Add the region, or list the step in that language's \`missing\` in src/lib/book-code.ts.`,
    );
  });
}

/** Every step id the JavaScript programs define, for the checks. */
export function sdkStepIds(): string[] {
  return [...REGIONS.get("js")!.keys()];
}
