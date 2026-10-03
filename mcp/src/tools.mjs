// The five tools, three prompts and the resources, all answered from the
// bundle that scripts/build-bundle.mjs derives from the docs and the repo.

import bundle from "./bundle.generated.mjs";
import { Index, tokens } from "./search.mjs";

const SITE = "https://queenmq.com";
export const ENDPOINT = `${SITE}/mcp`;

export class RpcError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

const SDK_LANGS = ["js", "python", "go", "php", "rust", "cpp", "http"];
const LANGUAGES = {
  js: "js", javascript: "js", typescript: "js", ts: "js", node: "js", nodejs: "js", "node.js": "js",
  python: "python", py: "python",
  go: "go", golang: "go",
  php: "php", laravel: "php",
  rust: "rust", rs: "rust",
  cpp: "cpp", "c++": "cpp", cxx: "cpp",
  http: "http", curl: "http", rest: "http",
  kafka: "kafka",
};
const LABEL = { js: "JavaScript", python: "Python", go: "Go", php: "PHP", rust: "Rust", cpp: "C++", http: "HTTP", kafka: "Kafka client" };
const FENCE = { js: "js", python: "python", go: "go", php: "php", rust: "rust", cpp: "cpp", http: "bash" };
const SEVERITY = { breaks: 0, surprise: 1, perf: 2 };

const pageBySlug = new Map(bundle.pages.map((p) => [p.slug, p]));
const sectionText = (s) => pageBySlug.get(s.slug).markdown.slice(s.start, s.end);
const programOf = (s) => bundle.programs[s.source] ?? "";

const PAGE_CAP = 40_000;
const SECTION_CAP = 6_000;
const ANSWER_CAP = 16_000;

const lazy = (make) => {
  let value;
  return () => (value ??= make());
};

const sectionIndex = lazy(() => new Index(bundle.sections, (s) => [
  { text: s.pageTitle, weight: 2 },
  { text: s.heading, weight: 3 },
  { text: sectionText(s), weight: 1 },
]));
const pageIndex = lazy(() => new Index(bundle.pages, (p) => [
  { text: p.slug.replace(/[/-]/g, " "), weight: 3 },
  { text: p.title, weight: 3 },
  { text: p.description, weight: 1 },
]));
// A snippet matches a task by its name and the headings it is shown under; its
// code only breaks ties, or a long program that says "consumer group" forty
// times would beat the snippet called "consume".
const snippetLabelIndex = lazy(() => new Index(bundle.snippets, (s) => [
  { text: s.task.replace(/-/g, " "), weight: 3 },
  { text: s.kind === "embedded" ? "embedded broker in process" : "", weight: 1 },
  { text: s.pages.map((p) => `${p.title} ${p.heading}`).join(" "), weight: 1 },
]));
const snippetCodeIndex = lazy(() => new Index(bundle.snippets, (s) => [{ text: s.code, weight: 1 }]));
const docCodeLabelIndex = lazy(() => new Index(bundle.docCode, (d) => [
  { text: d.heading, weight: 3 },
  { text: `${d.pageTitle} ${d.slug.replace(/[/-]/g, " ")}`, weight: 2 },
]));
const docCodeIndex = lazy(() => new Index(bundle.docCode, (d) => [{ text: d.code, weight: 1 }]));
const errorIndex = lazy(() => new Index(bundle.errors, (e) => [
  { text: e.code, weight: 6 },
  { text: e.section, weight: 2 },
  { text: e.fields.map(([, v]) => v).join(" "), weight: 1 },
]));
const trapIndex = lazy(() => new Index(bundle.traps, (t) => [
  { text: t.title, weight: 3 },
  { text: `${t.symptom} ${t.cause} ${t.fix}`, weight: 1 },
]));

const KAFKA_ALIASES = [
  [/java kafka-clients/i, "java jvm apache kafka-clients kotlin scala"],
  [/spring/i, "spring boot spring-kafka java"],
  [/franz-go/i, "franz go kgo twmb golang"],
  [/kafka-go/i, "segmentio kafka-go go golang"],
  [/sarama/i, "sarama ibm shopify go golang"],
  [/librdkafka/i, "librdkafka kcat c"],
  [/confluent-kafka-python/i, "confluent kafka python confluent_kafka"],
  [/^kafka-python/i, "kafka-python python kafka-python-ng dpkp"],
  [/aiokafka/i, "aiokafka python asyncio"],
  [/confluent\.kafka/i, "confluent kafka dotnet .net csharp c#"],
  [/^kafkajs/i, "kafkajs node javascript typescript"],
  [/kafka-javascript/i, "confluent kafka-javascript node javascript typescript"],
  [/node-rdkafka/i, "node-rdkafka node javascript blizzard"],
  [/platformatic/i, "platformatic node javascript typescript"],
  [/rust crate/i, "rust rdkafka rust-rdkafka"],
  [/ruby/i, "ruby rdkafka-ruby karafka"],
  [/php-rdkafka/i, "php rdkafka laravel"],
  [/brod/i, "brod erlang elixir"],
];
const kafkaIndex = lazy(() => new Index(bundle.kafka.clients, (c) => [
  { text: c.client, weight: 3 },
  { text: KAFKA_ALIASES.filter(([re]) => re.test(c.client)).map(([, words]) => words).join(" "), weight: 2 },
]));

// ---------------------------------------------------------------- helpers

const ok = (text) => ({ content: [{ type: "text", text }] });
const fail = (text) => ({ content: [{ type: "text", text }], isError: true });
const lang = (value) => LANGUAGES[String(value ?? "").trim().toLowerCase()];
const str = (value) => (typeof value === "string" ? value.trim() : "");

function cap(text, max, how) {
  if (text.length <= max) return text;
  const cut = text.lastIndexOf("\n", max);
  return `${text.slice(0, cut > max / 2 ? cut : max)}\n\n[Truncated. ${how}]`;
}

function parseSlug(value) {
  const [path, anchor = ""] = value.replace(SITE, "").split("#");
  return { slug: path.replace(/^\/+|\/+$/g, ""), anchor };
}

const sourceLink = (s) => (s.startsWith("http") ? s : `\`${s}\``);

// ---------------------------------------------------------------- guide

function guide(args) {
  const page = str(args.page);
  const topic = str(args.topic);
  if (page) {
    const { slug, anchor } = parseSlug(page);
    const p = bundle.pages.find((x) => x.slug === slug) ?? bundle.pages.find((x) => slug && x.slug.endsWith(`/${slug}`));
    if (!p) {
      const near = pageIndex().search(page, 5).map(({ doc }) => `\`${doc.slug}\` (${doc.title})`);
      return fail(`No page "${page}".${near.length ? ` Closest: ${near.join(", ")}.` : ""}`);
    }
    const section = str(args.section) || anchor;
    if (section) {
      const want = section.toLowerCase().replace(/^#/, "");
      const own = bundle.sections.filter((s) => s.slug === p.slug);
      const s = own.find((x) => x.heading.toLowerCase() === want || x.url.endsWith(`#${want}`)) ?? own.find((x) => x.heading.toLowerCase().includes(want));
      if (!s) return fail(`No section "${section}" on \`${p.slug}\`. Its sections: ${own.map((x) => x.heading).join(" · ")}`);
      return ok(`# ${p.title} › ${s.heading}\n${s.url}\n\n${cap(sectionText(s), PAGE_CAP, "The rest is on the page.")}`);
    }
    let text = p.markdown;
    if (text.length > PAGE_CAP) {
      const headings = bundle.sections.filter((s) => s.slug === p.slug).map((s) => s.heading);
      text = cap(text, PAGE_CAP, `Read one section with guide(page: "${p.slug}", section: "<heading>"). Sections: ${headings.join(" · ")}`);
    }
    return ok(`${p.url}\n\n${text}`);
  }
  if (!topic) return fail('Pass `topic` (what you want to know, in plain words) or `page` (a slug such as "concepts/transactions").');

  const hits = sectionIndex().search(topic, 15);
  if (!hits.length) return ok(`Nothing in the docs matches "${topic}". Pages: ${bundle.pages.map((p) => p.slug).filter(Boolean).join(", ")}`);
  const parts = [];
  const shown = new Set();
  let used = 0;
  for (const { doc } of hits) {
    if (parts.length === 3 || used > ANSWER_CAP) break;
    const text = cap(sectionText(doc), SECTION_CAP, `Read it all with guide(page: "${doc.slug}", section: "${doc.heading}").`);
    parts.push(`## ${doc.pageTitle} › ${doc.heading}\n${doc.url}\n\n${text}`);
    shown.add(`${doc.slug}#${doc.heading}`);
    used += text.length;
  }
  const more = [];
  for (const { doc } of hits) {
    if (more.length === 5) break;
    if (!shown.has(`${doc.slug}#${doc.heading}`)) more.push(`\`${doc.slug}\` › ${doc.heading}`);
  }
  let out = parts.join("\n\n---\n\n");
  if (more.length) out += `\n\n---\nAlso relevant: ${more.join(" · ")}. Read one with guide(page: "<slug>", section: "<heading>").`;
  return ok(out);
}

// ---------------------------------------------------------------- example

const CODE_CAP = 8_000;

function formatSnippet(s, full) {
  const program = programOf(s);
  const whole = full && program;
  const origin = s.source ? `Cut from \`${s.source}\`, a file Queen's test suite runs.` : "From Queen's tested snippets.";
  const where = s.pages.length ? ` Explained on ${s.pages.map((p) => `${p.url} (${p.title}${p.heading ? ` › ${p.heading}` : ""})`).join(", ")}.` : "";
  const title = whole ? `the whole program, ${s.source}` : s.task.replace(/-/g, " ");
  const code = whole ? program : cap(s.code, CODE_CAP, `The rest: example(task: "${s.task}", language: "${s.language}", full: true).`);
  let out = `### ${LABEL[s.language]}: ${title}\n${origin}${where}\n\n\`\`\`${s.fenceLang}\n${code}\n\`\`\``;
  if (!full && program && s.code.length <= CODE_CAP) out += `\nThe whole program: example(task: "${s.task}", language: "${s.language}", full: true).`;
  return out;
}

function formatDocCode(d) {
  return `### ${LABEL[d.language]}: ${d.pageTitle} › ${d.heading}\nFrom the docs page ${d.url}; written for the page, not cut from a tested file.\n\n\`\`\`${d.fenceLang}\n${d.code}\n\`\`\``;
}

const snippetLabel = (s) => `${s.task.replace(/-/g, " ")} ${s.kind === "embedded" ? "embedded broker" : ""} ${s.pages.map((p) => `${p.title} ${p.heading}`).join(" ")}`;
const docCodeLabel = (d) => `${d.heading} ${d.pageTitle} ${d.slug.replace(/[/-]/g, " ")}`;

/**
 * Candidates whose name or headings match the task, best first: the share of
 * the task's words the label covers decides, a tested snippet wins a tie, and
 * BM25 (label, then code) orders the rest.
 */
function candidates(task, keep) {
  // Coverage weighs each word by its rarity across the docs: matching "kv"
  // says more about a label than matching "state".
  const corpus = sectionIndex();
  const weight = (w) => Math.log(1 + (corpus.docs.length - (corpus.df.get(w) ?? 0) + 0.5) / ((corpus.df.get(w) ?? 0) + 0.5));
  const words = [...new Set(tokens(task))];
  const total = words.reduce((sum, w) => sum + weight(w), 0);
  const coverage = (label) => {
    const have = new Set(tokens(label));
    return total ? words.reduce((sum, w) => sum + (have.has(w) ? weight(w) : 0), 0) / total : 0;
  };
  const collect = (labelIndex, codeIndex, labelOf, tested) => {
    const code = new Map(codeIndex().search(task, 500, keep).map(({ doc, score }) => [doc, score]));
    return labelIndex()
      .search(task, 50, keep)
      .map(({ doc, score }) => ({ doc, tested, coverage: coverage(labelOf(doc)), score: score + 0.3 * (code.get(doc) ?? 0) }));
  };
  return [
    ...collect(snippetLabelIndex, snippetCodeIndex, snippetLabel, true),
    ...collect(docCodeLabelIndex, docCodeIndex, docCodeLabel, false),
  ].sort((a, b) => b.coverage - a.coverage || Number(b.tested) - Number(a.tested) || b.score - a.score);
}

const render = (hit, full) => (hit.tested ? formatSnippet(hit.doc, full) : formatDocCode(hit.doc));

/**
 * The lines of a tested program, in the asked language, where it does what the
 * task names. Code from the developer's own language beats another language's
 * docs code: API names do not translate, and an agent that translates will
 * borrow a field from the wrong SDK. Identifiers are split at their humps, so
 * `ScheduleTimerOp` counts for "timer", and only code lines can anchor the
 * window: a header comment mentions everything.
 */
function programExcerpt(task, language) {
  const corpus = sectionIndex();
  const weight = (w) => Math.log(1 + (corpus.docs.length - (corpus.df.get(w) ?? 0) + 0.5) / ((corpus.df.get(w) ?? 0) + 0.5));
  const words = [...new Set(tokens(task))];
  const total = words.reduce((sum, w) => sum + weight(w), 0);
  if (!total) return null;
  const humps = (line) => tokens(line.replace(/([a-z0-9])([A-Z])/g, "$1 $2"));
  const comment = /^\s*(\/\/|#|\*|\/\*|--)/;
  let best = null;
  const seen = new Set();
  for (const s of bundle.snippets) {
    if (s.language !== language) continue;
    const key = s.source || s.id;
    if (seen.has(key)) continue;
    seen.add(key);
    const lines = (programOf(s) || s.code).split("\n");
    const lineTokens = lines.map(humps);
    lines.forEach((line, i) => {
      if (comment.test(line)) return;
      const score = lineTokens[i].reduce((sum, w) => sum + (words.includes(w) ? weight(w) : 0), 0);
      if (score && (!best || score > best.score)) best = { s, lines, lineTokens, i, score };
    });
  }
  if (!best) return null;
  const from = Math.max(0, best.i - 8);
  const to = Math.min(best.lines.length, best.i + 22);
  const inWindow = new Set(best.lineTokens.slice(from, to).flat());
  const covered = words.reduce((sum, w) => sum + (inWindow.has(w) ? weight(w) : 0), 0) / total;
  if (covered < 0.5) return null;
  const { s } = best;
  const whole = programOf(s) ? ` The whole program: example(task: "${s.task}", language: "${language}", full: true).` : "";
  return `### ${LABEL[language]}: excerpt from ${s.source || s.id}, lines ${from + 1}-${to}\nFrom a program Queen's test suite runs, cut where it does what you asked.${whole}\n\n\`\`\`${s.fenceLang}\n${best.lines.slice(from, to).join("\n")}\n\`\`\``;
}

function example(args) {
  const language = lang(args.language);
  if (!language || language === "kafka") {
    return fail(`\`language\` must be one of ${SDK_LANGS.join(", ")}. For a Kafka client library pointed at Queen, call kafka_client.`);
  }
  const mine = bundle.snippets.filter((s) => s.language === language);
  const catalog = `Tested ${LABEL[language]} snippets: ${[...new Set(mine.map((s) => s.task))].join(", ") || "none yet"}.`;
  const task = str(args.task);
  if (!task || /^(list|all|any|everything|\?)$/i.test(task)) return ok(catalog);
  const full = args.full === true;

  const client = clientLine(language);
  const withClient = (result) => (client && !result.isError ? { ...result, content: [{ type: "text", text: `${result.content[0].text}\n\n${client}` }] } : result);
  const here = candidates(task, (x) => x.language === language);
  const anywhere = candidates(task, null);
  const best = here[0];
  if (best && best.coverage >= (anywhere[0]?.coverage ?? 0)) {
    const parts = [render(best, full)];
    const next = here[1];
    if (!full && next && next.tested === best.tested && next.coverage === best.coverage && next.doc.code.length < 3_500) parts.push(render(next, false));
    const others = [...new Set(here.slice(parts.length).filter((h) => h.tested && h.coverage > 0).map((h) => h.doc.task))].slice(0, 4);
    let out = parts.join("\n\n---\n\n");
    if (others.length) out += `\n\nAlso tested in ${LABEL[language]}: ${others.join(", ")}.`;
    if (!best.tested) out += `\n\nNo tested ${LABEL[language]} snippet covers "${task}". ${catalog}`;
    return withClient(ok(out));
  }

  const excerpt = programExcerpt(task, language);
  if (excerpt) {
    return withClient(ok(`${excerpt}\n\nNo ${LABEL[language]} snippet is named after "${task}", so this is the place a tested program does it. ${catalog}`));
  }

  let out = `No ${LABEL[language]} code covers "${task}". ${catalog}`;
  const users = [...new Set(snippetCodeIndex().search(task, 3, (x) => x.language === language && programOf(x)).map(({ doc }) => doc.task))];
  if (users.length) out += ` Tested ${LABEL[language]} programs whose code mentions it: ${users.map((u) => `${u} (example(task: "${u}", language: "${language}", full: true))`).join(", ")}.`;
  if (anywhere.length) {
    out += `\n\nThe closest code in another language. Translate it with the ${LABEL[language]} SDK's own names, and confirm each method exists before you use it:\n\n${render(anywhere[0], false)}`;
  }
  out += `\n\nThe HTTP API is the ground truth under every SDK: guide(topic: "${task}").`;
  return withClient(ok(out));
}

// ---------------------------------------------------------------- check

function formatTrap(t, language, n) {
  const code = t.code?.[language];
  let out = `${n}. [${t.severity}] ${t.title} (\`${t.id}\`)\n   Symptom: ${t.symptom}\n   Cause: ${t.cause}\n   Fix: ${t.fix}`;
  if (code) out += `\n\`\`\`${FENCE[language] ?? ""}\n${code}\n\`\`\``;
  return `${out}\n   Source: ${t.source.map(sourceLink).join(", ")}`;
}

function check(args) {
  const language = lang(args.language);
  if (!language) return fail(`\`language\` must be one of ${[...SDK_LANGS, "kafka"].join(", ")}.`);
  const applies = (t) => t.applies.includes(language) || (language !== "kafka" && t.applies.includes("all"));
  let traps = bundle.traps.filter(applies);
  if (!traps.length) return ok(`No ${LABEL[language]} traps are recorded yet.`);
  const topic = str(args.topic);
  const rank = new Map(topic ? trapIndex().search(topic, 1000).map(({ doc, score }) => [doc.id, score]) : []);
  traps = [...traps].sort((a, b) => (rank.get(b.id) ?? 0) - (rank.get(a.id) ?? 0) || SEVERITY[a.severity] - SEVERITY[b.severity]);
  const order = topic ? `those about "${topic}" first` : "most damaging first";
  return ok(
    `Queen 2.0 checklist for ${LABEL[language]} code: ${traps.length} items, ${order}. Check every item against the code, fix what applies, and cite the id.\n\n` +
      traps.map((t, i) => formatTrap(t, language, i + 1)).join("\n\n"),
  );
}

// ---------------------------------------------------------------- explain_error

function formatError(e) {
  const kafka = e.origin === "kafka" ? bundle.kafkaCodes.find((c) => c.name === e.code) : null;
  const tag = kafka ? ` (Kafka error ${kafka.code}${kafka.retriable ? ", retriable" : ", not retriable"})` : "";
  const fields = e.fields.slice(1).map(([k, v]) => `- ${k}: ${v}`).join("\n");
  return `**\`${e.code}\`**${tag} · ${e.section}${e.url ? ` · ${e.url}` : ""}\n${fields}`;
}

function explainError(args) {
  const q = str(args.error).replace(/^["'`]+|["'`]+$/g, "");
  if (!q) return fail("Pass the error: an HTTP status, an error `code`, a transaction `reason`, or a Kafka error name or number.");
  const lower = q.toLowerCase();
  const num = /^-?\d+$/.test(q) ? Number(q) : null;
  const httpStatus = num !== null && num >= 100 && num <= 599 ? num : null;
  const kafkaCode = num !== null && httpStatus === null ? bundle.kafkaCodes.find((c) => c.code === num) : bundle.kafkaCodes.find((c) => c.name === q.toUpperCase());

  const rows = [];
  const add = (e) => {
    if (!rows.includes(e)) rows.push(e);
  };
  for (const e of bundle.errors) if (rows.length < 8 && (e.code.toLowerCase() === lower || (kafkaCode && e.code === kafkaCode.name))) add(e);
  if (!rows.length && httpStatus !== null) {
    const re = new RegExp(`\\b${httpStatus}\\b`);
    for (const e of bundle.errors) if (e.origin === "queen" && rows.length < 8 && e.fields.some(([, v]) => re.test(v))) add(e);
  }
  if (!rows.length && !kafkaCode) for (const { doc } of errorIndex().search(q, 5)) add(doc);

  const parts = [];
  if (kafkaCode) {
    parts.push(`Kafka error ${kafkaCode.code} is \`${kafkaCode.name}\` (${kafkaCode.retriable ? "retriable" : "not retriable"}): ${kafkaCode.description}`);
  }
  parts.push(...rows.map(formatError));
  const traps = lower.length >= 3
    ? bundle.traps.filter((t) => `${t.title} ${t.symptom} ${t.cause} ${t.fix}`.toLowerCase().includes(lower)).slice(0, 3)
    : [];
  if (traps.length) {
    parts.push(`Known traps that produce it:\n\n${traps.map((t, i) => formatTrap(t, t.applies[0], i + 1)).join("\n\n")}`);
  }
  if (!parts.length) {
    return ok(`No Queen error matches "${q}". Pass the exact \`code\` or \`reason\` field of the response body; guide(page: "reference/errors") lists every body and code.`);
  }
  return ok(parts.join("\n\n"));
}

// ---------------------------------------------------------------- kafka_client

function kafkaClient(args) {
  const q = str(args.client);
  const known = bundle.kafka.clients.map((c) => c.client).join(", ");
  if (!q) return fail(`Pass the client library, e.g. "sarama", "kafkajs", "confluent-kafka-python". Tested: ${known}.`);
  const hits = kafkaIndex().search(q, 3);
  if (!hits.length) {
    return ok(`"${q}" is not in Queen's Kafka client matrix. Tested: ${known}. How any client connects: guide(page: "guides/kafka").`);
  }
  const top = hits[0].score;
  const rows = hits.filter((h) => h.score >= top * 0.6).map(({ doc }) => doc);
  const parts = rows.map((c) => {
    let out = `### ${c.client}: ${c.result}\n- Versions tested: ${c.versions} (verified ${c.verified})\n- Mandatory config: ${c.config}\n- Main caveat: ${c.caveat}`;
    if (c.result !== "PASS") {
      const key = c.client.toLowerCase().split(" (")[0].replace(/^ibm\//, "");
      const ev = bundle.kafka.evidence.find((e) => e.title.toLowerCase().includes(key));
      if (ev) out += `\n\n${cap(ev.text, 3_000, "The full evidence is in protocols/queen-kafka/compat/CLIENT_MATRIX.md.")}`;
    }
    return out;
  });
  const version = str(args.version);
  if (version) parts.unshift(`Asked about version ${version}: compare it with the tested versions and any version floor in the mandatory config below.`);

  const connect = bundle.sections.find((s) => s.slug === "guides/kafka" && /connect|point|bootstrap|listener|enable|start/i.test(s.heading));
  if (connect) parts.push(`## Connecting (from ${connect.url})\n\n${cap(sectionText(connect), 2_500, `Read it all with guide(page: "guides/kafka").`)}`);
  parts.push(
    `Result: PASS works with the config in its row; PARTIAL works, with a sharp edge (a version floor, a refused lane, a silent degradation). ` +
      `The facade's deliberate differences from Apache Kafka, which every client meets: guide(page: "reference/kafka"). Traps: check(language: "kafka").` +
      (bundle.kafka.updated ? ` Matrix last updated ${bundle.kafka.updated}.` : ""),
  );
  return ok(parts.join("\n\n"));
}

// ---------------------------------------------------------------- setup

const FEATURES = bundle.setup.map((f) => f.feature);

/** How to install, import and create the client in this language: start/clients plus the client card. */
function clientLine(language) {
  const parts = [];
  const install = bundle.install?.[language];
  if (install) parts.push(`Install and connect, ${LABEL[language]} (${install.url}):\n\n${install.markdown}`);
  const c = bundle.cards?.client?.[language];
  if (c?.calls?.length) parts.push(`The client in ${LABEL[language]}: ${c.calls.map((x) => `\`${x.call}\`${x.does ? ` (${x.does})` : ""}`).join("; ")}.`);
  return parts.join("\n\n");
}

function formatCard(card, language) {
  const lines = [];
  const label = LABEL[language];
  if (!card || card.status === "unknown") return `## In ${label}\nNot charted yet for ${label}: use the HTTP API, guide(page: "reference/http"), or example(language: "${language}") for the closest tested code.`;
  lines.push(`## In ${label}${card.status === "full" ? "" : ` (${card.status === "none" ? "not in this SDK" : "partly in this SDK"})`}`);
  if (clientLine(language)) lines.push(clientLine(language), "");
  const call = (c) => `- \`${c.call}\`${c.does ? `: ${c.does}` : ""}${c.source ? ` (${sourceLink(c.source)})` : ""}`;
  if (card.calls?.length) lines.push(...card.calls.map(call));
  if (card.inTransaction?.length) lines.push("", "Inside a transaction:", ...card.inTransaction.map(call));
  if (card.http?.length) lines.push("", "Over HTTP:", ...card.http.map(call));
  if (card.usage) {
    const how = card.usageTested ? `From \`${card.usageTested}\`, which Queen's test suite runs.` : "Composed from the SDK source, not run: confirm before relying on it.";
    lines.push("", how, `\`\`\`${FENCE[language] ?? ""}\n${card.usage}\n\`\`\``);
  }
  if (card.notes) lines.push("", card.notes);
  return lines.join("\n");
}

function recipeText(f) {
  const parts = [`# Set up: ${f.title}`];
  for (const slug of f.pages) {
    const page = pageBySlug.get(slug);
    if (!page) {
      parts.push(`The page \`${slug}\` is not in this docs build yet. Ask guide(topic: "${f.title}") for what the docs say today.`);
      continue;
    }
    const skip = new Set((f.skip ?? []).map((h) => h.toLowerCase()));
    const own = bundle.sections.filter((s) => s.slug === slug && !skip.has(s.heading.toLowerCase()));
    const body = own.map((s, i) => (i === 0 ? sectionText(s) : `## ${s.heading}\n\n${sectionText(s)}`)).join("\n\n");
    parts.push(`${page.description}\n${page.url}\n\n${cap(body, PAGE_CAP, `Read one section with guide(page: "${slug}", section: "<heading>").`)}`);
  }
  return parts.join("\n\n");
}

function setupTool(args) {
  const want = str(args.feature).toLowerCase().replace(/[\s_]+/g, "-");
  const list = bundle.setup.map((f) => `- \`${f.feature}\`: ${f.title}${f.pages.every((p) => pageBySlug.has(p)) ? "" : " (not in this docs build yet)"}`).join("\n");
  const f = bundle.setup.find((x) => x.feature === want) ?? bundle.setup.find((x) => want && (x.feature.includes(want) || x.words.split(" ").includes(want)));
  if (!f) return want ? fail(`No setup recipe for "${args.feature}". Recipes:\n${list}`) : ok(`Setup recipes:\n${list}`);

  const parts = [recipeText(f)];
  const language = lang(args.language);
  if (language && language !== "kafka" && f.card) parts.push(formatCard(bundle.cards?.[f.card]?.[language], language));

  const traps = trapIndex()
    .search(`${f.title} ${f.words}`, 6)
    .filter(({ doc }) => !language || doc.applies.includes("all") || doc.applies.includes(language))
    .map(({ doc }) => doc);
  if (traps.length) parts.push(`## Traps\n\n${traps.map((t, i) => formatTrap(t, language ?? t.applies[0], i + 1)).join("\n\n")}`);

  const more = (f.related ?? []).filter((slug) => pageBySlug.has(slug));
  const tail = [];
  if (more.length) tail.push(`Related: ${more.map((slug) => `guide(page: "${slug}")`).join(", ")}.`);
  if (f.card) tail.push(`Tested code: example(task: "${f.feature}", language: "<language>").`);
  if (tail.length) parts.push(tail.join(" "));
  return ok(parts.join("\n\n"));
}

export function setupMarkdown(feature) {
  const f = bundle.setup.find((x) => x.feature === feature);
  return f ? recipeText(f) : null;
}

// ---------------------------------------------------------------- tool table

const LANG_PROP = { type: "string", enum: SDK_LANGS, description: "The language of the code: js (JavaScript/TypeScript), python, go, php, rust, cpp, or http (plain HTTP, curl)." };
const READ_ONLY = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };

const TOOLS = [
  {
    name: "guide",
    title: "Queen docs",
    description:
      "Answer a question about Queen MQ 2.0 from its documentation: concepts (partitions, consumer groups, subscription modes, transactions, timers, KV, dedup, ephemeral queues), guides, limits, configuration and the HTTP API. Pass `topic` for the best-matching sections, or `page` (a slug like `concepts/transactions` from an earlier answer) to read a whole page, optionally narrowed to one `section`.",
    inputSchema: {
      type: "object",
      properties: {
        topic: { type: "string", description: "What you want to know, in plain words." },
        page: { type: "string", description: "A page slug such as concepts/transactions, or its full URL." },
        section: { type: "string", description: "With page: a heading on that page." },
      },
      additionalProperties: false,
    },
    run: guide,
  },
  {
    name: "example",
    title: "Tested Queen code",
    description:
      "Tested Queen code for one operation in one language: push, push with dedup, consume, pop, transaction (ack + push + KV + timer), replay, seek, streams, and whole example apps (chat, webhooks, saga, rate limiter, exactly-once, Kafka bridge, deferred work). Every snippet is cut from a file Queen's test suite runs, so its method names are exact. Call it before writing Queen code; do not guess SDK methods. `task: \"list\"` lists what exists for a language.",
    inputSchema: {
      type: "object",
      properties: {
        task: { type: "string", description: 'The operation or app, e.g. "consume with a consumer group", "transaction", "saga", or "list".' },
        language: LANG_PROP,
        full: { type: "boolean", description: "Return the whole program when the snippet comes from an example app." },
      },
      required: ["task", "language"],
      additionalProperties: false,
    },
    run: example,
  },
  {
    name: "check",
    title: "Queen trap checklist",
    description:
      "The checklist of known Queen 2.0 traps for code in one language: mistakes that lose, stall or duplicate messages, and behaviour that looks like a bug but is not. Call it before you finish writing or reviewing Queen code and check every item against the code. `language: \"kafka\"` lists the traps for Kafka clients pointed at Queen. `topic` puts the matching items first.",
    inputSchema: {
      type: "object",
      properties: {
        language: { ...LANG_PROP, enum: [...SDK_LANGS, "kafka"], description: `${LANG_PROP.description} Or kafka, for Kafka client libraries.` },
        topic: { type: "string", description: 'Optional: what the code does, e.g. "transactions" or "consumer groups".' },
      },
      required: ["language"],
      additionalProperties: false,
    },
    run: check,
  },
  {
    name: "explain_error",
    title: "Explain a Queen error",
    description:
      "What a Queen error means and what to do about it: an HTTP status, an error `code` from a response body, a transaction rollback `reason` (such as rejected_ack), a KV or timer error, or a Kafka error name or number that Queen's Kafka facade returned to a Kafka client.",
    inputSchema: {
      type: "object",
      properties: { error: { type: "string", description: 'The status, code, reason or Kafka error, e.g. "rejected_ack", "429", "COORDINATOR_NOT_AVAILABLE".' } },
      required: ["error"],
      additionalProperties: false,
    },
    run: explainError,
  },
  {
    name: "kafka_client",
    title: "Kafka client compatibility",
    description:
      "Whether a Kafka client library works against Queen's Kafka facade, which versions were tested, the configuration it needs, and its caveats. Covers Java kafka-clients, Spring Kafka, franz-go, kafka-go, sarama, librdkafka, confluent-kafka-python, kafka-python, aiokafka, Confluent.Kafka (.NET), kafkajs, @confluentinc/kafka-javascript, node-rdkafka, @platformatic/kafka, Rust rdkafka, Ruby rdkafka, php-rdkafka and brod.",
    inputSchema: {
      type: "object",
      properties: {
        client: { type: "string", description: 'The library, e.g. "sarama", "kafkajs", "confluent-kafka-python".' },
        version: { type: "string", description: "Optional: the version in use." },
      },
      required: ["client"],
      additionalProperties: false,
    },
    run: kafkaClient,
  },
  {
    name: "setup",
    title: "Set up a Queen feature",
    description:
      "Step-by-step setup for one Queen feature: KV state, timers, stream processing, ephemeral queues (request/reply), Kafka clients through the Kafka facade, the Postgres source (tables into queues), the Postgres sink (queues into tables), and the S3 sink (queues into object storage). Returns the docs for it, the exact calls in the given language's SDK, and the traps that apply. Call with no feature to list them.",
    inputSchema: {
      type: "object",
      properties: {
        feature: { type: "string", enum: FEATURES, description: "The feature to set up." },
        language: { ...LANG_PROP, description: `Optional: ${LANG_PROP.description}` },
      },
      additionalProperties: false,
    },
    run: setupTool,
  },
];

export function listTools() {
  return TOOLS.map(({ run, title, ...tool }) => ({ ...tool, title, annotations: { title, ...READ_ONLY } }));
}

export function callTool(name, args) {
  const tool = TOOLS.find((t) => t.name === name);
  if (!tool) throw new RpcError(-32602, `Unknown tool: ${name}`);
  if (args !== undefined && (typeof args !== "object" || args === null || Array.isArray(args))) {
    return fail("Arguments must be an object.");
  }
  for (const key of tool.inputSchema.required ?? []) {
    if (args?.[key] === undefined || args[key] === null || args[key] === "") return fail(`Missing required argument \`${key}\`.`);
  }
  return tool.run(args ?? {});
}

// ---------------------------------------------------------------- prompts

const PROMPTS = [
  {
    name: "design",
    title: "Design a flow on Queen",
    description: "Turn a description of an application flow into a Queen model: queues, partition keys, readers, one transaction per step, timers and KV state, before any code.",
    // One optional argument: Claude Code splits prompt arguments on spaces, so a
    // multi-word flow typed after the command arrives cut; the prompt then reads
    // the flow from the conversation instead.
    arguments: [{ name: "flow", description: "What the application does. More than a few words: describe it in the chat instead.", required: false }],
  },
  {
    name: "review",
    title: "Review Queen code",
    description: "Review the Queen code in the current changes against the checklist of known traps and the tested API.",
    arguments: [{ name: "language", description: "The language of the code.", required: false }],
  },
  {
    name: "from-kafka",
    title: "Bring a Kafka application to Queen",
    description: "Keep a Kafka client and point it at Queen's Kafka facade, or move the code to Queen's own SDK.",
    arguments: [
      { name: "client", description: "The Kafka client library in use, e.g. kafkajs or sarama.", required: false },
      { name: "goal", description: '"keep" to keep the Kafka client, "native" to move to Queen\'s SDK.', required: false },
    ],
  },
];
const DEFAULTS = {
  flow: "(not given here)",
  language: "the project's language",
  client: "its Kafka client",
  goal: "pick the path that fits the project and say why",
};

export function listPrompts() {
  return PROMPTS;
}

export function getPrompt(name, args = {}) {
  const prompt = PROMPTS.find((p) => p.name === name);
  if (!prompt) throw new RpcError(-32602, `Unknown prompt: ${name}`);
  for (const a of prompt.arguments) {
    if (a.required && !str(args[a.name])) throw new RpcError(-32602, `Missing required argument: ${a.name}`);
  }
  const text = (bundle.prompts[name] ?? "").replace(/\{\{(\w+)\}\}/g, (_, key) => str(args[key]) || DEFAULTS[key] || "");
  return { description: prompt.description, messages: [{ role: "user", content: { type: "text", text } }] };
}

// ---------------------------------------------------------------- resources, instructions

const AGENTS_URI = `${ENDPOINT}/AGENTS.md`;

export function agentsMd() {
  return `${bundle.primer}

## More
- Every docs page, as markdown: ${SITE}/llms.txt
- Error codes and what to do about each: ${SITE}/reference/errors/
- Kafka clients pointed at Queen: ${SITE}/guides/kafka/
- Tested code in every language and the full trap checklist: the MCP server at ${ENDPOINT} (${SITE}/start/ai-agents/)
`;
}

export function listResources() {
  return {
    resources: [
      { uri: AGENTS_URI, name: "AGENTS.md", title: "Queen primer for coding agents", description: "The Queen 2.0 model and its traps, for an AGENTS.md file.", mimeType: "text/markdown" },
      ...bundle.pages.map((p) => ({ uri: p.url, name: p.slug || "home", title: p.title, description: p.description, mimeType: "text/markdown" })),
    ],
  };
}

export function readResource(uri) {
  if (uri === AGENTS_URI) return { contents: [{ uri, mimeType: "text/markdown", text: agentsMd() }] };
  const p = bundle.pages.find((x) => x.url === uri || x.url === `${uri}/`);
  if (!p) throw new RpcError(-32002, `Resource not found: ${uri}`);
  return { contents: [{ uri: p.url, mimeType: "text/markdown", text: p.markdown }] };
}

export function instructions() {
  return bundle.usage ? `${bundle.primer}\n\n${bundle.usage}` : bundle.primer;
}

export function serverInfo() {
  const docs = bundle.meta.docs.replace("+dirty", ".dirty");
  return {
    name: "queen",
    title: "Queen MQ",
    version: `${bundle.meta.server}+docs.${docs}`,
    websiteUrl: `${SITE}/start/ai-agents/`,
    icons: [{ src: `${SITE}/favicon.svg`, mimeType: "image/svg+xml" }],
  };
}
