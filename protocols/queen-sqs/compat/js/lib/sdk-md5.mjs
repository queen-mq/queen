// What the INSTALLED `@aws-sdk/client-sqs` major does about MD5, read out of
// the package on disk.
//
// The received wisdom is that the JS v3 SDK "dropped MD5 validation". That is
// half true and the half matters: the checksum middlewares moved, were made
// configurable, and at some majors were unwired from the client entirely — so
// the only honest answer for a matrix row is the one this file computes, at the
// version that is actually installed, from that version's own source. Nothing
// here is asserted from memory, and nothing here fails a run: a future major
// that validates more (or less) changes the NOTE the suite prints, and the
// suite's own digests (`lib/md5.mjs`) are computed either way.
//
// What is looked for, and why each is the right thing to look for:
//
//   * `@aws-sdk/middleware-sdk-sqs` defines the three checksum middlewares. If
//     the package is absent, or the functions are gone, nothing validates.
//   * `@aws-sdk/client-sqs` must both IMPORT the plugins and APPLY them to the
//     command stacks. A package that ships the middleware and never wires it in
//     validates nothing, which is exactly the state some majors shipped.
//   * the client's resolved config must carry an `md5` hash constructor: every
//     one of the middlewares short-circuits on `options.md5 === false`.
//   * whether any of it mentions the ATTRIBUTE digests. In every major to date
//     the answer is no — `MD5OfMessageAttributes` and
//     `MD5OfMessageSystemAttributes` are returned to the caller unchecked.

import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const HERE = dirname(fileURLToPath(import.meta.url));

/** The package's version and the source of its main entry point, or null. */
function packageSource(name) {
  const candidates = [];
  try {
    candidates.push(require.resolve(`${name}/package.json`));
  } catch {
    // An `exports` map that does not publish package.json: fall back to the
    // path, which is where npm put it regardless of what the map says.
  }
  candidates.push(join(HERE, "..", "node_modules", ...name.split("/"), "package.json"));
  for (const path of candidates) {
    try {
      const pkg = JSON.parse(readFileSync(path, "utf8"));
      const main = (pkg.main ?? "index.js").replace(/^\.\//, "");
      const text = readFileSync(join(dirname(path), main), "utf8");
      return { name, version: pkg.version, path: join(dirname(path), main), text };
    } catch {
      // try the next candidate
    }
  }
  return null;
}

/**
 * The finding, as data. `validates` is what this major checks CLIENT-SIDE;
 * `evidence` is the sentence for each answer, naming the file it was read from
 * so a reader can check the claim rather than believe it.
 */
export function inspectSdkMd5() {
  const middleware = packageSource("@aws-sdk/middleware-sdk-sqs");
  const client = packageSource("@aws-sdk/client-sqs");
  const evidence = [];
  const validates = {
    bodyOnSend: false,
    bodyOnSendBatch: false,
    bodyOnReceive: false,
    attributes: false,
    systemAttributes: false,
  };

  if (!client) {
    return {
      inspected: false,
      validates,
      evidence: ["@aws-sdk/client-sqs could not be read from node_modules"],
      versions: {},
    };
  }

  const versions = {
    "@aws-sdk/client-sqs": client.version,
    "@aws-sdk/middleware-sdk-sqs": middleware?.version ?? "absent",
  };

  if (!middleware) {
    evidence.push("@aws-sdk/middleware-sdk-sqs is not installed: no checksum middleware exists");
    return { inspected: true, validates, evidence, versions };
  }

  const defines = (fn) => middleware.text.includes(fn);
  const applies = (plugin) =>
    client.text.includes(plugin) && new RegExp(`${plugin}\\(config\\)|${plugin}\\(this\\.config\\)`).test(client.text);
  // Every middleware bails out on `options.md5 === false`, so a client that
  // never resolves an `md5` into its config validates nothing even with the
  // plugins applied.
  const hasHash = /md5:\s*config\?\.md5\s*\?\?/.test(client.text) || /md5:\s*[A-Za-z_$][\w$]*/.test(client.text);

  validates.bodyOnSend = defines("sendMessageMiddleware") && applies("getSendMessagePlugin") && hasHash;
  validates.bodyOnSendBatch =
    defines("sendMessageBatchMiddleware") && applies("getSendMessageBatchPlugin") && hasHash;
  validates.bodyOnReceive = defines("receiveMessageMiddleware") && applies("getReceiveMessagePlugin") && hasHash;

  // The attribute digests: named nowhere in the middleware means checked
  // nowhere. `MD5OfBody` and `MD5OfMessageBody` are the body ones and are not
  // evidence for these.
  const mentionsAttributeDigest = /MD5OfMessageAttributes|MD5OfMessageSystemAttributes/.test(middleware.text);
  validates.attributes = mentionsAttributeDigest;
  validates.systemAttributes = /MD5OfMessageSystemAttributes/.test(middleware.text);

  evidence.push(
    `${middleware.name}@${middleware.version}: ` +
      `${defines("sendMessageMiddleware") ? "defines" : "does NOT define"} sendMessageMiddleware, ` +
      `${defines("sendMessageBatchMiddleware") ? "defines" : "does NOT define"} sendMessageBatchMiddleware, ` +
      `${defines("receiveMessageMiddleware") ? "defines" : "does NOT define"} receiveMessageMiddleware`,
  );
  evidence.push(
    `${client.name}@${client.version}: ` +
      `${applies("getSendMessagePlugin") ? "applies" : "does NOT apply"} getSendMessagePlugin, ` +
      `${applies("getSendMessageBatchPlugin") ? "applies" : "does NOT apply"} getSendMessageBatchPlugin, ` +
      `${applies("getReceiveMessagePlugin") ? "applies" : "does NOT apply"} getReceiveMessagePlugin, ` +
      `${hasHash ? "resolves" : "does NOT resolve"} an md5 hash into its config`,
  );
  evidence.push(
    mentionsAttributeDigest
      ? "the checksum middleware names an ATTRIBUTE digest — this major checks more than the body"
      : "no attribute digest is named anywhere in the checksum middleware: " +
        "MD5OfMessageAttributes and MD5OfMessageSystemAttributes are returned unchecked",
  );

  return { inspected: true, validates, evidence, versions };
}

/** One line for the run's header, in the words a matrix report wants. */
export function sdkMd5Summary(finding) {
  if (!finding.inspected) return "sdk md5: could not be determined";
  const body = [
    finding.validates.bodyOnSend && "SendMessage",
    finding.validates.bodyOnSendBatch && "SendMessageBatch",
    finding.validates.bodyOnReceive && "ReceiveMessage",
  ].filter(Boolean);
  const checked = body.length ? `body on ${body.join(", ")}` : "nothing";
  const attributes = finding.validates.attributes ? "AND the attribute digests" : "never the attribute digests";
  return `sdk md5: this major validates ${checked}, ${attributes} — so this suite computes all three itself`;
}
