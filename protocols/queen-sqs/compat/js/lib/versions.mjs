// Which version of a dependency this run actually exercised.
//
// It is not a formality: a matrix row that does not name its client version is
// a row nobody can reproduce, and "latest" moves under everyone. The package's
// own `package.json` is the source, and it is read TWICE — through the resolver
// first, then by path — because a package whose `exports` map does not publish
// `./package.json` (sqs-consumer is one) makes `require(name + "/package.json")`
// throw even though the file is sitting right there.

import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const HERE = dirname(fileURLToPath(import.meta.url));

export function packageVersion(name) {
  try {
    return require(`${name}/package.json`).version;
  } catch {
    // fall through to the path
  }
  try {
    const path = join(HERE, "..", "node_modules", ...name.split("/"), "package.json");
    return JSON.parse(readFileSync(path, "utf8")).version;
  } catch {
    return "unknown";
  }
}
