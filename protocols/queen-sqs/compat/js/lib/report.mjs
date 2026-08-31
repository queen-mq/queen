// The suite contract's reporting half, and nothing else.
//
// Copied from queen-kafka's CLIENT_MATRIX.md, verbatim in what matters, and
// implemented once so that every scenario file in this directory spells an
// assertion the same way:
//
//   * ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
//   * anything that is not an assertion is a `#` comment line, as the python
//     smokes do it, so a reader (or a grep) can separate the two;
//   * `RESULT: PASS` / `RESULT: FAIL` as the last line;
//   * a nonzero exit status when anything failed.
//
// Names are hierarchical (`SendMessage.md5_of_body`) and are the SAME names the
// python smokes use wherever the two suites assert the same fact. That is the
// point of a matrix: a row that fails an assertion no other row has is a client
// difference, and a row that fails one everybody fails is the facade.

let passes = 0;
const failures = [];

/** Every assertion that passed, counted. */
export function passCount() {
  return passes;
}

/** The names that failed, in the order they failed. */
export function failedNames() {
  return failures.slice();
}

export function ok(name) {
  passes += 1;
  console.log(`ok ${name}`);
  return true;
}

export function fail(name, detail) {
  failures.push(name);
  console.log(`FAIL ${name}: ${detail}`);
  return false;
}

/** A line that is NOT an assertion. Always `#`-prefixed. */
export function note(text) {
  console.log(`# ${text}`);
}

export function check(name, condition, detail = "") {
  return condition
    ? ok(name)
    : fail(name, detail || "condition was false");
}

/**
 * Structural equality, which is what almost every assertion in this suite
 * wants: the SDK answers objects and arrays, and `===` on two of those compares
 * identity and would pass nothing.
 *
 * `Uint8Array` is handled explicitly because it is what the v3 SDK hands back
 * for a `Binary` message attribute — comparing two of those as objects compares
 * their numeric keys, which happens to work and is not something to rely on.
 */
export function deepEqual(a, b) {
  if (a === b) return true;
  if (a instanceof Uint8Array || b instanceof Uint8Array) {
    if (!(a instanceof Uint8Array) || !(b instanceof Uint8Array)) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return false;
    return true;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((item, i) => deepEqual(item, b[i]));
  }
  if (a && b && typeof a === "object" && typeof b === "object") {
    const ka = Object.keys(a);
    const kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every((k) => Object.hasOwn(b, k) && deepEqual(a[k], b[k]));
  }
  return false;
}

/** How a value is printed in a FAIL detail: short, and never `[object Object]`. */
export function show(value) {
  if (value instanceof Uint8Array) return `bytes[${Array.from(value).join(",")}]`;
  if (typeof value === "string") return JSON.stringify(value);
  try {
    return JSON.stringify(value, (_k, v) => (v instanceof Uint8Array ? Array.from(v) : v));
  } catch {
    return String(value);
  }
}

export function checkEq(name, got, want) {
  return check(name, deepEqual(got, want), `got ${show(got)}, want ${show(want)}`);
}

/**
 * Run `fn`, require it to throw, and hand the error to `assert` — the shape
 * every error assertion in this suite has, with the try/catch written once.
 */
export async function expectThrow(name, fn, assert) {
  try {
    await fn();
  } catch (err) {
    return assert(err);
  }
  return fail(name, "the call succeeded; an error was expected");
}

/** The last two lines of a run, and its exit status. */
export function finish() {
  note(`${passes} passed, ${failures.length} failed`);
  for (const name of failures) note(`  failed: ${name}`);
  console.log(`RESULT: ${failures.length ? "FAIL" : "PASS"}`);
  return failures.length ? 1 : 0;
}
