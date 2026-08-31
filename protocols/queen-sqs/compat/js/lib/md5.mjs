// AWS's three MD5 fields, computed here rather than trusted.
//
// WHY THIS FILE EXISTS AT ALL. The installed `@aws-sdk/client-sqs` validates
// ONE of the three — `MD5OfMessageBody`, and the per-message `MD5OfBody` on a
// receive — and validates it only over the BODY (see `lib/sdk-md5.mjs`, which
// reads that fact out of the installed package rather than asserting it from
// memory). `MD5OfMessageAttributes` and `MD5OfMessageSystemAttributes` are
// returned by the facade, carried by the SDK into the response object, and
// checked by nobody. A facade that computed them over a normalized form of the
// attribute map — or returned a constant — would sail past a suite that leaned
// on the SDK, and would then fail on the Java and .NET SDKs, which do check.
//
// So the attribute digest is implemented here, from AWS's specification, and
// the assertions are ours:
//
//   name length    (4 bytes, big-endian)   name bytes
//   type length    (4 bytes, big-endian)   type bytes (the FULL label, custom suffix included)
//   transport byte (1 = String or Number, 2 = Binary)
//   value length   (4 bytes, big-endian)   value bytes (Binary: the DECODED bytes)
//
// fed in ascending order of NAME, over the whole map, once.
//
// Two details that look like details and are not: `Number` uses the STRING
// transport byte, and a custom label (`Number.float`) is hashed whole. Both are
// places where a plausible-looking implementation produces a digest that
// differs from every SDK's — which is why `scenarios/vectors.mjs` pins this
// implementation against the goldens in `protocols/queen-sqs/src/md5.rs` BEFORE the suite
// is allowed to compare anything to a live answer. Two implementations in two
// languages agreeing is the only thing that makes a golden worth having.

import { createHash } from "node:crypto";

export const TRANSPORT_STRING = 1;
export const TRANSPORT_BINARY = 2;

/** The label every system attribute carries: AWS defines exactly one, and it is a String. */
export const SYSTEM_TYPE = "String";

export function bodyMd5(body) {
  return createHash("md5").update(Buffer.from(body ?? "", "utf8")).digest("hex");
}

/**
 * Ascending order of the NAME's UTF-8 BYTES.
 *
 * JavaScript's default sort compares UTF-16 code units, which agrees with byte
 * order for every name anyone actually sends and disagrees above U+FFFF. The
 * digest is specified over bytes, so the comparator is over bytes.
 */
function byNameBytes(a, b) {
  return Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
}

function feed(hash, name, dataType, transport, value) {
  const nameBytes = Buffer.from(name, "utf8");
  const typeBytes = Buffer.from(dataType, "utf8");
  const length = Buffer.alloc(4);

  length.writeUInt32BE(nameBytes.length, 0);
  hash.update(length).update(nameBytes);

  length.writeUInt32BE(typeBytes.length, 0);
  hash.update(length).update(typeBytes);

  hash.update(Buffer.from([transport]));

  length.writeUInt32BE(value.length, 0);
  hash.update(length).update(value);
}

/**
 * The digest over a `MessageAttributes` map in the SDK's own shape
 * (`{name: {DataType, StringValue?, BinaryValue?}}`).
 *
 * Answers `undefined` for an empty map, because AWS OMITS the field there and
 * an omitted field is not the digest of nothing: an SDK that received
 * `d41d8c…` for a message with no attributes would have nothing to compare it
 * against.
 */
export function attributesMd5(attributes) {
  const names = Object.keys(attributes ?? {});
  if (names.length === 0) return undefined;
  const hash = createHash("md5");
  for (const name of names.sort(byNameBytes)) {
    const attribute = attributes[name];
    const dataType = attribute.DataType;
    const binary = dataType === "Binary" || dataType.startsWith("Binary.");
    if (binary) {
      const value = attribute.BinaryValue;
      feed(hash, name, dataType, TRANSPORT_BINARY, Buffer.from(value ?? new Uint8Array()));
    } else {
      feed(hash, name, dataType, TRANSPORT_STRING, Buffer.from(attribute.StringValue ?? "", "utf8"));
    }
  }
  return hash.digest("hex");
}

/**
 * The same algorithm over `MessageSystemAttributes`, whose values carry no
 * label of their own — the map is `name → value` and the digest still needs a
 * type, which is `String` for the single system attribute AWS defines.
 *
 * Takes either the SDK's send-side shape (`{name: {DataType, StringValue}}`)
 * or the receive side's flat `{name: value}`, because the two ends of the round
 * trip spell the same fact differently and both need digesting.
 */
export function systemAttributesMd5(system) {
  const names = Object.keys(system ?? {});
  if (names.length === 0) return undefined;
  const hash = createHash("md5");
  for (const name of names.sort(byNameBytes)) {
    const entry = system[name];
    const value = typeof entry === "string" ? entry : (entry.StringValue ?? "");
    feed(hash, name, SYSTEM_TYPE, TRANSPORT_STRING, Buffer.from(value, "utf8"));
  }
  return hash.digest("hex");
}
