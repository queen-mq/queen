// The MD5 vectors, offline: this suite's digests against the facade's own.
//
// WHY BEFORE ANYTHING ELSE. Every live MD5 assertion in `scenarios/sqs.mjs`
// compares an answer from the facade against a number `lib/md5.mjs` computed.
// If the two implementations of AWS's attribute encoding disagreed, that
// comparison would report the facade as wrong for a bug in the suite — the most
// expensive kind of red there is. So the JS implementation is pinned FIRST,
// against the goldens in `queen-sqs/src/md5.rs`, which were themselves derived
// from a second implementation written straight from the specification. A third
// implementation, in a third language, agreeing with both is what makes the
// goldens worth having.
//
// These assertions need no stack. They are part of `all` and they also run
// alone (`node run.mjs vectors`), which is the fastest way to tell a suite bug
// from a facade bug when a live MD5 assertion goes red.

import { checkEq, check } from "../lib/report.mjs";
import { attributesMd5, bodyMd5, systemAttributesMd5 } from "../lib/md5.mjs";

const binary = (bytes) => new Uint8Array(bytes);
const string = (dataType, value) => ({ DataType: dataType, StringValue: value });

export async function run() {
  // AWS's own published vector. If this line fails, nothing else in the file is
  // worth reading until it passes.
  checkEq("Md5.body_is_the_documented_aws_vector", bodyMd5("This is a test message"), "fafb00f5732ab283681e124bf8747ed1");
  // An empty body still HAS a digest — the field is always present on a send
  // result, unlike the attribute one.
  checkEq("Md5.body_of_empty_string", bodyMd5(""), "d41d8cd98f00b204e9800998ecf8427e");
  // Multi-byte UTF-8 digests as its BYTES, which is what every SDK hashes.
  checkEq("Md5.body_is_over_bytes_not_characters", bodyMd5("héllo"), "be50e8478cf24ff3595bc7307fb91b50");

  // The absence rule: no attributes means no field, not the digest of nothing.
  checkEq("Md5.no_attributes_means_no_digest", attributesMd5({}), undefined);
  checkEq("Md5.no_system_attributes_means_no_digest", systemAttributesMd5({}), undefined);

  checkEq(
    "Md5.one_string_attribute_matches_the_golden",
    attributesMd5({ "test-attribute": string("String", "test-value") }),
    "c38e447bda89281029d55c818cc8b9f9",
  );

  // `Number` shares the STRING transport byte and still digests differently
  // from the same value labelled `String`, because the LABEL is hashed too.
  // Both halves are load-bearing: transport byte 2 for Number would fail every
  // client, and normalizing the label away would fail them differently.
  checkEq(
    "Md5.number_hashes_its_label_over_the_string_transport",
    attributesMd5({ count: string("Number", "42") }),
    "2ee5fa915753ff72599b2514463a2897",
  );
  check(
    "Md5.number_and_string_of_one_value_differ",
    attributesMd5({ count: string("Number", "42") }) !== attributesMd5({ count: string("String", "42") }),
    "the two labels produced one digest",
  );

  // A binary attribute digests its DECODED bytes. The tempting bug is to hash
  // the base64 text, which is a different digest and one no client computes.
  checkEq(
    "Md5.binary_hashes_the_decoded_bytes",
    attributesMd5({ blob: { DataType: "Binary", BinaryValue: binary([0, 1, 2, 255]) } }),
    "3b1b4028306ffa157a32d5916f8f714b",
  );
  check(
    "Md5.binary_is_not_the_digest_of_its_base64",
    attributesMd5({ blob: { DataType: "Binary", BinaryValue: binary([0, 1, 2, 255]) } }) !==
      attributesMd5({ blob: string("Binary", "AAEC/w==") }),
    "the bytes and their base64 produced one digest",
  );

  // A custom label is hashed WHOLE, suffix included: `String.foo` is ten bytes
  // of type, not six.
  checkEq(
    "Md5.custom_label_is_hashed_in_full",
    attributesMd5({ label: string("String.foo", "bar") }),
    "58d3b219a649974d7b3c4c00ac2920a3",
  );

  // The golden the file is really pinned by: one of each kind at once.
  checkEq(
    "Md5.the_four_kinds_together_match_the_golden",
    attributesMd5({
      bin: { DataType: "Binary", BinaryValue: binary([0, 1, 2, 255]) },
      custom: string("String.foo", "bar"),
      num: string("Number", "42"),
      str: string("String", "hello"),
    }),
    "59a923d8b436253750446d622c646886",
  );

  // Insertion order cannot reach the digest: the names sort, and JS object key
  // order is insertion order, so this is a real risk here and not a formality.
  const forwards = {
    bin: { DataType: "Binary", BinaryValue: binary([0, 1, 2, 255]) },
    custom: string("String.foo", "bar"),
    num: string("Number", "42"),
    str: string("String", "hello"),
  };
  const backwards = {
    str: string("String", "hello"),
    num: string("Number", "42"),
    custom: string("String.foo", "bar"),
    bin: { DataType: "Binary", BinaryValue: binary([0, 1, 2, 255]) },
  };
  checkEq("Md5.insertion_order_does_not_reach_the_digest", attributesMd5(forwards), attributesMd5(backwards));

  // The system digest is the same algorithm with the label supplied: a system
  // map's values carry no type of their own, and AWS's one system attribute is
  // a String.
  checkEq(
    "Md5.system_digest_equals_the_string_attribute_of_the_same_pair",
    systemAttributesMd5({ AWSTraceHeader: "Root=1-2" }),
    attributesMd5({ AWSTraceHeader: string("String", "Root=1-2") }),
  );
  // ...and it takes the send-side shape too, which is what the SDK sends.
  checkEq(
    "Md5.system_digest_accepts_both_shapes",
    systemAttributesMd5({ AWSTraceHeader: string("String", "Root=1-2") }),
    systemAttributesMd5({ AWSTraceHeader: "Root=1-2" }),
  );

  // The length prefixes separate the fields: `ab`/`c` and `a`/`bc` are two
  // different messages and an implementation that concatenated without them
  // would give one digest for both.
  check(
    "Md5.length_prefixes_separate_the_fields",
    attributesMd5({ ab: string("String", "c") }) !== attributesMd5({ a: string("String", "bc") }),
    "two different attribute maps produced one digest",
  );
}
