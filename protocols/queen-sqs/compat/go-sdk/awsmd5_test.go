package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The digests this suite computes for itself have to agree with the facade's,
// or every MD5 assertion in the run is two implementations of one bug.
//
// The vectors are `queen-sqs/src/md5.rs`'s own goldens, copied deliberately: two
// independent implementations checked against the SAME published values is the
// point, and a value invented here would only prove this file agrees with
// itself. They run offline — `go test ./...` needs no rig — which makes them the
// one part of this suite a person can check before standing anything up.

func TestBodyDigest(t *testing.T) {
	// AWS's own published vector.
	if got, want := md5OfBody("This is a test message"), "fafb00f5732ab283681e124bf8747ed1"; got != want {
		t.Errorf("md5OfBody(documented vector) = %s, want %s", got, want)
	}
	// An empty body still has a digest — the field is always present on a send
	// result, unlike the attribute one.
	if got, want := md5OfBody(""), "d41d8cd98f00b204e9800998ecf8427e"; got != want {
		t.Errorf("md5OfBody(\"\") = %s, want %s", got, want)
	}
	// Multi-byte UTF-8 digests as its BYTES, which is what the SDK hashes.
	if got, want := md5OfBody("héllo"), "be50e8478cf24ff3595bc7307fb91b50"; got != want {
		t.Errorf("md5OfBody(multibyte) = %s, want %s", got, want)
	}
}

func TestAttributeDigests(t *testing.T) {
	cases := []struct {
		name       string
		attributes map[string]types.MessageAttributeValue
		want       string
	}{
		{
			// The absence rule: no attributes means NO FIELD, not the digest of
			// an empty input. An SDK that received d41d8c… for a message with no
			// attributes would compute nothing to compare it against.
			name: "none", attributes: nil, want: "",
		},
		{
			name:       "one string",
			attributes: map[string]types.MessageAttributeValue{"test-attribute": stringAttr("test-value")},
			want:       "c38e447bda89281029d55c818cc8b9f9",
		},
		{
			// Number shares the STRING transport byte and still digests
			// differently from the same value labelled String, because the label
			// itself is hashed.
			name:       "number",
			attributes: map[string]types.MessageAttributeValue{"count": typedAttr("Number", "42")},
			want:       "2ee5fa915753ff72599b2514463a2897",
		},
		{
			// A binary attribute digests its DECODED bytes. The tempting bug is
			// to hash the base64 text, which is a digest no client computes.
			name:       "binary",
			attributes: map[string]types.MessageAttributeValue{"blob": binaryAttr([]byte{0, 1, 2, 255})},
			want:       "3b1b4028306ffa157a32d5916f8f714b",
		},
		{
			// A custom label is hashed WHOLE, suffix included.
			name:       "custom label",
			attributes: map[string]types.MessageAttributeValue{"label": typedAttr("String.foo", "bar")},
			want:       "58d3b219a649974d7b3c4c00ac2920a3",
		},
		{
			name: "one of each kind",
			attributes: map[string]types.MessageAttributeValue{
				"bin":    binaryAttr([]byte{0, 1, 2, 255}),
				"custom": typedAttr("String.foo", "bar"),
				"num":    typedAttr("Number", "42"),
				"str":    stringAttr("hello"),
			},
			want: "59a923d8b436253750446d622c646886",
		},
	}
	for _, test := range cases {
		if got := md5OfMessageAttributes(test.attributes); got != test.want {
			t.Errorf("md5OfMessageAttributes(%s) = %q, want %q", test.name, got, test.want)
		}
	}
}

// Insertion order cannot reach the digest: Go's map iteration is deliberately
// randomised, so this is the one property that has to hold for the suite to be
// repeatable at all.
func TestAttributeDigestIsOrderIndependent(t *testing.T) {
	attributes := map[string]types.MessageAttributeValue{
		"bin":    binaryAttr([]byte{0, 1, 2, 255}),
		"custom": typedAttr("String.foo", "bar"),
		"num":    typedAttr("Number", "42"),
		"str":    stringAttr("hello"),
	}
	first := md5OfMessageAttributes(attributes)
	for i := 0; i < 32; i++ {
		if got := md5OfMessageAttributes(attributes); got != first {
			t.Fatalf("digest changed between iterations: %s then %s", first, got)
		}
	}
}

// The length prefixes are what keep two different attribute sets apart: without
// them `ab`/`c` and `a`/`bc` would feed the digest the same bytes.
func TestLengthPrefixesSeparateTheFields(t *testing.T) {
	one := md5OfMessageAttributes(map[string]types.MessageAttributeValue{"ab": stringAttr("c")})
	other := md5OfMessageAttributes(map[string]types.MessageAttributeValue{"a": stringAttr("bc")})
	if one == other {
		t.Errorf("ab/c and a/bc digested alike: %s", one)
	}
}

// The system map has no labels of its own, so the digest supplies `String` —
// the same encoding, one field short.
func TestSystemAttributeDigest(t *testing.T) {
	trace := "Root=1-5759e988-bd862e3fe1be46a994272793"
	system := map[string]types.MessageSystemAttributeValue{
		"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String(trace)},
	}
	if got, want := md5OfMessageSystemAttributes(system), "62a56dd927315f2b2e12832b84617ea5"; got != want {
		t.Errorf("md5OfMessageSystemAttributes = %s, want %s", got, want)
	}
	if got := md5OfMessageSystemAttributes(nil); got != "" {
		t.Errorf("an empty system map must have no digest, got %s", got)
	}
	// ...and it is the attribute encoding, not a second one: the same pair as a
	// labelled String attribute digests identically.
	same := md5OfMessageAttributes(map[string]types.MessageAttributeValue{"AWSTraceHeader": stringAttr(trace)})
	if same != md5OfMessageSystemAttributes(system) {
		t.Errorf("the two encodings disagree: %s vs %s", same, md5OfMessageSystemAttributes(system))
	}
}
