package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// AWS's own digests, recomputed here.
//
// aws-sdk-go-v2 validates `MD5OfMessageBody` (send) and `MD5OfBody` (receive)
// and NOTHING ELSE — see `service/sqs/cust_checksum_validation.go`. The
// attribute digests are validated by aws-sdk-go v1 and by the Java, JS and .NET
// SDKs, and a facade that got them wrong would still pass every Go call. So
// they are computed here, from AWS's documented algorithm, and asserted:
//
//	names in ascending BYTE order, and for each one 4-byte big-endian length +
//	UTF-8 name, then the same for the data type string, then a single transport
//	byte (1 = the value is a String, 2 = the value is Binary), then 4-byte
//	big-endian length + the value's bytes. MD5 of the whole thing, hex.
//
// `String.custom` and `Number.custom` carry the FULL type string into the
// digest and the transport byte of their base type, which is why the encoding
// takes the type as text rather than as a flag.

func md5OfBody(body string) string {
	sum := md5.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

// digestAttribute is one attribute reduced to what the digest reads.
type digestAttribute struct {
	name     string
	dataType string
	// Exactly one of these is set; binary is what decides the transport byte.
	stringValue string
	binaryValue []byte
	isBinary    bool
}

// md5OfMessageAttributes answers the digest of a SendMessage's
// `MessageAttributes`, or "" for none — AWS omits the field entirely rather
// than digesting an empty map.
func md5OfMessageAttributes(attributes map[string]types.MessageAttributeValue) string {
	if len(attributes) == 0 {
		return ""
	}
	list := make([]digestAttribute, 0, len(attributes))
	for name, value := range attributes {
		entry := digestAttribute{name: name, dataType: deref(value.DataType)}
		if value.BinaryValue != nil {
			entry.isBinary, entry.binaryValue = true, value.BinaryValue
		} else {
			entry.stringValue = deref(value.StringValue)
		}
		list = append(list, entry)
	}
	return digestOf(list)
}

// md5OfMessageSystemAttributes is the same algorithm over the system map —
// `AWSTraceHeader` is the only member this facade accepts on a send.
func md5OfMessageSystemAttributes(attributes map[string]types.MessageSystemAttributeValue) string {
	if len(attributes) == 0 {
		return ""
	}
	list := make([]digestAttribute, 0, len(attributes))
	for name, value := range attributes {
		entry := digestAttribute{name: name, dataType: deref(value.DataType)}
		if value.BinaryValue != nil {
			entry.isBinary, entry.binaryValue = true, value.BinaryValue
		} else {
			entry.stringValue = deref(value.StringValue)
		}
		list = append(list, entry)
	}
	return digestOf(list)
}

func digestOf(attributes []digestAttribute) string {
	sort.Slice(attributes, func(i, j int) bool {
		return bytes.Compare([]byte(attributes[i].name), []byte(attributes[j].name)) < 0
	})
	var buf bytes.Buffer
	field := func(raw []byte) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(raw)))
		buf.Write(length[:])
		buf.Write(raw)
	}
	for _, attribute := range attributes {
		field([]byte(attribute.name))
		field([]byte(attribute.dataType))
		if attribute.isBinary {
			buf.WriteByte(2)
			field(attribute.binaryValue)
		} else {
			buf.WriteByte(1)
			field([]byte(attribute.stringValue))
		}
	}
	sum := md5.Sum(buf.Bytes())
	return hex.EncodeToString(sum[:])
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
