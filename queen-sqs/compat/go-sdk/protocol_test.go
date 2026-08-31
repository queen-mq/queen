package main

import (
	"strings"
	"testing"

	"github.com/aws/smithy-go/logging"
)

// The protocol line is a MEASUREMENT, so the thing that reads it is tested like
// one. These are real `httputil.DumpRequestOut` renderings in shape — CRLF line
// endings, the request line first, the SigV4 credential scope in the
// Authorization header — because every one of those details is something the
// parser depends on.

const jsonRequestDump = "Request\n" +
	"POST / HTTP/1.1\r\n" +
	"Host: 127.0.0.1:19324\r\n" +
	"User-Agent: aws-sdk-go-v2/1.45.1 os/macos lang/go#1.25.7\r\n" +
	"Content-Length: 2\r\n" +
	"Authorization: AWS4-HMAC-SHA256 Credential=QSQSTEST/20260831/queen-1/sqs/aws4_request, " +
	"SignedHeaders=content-length;content-type;host;x-amz-date;x-amz-target, Signature=deadbeef\r\n" +
	"Content-Type: application/x-amz-json-1.0\r\n" +
	"X-Amz-Date: 20260831T041500Z\r\n" +
	"X-Amz-Target: AmazonSQS.ListQueues\r\n" +
	"Accept-Encoding: gzip\r\n" +
	"\r\n"

const queryRequestDump = "Request\n" +
	"POST / HTTP/1.1\r\n" +
	"Host: 127.0.0.1:19324\r\n" +
	"User-Agent: aws-sdk-go-v2/1.45.1 os/macos lang/go#1.25.7\r\n" +
	"Content-Length: 71\r\n" +
	"Authorization: AWS4-HMAC-SHA256 Credential=QSQSTEST/20260831/queen-1/sns/aws4_request, " +
	"SignedHeaders=content-length;content-type;host;x-amz-date, Signature=deadbeef\r\n" +
	"Content-Type: application/x-www-form-urlencoded; charset=utf-8\r\n" +
	"X-Amz-Date: 20260831T041500Z\r\n" +
	"Accept-Encoding: gzip\r\n" +
	"\r\n"

func TestRecorderReadsTheProtocolAndTheService(t *testing.T) {
	recorder := &protocolRecorder{counts: map[string]int{}}
	recorder.Logf(logging.Debug, "Request\n%v", strings.TrimPrefix(jsonRequestDump, "Request\n"))
	recorder.Logf(logging.Debug, "Request\n%v", strings.TrimPrefix(queryRequestDump, "Request\n"))
	recorder.Logf(logging.Debug, "Request\n%v", strings.TrimPrefix(queryRequestDump, "Request\n"))
	// A response dump is not a request and must not be counted as one.
	recorder.Logf(logging.Debug, "Response\n%v", "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\n\r\n")

	got := recorder.snapshot()
	want := map[string]int{
		"sqs: AWS JSON 1.0 (application/x-amz-json-1.0)":                    1,
		"sns: Query/XML (application/x-www-form-urlencoded; charset=utf-8)": 2,
	}
	if len(got) != len(want) {
		t.Fatalf("counted %v, want %v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Errorf("counted %v, want %v", got, want)
		}
	}
}

// An unsigned request would make the service column a guess, so it is reported
// as `unsigned` rather than attributed to whatever ran last — and [tProtocols]
// asserts there are none.
func TestRecorderNamesAnUnsignedRequest(t *testing.T) {
	recorder := &protocolRecorder{counts: map[string]int{}}
	recorder.Logf(logging.Debug, "Request\n%v",
		"POST / HTTP/1.1\r\nHost: h\r\nContent-Type: application/x-amz-json-1.0\r\n"+
			"X-Amz-Target: AmazonSQS.ListQueues\r\n\r\n")
	if got := recorder.snapshot(); got["unsigned: AWS JSON 1.0 (application/x-amz-json-1.0)"] != 1 {
		t.Errorf("counted %v", got)
	}
}

// A shape the parser does not know must be reported as unrecognized rather than
// silently folded into one of the two it does.
func TestRecorderReportsAnUnknownShape(t *testing.T) {
	recorder := &protocolRecorder{counts: map[string]int{}}
	recorder.Logf(logging.Debug, "Request\n%v",
		"POST / HTTP/1.1\r\nHost: h\r\nContent-Type: text/plain\r\n\r\n")
	for key := range recorder.snapshot() {
		if !strings.Contains(key, "unrecognized") {
			t.Errorf("got %q, want an unrecognized line", key)
		}
	}
}

func TestReportLinesAreBusiestFirst(t *testing.T) {
	recorder := &protocolRecorder{counts: map[string]int{"sqs: a": 1, "sns: b": 9}}
	lines := recorder.lines()
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "sns: b") {
		t.Errorf("got %v", lines)
	}
}
