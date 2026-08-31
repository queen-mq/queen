package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aws/smithy-go/logging"
)

// WHICH PROTOCOL THIS CLIENT ACTUALLY SPOKE, read off the client's own debug
// stream rather than inferred from the SDK's version.
//
// The suite contract asks for exactly that ("each rig reports which protocol its
// client actually spoke, read from the client's own debug output, never
// assumed"), and this is the Go SDK's literal debug output: the client is built
// with `ClientLogMode: aws.LogRequest`, so smithy-go's `RequestResponseLogger`
// dumps every outbound request through the configured `logging.Logger`
// (`smithy-go/transport/http/middleware_http_logging.go`: `logger.Logf(
// logging.Debug, "Request\n%v", …)`). This type IS that logger. It prints
// nothing — the dump carries a SigV4 Authorization header and a body-shaped
// request line, neither of which belongs in a suite's stdout — and records only
// what the report needs.
//
// The logger sits in the DESERIALIZE step, which runs after Finalize, so the
// request it dumps is the SIGNED one. That is what makes the service name
// readable: `Credential=<akid>/<date>/<region>/<service>/aws4_request` is the
// client's own statement of which service it thinks it is talking to, and it is
// the only way to tell an SNS request from an SQS one when both go to one port
// and the Query protocol puts the action in a body this dump does not include.
//
// The two shapes are distinguishable by one header pair: AWS JSON 1.0 sends
// `X-Amz-Target: AmazonSQS.<Action>` with an `application/x-amz-json-1.0` body,
// and the Query protocol sends neither and a form-encoded one.
type protocolRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

// recorded is the one recorder, shared by both clients so that the report is a
// single tally across services.
var recorded = &protocolRecorder{counts: map[string]int{}}

func newProtocolRecorder() *protocolRecorder { return recorded }

// The credential scope of a SigV4 header signature. The service is the fourth
// slash-separated field, per the signing spec.
var credentialScope = regexp.MustCompile(`Credential=[^/\s]+/[^/]+/[^/]+/([^/,\s]+)/aws4_request`)

func (r *protocolRecorder) Logf(_ logging.Classification, format string, v ...any) {
	message := fmt.Sprintf(format, v...)
	if !strings.HasPrefix(message, "Request") {
		return
	}
	headers := dumpedHeaders(message)
	contentType := headers["content-type"]
	target := headers["x-amz-target"]

	var spoken string
	switch {
	case target != "" && strings.Contains(contentType, "json"):
		spoken = fmt.Sprintf("AWS JSON 1.0 (%s)", contentType)
	case strings.Contains(contentType, "x-www-form-urlencoded"):
		spoken = fmt.Sprintf("Query/XML (%s)", contentType)
	default:
		spoken = fmt.Sprintf("unrecognized (Content-Type: %q, X-Amz-Target: %q)", contentType, target)
	}

	service := "unsigned"
	if match := credentialScope.FindStringSubmatch(headers["authorization"]); match != nil {
		service = match[1]
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[service+": "+spoken]++
}

// dumpedHeaders reads the headers out of an httputil.DumpRequestOut rendering:
// a request line, then headers, then a blank line. Names are lower-cased
// because a dump preserves whatever canonicalisation the transport applied.
func dumpedHeaders(dump string) map[string]string {
	headers := map[string]string{}
	lines := strings.Split(dump, "\n")
	// [0] is "Request", [1] is the request line; headers start after that.
	for _, line := range lines[min(2, len(lines)):] {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		headers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
	return headers
}

// snapshot answers a copy of the tally, keyed "<service>: <protocol>".
func (r *protocolRecorder) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.counts))
	for key, count := range r.counts {
		out[key] = count
	}
	return out
}

// lines answers one report line per (service, protocol) pair, busiest first.
func (r *protocolRecorder) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.counts))
	for key := range r.counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if r.counts[keys[i]] != r.counts[keys[j]] {
			return r.counts[keys[i]] > r.counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, fmt.Sprintf("%s — %d request(s)", key, r.counts[key]))
	}
	return out
}
