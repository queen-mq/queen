package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// The report's protocol line is only as good as the classifier behind it, and
// the classifier reads a format nobody in this repository controls. So it is
// checked against the REAL thing: two clients built exactly as [newRig] builds
// them, pointed at a throwaway HTTP server, and the tally read back.
//
// `protocol_test.go` checks the parser against hand-written dumps; this checks
// that aws-sdk-go-v2 actually produces what those dumps claim — that
// `service/sqs` still speaks AWS JSON 1.0 and `service/sns` still speaks Query,
// which is an SDK fact and not a facade one, and one that a future SDK major
// could change under this suite without a single line of it failing to compile.
//
// No rig, no facade: the server answers 500 to everything and the retryer is
// held to one attempt, because the only thing being read is what went OUT.
func TestTheClientsSpeakTheExpectedProtocols(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	t.Setenv("QUEEN_SQS_ENDPOINT", server.URL)
	t.Setenv("QUEEN_SQS_REGION", "queen-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "QSQSTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "qsqssecret")

	r, err := newRig()
	if err != nil {
		t.Fatalf("newRig: %v", err)
	}

	// The recorder is a process-wide singleton, so the assertion is on the
	// DELTA rather than on the whole tally.
	before := recorded.snapshot()
	ctx := context.Background()
	_, _ = r.sqs.ListQueues(ctx, &sqs.ListQueuesInput{},
		func(o *sqs.Options) { o.RetryMaxAttempts = 1 })
	_, _ = r.sns.ListTopics(ctx, &sns.ListTopicsInput{},
		func(o *sns.Options) { o.RetryMaxAttempts = 1 })
	after := recorded.snapshot()

	var sqsLine, snsLine string
	for key, count := range after {
		if count == before[key] {
			continue
		}
		t.Logf("recorded %q (%d new request(s))", key, count-before[key])
		switch {
		case strings.HasPrefix(key, "sqs: "):
			sqsLine = key
		case strings.HasPrefix(key, "sns: "):
			snsLine = key
		}
	}

	if !strings.HasPrefix(sqsLine, "sqs: AWS JSON 1.0") {
		t.Errorf("the SQS client spoke %q, want AWS JSON 1.0", sqsLine)
	}
	if !strings.HasPrefix(snsLine, "sns: Query/XML") {
		t.Errorf("the SNS client spoke %q, want Query/XML", snsLine)
	}
}
