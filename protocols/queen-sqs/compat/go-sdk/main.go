// Command gosdk is the queen-sqs Go client-matrix suite: aws-sdk-go-v2 driven
// against a live facade, a live broker and a live Postgres.
//
// WHAT THIS IS FOR. `smoke_m0.py` and `smoke_m4_sns.py` already drive this
// surface with boto3, and the crate's own suite drives it against `FakeQueen`.
// This file exists because the Go SDK is a DIFFERENT CLIENT, and the two places
// it differs from botocore are both load-bearing:
//
//  1. IT VALIDATES A DIGEST CLIENT-SIDE. botocore does not (`smoke_m0.py`'s
//     header says so, correctly). aws-sdk-go-v2's `service/sqs` ships
//     `cust_checksum_validation.go`, which recomputes `MD5OfMessageBody` on
//     SendMessage and SendMessageBatch and `MD5OfBody` on every message
//     ReceiveMessage answers, and fails the CALL when one disagrees. A facade
//     that answered a constant would sail past a suite that leaned on boto3 and
//     would break here. It is worth being exact about the half it does NOT
//     check: `MD5OfMessageAttributes` and `MD5OfMessageSystemAttributes` are
//     validated by the Java, JS and .NET SDKs and by aws-sdk-go v1, and NOT by
//     v2 — so those two digests are recomputed by this suite in [awsMD5.go],
//     with AWS's own binary encoding, exactly as the python suite does.
//     [tSdkChecksumValidation] proves the SDK's own check is armed rather than
//     assuming it, by corrupting one answer on the way back through the stack
//     and requiring the call to fail.
//
//  2. IT PICKS ITS EXCEPTION FROM A DIFFERENT FIELD. botocore maps SQS's JSON
//     errors from `QueryErrorCode`; smithy-go maps them from the `__type`
//     member alone (`deserializers.go`: `SanitizeErrorCode` strips the
//     `com.amazonaws.sqs#` namespace and the switch matches the SHAPE name),
//     and falls back to the `x-amzn-query-error` header only for an error it
//     does not model. So `errors.As(err, **types.QueueDoesNotExist)` here is a
//     test of a byte no python client reads.
//
// THE CONTRACT (queen-kafka's CLIENT_MATRIX.md, the same one both python
// suites copied):
//
//   - the stack comes from the environment, never from a hardcoded address;
//
//   - ONE `ok NAME` or `FAIL NAME: detail` line per assertion;
//
//   - `RESULT: PASS` or `RESULT: FAIL` as the last line;
//
//   - a nonzero exit status when anything failed;
//
//   - and the suite reports WHICH WIRE PROTOCOL its client actually spoke, read
//     from the client's own debug stream ([protocol.go]) and never assumed.
//
// How a person runs it (the README has the runner's one-liner, and why GOWORK
// has to be off):
//
//	$ protocols/queen-sqs/compat/rig.sh up
//	$ source protocols/queen-sqs/compat/.rig/env.sh
//	$ GOWORK=off go run ./protocols/queen-sqs/compat/go-sdk
//
// NAMES ARE UNIQUE PER RUN, for `smoke_m0.py`'s reason: `DeleteQueue` arms a
// 60-second `QueueDeletedRecently` tombstone, so a suite on fixed names could
// not be run twice inside a minute.
//
// A STANDARD QUEUE HANDS OUT AT MOST ONE MESSAGE PER LANE AT A TIME
// (`M0_SMOKE.md`, divergence D2). Every exhaustive read in this file is
// therefore a receive-DELETE-repeat loop ([rig.drainDeleting]), and the one
// place that needs several handles alive at once ([rig.hold]) sends an extra
// message rather than waiting on a lane that will not open.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// The rig's own values, and the only reason they are here rather than required:
// the python suites default the same way (`os.environ.get(..., "…")`), so a
// person with the stack up and no env sourced gets the stack they have rather
// than a usage error.
const (
	defaultEndpoint   = "http://127.0.0.1:19324"
	defaultRegion     = "queen-1"
	defaultAccount    = "000000000000"
	defaultAKID       = "QSQSTEST"
	defaultSecret     = "qsqssecret"
	defaultPartitions = 8
)

// How long the whole scenario list may take. Generous: several assertions wait
// out a lease, a long poll or a redrive, and the point of the bound is that a
// facade which stops answering ends the run rather than hanging a CI cell.
const runBudget = 15 * time.Minute

// The HTTP client's own ceiling. It has to sit ABOVE the longest long poll
// (SQS caps `WaitTimeSeconds` at 20) or the suite would be measuring its own
// transport rather than the facade's.
const httpTimeout = 45 * time.Second

// rig is one run: the two clients, the addresses they were built from, and what
// has to be torn down afterwards.
type rig struct {
	endpoint   string
	region     string
	account    string
	partitions int
	run        string

	sqs   *sqs.Client
	sns   *sns.Client
	proto *protocolRecorder

	// Queue URLs and topic ARNs to remove, newest first.
	queues []string
	topics []string
}

type scenario struct {
	name string
	fn   func(context.Context, *rig)
}

// The order is the order a person reads them in: queues exist, then messages
// move, then the lifecycle verbs, then FIFO, then redrive, then SNS on top of
// all of it. `delete_queue` is last because it arms the tombstone.
var scenarios = []scenario{
	{"queue_crud", tQueueCrud},
	{"queue_attributes", tQueueAttributes},
	{"queue_tags", tQueueTags},
	{"send_receive_delete", tSendReceiveDelete},
	{"sdk_checksum_validation", tSdkChecksumValidation},
	{"batches", tBatches},
	{"long_poll", tLongPoll},
	{"visibility", tVisibility},
	{"fifo_group_ordering", tFifoGroupOrdering},
	{"fifo_sequence_number", tFifoSequenceNumber},
	{"fifo_deduplication", tFifoDeduplication},
	{"dlq_redrive", tDlqRedrive},
	{"sns_topic_and_subscription", tSnsSubscribe},
	{"sns_publish_envelope", tSnsEnvelope},
	{"sns_raw_delivery", tSnsRaw},
	{"sns_filter_policy", tSnsFilterPolicy},
	{"errors", tErrors},
	{"delete_queue", tDeleteQueue},
	// Last on purpose: it reads the tally of everything the run put on the wire.
	{"protocols", tProtocols},
}

func main() { os.Exit(runSuite()) }

func runSuite() int {
	r, err := newRig()
	if err != nil {
		fail("rig.configured", err.Error())
		return report()
	}
	fmt.Printf("# endpoint %s  region %s  account %s  partitions %d  run %s\n",
		r.endpoint, r.region, r.account, r.partitions, r.run)
	fmt.Printf("# client %s\n", sdkVersions())

	ctx, cancel := context.WithTimeout(context.Background(), runBudget)
	defer cancel()

	// One call before anything else, so a stack that is down is one line rather
	// than every assertion failing on its own.
	if _, err := r.sqs.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		fail("rig.reachable", err.Error())
		return report()
	}

	for _, s := range scenarios {
		r.runScenario(ctx, s)
	}
	r.teardown()
	return report()
}

// runScenario runs one scenario and survives it. A panic — a nil pointer off an
// answer that did not have the field this suite expected is the likely one —
// costs that scenario and not the run: the trace goes to stderr and the
// assertions after it still happen, which is the python suites' behaviour too.
func (r *rig) runScenario(ctx context.Context, s scenario) {
	defer func() {
		if p := recover(); p != nil {
			fail(s.name, fmt.Sprintf("unexpected panic: %v", p))
			fmt.Fprintf(os.Stderr, "panic in %s: %v\n%s\n", s.name, p, debug.Stack())
		}
	}()
	s.fn(ctx, r)
}

// teardown removes what the run made, on a context of its own: the run's may
// already be spent, and a queue left behind holds its name for a minute after
// the next `DeleteQueue` anybody makes.
func (r *rig) teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i := len(r.topics) - 1; i >= 0; i-- {
		arn := r.topics[i]
		_, _ = r.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: &arn})
	}
	for i := len(r.queues) - 1; i >= 0; i-- {
		url := r.queues[i]
		_, _ = r.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: &url})
	}
}

// newRig reads the stack out of the environment and builds the two clients.
//
// `aws.Config` is assembled by hand rather than through `config.LoadDefaultConfig`
// on purpose: the credential is the rig's static triple and nothing else, so a
// developer machine with a real AWS profile, an SSO cache or an IMDS route
// cannot change what this suite signs with. `ClientLogMode` is what makes the
// protocol line in the report a MEASUREMENT — see [protocol.go].
func newRig() (*rig, error) {
	partitions, err := strconv.Atoi(env("QUEEN_SQS_PARTITIONS", strconv.Itoa(defaultPartitions)))
	if err != nil {
		return nil, fmt.Errorf("QUEEN_SQS_PARTITIONS is not a number: %w", err)
	}
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("no randomness for a run id: %w", err)
	}

	r := &rig{
		endpoint:   env("QUEEN_SQS_ENDPOINT", defaultEndpoint),
		region:     env("QUEEN_SQS_REGION", defaultRegion),
		account:    env("QUEEN_SQS_ACCOUNT", defaultAccount),
		partitions: partitions,
		run:        hex.EncodeToString(buf),
		proto:      newProtocolRecorder(),
	}

	cfg := aws.Config{
		Region: r.region,
		Credentials: credentials.NewStaticCredentialsProvider(
			env("AWS_ACCESS_KEY_ID", defaultAKID),
			env("AWS_SECRET_ACCESS_KEY", defaultSecret),
			// Deliberately no session token: the facade's third credential
			// field is a Queen bearer, not an AWS session token, and signing
			// with one would fail verification (`rig.sh` says the same where it
			// unsets AWS_SESSION_TOKEN).
			"",
		),
		HTTPClient:    &http.Client{Timeout: httpTimeout},
		ClientLogMode: aws.LogRequest,
		Logger:        r.proto,
	}
	base := aws.String(r.endpoint)
	r.sqs = sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = base })
	r.sns = sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = base })
	return r, nil
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// sdkVersions reads the module versions out of the binary's own build info, so
// the report names the SDK that ran rather than the one go.mod asked for.
func sdkVersions() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "aws-sdk-go-v2 (version unavailable: no build info)"
	}
	wanted := map[string]string{
		"github.com/aws/aws-sdk-go-v2/service/sqs": "",
		"github.com/aws/aws-sdk-go-v2/service/sns": "",
		"github.com/aws/smithy-go":                 "",
	}
	for _, dep := range info.Deps {
		if _, ok := wanted[dep.Path]; ok {
			wanted[dep.Path] = dep.Version
		}
	}
	parts := make([]string, 0, len(wanted))
	for path, version := range wanted {
		if version == "" {
			version = "(absent)"
		}
		parts = append(parts, path+" "+version)
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ") + ", " + info.GoVersion
}
