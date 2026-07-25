// Package queen provides a Go client for Queen MQ - a high-performance message queue.
package queen

import (
	"context"
	"time"
)

// Message represents a message from Queen MQ.
type Message struct {
	TransactionID string                 `json:"transactionId"`
	PartitionID   string                 `json:"partitionId"`
	LeaseID       string                 `json:"leaseId,omitempty"`
	Queue         string                 `json:"queue"`
	Partition     string                 `json:"partition"`
	Data          map[string]interface{} `json:"data"`
	CreatedAt     string                 `json:"createdAt"`
	ErrorMessage  string                 `json:"errorMessage,omitempty"`
	RetryCount    int                    `json:"retryCount"`

	// ProducerSub is the authenticated producer identity stamped by the server
	// from the JWT "sub" claim at push time. It is present only when JWT
	// authentication is enabled on the server. Clients CANNOT set this field
	// on push - the server always derives it from the validated JWT, which is
	// how impersonation is prevented. See the server docs on authentication
	// for details.
	ProducerSub string `json:"producerSub,omitempty"`

	// trace is the function to add traces to this message
	trace TraceFunc
}

// Trace adds a trace to this message. Never panics - errors are logged and returned gracefully.
func (m *Message) Trace(ctx context.Context, config TraceConfig) (*TraceResponse, error) {
	if m.trace == nil {
		return &TraceResponse{Success: false, Error: "trace function not set"}, nil
	}
	return m.trace(ctx, config)
}

// SetTrace sets the trace function for this message (internal use).
func (m *Message) SetTrace(fn TraceFunc) {
	m.trace = fn
}

// TraceFunc is the function signature for message tracing.
type TraceFunc func(ctx context.Context, config TraceConfig) (*TraceResponse, error)

// TraceConfig contains configuration for a trace operation.
type TraceConfig struct {
	TraceName string                 `json:"traceName,omitempty"`
	TraceNames []string              `json:"traceNames,omitempty"`
	EventType string                 `json:"eventType,omitempty"`
	Data      map[string]interface{} `json:"data"`
}

// TraceResponse is the response from a trace operation.
type TraceResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// ClientConfig contains configuration for the Queen client.
type ClientConfig struct {
	// URLs is the list of Queen server URLs
	URLs []string
	// URL is a single Queen server URL (convenience, converted to URLs)
	URL string
	// TimeoutMillis is the HTTP request timeout in milliseconds
	TimeoutMillis int
	// RetryAttempts is the number of retry attempts for failed requests
	RetryAttempts int
	// RetryDelayMillis is the initial retry delay in milliseconds (exponential backoff)
	RetryDelayMillis int
	// LoadBalancingStrategy is the strategy for load balancing ("round-robin", "session", "affinity")
	LoadBalancingStrategy string
	// AffinityHashRing is the number of virtual nodes per server for affinity strategy
	AffinityHashRing int
	// EnableFailover enables automatic failover to other servers
	EnableFailover bool
	// HealthRetryAfterMillis is the delay before retrying unhealthy backends
	HealthRetryAfterMillis int
	// BearerToken is the bearer token for authentication
	BearerToken string
	// Headers is a map of custom headers to include in every request
	Headers map[string]string
	// MaxIdleConnsPerHost caps idle keep-alive connections retained per host.
	// Go's net/http default is 2, which cripples high-concurrency, single-host
	// workloads (load generators, hot producer pools) by forcing per-request
	// connection setup/teardown. When <= 0, the client uses a high default (256).
	MaxIdleConnsPerHost int
	// MaxConnsPerHost optionally caps total connections per host (0 = unlimited).
	MaxConnsPerHost int
	// Retry429 controls client-side backoff when the server returns HTTP 429
	// (rate limited), separate from the 5xx/network RetryAttempts above. nil
	// (or a zero-value field within it) falls back to kind-based defaults; see
	// Retry429Config. PLAN_QUEEN_PROXY_CLOUD.md §4/§9 (client 429 backoff, B4).
	Retry429 *Retry429Config
}

// Retry429Config is the backoff policy applied when a request is rate
// limited (HTTP 429). Zero-value fields fall back to kind-based defaults:
//   - MaxAttempts: bounded (10 total attempts) for ordinary requests (push,
//     admin calls, non-waiting pop); unbounded for a long-poll pop
//     (wait=true, requested via WithLongPollRetry()) -- the outer poll is
//     already an indefinite loop, so a 429 there should back off and keep
//     waiting rather than give up. Explicitly setting MaxAttempts applies it
//     uniformly to both kinds.
//   - BaseMs: initial backoff (ms) used when the response has no Retry-After
//     header. Default 500.
//   - CapMs: ceiling (ms) for the exponential backoff. Default 30000.
//
// When the server does send a Retry-After header (seconds), it is honored
// (with +-20% jitter) instead of the computed exponential delay.
type Retry429Config struct {
	// MaxAttempts is the maximum number of attempts (including the first)
	// before giving up on repeated 429s. 0 = use the kind-based default.
	MaxAttempts int
	// BaseMs is the initial backoff in milliseconds absent a Retry-After
	// header. 0 = default (500ms).
	BaseMs int
	// CapMs is the backoff ceiling in milliseconds. 0 = default (30000ms).
	CapMs int
}

// QueueConfig contains configuration for queue creation.
type QueueConfig struct {
	// LeaseTime is the lease duration in seconds
	LeaseTime int `json:"leaseTime,omitempty"`
	// RetryLimit is the maximum number of retries before moving to DLQ
	RetryLimit int `json:"retryLimit,omitempty"`
	// Priority is the queue priority (higher = processed first)
	Priority int `json:"priority,omitempty"`
	// DelayedProcessing is the delay in seconds before messages are available
	DelayedProcessing int `json:"delayedProcessing,omitempty"`
	// WindowBuffer is the window buffer in seconds
	WindowBuffer int `json:"windowBuffer,omitempty"`
	// MaxSize is the maximum number of messages in the queue (0 = unlimited)
	MaxSize int `json:"maxSize,omitempty"`
	// RetentionSeconds is the retention period for pending messages (0 = forever)
	RetentionSeconds int `json:"retentionSeconds,omitempty"`
	// CompletedRetentionSeconds is the retention period for completed messages
	CompletedRetentionSeconds int `json:"completedRetentionSeconds,omitempty"`
	// RetentionEnabled turns on retention/cleanup for the queue. The server only
	// runs the retention service on queues with this flag set, so RetentionSeconds
	// and CompletedRetentionSeconds have no effect unless RetentionEnabled is true.
	RetentionEnabled bool `json:"retentionEnabled,omitempty"`
	// DeadLetterQueue routes messages that exhaust RetryLimit to the dead-letter
	// queue instead of dropping them. The server only dead-letters poison messages
	// when this (or DlqAfterMaxRetries) is set.
	DeadLetterQueue bool `json:"deadLetterQueue,omitempty"`
	// DlqAfterMaxRetries is the retry-exhaustion trigger for dead-lettering; the
	// server treats it as equivalent to DeadLetterQueue for the DLQ hand-off.
	DlqAfterMaxRetries bool `json:"dlqAfterMaxRetries,omitempty"`
	// EncryptionEnabled enables payload encryption
	EncryptionEnabled bool `json:"encryptionEnabled,omitempty"`
}

// BufferConfig contains configuration for client-side message buffering.
type BufferConfig struct {
	// MessageCount is the number of messages to buffer before flushing
	MessageCount int
	// TimeMillis is the time in milliseconds to wait before flushing
	TimeMillis int
}

// ConsumeOptions contains options for consuming messages.
type ConsumeOptions struct {
	Queue                   string
	Partition               string
	Namespace               string
	Task                    string
	Group                   string
	Concurrency             int
	Batch                   int
	Limit                   int
	IdleMillis              int
	AutoAck                 bool
	Wait                    bool
	TimeoutMillis           int
	RenewLease              bool
	RenewLeaseIntervalMillis int
	SubscriptionMode        string
	SubscriptionFrom        string
	Each                    bool
	// MaxPartitions is the v4 multi-partition pop cap: claim up to N
	// partitions in a single call (default 1 = legacy behavior). The
	// Batch field becomes a global cap on total messages returned across
	// all claimed partitions.
	MaxPartitions           int
}

// PopOptions contains options for popping messages.
type PopOptions struct {
	Batch            int
	Wait             bool
	TimeoutMillis    int
	AutoAck          bool
	ConsumerGroup    string
	SubscriptionMode string
	SubscriptionFrom string
	// MaxPartitions is the v4 multi-partition cap (default 1).
	MaxPartitions    int
}

// AckOptions contains options for acknowledging messages.
type AckOptions struct {
	ConsumerGroup string
	Error         string
}

// AckResponse is the response from an acknowledgment operation.
type AckResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// RenewResponse is the response from a lease renewal operation.
type RenewResponse struct {
	LeaseID      string    `json:"leaseId"`
	Success      bool      `json:"success"`
	NewExpiresAt time.Time `json:"newExpiresAt,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// BufferStats contains statistics about message buffers.
type BufferStats struct {
	ActiveBuffers         int     `json:"activeBuffers"`
	TotalBufferedMessages int     `json:"totalBufferedMessages"`
	OldestBufferAge       float64 `json:"oldestBufferAge"`
	FlushesPerformed      int     `json:"flushesPerformed"`
}

// DLQResponse is the response from a DLQ query.
type DLQResponse struct {
	Messages []Message `json:"messages"`
	Total    int       `json:"total"`
}

// TransactionResponse is the response from a transaction commit.
type TransactionResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// PushItem represents an item to be pushed to a queue.
type PushItem struct {
	Queue         string                 `json:"queue"`
	Partition     string                 `json:"partition,omitempty"`
	Payload       interface{}            `json:"payload"`
	TransactionID string                 `json:"transactionId,omitempty"`
	TraceID       string                 `json:"traceId,omitempty"`
}

// PushResponse is the response for a single pushed item.
type PushResponse struct {
	Status        string `json:"status"` // "queued", "duplicate", "failed"
	TransactionID string `json:"transactionId"`
	Error         string `json:"error,omitempty"`
}

// MessageHandler is the function signature for handling a single message.
type MessageHandler func(ctx context.Context, msg *Message) error

// BatchMessageHandler is the function signature for handling a batch of messages.
type BatchMessageHandler func(ctx context.Context, msgs []*Message) error

// Operation represents an operation in a transaction.
type Operation struct {
	Type          string      `json:"type"` // "ack" or "push"
	TransactionID string      `json:"transactionId,omitempty"`
	PartitionID   string      `json:"partitionId,omitempty"`
	Status        string      `json:"status,omitempty"`
	ConsumerGroup string      `json:"consumerGroup,omitempty"`
	Items         []PushItem  `json:"items,omitempty"`
}

// HealthResponse is the response from a health check.
type HealthResponse struct {
	Status string `json:"status"`
}

// QueueInfo contains information about a queue.
type QueueInfo struct {
	Name      string      `json:"name"`
	Namespace string      `json:"namespace,omitempty"`
	Task      string      `json:"task,omitempty"`
	Config    QueueConfig `json:"config,omitempty"`
}

// ConsumerGroupInfo contains information about a consumer group.
type ConsumerGroupInfo struct {
	Name                  string    `json:"name"`
	SubscriptionTimestamp time.Time `json:"subscriptionTimestamp,omitempty"`
}

// popResponse is the internal response structure for pop operations.
type popResponse struct {
	Messages []Message `json:"messages"`
}

// pushRequest is the internal request structure for push operations.
type pushRequest struct {
	Items []pushRequestItem `json:"items"`
}

// pushRequestItem is an item in a push request.
type pushRequestItem struct {
	Queue         string      `json:"queue"`
	Partition     string      `json:"partition,omitempty"`
	Payload       interface{} `json:"payload"`
	TransactionID string      `json:"transactionId,omitempty"`
	TraceID       string      `json:"traceId,omitempty"`
}

// ackRequest is the internal request structure for single ack operations.
type ackRequest struct {
	TransactionID string `json:"transactionId"`
	PartitionID   string `json:"partitionId"`
	LeaseID       string `json:"leaseId,omitempty"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	ConsumerGroup string `json:"consumerGroup,omitempty"`
}

// batchAckRequest is the internal request structure for batch ack operations.
type batchAckRequest struct {
	Acknowledgments []ackRequest `json:"acknowledgments"`
	ConsumerGroup   string       `json:"consumerGroup,omitempty"`
}

// transactionRequest is the internal request structure for transaction operations.
type transactionRequest struct {
	Operations     []Operation `json:"operations"`
	RequiredLeases []string    `json:"requiredLeases"`
}

// configureRequest is the internal request structure for queue configuration.
type configureRequest struct {
	Queue     string                 `json:"queue"`
	Namespace string                 `json:"namespace,omitempty"`
	Task      string                 `json:"task,omitempty"`
	Options   map[string]interface{} `json:"options,omitempty"`
}

// traceRequest is the internal request structure for trace operations.
type traceRequest struct {
	TransactionID string                 `json:"transactionId"`
	PartitionID   string                 `json:"partitionId"`
	ConsumerGroup string                 `json:"consumerGroup"`
	TraceNames    []string               `json:"traceNames,omitempty"`
	EventType     string                 `json:"eventType"`
	Data          map[string]interface{} `json:"data"`
}
