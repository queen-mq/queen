package queen

// Default configuration values for Queen Client.
// Convention:
// - Properties with "Millis" suffix → milliseconds
// - Properties with "Seconds" suffix → seconds
// - Properties without suffix (time-related) → seconds

// ClientDefaults contains default values for client configuration.
var ClientDefaults = ClientConfig{
	TimeoutMillis:           30000,    // 30 seconds
	RetryAttempts:           3,        // 3 retry attempts
	RetryDelayMillis:        1000,     // 1 second initial delay (exponential backoff)
	LoadBalancingStrategy:   "affinity", // 'round-robin', 'session', or 'affinity'
	AffinityHashRing:        128,      // Number of virtual nodes per server for affinity strategy
	EnableFailover:          true,     // Auto-failover to other servers
	HealthRetryAfterMillis:  5000,     // Retry unhealthy backends after 5 seconds
}

// QueueDefaults contains default values for queue configuration.
var QueueDefaults = QueueConfig{
	LeaseTime:                 300,   // 5 minutes (seconds)
	RetryLimit:                3,     // Max 3 retries before DLQ
	Priority:                  0,     // Default priority
	DelayedProcessing:         0,     // No delay (seconds)
	WindowBuffer:              0,     // No window buffering (seconds)
	MaxSize:                   0,     // No limit on messages per queue
	RetentionSeconds:          0,     // No retention (keep forever)
	CompletedRetentionSeconds: 0,     // No retention for completed messages
	RetentionEnabled:          false, // Retention/cleanup disabled by default
	DeadLetterQueue:           false, // Poison messages dropped (not dead-lettered) by default
	DlqAfterMaxRetries:        false, // DLQ-after-max-retries trigger off by default
	EncryptionEnabled:         false, // No encryption by default
}

// ConsumeDefaults contains default values for consume operations.
var ConsumeDefaults = ConsumeOptions{
	Concurrency:   1,     // Single worker
	Batch:         1,     // One message at a time
	AutoAck:       true,  // Client-side auto-ack (NOT sent to server)
	Wait:          true,  // Long polling enabled
	TimeoutMillis: 30000, // 30 seconds long poll timeout
	Limit:         0,     // No limit (run forever) - 0 means no limit
	IdleMillis:    0,     // No idle timeout - 0 means no timeout
	RenewLease:    false, // No auto-renewal
	Each:          false, // Process as batch by default
	MaxPartitions: 1,     // v4 multi-partition pop cap (1 = legacy single-partition)
}

// PopDefaults contains default values for pop operations.
var PopDefaults = PopOptions{
	Batch:         1,     // One message
	Wait:          false, // No long polling (immediate return)
	TimeoutMillis: 30000, // 30 seconds if wait=true
	AutoAck:       false, // Server-side auto-ack (false = manual ack required)
	MaxPartitions: 1,     // v4 multi-partition pop cap
}

// BufferDefaults contains default values for buffer configuration.
var BufferDefaults = BufferConfig{
	MessageCount: 100,  // Flush after 100 messages
	TimeMillis:   1000, // Or flush after 1 second
}

// DefaultPartition is the default partition name.
const DefaultPartition = "Default"

// QueueModeConsumerGroup is the consumer group name used when no group is specified.
const QueueModeConsumerGroup = "__QUEUE_MODE__"

// AckStatusCompleted is the status for successful message processing.
const AckStatusCompleted = "completed"

// AckStatusFailed is the status for failed message processing.
const AckStatusFailed = "failed"

// SubscriptionModeAll includes all messages (historical + new).
const SubscriptionModeAll = "all"

// SubscriptionModeNew skips historical messages, starts from new.
const SubscriptionModeNew = "new"

// SubscriptionModeNewOnly is an alias for SubscriptionModeNew.
const SubscriptionModeNewOnly = "new-only"

// SubscriptionFromNow starts subscription from current time.
const SubscriptionFromNow = "now"
