/**
 * Default configuration values for Queen Client
 * Following the convention:
 * - Properties with "Millis" suffix → milliseconds
 * - Properties with "Seconds" suffix → seconds  
 * - Properties without suffix (time-related) → seconds
 */

export const CLIENT_DEFAULTS = {
  timeoutMillis: 30000,                // 30 seconds
  retryAttempts: 3,                    // 3 retry attempts
  retryDelayMillis: 1000,              // 1 second initial delay (exponential backoff)
  loadBalancingStrategy: 'affinity',   // 'round-robin', 'session', or 'affinity'
  affinityHashRing: 128,               // Number of virtual nodes per server for affinity strategy
  enableFailover: true,                // Auto-failover to other servers
  healthRetryAfterMillis: 5000,        // Retry unhealthy backends after 5 seconds
  bearerToken: null,                   // Bearer token for proxy authentication
  headers: {},                         // Custom headers to include in every request
  handleSignals: true,                 // Register SIGINT/SIGTERM handlers (disable when used as a library)
  logger: null,                        // Custom logger instance (must implement info/warn/error)
  // Backoff policy for HTTP 429 (rate limited) responses from a queen_proxy
  // deployment. Optional -- omit for the defaults below. Shape:
  //   { maxAttempts?: number, baseMs?: number, capMs?: number }
  // maxAttempts defaults to 10 for push/admin calls; long-poll pop (wait=true)
  // retries unboundedly (paced by backoff) unless maxAttempts is set here, in
  // which case it applies to both. baseMs (500) / capMs (30000) size the
  // exponential backoff used when the server doesn't send Retry-After.
  retry429: undefined
}

export const QUEUE_DEFAULTS = {
  leaseTime: 300,                      // 5 minutes (seconds)
  retryLimit: 3,                       // Max 3 retries before DLQ
  priority: 0,                         // Default priority
  delayedProcessing: 0,                // No delay (seconds)
  windowBuffer: 0,                     // No window buffering (seconds)
  maxSize: 0,                         // No limit on messages per queue
  retentionSeconds: 0,                 // No retention (keep forever)
  completedRetentionSeconds: 0,        // No retention for completed messages
  encryptionEnabled: false             // No encryption by default
}

export const CONSUME_DEFAULTS = {
  concurrency: 1,                      // Single worker
  batch: 1,                            // One message at a time
  autoAck: true,                       // Client-side auto-ack (NOT sent to server)
  wait: true,                          // Long polling enabled
  timeoutMillis: 30000,                // 30 seconds long poll timeout
  limit: null,                         // No limit (run forever)
  idleMillis: null,                    // No idle timeout
  renewLease: false,                   // No auto-renewal
  renewLeaseIntervalMillis: null,      // Auto-renewal interval when enabled
  subscriptionMode: null,              // No subscription mode (standard queue mode)
  subscriptionFrom: null,              // No subscription start point
  maxPartitions: 1                     // v4 multi-partition pop cap (1 = legacy single-partition)
}

export const POP_DEFAULTS = {
  batch: 1,                            // One message
  wait: false,                         // No long polling (immediate return)
  timeoutMillis: 30000,                // 30 seconds if wait=true
  autoAck: false                       // Server-side auto-ack (false = manual ack required)
}

export const BUFFER_DEFAULTS = {
  messageCount: 100,                   // Flush after 100 messages
  timeMillis: 1000                     // Or flush after 1 second
}

