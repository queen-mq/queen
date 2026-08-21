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
  // Host to advertise on every request, independent of the address dialed.
  // A queen_proxy deployment picks the tenant cluster from the Host header's
  // first DNS label, so this selects a cluster when `url` points at a shared
  // address (an IP, a cell endpoint, a local rig) instead of the cluster's own
  // subdomain. Bare authority only: 'acme.eu1.queenmq.cloud' or 'acme:6711'.
  // The connection still goes to `url`; only the request authority (and TLS
  // SNI) is rewritten. In production each cluster has its own hostname, so the
  // base URL usually carries the right Host and this stays null.
  // NOTE: `headers: { Host }` cannot work (fetch forbids it) — it is mapped
  // onto this option with a warning.
  hostHeader: null,
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
  maxPartitions: 1,                    // v4 multi-partition pop cap (1 = legacy single-partition)
  // Last-value delivery for this consumer group (PLAN_CONFLATION §1.1): a pop
  // of a partition delivers only the NEWEST visible message and commits past
  // the ones it skipped. Off by default, and only ever SENT when true, so a
  // client that never touches it is byte-identical on the wire. Requires
  // broker >= 1.1.0 — an older one ignores the parameter, which the SDK
  // detects from the missing response echo and raises on (§4).
  conflation: false
}

export const POP_DEFAULTS = {
  batch: 1,                            // One message
  wait: false,                         // No long polling (immediate return)
  timeoutMillis: 30000,                // 30 seconds if wait=true
  autoAck: false                       // Server-side auto-ack (false = manual ack required)
}

export const BUFFER_DEFAULTS = {
  messageCount: 100,                   // Flush after 100 messages
  timeMillis: 1000,                    // Or flush after 1 second
  // Backpressure bound: once this many messages are waiting, a buffered push
  // WAITS for the flusher to drain below it instead of growing the heap. 0 (or
  // absent) means 4 x messageCount -- "unbounded" is deliberately not
  // expressible, because unbounded was the defect. Measured motivation
  // (2026-08-20): without a bound, a producer filling at 1.46M msg/s against a
  // 1.0M msg/s flush pipeline accumulated 20.9M messages (11.7 GB) in 45
  // seconds and lost every one of them at process exit, with zero client-side
  // errors. The bound is approximate: a batch that fails to send is put back,
  // so occupancy can briefly overshoot by up to one messageCount.
  maxSize: 400,
  // How long the flusher waits before retrying a batch whose POST failed. The
  // batch is re-queued at the front of the buffer and retried until it lands
  // (or the buffer is stopped) -- never dropped. 0 means 250.
  retryDelayMillis: 250
}

