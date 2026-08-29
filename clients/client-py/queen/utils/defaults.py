"""
Default configuration values for Queen Client
Following the convention:
- Properties with "Millis" suffix → milliseconds
- Properties with "Seconds" suffix → seconds
- Properties without suffix (time-related) → seconds
"""

from typing import Any, Dict, Optional

CLIENT_DEFAULTS: Dict[str, Any] = {
    "timeout_millis": 30000,  # 30 seconds
    "retry_attempts": 3,  # 3 retry attempts
    "retry_delay_millis": 1000,  # 1 second initial delay (exponential backoff)
    "load_balancing_strategy": "affinity",  # 'round-robin', 'session', or 'affinity'
    "affinity_hash_ring": 128,  # Number of virtual nodes per server for affinity strategy
    "enable_failover": True,  # Auto-failover to other servers
    "health_retry_after_millis": 5000,  # Retry unhealthy backends after 5 seconds
    "bearer_token": None,  # Bearer token for proxy authentication
    "headers": {},  # Custom headers to include in every request
}

QUEUE_DEFAULTS: Dict[str, Any] = {
    "lease_time": 300,  # 5 minutes (seconds)
    "retry_limit": 3,  # Max 3 retries before DLQ
    "priority": 0,  # Default priority
    "delayed_processing": 0,  # No delay (seconds)
    "window_buffer": 0,  # No window buffering (seconds)
    "max_size": 0,  # No limit on messages per queue
    "retention_seconds": 0,  # No retention (keep forever)
    "completed_retention_seconds": 0,  # No retention for completed messages
    "encryption_enabled": False,  # No encryption by default
}

# `batch` and `max_partitions` here are the AUTOPILOT-OFF defaults. With
# autopilot on (the default) a knob the caller never set is not defaulted at all
# -- it is omitted from the pop so the broker sizes it (see utils/autopilot.py).
# These values are what comes back with QueueBuilder.autopilot(False), or with
# QUEEN_SDK_POP_AUTOPILOT=off for a whole process.
CONSUME_DEFAULTS: Dict[str, Any] = {
    "concurrency": 1,  # Single worker
    "batch": 1,  # One message at a time (autopilot off only)
    # v4 multi-partition pop cap (autopilot off only). Listed here at last: §4
    # of PLAN_CONFLATION calls out by name that `partitions` never made it into
    # this table, which is how the consume path ended up with a builder option
    # that had no default to read.
    "max_partitions": 1,
    "auto_ack": True,  # Client-side auto-ack (NOT sent to server)
    "wait": True,  # Long polling enabled
    "timeout_millis": 30000,  # 30 seconds long poll timeout
    "limit": None,  # No limit (run forever)
    "idle_millis": None,  # No idle timeout
    "renew_lease": False,  # No auto-renewal
    "renew_lease_interval_millis": None,  # Auto-renewal interval when enabled
    "subscription_mode": None,  # No subscription mode (standard queue mode)
    "subscription_from": None,  # No subscription start point
    # Last-value delivery for this consumer group on this queue
    # (PLAN_CONFLATION §1.1). A DELIVERY POLICY of the group, not a per-call
    # flag: it is persisted on first registration and the stored value wins for
    # every consumer of that group afterwards. Default off -- a group created
    # without it behaves byte-identically to before the feature existed.
    #
    # It is listed here, and not only on the builder, on purpose: `partitions`
    # / maxPartitions never made it into this table (PLAN_CONFLATION §4 calls
    # out the drift by name), which is how the consume path ended up with a
    # builder option that had no default to read.
    "conflation": False,
}

# As in CONSUME_DEFAULTS, `batch` is the autopilot-OFF default.
POP_DEFAULTS: Dict[str, Any] = {
    "batch": 1,  # One message (autopilot off only)
    "wait": False,  # No long polling (immediate return)
    "timeout_millis": 30000,  # 30 seconds if wait=true
    "auto_ack": False,  # Server-side auto-ack (false = manual ack required)
}

BUFFER_DEFAULTS: Dict[str, Any] = {
    "message_count": 100,  # Flush after 100 messages
    "time_millis": 1000,  # Or flush after 1 second
    # Backpressure bound: once this many messages are waiting, the add path
    # BLOCKS until the flusher drains below it (4 x message_count).
    # Measured motivation (2026-08-20): with no bound at all, a producer
    # filling at 1.46M msg/s against a 1.0M msg/s flush pipeline accumulated
    # 20.9M messages (11.7 GB RSS) in 45 seconds and lost every one of them at
    # process exit, with zero client-side errors reported. 0 or absent resolves
    # to this default, NOT to infinity: unbounded was the defect, so it is not
    # expressible. See queen/buffer/message_buffer.py:resolve_buffer_options.
    "max_size": 400,
    # How long the flusher waits before retrying THE SAME batch after a failed
    # POST. A failed batch is re-queued at the front of the buffer and never
    # dropped, so with max_size above, a broker outage degrades into blocked
    # producers and bounded memory instead of silent loss.
    "retry_delay_millis": 250,
}

