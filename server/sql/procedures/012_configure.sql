-- ============================================================================
-- Configure Queue Stored Procedure
-- ============================================================================
-- Creates or updates queue configuration with all options
-- ============================================================================

-- Track B (§5): p_tenant scopes the config row's identity; the broker passes the
-- resolved tenant (default when the feature is off ⇒ byte-identical).
CREATE OR REPLACE FUNCTION queen.configure_queue_v1(
    p_queue_name TEXT,
    p_options JSONB DEFAULT '{}'::jsonb,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_namespace TEXT;
    v_task TEXT;
    v_priority INTEGER;
    v_lease_time INTEGER;
    v_retry_limit INTEGER;
    v_retry_delay INTEGER;
    v_max_size INTEGER;
    v_ttl INTEGER;
    v_dead_letter_queue BOOLEAN;
    v_dlq_after_max_retries BOOLEAN;
    v_delayed_processing INTEGER;
    v_window_buffer INTEGER;
    v_retention_seconds INTEGER;
    v_completed_retention_seconds INTEGER;
    v_retention_enabled BOOLEAN;
    v_encryption_enabled BOOLEAN;
    v_max_wait_time_seconds INTEGER;
    v_min_pop_wait_time INTEGER;
    v_dedup_window_seconds INTEGER;
    v_retention_sink_hold TEXT;
    v_retention_sink_hold_max_seconds INTEGER;
    v_queue_id UUID;
BEGIN
    -- Parse options with defaults
    v_namespace := COALESCE(p_options->>'namespace', '');
    v_task := COALESCE(p_options->>'task', '');
    v_priority := COALESCE((p_options->>'priority')::integer, 0);
    v_lease_time := COALESCE((p_options->>'leaseTime')::integer, 300);
    v_retry_limit := COALESCE((p_options->>'retryLimit')::integer, 3);
    v_retry_delay := COALESCE((p_options->>'retryDelay')::integer, 1000);
    v_max_size := COALESCE((p_options->>'maxSize')::integer, 0);
    v_ttl := COALESCE((p_options->>'ttl')::integer, 3600);
    -- RUSTFIX item 2: default TRUE when the option is OMITTED (JSON key absent →
    -- ->>'deadLetterQueue' is NULL). An explicit deadLetterQueue:false yields
    -- ('false')::boolean = false and is preserved — so a client can still disable it.
    v_dead_letter_queue := COALESCE((p_options->>'deadLetterQueue')::boolean, true);
    v_dlq_after_max_retries := COALESCE((p_options->>'dlqAfterMaxRetries')::boolean, true);
    v_delayed_processing := COALESCE((p_options->>'delayedProcessing')::integer, 0);
    v_window_buffer := COALESCE((p_options->>'windowBuffer')::integer, 0);
    v_retention_seconds := COALESCE((p_options->>'retentionSeconds')::integer, 0);
    v_completed_retention_seconds := COALESCE((p_options->>'completedRetentionSeconds')::integer, 0);
    v_retention_enabled := COALESCE((p_options->>'retentionEnabled')::boolean, false);
    v_encryption_enabled := COALESCE((p_options->>'encryptionEnabled')::boolean, false);
    v_max_wait_time_seconds := COALESCE((p_options->>'maxWaitTimeSeconds')::integer, 0);
    -- MINIMUM POP WAIT (TASK M): milliseconds a NON-EMPTY pop may hold an
    -- UNDER-FULL batch before claiming, so one commit carries more messages.
    -- Default 0 = OFF (byte-identical to every pre-feature deployment). Clamped
    -- to [0, 60000]: it is bounded independently by the caller's own long-poll
    -- deadline, so this only stops a typo (e.g. seconds passed as ms) from
    -- parking a delivery for an absurd window.
    v_min_pop_wait_time := LEAST(GREATEST(COALESCE((p_options->>'minPopWaitTime')::integer, 0), 0), 60000);
    -- Dedup window now lives on queen.queues (queue-identity merge): the SP is
    -- the single writer, replacing the handler's separate log-queue upsert.
    -- Default 3600 = the column default; clamped at 0 like the handler used to.
    v_dedup_window_seconds := GREATEST(COALESCE((p_options->>'dedupWindowSeconds')::integer, 3600), 0);
    -- RETENTION SINK HOLD (PLAN_S3_SINK.md §5.3, decision D5). Default '' = off
    -- and 604800 = 7 days, i.e. the SQL defaults of the two columns, so the
    -- full-replace rule of this SP leaves a queue that never mentions them
    -- byte-identical to a pre-feature one.
    v_retention_sink_hold := COALESCE(p_options->>'retentionSinkHold', '');
    v_retention_sink_hold_max_seconds :=
        COALESCE((p_options->>'retentionSinkHoldMaxSeconds')::integer, 604800);

    -- These two are REJECTED out of range, not clamped like minPopWaitTime and
    -- dedupWindowSeconds above, and the difference is deliberate. Those two are
    -- bounded independently by something else (the caller's own long-poll
    -- deadline; the probe simply not firing), so a clamp turns a typo into a
    -- harmless value. These GOVERN DELETION: a silently clamped 0 would floor
    -- the hold at now()-60s and hand the lake a hole, a silently clamped
    -- 999999999 would park retention for 31 years. Both directions are damage
    -- an operator must be told about, so the call fails and the queue keeps the
    -- configuration it had.
    --
    -- The sink name is the middle segment of the KV key the retention cycle
    -- probes (`s3:<sink>:<esc queue>:committed`, PLAN_S3_SINK §4.3), and that
    -- key is built by CONCATENATION. The character set is therefore the one
    -- thing standing between a sink name and authority over the key structure:
    -- a name containing ':' composes the same key as a different (sink, queue)
    -- pair. It is the same set the sink's own escape() preserves
    -- (connectors/queen-s3/src/layout.rs), so a legal name is never rewritten.
    IF v_retention_sink_hold !~ '^[A-Za-z0-9._-]{0,64}$' THEN
        RETURN jsonb_build_object(
            'error', format('retentionSinkHold must match [A-Za-z0-9._-]{0,64}, got %L',
                            v_retention_sink_hold));
    END IF;
    -- 60 s floor: below one retention cycle's own cadence the hold cannot mean
    -- anything. 31536000 = one year ceiling: past that the cap is
    -- indistinguishable from the unbounded retention D5 exists to prevent.
    IF v_retention_sink_hold_max_seconds < 60 OR v_retention_sink_hold_max_seconds > 31536000 THEN
        RETURN jsonb_build_object(
            'error', format('retentionSinkHoldMaxSeconds must be between 60 and 31536000, got %s',
                            v_retention_sink_hold_max_seconds));
    END IF;

    -- Insert or update queue (Track B: identity is (tenant_id, name)).
    INSERT INTO queen.queues (
        tenant_id, name, namespace, task, priority, lease_time, retry_limit, retry_delay,
        max_queue_size, ttl, dead_letter_queue, dlq_after_max_retries, delayed_processing,
        window_buffer, retention_seconds, completed_retention_seconds,
        retention_enabled, encryption_enabled, max_wait_time_seconds, min_pop_wait_time,
        dedup_window_seconds, retention_sink_hold, retention_sink_hold_max_seconds
    ) VALUES (
        p_tenant, p_queue_name, v_namespace, v_task, v_priority, v_lease_time, v_retry_limit, v_retry_delay,
        v_max_size, v_ttl, v_dead_letter_queue, v_dlq_after_max_retries, v_delayed_processing,
        v_window_buffer, v_retention_seconds, v_completed_retention_seconds,
        v_retention_enabled, v_encryption_enabled, v_max_wait_time_seconds, v_min_pop_wait_time,
        v_dedup_window_seconds, v_retention_sink_hold, v_retention_sink_hold_max_seconds
    )
    ON CONFLICT (tenant_id, name) DO UPDATE SET
        namespace = EXCLUDED.namespace,
        task = EXCLUDED.task,
        priority = EXCLUDED.priority,
        lease_time = EXCLUDED.lease_time,
        retry_limit = EXCLUDED.retry_limit,
        retry_delay = EXCLUDED.retry_delay,
        max_queue_size = EXCLUDED.max_queue_size,
        ttl = EXCLUDED.ttl,
        dead_letter_queue = EXCLUDED.dead_letter_queue,
        dlq_after_max_retries = EXCLUDED.dlq_after_max_retries,
        delayed_processing = EXCLUDED.delayed_processing,
        window_buffer = EXCLUDED.window_buffer,
        retention_seconds = EXCLUDED.retention_seconds,
        completed_retention_seconds = EXCLUDED.completed_retention_seconds,
        retention_enabled = EXCLUDED.retention_enabled,
        encryption_enabled = EXCLUDED.encryption_enabled,
        max_wait_time_seconds = EXCLUDED.max_wait_time_seconds,
        min_pop_wait_time = EXCLUDED.min_pop_wait_time,
        dedup_window_seconds = EXCLUDED.dedup_window_seconds,
        retention_sink_hold = EXCLUDED.retention_sink_hold,
        retention_sink_hold_max_seconds = EXCLUDED.retention_sink_hold_max_seconds
    RETURNING id INTO v_queue_id;
    
    -- The legacy "ensure a Default partition row" write went away with the rows
    -- message plane: the log engine creates partitions lazily on the first push
    -- (queen.log_partitions), so /configure created a phantom row nothing read.

    RETURN jsonb_build_object(
        'configured', true,
        'queueId', v_queue_id,
        -- Key kept for wire compatibility (webdoc reference/http/queues.mdx), but
        -- always NULL: /configure no longer creates a partition, the first push does.
        'partitionId', NULL::uuid,
        'queue', p_queue_name,
        'namespace', v_namespace,
        'task', v_task,
        -- Wire-compat literal: the storage engine-selector column is gone (one
        -- engine), but the wire keeps echoing 'segments' (same as
        -- 010_log_admin/011_log_stats).
        'storage', 'segments',
        'options', jsonb_build_object(
            'priority', v_priority,
            'leaseTime', v_lease_time,
            'retryLimit', v_retry_limit,
            'retryDelay', v_retry_delay,
            'maxSize', v_max_size,
            'ttl', v_ttl,
            'deadLetterQueue', v_dead_letter_queue,
            'dlqAfterMaxRetries', v_dlq_after_max_retries,
            'delayedProcessing', v_delayed_processing,
            'windowBuffer', v_window_buffer,
            'retentionSeconds', v_retention_seconds,
            'completedRetentionSeconds', v_completed_retention_seconds,
            'retentionEnabled', v_retention_enabled,
            'encryptionEnabled', v_encryption_enabled,
            'maxWaitTimeSeconds', v_max_wait_time_seconds,
            'minPopWaitTime', v_min_pop_wait_time,
            'dedupWindowSeconds', v_dedup_window_seconds,
            'retentionSinkHold', v_retention_sink_hold,
            'retentionSinkHoldMaxSeconds', v_retention_sink_hold_max_seconds
        )
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.configure_queue_v1(TEXT, JSONB, UUID) TO PUBLIC;

