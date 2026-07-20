-- ============================================================================
-- Configure Queue Stored Procedure
-- ============================================================================
-- Creates or updates queue configuration with all options
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.configure_queue_v1(
    p_queue_name TEXT,
    p_options JSONB DEFAULT '{}'::jsonb
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
    v_queue_id UUID;
    v_partition_id UUID;
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
    v_dead_letter_queue := COALESCE((p_options->>'deadLetterQueue')::boolean, false);
    v_dlq_after_max_retries := COALESCE((p_options->>'dlqAfterMaxRetries')::boolean, false);
    v_delayed_processing := COALESCE((p_options->>'delayedProcessing')::integer, 0);
    v_window_buffer := COALESCE((p_options->>'windowBuffer')::integer, 0);
    v_retention_seconds := COALESCE((p_options->>'retentionSeconds')::integer, 0);
    v_completed_retention_seconds := COALESCE((p_options->>'completedRetentionSeconds')::integer, 0);
    v_retention_enabled := COALESCE((p_options->>'retentionEnabled')::boolean, false);
    v_encryption_enabled := COALESCE((p_options->>'encryptionEnabled')::boolean, false);
    v_max_wait_time_seconds := COALESCE((p_options->>'maxWaitTimeSeconds')::integer, 0);
    
    -- Insert or update queue
    INSERT INTO queen.queues (
        name, namespace, task, priority, lease_time, retry_limit, retry_delay,
        max_queue_size, ttl, dead_letter_queue, dlq_after_max_retries, delayed_processing,
        window_buffer, retention_seconds, completed_retention_seconds, 
        retention_enabled, encryption_enabled, max_wait_time_seconds
    ) VALUES (
        p_queue_name, v_namespace, v_task, v_priority, v_lease_time, v_retry_limit, v_retry_delay,
        v_max_size, v_ttl, v_dead_letter_queue, v_dlq_after_max_retries, v_delayed_processing,
        v_window_buffer, v_retention_seconds, v_completed_retention_seconds,
        v_retention_enabled, v_encryption_enabled, v_max_wait_time_seconds
    )
    ON CONFLICT (name) DO UPDATE SET
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
        max_wait_time_seconds = EXCLUDED.max_wait_time_seconds
    RETURNING id INTO v_queue_id;
    
    -- Ensure default partition exists
    INSERT INTO queen.partitions (queue_id, name)
    VALUES (v_queue_id, 'Default')
    ON CONFLICT (queue_id, name) DO NOTHING
    RETURNING id INTO v_partition_id;
    
    -- If partition already existed, get its id
    IF v_partition_id IS NULL THEN
        SELECT id INTO v_partition_id
        FROM queen.partitions
        WHERE queue_id = v_queue_id AND name = 'Default';
    END IF;
    
    RETURN jsonb_build_object(
        'configured', true,
        'queueId', v_queue_id,
        'partitionId', v_partition_id,
        'queue', p_queue_name,
        'namespace', v_namespace,
        'task', v_task,
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
            'maxWaitTimeSeconds', v_max_wait_time_seconds
        )
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.configure_queue_v1(TEXT, JSONB) TO PUBLIC;

