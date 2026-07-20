-- ============================================================================
-- queen.get_prometheus_metrics_v1
-- ============================================================================
-- Single round-trip aggregator used by GET /metrics/prometheus. Combines:
--   * cluster lifetime totals (queen.worker_metrics_summary)
--   * latest-bucket per-queue throughput / lag / parked
--     (queen.queue_lag_metrics)
--   * latest-bucket per-worker event-loop / pool / throughput
--     (queen.worker_metrics)
--   * dead-letter-queue depth, total + per queue
--
-- Scope is bounded to the last 5 minutes of buckets so the latest-bucket
-- DISTINCT ON / aggregation never scans the full retention window.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.get_prometheus_metrics_v1()
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ := NOW() - INTERVAL '5 minutes';
BEGIN
    RETURN jsonb_build_object(
        -- Cluster lifetime totals (singleton, identical across replicas).
        'system_totals', (
            SELECT jsonb_build_object(
                'pushRequests',  total_push_requests,
                'popRequests',   total_pop_requests,
                'ackRequests',   total_ack_requests,
                'transactions',  total_transactions,
                'pushMessages',  total_push_messages,
                'popMessages',   total_pop_messages,
                'ackMessages',   total_ack_messages,
                'ackSuccess',    total_ack_success,
                'ackFailed',     total_ack_failed,
                'dbErrors',      total_db_errors,
                'dlqCount',      total_dlq
            )
            FROM queen.worker_metrics_summary
            WHERE id = 1
        ),

        -- Per-queue: most recent bucket within the last 5 minutes.
        -- Cluster-wide aggregate (already SUM-ed across workers in
        -- queue_lag_metrics on insert).
        'per_queue_lag', COALESCE((
            SELECT jsonb_agg(row_to_json(q))
            FROM (
                SELECT DISTINCT ON (queue_name)
                    queue_name           AS queue,
                    pop_count,
                    avg_lag_ms,
                    max_lag_ms,
                    push_request_count,
                    push_message_count,
                    pop_empty_count,
                    ack_request_count,
                    ack_success_count,
                    ack_failed_count,
                    transaction_count,
                    COALESCE(parked_count, 0) AS parked_count,
                    EXTRACT(EPOCH FROM (NOW() - bucket_time))::bigint
                                              AS bucket_age_seconds
                FROM queen.queue_lag_metrics
                WHERE bucket_time >= v_cutoff
                ORDER BY queue_name, bucket_time DESC
            ) q
        ), '[]'::jsonb),

        -- Per-worker: most recent bucket within the last 5 minutes, keyed
        -- by (hostname, worker_id, pid). Includes both event-loop health
        -- gauges and last-minute throughput counters.
        'per_worker', COALESCE((
            SELECT jsonb_agg(row_to_json(w))
            FROM (
                SELECT DISTINCT ON (hostname, worker_id, pid)
                    hostname,
                    worker_id,
                    pid,
                    EXTRACT(EPOCH FROM (NOW() - bucket_time))::bigint
                                                AS bucket_age_seconds,
                    avg_event_loop_lag_ms,
                    max_event_loop_lag_ms,
                    avg_free_slots,
                    min_free_slots,
                    db_connections,
                    avg_job_queue_size,
                    max_job_queue_size,
                    backoff_size,
                    jobs_done,
                    push_request_count,
                    pop_request_count,
                    ack_request_count,
                    transaction_count,
                    push_message_count,
                    pop_message_count,
                    ack_message_count,
                    ack_success_count,
                    ack_failed_count,
                    lag_count,
                    avg_lag_ms,
                    max_lag_ms,
                    db_error_count,
                    dlq_count
                FROM queen.worker_metrics
                WHERE bucket_time >= v_cutoff
                ORDER BY hostname, worker_id, pid, bucket_time DESC
            ) w
        ), '[]'::jsonb),

        -- DLQ depth: cluster total + per-queue breakdown. Scanned with an
        -- index-only count on dead_letter_queue.
        'dlq', jsonb_build_object(
            'total', (SELECT COUNT(*) FROM queen.dead_letter_queue),
            'per_queue', COALESCE((
                SELECT jsonb_agg(row_to_json(d))
                FROM (
                    SELECT q.name AS queue, COUNT(*)::bigint AS count
                    FROM queen.dead_letter_queue dlq
                    JOIN queen.partitions p ON p.id = dlq.partition_id
                    JOIN queen.queues q ON q.id = p.queue_id
                    GROUP BY q.name
                    ORDER BY q.name
                ) d
            ), '[]'::jsonb)
        )
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_prometheus_metrics_v1() TO PUBLIC;
