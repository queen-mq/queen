#pragma once

#include <App.h>

namespace queen {
namespace routes {

struct RouteContext;

// ============================================================================
// Route Setup Functions
// ============================================================================
// Each function sets up a category of routes on the provided uWS::App

/**
 * Setup CORS preflight OPTIONS handler
 */
void setup_cors_routes(uWS::App* app);

/**
 * Setup health check endpoint
 * Routes: GET /health
 */
void setup_health_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup maintenance mode endpoints
 * Routes: GET/POST /api/v1/system/maintenance
 */
void setup_maintenance_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup queue configuration endpoints
 * Routes: POST /api/v1/configure
 */
void setup_configure_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup message push endpoints
 * Routes: POST /api/v1/push
 */
void setup_push_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup message pop endpoints
 * Routes: 
 *   GET /api/v1/pop
 *   GET /api/v1/pop/queue/:queue
 *   GET /api/v1/pop/queue/:queue/partition/:partition
 */
void setup_pop_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup message acknowledgment endpoints
 * Routes:
 *   POST /api/v1/ack
 *   POST /api/v1/ack/batch
 */
void setup_ack_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup transaction endpoints
 * Routes: POST /api/v1/transaction
 */
void setup_transaction_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup queen-streams endpoints
 *
 * Routes:
 *   POST /streams/v1/queries        - register / re-register a streaming query
 *   POST /streams/v1/cycle          - atomic per-partition state+sink+ack commit
 *   POST /streams/v1/state/get      - batched per-partition state read
 *
 * Each route submits a JobRequest to libqueen with a new JobType
 * (STREAMS_REGISTER_QUERY, STREAMS_CYCLE, STREAMS_STATE_GET) and dispatches
 * the matching SP through the existing async-PG drain orchestrator. The
 * URL prefix /streams/v1/ is intentionally separate from /api/v1/ so a
 * future extracted streams binary can take over this namespace without
 * breaking SDK clients.
 */
void setup_streams_register_query_routes(uWS::App* app, const RouteContext& ctx);
void setup_streams_cycle_routes(uWS::App* app, const RouteContext& ctx);
void setup_streams_state_get_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup lease management endpoints
 * Routes: POST /api/v1/lease/:leaseId/extend
 */
void setup_lease_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup metrics endpoints
 * Routes: GET /metrics
 */
void setup_metrics_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup Prometheus exposition endpoint.
 *
 * Returns metrics in the Prometheus text format
 * (Content-Type: text/plain; version=0.0.4) so Prometheus / Grafana can
 * scrape this replica directly.
 *
 * Live process state (db pool, threadpools, file buffer, response
 * registries, latest MetricsCollector sample, shared_state) is read
 * in-process; lifetime throughput counters are read once per scrape from
 * queen.worker_metrics_summary via libqueen.
 *
 * Routes: GET /metrics/prometheus
 */
void setup_prometheus_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup resource listing endpoints
 * Routes:
 *   GET /api/v1/resources/queues
 *   GET /api/v1/resources/queues/:queue
 *   DELETE /api/v1/resources/queues/:queue
 *   GET /api/v1/resources/namespaces
 *   GET /api/v1/resources/tasks
 *   GET /api/v1/resources/overview
 */
void setup_resource_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup message listing and detail endpoints
 * Routes:
 *   GET /api/v1/messages
 *   GET /api/v1/messages/:partitionId/:transactionId
 *   DELETE /api/v1/messages/:partitionId/:transactionId
 */
void setup_message_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup dead letter queue endpoints
 * Routes: GET /api/v1/dlq
 */
void setup_dlq_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup message tracing endpoints
 * Routes:
 *   POST /api/v1/traces
 *   GET /api/v1/traces/:partitionId/:transactionId
 *   GET /api/v1/traces/by-name/:traceName
 *   GET /api/v1/traces/names
 */
void setup_trace_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup status and analytics endpoints
 * Routes:
 *   GET /api/v1/status
 *   GET /api/v1/status/queues
 *   GET /api/v1/status/queues/:queue
 *   GET /api/v1/status/queues/:queue/messages
 *   GET /api/v1/status/analytics
 *   GET /api/v1/analytics/system-metrics
 *   GET /api/v1/status/buffers
 */
void setup_status_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup consumer group endpoints
 * Routes:
 *   POST /api/v1/stats/refresh                     (trigger stats recomputation)
 *   GET /api/v1/consumer-groups
 *   GET /api/v1/consumer-groups/lagging
 *   GET /api/v1/consumer-groups/:group
 *   DELETE /api/v1/consumer-groups/:group
 *   POST /api/v1/consumer-groups/:group/subscription
 */
void setup_consumer_group_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup static file serving for webapp
 * Routes:
 *   GET /
 *   GET /assets/... (all assets)
 *   GET /... (SPA fallback for all other routes)
 */
void setup_static_file_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup database migration endpoints
 * Routes:
 *   POST /api/v1/migration/test-connection
 *   POST /api/v1/migration/start
 *   GET /api/v1/migration/status
 *   POST /api/v1/migration/validate
 *   POST /api/v1/migration/reset
 */
void setup_migration_routes(uWS::App* app, const RouteContext& ctx);

/**
 * Setup internal endpoints for inter-instance communication
 * Routes:
 *   WS /internal/ws/peer - WebSocket for peer notifications
 *   POST /internal/api/notify - HTTP fallback for notifications
 *   GET /internal/api/inter-instance/stats - Peer notification statistics
 */
void setup_internal_routes(uWS::App* app, const RouteContext& ctx);

} // namespace routes
} // namespace queen

