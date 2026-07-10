#pragma once
// Storage v2 (segments) route handlers — wire format v1 preserved.
// See server/include/queen/storage_v2.hpp and lib/schema/procedures/023_storage_v2.sql.

#include <string>
#include <vector>

#include "queen/routes/route_context.hpp"
#include "queen/queue_types.hpp"

namespace queen::routes_v2 {

using queen::routes::RouteContext;

// True when the queue is configured with storage='segments' (config cache;
// unknown queues default to v1 rows).
bool is_segment_queue(const std::string& queue);

// Handles a push whose items ALL target segment queues. Items must already be
// validated and producer-stamped by the caller. Emits the v1 wire response
// (items array with per-item status). Payloads of queues with
// encryption_enabled are encrypted per frame (flag bit2). No file-buffer
// failover: on DB error the client gets a 503 (documented slice-1 limitation
// for segment queues).
void handle_push_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                    std::vector<PushItem> items);

// Specific-partition pop for a segment queue; emits the v1 wire response.
// wait=true long-polls until a message arrives or timeout_ms elapses
// (deadline polling on the worker loop; see storage_v2_routes.cpp).
void handle_pop_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                   const std::string& queue, const std::string& partition,
                   const std::string& consumer_group, int batch,
                   int lease_seconds, bool auto_ack, bool wait, int timeout_ms);

// Wildcard pop (any partition) for a segment queue: claims up to
// max_partitions partitions in one call and emits the v1 wire response
// (flat messages[] with per-message partition fields; top-level fields from
// the first claimed partition, mirroring pop_unified_batch_v4).
void handle_pop_wildcard_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                            const std::string& queue,
                            const std::string& consumer_group, int batch,
                            int lease_seconds, bool auto_ack, int max_partitions,
                            bool wait, int timeout_ms);

// If the acks target a v2 lease — in-process registry hit, or (on a registry
// miss) the partitionId belongs to q2 — handles the whole batch and returns
// true. Returns false only when the batch is provably not v2 (no leaseId, or
// the partition is cached as not-q2); an unknown partition is resolved with
// an async probe, in which case the function takes ownership of the response
// and dispatches the v1 ack itself when the probe says "rows engine".
bool try_handle_ack_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                       const nlohmann::json& acks);

}  // namespace queen::routes_v2
