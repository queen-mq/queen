#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/queue_types.hpp"
#include "queen/response_queue.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen.hpp"  // libqueen
#include <spdlog/spdlog.h>

#include <optional>

// External globals (declared in acceptor_server.cpp)
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;
}

namespace queen {
namespace routes {

void setup_configure_routes(uWS::App* app, const RouteContext& ctx) {
    app->post("/api/v1/configure", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for queue configuration
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    spdlog::debug("Configure request body: {}", body.dump());
                    
                    if (!body.contains("queue") || !body["queue"].is_string()) {
                        send_error_response(res, "queue is required", 400);
                        return;
                    }
                    
                    std::string queue_name = body["queue"];
                    spdlog::debug("Queue name: {}", queue_name);
                    
                    // Build options JSON for stored procedure. Explicit empty
                    // OBJECT: a default-constructed json is null, and nlohmann
                    // value() on null throws type_error.306 — which used to
                    // escape the deferred callback and kill the worker thread
                    // when a configure carried none of the copied options.
                    nlohmann::json options_json = nlohmann::json::object();
                    
                    // Safe null handling for namespace/task
                    if (body.contains("namespace") && body["namespace"].is_string()) {
                        options_json["namespace"] = body["namespace"].get<std::string>();
                    }
                    if (body.contains("task") && body["task"].is_string()) {
                        options_json["task"] = body["task"].get<std::string>();
                    }
                    
                    // Extract options
                    if (body.contains("options") && body["options"].is_object()) {
                        auto opts = body["options"];
                        if (opts.contains("leaseTime")) options_json["leaseTime"] = opts["leaseTime"];
                        if (opts.contains("maxSize")) options_json["maxSize"] = opts["maxSize"];
                        if (opts.contains("ttl")) options_json["ttl"] = opts["ttl"];
                        if (opts.contains("retryLimit")) options_json["retryLimit"] = opts["retryLimit"];
                        if (opts.contains("retryDelay")) options_json["retryDelay"] = opts["retryDelay"];
                        if (opts.contains("deadLetterQueue")) options_json["deadLetterQueue"] = opts["deadLetterQueue"];
                        if (opts.contains("dlqAfterMaxRetries")) options_json["dlqAfterMaxRetries"] = opts["dlqAfterMaxRetries"];
                        if (opts.contains("priority")) options_json["priority"] = opts["priority"];
                        if (opts.contains("delayedProcessing")) options_json["delayedProcessing"] = opts["delayedProcessing"];
                        if (opts.contains("windowBuffer")) options_json["windowBuffer"] = opts["windowBuffer"];
                        if (opts.contains("retentionSeconds")) options_json["retentionSeconds"] = opts["retentionSeconds"];
                        if (opts.contains("completedRetentionSeconds")) options_json["completedRetentionSeconds"] = opts["completedRetentionSeconds"];
                        if (opts.contains("retentionEnabled")) options_json["retentionEnabled"] = opts["retentionEnabled"];
                        if (opts.contains("encryptionEnabled")) options_json["encryptionEnabled"] = opts["encryptionEnabled"];
                        if (opts.contains("maxWaitTimeSeconds")) options_json["maxWaitTimeSeconds"] = opts["maxWaitTimeSeconds"];
                    }

                    // STORAGE V2: options.storage selects the engine ('rows' v1
                    // default, 'segments' v2). configure_queue_v1 ignores
                    // unknown keys, so the flag is persisted by a follow-up
                    // UPDATE after a successful configure (see below).
                    std::string storage_requested;  // empty = not provided
                    if (body.contains("options") && body["options"].is_object()
                        && body["options"].contains("storage")) {
                        const auto& sv = body["options"]["storage"];
                        if (!sv.is_string()
                            || (sv.get<std::string>() != "rows"
                                && sv.get<std::string>() != "segments")) {
                            send_error_response(res,
                                "options.storage must be 'rows' or 'segments'", 400);
                            return;
                        }
                        storage_requested = sv.get<std::string>();
                    }

                    // Current storage (cache, falling back to DB): needed both
                    // to detect a flip and to PRESERVE the flag in the cache
                    // when the caller re-configures without options.storage.
                    bool queue_known = false;
                    std::string existing_storage = "rows";
                    if (global_shared_state) {
                        auto existing = global_shared_state->get_or_fetch_queue_config(queue_name);
                        if (existing) {
                            queue_known = true;
                            existing_storage = existing->storage;
                        }
                    }
                    const std::string effective_storage = !storage_requested.empty()
                        ? storage_requested
                        : (queue_known ? existing_storage : "rows");

                    // encryptionEnabled is supported on BOTH engines: the v2
                    // push path encrypts the frame payload into an
                    // {"encrypted","iv","authTag"} envelope (frame flag bit 2)
                    // and the pop path decrypts it — no engine-specific
                    // validation needed (the slice-1 reject is gone).

                    // options.dedupWindowSeconds — segments-only knob bounding
                    // the q2 dedup window (q2.queues.dedup_window_seconds,
                    // 0 = off). configure_queue_v1 predates it, so it is
                    // persisted by q2.set_queue_options_v1 after a successful
                    // configure (see finalize_configured below). Rejected on
                    // rows queues instead of being silently ignored.
                    std::optional<long long> dedup_window;  // empty = not provided
                    if (body.contains("options") && body["options"].is_object()
                        && body["options"].contains("dedupWindowSeconds")) {
                        const auto& dv = body["options"]["dedupWindowSeconds"];
                        if (!dv.is_number_integer() || dv.get<long long>() < 0
                            || dv.get<long long>() > 2147483647LL) {
                            send_error_response(res,
                                "options.dedupWindowSeconds must be a non-negative int32", 400);
                            return;
                        }
                        if (effective_storage != "segments") {
                            send_error_response(res,
                                "options.dedupWindowSeconds requires storage='segments' "
                                "(the rows engine dedups through its unique index, not a window)", 400);
                            return;
                        }
                        dedup_window = dv.get<long long>();
                    }

                    // Flip probe policy (fail-closed): whenever a storage value
                    // is requested and the queue isn't KNOWN to already run on
                    // it, probe the engine being abandoned (the opposite of the
                    // requested one) for data. Unknown config (cache+DB lookup
                    // miss) therefore still probes — a flip can never slip
                    // through on a config-lookup failure. New/empty queues pass
                    // (EXISTS is false) at the cost of one cheap probe.
                    const bool storage_noop = queue_known && !storage_requested.empty()
                        && storage_requested == existing_storage;
                    const bool need_flip_probe = !storage_requested.empty() && !storage_noop;
                    const std::string abandoned_storage =
                        (storage_requested == "segments") ? "rows" : "segments";

                    spdlog::info("[Worker {}] Configuring queue: {} via stored procedure", ctx.worker_id, queue_name);
                    
                    // Register response for async delivery
                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );

                    // Capture values for callbacks
                    auto queen_ptr = ctx.queen;
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;
                    std::string captured_queue_name = queue_name;
                    nlohmann::json captured_options = options_json;

                    // Final step: optionally refresh the config cache (storage
                    // preserved/updated via effective_storage) and respond.
                    auto apply_cache_and_respond = [worker_id, request_id, captured_queue_name,
                                                    captured_options, effective_storage,
                                                    dedup_window](
                                                       nlohmann::json json_response,
                                                       int status_code, bool is_error,
                                                       bool update_cache) {
                        // Never let an nlohmann type error escape into the
                        // worker event loop (it would unwind the whole worker
                        // thread): degrade to skipping the cache refresh.
                        try {
                        if (update_cache && global_shared_state) {
                            caches::CachedQueueConfig cached_config;
                            cached_config.name = captured_queue_name;
                            cached_config.namespace_name = captured_options.value("namespace", "");
                            cached_config.task = captured_options.value("task", "");
                            cached_config.priority = captured_options.value("priority", 0);
                            cached_config.lease_time = captured_options.value("leaseTime", 300);
                            cached_config.retry_limit = captured_options.value("retryLimit", 3);
                            cached_config.retry_delay = captured_options.value("retryDelay", 1000);
                            cached_config.max_size = captured_options.value("maxSize", 0);
                            cached_config.ttl = captured_options.value("ttl", 3600);
                            cached_config.dlq_enabled = captured_options.value("deadLetterQueue", false);
                            cached_config.dlq_after_max_retries = captured_options.value("dlqAfterMaxRetries", false);
                            cached_config.delayed_processing = captured_options.value("delayedProcessing", 0);
                            cached_config.window_buffer = captured_options.value("windowBuffer", 0);
                            cached_config.retention_seconds = captured_options.value("retentionSeconds", 0);
                            cached_config.completed_retention_seconds = captured_options.value("completedRetentionSeconds", 0);
                            cached_config.encryption_enabled = captured_options.value("encryptionEnabled", false);
                            // Storage engine: requested value, or the queue's
                            // pre-existing one when the caller didn't send it
                            // (a re-configure must not silently flip a
                            // segments queue back to rows in the cache).
                            cached_config.storage = effective_storage;
                            global_shared_state->set_queue_config(captured_queue_name, cached_config);
                            spdlog::debug("Queue '{}' config cached (storage='{}')",
                                          captured_queue_name, effective_storage);
                        }
                        if (!is_error && json_response.is_object()
                            && json_response.contains("options")
                            && json_response["options"].is_object()) {
                            // configure_queue_v1 predates the storage flag;
                            // reflect the effective engine in the reply.
                            json_response["options"]["storage"] = effective_storage;
                            // Same for the dedup window: echoed only once the
                            // whole chain (including set_queue_options_v1)
                            // succeeded — update_cache is true exactly then.
                            if (update_cache && dedup_window.has_value()) {
                                json_response["options"]["dedupWindowSeconds"] = *dedup_window;
                            }
                        }
                        } catch (const std::exception& e) {
                            spdlog::warn("Configure: cache update for '{}' skipped: {}",
                                         captured_queue_name, e.what());
                        }
                        worker_response_registries[worker_id]->send_response(
                            request_id, json_response, is_error, status_code);
                    };

                    // Terminal success step, entered only after configure (and
                    // the storage-flag UPDATE when requested) succeeded: apply
                    // the dedup window through q2.set_queue_options_v1, then
                    // cache+respond. The cache refreshes ONLY when every
                    // persisted step succeeded; a failed knob drops the cache
                    // entry so the next access refetches DB truth.
                    auto finalize_configured = [queen_ptr, worker_loop, worker_id, request_id,
                                                captured_queue_name, dedup_window,
                                                apply_cache_and_respond](
                                                   nlohmann::json json_response) {
                        if (!dedup_window.has_value()) {
                            apply_cache_and_respond(std::move(json_response), 200, false, true);
                            return;
                        }
                        queen::JobRequest opt;
                        opt.op_type = queen::JobType::CUSTOM;
                        opt.request_id = request_id;
                        opt.sql = "SELECT q2.set_queue_options_v1($1, $2::int)";
                        opt.params = {captured_queue_name, std::to_string(*dedup_window)};

                        queen_ptr->submit(std::move(opt),
                            [worker_loop, worker_id, request_id, captured_queue_name,
                             json_response = std::move(json_response),
                             apply_cache_and_respond](std::string opt_result) mutable {
                            worker_loop->defer([opt_result = std::move(opt_result), worker_id,
                                                request_id, captured_queue_name,
                                                json_response = std::move(json_response),
                                                apply_cache_and_respond]() mutable {
                                bool opt_ok = false;
                                std::string opt_err = "dedup window update failed";
                                try {
                                    auto r = nlohmann::json::parse(opt_result);
                                    if (r.is_object() && r.contains("error") && !r["error"].is_null()) {
                                        opt_err = r["error"].is_string()
                                            ? r["error"].get<std::string>() : r["error"].dump();
                                    } else {
                                        opt_ok = true;
                                    }
                                } catch (const std::exception& e) {
                                    opt_err = e.what();
                                }
                                if (!opt_ok) {
                                    if (global_shared_state) {
                                        global_shared_state->delete_queue_config(captured_queue_name);
                                    }
                                    apply_cache_and_respond(
                                        {{"error", "queue configured but dedupWindowSeconds update failed: " + opt_err}},
                                        500, true, false);
                                    return;
                                }
                                apply_cache_and_respond(std::move(json_response), 200, false, true);
                            });
                        });
                    };

                    // Configure via stored procedure; on success persist the
                    // storage flag (configure_queue_v1 ignores unknown keys,
                    // so 'storage' needs its own UPDATE) before caching.
                    auto submit_configure = [queen_ptr, worker_loop, worker_id, request_id,
                                             captured_queue_name, storage_requested,
                                             captured_options, apply_cache_and_respond,
                                             finalize_configured]() {
                        queen::JobRequest job_req;
                        job_req.op_type = queen::JobType::CUSTOM;
                        job_req.request_id = request_id;
                        job_req.sql = "SELECT queen.configure_queue_v1($1, $2::jsonb)";
                        job_req.params = {captured_queue_name, captured_options.dump()};

                        queen_ptr->submit(std::move(job_req),
                            [queen_ptr, worker_loop, worker_id, request_id, captured_queue_name,
                             storage_requested, apply_cache_and_respond,
                             finalize_configured](std::string result) {
                            worker_loop->defer([result = std::move(result), queen_ptr, worker_loop,
                                                worker_id, request_id, captured_queue_name,
                                                storage_requested, apply_cache_and_respond,
                                                finalize_configured]() {
                                nlohmann::json json_response;
                                int status_code = 200;
                                bool is_error = false;
                                bool configured = false;

                                try {
                                    json_response = nlohmann::json::parse(result);
                                    if (json_response.contains("error") && !json_response["error"].is_null()) {
                                        is_error = true;
                                        status_code = 500;
                                    } else if (json_response.contains("configured")
                                               && json_response["configured"].get<bool>()) {
                                        configured = true;
                                    }
                                } catch (const std::exception& e) {
                                    json_response = {{"error", e.what()}};
                                    status_code = 500;
                                    is_error = true;
                                }

                                if (!configured) {
                                    // Configure failed: answer as-is, never
                                    // cache (nothing was persisted).
                                    apply_cache_and_respond(std::move(json_response),
                                                            status_code, is_error, false);
                                    return;
                                }
                                if (storage_requested.empty()) {
                                    // No storage flag to persist: straight to
                                    // the terminal step (dedup knob + cache).
                                    finalize_configured(std::move(json_response));
                                    return;
                                }

                                // Persist the storage flag, then cache+respond.
                                queen::JobRequest upd;
                                upd.op_type = queen::JobType::CUSTOM;
                                upd.request_id = request_id;
                                upd.sql = "UPDATE queen.queues SET storage = $2 WHERE name = $1";
                                upd.params = {captured_queue_name, storage_requested};

                                queen_ptr->submit(std::move(upd),
                                    [worker_loop, worker_id, request_id, captured_queue_name,
                                     json_response = std::move(json_response),
                                     apply_cache_and_respond,
                                     finalize_configured](std::string upd_result) mutable {
                                    worker_loop->defer([upd_result = std::move(upd_result), worker_id,
                                                        request_id, captured_queue_name,
                                                        json_response = std::move(json_response),
                                                        apply_cache_and_respond,
                                                        finalize_configured]() mutable {
                                        bool upd_ok = false;
                                        std::string upd_err = "storage update failed";
                                        try {
                                            auto r = nlohmann::json::parse(upd_result);
                                            if (r.is_object() && r.contains("error") && !r["error"].is_null()) {
                                                upd_err = r["error"].is_string()
                                                    ? r["error"].get<std::string>() : r["error"].dump();
                                            } else {
                                                upd_ok = true;
                                            }
                                        } catch (const std::exception& e) {
                                            upd_err = e.what();
                                        }

                                        if (!upd_ok) {
                                            // Options applied but the engine flag
                                            // didn't: drop the cache entry so the
                                            // next access refetches DB truth.
                                            if (global_shared_state) {
                                                global_shared_state->delete_queue_config(captured_queue_name);
                                            }
                                            apply_cache_and_respond(
                                                {{"error", "queue configured but storage flag update failed: " + upd_err}},
                                                500, true, false);
                                            return;
                                        }
                                        finalize_configured(std::move(json_response));
                                    });
                                });
                            });
                        });
                    };

                    if (need_flip_probe) {
                        // Flipping an existing queue's engine with data present
                        // strands messages in the old engine's tables (each
                        // pop path only reads its own): probe the engine being
                        // abandoned and reject unless empty. Empty queues flip
                        // freely.
                        queen::JobRequest probe;
                        probe.op_type = queen::JobType::CUSTOM;
                        probe.request_id = request_id;
                        probe.sql = (abandoned_storage == "rows")
                            ? "SELECT jsonb_build_object('hasData', EXISTS ("
                              "SELECT 1 FROM queen.messages m "
                              "JOIN queen.partitions p ON p.id = m.partition_id "
                              "JOIN queen.queues q ON q.id = p.queue_id WHERE q.name = $1))"
                            : "SELECT jsonb_build_object('hasData', EXISTS ("
                              "SELECT 1 FROM q2.segments s "
                              "JOIN q2.partitions p ON p.id = s.partition_id "
                              "JOIN q2.queues q ON q.id = p.queue_id WHERE q.name = $1))";
                        probe.params = {queue_name};

                        std::string flip_msg = "cannot change storage of queue '" + queue_name
                            + "' from '" + abandoned_storage + "' to '" + storage_requested
                            + "': queue has data (empty queues may flip freely)";

                        ctx.queen->submit(std::move(probe),
                            [worker_loop, worker_id, request_id, flip_msg,
                             submit_configure](std::string result) {
                            worker_loop->defer([result = std::move(result), worker_id, request_id,
                                                flip_msg, submit_configure]() {
                                bool has_data = true;  // fail closed
                                std::string err;
                                try {
                                    auto r = nlohmann::json::parse(result);
                                    if (r.is_object() && r.contains("error") && !r["error"].is_null()) {
                                        err = r["error"].is_string()
                                            ? r["error"].get<std::string>() : r["error"].dump();
                                    } else if (r.is_object() && r.contains("hasData")
                                               && r["hasData"].is_boolean()) {
                                        has_data = r["hasData"].get<bool>();
                                    } else {
                                        err = "unexpected storage probe result";
                                    }
                                } catch (const std::exception& e) {
                                    err = e.what();
                                }

                                if (!err.empty()) {
                                    worker_response_registries[worker_id]->send_response(
                                        request_id,
                                        {{"error", "storage flip probe failed: " + err}},
                                        true, 500);
                                    return;
                                }
                                if (has_data) {
                                    worker_response_registries[worker_id]->send_response(
                                        request_id, {{"error", flip_msg}}, true, 400);
                                    return;
                                }
                                submit_configure();
                            });
                        });
                    } else {
                        submit_configure();
                    }
                    
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
}

} // namespace routes
} // namespace queen
