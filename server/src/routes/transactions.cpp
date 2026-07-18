#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen.hpp"  // libqueen
#include "queen/response_queue.hpp"
#include "queen/encryption.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/routes/storage_v2_routes.hpp"  // is_segment_queue routing
#include "queen/storage_v2.hpp"                // frames + LeaseRegistry
#include <spdlog/spdlog.h>

#include <algorithm>
#include <map>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

// External globals
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;
}

namespace queen {
namespace routes {

namespace {

namespace sv2 = queen::storage_v2;

// ===========================================================================
// Storage v2 (segments) transaction: atomic push+ack through ONE call to
// queen.seg_transaction_wire_v1 (026_storage_v2_maintenance.sql). Wire-compatible
// with the v1 endpoint: same request shape, same response shape
// ({transactionId, success, results:[...]}, per-flattened-op result rows).
//
// Engine routing: the request is served here only when it is "v2-shaped" —
// some push targets a storage='segments' queue, or some ack resolves to a
// v2 lease in this broker's LeaseRegistry. Ack ops carry no queue name
// (SDKs send {type:'ack', transactionId, partitionId, status} plus a
// top-level requiredLeases hint array), so registry resolution doubles as
// their engine detection: acks of rows leases never hit the v2 registry and
// fall through to the v1 path untouched.
//
// ATOMICITY: ack positions are computed with the READ-ONLY
// LeaseRegistry::preview_txn_ack — registry state is consumed (via ack()
// replay) only AFTER the SQL commit succeeds. A rolled-back transaction
// (e.g. duplicate push) leaves the leased batches ackable through the plain
// /api/v1/ack path as if the transaction never happened.
//
// Ack semantics mirror the wire ack path (storage_v2_routes.cpp): when the
// transaction's acks close a leased batch (every delivered message
// responded, or any failure closes it early), the SQL payload carries the
// terminal cursor position (highest contiguous acked-OK prefix) and
// queen.seg_ack_segments_v1 persists it atomically with the pushes. Acks covering
// only part of a batch are recorded broker-side on success — the cursor
// moves when the remaining wire acks complete the batch (at-least-once,
// same as a partial /api/v1/ack).
//
// CROSS-BROKER: an ack group the local registry cannot resolve (the pop was
// served by another broker instance, or the registry died with a restart)
// is shipped to SQL in the txns form ({"partitionId","group","worker",
// "txns":[{"txn","ok"},...]}); queen.seg_transaction_wire_v1 resolves each txn
// through the queen.seg_dedup window — the same resolver as the wire-ack fallback
// queen.seg_ack_by_txn_v1 — atomically with the rest of the batch. The only local
// rejection left is a partition id that cannot belong to q2 at all (not a
// uuid, i.e. a rows-engine ack mixed into a segments transaction); a uuid
// that is not a q2 partition is rejected by SQL and rolls the whole
// transaction back.
// ===========================================================================

struct V2TxnPush {
    size_t index;                 // flattened result index (v1 order)
    std::string queue, partition;
    nlohmann::json payload;
    std::optional<std::string> txn_id;    // client-supplied transactionId
    std::optional<std::string> trace_id;
};

struct V2TxnAck {
    size_t index;
    std::string txn, partition_id, group, status;
    std::string lease;            // per-op leaseId hint (raw HTTP callers)
    bool ok;                      // status maps to acked-OK
};

// Returns true when this function owned the response (served or rejected);
// false = not v2-shaped, the caller runs the v1 path untouched.
bool try_handle_transaction_v2(const RouteContext& ctx,
                               uWS::HttpResponse<false>* res,
                               const nlohmann::json& body) {
    const auto& operations = body["operations"];

    // ------------------------------------------------- flatten + classify
    std::vector<V2TxnPush> pushes;
    std::vector<V2TxnAck> acks;
    bool any_segments = false, any_rows = false, any_unknown_op = false;
    size_t flat_idx = 0;

    auto classify_queue = [&](const std::string& q) {
        if (!q.empty() && queen::routes_v2::is_segment_queue(q)) any_segments = true;
        else any_rows = true;
    };
    auto parse_push = [&](const nlohmann::json& item) {
        V2TxnPush p;
        p.index = flat_idx++;
        if (item.is_object()) {
            p.queue = item.value("queue", "");
            p.partition = item.value("partition", "Default");
            p.payload = item.contains("payload") ? item["payload"]
                        : item.value("data", nlohmann::json::object());
            if (item.contains("transactionId") && item["transactionId"].is_string())
                p.txn_id = item["transactionId"].get<std::string>();
            if (item.contains("traceId") && item["traceId"].is_string())
                p.trace_id = item["traceId"].get<std::string>();
        } else {
            p.partition = "Default";
            p.payload = nlohmann::json::object();
        }
        classify_queue(p.queue);
        pushes.push_back(std::move(p));
    };

    for (const auto& op : operations) {
        std::string op_type = op.is_object() ? op.value("type", "") : "";
        if (op_type == "push" && op.contains("items") && op["items"].is_array()) {
            for (const auto& item : op["items"]) parse_push(item);
        } else if (op_type == "push") {
            parse_push(op);  // flat form {type:'push', queue, payload, ...}
        } else if (op_type == "ack") {
            V2TxnAck a;
            a.index = flat_idx++;
            a.txn = op.value("transactionId", "");
            a.partition_id = op.value("partitionId", "");
            a.group = op.value("consumerGroup", "__QUEUE_MODE__");
            a.status = op.value("status", "completed");
            a.lease = op.value("leaseId", "");
            a.ok = (a.status == "completed" || a.status == "success");
            acks.push_back(std::move(a));
        } else {
            any_unknown_op = true;
            flat_idx++;
        }
    }

    // -------------------------- resolve acks against the local registry
    // Lease hints: top-level requiredLeases (SDKs) + per-op leaseId (raw
    // HTTP callers); preview_txn_ack falls back to a full registry scan.
    std::vector<std::string> lease_hints;
    if (body.contains("requiredLeases") && body["requiredLeases"].is_array()) {
        for (const auto& l : body["requiredLeases"])
            if (l.is_string() && !l.get<std::string>().empty())
                lease_hints.push_back(l.get<std::string>());
    }
    for (const auto& op : operations) {
        if (op.is_object() && op.value("type", "") == "ack") {
            std::string l = op.value("leaseId", "");
            if (!l.empty()) lease_hints.push_back(l);
        }
    }

    // One leased batch per (partition, group): group the acks accordingly,
    // preserving arrival order within each group.
    struct AckGroup {
        std::string partition_id, group;
        std::string lease_hint;   // first per-op leaseId of the group, if any
        std::vector<std::pair<std::string, bool>> items;  // (txn, ok)
        sv2::LeaseRegistry::TxnAckPreview preview;
    };
    std::vector<AckGroup> ack_groups;
    std::map<std::pair<std::string, std::string>, size_t> ack_group_of;
    for (const auto& a : acks) {
        auto key = std::make_pair(a.partition_id, a.group);
        auto it = ack_group_of.find(key);
        if (it == ack_group_of.end()) {
            ack_group_of[key] = ack_groups.size();
            ack_groups.push_back({a.partition_id, a.group, {}, {}, {}});
            it = ack_group_of.find(key);
        }
        auto& grp = ack_groups[it->second];
        if (grp.lease_hint.empty() && !a.lease.empty()) grp.lease_hint = a.lease;
        grp.items.emplace_back(a.txn, a.ok);
    }

    auto& reg = sv2::LeaseRegistry::instance();
    bool any_ack_resolved = false;
    for (auto& g : ack_groups) {
        g.preview = reg.preview_txn_ack(g.partition_id, g.group, g.items,
                                        lease_hints);
        if (g.preview.found) any_ack_resolved = true;
    }

    // Deterministic partition order in the SQL payload: the q2 ack
    // procedures lock consumer rows in payload order, so two brokers acking
    // an overlapping partition set in different arrival orders could
    // deadlock. Sorting by partition uuid (group as tie-break) gives every
    // broker the same lock order.
    std::sort(ack_groups.begin(), ack_groups.end(),
              [](const AckGroup& x, const AckGroup& y) {
                  return std::tie(x.partition_id, x.group) <
                         std::tie(y.partition_id, y.group);
              });

    // Not v2-shaped: the v1 path owns the request, byte-identical behavior.
    if (!any_segments && !any_ack_resolved) return false;

    // ------------------------------------------------------------ validate
    // Response id generated up front: v2 rejections carry the same body keys
    // as a v1 transaction failure ({transactionId, success:false, error,
    // results}) so SDK error handling sees one shape regardless of engine.
    std::string txn_response_id = ctx.async_queue_manager->generate_uuid();
    auto reject = [&](const std::string& msg, int status) {
        send_json_response(res,
                           nlohmann::json{{"transactionId", txn_response_id},
                                          {"success", false},
                                          {"error", msg},
                                          {"results", nlohmann::json::array()}},
                           status);
    };

    if (any_rows) {
        reject("mixed rows/segments transactions are not supported: every queue "
               "in a transaction must use the same storage engine", 400);
        return true;
    }
    if (any_unknown_op) {
        reject("segments transaction supports only push and ack operations", 400);
        return true;
    }
    for (const auto& g : ack_groups) {
        if (g.preview.found) continue;
        // Registry miss: served SQL-side (txns form, see the payload build)
        // UNLESS the partition id cannot belong to q2 at all — q2 partition
        // ids are uuids, so a non-uuid id is a rows-engine ack mixed into a
        // segments transaction.
        uint8_t scratch[16];
        if (!sv2::uuid_to_bytes(g.partition_id, scratch)) {
            reject("ack partitionId '" + g.partition_id + "' is not a "
                   "segments (q2) partition: mixed rows/segments transactions "
                   "are not supported", 409);
            return true;
        }
    }

    spdlog::info("[Worker {}] TRANSACTION: Executing segments transaction "
                 "({} pushes, {} acks)",
                 ctx.worker_id, pushes.size(), acks.size());

    // ----------------- build push groups (mirrors handle_push_v2 exactly:
    // one segment per (queue, partition), intra-batch dedup first-wins,
    // per-frame encryption when the queue asks for it)
    struct PushGroup {
        std::string queue, partition;
        std::vector<sv2::FrameIn> frames;
        std::map<std::string, std::string> seen_txn;  // txn -> first message_id
    };
    std::vector<PushGroup> groups;
    std::map<std::string, size_t> group_of;

    EncryptionService* enc_service = get_encryption_service();
    bool enc_ready = enc_service && enc_service->is_enabled();
    std::map<std::string, bool> enc_by_queue;
    auto queue_wants_encryption = [&](const std::string& q) {
        auto e = enc_by_queue.find(q);
        if (e != enc_by_queue.end()) return e->second;
        bool on = false;
        if (global_shared_state) {
            auto cfg = global_shared_state->get_or_fetch_queue_config(q);
            on = cfg && cfg->encryption_enabled;
        }
        enc_by_queue[q] = on;
        return on;
    };

    // Per-item echo for the response (ids are known before the SQL runs).
    struct PushEcho { size_t index; std::string txn, mid, queue; bool duplicate; };
    std::vector<PushEcho> echoes;
    echoes.reserve(pushes.size());

    for (const auto& it : pushes) {
        std::string key = it.queue + "\x1f" + it.partition;
        auto g = group_of.find(key);
        if (g == group_of.end()) {
            group_of[key] = groups.size();
            groups.push_back({it.queue, it.partition, {}, {}});
            g = group_of.find(key);
        }
        auto& grp = groups[g->second];

        sv2::FrameIn f;
        f.message_id = ctx.async_queue_manager->generate_uuid();
        f.transaction_id = (it.txn_id && !it.txn_id->empty())
                               ? *it.txn_id
                               : ctx.async_queue_manager->generate_uuid();

        auto seen = grp.seen_txn.find(f.transaction_id);
        if (seen != grp.seen_txn.end()) {
            // Intra-batch duplicate: first occurrence wins, echo its id.
            echoes.push_back({it.index, f.transaction_id, seen->second,
                              it.queue, true});
            continue;
        }
        grp.seen_txn[f.transaction_id] = f.message_id;
        echoes.push_back({it.index, f.transaction_id, f.message_id,
                          it.queue, false});

        if (it.trace_id && !it.trace_id->empty()) f.trace_id = *it.trace_id;
        f.payload = it.payload.dump();
        if (queue_wants_encryption(it.queue)) {
            if (enc_ready) {
                auto encd = enc_service->encrypt_payload(f.payload);
                if (encd.has_value()) {
                    f.payload = nlohmann::json{{"encrypted", encd->encrypted},
                                               {"iv", encd->iv},
                                               {"authTag", encd->auth_tag}}.dump();
                    f.encrypted = true;
                } else {
                    spdlog::warn("[Worker {}] TRANSACTION v2: Encryption failed for queue '{}', storing plaintext",
                                 ctx.worker_id, it.queue);
                }
            } else {
                spdlog::warn("[Worker {}] TRANSACTION v2: Queue '{}' requires encryption but service not available",
                             ctx.worker_id, it.queue);
            }
        }
        grp.frames.push_back(std::move(f));
    }
    groups.erase(std::remove_if(groups.begin(), groups.end(),
                                [](const PushGroup& g) { return g.frames.empty(); }),
                 groups.end());

    // ------------------------------------------ SQL payload (026 contract)
    nlohmann::json payload = {{"pushes", nlohmann::json::array()},
                              {"acks", nlohmann::json::array()}};
    for (const auto& g : groups) {
        nlohmann::json metas = nlohmann::json::array();
        for (size_t k = 0; k < g.frames.size(); k++) {
            metas.push_back({{"i", static_cast<int>(k)},
                             {"mid", g.frames[k].message_id},
                             {"txn", g.frames[k].transaction_id}});
        }
        auto blob = sv2::zstd_compress(sv2::pack_frames(g.frames));
        payload["pushes"].push_back(
            {{"queue", g.queue},
             {"partition", g.partition},
             {"metas", std::move(metas)},
             {"blobB64", sv2::b64_encode(blob.data(), blob.size())},
             {"count", g.frames.size()}});
    }
    for (const auto& g : ack_groups) {
        if (g.preview.found && !g.preview.complete) {
            continue;  // partial batch: recorded broker-side on success
        }
        if (g.preview.found) {
            // Locally-resolved batch closing now: terminal cursor position.
            payload["acks"].push_back({{"partitionId", g.partition_id},
                                       {"group", g.preview.consumer_group},
                                       {"worker", g.preview.lease_id},
                                       {"uptoSeq", g.preview.upto_seq},
                                       {"uptoOff", g.preview.upto_off},
                                       {"ok", g.preview.ok},
                                       {"count", g.preview.acked_count}});
            continue;
        }
        // Registry miss (cross-broker / post-restart lease): txns form, the
        // positions are resolved in SQL through the queen.seg_dedup window. The
        // worker (lease) comes from the group's own per-op leaseId hint,
        // falling back to the request's requiredLeases when they all agree.
        std::string worker = g.lease_hint;
        if (worker.empty()) {
            std::string only;
            bool unambiguous = true;
            for (const auto& h : lease_hints) {
                if (only.empty()) only = h;
                else if (h != only) { unambiguous = false; break; }
            }
            if (unambiguous) worker = only;
        }
        nlohmann::json txns = nlohmann::json::array();
        for (const auto& item : g.items) {
            txns.push_back({{"txn", item.first}, {"ok", item.second}});
        }
        payload["acks"].push_back({{"partitionId", g.partition_id},
                                   {"group", g.group},
                                   {"worker", worker},
                                   {"txns", std::move(txns)}});
    }

    // -------------------------------------------------- dispatch + respond
    std::string request_id =
        worker_response_registries[ctx.worker_id]->register_response(
            res, ctx.worker_id, nullptr);

    queen::JobRequest job;
    job.op_type = queen::JobType::CUSTOM;
    job.request_id = request_id;
    job.sql = "SELECT queen.seg_transaction_wire_v1($1::jsonb)";
    job.params = {payload.dump()};

    auto worker_loop = ctx.worker_loop;
    auto worker_id = ctx.worker_id;
    auto cluster = ctx.queen;
    size_t result_slots = flat_idx;

    ctx.queen->submit(std::move(job), [worker_loop, worker_id, cluster, request_id,
                                       txn_response_id, ack_groups, echoes,
                                       acks, result_slots](std::string result) {
        worker_loop->defer([=]() {
            bool committed = false;
            std::string err;
            try {
                auto r = nlohmann::json::parse(result);
                if (r.is_object() && r.value("ok", false)) {
                    committed = true;
                } else if (r.is_object() && r.contains("error") && !r["error"].is_null()) {
                    err = r["error"].is_string() ? r["error"].get<std::string>()
                                                 : r["error"].dump();
                } else {
                    err = "unexpected transaction result: " + r.dump();
                }
            } catch (const std::exception& e) {
                err = e.what();
            }

            if (!committed) {
                // Rolled back atomically in SQL. The registry was never
                // touched: the leased batches stay ackable through the plain
                // wire path (v1-shaped failure body, HTTP 200 like v1).
                nlohmann::json out = {{"transactionId", txn_response_id},
                                      {"success", false},
                                      {"error", err},
                                      {"results", nlohmann::json::array()}};
                worker_response_registries[worker_id]->send_response_raw(
                    request_id, out.dump(), 200);
                return;
            }

            // Committed: NOW consume the registry state. Completed batches
            // drop their entries (positions already persisted by the SQL
            // ack); partial batches record the responses so later wire acks
            // complete them with the cumulative contiguous prefix.
            auto& reg2 = sv2::LeaseRegistry::instance();
            for (const auto& g : ack_groups) {
                if (!g.preview.found) continue;  // SQL-side group: no local state
                for (const auto& item : g.items) {
                    auto oc = reg2.ack(g.preview.lease_id, g.partition_id,
                                       item.first, item.second);
                    if (!oc.complete || g.preview.complete) continue;
                    // The batch closed DURING replay: wire acks for its other
                    // messages landed between the preview and the SQL commit,
                    // and this replayed response was the closing one. The
                    // registry entry is now erased, so nobody else will ever
                    // persist this cursor — dispatch the terminal
                    // queen.seg_ack_segments_v1 immediately (it used to be
                    // discarded, leaving the lease taken until expiry).
                    // Fire-and-forget: the transaction already committed; a
                    // failure here (lease raced/expired) means redelivery,
                    // the same at-least-once outcome as any lost terminal
                    // ack, so it is logged and not surfaced to the client.
                    queen::JobRequest ack_job;
                    ack_job.op_type = queen::JobType::CUSTOM;
                    ack_job.request_id = request_id;
                    ack_job.sql = "SELECT queen.seg_ack_segments_v1($1, $2, $3, $4, "
                                  "$5::bigint, $6::int, $7::bool, $8::int)";
                    ack_job.params = {oc.queue, oc.partition, oc.consumer_group,
                                      g.preview.lease_id,
                                      std::to_string(oc.upto_seq),
                                      std::to_string(oc.upto_off),
                                      oc.ok ? "true" : "false",
                                      std::to_string(oc.acked_count)};
                    cluster->submit(std::move(ack_job), [](std::string ack_result) {
                        try {
                            auto ar = nlohmann::json::parse(ack_result);
                            if (!ar.value("ok", false)) {
                                spdlog::warn("TRANSACTION v2: terminal ack for a batch "
                                             "completed during replay failed: {}",
                                             ar.dump());
                            }
                        } catch (...) {
                            spdlog::warn("TRANSACTION v2: terminal ack for a batch "
                                         "completed during replay failed: {}",
                                         ack_result);
                        }
                    });
                }
            }

            nlohmann::json results = nlohmann::json::array();
            for (size_t i = 0; i < result_slots; i++) results.push_back(nullptr);
            for (const auto& e : echoes) {
                nlohmann::json rp = {{"index", e.index},
                                     {"type", "push"},
                                     {"success", true},
                                     {"transactionId", e.txn},
                                     {"messageId", e.mid},
                                     {"queueName", e.queue}};
                if (e.duplicate) rp["duplicate"] = true;
                results[e.index] = std::move(rp);
            }
            for (const auto& a : acks) {
                results[a.index] = {{"index", a.index},
                                    {"type", "ack"},
                                    {"success", true},
                                    {"transactionId", a.txn},
                                    {"error", nullptr},
                                    {"dlq", false}};
            }
            nlohmann::json out = {{"transactionId", txn_response_id},
                                  {"success", true},
                                  {"results", std::move(results)}};
            worker_response_registries[worker_id]->send_response_raw(
                request_id, out.dump(), 200);
        });
    });
    return true;
}

}  // namespace

void setup_transaction_routes(uWS::App* app, const RouteContext& ctx) {
    // ASYNC Transaction API (atomic operations)
    app->post("/api/v1/transaction", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for transactions
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.contains("operations") || !body["operations"].is_array()) {
                        send_error_response(res, "operations array required", 400);
                        return;
                    }
                    
                    auto operations = body["operations"];
                    if (operations.empty()) {
                        send_error_response(res, "operations array cannot be empty", 400);
                        return;
                    }

                    // STORAGE V2: transactions whose queues are ALL
                    // storage='segments' are served atomically by
                    // queen.seg_transaction_wire_v1 (returns true = response owned,
                    // success or rejection). Mixed rows/segments requests are
                    // rejected inside; pure-rows requests fall through to the
                    // v1 path untouched.
                    if (try_handle_transaction_v2(ctx, res, body)) {
                        return;
                    }

                    spdlog::info("[Worker {}] TRANSACTION: Executing transaction ({} operations)", ctx.worker_id, operations.size());
                    
                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );
                    
                    // Flatten and normalize operations for stored procedure
                    // Client sends: {type:"push", items:[{queue, payload}]}
                    // SP expects:   {type:"push", queue, payload, messageId, is_encrypted}
                    
                    // Get encryption service once
                    EncryptionService* enc_service = get_encryption_service();
                    
                    nlohmann::json ops_json = nlohmann::json::array();
                        for (const auto& op : operations) {
                        std::string op_type = op.value("type", "");
                        
                        if (op_type == "push" && op.contains("items")) {
                            // Flatten items array into individual push operations
                            for (const auto& item : op["items"]) {
                                std::string queue_name = item.value("queue", "");
                                nlohmann::json payload = item.value("payload", nlohmann::json::object());
                                
                                // Check if queue has encryption enabled
                                bool queue_encryption_enabled = false;
                                if (global_shared_state) {
                                    auto cached_config = global_shared_state->get_queue_config(queue_name);
                                    if (cached_config) {
                                        queue_encryption_enabled = cached_config->encryption_enabled;
                                    }
                                }
                                
                                // Encrypt payload if needed
                                nlohmann::json payload_to_store = payload;
                                bool is_encrypted = false;
                                
                                if (queue_encryption_enabled && enc_service && enc_service->is_enabled()) {
                                    std::string payload_str = payload.dump();
                                    auto encrypted = enc_service->encrypt_payload(payload_str);
                                    if (encrypted.has_value()) {
                                        payload_to_store = {
                                            {"encrypted", encrypted->encrypted},
                                            {"iv", encrypted->iv},
                                            {"authTag", encrypted->auth_tag}
                                        };
                                        is_encrypted = true;
                                        spdlog::info("[Worker {}] TRANSACTION: Encrypted payload for queue '{}'", 
                                                    ctx.worker_id, queue_name);
                                    }
                                }
                                
                                nlohmann::json flat_op = {
                                    {"type", "push"},
                                    {"queue", queue_name},
                                    {"partition", item.value("partition", "Default")},
                                    {"payload", payload_to_store},
                                    {"is_encrypted", is_encrypted},
                                    {"messageId", ctx.async_queue_manager->generate_uuid()}
                        };
                                if (item.contains("transactionId")) {
                                    flat_op["transactionId"] = item["transactionId"];
                        }
                                if (item.contains("traceId")) {
                                    flat_op["traceId"] = item["traceId"];
                                }
                                ops_json.push_back(flat_op);
                            }
                        } else if (op_type == "ack") {
                            // ACK operations are already in correct format
                            ops_json.push_back(op);
                        } else {
                            // Copy other operations as-is
                            ops_json.push_back(op);
                        }
                    }
                    
                    // Build Queen job request
                    queen::JobRequest job_req;
                    job_req.op_type = queen::JobType::TRANSACTION;
                    job_req.request_id = request_id;
                    job_req.params = {ops_json.dump()};
                    job_req.item_count = ops_json.size();
                    
                    // Capture context for callback
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;
                    
                    ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
                        worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                            nlohmann::json json_response;
                            int status_code = 200;
                            bool is_error = false;
                            
                            try {
                                json_response = nlohmann::json::parse(result);
                            } catch (const std::exception& e) {
                                json_response = {{"error", e.what()}};
                                status_code = 500;
                                is_error = true;
                            }
                            
                            worker_response_registries[worker_id]->send_response(
                                request_id, json_response, is_error, status_code);
                        });
                    });
                    
                    spdlog::debug("[Worker {}] TRANSACTION: Submitted {} ops (request_id={})", 
                                 ctx.worker_id, ops_json.size(), request_id);
                    
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
