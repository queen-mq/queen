// Storage v2 (segments) route handlers. Wire format v1 stays byte-compatible:
// clients cannot tell which engine served them. SQL side: q2.* wire wrappers
// in lib/schema/procedures/023_storage_v2.sql (blobs travel base64 through the
// text-mode libpq layer).
#include "queen/routes/storage_v2_routes.hpp"

#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/response_queue.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/storage_v2.hpp"
#include "queen.hpp"  // libqueen JobRequest
#include <spdlog/spdlog.h>

#include <algorithm>
#include <memory>

// External globals (declared in acceptor_server.cpp)
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;
}

namespace queen::routes_v2 {

namespace sv2 = queen::storage_v2;

bool is_segment_queue(const std::string& queue) {
    if (!global_shared_state) return false;
    auto cfg = global_shared_state->get_or_fetch_queue_config(queue);
    return cfg && cfg->storage == "segments";
}

// --------------------------------------------------------------------- push
void handle_push_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                    std::vector<PushItem> items) {
    // Group per (queue, partition) preserving arrival order (one segment per
    // group; cross-request fusion is a later optimization).
    struct Group {
        std::string queue, partition;
        std::vector<size_t> idx;                  // original item indices
        std::vector<sv2::FrameIn> frames;
        std::map<std::string, std::string> seen_txn;  // txn -> first message_id
    };
    std::vector<Group> groups;
    std::map<std::string, size_t> group_of;
    // Intra-batch duplicates (v1's dup_rank semantics: first occurrence wins)
    // are resolved BEFORE packing — the blob must not contain them.
    nlohmann::json pre_results = nlohmann::json::array();
    for (size_t i = 0; i < items.size(); i++) pre_results.push_back(nullptr);

    for (size_t i = 0; i < items.size(); i++) {
        auto& it = items[i];
        std::string key = it.queue + "\x1f" + it.partition;
        auto g = group_of.find(key);
        if (g == group_of.end()) {
            group_of[key] = groups.size();
            groups.push_back({it.queue, it.partition, {}, {}, {}});
            g = group_of.find(key);
        }
        auto& grp = groups[g->second];

        sv2::FrameIn f;
        f.message_id = ctx.async_queue_manager->generate_uuid();
        f.transaction_id = (it.transaction_id && !it.transaction_id->empty())
                               ? *it.transaction_id
                               : ctx.async_queue_manager->generate_uuid();

        auto seen = grp.seen_txn.find(f.transaction_id);
        if (seen != grp.seen_txn.end()) {
            pre_results[i] = {{"index", static_cast<int>(i)},
                              {"transaction_id", f.transaction_id},
                              {"status", "duplicate"},
                              {"message_id", seen->second},
                              {"queueName", it.queue}};
            continue;
        }
        grp.seen_txn[f.transaction_id] = f.message_id;

        if (it.trace_id && !it.trace_id->empty()) f.trace_id = *it.trace_id;
        if (it.producer_sub && !it.producer_sub->empty()) f.producer_sub = *it.producer_sub;
        f.payload = it.payload.dump();
        grp.idx.push_back(i);
        grp.frames.push_back(std::move(f));
    }
    // Drop groups emptied by intra-batch dedup.
    groups.erase(std::remove_if(groups.begin(), groups.end(),
                                [](const Group& g) { return g.frames.empty(); }),
                 groups.end());

    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);

    // Aggregation state: all callbacks land on this worker's loop (defer), so
    // no locking is needed.
    struct State {
        nlohmann::json results;
        size_t pending;
        bool failed = false;
        std::string error;
    };
    auto st = std::make_shared<State>();
    st->results = std::move(pre_results);  // intra-batch dups pre-filled
    st->pending = groups.size();

    if (groups.empty()) {  // everything was an intra-batch duplicate
        worker_response_registries[ctx.worker_id]->send_response_raw(
            request_id, st->results.dump(), 201);
        return;
    }

    auto worker_loop = ctx.worker_loop;
    auto worker_id = ctx.worker_id;
    auto shared_state = global_shared_state;

    for (auto& g : groups) {
        nlohmann::json metas = nlohmann::json::array();
        for (size_t k = 0; k < g.frames.size(); k++) {
            metas.push_back({{"i", static_cast<int>(k)},
                             {"mid", g.frames[k].message_id},
                             {"txn", g.frames[k].transaction_id}});
        }
        auto blob = sv2::zstd_compress(sv2::pack_frames(g.frames));
        std::string blob_b64 = sv2::b64_encode(blob.data(), blob.size());

        queen::JobRequest job;
        job.op_type = queen::JobType::CUSTOM;
        job.request_id = request_id;
        job.sql = "SELECT q2.push_segment_wire_v1($1, $2, $3::jsonb, $4, $5::int)";
        job.params = {g.queue, g.partition, metas.dump(), std::move(blob_b64),
                      std::to_string(g.frames.size())};

        // Copies needed by the callback to fill per-item results.
        auto g_idx = g.idx;
        std::vector<std::string> mids, txns;
        mids.reserve(g.frames.size());
        txns.reserve(g.frames.size());
        for (auto& f : g.frames) { mids.push_back(f.message_id); txns.push_back(f.transaction_id); }
        auto queue_name = g.queue;
        auto partition_name = g.partition;

        ctx.queen->submit(std::move(job), [worker_loop, worker_id, request_id, st,
                                           g_idx, mids, txns, queue_name, partition_name,
                                           shared_state](std::string result) {
            worker_loop->defer([=]() {
                try {
                    auto r = nlohmann::json::parse(result);
                    if (r.contains("error") && !r["error"].is_null()) {
                        st->failed = true;
                        st->error = r["error"].is_string() ? r["error"].get<std::string>() : r["error"].dump();
                    } else if (r.value("status", "") == "duplicate") {
                        // Frame-index -> original message id map from the wrapper.
                        std::map<int, std::string> orig;
                        for (auto& d : r["dups"]) orig[d["i"].get<int>()] = d.value("mid", "");
                        for (size_t k = 0; k < g_idx.size(); k++) {
                            bool dup = orig.count(static_cast<int>(k)) > 0;
                            st->results[g_idx[k]] = {
                                {"index", static_cast<int>(g_idx[k])},
                                {"transaction_id", txns[k]},
                                {"status", dup ? "duplicate" : "failed"},
                                {"message_id", dup ? orig[static_cast<int>(k)] : ""},
                                {"queueName", queue_name}};
                        }
                        // Slice-1: a mixed batch (some dup) is NOT auto-retried
                        // server-side; the non-dup items report failed and the
                        // client retries them (idempotent by transactionId).
                    } else {
                        for (size_t k = 0; k < g_idx.size(); k++) {
                            st->results[g_idx[k]] = {
                                {"index", static_cast<int>(g_idx[k])},
                                {"transaction_id", txns[k]},
                                {"status", "queued"},
                                {"message_id", mids[k]},
                                {"trace_id", nullptr},
                                {"queueName", queue_name}};
                        }
                        if (shared_state) {
                            shared_state->notify_message_available(queue_name, partition_name);
                        }
                    }
                } catch (const std::exception& e) {
                    st->failed = true;
                    st->error = e.what();
                }
                if (--st->pending == 0) {
                    if (st->failed) {
                        nlohmann::json err = {{"error", "segments push failed: " + st->error}};
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, err.dump(), 503);
                    } else {
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, st->results.dump(), 201);
                    }
                }
            });
        });
    }
}

// ---------------------------------------------------------------------- pop
void handle_pop_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                   const std::string& queue, const std::string& partition,
                   const std::string& consumer_group, int batch,
                   int lease_seconds, bool auto_ack) {
    std::string worker_id_str = ctx.async_queue_manager->generate_uuid();
    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);

    queen::JobRequest job;
    job.op_type = queen::JobType::CUSTOM;
    job.request_id = request_id;
    job.sql = "SELECT q2.pop_segments_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool)";
    job.params = {queue, partition, consumer_group, std::to_string(batch),
                  std::to_string(lease_seconds), worker_id_str,
                  auto_ack ? "true" : "false"};

    auto worker_loop = ctx.worker_loop;
    auto uws_worker = ctx.worker_id;

    ctx.queen->submit(std::move(job), [worker_loop, uws_worker, request_id, queue,
                                       partition, consumer_group, worker_id_str,
                                       auto_ack](std::string result) {
        worker_loop->defer([=]() {
            nlohmann::json out;
            int status_code = 200;
            try {
                auto r = nlohmann::json::parse(result);
                if (r.contains("error") && !r["error"].is_null()) {
                    out = {{"error", r["error"]}};
                    status_code = 500;
                } else {
                    std::string partition_id = r.value("partitionId", "");
                    nlohmann::json messages = nlohmann::json::array();
                    sv2::LeaseBatch lease;
                    lease.queue = queue;
                    lease.partition = partition;
                    lease.consumer_group = consumer_group;

                    for (auto& seg : r["segments"]) {
                        auto blob = sv2::b64_decode(seg["blob"].get<std::string>());
                        auto raw = sv2::zstd_decompress(blob);
                        std::vector<sv2::FrameOut> frames;
                        if (raw.empty() || !sv2::unpack_frames(raw, frames)) {
                            throw std::runtime_error("segment blob decode failed");
                        }
                        int64_t seq = seg["seq"].get<int64_t>();
                        int start = seg["startOff"].get<int>();
                        int take = seg["take"].get<int>();
                        std::string created_at = seg.value("createdAt", "");
                        for (int k = start; k < start + take &&
                                            k < static_cast<int>(frames.size()); k++) {
                            auto& f = frames[k];
                            nlohmann::json m = {
                                {"id", f.message_id},
                                {"transactionId", f.transaction_id},
                                {"traceId", f.trace_id ? nlohmann::json(*f.trace_id)
                                                       : nlohmann::json(nullptr)},
                                {"data", nlohmann::json::parse(f.payload, nullptr, false)},
                                {"producerSub", f.producer_sub
                                                    ? nlohmann::json(*f.producer_sub)
                                                    : nlohmann::json(nullptr)},
                                {"createdAt", created_at},
                                {"partitionId", partition_id},
                                {"partition", partition},
                                {"leaseId", auto_ack ? "" : worker_id_str},
                                {"consumerGroup", consumer_group}};
                            messages.push_back(std::move(m));
                            lease.positions.push_back({seq, k + 1, f.transaction_id});
                        }
                    }
                    if (!auto_ack && !lease.positions.empty()) {
                        lease.acked.assign(lease.positions.size(), false);
                        sv2::LeaseRegistry::instance().put(worker_id_str, std::move(lease));
                    }
                    if (messages.empty()) status_code = 204;
                    out = {{"success", true},
                           {"queue", queue},
                           {"partition", partition},
                           {"partitionId", partition_id},
                           {"leaseId", (auto_ack || messages.empty()) ? "" : worker_id_str},
                           {"consumerGroup", consumer_group},
                           {"messages", std::move(messages)}};
                }
            } catch (const std::exception& e) {
                out = {{"error", std::string("segments pop failed: ") + e.what()}};
                status_code = 500;
            }
            worker_response_registries[uws_worker]->send_response_raw(
                request_id, out.dump(), status_code);
        });
    });
}

// ---------------------------------------------------------------------- ack
bool try_handle_ack_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                       const nlohmann::json& acks) {
    // Route the batch as v2 only if the first ack's leaseId is a v2 lease.
    // Mixed v1/v2 batches are not supported in slice 1 (a lease never spans
    // engines, and clients ack one popped batch at a time).
    if (!acks.is_array() || acks.empty()) return false;
    std::string lease_id = acks[0].value("leaseId", "");
    if (lease_id.empty()) return false;

    auto& reg = sv2::LeaseRegistry::instance();
    nlohmann::json results = nlohmann::json::array();
    std::optional<sv2::LeaseRegistry::AckOutcome> final_outcome;
    bool any_known = false;

    for (auto& a : acks) {
        std::string txn = a.value("transactionId", "");
        std::string status = a.value("status", "completed");
        auto oc = reg.ack(lease_id, txn, status == "completed");
        if (!oc.known) {
            results.push_back({{"transactionId", txn},
                               {"status", "failed"},
                               {"error", "unknown lease or transaction"}});
            continue;
        }
        any_known = true;
        results.push_back({{"transactionId", txn}, {"status", status}});
        if (oc.complete) final_outcome = oc;
    }
    if (!any_known && !final_outcome) return false;  // not a v2 lease after all

    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);
    auto worker_loop = ctx.worker_loop;
    auto uws_worker = ctx.worker_id;

    if (!final_outcome) {
        // Batch not complete yet: cursor moves when the rest arrives.
        worker_response_registries[uws_worker]->send_response_raw(
            request_id, results.dump(), 200);
        return true;
    }

    const auto& oc = *final_outcome;
    queen::JobRequest job;
    job.op_type = queen::JobType::CUSTOM;
    job.request_id = request_id;
    job.sql = "SELECT q2.ack_segments_v1($1, $2, $3, $4, $5::bigint, $6::int, $7::bool, $8::int)";
    job.params = {oc.queue, oc.partition, oc.consumer_group, lease_id,
                  std::to_string(oc.upto_seq), std::to_string(oc.upto_off),
                  oc.ok ? "true" : "false", std::to_string(oc.acked_count)};

    ctx.queen->submit(std::move(job), [worker_loop, uws_worker, request_id,
                                       results](std::string result) {
        worker_loop->defer([=]() {
            int status_code = 200;
            nlohmann::json out = results;
            try {
                auto r = nlohmann::json::parse(result);
                if (!r.value("ok", false)) {
                    out = {{"error", r.value("error", "ack failed")}};
                    status_code = 409;
                }
            } catch (const std::exception& e) {
                out = {{"error", e.what()}};
                status_code = 500;
            }
            worker_response_registries[uws_worker]->send_response_raw(
                request_id, out.dump(), status_code);
        });
    });
    return true;
}

}  // namespace queen::routes_v2
