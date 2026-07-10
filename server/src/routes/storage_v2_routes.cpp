// Storage v2 (segments) route handlers. Wire format v1 stays byte-compatible:
// clients cannot tell which engine served them. SQL side: q2.* wire wrappers
// in lib/schema/procedures/023_storage_v2.sql + 024 (wildcard/attempt) + 025
// (DLQ) — blobs travel base64 through the text-mode libpq layer.
#include "queen/routes/storage_v2_routes.hpp"

#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/encryption.hpp"
#include "queen/response_queue.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/file_buffer.hpp"
#include "queen/storage_v2.hpp"
#include "queen.hpp"  // libqueen JobRequest
#include <libusockets.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

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

// ===================================================================== helpers
namespace {

// v2 long-poll re-check interval (see park_pop for why polling, not notify).
constexpr int POP_WAIT_POLL_MS = 100;

// Decrypt one frame payload envelope ({"encrypted","iv","authTag"}). Returns
// the plaintext JSON, or the envelope unchanged when the service is off or
// rejects the ciphertext — the same degraded behavior as v1's
// pop_decrypt_messages (client sees the envelope as `data`).
std::string decrypt_frame_payload(const std::string& envelope_json) {
    EncryptionService* enc = get_encryption_service();
    if (!enc || !enc->is_enabled()) return envelope_json;
    auto env = nlohmann::json::parse(envelope_json, nullptr, false);
    if (env.is_discarded() || !env.is_object() || !env.contains("encrypted") ||
        !env.contains("iv") || !env.contains("authTag")) {
        return envelope_json;
    }
    try {
        EncryptionService::EncryptedData data{env["encrypted"].get<std::string>(),
                                              env["iv"].get<std::string>(),
                                              env["authTag"].get<std::string>()};
        auto plain = enc->decrypt_payload(data);
        if (plain.has_value()) return *plain;
    } catch (...) {}
    return envelope_json;
}

// First delivered frame of a batch — the poison-head snapshot q2.dlq_head_v1
// needs (position + identity + decrypted payload).
struct FirstFrame {
    bool valid = false;
    int64_t seq = 0;
    int frame_idx = 0;
    std::string message_id;
    std::string txn;
    std::string payload;  // plaintext JSON text
};

// Decompress + slice one partition's wire segments into v1 messages and lease
// positions. Shared by the specific and wildcard pop paths. `lease` may be
// null (auto-ack), `first` may be null when the poison-head snapshot is not
// needed. Throws on blob decode failure.
void slice_partition_segments(const nlohmann::json& segments,
                              const std::string& partition_name,
                              const std::string& partition_id,
                              const std::string& consumer_group,
                              const std::string& lease_field,
                              nlohmann::json& messages,
                              sv2::LeaseBatch* lease,
                              FirstFrame* first) {
    for (const auto& seg : segments) {
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
            std::string payload =
                f.encrypted ? decrypt_frame_payload(f.payload) : f.payload;
            if (first && !first->valid) {
                first->valid = true;
                first->seq = seq;
                first->frame_idx = k;
                first->message_id = f.message_id;
                first->txn = f.transaction_id;
                first->payload = payload;
            }
            nlohmann::json m = {
                {"id", f.message_id},
                {"transactionId", f.transaction_id},
                {"traceId", f.trace_id ? nlohmann::json(*f.trace_id)
                                       : nlohmann::json(nullptr)},
                {"data", nlohmann::json::parse(payload, nullptr, false)},
                {"producerSub", f.producer_sub ? nlohmann::json(*f.producer_sub)
                                               : nlohmann::json(nullptr)},
                {"createdAt", created_at},
                {"partitionId", partition_id},
                {"partition", partition_name},
                {"leaseId", lease_field},
                {"consumerGroup", consumer_group}};
            messages.push_back(std::move(m));
            if (lease) lease->positions.push_back({seq, k + 1, f.transaction_id});
        }
    }
}

}  // namespace

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
    nlohmann::json failover_items = nlohmann::json::array();

    // Per-queue encryption (v1 parity: see push.cpp). Config lookups are
    // cache hits, memoized per request anyway.
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

        // Encrypt the frame payload when the queue asks for it: the envelope
        // replaces the payload and frame flag bit2 tells the pop path to
        // decrypt. Failures degrade to plaintext with a warning (v1 parity).
        if (queue_wants_encryption(it.queue)) {
            if (enc_ready) {
                auto encd = enc_service->encrypt_payload(f.payload);
                if (encd.has_value()) {
                    f.payload = nlohmann::json{{"encrypted", encd->encrypted},
                                               {"iv", encd->iv},
                                               {"authTag", encd->auth_tag}}.dump();
                    f.encrypted = true;
                } else {
                    spdlog::warn("[Worker {}] PUSH v2: Encryption failed for queue '{}', storing plaintext",
                                 ctx.worker_id, it.queue);
                }
            } else {
                spdlog::warn("[Worker {}] PUSH v2: Queue '{}' requires encryption but service not available",
                             ctx.worker_id, it.queue);
            }
        }

        grp.idx.push_back(i);
        // Failover snapshot: ORIGINAL payload (pre-encryption) + the generated
        // transactionId, in the same event shape finish_push_submit buffers,
        // so replay (push_messages_internal) routes it by the current flag.
        {
            nlohmann::json ev = {{"queue", it.queue},
                                 {"partition", it.partition},
                                 {"payload", it.payload},
                                 {"transactionId", f.transaction_id}};
            if (it.trace_id && !it.trace_id->empty()) ev["traceId"] = *it.trace_id;
            if (it.producer_sub && !it.producer_sub->empty()) ev["producerSub"] = *it.producer_sub;
            failover_items.push_back(std::move(ev));
        }
        grp.frames.push_back(std::move(f));
    }
    // Drop groups emptied by intra-batch dedup.
    groups.erase(std::remove_if(groups.begin(), groups.end(),
                                [](const Group& g) { return g.frames.empty(); }),
                 groups.end());

    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);

    // DB-error -> file-buffer failover parity with v1 (finish_push_submit).
    if (ctx.push_failover_storage) {
        ctx.push_failover_storage->store(request_id, failover_items.dump());
    }

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

        auto push_failover_storage = ctx.push_failover_storage;
        auto file_buffer = ctx.file_buffer;
        ctx.queen->submit(std::move(job), [worker_loop, worker_id, request_id, st,
                                           g_idx, mids, txns, queue_name, partition_name,
                                           shared_state, push_failover_storage,
                                           file_buffer](std::string result) {
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
                        // DB error: v1-parity failover — buffer the whole batch
                        // to the file buffer and answer 'buffered' (replay
                        // routes by the queue's CURRENT storage flag).
                        if (push_failover_storage && file_buffer) {
                            auto stored = push_failover_storage->retrieve_and_remove(request_id);
                            if (stored.has_value()) {
                                try {
                                    auto evs = nlohmann::json::parse(stored.value());
                                    nlohmann::json results = nlohmann::json::array();
                                    bool all_buffered = true;
                                    for (auto& ev : evs) {
                                        ev["failover"] = true;
                                        if (file_buffer->write_event(ev)) {
                                            results.push_back({{"status", "buffered"},
                                                               {"queue", ev.value("queue", "")},
                                                               {"partition", ev.value("partition", "")},
                                                               {"transactionId", ev.value("transactionId", "")}});
                                        } else {
                                            all_buffered = false;
                                            results.push_back({{"status", "failed"},
                                                               {"error", "File buffer write failed"}});
                                        }
                                    }
                                    spdlog::warn("segments push DB error -> buffered {} items: {}",
                                                 evs.size(), st->error);
                                    worker_response_registries[worker_id]->send_response_raw(
                                        request_id, results.dump(), all_buffered ? 201 : 500);
                                    return;
                                } catch (const std::exception& e) {
                                    spdlog::error("segments push failover failed: {}", e.what());
                                }
                            }
                        }
                        nlohmann::json err = {{"error", "segments push failed: " + st->error}};
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, err.dump(), 503);
                    } else {
                        if (push_failover_storage) {
                            push_failover_storage->remove(request_id);
                        }
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, st->results.dump(), 201);
                    }
                }
            });
        });
    }
}

// ---------------------------------------------------------------------- pop
namespace {

// One in-flight v2 pop request. Everything is touched only on the owning
// worker's loop thread (route handler, defer callbacks, loop timers), except
// `aborted` which the response registry's abort hook flips (also loop thread;
// atomic keeps it self-evidently safe).
struct PopWaitJob {
    // dispatch plumbing
    QueenCluster* cluster = nullptr;
    uWS::Loop* loop = nullptr;
    int uws_worker = 0;
    std::string request_id;
    // query
    bool wildcard = false;
    std::string queue, partition, group;
    std::string lease_worker;  // q2 worker id; doubles as the wire leaseId
    int batch = 1;
    int lease_seconds = 300;
    int max_partitions = 1;
    bool auto_ack = false;
    // long-poll
    bool wait = false;
    std::chrono::steady_clock::time_point deadline{};
    std::shared_ptr<std::atomic<bool>> aborted =
        std::make_shared<std::atomic<bool>>(false);
    us_timer_t* timer = nullptr;  // parked-wait rearm timer (loop thread only)
    // poison handling (specific-partition pops only; the wildcard wire does
    // not carry `attempt` in slice 2)
    int retry_limit = 3;
    bool dlq_enabled = false;
    bool dlq_retried = false;  // at most one DLQ eviction + re-pop per request
};

void submit_pop(const std::shared_ptr<PopWaitJob>& pj);

void close_pop_timer(PopWaitJob& pj) {
    if (!pj.timer) return;
    auto* self = static_cast<std::shared_ptr<PopWaitJob>*>(us_timer_ext(pj.timer));
    us_timer_t* t = pj.timer;
    pj.timer = nullptr;
    // Release the self-reference BEFORE the close frees the timer memory.
    self->~shared_ptr();
    us_timer_close(t);
}

void finish_pop(const std::shared_ptr<PopWaitJob>& pj, std::string body, int status) {
    close_pop_timer(*pj);
    worker_response_registries[pj->uws_worker]->send_response_raw(
        pj->request_id, std::move(body), status);
}

void pop_wait_timer_cb(us_timer_t* t) {
    auto pj = *static_cast<std::shared_ptr<PopWaitJob>*>(us_timer_ext(t));
    if (pj->aborted->load()) {  // client went away while parked: stop polling
        close_pop_timer(*pj);
        return;
    }
    submit_pop(pj);
}

// Long-poll parking. v1's wait lives inside libqueen: empty JobType::POP jobs
// with a wait_deadline are held in a backoff tracker and re-promoted either on
// next_check expiry or when notify_message_available (local push / peer UDP)
// resets their backoff. That machinery is keyed to POP jobs and v1 SQL; v2
// pops are CUSTOM jobs, so hooking it would mean invasive libqueen changes.
// Instead a parked v2 pop re-runs its SQL every POP_WAIT_POLL_MS on a
// one-shot loop timer until the deadline — correct, self-contained, worst
// case one poll interval of extra delivery latency.
void park_pop(const std::shared_ptr<PopWaitJob>& pj) {
    if (!pj->timer) {
        // fallthrough MUST be 0: uSockets' us_timer_close decrements the
        // loop's poll count unconditionally, but us_create_timer only
        // increments it when !fallthrough — a fallthrough timer would
        // underflow num_polls on close and make the worker loop exit.
        pj->timer = us_create_timer(reinterpret_cast<us_loop_t*>(pj->loop), 0,
                                    sizeof(std::shared_ptr<PopWaitJob>));
        new (us_timer_ext(pj->timer)) std::shared_ptr<PopWaitJob>(pj);
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                         pj->deadline - std::chrono::steady_clock::now()).count();
    int delay = static_cast<int>(std::clamp<long long>(
        remaining, 1, static_cast<long long>(POP_WAIT_POLL_MS)));
    us_timer_set(pj->timer, pop_wait_timer_cb, delay, 0);  // one-shot; rearmed on next empty result
}

// Serve a specific-partition wire result: slice, register the lease, respond.
void serve_pop_specific(const std::shared_ptr<PopWaitJob>& pj, const nlohmann::json& r) {
    std::string partition_id = r.value("partitionId", "");
    nlohmann::json messages = nlohmann::json::array();
    sv2::LeaseBatch lease;
    lease.queue = pj->queue;
    lease.partition = pj->partition;
    lease.consumer_group = pj->group;

    slice_partition_segments(r["segments"], pj->partition, partition_id,
                             pj->group, pj->auto_ack ? "" : pj->lease_worker,
                             messages, pj->auto_ack ? nullptr : &lease, nullptr);

    if (!pj->auto_ack && !lease.positions.empty()) {
        sv2::LeaseRegistry::instance().put(pj->lease_worker, partition_id,
                                           std::move(lease));
    }
    bool empty = messages.empty();
    nlohmann::json out = {{"success", true},
                          {"queue", pj->queue},
                          {"partition", pj->partition},
                          {"partitionId", partition_id},
                          {"leaseId", (pj->auto_ack || empty) ? "" : pj->lease_worker},
                          {"consumerGroup", pj->group},
                          {"messages", std::move(messages)}};
    finish_pop(pj, out.dump(), empty ? 204 : 200);
}

// Serve a wildcard wire result: slice every claimed partition into one flat
// messages[], register per-partition batches under the shared leaseId, and
// mirror pop_unified_batch_v4's multi-partition shape (top-level
// partition/partitionId reflect the FIRST claimed partition; per-message
// fields are authoritative).
void serve_pop_wildcard(const std::shared_ptr<PopWaitJob>& pj, const nlohmann::json& r) {
    const auto& partitions = r["partitions"];
    nlohmann::json messages = nlohmann::json::array();
    std::string first_name, first_id;
    bool first_set = false;
    // Slice everything first: a decode failure must not leave a partial
    // registry registration behind.
    std::vector<std::pair<std::string, sv2::LeaseBatch>> batches;
    for (const auto& p : partitions) {
        std::string pname = p.value("partition", "");
        std::string pid = p.value("partitionId", "");
        if (!first_set) { first_name = pname; first_id = pid; first_set = true; }
        sv2::LeaseBatch lease;
        lease.queue = pj->queue;
        lease.partition = pname;
        lease.consumer_group = pj->group;
        slice_partition_segments(p["segments"], pname, pid, pj->group,
                                 pj->auto_ack ? "" : pj->lease_worker,
                                 messages, pj->auto_ack ? nullptr : &lease, nullptr);
        if (!pj->auto_ack && !lease.positions.empty()) {
            batches.emplace_back(pid, std::move(lease));
        }
    }
    for (auto& b : batches) {
        sv2::LeaseRegistry::instance().put(pj->lease_worker, b.first,
                                           std::move(b.second));
    }
    bool empty = messages.empty();
    nlohmann::json out = {{"success", true},
                          {"queue", pj->queue},
                          {"partition", first_name},
                          {"partitionId", first_id},
                          {"leaseId", (pj->auto_ack || empty) ? "" : pj->lease_worker},
                          {"consumerGroup", pj->group},
                          {"messages", std::move(messages)},
                          {"partitionsClaimed", partitions.size()}};
    finish_pop(pj, out.dump(), empty ? 204 : 200);
}

// Poison head: the delivered batch's attempt count exceeded the queue's retry
// limit and the queue dead-letters. Snapshot the FIRST delivered frame
// (decrypted), hand it to q2.dlq_head_v1 (which files it, advances the cursor
// past the frame and releases the lease), then re-run the pop once — the
// fresh batch starts after the poison message. dlq_retried guards the single
// re-pop per request: if the next head is poisoned too it is served as-is and
// the NEXT pop request evicts it (progress guaranteed, no loops).
void dlq_poison_head(const std::shared_ptr<PopWaitJob>& pj,
                     std::shared_ptr<nlohmann::json> r, int attempt) {
    pj->dlq_retried = true;
    FirstFrame first;
    try {
        nlohmann::json head_only = nlohmann::json::array();
        head_only.push_back((*r)["segments"][0]);
        nlohmann::json scratch = nlohmann::json::array();
        slice_partition_segments(head_only, pj->partition, "", pj->group, "",
                                 scratch, nullptr, &first);
    } catch (...) {
        first.valid = false;
    }
    if (!first.valid) {
        // Cannot extract the head (corrupt blob would fail the serve path too,
        // with the same 500 the client would have seen without DLQ handling).
        serve_pop_specific(pj, *r);
        return;
    }

    queen::JobRequest job;
    job.op_type = queen::JobType::CUSTOM;
    job.request_id = pj->request_id;
    job.sql = "SELECT q2.dlq_head_v1($1::uuid, $2, $3, $4::bigint, $5::int, $6::uuid, $7, $8::jsonb, $9)";
    job.params = {r->value("partitionId", ""), pj->group, pj->lease_worker,
                  std::to_string(first.seq), std::to_string(first.frame_idx),
                  first.message_id, first.txn, first.payload,
                  "Retries exhausted (attempt " + std::to_string(attempt) +
                      " > retryLimit " + std::to_string(pj->retry_limit) + ")"};

    auto loop = pj->loop;
    pj->cluster->submit(std::move(job), [pj, r, loop](std::string result) {
        loop->defer([pj, r, result = std::move(result)]() {
            bool ok = false;
            try {
                auto dr = nlohmann::json::parse(result);
                ok = dr.value("ok", false);
            } catch (...) {}
            if (ok) {
                submit_pop(pj);  // cursor moved past the poison head
                return;
            }
            // DLQ refused (lease raced/expired or SQL error): serve the
            // original batch; attempts keep rising and a later pop retries
            // the eviction.
            try {
                serve_pop_specific(pj, *r);
            } catch (const std::exception& e) {
                finish_pop(pj, nlohmann::json{{"error",
                    std::string("segments pop failed: ") + e.what()}}.dump(), 500);
            }
        });
    });
}

void handle_pop_wire_result(const std::shared_ptr<PopWaitJob>& pj,
                            const std::string& result) {
    try {
        auto r = nlohmann::json::parse(result);
        if (r.contains("error") && !r["error"].is_null()) {
            finish_pop(pj, nlohmann::json{{"error", r["error"]}}.dump(), 500);
            return;
        }
        bool empty = pj->wildcard
                         ? (!r.contains("partitions") || r["partitions"].empty())
                         : (!r.contains("segments") || r["segments"].empty());
        if (empty) {
            if (pj->wait && !pj->aborted->load() &&
                std::chrono::steady_clock::now() < pj->deadline) {
                park_pop(pj);
                return;
            }
            if (pj->wildcard) {
                // v4 wildcard empty shape (pop_unified_batch_v4 parity).
                nlohmann::json out = {{"success", false},
                                      {"error", "no_available_partition"},
                                      {"messages", nlohmann::json::array()}};
                finish_pop(pj, out.dump(), 204);
            } else {
                nlohmann::json out = {{"success", true},
                                      {"queue", pj->queue},
                                      {"partition", pj->partition},
                                      {"partitionId", r.value("partitionId", "")},
                                      {"leaseId", ""},
                                      {"consumerGroup", pj->group},
                                      {"messages", nlohmann::json::array()}};
                finish_pop(pj, out.dump(), 204);
            }
            return;
        }
        if (pj->wildcard) {
            serve_pop_wildcard(pj, r);
        } else {
            int attempt = r.value("attempt", 0);  // 0 on auto-ack by contract
            if (!pj->auto_ack && pj->dlq_enabled && !pj->dlq_retried &&
                attempt > pj->retry_limit) {
                dlq_poison_head(pj, std::make_shared<nlohmann::json>(std::move(r)),
                                attempt);
                return;
            }
            serve_pop_specific(pj, r);
        }
    } catch (const std::exception& e) {
        finish_pop(pj, nlohmann::json{{"error",
            std::string("segments pop failed: ") + e.what()}}.dump(), 500);
    }
}

void submit_pop(const std::shared_ptr<PopWaitJob>& pj) {
    queen::JobRequest job;
    job.op_type = queen::JobType::CUSTOM;
    job.request_id = pj->request_id;
    if (pj->wildcard) {
        job.sql = "SELECT q2.pop_wildcard_wire_v1($1, $2, $3::int, $4::int, $5, $6::bool, $7::int)";
        job.params = {pj->queue, pj->group, std::to_string(pj->batch),
                      std::to_string(pj->lease_seconds), pj->lease_worker,
                      pj->auto_ack ? "true" : "false",
                      std::to_string(pj->max_partitions)};
    } else {
        job.sql = "SELECT q2.pop_segments_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool)";
        job.params = {pj->queue, pj->partition, pj->group, std::to_string(pj->batch),
                      std::to_string(pj->lease_seconds), pj->lease_worker,
                      pj->auto_ack ? "true" : "false"};
    }
    auto loop = pj->loop;
    pj->cluster->submit(std::move(job), [pj, loop](std::string result) {
        loop->defer([pj, result = std::move(result)]() {
            handle_pop_wire_result(pj, result);
        });
    });
}

std::shared_ptr<PopWaitJob> make_pop_job(const RouteContext& ctx,
                                         uWS::HttpResponse<false>* res,
                                         const std::string& queue,
                                         const std::string& consumer_group,
                                         int batch, int lease_seconds,
                                         bool auto_ack, bool wait, int timeout_ms) {
    auto pj = std::make_shared<PopWaitJob>();
    pj->cluster = ctx.queen;
    pj->loop = ctx.worker_loop;
    pj->uws_worker = ctx.worker_id;
    pj->queue = queue;
    pj->group = consumer_group;
    pj->batch = batch;
    pj->lease_seconds = lease_seconds;
    pj->auto_ack = auto_ack;
    pj->lease_worker = ctx.async_queue_manager->generate_uuid();
    pj->wait = wait && timeout_ms > 0;
    if (pj->wait) {
        pj->deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    }
    if (global_shared_state) {
        auto cfg = global_shared_state->get_or_fetch_queue_config(queue);
        if (cfg) {
            pj->retry_limit = cfg->retry_limit;
            pj->dlq_enabled = cfg->dlq_enabled || cfg->dlq_after_max_retries;
        }
    }
    // Waiting pops watch for client aborts so parked timers stop polling a
    // dead connection (mirrors v1's invalidate_request-on-abort).
    auto aborted = pj->aborted;
    pj->request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id,
        pj->wait ? ResponseRegistry::AbortCallback(
                       [aborted](const std::string&) { aborted->store(true); })
                 : ResponseRegistry::AbortCallback(nullptr));
    return pj;
}

}  // namespace

void handle_pop_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                   const std::string& queue, const std::string& partition,
                   const std::string& consumer_group, int batch,
                   int lease_seconds, bool auto_ack, bool wait, int timeout_ms) {
    auto pj = make_pop_job(ctx, res, queue, consumer_group, batch, lease_seconds,
                           auto_ack, wait, timeout_ms);
    pj->partition = partition;
    submit_pop(pj);
}

void handle_pop_wildcard_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                            const std::string& queue,
                            const std::string& consumer_group, int batch,
                            int lease_seconds, bool auto_ack, int max_partitions,
                            bool wait, int timeout_ms) {
    auto pj = make_pop_job(ctx, res, queue, consumer_group, batch, lease_seconds,
                           auto_ack, wait, timeout_ms);
    pj->wildcard = true;
    pj->max_partitions = max_partitions;
    submit_pop(pj);
}

// ---------------------------------------------------------------------- ack
namespace {

// partitionId -> "belongs to q2" cache, feeding the cross-process ack
// fallback. Advisory only (uuids never migrate between engines, so entries
// cannot go stale the dangerous way); wiped wholesale when full rather than
// tracking LRU order.
class Q2PartitionCache {
public:
    std::optional<bool> get(const std::string& pid) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = m_.find(pid);
        if (it == m_.end()) return std::nullopt;
        return it->second;
    }
    void put(const std::string& pid, bool is_q2) {
        std::lock_guard<std::mutex> g(mu_);
        if (m_.size() >= kMax) m_.clear();
        m_[pid] = is_q2;
    }

private:
    static constexpr size_t kMax = 65536;
    std::mutex mu_;
    std::unordered_map<std::string, bool> m_;
};

Q2PartitionCache& q2_partition_cache() {
    static Q2PartitionCache c;
    return c;
}

// v1 passthrough used when the async membership probe says "rows engine":
// by then the response is already owned here, so the same JobType::ACK job
// ack.cpp would have built is dispatched from this side (identical wire).
void dispatch_ack_v1(QueenCluster* cluster, uWS::Loop* loop, int uws_worker,
                     const std::string& request_id, const nlohmann::json& acks) {
    queen::JobRequest job;
    job.op_type = queen::JobType::ACK;
    job.request_id = request_id;
    job.params = {acks.dump()};
    job.item_count = acks.size();
    cluster->submit(std::move(job), [loop, uws_worker, request_id](std::string result) {
        loop->defer([result = std::move(result), uws_worker, request_id]() {
            nlohmann::json out;
            int status_code = 200;
            bool is_error = false;
            try {
                out = nlohmann::json::parse(result);
            } catch (const std::exception& e) {
                out = {{"error", e.what()}};
                status_code = 500;
                is_error = true;
            }
            worker_response_registries[uws_worker]->send_response(
                request_id, out, is_error, status_code);
        });
    });
}

// Cross-process / post-restart v2 ack: no registry entry, so positions are
// resolved in SQL via the q2.dedup txn index. One q2.ack_by_txn_v1 call per
// partition (a wildcard lease spans partitions under one leaseId).
void dispatch_ack_by_txn(QueenCluster* cluster, uWS::Loop* loop, int uws_worker,
                         const std::string& request_id, const nlohmann::json& acks,
                         const std::string& lease_id) {
    struct Part {
        std::string group;
        nlohmann::json txns = nlohmann::json::array();  // [{"txn","ok"}]
        std::vector<std::pair<std::string, std::string>> items;  // txn -> status
    };
    std::map<std::string, Part> parts;
    for (const auto& a : acks) {
        auto& p = parts[a.value("partitionId", "")];
        p.group = a.value("consumerGroup", "__QUEUE_MODE__");
        std::string txn = a.value("transactionId", "");
        std::string status = a.value("status", "completed");
        p.txns.push_back({{"txn", txn}, {"ok", status == "completed"}});
        p.items.emplace_back(txn, status);
    }

    struct State {
        nlohmann::json results = nlohmann::json::array();
        size_t pending = 0;
    };
    auto st = std::make_shared<State>();
    st->pending = parts.size();

    for (auto& kv : parts) {
        queen::JobRequest job;
        job.op_type = queen::JobType::CUSTOM;
        job.request_id = request_id;
        job.sql = "SELECT q2.ack_by_txn_v1($1::uuid, $2, $3, $4::jsonb)";
        job.params = {kv.first, kv.second.group, lease_id, kv.second.txns.dump()};

        auto items = kv.second.items;
        cluster->submit(std::move(job), [loop, uws_worker, request_id, st,
                                         items](std::string result) {
            loop->defer([=]() {
                bool ok = false;
                std::string err;
                try {
                    auto r = nlohmann::json::parse(result);
                    ok = r.value("ok", false);
                    if (!ok) {
                        err = r.contains("error") && r["error"].is_string()
                                  ? r["error"].get<std::string>() : "ack failed";
                    }
                } catch (const std::exception& e) {
                    err = e.what();
                }
                for (const auto& it : items) {
                    if (ok) {
                        st->results.push_back({{"transactionId", it.first},
                                               {"status", it.second}});
                    } else {
                        st->results.push_back({{"transactionId", it.first},
                                               {"status", "failed"},
                                               {"error", err}});
                    }
                }
                if (--st->pending == 0) {
                    worker_response_registries[uws_worker]->send_response_raw(
                        request_id, st->results.dump(), 200);
                }
            });
        });
    }
}

// Registry missed the lease entirely: v1 ack (common), or a v2 lease held by
// another process / lost to a restart. Route on q2 partition membership.
bool try_handle_ack_v2_fallback(const queen::routes::RouteContext& ctx,
                                uWS::HttpResponse<false>* res,
                                const nlohmann::json& acks,
                                const std::string& lease_id) {
    std::string pid = acks[0].value("partitionId", "");
    uint8_t scratch[16];
    if (!sv2::uuid_to_bytes(pid, scratch)) return false;  // not a uuid: not q2
    auto cached = q2_partition_cache().get(pid);
    if (cached.has_value() && !*cached) return false;  // known rows partition

    // From here on this side owns the response.
    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);
    auto cluster = ctx.queen;
    auto loop = ctx.worker_loop;
    int uws_worker = ctx.worker_id;

    if (cached.has_value()) {  // known q2 partition: straight to the resolver
        dispatch_ack_by_txn(cluster, loop, uws_worker, request_id, acks, lease_id);
        return true;
    }

    // Membership unknown: one tiny PK probe per unseen partitionId, cached
    // both ways. to_jsonb keeps the CUSTOM result a clean JSON boolean.
    queen::JobRequest probe;
    probe.op_type = queen::JobType::CUSTOM;
    probe.request_id = request_id;
    probe.sql = "SELECT to_jsonb(EXISTS(SELECT 1 FROM q2.partitions WHERE id = $1::uuid))";
    probe.params = {pid};

    nlohmann::json acks_copy = acks;
    cluster->submit(std::move(probe), [cluster, loop, uws_worker, request_id,
                                       acks_copy, lease_id, pid](std::string result) {
        loop->defer([=]() {
            bool known = false, is_q2 = false;
            try {
                auto r = nlohmann::json::parse(result);
                if (r.is_boolean()) {
                    known = true;
                    is_q2 = r.get<bool>();
                }
            } catch (...) {}
            if (!known) {
                // Probe failed (DB error): fail the ack instead of guessing
                // an engine. Client retries; leases redeliver on expiry.
                worker_response_registries[uws_worker]->send_response_raw(
                    request_id,
                    nlohmann::json{{"error", "ack engine probe failed"}}.dump(), 503);
                return;
            }
            q2_partition_cache().put(pid, is_q2);
            if (is_q2) {
                dispatch_ack_by_txn(cluster, loop, uws_worker, request_id,
                                    acks_copy, lease_id);
            } else {
                dispatch_ack_v1(cluster, loop, uws_worker, request_id, acks_copy);
            }
        });
    });
    return true;
}

}  // namespace

bool try_handle_ack_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                       const nlohmann::json& acks) {
    // Route the batch as v2 only if the first ack's leaseId is a v2 lease.
    // Mixed v1/v2 batches are not supported (a lease never spans engines, and
    // clients ack one popped batch at a time).
    if (!acks.is_array() || acks.empty()) return false;
    std::string lease_id = acks[0].value("leaseId", "");
    if (lease_id.empty()) return false;

    auto& reg = sv2::LeaseRegistry::instance();
    nlohmann::json results = nlohmann::json::array();
    std::vector<sv2::LeaseRegistry::AckOutcome> outcomes;
    bool any_known = false;

    for (const auto& a : acks) {
        std::string txn = a.value("transactionId", "");
        std::string pid = a.value("partitionId", "");
        std::string status = a.value("status", "completed");
        auto oc = reg.ack(lease_id, pid, txn, status == "completed");
        if (!oc.known) {
            results.push_back({{"transactionId", txn},
                               {"status", "failed"},
                               {"error", "unknown lease or transaction"}});
            continue;
        }
        any_known = true;
        results.push_back({{"transactionId", txn}, {"status", status}});
        if (oc.complete) outcomes.push_back(std::move(oc));
    }
    if (!any_known) {
        // Not in this process's registry: either a v1 ack or a v2 lease from
        // another broker / before a restart. The fallback decides by q2
        // partition membership (and may take ownership of the response).
        return try_handle_ack_v2_fallback(ctx, res, acks, lease_id);
    }

    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);
    auto worker_loop = ctx.worker_loop;
    auto uws_worker = ctx.worker_id;

    if (outcomes.empty()) {
        // No partition batch completed yet: cursors move when the rest arrives.
        worker_response_registries[uws_worker]->send_response_raw(
            request_id, results.dump(), 200);
        return true;
    }

    // One q2.ack_segments_v1 per completed partition (a wildcard lease can
    // complete several at once). Callbacks all land on this worker's loop.
    struct State {
        nlohmann::json results;
        size_t pending = 0;
        bool failed = false;
        int status_code = 200;
        std::string error;
    };
    auto st = std::make_shared<State>();
    st->results = std::move(results);
    st->pending = outcomes.size();

    for (const auto& oc : outcomes) {
        queen::JobRequest job;
        job.op_type = queen::JobType::CUSTOM;
        job.request_id = request_id;
        job.sql = "SELECT q2.ack_segments_v1($1, $2, $3, $4, $5::bigint, $6::int, $7::bool, $8::int)";
        job.params = {oc.queue, oc.partition, oc.consumer_group, lease_id,
                      std::to_string(oc.upto_seq), std::to_string(oc.upto_off),
                      oc.ok ? "true" : "false", std::to_string(oc.acked_count)};

        ctx.queen->submit(std::move(job), [worker_loop, uws_worker, request_id,
                                           st](std::string result) {
            worker_loop->defer([=]() {
                try {
                    auto r = nlohmann::json::parse(result);
                    if (!r.value("ok", false) && !st->failed) {
                        st->failed = true;
                        st->status_code = 409;
                        st->error = r.contains("error") && r["error"].is_string()
                                        ? r["error"].get<std::string>() : "ack failed";
                    }
                } catch (const std::exception& e) {
                    if (!st->failed) {
                        st->failed = true;
                        st->status_code = 500;
                        st->error = e.what();
                    }
                }
                if (--st->pending == 0) {
                    if (st->failed) {
                        worker_response_registries[uws_worker]->send_response_raw(
                            request_id, nlohmann::json{{"error", st->error}}.dump(),
                            st->status_code);
                    } else {
                        worker_response_registries[uws_worker]->send_response_raw(
                            request_id, st->results.dump(), 200);
                    }
                }
            });
        });
    }
    return true;
}

}  // namespace queen::routes_v2
