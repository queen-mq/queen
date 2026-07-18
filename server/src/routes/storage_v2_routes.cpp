// Storage v2 (segments) route handlers. Wire format v1 stays byte-compatible:
// clients cannot tell which engine served them. SQL side: queen.seg_* wire wrappers
// in lib/schema/procedures/023_storage_v2.sql + 024 (wildcard/attempt) + 025
// (DLQ) — blobs travel base64 through the text-mode libpq layer.
#include "queen/routes/storage_v2_routes.hpp"

#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/config.hpp"  // get_env_int (fusion knobs)
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

// First delivered frame of a batch — the poison-head snapshot queen.seg_dlq_head_v1
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
            if (lease) {
                lease->positions.push_back({seq, k + 1, f.transaction_id,
                                            f.message_id});
            }
        }
    }
}

}  // namespace

// --------------------------------------------------------------------- push
namespace {

// Cross-request fusion knobs (read once at first use). One HTTP push == one
// segment per (queue,partition) degenerates to 1-frame segments for
// single-message producers at high rate — exactly the shape the segments
// engine is worst at. Parking frames for a few ms and flushing ONE fused
// segment transfers the spike's measured batch wins (see
// benchmark-queen/storage-v2-spike/README.md) to non-batchy producers, with
// the v1 engine's self-clocked group-commit philosophy (preferred_batch_size
// OR max_hold_ms, see benchmark-queen/fusion-hysteresis/README.md).
// QUEEN_V2_FUSION_HOLD_MS=0 bypasses the accumulator entirely: every request
// flushes its own segments inline — exact pre-fusion behavior, the zero-risk
// switch.
int fusion_hold_ms() {
    static const int v = std::max(0, get_env_int("QUEEN_V2_FUSION_HOLD_MS", 15));
    return v;
}
size_t fusion_frames() {
    static const size_t v = static_cast<size_t>(
        std::max(1, get_env_int("QUEEN_V2_FUSION_FRAMES", 100)));
    return v;
}

// Aggregation state of one HTTP push request. The request parks until every
// segment flush carrying its frames commits (up to one flush per
// (queue,partition) it touched); `pending` counts those flushes. All
// mutation happens on the owning worker's loop thread (route handler, flush
// timer, defer callbacks), so no locking.
struct PushState {
    std::string request_id;
    int uws_worker = 0;
    nlohmann::json results;  // per-item, indexed like the request body
    size_t pending = 0;      // segment flushes still in flight
    bool failed = false;
    std::string error;
    std::shared_ptr<PushFailoverStorage> failover;
    std::shared_ptr<FileBufferManager> file_buffer;
};

// Terminal response for one push request (all its flushes accounted for).
// DB error on any flush: v1-parity failover — buffer the request's ORIGINAL
// items (snapshotted pre-encryption in push_failover_storage) to the file
// buffer and answer 'buffered' 201 (replay routes each event by the queue's
// CURRENT storage flag); otherwise per-item results, 201. Each request parked
// on a failed fused flush goes through here with its own snapshot, so every
// one of them is answered consistently.
void finalize_push_request(const std::shared_ptr<PushState>& st) {
    if (st->failed) {
        if (st->failover && st->file_buffer) {
            auto stored = st->failover->retrieve_and_remove(st->request_id);
            if (stored.has_value()) {
                try {
                    auto evs = nlohmann::json::parse(stored.value());
                    nlohmann::json results = nlohmann::json::array();
                    bool all_buffered = true;
                    for (auto& ev : evs) {
                        ev["failover"] = true;
                        if (st->file_buffer->write_event(ev)) {
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
                    worker_response_registries[st->uws_worker]->send_response_raw(
                        st->request_id, results.dump(), all_buffered ? 201 : 500);
                    return;
                } catch (const std::exception& e) {
                    spdlog::error("segments push failover failed: {}", e.what());
                }
            }
        }
        nlohmann::json err = {{"error", "segments push failed: " + st->error}};
        worker_response_registries[st->uws_worker]->send_response_raw(
            st->request_id, err.dump(), 503);
    } else {
        if (st->failover) st->failover->remove(st->request_id);
        worker_response_registries[st->uws_worker]->send_response_raw(
            st->request_id, st->results.dump(), 201);
    }
}

// One pending fused segment: frames from >= 1 requests to a single
// (queue,partition), flushed as ONE q2 segment when it reaches
// QUEEN_V2_FUSION_FRAMES frames or its oldest frame reaches
// QUEEN_V2_FUSION_HOLD_MS. A request's frames for one (queue,partition)
// always land in the SAME flush (appended whole, then the threshold is
// checked), so `requests` holds each contributor exactly once.
struct FusionGroup {
    std::string queue, partition;
    std::vector<sv2::FrameIn> frames;
    struct Owner {                       // parallel to frames
        std::shared_ptr<PushState> st;
        int item_idx;                    // index in the owning request's body
    };
    std::vector<Owner> owners;
    // Duplicate items PARKED on the group: a txn already carried by one of
    // `frames` (same request or another parked request). They are answered at
    // flush completion with the ORIGINAL frame's outcome — a dup must share
    // its original's fate, not assume it commits ('duplicate' of a message
    // that then failed would fabricate a message id).
    struct DupOwner {
        std::shared_ptr<PushState> st;
        int item_idx;
        std::string txn;
    };
    std::vector<DupOwner> dup_owners;
    std::vector<std::shared_ptr<PushState>> requests;  // distinct contributors
    std::map<std::string, std::string> seen_txn;  // txn -> first message_id
    std::chrono::steady_clock::time_point deadline;  // first frame + HOLD_MS
};

// One accumulator per uWS worker thread: the route handler, the flush timer
// and every defer callback run on this worker's loop, so the structure is
// single-threaded by construction. Independent fusion across workers is
// correct — worst case the same (queue,partition) yields one segment per
// worker instead of one.
//
// No flush-on-shutdown: a parked frame belongs to an HTTP request that never
// got its 201, and in-flight HTTP dies with the process anyway — the client
// retries (idempotent by transactionId) exactly as it would for a request
// killed between submit and commit pre-fusion. The file buffer covers
// DB-down, not process death.
struct FusionWorker {
    QueenCluster* cluster = nullptr;
    uWS::Loop* loop = nullptr;
    us_timer_t* timer = nullptr;                // armed iff groups pending
    std::map<std::string, FusionGroup> groups;  // "queue\x1fpartition"
};

FusionWorker& fusion_worker() {
    static thread_local FusionWorker fw;
    return fw;
}

// q2 partition uuid per (queue,partition), for stamping partition_id on
// queued push rows (v1 rows carry it: 001_push.sql results). Filled from the
// wire wrapper's partitionId when it returns one, else from a one-off probe;
// entries never go stale (partition uuids are immutable), wiped wholesale
// when full rather than tracking LRU order.
class PushPidCache {
public:
    std::string get(const std::string& key) {
        std::lock_guard<std::mutex> g(mu_);
        auto it = m_.find(key);
        return it == m_.end() ? std::string() : it->second;
    }
    void put(const std::string& key, const std::string& pid) {
        if (pid.empty()) return;
        std::lock_guard<std::mutex> g(mu_);
        if (m_.size() >= kMax) m_.clear();
        m_[key] = pid;
    }

private:
    static constexpr size_t kMax = 65536;
    std::mutex mu_;
    std::unordered_map<std::string, std::string> m_;
};

PushPidCache& push_pid_cache() {
    static PushPidCache c;
    return c;
}

// One in-flight segment write ATTEMPT for a flushed FusionGroup. The QDUP
// repack path (see handle_flush_result) re-submits the same flush minus the
// duplicate frames as attempt 1; the parked requests keep their `pending`
// bookkeeping across attempts and are finalized exactly once, in
// conclude_flush.
struct SegmentFlush {
    std::string queue, partition;
    std::vector<sv2::FrameIn> frames;               // this attempt's frames
    std::vector<FusionGroup::Owner> owners;         // parallel to frames
    std::vector<FusionGroup::DupOwner> dup_owners;  // parked window dups
    std::vector<std::shared_ptr<PushState>> requests;
    int attempt = 0;
    // txn -> terminal outcome of the frame that carried it ({"status",
    // "message_id"[, "error"]}); parked window dups are answered from here at
    // conclude time so a dup always shares its original's fate.
    std::map<std::string, nlohmann::json> outcome_by_txn;
};

void submit_flush(QueenCluster* cluster, uWS::Loop* loop,
                  std::shared_ptr<SegmentFlush> fl);

// Terminal step of a flush (all frames resolved): answer the parked window
// dups from their originals' outcomes, then release every contributing
// request's pending slot.
void conclude_flush(const std::shared_ptr<SegmentFlush>& fl) {
    for (const auto& d : fl->dup_owners) {
        nlohmann::json row = {{"index", d.item_idx},
                              {"transaction_id", d.txn},
                              {"queueName", fl->queue}};
        auto it = fl->outcome_by_txn.find(d.txn);
        if (it == fl->outcome_by_txn.end()) {
            // Unreachable (a dup's original frame is always in this flush);
            // answer failed so the producer retries rather than hanging.
            row["status"] = "failed";
            row["error"] = "duplicate original unresolved";
        } else {
            std::string s = it->second.value("status", "failed");
            // Original queued -> this item is the duplicate of it; original
            // itself a committed-duplicate -> same original message id;
            // original failed -> the dup failed with it.
            row["status"] = (s == "queued") ? "duplicate" : s;
            if (it->second.contains("message_id")) row["message_id"] = it->second["message_id"];
            if (it->second.contains("error")) row["error"] = it->second["error"];
        }
        d.st->results[d.item_idx] = std::move(row);
    }
    for (const auto& st : fl->requests) {
        if (--st->pending == 0) finalize_push_request(st);
    }
}

// DB error on a flush: every parked request fails over independently
// (finalize_push_request handles the file-buffer path).
void fail_flush(const std::shared_ptr<SegmentFlush>& fl, const std::string& error) {
    for (const auto& st : fl->requests) {
        st->failed = true;
        st->error = error;
        if (--st->pending == 0) finalize_push_request(st);
    }
}

// Fill the per-item results for a fully committed attempt (v1 row shape:
// 001_push.sql — status/message_id plus partition_id and the echoed
// trace_id) and record the outcomes for parked dups.
void fill_queued_results(const std::shared_ptr<SegmentFlush>& fl,
                         const std::string& pid) {
    for (size_t k = 0; k < fl->frames.size(); k++) {
        const auto& f = fl->frames[k];
        nlohmann::json row = {
            {"index", fl->owners[k].item_idx},
            {"transaction_id", f.transaction_id},
            {"status", "queued"},
            {"message_id", f.message_id},
            {"partition_id", pid.empty() ? nlohmann::json(nullptr)
                                         : nlohmann::json(pid)},
            {"trace_id", f.trace_id ? nlohmann::json(*f.trace_id)
                                    : nlohmann::json(nullptr)},
            {"queueName", fl->queue}};
        fl->outcome_by_txn[f.transaction_id] = {{"status", "queued"},
                                                {"message_id", f.message_id}};
        fl->owners[k].st->results[fl->owners[k].item_idx] = std::move(row);
    }
}

// Committed attempt: stamp results (resolving the q2 partition uuid through
// the wrapper result, the cache, or a one-off probe), notify waiters,
// conclude.
void finish_flush_queued(QueenCluster* cluster, uWS::Loop* loop,
                         const std::shared_ptr<SegmentFlush>& fl,
                         const nlohmann::json& r) {
    auto shared_state = global_shared_state;
    auto notify_and_conclude = [fl, shared_state](const std::string& pid) {
        fill_queued_results(fl, pid);
        if (shared_state) {
            shared_state->notify_message_available(fl->queue, fl->partition);
        }
        conclude_flush(fl);
    };

    std::string cache_key = fl->queue + "\x1f" + fl->partition;
    std::string pid = r.value("partitionId", "");
    if (!pid.empty()) {
        push_pid_cache().put(cache_key, pid);
        notify_and_conclude(pid);
        return;
    }
    pid = push_pid_cache().get(cache_key);
    if (!pid.empty()) {
        notify_and_conclude(pid);
        return;
    }
    // Wrapper predates the partitionId key and the cache is cold: one probe
    // per (queue,partition), cached forever afterwards.
    queen::JobRequest probe;
    probe.op_type = queen::JobType::CUSTOM;
    probe.request_id = fl->requests.front()->request_id;
    probe.sql = "SELECT to_jsonb(COALESCE((SELECT p.id::text FROM queen.seg_partitions p "
                "JOIN queen.seg_queues q ON q.id = p.queue_id "
                "WHERE q.name = $1 AND p.name = $2), ''))";
    probe.params = {fl->queue, fl->partition};
    cluster->submit(std::move(probe), [loop, fl, cache_key,
                                       notify_and_conclude](std::string result) {
        loop->defer([=]() {
            std::string probed;
            try {
                auto pr = nlohmann::json::parse(result);
                if (pr.is_string()) probed = pr.get<std::string>();
            } catch (...) {}
            push_pid_cache().put(cache_key, probed);
            // Probe failure degrades to partition_id:null on this response
            // only — never fails a push that already committed.
            notify_and_conclude(probed);
        });
    });
}

void handle_flush_result(QueenCluster* cluster, uWS::Loop* loop,
                         const std::shared_ptr<SegmentFlush>& fl,
                         const std::string& result) {
    try {
        auto r = nlohmann::json::parse(result);
        if (r.contains("error") && !r["error"].is_null()) {
            fail_flush(fl, r["error"].is_string() ? r["error"].get<std::string>()
                                                  : r["error"].dump());
            return;
        }
        if (r.value("status", "") == "duplicate") {
            // Frame-index -> original message id map from the wrapper. The
            // unique_violation rolled back the WHOLE segment: dup frames
            // answer 'duplicate' with the ORIGINAL message id, and the fresh
            // frames are REPACKED into a new blob and retried once — a push
            // mixing committed-dups with fresh items must not fail the fresh
            // ones. Only reachable on dedup-window queues fed client txns
            // colliding with a COMMITTED message; same-window collisions are
            // absorbed by seen_txn upstream.
            std::map<int, std::string> orig;
            if (r.contains("dups") && r["dups"].is_array()) {
                for (auto& d : r["dups"]) orig[d["i"].get<int>()] = d.value("mid", "");
            }
            std::vector<sv2::FrameIn> next_frames;
            std::vector<FusionGroup::Owner> next_owners;
            for (size_t k = 0; k < fl->frames.size(); k++) {
                auto dit = orig.find(static_cast<int>(k));
                if (dit != orig.end()) {
                    const std::string& txn = fl->frames[k].transaction_id;
                    fl->outcome_by_txn[txn] = {{"status", "duplicate"},
                                               {"message_id", dit->second}};
                    fl->owners[k].st->results[fl->owners[k].item_idx] = {
                        {"index", fl->owners[k].item_idx},
                        {"transaction_id", txn},
                        {"status", "duplicate"},
                        {"message_id", dit->second},
                        {"queueName", fl->queue}};
                } else if (fl->attempt == 0) {
                    next_frames.push_back(std::move(fl->frames[k]));
                    next_owners.push_back(fl->owners[k]);
                } else {
                    // Second QDUP in a row for a frame the wrapper did NOT
                    // list as duplicate: give up on it with an explicit error
                    // (producers retry idempotently by transactionId).
                    const std::string& txn = fl->frames[k].transaction_id;
                    nlohmann::json row = {
                        {"index", fl->owners[k].item_idx},
                        {"transaction_id", txn},
                        {"status", "failed"},
                        {"error", "duplicate check failed twice for this segment"},
                        {"queueName", fl->queue}};
                    fl->outcome_by_txn[txn] = {{"status", "failed"},
                                               {"error", row["error"]}};
                    fl->owners[k].st->results[fl->owners[k].item_idx] = std::move(row);
                }
            }
            if (next_frames.empty()) {
                conclude_flush(fl);  // everything was a committed-dup
                return;
            }
            // Repack and retry ONCE (rare path): same flush, attempt 1.
            fl->frames = std::move(next_frames);
            fl->owners = std::move(next_owners);
            fl->attempt = 1;
            submit_flush(cluster, loop, fl);
            return;
        }
        finish_flush_queued(cluster, loop, fl, r);
    } catch (const std::exception& e) {
        fail_flush(fl, e.what());
    }
}

// Pack, compress and submit one attempt; completion lands back on the worker
// loop in handle_flush_result.
void submit_flush(QueenCluster* cluster, uWS::Loop* loop,
                  std::shared_ptr<SegmentFlush> fl) {
    nlohmann::json metas = nlohmann::json::array();
    for (size_t k = 0; k < fl->frames.size(); k++) {
        metas.push_back({{"i", static_cast<int>(k)},
                         {"mid", fl->frames[k].message_id},
                         {"txn", fl->frames[k].transaction_id}});
    }
    auto blob = sv2::zstd_compress(sv2::pack_frames(fl->frames));
    std::string blob_b64 = sv2::b64_encode(blob.data(), blob.size());

    queen::JobRequest job;
    job.op_type = queen::JobType::SEGMENT_PUSH;
    // Tracing only (CUSTOM jobs are never invalidated by request id); a fused
    // segment spans requests, so the first contributor's id stands in.
    job.request_id = fl->requests.front()->request_id;
    job.sql = "SELECT queen.seg_push_segment_wire_v1($1, $2, $3::jsonb, $4, $5::int)";
    job.params = {fl->queue, fl->partition, metas.dump(), std::move(blob_b64),
                  std::to_string(fl->frames.size())};

    cluster->submit(std::move(job), [cluster, loop, fl](std::string result) {
        loop->defer([cluster, loop, fl, result = std::move(result)]() {
            handle_flush_result(cluster, loop, fl, result);
        });
    });
}

// Adapter kept for the fusion flush paths and the HOLD_MS=0 inline path.
void flush_segment(QueenCluster* cluster, uWS::Loop* loop, FusionGroup g) {
    if (g.frames.empty()) return;  // defensive; pending groups always carry frames
    auto fl = std::make_shared<SegmentFlush>();
    fl->queue = std::move(g.queue);
    fl->partition = std::move(g.partition);
    fl->frames = std::move(g.frames);
    fl->owners = std::move(g.owners);
    fl->dup_owners = std::move(g.dup_owners);
    fl->requests = std::move(g.requests);
    submit_flush(cluster, loop, std::move(fl));
}

void fusion_timer_cb(us_timer_t* t);

// (Re)arm the one-shot flush timer for the earliest pending deadline.
// Deadlines are monotone in group creation time (now + HOLD_MS), so a newly
// created group never needs an earlier wake-up than the armed one; a group
// flushed early by the frame threshold at worst leaves the timer firing with
// nothing due, and the callback re-arms for the survivors.
void arm_fusion_timer(FusionWorker& fw) {
    auto earliest = std::chrono::steady_clock::time_point::max();
    for (const auto& kv : fw.groups) {
        earliest = std::min(earliest, kv.second.deadline);
    }
    if (!fw.timer) {
        // fallthrough MUST be 0 — see park_pop: us_timer_close decrements the
        // loop's poll count unconditionally, but us_create_timer only
        // increments it when !fallthrough. The ext is a plain FusionWorker*
        // (thread_local, outlives the timer): nothing to destruct on close,
        // unlike close_pop_timer's shared_ptr dance.
        fw.timer = us_create_timer(reinterpret_cast<us_loop_t*>(fw.loop), 0,
                                   sizeof(FusionWorker*));
        *static_cast<FusionWorker**>(us_timer_ext(fw.timer)) = &fw;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                         earliest - std::chrono::steady_clock::now()).count();
    int delay = static_cast<int>(std::clamp<long long>(
        remaining, 1, static_cast<long long>(std::max(fusion_hold_ms(), 1))));
    us_timer_set(fw.timer, fusion_timer_cb, delay, 0);  // one-shot; rearmed while groups remain
}

void fusion_timer_cb(us_timer_t* t) {
    auto& fw = **static_cast<FusionWorker**>(us_timer_ext(t));
    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> due;
    for (const auto& kv : fw.groups) {
        if (kv.second.deadline <= now) due.push_back(kv.first);
    }
    for (const auto& key : due) {
        auto it = fw.groups.find(key);
        FusionGroup g = std::move(it->second);
        fw.groups.erase(it);
        flush_segment(fw.cluster, fw.loop, std::move(g));
    }
    if (fw.groups.empty()) {
        us_timer_close(fw.timer);
        fw.timer = nullptr;
    } else {
        arm_fusion_timer(fw);
    }
}

}  // namespace

void handle_push_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                    std::vector<PushItem> items) {
    // Group per (queue, partition) preserving arrival order.
    struct Group {
        std::string queue, partition;
        std::vector<size_t> idx;                  // original item indices
        std::vector<sv2::FrameIn> frames;
        // Intra-batch duplicates (v1's dup_rank semantics: first occurrence
        // wins) never enter the blob; they PARK here and are answered with
        // the original's outcome at flush completion (like window dups).
        struct Dup { size_t item_idx; std::string txn; };
        std::vector<Dup> dups;
        std::map<std::string, std::string> seen_txn;  // txn -> first message_id
    };
    std::vector<Group> groups;
    std::map<std::string, size_t> group_of;
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
            groups.push_back({it.queue, it.partition, {}, {}, {}, {}});
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
            grp.dups.push_back({i, f.transaction_id});
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

    auto st = std::make_shared<PushState>();
    st->request_id = request_id;
    st->uws_worker = ctx.worker_id;
    st->results = std::move(pre_results);  // all-null template, filled per flush
    st->failover = ctx.push_failover_storage;
    st->file_buffer = ctx.file_buffer;

    if (groups.empty()) {  // no items (dups always ride a framed group)
        finalize_push_request(st);
        return;
    }

    if (fusion_hold_ms() <= 0) {
        // Fusion bypass: one segment per (queue,partition) per request,
        // flushed inline — exact pre-fusion behavior.
        st->pending = groups.size();
        for (auto& g : groups) {
            FusionGroup fg;
            fg.queue = std::move(g.queue);
            fg.partition = std::move(g.partition);
            fg.frames = std::move(g.frames);
            fg.owners.reserve(fg.frames.size());
            for (size_t k = 0; k < fg.frames.size(); k++) {
                fg.owners.push_back({st, static_cast<int>(g.idx[k])});
            }
            for (auto& d : g.dups) {
                fg.dup_owners.push_back({st, static_cast<int>(d.item_idx), d.txn});
            }
            fg.requests.push_back(st);
            flush_segment(ctx.queen, ctx.worker_loop, std::move(fg));
        }
        return;
    }

    // CROSS-REQUEST FUSION: park this request's frames in the worker's
    // accumulator; the flush (frame threshold here, HOLD_MS deadline on the
    // loop timer) commits one fused segment per (queue,partition) and answers
    // every parked request with its own per-item results. We are ON the
    // worker loop thread and flush callbacks defer to it, so nothing below
    // interleaves with a completion.
    auto& fw = fusion_worker();
    fw.cluster = ctx.queen;
    fw.loop = ctx.worker_loop;
    const auto now = std::chrono::steady_clock::now();
    for (auto& g : groups) {
        std::string key = g.queue + "\x1f" + g.partition;
        auto [it, created] = fw.groups.try_emplace(key);
        FusionGroup& fg = it->second;
        if (created) {
            fg.queue = g.queue;
            fg.partition = g.partition;
            fg.deadline = now + std::chrono::milliseconds(fusion_hold_ms());
        }
        bool contributed = false;
        for (size_t k = 0; k < g.frames.size(); k++) {
            auto& f = g.frames[k];
            auto seen = fg.seen_txn.find(f.transaction_id);
            if (seen != fg.seen_txn.end()) {
                // Intra-window duplicate across parked requests: first wins,
                // no second frame enters the blob. The dup item PARKS on the
                // group and is answered with the original's outcome when the
                // flush commits — answering 'duplicate' up front would vouch
                // for a message that might still fail.
                fg.dup_owners.push_back({st, static_cast<int>(g.idx[k]),
                                         f.transaction_id});
                contributed = true;
                continue;
            }
            fg.seen_txn[f.transaction_id] = f.message_id;
            fg.owners.push_back({st, static_cast<int>(g.idx[k])});
            fg.frames.push_back(std::move(f));
            contributed = true;
        }
        for (auto& d : g.dups) {  // request-local dups park the same way
            fg.dup_owners.push_back({st, static_cast<int>(d.item_idx), d.txn});
            contributed = true;
        }
        if (contributed) {
            st->pending++;
            fg.requests.push_back(st);
        } else if (fg.frames.empty() && fg.dup_owners.empty()) {
            // Unreachable in practice (a fresh group always takes its first
            // frame); guards a frameless group from parking forever.
            fw.groups.erase(it);
            continue;
        }
        if (fg.frames.size() >= fusion_frames()) {
            FusionGroup out = std::move(fg);
            fw.groups.erase(it);
            flush_segment(fw.cluster, fw.loop, std::move(out));
        }
    }
    if (!fw.groups.empty() && !fw.timer) arm_fusion_timer(fw);
    if (st->pending == 0) {
        // Defensive: dup items contribute pending slots too, so a request
        // with items always parks; only an empty item list lands here.
        finalize_push_request(st);
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
    // Effective subscription window (v1 semantics: "" = all). Forwarded to
    // queen.seg_pop_segments_wire_v1's p_sub_mode/p_sub_from args.
    std::string sub_mode;
    std::string sub_from;
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
                                           std::move(lease), pj->lease_seconds);
    }
    bool empty = messages.empty();
    nlohmann::json out = {{"success", true},
                          {"queue", pj->queue},
                          {"partition", pj->partition},
                          {"partitionId", partition_id},
                          {"leaseId", (pj->auto_ack || empty) ? "" : pj->lease_worker},
                          {"consumerGroup", pj->group},
                          {"messages", std::move(messages)}};
    if (!empty) out["partitionsClaimed"] = 1;  // v4 wire parity on delivering pops
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
                                           std::move(b.second), pj->lease_seconds);
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
// (decrypted), hand it to queen.seg_dlq_head_v1 (which files it, advances the cursor
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
    job.op_type = queen::JobType::SEGMENT_POP;
    job.request_id = pj->request_id;
    job.sql = "SELECT queen.seg_dlq_head_v1($1::uuid, $2, $3, $4::bigint, $5::int, $6::uuid, $7, $8::jsonb, $9)";
    job.params = {r->value("partitionId", ""), pj->group, pj->lease_worker,
                  std::to_string(first.seq), std::to_string(first.frame_idx),
                  first.message_id, first.txn, first.payload,
                  "Retries exhausted (attempt " + std::to_string(attempt) +
                      " > retryLimit " + std::to_string(pj->retry_limit) +
                      " + first delivery)"};

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
            // v1 grants retryLimit retries AFTER the first delivery, so a
            // message is served retry_limit + 1 times before it is poison
            // (verified against the rows engine: 3 deliveries at limit=2);
            // attempt exceeds that budget -> dead-letter the head.
            if (!pj->auto_ack && pj->dlq_enabled && !pj->dlq_retried &&
                attempt > pj->retry_limit + 1) {
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
    job.op_type = queen::JobType::SEGMENT_POP;
    job.request_id = pj->request_id;
    if (pj->wildcard) {
        job.sql = "SELECT queen.seg_pop_wildcard_wire_v1($1, $2, $3::int, $4::int, $5, $6::bool, $7::int, $8, $9)";
        job.params = {pj->queue, pj->group, std::to_string(pj->batch),
                      std::to_string(pj->lease_seconds), pj->lease_worker,
                      pj->auto_ack ? "true" : "false",
                      std::to_string(pj->max_partitions),
                      pj->sub_mode, pj->sub_from};
    } else {
        // p_sub_mode/p_sub_from carry the v1 subscription window ("" = all;
        // they have SQL defaults, so older 7-arg callers stay valid).
        job.sql = "SELECT queen.seg_pop_segments_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool, $8, $9)";
        job.params = {pj->queue, pj->partition, pj->group, std::to_string(pj->batch),
                      std::to_string(pj->lease_seconds), pj->lease_worker,
                      pj->auto_ack ? "true" : "false",
                      pj->sub_mode, pj->sub_from};
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
                   int lease_seconds, bool auto_ack, bool wait, int timeout_ms,
                   const std::string& sub_mode, const std::string& sub_from) {
    auto pj = make_pop_job(ctx, res, queue, consumer_group, batch, lease_seconds,
                           auto_ack, wait, timeout_ms);
    pj->partition = partition;
    pj->sub_mode = sub_mode;
    pj->sub_from = sub_from;
    submit_pop(pj);
}

void handle_pop_wildcard_v2(const RouteContext& ctx, uWS::HttpResponse<false>* res,
                            const std::string& queue,
                            const std::string& consumer_group, int batch,
                            int lease_seconds, bool auto_ack, int max_partitions,
                            bool wait, int timeout_ms,
                            const std::string& sub_mode, const std::string& sub_from) {
    auto pj = make_pop_job(ctx, res, queue, consumer_group, batch, lease_seconds,
                           auto_ack, wait, timeout_ms);
    pj->wildcard = true;
    pj->max_partitions = max_partitions;
    pj->sub_mode = sub_mode;
    pj->sub_from = sub_from;
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

// Appends queueName/partitionName (resolved from the q2 partition uuid in
// $1) to a jsonb result object — the v1 ack rows carry both names.
constexpr const char* kAckNamesSuffix =
    "COALESCE((SELECT jsonb_build_object('queueName', q.name, "
    "'partitionName', p.name) FROM queen.seg_partitions p "
    "JOIN queen.seg_queues q ON q.id = p.queue_id WHERE p.id = $1::uuid), '{}'::jsonb)";

// Cross-process / post-restart v2 ack: no registry entry, so positions are
// resolved in SQL via the queen.seg_dedup txn index. One SQL call per partition (a
// wildcard lease spans partitions under one leaseId). Emits v1-shaped rows
// (003_ack.sql:207-216): validation failures answer per-item success:false
// on HTTP 200, never a batch-level error status.
void dispatch_ack_by_txn(QueenCluster* cluster, uWS::Loop* loop, int uws_worker,
                         const std::string& request_id, const nlohmann::json& acks,
                         const std::string& lease_id) {
    struct Item { int idx; std::string txn; std::string status; std::string error; };
    struct Part {
        std::string group;
        nlohmann::json txns = nlohmann::json::array();  // [{"txn","ok"}]
        std::vector<Item> items;
    };
    std::map<std::string, Part> parts;
    int idx = 0;
    for (const auto& a : acks) {
        auto& p = parts[a.value("partitionId", "")];
        p.group = a.value("consumerGroup", "__QUEUE_MODE__");
        std::string txn = a.value("transactionId", "");
        std::string status = a.value("status", "completed");
        // Explicit 'dlq' disposes the position like a completed ack (the
        // message leaves the stream either way; v1 dead-letters it).
        bool ok = status == "completed" || status == "success" || status == "dlq";
        p.txns.push_back({{"txn", txn}, {"ok", ok}});
        p.items.push_back({idx, txn, status,
                           a.contains("error") && a["error"].is_string()
                               ? a["error"].get<std::string>() : ""});
        idx++;
    }

    struct State {
        nlohmann::json results;
        size_t pending = 0;
    };
    auto st = std::make_shared<State>();
    st->results = nlohmann::json::array();
    for (int i = 0; i < idx; i++) st->results.push_back(nullptr);
    st->pending = parts.size();

    for (auto& kv : parts) {
        queen::JobRequest job;
        job.op_type = queen::JobType::SEGMENT_ACK;
        job.request_id = request_id;
        bool single_dlq =
            kv.second.items.size() == 1 && kv.second.items[0].status == "dlq";
        if (single_dlq) {
            // Explicit dead-letter with no in-process batch: the position
            // comes from the dedup window and queen.seg_dlq_head_v1 files it,
            // advances the cursor past the frame and releases the lease (v1
            // dead-letters immediately on explicit dlq status). Queues with
            // dedup disabled cannot resolve the position: zero rows, answered
            // as a failed ack below. Mixed batches keep the ack_by_txn path:
            // their dlq positions dispose via the cursor (no queen.seg_dlq snapshot
            // — the lease cannot survive two terminal calls).
            const auto& it0 = kv.second.items[0];
            job.sql = std::string(
                          "SELECT queen.seg_dlq_head_v1($1::uuid, $2, $3, d.seq, "
                          "d.frame_idx, d.message_id, $4, NULL::jsonb, $5) || ") +
                      kAckNamesSuffix +
                      " FROM queen.seg_dedup d WHERE d.partition_id = $1::uuid "
                      "AND d.txn_hash = hashtextextended($4, 0)";
            job.params = {kv.first, kv.second.group, lease_id, it0.txn,
                          it0.error.empty() ? "Dead-lettered by consumer ack"
                                            : it0.error};
        } else {
            job.sql = std::string(
                          "SELECT queen.seg_ack_by_txn_v1($1::uuid, $2, $3, $4::jsonb) || ") +
                      kAckNamesSuffix;
            job.params = {kv.first, kv.second.group, lease_id,
                          kv.second.txns.dump()};
        }

        auto items = kv.second.items;
        cluster->submit(std::move(job), [loop, uws_worker, request_id, st,
                                         items](std::string result) {
            loop->defer([=]() {
                bool ok = false;
                std::string err;
                nlohmann::json queue_name = nullptr, partition_name = nullptr;
                try {
                    auto r = nlohmann::json::parse(result);
                    if (r.is_object()) {
                        ok = r.value("ok", false);
                        if (r.contains("queueName")) queue_name = r["queueName"];
                        if (r.contains("partitionName")) partition_name = r["partitionName"];
                        if (!ok) {
                            err = r.contains("error") && r["error"].is_string()
                                      ? r["error"].get<std::string>() : "ack failed";
                        }
                    } else {
                        err = "position not resolvable (dedup window)";
                    }
                } catch (const std::exception& e) {
                    err = e.what();
                }
                for (const auto& it : items) {
                    st->results[it.idx] = {
                        {"index", it.idx},
                        {"transactionId", it.txn},
                        {"success", ok},
                        {"error", ok ? nlohmann::json(nullptr)
                                     : nlohmann::json(err)},
                        {"queueName", queue_name},
                        {"partitionName", partition_name},
                        // Both resolvers release the lease on success.
                        {"leaseReleased", ok},
                        {"dlq", ok && it.status == "dlq"}};
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
    probe.sql = "SELECT to_jsonb(EXISTS(SELECT 1 FROM queen.seg_partitions WHERE id = $1::uuid))";
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
    // v1 wire rows (003_ack.sql:207-216): {index, transactionId, success,
    // error, queueName, partitionName, leaseReleased, dlq}. v1 answers
    // success:true for every PROCESSED ack — completed, failed (nack) and
    // dlq alike; success:false is reserved for validation failures (unknown
    // message, invalid/expired lease) and always travels on HTTP 200.
    nlohmann::json results = nlohmann::json::array();
    std::map<std::string, std::vector<size_t>> rows_by_pid;   // result rows
    std::map<std::string, std::string> dlq_error_by_pid;
    struct PartitionJob {
        sv2::LeaseRegistry::AckOutcome oc;
        std::string pid;
        std::vector<size_t> rows;      // rewritten if the terminal SQL fails
        std::string dlq_error;
    };
    std::vector<PartitionJob> jobs;
    bool any_known = false;

    int idx = -1;
    for (const auto& a : acks) {
        idx++;
        std::string txn = a.value("transactionId", "");
        std::string pid = a.value("partitionId", "");
        std::string status = a.value("status", "completed");
        bool s_ok = status == "completed" || status == "success";
        bool s_dlq = status == "dlq";
        auto oc = reg.ack(lease_id, pid, txn, s_ok, s_dlq);
        if (!oc.known) {
            results.push_back({{"index", idx},
                               {"transactionId", txn},
                               {"success", false},
                               {"error", "Message not found"},
                               {"queueName", nullptr},
                               {"partitionName", nullptr},
                               {"leaseReleased", false},
                               {"dlq", false}});
            continue;
        }
        any_known = true;  // includes idempotent retries (oc.already): they
                           // must NOT fall through to ack_by_txn, which would
                           // release the still-live lease under the batch
        results.push_back({{"index", idx},
                           {"transactionId", txn},
                           {"success", true},
                           {"error", nullptr},
                           {"queueName", oc.queue},
                           {"partitionName", oc.partition},
                           {"leaseReleased", oc.complete},
                           {"dlq", s_dlq}});
        if (oc.already) continue;  // no-op retry: no SQL, no rewrite exposure
        rows_by_pid[pid].push_back(results.size() - 1);
        if (s_dlq) {
            std::string e = a.contains("error") && a["error"].is_string()
                                ? a["error"].get<std::string>() : "";
            dlq_error_by_pid[pid] =
                e.empty() ? "Dead-lettered by consumer ack" : e;
        }
        if (oc.complete) {
            std::string derr;
            auto dit = dlq_error_by_pid.find(pid);
            if (dit != dlq_error_by_pid.end()) derr = dit->second;
            jobs.push_back({std::move(oc), pid, rows_by_pid[pid], derr});
        }
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

    if (jobs.empty()) {
        // No partition batch completed yet: cursors move when the rest arrives.
        worker_response_registries[uws_worker]->send_response_raw(
            request_id, results.dump(), 200);
        return true;
    }

    // One terminal SQL call per completed partition (a wildcard lease can
    // complete several at once). Callbacks all land on this worker's loop.
    // A failing call (lease raced/expired) rewrites ITS partition's rows to
    // success:false — still HTTP 200, v1 parity.
    struct State {
        nlohmann::json results;
        size_t pending = 0;
        bool exception = false;
        std::string exc_error;
    };
    auto st = std::make_shared<State>();
    st->results = std::move(results);
    st->pending = jobs.size();

    for (auto& pj : jobs) {
        queen::JobRequest job;
        job.op_type = queen::JobType::SEGMENT_ACK;
        job.request_id = request_id;
        if (!pj.oc.dlq_positions.empty()) {
            // Explicit status='dlq' inside the acked prefix: file the LAST
            // such position via queen.seg_dlq_head_v1 — it inserts the queen.seg_dlq row
            // (id/txn snapshot; payloads are not retained at ack time),
            // advances the cursor past that frame and releases the lease.
            // When it is the prefix end (single-message dlq, or dlq as the
            // final disposed message — the practical patterns) the cursor
            // move is exactly ack_segments_v1's; a dlq mid-prefix redelivers
            // the acked tail (at-least-once, never loss), and earlier dlq
            // positions of the same batch dispose without their own row (the
            // lease cannot survive two terminal calls).
            const auto& dp = pj.oc.dlq_positions.back();
            job.sql = "SELECT queen.seg_dlq_head_v1($1::uuid, $2, $3, $4::bigint, $5::int, $6::uuid, $7, NULL::jsonb, $8)";
            job.params = {pj.pid, pj.oc.consumer_group, lease_id,
                          std::to_string(dp.seq), std::to_string(dp.frame_idx),
                          dp.mid, dp.txn,
                          pj.dlq_error.empty() ? "Dead-lettered by consumer ack"
                                               : pj.dlq_error};
        } else {
            job.sql = "SELECT queen.seg_ack_segments_v1($1, $2, $3, $4, $5::bigint, $6::int, $7::bool, $8::int)";
            job.params = {pj.oc.queue, pj.oc.partition, pj.oc.consumer_group,
                          lease_id, std::to_string(pj.oc.upto_seq),
                          std::to_string(pj.oc.upto_off),
                          pj.oc.ok ? "true" : "false",
                          std::to_string(pj.oc.acked_count)};
        }

        auto rows = pj.rows;
        ctx.queen->submit(std::move(job), [worker_loop, uws_worker, request_id,
                                           st, rows](std::string result) {
            worker_loop->defer([=]() {
                bool ok = false;
                std::string err;
                try {
                    auto r = nlohmann::json::parse(result);
                    ok = r.is_object() && r.value("ok", false);
                    if (!ok) {
                        err = r.is_object() && r.contains("error") && r["error"].is_string()
                                  ? r["error"].get<std::string>() : "ack failed";
                    }
                } catch (const std::exception& e) {
                    st->exception = true;
                    st->exc_error = e.what();
                }
                if (!ok && !st->exception) {
                    for (size_t ri : rows) {
                        st->results[ri]["success"] = false;
                        st->results[ri]["error"] = err;
                        st->results[ri]["leaseReleased"] = false;
                        st->results[ri]["dlq"] = false;
                    }
                }
                if (--st->pending == 0) {
                    if (st->exception) {
                        worker_response_registries[uws_worker]->send_response_raw(
                            request_id,
                            nlohmann::json{{"error", st->exc_error}}.dump(), 500);
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
