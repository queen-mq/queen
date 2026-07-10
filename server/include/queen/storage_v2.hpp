#pragma once
// Storage v2 (segments engine) broker-side utilities.
//
// One segment row = K messages packed as length-prefixed frames, compressed
// together with zstd. SQL side: lib/schema/procedures/023_storage_v2.sql
// (schema q2); blobs travel base64-encoded through the text-mode libpq layer.
//
// Frame layout (little-endian):
//   u32  frame_len            length of everything after this field
//   u8   flags                bit0: trace_id present, bit1: producer_sub present
//   u8[16] message_id         UUID bytes
//   [u8[16] trace_id]         iff flags & 1
//   u16  txn_len, txn bytes
//   [u16 psub_len, psub]      iff flags & 2
//   payload bytes             rest of the frame
//
// Consumption cursor semantics live entirely in SQL; here we only pack, unpack
// and keep the per-lease ack bookkeeping (leaseId -> delivered positions).

#include <cstdint>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <zstd.h>

namespace queen::storage_v2 {

struct FrameIn {
    std::string message_id;                // canonical uuid string (with dashes)
    std::string transaction_id;
    std::optional<std::string> trace_id;   // uuid string
    std::optional<std::string> producer_sub;
    std::string payload;                   // raw JSON bytes, passthrough
};

struct FrameOut {
    std::string message_id;
    std::string transaction_id;
    std::optional<std::string> trace_id;
    std::optional<std::string> producer_sub;
    std::string payload;
};

// ------------------------------------------------------------------ uuid/hex
inline bool uuid_to_bytes(const std::string& uuid, uint8_t out[16]) {
    int hi = -1, n = 0;
    for (char c : uuid) {
        if (c == '-') continue;
        int v;
        if (c >= '0' && c <= '9') v = c - '0';
        else if (c >= 'a' && c <= 'f') v = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') v = c - 'A' + 10;
        else return false;
        if (hi < 0) { hi = v; }
        else {
            if (n >= 16) return false;
            out[n++] = static_cast<uint8_t>((hi << 4) | v);
            hi = -1;
        }
    }
    return n == 16 && hi < 0;
}

inline std::string bytes_to_uuid(const uint8_t* b) {
    static const char* hex = "0123456789abcdef";
    std::string s;
    s.reserve(36);
    for (int i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) s += '-';
        s += hex[b[i] >> 4];
        s += hex[b[i] & 0xf];
    }
    return s;
}

// ------------------------------------------------------------------- base64
inline std::string b64_encode(const uint8_t* data, size_t len) {
    static const char* tbl =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 2 < len; i += 3) {
        uint32_t v = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63];
        out += tbl[(v >> 6) & 63];  out += tbl[v & 63];
    }
    if (i + 1 == len) {
        uint32_t v = data[i] << 16;
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63];
        out += "==";
    } else if (i + 2 == len) {
        uint32_t v = (data[i] << 16) | (data[i + 1] << 8);
        out += tbl[(v >> 18) & 63]; out += tbl[(v >> 12) & 63];
        out += tbl[(v >> 6) & 63]; out += '=';
    }
    return out;
}

inline std::vector<uint8_t> b64_decode(const std::string& in) {
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    std::vector<uint8_t> out;
    out.reserve((in.size() / 4) * 3);
    uint32_t buf = 0;
    int bits = 0;
    for (char c : in) {
        if (c == '=' || c == '\n' || c == '\r') continue;
        int v = val(c);
        if (v < 0) continue;
        buf = (buf << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<uint8_t>((buf >> bits) & 0xff));
        }
    }
    return out;
}

// -------------------------------------------------------------------- zstd
inline std::vector<uint8_t> zstd_compress(const std::string& raw, int level = 3) {
    size_t bound = ZSTD_compressBound(raw.size());
    std::vector<uint8_t> out(bound);
    size_t n = ZSTD_compress(out.data(), bound, raw.data(), raw.size(), level);
    if (ZSTD_isError(n)) return {};
    out.resize(n);
    return out;
}

inline std::string zstd_decompress(const std::vector<uint8_t>& blob) {
    unsigned long long raw_size =
        ZSTD_getFrameContentSize(blob.data(), blob.size());
    if (raw_size == ZSTD_CONTENTSIZE_ERROR || raw_size == ZSTD_CONTENTSIZE_UNKNOWN)
        return {};
    std::string out;
    out.resize(raw_size);
    size_t n = ZSTD_decompress(out.data(), out.size(), blob.data(), blob.size());
    if (ZSTD_isError(n) || n != raw_size) return {};
    return out;
}

// ------------------------------------------------------------------- frames
inline void append_u16(std::string& s, uint16_t v) {
    s += static_cast<char>(v & 0xff);
    s += static_cast<char>((v >> 8) & 0xff);
}
inline void append_u32(std::string& s, uint32_t v) {
    for (int i = 0; i < 4; i++) s += static_cast<char>((v >> (8 * i)) & 0xff);
}

inline std::string pack_frames(const std::vector<FrameIn>& frames) {
    std::string out;
    size_t est = 0;
    for (const auto& f : frames) est += f.payload.size() + f.transaction_id.size() + 64;
    out.reserve(est);
    for (const auto& f : frames) {
        uint8_t mid[16], tid[16];
        if (!uuid_to_bytes(f.message_id, mid)) std::memset(mid, 0, 16);
        uint8_t flags = 0;
        bool has_trace = f.trace_id && uuid_to_bytes(*f.trace_id, tid);
        if (has_trace) flags |= 1;
        if (f.producer_sub) flags |= 2;

        std::string body;
        body.reserve(f.payload.size() + f.transaction_id.size() + 40);
        body += static_cast<char>(flags);
        body.append(reinterpret_cast<const char*>(mid), 16);
        if (has_trace) body.append(reinterpret_cast<const char*>(tid), 16);
        append_u16(body, static_cast<uint16_t>(f.transaction_id.size()));
        body += f.transaction_id;
        if (f.producer_sub) {
            append_u16(body, static_cast<uint16_t>(f.producer_sub->size()));
            body += *f.producer_sub;
        }
        body += f.payload;

        append_u32(out, static_cast<uint32_t>(body.size()));
        out += body;
    }
    return out;
}

inline bool unpack_frames(const std::string& raw, std::vector<FrameOut>& out) {
    size_t o = 0;
    auto rd_u16 = [&](size_t p) -> uint16_t {
        return static_cast<uint16_t>(static_cast<uint8_t>(raw[p])) |
               (static_cast<uint16_t>(static_cast<uint8_t>(raw[p + 1])) << 8);
    };
    while (o + 4 <= raw.size()) {
        uint32_t len = 0;
        for (int i = 3; i >= 0; i--)
            len = (len << 8) | static_cast<uint8_t>(raw[o + i]);
        size_t end = o + 4 + len;
        if (end > raw.size() || len < 19) return false;
        size_t p = o + 4;
        uint8_t flags = static_cast<uint8_t>(raw[p]); p += 1;
        FrameOut f;
        f.message_id = bytes_to_uuid(reinterpret_cast<const uint8_t*>(raw.data() + p));
        p += 16;
        if (flags & 1) {
            if (p + 16 > end) return false;
            f.trace_id = bytes_to_uuid(reinterpret_cast<const uint8_t*>(raw.data() + p));
            p += 16;
        }
        if (p + 2 > end) return false;
        uint16_t txn_len = rd_u16(p); p += 2;
        if (p + txn_len > end) return false;
        f.transaction_id.assign(raw, p, txn_len); p += txn_len;
        if (flags & 2) {
            if (p + 2 > end) return false;
            uint16_t ps_len = rd_u16(p); p += 2;
            if (p + ps_len > end) return false;
            f.producer_sub = raw.substr(p, ps_len); p += ps_len;
        }
        f.payload.assign(raw, p, end - p);
        out.push_back(std::move(f));
        o = end;
    }
    return o == raw.size();
}

// ------------------------------------------------------- lease bookkeeping
// The v1 wire ack carries (transactionId, partitionId, leaseId): for segment
// queues the broker resolves txn -> position through this registry, populated
// at pop time. Single-process scope; a pop answered by one worker thread and
// acked through another still hits it (shared map + mutex). Cross-process
// deployments fall back to the q2.dedup resolver (phase 2; documented).
struct LeaseBatch {
    std::string queue;
    std::string partition;
    std::string consumer_group;
    // Delivered positions, in order: (seq, offset_after_this_message) plus txn.
    struct Pos { int64_t seq; int32_t off_after; std::string txn; };
    std::vector<Pos> positions;
    std::vector<bool> acked;
    size_t acked_count = 0;
    bool failed = false;
};

class LeaseRegistry {
public:
    static LeaseRegistry& instance() {
        static LeaseRegistry r;
        return r;
    }
    void put(const std::string& lease_id, LeaseBatch batch) {
        std::lock_guard<std::mutex> g(mu_);
        map_[lease_id] = std::move(batch);
    }
    // Applies one ack; returns the final position to persist when the batch
    // is complete (all acked, or failed => highest contiguous prefix).
    struct AckOutcome {
        bool known = false;        // lease found and txn matched
        bool complete = false;     // time to call q2.ack_segments_v1
        bool ok = true;            // p_ok for the SQL call
        int64_t upto_seq = 0;
        int32_t upto_off = 0;
        int32_t acked_count = 0;
        std::string queue, partition, consumer_group;
    };
    AckOutcome ack(const std::string& lease_id, const std::string& txn, bool success) {
        std::lock_guard<std::mutex> g(mu_);
        AckOutcome r;
        auto it = map_.find(lease_id);
        if (it == map_.end()) return r;
        auto& b = it->second;
        for (size_t i = 0; i < b.positions.size(); i++) {
            if (b.positions[i].txn == txn && !b.acked[i]) {
                b.acked[i] = true;
                b.acked_count++;
                if (!success) b.failed = true;
                r.known = true;
                break;
            }
        }
        if (!r.known) return r;
        if (b.acked_count == b.positions.size() || b.failed) {
            // Highest contiguous acked prefix.
            size_t n = 0;
            while (n < b.acked.size() && b.acked[n]) n++;
            r.complete = true;
            r.ok = n > 0;
            if (n > 0) {
                r.upto_seq = b.positions[n - 1].seq;
                r.upto_off = b.positions[n - 1].off_after;
            }
            r.acked_count = static_cast<int32_t>(n);
            r.queue = b.queue;
            r.partition = b.partition;
            r.consumer_group = b.consumer_group;
            map_.erase(it);
        }
        return r;
    }
    void drop(const std::string& lease_id) {
        std::lock_guard<std::mutex> g(mu_);
        map_.erase(lease_id);
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, LeaseBatch> map_;
};

}  // namespace queen::storage_v2
