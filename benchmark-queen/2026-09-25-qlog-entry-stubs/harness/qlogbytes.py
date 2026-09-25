#!/usr/bin/env python3
# qlogbytes.py QLOG_DIR [MESSAGES] — split every .qlog file's bytes into: message
# records, WHOLE entry records (kind 2), entry STUBS (kind 3), the zero-filled
# preallocated tail and file headers. Layout: server/src/rsm/qlog/record.rs.
import os, struct, sys, json
from collections import Counter
root = sys.argv[1]
b = Counter(); n = Counter(); copies = Counter()
files = 0
for dp, _, fns in os.walk(root):
    for fn in fns:
        if not fn.endswith(".qlog"):
            continue
        files += 1
        with open(os.path.join(dp, fn), "rb") as f:
            data = f.read()
        size = len(data)
        b["file_header"] += min(32, size)
        off = 32
        while off + 4 <= size:
            (ln,) = struct.unpack_from("<I", data, off)
            if ln == 0:
                break
            if off + 4 + ln > size or ln < 45:
                b["torn_or_unknown"] += size - off; off = size; break
            kind = data[off + 48] & 0x7F
            if kind == 2:
                b["entry_whole"] += 4 + ln; n["entry_whole"] += 1
                (pid,) = struct.unpack_from("<Q", data, off + 20); copies[pid] += 1
            elif kind == 3:
                b["entry_stub"] += 4 + ln; n["entry_stub"] += 1
            else:
                b["message_records"] += 4 + ln; n["message_records"] += 1
                (cnt,) = struct.unpack_from("<I", data, off + 36); n["messages"] += cnt
            off += 4 + ln
        if off < size:
            tail = data[off:]; z = len(tail) - len(tail.lstrip(b"\0"))
            b["prealloc_zeros"] += z; b["torn_or_unknown"] += len(tail) - z
total = sum(b.values())
msgs = int(sys.argv[2]) if len(sys.argv) > 2 else n["messages"]
out = {"files": files, "total_bytes": total, "messages_in_log": n["messages"]}
for k in ("message_records", "entry_whole", "entry_stub", "prealloc_zeros", "file_header", "torn_or_unknown"):
    out[k] = b[k]
out["n_entry_whole"] = n["entry_whole"]; out["n_entry_stub"] = n["entry_stub"]
out["bytes_per_msg_total"] = round(total / max(msgs, 1), 1)
out["bytes_per_msg_entries"] = round((b["entry_whole"] + b["entry_stub"]) / max(msgs, 1), 1)
out["bytes_per_msg_messages"] = round(b["message_records"] / max(msgs, 1), 1)
if n["entry_whole"]:
    out["avg_whole_entry_bytes"] = round(b["entry_whole"] / n["entry_whole"])
    out["copies_weighted"] = round(sum(k * v for k, v in copies.items()) / n["entry_whole"], 1)
print(json.dumps(out))
