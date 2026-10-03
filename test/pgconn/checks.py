"""What the queue must hold, checked from the messages alone.

`analyze()` takes every message a reader popped from the source's queues and
the tables they mirror, and checks:

  * every message is a well-formed event (PLAN §4.6);
  * every transactionId is unique across the queues;
  * per partition the `lsn` field never decreases (commit order);
  * every key lives in exactly one partition (per-key order needs it), named
    as §4.6 renders it (a deviation is a note, not a failure);
  * replaying each partition in offset order — c/r = put `after`, u = merge
    `after` keeping the `unchanged` columns, d = delete — gives exactly the
    table's rows (`SELECT`), column by column.

Hard failures go to `errors`, spec deviations that cost no data to `notes`.
"""

import decimal
import hashlib
import re
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal

# Sums of 30-digit amounts must be exact: the default context keeps 28
# significant digits and would round the EXPECTED side.
decimal.DefaultContext.prec = 200
decimal.getcontext().prec = 200

from pg import lsn_int

MISSING = "<<unknown to a consumer: unchanged TOAST with no earlier value>>"
# `…:<seq>.<k>`: the k-th (k >= 1) event of one change, e.g. the `c` of a key
# move (connectors/queen-pg/src/source/events.rs stream_txn_id).
# Snapshot rows `pg:<epoch>:s:<hw16>:<seq>`, and FILL events at a high
# watermark `pg:<epoch>:s:<hw16>:f<seq>` (a `u` carrying the columns a dropped
# chunk row held and no stream event ever carried: unchanged TOAST).
TXN_RE = re.compile(r"^pg:([0-9a-f]{8}):(?:s:[0-9A-Fa-f]{16}:f?\d+|[0-9A-Fa-f]{16}:\d+(?:\.\d+)?)$")
FILL_RE = re.compile(r"^pg:[0-9a-f]{8}:s:[0-9A-Fa-f]{16}:f\d+$")


def is_fill(txn):
    return bool(FILL_RE.match(txn or ""))


class TableSpec:
    def __init__(self, table, queue, key_cols, ts_cols=(), partition="key"):
        self.table = table  # schema.name as the events name it
        self.queue = queue
        self.key_cols = list(key_cols)
        self.ts_cols = set(ts_cols)
        self.partition = partition  # "key" | "single"


def pg_text(v):
    """A key value (as the event JSON carries it) back to PG's text output,
    for the types the suite uses as keys (int, bigint, text)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "t" if v else "f"
    return str(v)


def render_partition(values):
    """PLAN §4.6: one column -> its text; several -> joined with `|`, `|` and
    `\\` escaped with `\\`; NULL -> `\\N`; longer than 128 bytes -> `~` + 32
    hex of SHA-256."""
    if len(values) == 1:
        s = "\\N" if values[0] is None else values[0]
    else:
        parts = []
        for v in values:
            if v is None:
                parts.append("\\N")
            else:
                parts.append(v.replace("\\", "\\\\").replace("|", "\\|"))
        s = "|".join(parts)
    if len(s.encode("utf-8")) > 128:
        s = "~" + hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]
    return s


def norm(v, is_ts):
    if is_ts and isinstance(v, str):
        s = v.replace(" ", "T")
        if re.search(r"[+-]\d\d$", s):
            s += ":00"
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return v
    return v


def epoch_of(txn):
    m = TXN_RE.match(txn or "")
    if m:
        return m.group(1)
    if txn and txn.startswith("pg:") and txn.count(":") >= 2:
        return txn.split(":")[1]
    return None


class Analysis:
    def __init__(self):
        self.errors = []
        self.notes = []
        self.stats = Counter()
        self.state = {}  # (table, key tuple) -> row
        self.history = defaultdict(list)  # (table, key) -> [(queue, partition, offset, op, unchanged)]
        self.epochs = Counter()
        self.parts = set()
        self.messages = 0

    def err(self, msg):
        if len(self.errors) < 40:
            self.errors.append(msg)
        self.stats["errors"] += 1

    def note(self, msg):
        if len(self.notes) < 20 and msg not in self.notes:
            self.notes.append(msg)


def analyze(messages_by_queue, specs, only_epoch=None, txn_ids=None):
    """`messages_by_queue`: {queue: [popped message dicts]}; `specs`: [TableSpec].
    `only_epoch`: replay only that epoch's events (after a resync). `txn_ids`:
    a dict shared across calls to check uniqueness over several queues."""
    a = Analysis()
    by_table = {s.table: s for s in specs}
    txn_ids = {} if txn_ids is None else txn_ids
    key_part = {}
    bad_names = 0
    for queue, msgs in messages_by_queue.items():
        parts = defaultdict(list)
        for m in msgs:
            parts[m.get("partition")].append(m)
        for pname, pm in parts.items():
            pm.sort(key=lambda m: m.get("offset"))
            a.parts.add((queue, pname))
            last_lsn = None
            last_off = None
            for m in pm:
                a.messages += 1
                off = m.get("offset")
                where = f"{queue}/{pname}@{off}"
                if last_off is not None and off == last_off:
                    a.err(f"{where}: offset delivered twice")
                last_off = off
                txn = m.get("transactionId")
                prev = txn_ids.get(txn)
                if prev is not None:
                    a.err(f"transactionId {txn!r} is in the queue twice: {prev} and {where}")
                else:
                    txn_ids[txn] = where
                if not TXN_RE.match(txn or ""):
                    a.note(f"transactionId {txn!r} at {where} does not follow pg:<epoch>:<16 hex>:<seq>[.k] or pg:<epoch>:s:<16 hex>:[f]<seq> (PLAN §4.4)")
                ep = epoch_of(txn)
                a.epochs[ep] += 1
                ev = m.get("data")
                if not isinstance(ev, dict):
                    a.err(f"{where}: payload is not an event object: {str(ev)[:200]}")
                    continue
                op = ev.get("op")
                a.stats[f"op_{op}"] += 1
                if op not in ("c", "u", "d", "r", "t"):
                    a.err(f"{where}: unknown op {op!r}")
                    continue
                # Commit order: the lsn field never goes back in a partition.
                try:
                    lsn = lsn_int(ev.get("lsn"))
                except (ValueError, AttributeError):
                    a.err(f"{where}: lsn {ev.get('lsn')!r} is not an LSN")
                    lsn = None
                if lsn is not None and last_lsn is not None and lsn < last_lsn:
                    a.err(f"{where}: lsn {ev.get('lsn')} is below the previous message's in the partition (order broken)")
                if lsn is not None:
                    last_lsn = lsn
                if op == "t":
                    a.stats["truncates"] += 1
                    if only_epoch is None or ep == only_epoch:
                        for t in ev.get("tables") or []:
                            for k in [k for k in a.state if k[0] == t]:
                                del a.state[k]
                    continue
                table = ev.get("table")
                spec = by_table.get(table)
                if spec is None:
                    a.err(f"{where}: event for table {table!r}, not one the source mirrors")
                    continue
                if spec.queue != queue:
                    a.err(f"{where}: event of {table} in queue {queue}, expected {spec.queue}")
                key = ev.get("key")
                if not isinstance(key, dict) or any(c not in key for c in spec.key_cols):
                    a.err(f"{where}: key {key!r} lacks the key columns {spec.key_cols}")
                    continue
                k = tuple(key[c] for c in spec.key_cols)
                fill = is_fill(txn)
                if fill:
                    a.stats["fills"] += 1
                    if op != "u":
                        a.err(f"{where}: fill id {txn!r} on a {op!r} event (a fill is a 'u')")
                if op != "r" and not fill and ev.get("xid") is None:
                    a.note(f"{where}: {op} event without xid")
                if not fill and not isinstance(ev.get("seq"), int):
                    a.note(f"{where}: seq {ev.get('seq')!r} is not an integer")
                # One partition per key, named after it (§4.6).
                if spec.partition == "key":
                    seen = key_part.get((table, k))
                    if seen is None:
                        key_part[(table, k)] = pname
                    elif seen != pname:
                        a.err(f"key {k} of {table} is in two partitions: {seen!r} and {pname!r} (per-key order is lost)")
                    want = render_partition([pg_text(key[c]) for c in spec.key_cols])
                    if want != pname:
                        bad_names += 1
                        if bad_names <= 3:
                            a.note(f"{where}: partition named {pname!r}, §4.6 renders key {k} as {want!r}")
                elif spec.partition == "single" and pname != "all":
                    a.note(f"{where}: partitionBy single, partition named {pname!r} (§4.6 says 'all')")
                unchanged = ev.get("unchanged") or []
                a.history[(table, k)].append((queue, pname, off, op, tuple(unchanged)))
                if only_epoch is not None and ep != only_epoch:
                    continue
                skey = (table, k)
                if op in ("c", "r"):
                    after = ev.get("after")
                    if not isinstance(after, dict):
                        a.err(f"{where}: {op} without an after image")
                        continue
                    row = dict(after)
                    for c in unchanged:
                        row[c] = MISSING
                        a.stats["c_with_unchanged"] += 1
                    a.state[skey] = row
                elif op == "u":
                    after = ev.get("after")
                    if not isinstance(after, dict):
                        a.err(f"{where}: u without an after image")
                        continue
                    before = ev.get("before")
                    base_key = skey
                    if isinstance(before, dict) and all(c in before for c in spec.key_cols):
                        bk = tuple(before[c] for c in spec.key_cols)
                        if bk != k:
                            base_key = (table, bk)
                    base = a.state.pop(base_key, None) if base_key != skey else a.state.get(skey)
                    row = dict(after)
                    for c in unchanged:
                        if base is not None and c in base:
                            row[c] = base[c]
                        else:
                            row[c] = MISSING
                            a.stats["unchanged_without_base"] += 1
                    a.state[skey] = row
                elif op == "d":
                    a.state.pop(skey, None)
    if bad_names > 3:
        a.note(f"... {bad_names} messages in partitions not named as §4.6 renders their key")
    known = [e for e in a.epochs if e]
    a.stats["epochs"] = len(known)
    return a


def compare(a, spec, table_rows, limit=12):
    """Problems between the replayed state of `spec.table` and its rows
    ({key tuple: row} from PG)."""
    problems = []
    mine = {k[1]: row for k, row in a.state.items() if k[0] == spec.table}
    missing = [k for k in table_rows if k not in mine]
    extra = [k for k in mine if k not in table_rows]
    differ = []
    for k, want in table_rows.items():
        got = mine.get(k)
        if got is None:
            continue
        cols = set(want) | set(got)
        bad = []
        for c in sorted(cols):
            is_ts = c in spec.ts_cols
            gv, wv = got.get(c, "<absent>"), want.get(c, "<absent>")
            if norm(gv, is_ts) != norm(wv, is_ts):
                bad.append(c)
        if bad:
            differ.append((k, bad))
    if missing:
        problems.append(f"{spec.table}: {len(missing)} rows of the table are not in the replay, e.g. keys {missing[:5]}")
    if extra:
        problems.append(f"{spec.table}: {len(extra)} keys in the replay are not in the table, e.g. {extra[:5]}")
    if differ:
        problems.append(f"{spec.table}: {len(differ)} rows differ")
        for k, bad in differ[:limit]:
            parts = []
            for c in bad[:4]:
                gv = mine[k].get(c, "<absent>")
                wv = table_rows[k].get(c, "<absent>")
                parts.append(f"{c}: queue {clip(gv)} / table {clip(wv)}")
            problems.append(f"   key {k}: " + "; ".join(parts) + f"; history {a.history.get((spec.table, k), [])[-6:]}")
    return problems


def clip(v, n=70):
    s = repr(v)
    if len(s) > n:
        if isinstance(v, str):
            return f"<text len {len(v)} md5 {hashlib.md5(v.encode()).hexdigest()[:10]}>"
        return s[:n] + "…"
    return s


def summarize(a):
    s = a.stats
    ops = ", ".join(f"{o}={s.get('op_' + o, 0)}" for o in "crud" if s.get("op_" + o))
    fills = f", {s['fills']} of the u are fills" if s.get("fills") else ""
    return f"{a.messages} messages in {len(a.parts)} partitions ({ops}{fills}), epochs {dict(a.epochs)}"


def decimal_sum(values):
    total = Decimal(0)
    for v in values:
        total += Decimal(str(v))
    return total


# --- the checker checks itself ------------------------------------------------------------------


def selftest():
    """Feed hand-made queues to analyze()/compare(): a correct one must pass,
    and each kind of damage must be caught for the right reason. Returns a
    list of failures (empty = the checker is not vacuous)."""
    T = "s.t"
    spec = TableSpec(T, "q", ["id"], ts_cols=["at"])
    big = "X" * 50

    def msg(part, off, txn, ev):
        return {"partition": part, "offset": off, "transactionId": txn, "data": ev}

    def ev(op, i, after=None, unchanged=None, lsn="0/100", seq=0, before=None):
        e = {"op": op, "table": T, "key": {"id": i}, "after": after, "before": before, "lsn": lsn, "seq": seq, "xid": 7, "ts": "t"}
        if unchanged:
            e["unchanged"] = unchanged
        return e

    def good():
        return [
            msg("1", 0, "pg:0000abcd:s:0000000000000100:0", ev("r", 1, {"id": 1, "v": 1, "big": big, "at": "2026-10-02T10:00:00+00:00"}, lsn="0/100")),
            msg("2", 0, "pg:0000abcd:s:0000000000000100:1", ev("r", 2, {"id": 2, "v": 2, "big": big, "at": None}, lsn="0/100", seq=1)),
            msg("3", 0, "pg:0000abcd:s:0000000000000100:2", ev("r", 3, {"id": 3, "v": 3, "big": None, "at": None}, lsn="0/100", seq=2)),
            msg("2", 1, "pg:0000abcd:0000000000000200:0", ev("u", 2, {"id": 2, "v": 20, "at": None}, ["big"], lsn="0/200")),
            msg("3", 1, "pg:0000abcd:0000000000000200:1", ev("d", 3, None, lsn="0/200", seq=1)),
            msg("4", 0, "pg:0000abcd:0000000000000300:0", ev("c", 4, {"id": 4, "v": Decimal("4.50"), "big": "b", "at": "2026-10-02 10:00:00+00"}, lsn="0/300")),
        ]

    table = {
        (1,): {"id": 1, "v": 1, "big": big, "at": "2026-10-02T10:00:00.000+00:00"},
        (2,): {"id": 2, "v": 20, "big": big, "at": None},
        (4,): {"id": 4, "v": Decimal("4.5"), "big": "b", "at": "2026-10-02T10:00:00+00:00"},
    }
    fails = []

    def run(msgs, want_err=None, want_problem=None, label=""):
        a = analyze({"q": msgs}, [spec])
        probs = compare(a, spec, table)
        if want_err is None and want_problem is None:
            if a.errors or probs:
                fails.append(f"{label}: a correct queue was refused: {a.errors} {probs}")
            return
        if want_err and not any(want_err in e for e in a.errors):
            fails.append(f"{label}: expected an error containing {want_err!r}, got {a.errors}")
        if want_problem and not any(want_problem in p for p in probs):
            fails.append(f"{label}: expected a difference containing {want_problem!r}, got {probs}")

    run(good(), label="good")
    m = good()
    m[4]["transactionId"] = m[3]["transactionId"]
    run(m, want_err="twice", label="duplicate transactionId")
    m = good()
    m[3]["data"]["lsn"] = "0/50"
    run(m, want_err="below the previous", label="lsn going back")
    m = [x for x in good() if x["data"]["op"] != "c"]
    run(m, want_problem="not in the replay", label="lost insert")
    m = [x for x in good() if x["data"]["op"] != "d"]
    run(m, want_problem="not in the table", label="lost delete")
    m = good()
    m[3]["data"]["after"]["v"] = 21
    run(m, want_problem="rows differ", label="wrong value")
    m = good()
    m.append(msg("4b", 0, "pg:0000abcd:0000000000000400:0", ev("u", 4, {"id": 4, "v": Decimal("4.5"), "big": "b", "at": None}, lsn="0/400")))
    run(m, want_err="two partitions", label="key in two partitions")
    m = [x for x in good() if not (x["data"]["op"] == "r" and x["data"]["key"]["id"] == 2)]
    run(m, want_problem="rows differ", label="unchanged TOAST without an earlier value")
    m = good()
    m[2], m[4] = m[4], m[2]
    m[2]["offset"], m[4]["offset"] = 0, 1
    m[2]["partition"] = m[4]["partition"] = "3"
    run(m, want_err="below the previous", label="delete before the snapshot row (order)")
    m = good()
    m.append(dict(m[1]))
    run(m, want_err="offset delivered twice", label="same offset twice")
    # Key 2's snapshot row was dropped by a window change that left `big`
    # unchanged; the FILL at the high watermark carries it: accepted, applied.
    m = [x for x in good() if not (x["data"]["op"] == "r" and x["data"]["key"]["id"] == 2)]
    m = [dict(x, offset=x["offset"] - 1) if x["partition"] == "2" else x for x in m]
    fillev = ev("u", 2, {"id": 2, "big": big}, ["v", "at"], lsn="0/250")
    del fillev["xid"]
    m.append(msg("2", 1, "pg:0000abcd:s:0000000000000250:f0", fillev))
    a = analyze({"q": m}, [spec])
    probs = compare(a, spec, table)
    if a.errors or probs or a.notes or a.stats.get("fills") != 1:
        fails.append(f"a fill event was not accepted/applied: errors {a.errors} differences {probs} notes {a.notes}")
    run([x for x in m if not is_fill(x["transactionId"])], want_problem="rows differ", label="the same queue without its fill")
    if render_partition(["ap|x", "5"]) != "ap\\|x|5" or render_partition(["c\\d", None]) != "c\\\\d|\\N" or render_partition(["x" * 200])[0] != "~":
        fails.append("render_partition does not follow §4.6")
    return fails
