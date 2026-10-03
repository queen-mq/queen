"""The scenarios (PLAN_PG_CONNECTORS.md §7, (a)–(i)).

Each scenario gets a `Scn`: it names everything the scenario creates (tables,
connectors, slots, publications, a database, progress rows) and cleans all of
it up in `cleanup()`, which runs whatever happened. Every check is made on
what the data says — the queue read back message by message, the tables read
back row by row — never on what a connector says it did.
"""

import json
import random
import re
import shutil
import threading
import time
from collections import Counter, defaultdict
from decimal import Decimal

import checks
from checks import TableSpec, analyze, compare, summarize
from pg import PgError, Writer, ident, lit, lsn_int, lsn_str, rand_text, rnd_word
from queen import (
    Broker,
    Failed,
    QueueReader,
    error_code_of,
    find_lease,
    leader_of,
    make_cluster,
    phase_of,
    push,
    short,
    status_of,
    wait_caught_up,
    wait_leader,
)

LIVE_PHASES = ("connecting", "snapshot", "streaming", "waiting_for_slot", "running")


# =============================================================================================
# Per-scenario resources
# =============================================================================================


class Scn:
    def __init__(self, h, tag):
        self.h = h
        self.pg = h.pg
        self.tag = tag
        self.brokers = []
        self.connectors = []  # names
        self.slots = []
        self.pubs = []  # (name, db)
        self.tables = []  # (table, db)
        self.dbs = []
        self.progress = []  # (table, db, connector name)
        self.drop_if_created = []  # (kind, name, db): objects the sink may create
        self.notes = []
        self.findings = []
        self.cleanup_log = []
        self.threads = []  # writers, samplers, watches: stopped first in cleanup

    def bg(self, obj):
        """Register a background helper (anything with stop_evt) and start it."""
        self.threads.append(obj)
        return obj.start()

    def writer(self, gen, rnd, name, per_call=10, db=None):
        return self.bg(Writer(self.pg, gen, random.Random(rnd.random()), per_call=per_call, name=name, db=db))

    # names

    def conn_name(self, suffix):
        return f"pgc-{self.h.rid}-{self.tag}-{suffix}"

    @staticmethod
    def slot_of(name):
        return "queen_" + name.replace("-", "_")

    def table(self, short_name):
        return f"{self.h.schema}.{self.tag}_{short_name}"

    # objects

    def create_table(self, table, ddl, db=None, extra=""):
        self.pg.run(f"DROP TABLE IF EXISTS {ident(table)}; CREATE TABLE {ident(table)} ({ddl}); {extra}", db=db)
        self.tables.append((table, db))

    def broker(self, name=None):
        b = Broker(self.h, name or f"{self.tag}-node")
        self.brokers.append(b)
        return b

    def cluster(self, n=3):
        nodes = make_cluster(self.h, f"{self.tag}", n)
        self.brokers.extend(nodes)
        return nodes

    def start(self, b):
        b.start()
        b.wait_healthy()
        b.connectors()  # raises Unsupported on a binary without the routes
        return b

    def put_source(self, b, name, doc):
        self.connectors.append(name)
        self.slots.append(self.slot_of(name))
        self.pubs.append((self.slot_of(name), None))
        return b.put_connector(name, doc)

    def put_sink(self, b, name, doc, progress_table, db=None):
        self.connectors.append(name)
        self.progress.append((progress_table, db, name))
        return b.put_connector(name, doc)

    def note(self, msg):
        self.notes.append(msg)
        self.h.say(f"    note: {msg}")

    def finding(self, msg):
        self.findings.append(msg)
        self.h.say(f"    FINDING: {msg}")

    def cleanup(self):
        """Everything this scenario created, whatever happened. Slots and
        publications ALWAYS go (a leaked slot pins WAL on a shared server);
        tables stay only with --keep."""
        h, pg = self.h, self.pg
        for t in self.threads:
            t.stop_evt.set()
        for t in self.threads:
            t.thread.join(30)
        live = [b for b in self.brokers if b.alive()]
        # The API's own teardown first (DELETE ?dropSlot=true), best effort.
        for name in self.connectors:
            for b in live:
                try:
                    st, _, raw = b.delete_connector(name, drop_slot=True)
                    self.cleanup_log.append(f"DELETE {name}?dropSlot=true -> {st}")
                    break
                except OSError:
                    continue
        if live and self.slots:
            deadline = time.time() + 8
            while time.time() < deadline and any(pg.slot(s) for s in self.slots):
                time.sleep(0.3)
            left = [s for s in self.slots if pg.slot(s)]
            if left:
                self.cleanup_log.append(f"slots still present 8 s after DELETE ?dropSlot=true: {left}")
        h.kill_all()
        for s in self.slots:
            try:
                if not pg.drop_slot(s):
                    self.cleanup_log.append(f"could not drop slot {s}")
            except PgError as e:
                self.cleanup_log.append(f"drop slot {s}: {e}")
        for name, db in self.pubs:
            try:
                pg.drop_publication(name, db=db)
            except PgError:
                pass
        for table, db, name in self.progress:
            try:
                if pg.table_exists(table, db=db):
                    pg.run(f"DELETE FROM {ident(table)} WHERE sink LIKE {lit('%/' + name)}", db=db, su=True, check=False)
            except PgError:
                pass
        for kind, name, db in self.drop_if_created:
            try:
                if kind == "table" and pg.table_exists(name, db=db):
                    if pg.count(name, db=db) == 0:
                        pg.run(f"DROP TABLE IF EXISTS {ident(name)}", db=db, su=True, check=False)
                elif kind == "schema":
                    empty = pg.value(
                        f"SELECT count(*) = 0 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = {lit(name)}",
                        db=db,
                        su=True,
                    )
                    if empty == "t":
                        pg.run(f"DROP SCHEMA IF EXISTS {ident(name)}", db=db, su=True, check=False)
            except PgError:
                pass
        if not h.args.keep:
            for table, db in self.tables:
                pg.run(f"DROP TABLE IF EXISTS {ident(table)} CASCADE", db=db, su=True, check=False)
            for db in self.dbs:
                pg.drop_database(db)
            for b in self.brokers:
                shutil.rmtree(b.data_dir, ignore_errors=True)
        # Final word on slots: none of ours may survive.
        left = [s for s in self.slots if pg.slot(s)]
        if left:
            self.cleanup_log.append(f"LEAKED SLOTS {left}")
            h.say(f"    WARNING: slots {left} could not be dropped")


# =============================================================================================
# Small helpers
# =============================================================================================


def conn(h, db=None):
    pg = h.pg
    return {
        "host": pg.host,
        "port": pg.port,
        "database": db or pg.db,
        "user": pg.user,
        "password": pg.password,
        "sslMode": "disable",
        "connectTimeoutMs": 5000,
    }


def source_doc(h, tables, **opts):
    src = {"tables": tables}
    src.update(opts)
    return {"kind": "source", "enabled": True, "connection": conn(h), "source": src}


def sink_doc(h, queue, table, mode, db=None, **opts):
    snk = {"queue": queue, "table": table, "mode": mode}
    snk.update(opts)
    return {"kind": "sink", "enabled": True, "connection": conn(h, db), "sink": snk}


def wait_for(pred, timeout, what, interval=0.25, diag=None):
    deadline = time.time() + timeout
    while True:
        try:
            v = pred()
        except (OSError, Failed):
            v = None
        if v:
            return v
        if time.time() > deadline:
            extra = ""
            if diag:
                try:
                    extra = "\n" + diag()
                except Exception as e:  # noqa: BLE001
                    extra = f"\n(diagnostics failed: {e})"
            raise Failed(f"timed out after {timeout:.0f}s waiting for {what}{extra}")
        time.sleep(interval)


def pointer(brokers, name):
    """The source pointer (§4.2) read through the first live node that
    answers, or None."""
    for b in brokers:
        if not b.alive():
            continue
        try:
            return b.kv_value(f"src:{name}:pointer")
        except (OSError, Failed):
            continue
    return None


def lease(brokers, name, exclude=None):
    """(found, value, expiresAt) of the source lease, via a live node."""
    for b in brokers:
        if not b.alive() or b is exclude:
            continue
        try:
            r = b.kv_get(f"src:{name}:lease")
            return bool(r.get("found")), r.get("value"), r.get("expiresAt"), r.get("version")
        except (OSError, Failed):
            continue
    return None


def node_of(brokers, lease_value):
    """The broker a lease value names (QUEEN_SERVER_ID = the broker's name)."""
    if not isinstance(lease_value, dict):
        return None
    node = str(lease_value.get("node", ""))
    for b in brokers:
        if node == b.name or node == str(b.node_id) or node.endswith(b.name):
            return b
    return None


def conn_diag(brokers, names, n_log=15):
    lines = []
    for b in brokers:
        if not b.alive():
            lines.append(f"  {b.name}: not running")
            continue
        for name in names:
            try:
                e = b.connector(name)
                lines.append(f"  {b.name} {name}: {short(json.dumps(status_of(e), default=str), 600)}")
            except Exception as ex:  # noqa: BLE001
                lines.append(f"  {b.name} {name}: GET failed: {ex}")
        lines.append(b.tail(n_log, pattern=r"queen-pg|queen_pg|pg_inproc|connector|ERROR|panick"))
    return "\n".join(lines)


def find_number(entry, key):
    """The first numeric `key` in a nested status object."""
    out = []

    def walk(v):
        if isinstance(v, dict):
            x = v.get(key)
            if isinstance(x, (int, float, Decimal)) and not isinstance(x, bool):
                out.append(x)
            for y in v.values():
                walk(y)
        elif isinstance(v, list):
            for y in v:
                walk(y)

    walk(entry)
    return out[0] if out else None


def metric_sum(text, name, connector):
    total = None
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        m = re.match(re.escape(name) + r"(\{[^}]*\})?\s+([0-9.eE+-]+)$", line.strip())
        if m and f'connector="{connector}"' in (m.group(1) or ""):
            total = (total or 0) + float(m.group(2))
    return total


class InvariantSampler:
    """§4.4: confirmed_flush_lsn <= pointer.lsn, always. Samples the slot
    FIRST and the pointer SECOND (both only grow), so a sample can only err on
    the safe side; a violation is real."""

    def __init__(self, h, brokers, name, slot):
        self.h = h
        self.brokers = brokers
        self.name = name
        self.slot = slot
        self.samples = 0
        self.violations = []
        self.max_lag = 0
        self.stop_evt = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        while not self.stop_evt.is_set():
            try:
                sl = self.h.pg.slot(self.slot)
                ptr = pointer(self.brokers, self.name)
                if sl and ptr and sl.get("confirmed_flush_lsn") is not None and ptr.get("lsn"):
                    if ptr.get("slot") not in (None, self.slot):
                        pass
                    p = lsn_int(ptr["lsn"])
                    c = sl["confirmed_flush_lsn"]
                    self.samples += 1
                    if c > p:
                        self.violations.append((time.strftime("%H:%M:%S"), lsn_str(c), ptr["lsn"]))
                    self.max_lag = max(self.max_lag, p - c)
            except Exception:  # noqa: BLE001 — a broker mid-restart answers nothing
                pass
            self.stop_evt.wait(0.5)

    def stop(self):
        self.stop_evt.set()
        self.thread.join(10)
        return self


def converge_source(h, s, get_broker, specs, names, timeout=240, only_epoch=None, tag="r", stable_s=3.0):
    """Read the source's queues until replaying them gives exactly the tables.
    Returns (analysis, readers). Fails at once on an invariant violation
    (duplicate transactionId, order), and after `timeout` on a replay that
    never matches (with the differences)."""
    readers = {sp.queue: QueueReader(get_broker(), sp.queue, f"{h.rid}{s.tag}{tag}") for sp in specs}
    deadline = time.time() + timeout
    t0 = time.time()
    rounds = 0
    while True:
        rounds += 1
        b = get_broker()
        for r in readers.values():
            r.set_broker(b)
            r.drain()
        a = analyze({q: r.messages for q, r in readers.items()}, specs, only_epoch=only_epoch)
        if a.errors:
            raise Failed("invariant violated in the queue:\n  " + "\n  ".join(a.errors[:15]) + f"\n  ({summarize(a)})")
        problems = []
        for sp in specs:
            problems += compare(a, sp, h.pg.table_json(sp.table, sp.key_cols))
        if not problems:
            break
        if time.time() > deadline:
            raise Failed(
                f"the queue never replays to the table ({timeout}s after the writes stopped, {rounds} reads):\n  "
                + "\n  ".join(problems[:30])
                + f"\n  ({summarize(a)})\n"
                + conn_diag([x for x in s.brokers if x.alive()], names)
            )
        time.sleep(1.5)
    waited = time.time() - t0
    # Stable: nothing new arrives once it matched (a late duplicate would).
    time.sleep(stable_s)
    before = {q: len(r.messages) for q, r in readers.items()}
    b = get_broker()
    for r in readers.values():
        r.set_broker(b)
        r.drain()
    late = {q: len(r.messages) - before[q] for q, r in readers.items() if len(r.messages) != before[q]}
    a = analyze({q: r.messages for q, r in readers.items()}, specs, only_epoch=only_epoch)
    if a.errors:
        raise Failed("invariant violated after convergence:\n  " + "\n  ".join(a.errors[:15]))
    problems = []
    for sp in specs:
        problems += compare(a, sp, h.pg.table_json(sp.table, sp.key_cols))
    if problems:
        raise Failed(f"the replay matched, then {late} more messages broke it:\n  " + "\n  ".join(problems[:20]))
    # Complete: every offset of every partition was read exactly once.
    for sp in specs:
        r = readers[sp.queue]
        if r.dup_deliveries:
            raise Failed(f"{sp.queue}: the reader was handed {len(r.dup_deliveries)} offsets twice, e.g. {r.dup_deliveries[:3]}")
        gaps = r.complete_against(b.partitions(sp.queue))
        if gaps:
            raise Failed(f"{sp.queue}: the reader did not see every offset:\n  " + "\n  ".join(gaps[:10]))
    a.waited = waited
    a.late = late
    for n in a.notes:
        s.note(n)
    return a, readers


def check_redacted(h, b, name):
    """§3.1: reads never return the password nor its sealed form, and the
    broker never logs it. Returns the evidence line; raises on a leak."""
    pw = h.pg.password
    st, doc, raw = b.call("GET", f"/api/v1/connectors/{name}", timeout=10)
    st2, _, raw2 = b.call("GET", "/api/v1/connectors", timeout=10)
    conn_ = (doc or {}).get("connection") or {}
    leaks = []
    if pw.encode() in (raw or b"") or pw.encode() in (raw2 or b""):
        leaks.append("the password text is in a GET answer")
    if "passwordSealed" in conn_ or b"passwordSealed" in (raw2 or b""):
        leaks.append("passwordSealed is in a GET answer")
    if pw in b.log(whole=True):
        leaks.append("the password text is in the broker log")
    if leaks:
        raise Failed(f"password leak: {leaks}")
    return f"password redacted (connection.password = {conn_.get('password')!r}), not in the log"


def wait_snapshot_done(brokers, name, timeout, diag_names):
    def done():
        p = pointer(brokers, name)
        return p if (p and p.get("snapshot") is None and p.get("lsn")) else None

    return wait_for(done, timeout, f"the snapshot of {name} to finish (pointer.snapshot = null)", 0.5,
                    diag=lambda: conn_diag([b for b in brokers if b.alive()], diag_names))


def wait_pointer(brokers, name, timeout, diag_names):
    return wait_for(lambda: pointer(brokers, name), timeout, f"the pointer of {name} to exist", 0.3,
                    diag=lambda: conn_diag([b for b in brokers if b.alive()], diag_names))


def amount_text(rnd):
    cents = rnd.randint(-50_000, 150_000)
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def json_doc(rnd, i):
    """A jsonb value as JSON text: big ints past 2^53, decimals, escapes."""
    word = rnd_word(rnd)
    return json.dumps(
        {
            "i": i,
            "big": 9007199254740993 + rnd.randint(0, 10**6),
            "neg": -(2**62) + rnd.randint(0, 1000),
            "d": float(f"0.{rnd.randint(1, 999)}"),
            "s": word,
            "nested": {"arr": [1, "two", None, True, {"k": [3.25]}], "empty": {}},
        },
        ensure_ascii=False,
    )


def sql_tags(rnd):
    r = rnd.random()
    if r < 0.1:
        return "NULL"
    if r < 0.2:
        return "'{}'::text[]"
    items = [rnd.choice(["a", "b,c", 'q"uote', "br{ace}", "sp ace", "uni é", "back\\slash"]) for _ in range(rnd.randint(1, 4))]
    if rnd.random() < 0.2:
        return "ARRAY[" + ", ".join([lit(x) for x in items] + ["NULL"]) + "]::text[]"
    return "ARRAY[" + ", ".join(lit(x) for x in items) + "]::text[]"


# =============================================================================================
# (a) and (b): snapshot + stream with concurrent writes, exactly once per key
# =============================================================================================


REGIONS = ["eu", "us", "ap|x", "c\\d"]


def gen_ab(t_single, t_comp, n1, n2):
    st = {"ns": 10_000_000, "nc": 10_000_000}

    def pick_single(rnd):
        if st["ns"] > 10_000_000 and rnd.random() < 0.3:
            return rnd.randint(10_000_001, st["ns"])
        return rnd.randint(1, n1)

    def pick_comp(rnd):
        if st["nc"] > 10_000_000 and rnd.random() < 0.3:
            return rnd.choice(REGIONS + ["zz"]), rnd.randint(10_000_001, st["nc"])
        i = rnd.randint(1, n2)
        return REGIONS[i % 4], i

    def ins_single(rnd, i):
        return (
            f"INSERT INTO {t_single} (id, val, label, amount, flag, doc, tags, at) VALUES ("
            f"{i}, {rnd.randint(0, 999)}, {lit(rnd_word(rnd))}, {amount_text(rnd)}, "
            f"{rnd.choice(['true', 'false', 'NULL'])}, {lit(json_doc(rnd, i))}::jsonb, {sql_tags(rnd)}, now())"
        )

    def gen(rnd):
        stmts = []
        for _ in range(rnd.randint(1, 5)):
            r = rnd.random()
            if r < 0.14:
                st["ns"] += 1
                stmts.append(ins_single(rnd, st["ns"]))
            elif r < 0.24:
                st["nc"] += 1
                reg = rnd.choice(REGIONS)
                stmts.append(f"INSERT INTO {t_comp} (region, id, qty, note) VALUES ({lit(reg)}, {st['nc']}, {rnd.randint(0, 99)}, {lit(rnd_word(rnd))})")
            elif r < 0.50:
                i = pick_single(rnd)
                sets = rnd.choice(
                    [
                        f"val = val + {rnd.randint(1, 9)}",
                        f"label = {lit(rnd_word(rnd))}, amount = {amount_text(rnd)}",
                        f"doc = {lit(json_doc(rnd, i))}::jsonb, flag = NOT coalesce(flag, false)",
                        f"tags = {sql_tags(rnd)}, at = now()",
                        "label = NULL, amount = NULL",
                    ]
                )
                stmts.append(f"UPDATE {t_single} SET {sets} WHERE id = {i}")
            elif r < 0.60:
                reg, i = pick_comp(rnd)
                stmts.append(f"UPDATE {t_comp} SET qty = qty + 1, note = {lit(rnd_word(rnd))} WHERE region = {lit(reg)} AND id = {i}")
            elif r < 0.66:
                lo = rnd.randint(1, max(1, n1 - 5))
                stmts.append(f"UPDATE {t_single} SET val = val + 1 WHERE id BETWEEN {lo} AND {lo + rnd.randint(1, 5)}")
            elif r < 0.73:
                stmts.append(f"DELETE FROM {t_single} WHERE id = {pick_single(rnd)}")
            elif r < 0.78:
                reg, i = pick_comp(rnd)
                stmts.append(f"DELETE FROM {t_comp} WHERE region = {lit(reg)} AND id = {i}")
            elif r < 0.83:
                # The key changes: d to the old partition, c to the new one.
                st["ns"] += 1
                stmts.append(f"UPDATE {t_single} SET id = {st['ns']}, val = val + 100 WHERE id = {pick_single(rnd)}")
            elif r < 0.86:
                reg, i = pick_comp(rnd)
                st["nc"] += 1
                stmts.append(f"UPDATE {t_comp} SET region = {lit(rnd.choice(REGIONS + ['zz']))}, id = {st['nc']} WHERE region = {lit(reg)} AND id = {i}")
            elif r < 0.91:
                i = pick_single(rnd)
                stmts.append(f"UPDATE {t_single} SET val = val + 1 WHERE id = {i}")
                stmts.append(f"UPDATE {t_single} SET label = 'twice' WHERE id = {i}")
            elif r < 0.95:
                st["ns"] += 1
                stmts.append(ins_single(rnd, st["ns"]))
                stmts.append(f"UPDATE {t_single} SET val = -1 WHERE id = {st['ns']}")
                if rnd.random() < 0.5:
                    stmts.append(f"DELETE FROM {t_single} WHERE id = {st['ns']}")
            else:
                stmts.append(f"UPDATE {t_single} SET val = val + 1 WHERE id % 997 = {rnd.randint(0, 996)} AND id <= {n1}")
        return "BEGIN;\n" + ";\n".join(stmts) + ";\nCOMMIT;\n"

    return gen


def source_exact(h, s, kills, chunk=250):
    pg = h.pg
    rnd = h.rnd(s.tag)
    n = h.scaled(20_000)
    n1, n2 = n // 2, n - n // 2
    t_single, t_comp = s.table("single"), s.table("comp")
    q_single, q_comp = f"{s.tag}-single", f"{s.tag}-comp"
    s.create_table(t_single, "id bigint PRIMARY KEY, val int, label text, amount numeric(20,6), flag bool, doc jsonb, tags text[], at timestamptz")
    s.create_table(t_comp, "region text, id int, qty int, note text, PRIMARY KEY (region, id)")
    pg.run(
        f"INSERT INTO {t_single} SELECT i, (i * 7) % 1000, 'row ' || i, (i * 1.25)::numeric(20,6), i % 2 = 0, "
        f"jsonb_build_object('i', i, 'big', 9007199254740993 + i, 'd', 0.5 + i, 's', 'v\"' || i), "
        f"ARRAY['t' || (i % 5), 'u'], timestamptz '2026-10-02 10:00:00+00' + i * interval '1.5 second' "
        f"FROM generate_series(1, {n1}) i;\n"
        f"INSERT INTO {t_comp} SELECT (ARRAY['eu','us','ap|x','c\\d'])[1 + i % 4], i, i % 17, 'n' || i FROM generate_series(1, {n2}) i;"
    )
    specs = [
        TableSpec(t_single, q_single, ["id"], ts_cols=["at"]),
        TableSpec(t_comp, q_comp, ["region", "id"]),
    ]
    b = s.start(s.broker())
    b.configure_queue(q_single)
    b.configure_queue(q_comp)
    name = s.conn_name("src")
    slot = s.slot_of(name)
    writer = s.writer(gen_ab(t_single, t_comp, n1, n2), rnd, f"{s.tag}-writer")
    time.sleep(1.0)
    doc = source_doc(
        h,
        [{"table": t_single, "queue": q_single}, {"table": t_comp, "queue": q_comp}],
        snapshotChunkRows=chunk,
        maxBundleMessages=500,
        lingerMs=20,
        heartbeatSeconds=2,
    )
    t0 = time.time()
    s.put_source(b, name, doc)
    sampler = s.bg(InvariantSampler(h, [b], name, slot))
    ptr0 = wait_pointer([b], name, 60, [name])
    redaction = check_redacted(h, b, name)
    epoch = ptr0.get("epoch")
    h.say(f"  {s.tag}: source {name} created; epoch {epoch}, slot {slot}, pointer {ptr0.get('lsn')} (+{time.time() - t0:.1f}s)")
    kill_log = []
    snap_targets = [n * rnd.uniform(0.15, 0.35), n * rnd.uniform(0.55, 0.8)]
    for k in range(kills):
        if k < len(snap_targets):
            # Inside the snapshot: when the pointer's snapshot progress passes
            # the target (it moves once per committed chunk).
            deadline = time.time() + 120
            while time.time() < deadline:
                p = pointer([b], name) or {}
                sp = p.get("snapshot")
                if not p.get("lsn") or (sp and (sp.get("rows") or 0) < snap_targets[k]):
                    time.sleep(0.01)
                    continue
                break
        else:
            time.sleep(rnd.uniform(1.0, 5.0))
        p = pointer([b], name) or {}
        where = "snapshot" if p.get("snapshot") else ("split txn" if p.get("inTxn") else "stream")
        down, up = b.crash_restart()
        kill_log.append(f"#{k + 1} in {where} at lsn {p.get('lsn')} (rows {((p.get('snapshot') or {}).get('rows'))}), back in {down + up:.1f}s")
        h.say(f"  {s.tag}: kill -9 {kill_log[-1]}")
    ptr = wait_snapshot_done([b], name, 600, [name])
    t_snap = time.time() - t0
    h.say(f"  {s.tag}: snapshot done {t_snap:.1f}s after the connector was created; pointer {ptr.get('lsn')}")
    time.sleep(8.0)
    wst = writer.stop()
    h.say(f"  {s.tag}: writer stopped: {wst}")
    a, readers = converge_source(h, s, lambda: b, specs, [name], timeout=240)
    sampler.stop()
    # One epoch: a crash never re-snapshots.
    epochs = [e for e in a.epochs if e]
    if len(epochs) != 1:
        raise Failed(f"messages of {len(epochs)} epochs ({dict(a.epochs)}) without a resync: the pointer was lost")
    if sampler.violations:
        raise Failed(f"confirmed_flush_lsn ran ahead of the pointer (§4.4 invariant) in {len(sampler.violations)} of {sampler.samples} samples: {sampler.violations[:5]}")
    counts = {sp.table.split(".")[-1]: pg.count(sp.table) for sp in specs}
    pfinal = pointer([b], name) or {}
    sl = pg.slot(slot) or {}
    line = (
        f"{n} rows snapshotted ({t_snap:.1f}s) under {wst['txns_ok']} writer txns ({wst['failed_calls']} failed calls); "
        f"{summarize(a)}; replay == SELECT for {counts} {a.waited:.1f}s after the writes stopped; "
        f"transactionIds unique, lsn monotonic per partition, every offset read once; "
        f"slot invariant confirmed<=pointer held in {sampler.samples} samples; pointer {pfinal.get('lsn')} slot {lsn_str(sl['confirmed_flush_lsn']) if sl.get('confirmed_flush_lsn') else None}"
    )
    if kill_log:
        line += "; kills: " + "; ".join(kill_log)
    st = status_of(b.connector(name) or {})
    line += f"; status phase {st.get('phase')!r}; {redaction}"
    return line


def scenario_a(h, s):
    return source_exact(h, s, kills=0)


def scenario_b(h, s):
    # 100-row chunks: the snapshot takes long enough for the first two kills.
    return source_exact(h, s, kills=5, chunk=100)


# =============================================================================================
# (c) three nodes: takeover, hand-over, then a sink on every node
# =============================================================================================


def gen_simple(table, n, extra_cols=True):
    st = {"next": 10_000_000}

    def gen(rnd):
        stmts = []
        for _ in range(rnd.randint(1, 4)):
            r = rnd.random()
            if r < 0.2:
                st["next"] += 1
                stmts.append(f"INSERT INTO {table} (id, val, label) VALUES ({st['next']}, {rnd.randint(0, 99)}, {lit(rnd_word(rnd))})")
            elif r < 0.75:
                i = rnd.randint(1, n) if st["next"] == 10_000_000 or rnd.random() < 0.8 else rnd.randint(10_000_001, st["next"])
                stmts.append(f"UPDATE {table} SET val = val + 1, label = {lit(rnd_word(rnd))} WHERE id = {i}")
            elif r < 0.9:
                stmts.append(f"DELETE FROM {table} WHERE id = {rnd.randint(1, n)}")
            else:
                st["next"] += 1
                stmts.append(f"UPDATE {table} SET id = {st['next']} WHERE id = {rnd.randint(1, n)}")
        return "BEGIN;\n" + ";\n".join(stmts) + ";\nCOMMIT;\n"

    return gen


class LeaseWatch:
    """Reads the source lease through `via` every 50 ms; tells whether the row
    of `node` went (absent, or another node's) BEFORE its own expiresAt —
    only a release can do that."""

    def __init__(self, via, name, node, nodes):
        self.via = via
        self.name = name
        self.node = node
        self.nodes = nodes
        self.obs = []
        self.stop_evt = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        while not self.stop_evt.is_set():
            try:
                r = self.via.kv_get(f"src:{self.name}:lease")
                self.obs.append((time.time(), r))
            except Exception:  # noqa: BLE001
                pass
            self.stop_evt.wait(0.05)

    def stop(self):
        from datetime import datetime

        self.stop_evt.set()
        self.thread.join(5)
        last_mine = None
        for t, r in self.obs:
            nb = node_of(self.nodes, r.get("value")) if r.get("found") else None
            holder = nb.name if nb else (((r.get("value") or {}).get("node")) if r.get("found") else None)
            if nb is self.node:
                last_mine = r
                continue
            if last_mine is None:
                continue
            exp = last_mine.get("expiresAt")
            exp_s = None
            if exp:
                try:
                    exp_s = datetime.fromisoformat(exp.replace("Z", "+00:00")).timestamp()
                except ValueError:
                    exp_s = None
            return {"seen_at": t, "now": holder or "absent", "before_expiry": exp_s is not None and t < exp_s,
                    "margin_s": (exp_s - t) if exp_s else None}
        return {"before_expiry": False, "observations": len(self.obs), "last_mine": last_mine}


def scenario_c(h, s):
    pg = h.pg
    rnd = h.rnd("c")
    ttl = h.lease_ttl_ms / 1000
    n = h.scaled(10_000)
    t_src, t_copy = s.table("src"), s.table("copy")
    q = "c-src"
    cols = "id bigint PRIMARY KEY, val int, label text, note text"
    s.create_table(t_src, cols)
    s.create_table(t_copy, cols)
    pg.run(f"INSERT INTO {t_src} SELECT i, i % 100, 'l' || i, repeat('n', i % 50) FROM generate_series(1, {n}) i")
    spec = TableSpec(t_src, q, ["id"])
    nodes = s.cluster(3)
    for b in nodes:
        b.start()
    for b in nodes:
        b.wait_healthy(120)
    leader = wait_leader(nodes)
    nodes[0].connectors()
    leader.configure_queue(q)
    report = []
    name = s.conn_name("src")
    slot = s.slot_of(name)
    writer = s.writer(gen_simple(t_src, n), rnd, "c-writer")
    # Small chunks and an early kill: the takeover lands mid-snapshot, so the
    # new owner resumes the snapshot from the pointer.
    s.put_source(nodes[0], name, source_doc(h, [{"table": t_src, "queue": q}], snapshotChunkRows=100, maxBundleMessages=500, heartbeatSeconds=2))
    sampler = s.bg(InvariantSampler(h, nodes, name, slot))

    def owner():
        l = lease(nodes, name)
        if l and l[0]:
            return node_of(nodes, l[1])
        return None

    own = wait_for(owner, 60, f"a node to hold the lease of {name}", 0.2, diag=lambda: conn_diag(nodes, [name]))
    wait_pointer(nodes, name, 60, [name])
    phases = {}
    for b in nodes:
        e = b.connector(name)
        phases[b.name] = phase_of(e)
        if b is not own and phase_of(e) not in (None, "standby"):
            s.note(f"{b.name} is not the lease owner but reports phase {phase_of(e)!r}")
    api_lease = find_lease(own.connector(name) or {})
    h.say(f"  c: roles { {b.name: b.role() for b in nodes} }; owner {own.name}; phases {phases}; API lease {api_lease}")
    report.append(f"owner {own.name} ({own.role()}), phases {phases}")

    # --- kill -9 the owner, mid-snapshot ------------------------------------------------
    target = n * rnd.uniform(0.2, 0.6)
    deadline = time.time() + 120
    while time.time() < deadline:
        p = pointer(nodes, name) or {}
        sp = p.get("snapshot")
        if p.get("lsn") and (not sp or (sp.get("rows") or 0) >= target):
            break
        time.sleep(0.01)
    p_before = pointer(nodes, name) or {}
    victim = own
    survivors = [b for b in nodes if b is not victim]
    t_kill = victim.kill9()
    victim.wait_exit(10, expect=None)
    claimed = {}

    def taken():
        l = lease(survivors, name)
        if l and l[0]:
            nb = node_of(nodes, l[1])
            if nb is not None and nb is not victim:
                claimed.setdefault("node", nb)
                claimed.setdefault("t", time.time() - t_kill)
                return nb
        return None

    new_own = wait_for(taken, 2 * ttl + 20, f"another node to take the lease after kill -9 of {victim.name}", 0.1,
                       diag=lambda: conn_diag(survivors, [name]))
    t_claim = claimed["t"]

    def resumed():
        p = pointer(survivors, name) or {}
        moved = p.get("lsn") and p_before.get("lsn") and lsn_int(p["lsn"]) > lsn_int(p_before["lsn"])
        snap_moved = (p.get("snapshot") or {}).get("rows") != (p_before.get("snapshot") or {}).get("rows")
        return (moved or snap_moved) and p

    wait_for(resumed, 60, f"the new owner {new_own.name} to move the pointer", 0.2, diag=lambda: conn_diag(survivors, [name]))
    t_resume = time.time() - t_kill
    if t_claim > 2 * ttl + 10:
        raise Failed(f"takeover took {t_claim:.1f}s after kill -9, more than 2 x TTL + 10 s")
    where = "snapshot" if p_before.get("snapshot") else "stream"
    report.append(f"kill -9 {victim.name} (in {where}): lease taken by {new_own.name} in {t_claim:.1f}s (TTL {ttl:.0f}s), pointer moving again at {t_resume:.1f}s")
    h.say(f"  c: {report[-1]}")
    victim.start()
    victim.wait_healthy(120)
    wait_caught_up(nodes, victim)

    # --- SIGTERM the new owner: a released lease is handed over at once -----------------
    time.sleep(1.5)
    stopping = new_own
    others = [b for b in nodes if b is not stopping]
    watch = s.bg(LeaseWatch(others[0], name, stopping, nodes))
    time.sleep(0.3)
    t_term = stopping.sigterm()
    try:
        rc = stopping.proc.wait(30)
    except Exception:  # noqa: BLE001
        stopping.kill9()
        raise Failed(f"{stopping.name} did not exit within 30 s of SIGTERM\n{stopping.tail()}") from None
    exit_s = time.time() - t_term
    h.live.discard(stopping)
    claimed.clear()

    def taken2():
        l = lease(others, name)
        if l and l[0]:
            nb = node_of(nodes, l[1])
            if nb is not None and nb is not stopping:
                return nb
        return None

    next_own = wait_for(taken2, 2 * ttl + 20, f"a node to take the lease after SIGTERM of {stopping.name}", 0.05,
                        diag=lambda: conn_diag(others, [name]))
    t_hand = time.time() - t_term
    rel = watch.stop()
    report.append(
        f"SIGTERM {stopping.name}: exit rc={rc} in {exit_s:.2f}s, lease "
        + ("RELEASED" if rel.get("before_expiry") else "NOT released (expired)")
        + (f" {rel['margin_s']:.1f}s before its expiry" if rel.get("before_expiry") and rel.get("margin_s") is not None else "")
        + f", taken by {next_own.name} {t_hand:.1f}s after the signal"
    )
    h.say(f"  c: {report[-1]}")
    deferred = []
    if rc != 0:
        deferred.append(f"{stopping.name} exited {rc} on SIGTERM")
    if not rel.get("before_expiry"):
        deferred.append(f"SIGTERM of the owner did not release the lease (hand-over waited for expiry: {t_hand:.1f}s; {rel})")
    stopping.start()
    stopping.wait_healthy(120)
    wait_caught_up(nodes, stopping)
    time.sleep(3.0)
    wst = writer.stop()
    wait_snapshot_done(nodes, name, 300, [name])
    a, _ = converge_source(h, s, lambda: leader_of(nodes) or nodes[0], [spec], [name], timeout=240)
    sampler.stop()
    epochs = [e for e in a.epochs if e]
    if len(epochs) != 1:
        raise Failed(f"messages of {len(epochs)} epochs ({dict(a.epochs)}) after a takeover: the pointer was not continued")
    if sampler.violations:
        raise Failed(f"confirmed_flush_lsn ran ahead of the pointer in {len(sampler.violations)} samples: {sampler.violations[:5]}")
    report.append(f"writer {wst['txns_ok']} txns; {summarize(a)}; replay == SELECT ({pg.count(t_src)} rows), ids unique, lsn monotonic")
    h.say(f"  c: {report[-1]}")

    # --- a sink on all three nodes ---------------------------------------------------------
    sname = s.conn_name("sink")
    prog = s.table("progress")
    t_s0 = time.time()
    s.put_sink(nodes[1], sname, sink_doc(h, q, t_copy, "cdc", batch=100, workers=2, leaseSeconds=10, progressTable=prog), prog)
    s.tables.append((prog, None))

    def copied():
        return pg.table_digest(t_copy, ["id"]) == pg.table_digest(t_src, ["id"])

    def sink_diag():
        a_, b_ = pg.table_digest(t_src, ["id"]), pg.table_digest(t_copy, ["id"])
        miss = [k for k in a_ if k not in b_]
        diff = [k for k in a_ if k in b_ and a_[k] != b_[k]]
        extra = [k for k in b_ if k not in a_]
        return f"copy: {len(b_)} rows, source {len(a_)}; missing {miss[:5]} ({len(miss)}), differ {diff[:5]} ({len(diff)}), extra {extra[:5]}\n" + conn_diag(nodes, [sname])

    wait_for(copied, 240, f"{t_copy} == {t_src} through the sink", 1.0, diag=sink_diag)
    t_sink = time.time() - t_s0
    time.sleep(12)  # past leaseSeconds: a redelivery that double-applied would show now
    if not copied():
        raise Failed("the copy matched, then diverged (a late redelivery applied twice?)\n" + sink_diag())
    applied = {}
    for b in nodes:
        e = b.connector(sname) or {}
        v = find_number(status_of(e), "applied")
        if v is None:
            v = metric_sum(b.metrics(), "queen_pg_sink_applied_total", sname)
        applied[b.name] = v
    workers = [k for k, v in applied.items() if v]
    report.append(f"sink cdc on 3 nodes: copy == source ({pg.count(t_copy)} rows) {t_sink:.1f}s after it was created; applied per node {applied}")
    h.say(f"  c: {report[-1]}")
    if len(workers) < 2:
        deferred.append(f"the sink's work did not spread: applied per node {applied}")
    if deferred:
        raise Failed("; ".join(deferred) + ". Everything else: " + "; ".join(report))
    return "; ".join(report)


# =============================================================================================
# (d) sink exactly-once under kill -9
# =============================================================================================


def scenario_d(h, s):
    pg = h.pg
    rnd = h.rnd("d")
    n_msgs = h.scaled(20_000)
    n_parts = 200
    n_acct = 1000
    t_acct = s.table("accounts")
    s.create_table(t_acct, "id bigint PRIMARY KEY, balance numeric NOT NULL DEFAULT 0")
    precise = list(range(1_000_001, 1_000_011))
    pg.run(f"INSERT INTO {t_acct} (id) SELECT i FROM generate_series(1, {n_acct}) i; INSERT INTO {t_acct} (id) SELECT unnest(ARRAY{precise})")
    # The default progress table (queen.sink_progress): remember what existed.
    had_schema = pg.value("SELECT 1 FROM pg_namespace WHERE nspname = 'queen'", su=True) is not None
    had_table = pg.table_exists("queen.sink_progress")
    if not had_table:
        s.drop_if_created.append(("table", "queen.sink_progress", None))
    if not had_schema:
        s.drop_if_created.append(("schema", "queen", None))
    b = s.start(s.broker())
    q = "d-ledger"
    b.configure_queue(q)
    items = []
    expected = defaultdict(Decimal)
    for i in range(n_msgs):
        part = f"acct-{i % n_parts}"
        if i % 200 == 7:
            acct = rnd.choice(precise)
            amt = f"{rnd.randint(10**18, 10**19)}.{rnd.randint(0, 10**10 - 1):010d}"
        else:
            acct = rnd.randint(1, n_acct)
            amt = amount_text(rnd)
        expected[acct] += Decimal(amt)
        items.append((q, part, f"d-{s.h.rid}-{i}", '{"account_id":%d,"amount":%s,"note":"m%d"}' % (acct, amt, i)))
    t0 = time.time()
    for i in range(0, len(items), 500):
        push([b.url], items[i : i + 500])
    h.say(f"  d: pushed {n_msgs} messages over {n_parts} partitions in {time.time() - t0:.1f}s")
    name = s.conn_name("sink")
    doc = sink_doc(
        h,
        q,
        t_acct,
        "sql",
        statement=f"UPDATE {t_acct} SET balance = balance + $1::numeric WHERE id = $2::bigint",
        params=["$.amount", "$.account_id"],
        batch=10,
        workers=2,
        leaseSeconds=5,
        maxAttempts=3,
    )
    s.put_sink(b, name, doc, "queen.sink_progress")
    sink_like = "%/" + name

    def covered():
        if not pg.table_exists("queen.sink_progress"):
            return 0
        v = pg.value(f"SELECT coalesce(sum(last_offset + 1), 0) FROM queen.sink_progress WHERE sink LIKE {lit(sink_like)}", su=True)
        return int(v or 0)

    bins = [(0.05, 0.18), (0.22, 0.36), (0.40, 0.54), (0.58, 0.72), (0.76, 0.88)]
    targets = [int(n_msgs * rnd.uniform(lo, hi)) for lo, hi in bins]
    kills = []
    t_start = time.time()
    for k, target in enumerate(targets):
        def reached():
            c = covered()
            return c >= target and c

        c = wait_for(reached, 180, f"the sink to cover {target} messages", 0.03, diag=lambda: conn_diag([b], [name]))
        if c >= n_msgs:
            kills.append(f"#{k + 1}: sink already done ({c}), not killed")
            continue
        down, up = b.crash_restart()
        kills.append(f"#{k + 1} at {c}/{n_msgs} (back in {down + up:.1f}s)")
        h.say(f"  d: kill -9 {kills[-1]}")
    mid_run = sum(1 for x in kills if "not killed" not in x)

    def balances():
        rows = pg.rows(f"SELECT id, balance FROM {t_acct}", su=True)
        return {int(r[0]): Decimal(r[1]) for r in rows}

    def exact():
        if covered() < n_msgs:
            return False
        got = balances()
        return all(got.get(acct, Decimal(0)) == expected.get(acct, Decimal(0)) for acct in got)

    def bal_diag():
        got = balances()
        bad = [(a_, str(got.get(a_)), str(expected.get(a_, Decimal(0)))) for a_ in sorted(got) if got.get(a_) != expected.get(a_, Decimal(0))]
        bad_p = [x for x in bad if x[0] in precise]
        bad_n = [x for x in bad if x[0] not in precise]
        return (
            f"covered {covered()}/{n_msgs}; wrong balances (account, table, expected): {len(bad_n)} of the {n_acct} ordinary "
            f"accounts {bad_n[:6]}; {len(bad_p)} of the {len(precise)} accounts fed 30-digit amounts {bad_p[:3]}\n"
            + conn_diag([b], [name])
        )

    wait_for(exact, 240, "every balance to equal its sum", 0.5, diag=bal_diag)
    t_done = time.time() - t_start
    time.sleep(5 + 6)  # leaseSeconds + margin: a redelivered batch would land now
    if not exact():
        raise Failed("balances were exact, then moved: a redelivery was applied twice\n" + bal_diag())
    # Progress rows == the highest offset of every partition.
    parts = {p["name"]: p for p in b.partitions(q)}
    rows = pg.rows(f"SELECT partition_id, partition, last_offset FROM queen.sink_progress WHERE sink LIKE {lit(sink_like)}", su=True)
    by_name = defaultdict(list)
    for pid, pname, last in rows:
        by_name[pname].append((int(pid), int(last)))
    problems = []
    for pname, p in parts.items():
        got = by_name.get(pname)
        if not got:
            problems.append(f"{pname}: no progress row")
        elif max(x[1] for x in got) != int(p["lastOffset"]):
            problems.append(f"{pname}: progress {got}, partition lastOffset {p['lastOffset']}")
    stray = [x for x in by_name if x not in parts]
    if stray:
        problems.append(f"progress rows for partitions the queue does not have: {stray[:5]}")
    if len(rows) != len(parts):
        problems.append(f"{len(rows)} progress rows for {len(parts)} partitions")
    if problems:
        raise Failed("progress table != the queue's offsets:\n  " + "\n  ".join(problems[:15]))
    # The queue itself holds every push once.
    r = QueueReader(b, q, f"{h.rid}d")
    r.drain()
    txns = Counter(m.get("transactionId") for m in r.messages)
    dup = [t for t, c in txns.items() if c > 1]
    want = {it[2] for it in items}
    if dup or set(txns) != want:
        raise Failed(f"the queue does not hold each push once: {len(txns)} distinct of {len(want)}, duplicates {dup[:5]}")
    e = b.connector(name) or {}
    st = status_of(e)
    counters = {k: find_number(st, k) for k in ("applied", "skipped", "dlq", "batches")}
    if mid_run < 5:
        raise Failed(f"only {mid_run} of 5 kills landed mid-run ({kills}); balances were exact anyway")
    prec = {a_: str(expected[a_]) for a_ in precise[:2]}
    return (
        f"{n_msgs} msgs / {n_parts} partitions applied by sql mode (batch 10, 2 workers) under 5 x kill -9 ({'; '.join(kills)}); "
        f"all {len(expected)} touched balances exact ({len(precise)} accounts of 30-digit sums, e.g. {prec}) {t_done:.1f}s after the start, "
        f"still exact after leaseSeconds+6 s; {len(rows)} progress rows == every partition's last offset; status {counters}"
    )


# =============================================================================================
# (e) round trip PG A -> Queen -> PG B (another database) with concurrent writes
# =============================================================================================


E_COLS = "id bigint PRIMARY KEY, j jsonb, n numeric, big bigint, arr text[], toast text, val int, label text, at timestamptz"


def gen_e(t, n, rnd_text_len):
    st = {"next": 10_000_000}

    def big_text(rnd):
        return rand_text(rnd, rnd.randint(*rnd_text_len))

    def gen(rnd):
        stmts = []
        for _ in range(rnd.randint(1, 3)):
            r = rnd.random()
            ids = ", ".join(str(rnd.randint(1, n)) for _ in range(rnd.randint(5, 15)))
            if r < 0.55:
                # The bulk: updates that do NOT touch the TOASTed column.
                stmts.append(f"UPDATE {t} SET val = coalesce(val, 0) + 1, label = {lit(rnd_word(rnd))}, at = now() WHERE id IN ({ids})")
            elif r < 0.70:
                stmts.append(
                    f"UPDATE {t} SET j = jsonb_set(coalesce(j, '{{}}'::jsonb), '{{w}}', to_jsonb({rnd.randint(0, 10**6)})), "
                    f"n = n + 0.000000000000000000000001, big = big - 1, arr = array_append(arr, {lit(rnd_word(rnd))}) WHERE id IN ({ids})"
                )
            elif r < 0.82:
                st["next"] += 1
                i = st["next"]
                toast = lit(big_text(rnd)) if rnd.random() < 0.5 else "NULL"
                stmts.append(
                    f"INSERT INTO {t} VALUES ({i}, {lit(json_doc(rnd, i))}::jsonb, 123456789012345678901234.567891 + {i}, "
                    f"{9007199254740993 + i * 7}, {sql_tags(rnd)}, {toast}, {rnd.randint(0, 9)}, {lit(rnd_word(rnd))}, now())"
                )
            elif r < 0.92:
                stmts.append(f"DELETE FROM {t} WHERE id = {rnd.randint(1, n)}")
            elif r < 0.94:
                stmts.append(f"UPDATE {t} SET toast = {lit(big_text(rnd))} WHERE id = {rnd.randint(1, n)}")
            else:
                st["next"] += 1
                stmts.append(f"UPDATE {t} SET label = 'moved' WHERE id = {rnd.randint(1, n)}")
        return "BEGIN;\n" + ";\n".join(stmts) + ";\nCOMMIT;\n"

    return gen


def gen_hot(t, n):
    """Single-row updates of columns OTHER than the TOASTed one, on the rows
    that hold an out-of-line value (every third id): one row per transaction,
    so this writer never waits holding a lock (no deadlock with the other)."""

    def gen(rnd):
        i = 3 * rnd.randint(1, max(1, n // 3))
        return f"BEGIN;\nUPDATE {t} SET val = coalesce(val, 0) + 1, at = now() WHERE id = {i};\nCOMMIT;\n"

    return gen


def scenario_e(h, s):
    pg = h.pg
    rnd = h.rnd("e")
    n = h.scaled(3000)
    t_a = s.table("src")
    db_b = f"pgc_{h.rid}_b"
    t_b = "public.e_copy"
    s.create_table(t_a, E_COLS, extra=f"ALTER TABLE {ident(t_a)} ALTER COLUMN toast SET STORAGE EXTERNAL;")
    pg.create_database(db_b)
    s.dbs.append(db_b)
    pg.run(f"CREATE TABLE {t_b} ({E_COLS})", db=db_b)
    # Seed rows: one in three carries an incompressible 20-40 kB value, stored
    # out of line (STORAGE EXTERNAL), which pgoutput sends as 'u' (unchanged)
    # in every later update that does not touch it. The width makes each
    # 500-row chunk read take tens of milliseconds: the writers' updates land
    # inside the watermark windows (§4.5 step 4), which is the point.
    seed_rnd = random.Random(rnd.random())
    values = []
    for i in range(1, n + 1):
        toast = lit(rand_text(seed_rnd, seed_rnd.randint(20_000, 40_000))) if i % 3 == 0 else (lit(f"short {i}") if i % 3 == 1 else "NULL")
        values.append(
            f"({i}, {lit(json_doc(seed_rnd, i))}::jsonb, 123456789012345678901234.567891 + {i}, "
            f"{(9007199254740993 + i * 1000003) * (-1 if i % 10 == 0 else 1)}, {sql_tags(seed_rnd)}, {toast}, {i % 10}, {lit(rnd_word(seed_rnd))}, now())"
        )
    for i in range(0, len(values), 100):
        pg.run(f"INSERT INTO {t_a} VALUES " + ",\n".join(values[i : i + 100]))
    b = s.start(s.broker())
    q = "e-src"
    b.configure_queue(q)
    src = s.conn_name("src")
    snk = s.conn_name("sink")
    writer = s.writer(gen_e(t_a, n, (20_000, 40_000)), rnd, "e-writer")
    hot = s.writer(gen_hot(t_a, n), rnd, "e-hot", per_call=20)
    time.sleep(1.5)
    t0 = time.time()
    s.put_source(b, src, source_doc(h, [{"table": t_a, "queue": q}], snapshotChunkRows=500, maxBundleMessages=500, heartbeatSeconds=2))
    s.put_sink(b, snk, sink_doc(h, q, t_b, "cdc", db=db_b, batch=200, workers=2, leaseSeconds=10), "queen.sink_progress", db=db_b)
    wait_snapshot_done([b], src, 600, [src, snk])
    t_snap = time.time() - t0
    time.sleep(8.0)
    wst = writer.stop()
    hst = hot.stop()
    wst["txns_ok"] += hst["txns_ok"]
    h.say(f"  e: snapshot done in {t_snap:.1f}s; writers {wst} + hot {hst}")

    def equal():
        return pg.table_digest(t_a, ["id"]) == pg.table_digest(t_b, ["id"], db=db_b)

    try:
        wait_for(equal, 120, f"B ({db_b}.{t_b}) == A ({t_a})", 1.0)
    except Failed:
        raise Failed(e_diag(h, s, b, t_a, t_b, db_b, q, src, snk)) from None
    t_eq = time.time() - t0
    # The source's own record agrees (attributes a failure to a side).
    spec = TableSpec(t_a, q, ["id"], ts_cols=["at"])
    a, _ = converge_source(h, s, lambda: b, [spec], [src], timeout=60, tag="v")
    toasted = pg.value(f"SELECT count(*) FROM {t_a} WHERE length(toast) > 2000", su=True)
    u_unch = sum(1 for hist in a.history.values() for (_, _, _, op, unch) in hist if op == "u" and "toast" in unch)
    time.sleep(12)
    if not equal():
        raise Failed("B matched A, then diverged (a late redelivery?)\n" + e_diag(h, s, b, t_a, t_b, db_b, q, src, snk))
    return (
        f"A {pg.count(t_a)} rows ({toasted} with an out-of-line TOAST value, jsonb, 30-digit numeric, bigint>2^53, text[]) "
        f"== B in database {db_b}, all columns (md5 of every row), {t_eq:.1f}s after the source started; "
        f"snapshot {t_snap:.1f}s under {wst['txns_ok']} writer txns; {summarize(a)}; {u_unch} updates carried toast as unchanged"
    )


def e_diag(h, s, b, t_a, t_b, db_b, q, src, snk):
    pg = h.pg
    da, db_ = pg.table_digest(t_a, ["id"]), pg.table_digest(t_b, ["id"], db=db_b)
    miss = [k for k in da if k not in db_]
    extra = [k for k in db_ if k not in da]
    diff = [k for k in da if k in db_ and da[k] != db_[k]]
    lines = [f"B != A: A {len(da)} rows, B {len(db_)}; missing in B {len(miss)} {miss[:5]}, extra {len(extra)} {extra[:5]}, differing {len(diff)}"]
    if diff:
        ids = ", ".join(diff[:8])
        ra = {r["id"]: r for r in pg.json_rows(f"SELECT to_jsonb(t)::text FROM {t_a} t WHERE id IN ({ids})", su=True)}
        rb = {r["id"]: r for r in pg.json_rows(f"SELECT to_jsonb(t)::text FROM {t_b} t WHERE id IN ({ids})", db=db_b, su=True)}
        reader = QueueReader(b, q, f"{h.rid}ediag")
        try:
            reader.drain()
        except Exception:  # noqa: BLE001
            pass
        hist = defaultdict(list)
        for m in reader.messages:
            ev = m.get("data") or {}
            k = (ev.get("key") or {}).get("id")
            hist[k].append((m.get("offset"), ev.get("op"), tuple(ev.get("unchanged") or [])))
        for k in [int(x) for x in diff[:8]]:
            A, B = ra.get(k, {}), rb.get(k, {})
            cols = [c for c in A if A.get(c) != B.get(c)]
            desc = "; ".join(f"{c}: A {checks.clip(A.get(c))} B {checks.clip(B.get(c))}" for c in cols[:4])
            lines.append(f"  id {k}: {desc}; queue events (offset, op, unchanged): {sorted(hist.get(k, []))[-8:]}")
        seeded = int(h.scaled(3000))
        toast_lost = [
            int(k)
            for k in diff
            if int(k) <= seeded
            and "r" not in [op for _, op, _ in hist.get(int(k), [])]
            and any(op == "u" and "toast" in unch for _, op, unch in hist.get(int(k), []))
        ]
        if toast_lost:
            lines.append(
                f"  {len(toast_lost)} of the {len(diff)} differing rows existed before the snapshot, have NO snapshot ('r') "
                f"event, and their 'u' events left the TOAST column unchanged: e.g. ids {toast_lost[:6]}. Their chunk dropped "
                f"the key for a stream change between its watermarks (§4.5 step 4) and no event ever carried the value"
            )
    lines.append(conn_diag([b], [src, snk]))
    return "\n".join(lines)


# =============================================================================================
# (f) the slot dropped behind the source's back: slot_lost, then resync
# =============================================================================================


def scenario_f(h, s):
    pg = h.pg
    rnd = h.rnd("f")
    n = h.scaled(2000)
    t = s.table("src")
    q = "f-src"
    s.create_table(t, "id bigint PRIMARY KEY, val int, label text")
    pg.run(f"INSERT INTO {t} SELECT i, i % 10, 'l' || i FROM generate_series(1, {n}) i")
    spec = TableSpec(t, q, ["id"])
    b = s.start(s.broker())
    b.configure_queue(q)
    name = s.conn_name("src")
    slot = s.slot_of(name)
    s.put_source(b, name, source_doc(h, [{"table": t, "queue": q}], snapshotChunkRows=500, heartbeatSeconds=2))
    p1 = wait_snapshot_done([b], name, 300, [name])
    e1 = p1.get("epoch")
    w = s.writer(gen_simple(t, n), rnd, "f-writer")
    time.sleep(3)
    w.stop()
    # Drop the slot as superuser, terminating its walsender in the same breath
    # (the source reconnects at once).
    t_drop = time.time()
    if not pg.drop_slot(slot):
        raise Failed(f"could not drop slot {slot} (it kept being re-acquired)")
    h.say(f"  f: slot {slot} dropped behind the source's back")
    # Writes while there is no slot: they reach Queen only through the resync.
    pg.run(
        f"BEGIN; INSERT INTO {t} SELECT 20000000 + i, i, 'gap' FROM generate_series(1, 50) i; "
        f"UPDATE {t} SET val = val + 1000, label = 'gap-upd' WHERE id <= 50; DELETE FROM {t} WHERE id BETWEEN 51 AND 100; COMMIT;"
    )

    def lost():
        e = b.connector(name)
        codes = error_code_of(e)
        return ("slot_lost" in codes) and e

    e = wait_for(lost, 60, "the connector to report slot_lost", 0.3, diag=lambda: conn_diag([b], [name]))
    t_lost = time.time() - t_drop
    st = status_of(e)
    # Never recreated silently: the slot stays gone while the error stands.
    recreated = []
    for _ in range(20):
        if pg.slot(slot):
            recreated.append(time.strftime("%H:%M:%S"))
        time.sleep(0.25)
    if recreated:
        raise Failed(f"the slot came back without a resync (seen at {recreated[:3]}) while the status said slot_lost")
    h.say(f"  f: slot_lost reported {t_lost:.1f}s after the drop: phase {st.get('phase')!r} error {st.get('error')}")
    st_code, body, raw = b.resync(name)
    if st_code not in (200, 202, 204):
        raise Failed(f"POST /api/v1/connectors/{name}/resync -> {st_code}: {short(raw)}")
    t_rs = time.time()

    def new_epoch():
        p = pointer([b], name)
        return p if (p and p.get("epoch") and p.get("epoch") != e1 and p.get("snapshot") is None and p.get("lsn")) else None

    p2 = wait_for(new_epoch, 300, "a new epoch's snapshot to finish after the resync", 0.5, diag=lambda: conn_diag([b], [name]))
    e2 = p2["epoch"]
    t_resnap = time.time() - t_rs
    if not pg.slot(slot):
        raise Failed(f"after the resync the slot {slot} does not exist")
    # Writes after the resync stream as usual.
    w2 = s.writer(gen_simple(t, n), rnd, "f-writer2")
    time.sleep(3)
    wst = w2.stop()
    a, _ = converge_source(h, s, lambda: b, [spec], [name], timeout=180, only_epoch=e2)
    phase = phase_of(b.connector(name))
    old = a.epochs.get(e1, 0)
    new = a.epochs.get(e2, 0)
    return (
        f"slot dropped -> status error code slot_lost in {t_lost:.1f}s (phase {st.get('phase')!r}), slot not recreated while it stood; "
        f"resync -> epoch {e1} -> {e2}, snapshot again in {t_resnap:.1f}s, phase {phase!r}; replay of epoch {e2} == SELECT "
        f"({pg.count(t)} rows, gap inserts/updates/deletes included, {wst['txns_ok']} writer txns after; "
        f"{new} messages of the new epoch, {old} of the old, ids unique)"
    )


# =============================================================================================
# (g) one huge transaction, split, kill -9 in the middle
# =============================================================================================


def scenario_g(h, s):
    pg = h.pg
    rnd = h.rnd("g")
    n = h.scaled(200_000)
    need = n * 900 + 300 * 2**20
    free = shutil.disk_usage(h.work).free
    # Never take a shared laptop disk below ~6 GB.
    if free < need + 6 * 2**30:
        raise Skip(f"needs ~{need >> 20} MB and keeps 6 GB free on {h.work}: {free >> 20} MB free")
    t = s.table("big")
    q = "g-big"
    s.create_table(t, "id bigint PRIMARY KEY, v int")
    b = s.start(s.broker())
    b.configure_queue(q)
    name = s.conn_name("src")
    s.put_source(b, name, source_doc(h, [{"table": t, "queue": q, "partitionBy": "single"}], maxBundleMessages=500, heartbeatSeconds=2))
    wait_snapshot_done([b], name, 120, [name])
    t0 = time.time()
    # ONE statement in autocommit = one PG transaction; the LSN read right
    # after it is at or past its commit record.
    pg.run(f"INSERT INTO {t} SELECT i, i % 997 FROM generate_series(1, {n}) i", timeout=900)
    end_lsn = pg.current_lsn()
    t_ins = time.time() - t0
    h.say(f"  g: one transaction inserted {n} rows in {t_ins:.1f}s; WAL now {lsn_str(end_lsn)}")
    frac = rnd.uniform(0.3, 0.6)
    seen_done = set()
    killed = None
    deadline = time.time() + 900
    while True:
        p = pointer([b], name) or {}
        it = p.get("inTxn")
        if it:
            done = int(it.get("done") or 0)
            seen_done.add(done)
            if killed is None and done >= n * frac:
                down, up = b.crash_restart()
                killed = (done, down + up)
                h.say(f"  g: kill -9 with inTxn.done = {done} of {n}; back in {down + up:.1f}s")
        elif p.get("lsn") and (seen_done or killed or lsn_int(p["lsn"]) >= end_lsn):
            # inTxn cleared after it was seen set (or the pointer is past the
            # commit): the last chunk committed.
            break
        if time.time() > deadline:
            raise Failed(f"the split transaction did not finish in 900 s: pointer {p}\n" + conn_diag([b], [name]))
        time.sleep(0.05)
    t_ptr = time.time() - t0
    if killed is None:
        s.note(f"the transaction was pushed before inTxn.done reached {frac:.0%}: the kill did not land in the middle")
    # Read the one partition; check without keeping every message.
    st = {"msgs": 0, "dups": 0, "last_off": -1, "order_bad": 0}
    seqs = bytearray(n)
    ids = bytearray(n + 1)
    bad, lsns, ops, txids = [], set(), Counter(), set()

    def consume(m):
        st["msgs"] += 1
        ev = m.get("data") or {}
        ops[ev.get("op")] += 1
        lsns.add(ev.get("lsn"))
        i = (ev.get("key") or {}).get("id")
        sq = ev.get("seq")
        if not isinstance(i, int) or not 1 <= i <= n:
            if len(bad) < 5:
                bad.append(f"id {i!r} at offset {m.get('offset')}")
            return
        if ids[i]:
            st["dups"] += 1
        ids[i] = 1
        if isinstance(sq, int) and 0 <= sq < n:
            if seqs[sq]:
                st["dups"] += 1
            seqs[sq] = 1
        elif len(bad) < 5:
            bad.append(f"seq {sq!r} at offset {m.get('offset')}")
        if n <= 400_000:
            tx = m.get("transactionId")
            if tx in txids:
                st["dups"] += 1
            txids.add(tx)
        off = m.get("offset")
        if off is not None and off <= st["last_off"]:
            st["order_bad"] += 1
        if off is not None:
            st["last_off"] = off

    reader = QueueReader(b, q, f"{h.rid}g", consume=consume)
    rdeadline = time.time() + 900
    while st["msgs"] < n:
        reader.drain(max_seconds=900)
        if time.time() > rdeadline:
            break
        if st["msgs"] < n:
            time.sleep(1.0)
    t_all = time.time() - t0
    missing = n - sum(ids[1:])
    if st["dups"] or missing or bad or ops.get("c", 0) != n or len(lsns) != 1 or st["order_bad"] or reader.dup_deliveries:
        raise Failed(
            f"{st['msgs']} messages for {n} rows: {missing} ids missing, {st['dups']} duplicates, ops {dict(ops)}, "
            f"{len(lsns)} distinct lsn values {sorted(lsns)[:3]}, bad {bad}, offsets out of order {st['order_bad']}"
        )
    seq_missing = n - sum(seqs)
    if seq_missing:
        raise Failed(f"seq values 0..{n - 1}: {seq_missing} missing")
    time.sleep(3)
    reader.drain()
    if st["msgs"] != n:
        raise Failed(f"{st['msgs'] - n} more messages arrived after all {n} rows were in the queue")
    return (
        f"one PG transaction of {n} rows (inserted in {t_ins:.1f}s) pushed in pieces "
        f"(inTxn.done seen at {len(seen_done)} distinct values, maxBundleMessages 500), "
        + (f"kill -9 at done={killed[0]} (back in {killed[1]:.1f}s); " if killed else "no mid-transaction kill; ")
        + f"pointer past the commit {t_ptr:.1f}s after it, all read back by {t_all:.1f}s; {st['msgs']} messages, every id once, seq 0..{n - 1} once, "
        f"one commit lsn {next(iter(lsns))}, offsets in order"
    )


class Skip(Exception):
    pass


# =============================================================================================
# (h) a poison message goes to the DLQ, everything else is applied
# =============================================================================================


def scenario_h(h, s):
    pg = h.pg
    rnd = h.rnd("h")
    t = s.table("target")
    s.create_table(t, "id bigint PRIMARY KEY, qty int NOT NULL CONSTRAINT h_qty_nonneg CHECK (qty >= 0), label text")
    b = s.start(s.broker())
    q = "h-q"
    b.configure_queue(q)
    n_parts = 20
    msgs = []  # (partition, txn, payload dict, poison)
    for i in range(1, 801):
        msgs.append((f"p{i % n_parts}", f"h-{h.rid}-{i}", {"id": i, "qty": i % 50, "label": f"L{i}"}, False))
    for j, i in enumerate(rnd.sample(range(1, 801), 150)):
        msgs.append((f"p{i % n_parts}", f"h-{h.rid}-u{j}", {"id": i, "qty": 1000 + j, "label": f"U{j}"}, False))
    # Poison: a new id in the middle of p3, an update of an existing id as the
    # LAST message of p7, a new id as the first of p11 (pushed first below).
    poison = [
        ("p3", f"h-{h.rid}-poison-mid", {"id": 90003, "qty": -5, "label": "poison mid"}, True),
        ("p7", f"h-{h.rid}-poison-last", {"id": 7, "qty": -1, "label": "poison last"}, True),
        ("p11", f"h-{h.rid}-poison-first", {"id": 90011, "qty": -2, "label": "poison first"}, True),
    ]
    ordered = [poison[2]] + msgs[:400] + [poison[0]] + msgs[400:] + [poison[1]]
    items = [(q, p, txn, json.dumps(pl)) for p, txn, pl, _ in ordered]
    for i in range(0, len(items), 200):
        push([b.url], items[i : i + 200])
    expected = {}
    for p, txn, pl, bad in ordered:
        if not bad:
            expected[pl["id"]] = (pl["qty"], pl["label"])
    name = s.conn_name("sink")
    prog = s.table("progress")
    s.tables.append((prog, None))
    s.put_sink(b, name, sink_doc(h, q, t, "upsert", key=["id"], batch=50, maxAttempts=2, leaseSeconds=10, progressTable=prog), prog)
    group = f"pg-{name}"

    def table_now():
        return {int(r[0]): (int(r[1]), r[2]) for r in pg.rows(f"SELECT id, qty, label FROM {t}", su=True)}

    def done():
        if table_now() != expected:
            return False
        d = b.dlq(q, group)
        return d if len(d.get("messages") or []) >= 3 else False

    def diag():
        got = table_now()
        miss = [k for k in expected if k not in got]
        wrong = [(k, got[k], expected[k]) for k in expected if k in got and got[k] != expected[k]]
        extra = [k for k in got if k not in expected]
        d = b.dlq(q, group)
        return (f"table: {len(got)} rows, expected {len(expected)}; missing {miss[:5]}, wrong {wrong[:5]}, extra {extra[:5]}; "
                f"DLQ {[(m.get('transactionId'), m.get('errorMessage')) for m in d.get('messages') or []][:5]}\n" + conn_diag([b], [name]))

    t0 = time.time()
    d = wait_for(done, 180, "every good message applied and the 3 poison messages in the DLQ", 0.5, diag=diag)
    t_done = time.time() - t0
    time.sleep(12)
    d = b.dlq(q, group)
    dl = d.get("messages") or []
    got_txn = Counter(m.get("transactionId") for m in dl)
    want_txn = {p[1] for p in poison}
    if set(got_txn) != want_txn or any(c != 1 for c in got_txn.values()):
        raise Failed(f"the DLQ holds {dict(got_txn)}, expected each of {sorted(want_txn)} once")
    if table_now() != expected:
        raise Failed("the table changed after it matched\n" + diag())
    errs = [str(m.get("errorMessage") or "") for m in dl]
    if not all(("h_qty_nonneg" in e or "check" in e.lower() or "23514" in e) for e in errs):
        s.note(f"DLQ errorMessage does not name the CHECK violation: {errs}")
    parts = {p["name"]: p for p in b.partitions(q)}
    prog_rows = {r[1]: int(r[2]) for r in pg.rows(f"SELECT partition_id, partition, last_offset FROM {prog} WHERE sink LIKE {lit('%/' + name)}", su=True)}
    last_p7 = int(parts["p7"]["lastOffset"])
    if prog_rows.get("p7") == last_p7:
        s.note(f"p7's last message is the poison (offset {last_p7}) and its offset IS in the progress table (§5.4 says it is not written)")
    # Every partition's progress is at its last offset; p7's last message is
    # the poison, so p7 may stop one before it (§5.4: not written).
    lag = {
        k: (prog_rows.get(k), int(v["lastOffset"]))
        for k, v in parts.items()
        if prog_rows.get(k) not in ((int(v["lastOffset"]), int(v["lastOffset"]) - 1) if k == "p7" else (int(v["lastOffset"]),))
    }
    if lag:
        raise Failed(f"progress rows do not reach the partitions' ends: {lag}")
    st = status_of(b.connector(name) or {})
    return (
        f"{len(ordered)} msgs over {n_parts} partitions, upsert into a table with CHECK (qty >= 0): the 3 poison messages "
        f"(first/middle/last of their partitions) are in the DLQ once each ({[e[:60] for e in errs]}), all {len(expected)} "
        f"other rows exact in {t_done:.1f}s; p7 progress {prog_rows.get('p7')} (poison at {last_p7}); status dlq={find_number(st, 'dlq')}"
    )


# =============================================================================================
# (i) TOAST: an unchanged out-of-line value survives cdc apply
# =============================================================================================


def scenario_i(h, s):
    pg = h.pg
    rnd = h.rnd("i")
    cols = "id bigint PRIMARY KEY, big text, counter int NOT NULL DEFAULT 0, note text"
    t_src, t_copy = s.table("src"), s.table("copy")
    s.create_table(t_src, cols, extra=f"ALTER TABLE {ident(t_src)} ALTER COLUMN big SET STORAGE EXTERNAL;")
    s.create_table(t_copy, cols)
    big1, big2 = rand_text(rnd, 100_000), rand_text(rnd, 100_000)
    pg.run(f"INSERT INTO {t_src} (id, big, note) VALUES (1, {lit(big1)}, 'snapshot row')")
    b = s.start(s.broker())
    q = "i-src"
    b.configure_queue(q)
    src, snk = s.conn_name("src"), s.conn_name("sink")
    prog = s.table("progress")
    s.tables.append((prog, None))
    s.put_source(b, src, source_doc(h, [{"table": t_src, "queue": q}], heartbeatSeconds=2))
    s.put_sink(b, snk, sink_doc(h, q, t_copy, "cdc", batch=20, leaseSeconds=10, progressTable=prog), prog)
    wait_snapshot_done([b], src, 120, [src, snk])
    pg.run(f"INSERT INTO {t_src} (id, big, note) VALUES (2, {lit(big2)}, 'stream row')")
    pg.run("".join(f"UPDATE {t_src} SET counter = counter + 1, note = 'upd {k}' WHERE id IN (1, 2);\n" for k in range(100)))

    def state(table):
        # NULL big -> length -1 / md5 'NULL' (a lost value must compare unequal, not crash).
        return {
            int(r[0]): (int(r[1]), r[2], int(r[3]), r[4])
            for r in pg.rows(f"SELECT id, coalesce(length(big), -1), coalesce(md5(big), 'NULL'), counter, coalesce(note, 'NULL') FROM {table}", su=True)
        }

    want = state(t_src)
    try:
        wait_for(lambda: state(t_copy) == want, 120, "the copy to match (big value, counter = 100)", 0.5)
    except Failed:
        raise Failed(f"source {want}\n copy {state(t_copy)}\n" + conn_diag([b], [src, snk])) from None
    reader = QueueReader(b, q, f"{h.rid}i")
    reader.drain()
    u_unch = sum(1 for m in reader.messages if (m.get("data") or {}).get("op") == "u" and "big" in ((m.get("data") or {}).get("unchanged") or []))
    u_all = sum(1 for m in reader.messages if (m.get("data") or {}).get("op") == "u")
    line = (
        f"rows 1 (snapshot) and 2 (stream) with 100 kB out-of-line values, 100 updates each of another column: copy == source "
        f"(length {want[1][0]}/{want[2][0]}, md5 equal, counter 100); {u_unch} of {u_all} update events carried big as unchanged"
    )
    if u_unch == 0:
        s.note("no update event marked big as unchanged: the TOAST path was not exercised (REPLICA IDENTITY FULL? the value resent?)")
    # Beyond §7: the key of a TOASTed row changes (key partitioning sends d to
    # the old key's partition and c to the new one; the new tuple carries the
    # big value as 'unchanged').
    pg.run(f"UPDATE {t_src} SET id = 3 WHERE id = 2")
    want2 = state(t_src)
    try:
        wait_for(lambda: state(t_copy) == want2, 30, "the copy to follow a key change of a TOASTed row", 0.5)
        line += "; probe: key change 2->3 of a TOASTed row also kept the value"
    except Failed:
        got = state(t_copy)
        ev = [((m.get("data") or {}).get("op"), (m.get("data") or {}).get("key"), (m.get("data") or {}).get("unchanged")) for m in _tail_events(b, q, h)]
        s.finding(
            f"probe beyond §7: UPDATE ... SET id = 3 WHERE id = 2 on a row whose 100 kB value is out of line: source row 3 = "
            f"{want2.get(3)}, copy row 3 = {got.get(3)} (row 2 in copy: {got.get(2)}); last events {ev[-3:]}. With key "
            f"partitioning the 'c' to the new key's partition has the value as unchanged and no earlier value in that partition."
        )
        line += "; probe (key change of a TOASTed row) LOST the value: see findings"
    return line


def _tail_events(b, q, h):
    r = QueueReader(b, q, f"{h.rid}it")
    try:
        r.drain()
    except Exception:  # noqa: BLE001
        return []
    return sorted(r.messages, key=lambda m: (m.get("createdAt") or "", m.get("offset") or 0))


def scenario_self(h, s):
    fails = checks.selftest()
    if fails:
        raise Failed("the queue checker is vacuous:\n  " + "\n  ".join(fails))
    return (
        "the checker accepts a correct queue and refuses each damage for its reason: duplicate transactionId, lsn going "
        "back, a lost insert, a lost delete, a wrong value, a key in two partitions, unchanged TOAST without an earlier "
        "value, a delete ordered before its snapshot row, an offset delivered twice; a fill repairs a dropped chunk row "
        "(and its absence is caught); §4.6 partition rendering"
    )


SCENARIOS = [
    ("self", scenario_self, "the queue checker refuses every kind of damage it exists to catch (no broker)"),
    ("a", scenario_a, "source: snapshot of 20k rows (composite + single PK) + stream, concurrent multi-statement writes, exactly once per key"),
    ("b", scenario_b, "as (a) with kill -9 of the broker at 5 random moments"),
    ("c", scenario_c, "3 nodes: kill -9 the lease owner (takeover), SIGTERM the new one (hand-over), then a cdc sink on all 3"),
    ("d", scenario_d, "sink sql mode exactly once: 20k msgs / 200 partitions, kill -9 x5 mid-run, balances and progress exact"),
    ("e", scenario_e, "round trip A -> Queen -> B (another database) under concurrent writes: B == A, every column"),
    ("f", scenario_f, "slot dropped behind the source's back: slot_lost, resync, new epoch correct"),
    ("g", scenario_g, "one 200k-row PG transaction (2M with --scale 10) split into many Queen transactions, kill -9 mid-way"),
    ("h", scenario_h, "poison message: CHECK violation -> DLQ, everything else applied"),
    ("i", scenario_i, "TOAST: a 100 kB unchanged value survives 100 updates through the cdc sink"),
]
