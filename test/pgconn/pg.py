"""PostgreSQL side of the pgconn e2e suite: psql subprocesses only (stdlib).

Every statement goes through `psql -X -q -A -t -v ON_ERROR_STOP=1`, as the
superuser or as the connector role, with the session in UTC/ISO so the text
output of every type is the one the connectors read. Rows come back separated
by \\x1e and fields by \\x1f, so a newline inside a value cannot split a row.

Connection (CI points it at any PostgreSQL 17+ with wal_level=logical):

  QUEEN_PGCONN_E2E_HOST       127.0.0.1
  QUEEN_PGCONN_E2E_PORT       55432
  QUEEN_PGCONN_E2E_SUPERUSER  postgres        (QUEEN_PGCONN_E2E_SUPERPASSWORD if it needs one)
  QUEEN_PGCONN_E2E_USER       queen_test      (LOGIN REPLICATION, owner of the database)
  QUEEN_PGCONN_E2E_PASSWORD   queen_test_pw
  QUEEN_PGCONN_E2E_DB         queen_test
  PSQL                        psql on PATH, else /opt/homebrew/opt/postgresql@18/bin/psql
"""

import json
import os
import random
import shutil
import subprocess
import threading
import time
from decimal import Decimal

FIELD = "\x1f"
RECORD = "\x1e"
PSQL_FALLBACK = "/opt/homebrew/opt/postgresql@18/bin/psql"


class PgError(Exception):
    pass


def lsn_int(s):
    """`X/Y` (hex halves) -> int. None stays None."""
    if s is None:
        return None
    hi, lo = str(s).split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def lsn_str(n):
    return "%X/%X" % (n >> 32, n & 0xFFFFFFFF)


def lit(s):
    """A SQL string literal."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def ident(s):
    """A SQL identifier (each dotted part quoted)."""
    return ".".join('"' + p.replace('"', '""') + '"' for p in str(s).split("."))


def find_psql():
    env = os.environ.get("PSQL")
    if env:
        return env
    on_path = shutil.which("psql")
    if on_path:
        return on_path
    if os.access(PSQL_FALLBACK, os.X_OK):
        return PSQL_FALLBACK
    return None


def loads(text):
    """JSON with exact numbers: decimals as Decimal, integers as int."""
    return json.loads(text, parse_float=Decimal)


class PG:
    def __init__(self):
        e = os.environ.get
        self.host = e("QUEEN_PGCONN_E2E_HOST", "127.0.0.1")
        self.port = int(e("QUEEN_PGCONN_E2E_PORT", "55432"))
        self.superuser = e("QUEEN_PGCONN_E2E_SUPERUSER", "postgres")
        self.superpassword = e("QUEEN_PGCONN_E2E_SUPERPASSWORD", "")
        self.user = e("QUEEN_PGCONN_E2E_USER", "queen_test")
        self.password = e("QUEEN_PGCONN_E2E_PASSWORD", "queen_test_pw")
        self.db = e("QUEEN_PGCONN_E2E_DB", "queen_test")
        self.psql = find_psql()
        self.calls = 0

    def describe(self):
        return f"{self.user}@{self.host}:{self.port}/{self.db} (superuser {self.superuser}, psql {self.psql})"

    # --- raw psql --------------------------------------------------------------------

    def _cmd(self, db, su):
        return [
            self.psql,
            "-h", self.host,
            "-p", str(self.port),
            "-U", self.superuser if su else self.user,
            "-d", db or self.db,
            "-X", "-q", "-A", "-t",
            "-v", "ON_ERROR_STOP=1",
            "-F", FIELD,
            "-R", RECORD,
            "-f", "-",
        ]

    def _env(self, su):
        env = dict(os.environ)
        for k in list(env):
            if k.startswith("PG") and k not in ("PGTZ",):
                del env[k]
        env.update(
            {
                "PGPASSWORD": (self.superpassword if su else self.password) or "",
                "PGCONNECT_TIMEOUT": "10",
                "PGTZ": "UTC",
                "PGDATESTYLE": "ISO",
                "PGAPPNAME": "pgconn-e2e",
                "PGSSLMODE": "disable",
                "PGCLIENTENCODING": "UTF8",
            }
        )
        if not env["PGPASSWORD"]:
            del env["PGPASSWORD"]
        return env

    def run(self, sql, db=None, su=False, timeout=300, check=True):
        """Run `sql` (one or more statements); returns (rc, stdout, stderr)."""
        self.calls += 1
        try:
            p = subprocess.run(
                self._cmd(db, su),
                input=sql.encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._env(su),
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            if check:
                raise PgError(f"psql timed out after {timeout}s: {sql[:200]}") from None
            return 124, "", "timeout"
        out = p.stdout.decode("utf-8", "replace")
        err = p.stderr.decode("utf-8", "replace")
        if check and p.returncode != 0:
            raise PgError(f"psql rc={p.returncode}: {err.strip()[:800]}\n  sql: {sql.strip()[:400]}")
        return p.returncode, out, err

    def rows(self, sql, db=None, su=False, timeout=300):
        _, out, _ = self.run(sql, db=db, su=su, timeout=timeout)
        recs = [r for r in out.split(RECORD)]
        # psql ends the output with a newline after the last record.
        recs = [r[1:] if r.startswith("\n") else r for r in recs]
        recs = [r for r in recs if r not in ("", "\n")]
        return [r.rstrip("\n").split(FIELD) for r in recs]

    def value(self, sql, db=None, su=False):
        rs = self.rows(sql, db=db, su=su)
        if not rs:
            return None
        return rs[0][0]

    def json_rows(self, sql, db=None, su=False, timeout=300):
        """`sql` must return ONE column holding JSON text per row."""
        return [loads(r[0]) for r in self.rows(sql, db=db, su=su, timeout=timeout) if r and r[0] != ""]

    # --- server facts --------------------------------------------------------------------

    def preflight(self):
        """Refuse to run against a server the connectors cannot use. Returns a
        dict of facts for the transcript."""
        if not self.psql:
            raise PgError("no psql found (set PSQL, or put psql on PATH)")
        facts = {}
        rs = self.rows(
            "SELECT current_setting('server_version_num'), current_setting('wal_level'), "
            "current_setting('max_replication_slots'), current_setting('max_wal_senders'), "
            "current_setting('max_slot_wal_keep_size'), version()",
            su=True,
        )
        num, wal, slots, senders, keep, ver = rs[0]
        facts.update(version=ver.split(" on ")[0], server_version_num=int(num), wal_level=wal,
                     max_replication_slots=int(slots), max_wal_senders=int(senders),
                     max_slot_wal_keep_size=keep)
        if int(num) < 170000:
            raise PgError(f"PostgreSQL {num} < 17: the connectors need 17+")
        if wal != "logical":
            raise PgError(f"wal_level={wal}: start the server with wal_level=logical")
        # The connector role and its database (created when missing: a fresh CI
        # container has only the superuser).
        have_role = self.value(f"SELECT rolreplication FROM pg_roles WHERE rolname = {lit(self.user)}", su=True)
        if have_role is None:
            self.run(f"CREATE ROLE {ident(self.user)} LOGIN REPLICATION PASSWORD {lit(self.password)}", db="postgres", su=True)
            facts["created_role"] = self.user
        elif have_role != "t":
            raise PgError(f"role {self.user} lacks REPLICATION")
        have_db = self.value(f"SELECT 1 FROM pg_database WHERE datname = {lit(self.db)}", db="postgres", su=True)
        if have_db is None:
            self.run(f"CREATE DATABASE {ident(self.db)} OWNER {ident(self.user)}", db="postgres", su=True)
            facts["created_db"] = self.db
        who = self.value("SELECT current_user")
        if who != self.user:
            raise PgError(f"logged in as {who}, expected {self.user}")
        return facts

    def current_lsn(self):
        return lsn_int(self.value("SELECT pg_current_wal_lsn()", su=True))

    # --- replication slots, publications ------------------------------------------------

    def slot(self, name):
        """The slot row, or None."""
        rs = self.rows(
            "SELECT slot_name, plugin, active, coalesce(active_pid::text, ''), coalesce(confirmed_flush_lsn::text, ''), "
            "coalesce(wal_status, ''), coalesce(invalidation_reason, ''), failover "
            f"FROM pg_replication_slots WHERE slot_name = {lit(name)}",
            su=True,
        )
        if not rs:
            return None
        r = rs[0]
        return {
            "slot_name": r[0],
            "plugin": r[1],
            "active": r[2] == "t",
            "active_pid": int(r[3]) if r[3] else None,
            "confirmed_flush_lsn": lsn_int(r[4]) if r[4] else None,
            "wal_status": r[5],
            "invalidation_reason": r[6] or None,
            "failover": r[7] == "t",
        }

    def slots_like(self, pattern):
        return [r[0] for r in self.rows(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE {lit(pattern)}", su=True)]

    def kill_walsender(self, slot):
        """Terminate the walsender that holds `slot` (if any). Returns the pid."""
        pid = self.value(
            f"SELECT active_pid FROM pg_replication_slots WHERE slot_name = {lit(slot)} AND active_pid IS NOT NULL", su=True
        )
        if pid:
            self.run(f"SELECT pg_terminate_backend({int(pid)})", su=True, check=False)
            return int(pid)
        return None

    def drop_slot(self, slot, attempts=40):
        """Drop `slot` even while a connector keeps reconnecting to it: terminate
        its walsender and drop in ONE round trip, repeatedly. Returns True when
        the slot is gone."""
        sql = (
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
            f"WHERE slot_name = {lit(slot)} AND active_pid IS NOT NULL;\n"
            "SELECT pg_sleep(0.05);\n"
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = {lit(slot)} AND NOT active;\n"
        )
        for _ in range(attempts):
            if self.slot(slot) is None:
                return True
            self.run(sql, su=True, check=False)
            if self.slot(slot) is None:
                return True
            time.sleep(0.1)
        return self.slot(slot) is None

    def publications_like(self, pattern, db=None):
        return [r[0] for r in self.rows(f"SELECT pubname FROM pg_publication WHERE pubname LIKE {lit(pattern)}", db=db, su=True)]

    def drop_publication(self, name, db=None):
        self.run(f"DROP PUBLICATION IF EXISTS {ident(name)}", db=db, su=True, check=False)

    # --- schemas, tables, databases ---------------------------------------------------------

    def create_schema(self, schema):
        self.run(f"CREATE SCHEMA IF NOT EXISTS {ident(schema)} AUTHORIZATION {ident(self.user)}", su=True)

    def drop_schema(self, schema, db=None):
        self.run(f"DROP SCHEMA IF EXISTS {ident(schema)} CASCADE", db=db, su=True, check=False)

    def schemas_like(self, pattern, db=None):
        return [r[0] for r in self.rows(f"SELECT nspname FROM pg_namespace WHERE nspname LIKE {lit(pattern)}", db=db, su=True)]

    def create_database(self, name):
        self.run(f"DROP DATABASE IF EXISTS {ident(name)} WITH (FORCE)", db="postgres", su=True, check=False)
        self.run(f"CREATE DATABASE {ident(name)} OWNER {ident(self.user)}", db="postgres", su=True)

    def drop_database(self, name):
        self.run(f"DROP DATABASE IF EXISTS {ident(name)} WITH (FORCE)", db="postgres", su=True, check=False)

    def databases_like(self, pattern):
        return [r[0] for r in self.rows(f"SELECT datname FROM pg_database WHERE datname LIKE {lit(pattern)}", db="postgres", su=True)]

    def table_exists(self, table, db=None):
        return self.value(f"SELECT to_regclass({lit(table)}) IS NOT NULL", db=db, su=True) == "t"

    def count(self, table, db=None):
        return int(self.value(f"SELECT count(*) FROM {ident(table)}", db=db, su=True))

    def table_json(self, table, key_cols, db=None):
        """{key tuple: row as parsed JSON} — the ground truth a replay is
        compared with. Session UTC, so timestamptz renders as +00:00."""
        keys = ", ".join(ident(k) for k in key_cols)
        out = {}
        for row in self.json_rows(f"SELECT to_jsonb(t)::text FROM {ident(table)} t ORDER BY {keys}", db=db, su=True):
            out[tuple(row[k] for k in key_cols)] = row
        return out

    def table_digest(self, table, key_cols, db=None):
        """{key text: md5 of the row's text output}: two tables of the same
        shape are equal iff their digests are."""
        keys = ", ".join(ident(k) for k in key_cols)
        ktext = " || '|' || ".join(f"coalesce({ident(k)}::text, '\\N')" for k in key_cols)
        rs = self.rows(f"SELECT {ktext}, md5(t::text) FROM {ident(table)} t ORDER BY {keys}", db=db, su=True)
        return {r[0]: r[1] for r in rs}


# --- the concurrent writer --------------------------------------------------------------------


class Writer:
    """Runs transactions from `gen(rnd)` (each a `BEGIN; …; COMMIT;` script)
    in a background thread, `per_call` of them per psql process, until
    stopped. What committed is whatever the table holds at the end: that is the
    ground truth, so a failed call (a constraint, a lost connection) is only
    counted."""

    def __init__(self, pg, gen, rnd, per_call=20, pause=0.0, name="writer", db=None):
        self.pg = pg
        self.gen = gen
        self.rnd = rnd
        self.per_call = per_call
        self.pause = pause
        self.name = name
        self.db = db
        self.stop_evt = threading.Event()
        self.calls = 0
        self.txns = 0
        self.failures = 0
        self.last_error = ""
        self.fatal = None
        self.thread = threading.Thread(target=self._run, daemon=True, name=name)

    def start(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            while not self.stop_evt.is_set():
                script = "".join(self.gen(self.rnd) for _ in range(self.per_call))
                rc, _, err = self.pg.run(script, db=self.db, check=False, timeout=120)
                self.calls += 1
                if rc == 0:
                    self.txns += self.per_call
                else:
                    self.failures += 1
                    self.last_error = err.strip()[:300]
                if self.pause:
                    self.stop_evt.wait(self.pause)
        except Exception as e:  # noqa: BLE001 — reported by stop()
            self.fatal = e

    def stop(self):
        self.stop_evt.set()
        self.thread.join(180)
        if self.fatal:
            raise PgError(f"{self.name} failed: {self.fatal}")
        return {"calls": self.calls, "txns_ok": self.txns, "failed_calls": self.failures, "last_error": self.last_error}


def rand_text(rnd, n):
    """`n` characters that do not compress (so a long one is stored out of
    line, TOASTed, not inline-compressed)."""
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(rnd.choice(alphabet) for _ in range(n))


def rnd_word(rnd):
    return rnd.choice(["alpha", "beta", "gamma", "delta", "x'y", "quote\"d", "uni é 日本", "back\\slash", "pipe|bar", ""])


__all__ = ["PG", "PgError", "Writer", "lsn_int", "lsn_str", "lit", "ident", "loads", "rand_text", "rnd_word", "random"]
