#!/usr/bin/env python3
"""End-to-end suite of the PostgreSQL source and sink running IN-PROCESS in
the broker (PLAN_PG_CONNECTORS.md §7): real `queen` processes (one node or a
three-node raft cluster), a real PostgreSQL 17+, real kill -9 and SIGTERM.

  python3 test/pgconn/run.py                  # every scenario (a..i)
  python3 test/pgconn/run.py a d h            # some of them (also "a,d,h")
  python3 test/pgconn/run.py --seed 42        # replay a run's randomness
  python3 test/pgconn/run.py --scale 10 g     # 2M-row transaction
  python3 test/pgconn/run.py --keep           # keep data dirs and tables

Exit code: 0 every scenario passed (or was skipped), 1 one failed, 2 the suite
could not run (no binary, no PostgreSQL, a binary without the connectors).

Standard library only. See README.md for what each scenario proves.
"""

import argparse
import os
import random
import secrets
import shutil
import sys
import tempfile
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.dont_write_bytecode = True  # no __pycache__ beside the suite

from pg import PG, PgError  # noqa: E402
from queen import Failed, Unsupported  # noqa: E402
from scenarios import SCENARIOS, Scn, Skip  # noqa: E402


class Harness:
    def __init__(self, args):
        self.args = args
        self.bin = args.bin
        self.work = args.work
        self.rust_log = args.rust_log
        self.seed = args.seed
        self.live = set()
        self.pg = PG()
        self.rid = secrets.token_hex(3)
        self.schema = f"pgc_{self.rid}"
        self.encryption_key = secrets.token_hex(32)
        self.lease_ttl_ms = 3000
        os.makedirs(os.path.join(self.work, "logs"), exist_ok=True)
        os.makedirs(os.path.join(self.work, "data"), exist_ok=True)
        self.transcript = open(os.path.join(self.work, "harness.log"), "a", buffering=1)

    def say(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)
        self.transcript.write(line + "\n")

    def rnd(self, name):
        return random.Random(f"{self.seed}:{name}")

    def scaled(self, n):
        return max(1, int(round(n * self.args.scale)))

    def kill_all(self):
        for b in list(self.live):
            if b.alive():
                b.kill9()
                try:
                    b.wait_exit(10, expect=None)
                except Failed:
                    pass
        self.live.clear()


def default_bin():
    env = os.environ.get("QUEEN_BIN")
    if env:
        return env
    for p in (os.path.join(REPO, "server", "target", "debug", "queen"), os.path.join(REPO, "target", "debug", "queen")):
        if os.access(p, os.X_OK):
            return p
    return os.path.join(REPO, "server", "target", "debug", "queen")


def leftovers(h):
    """Objects of EARLIER runs of this suite (prefix pgc / queen_pgc) that a
    crash left behind: inactive slots, their publications, schemas, the
    second databases, progress rows. Never anything of another name."""
    pg = h.pg
    dropped = []
    for s in pg.slots_like("queen\\_pgc\\_%"):
        sl = pg.slot(s)
        if sl and not sl["active"]:
            if pg.drop_slot(s, attempts=3):
                dropped.append(f"slot {s}")
    for p in pg.publications_like("queen\\_pgc\\_%"):
        pg.drop_publication(p)
        dropped.append(f"publication {p}")
    for sch in pg.schemas_like("pgc\\_%"):
        if sch != h.schema:
            pg.drop_schema(sch)
            dropped.append(f"schema {sch}")
    for db in pg.databases_like("pgc\\_%\\_b"):
        pg.drop_database(db)
        dropped.append(f"database {db}")
    if pg.table_exists("queen.sink_progress"):
        pg.run("DELETE FROM queen.sink_progress WHERE sink LIKE '%/pgc-%'", su=True, check=False)
    return dropped


def main():
    ap = argparse.ArgumentParser(description="End-to-end suite of the in-process PostgreSQL source and sink.")
    ap.add_argument("scenarios", nargs="*", help="scenario letters (default: all of " + ",".join(n for n, _, _ in SCENARIOS) + ")")
    ap.add_argument("--keep", action="store_true", help="keep data dirs, logs and the scenarios' tables (slots are always dropped)")
    ap.add_argument("--seed", type=int, default=None, help="seed of every random choice (default: a new one, printed)")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply row/message counts (g: 200k rows at 1, 2M at 10)")
    ap.add_argument("--bin", default=default_bin(), help="the broker binary (default $QUEEN_BIN or server/target/debug/queen)")
    ap.add_argument("--work", default=None, help="work directory (default: a new temp dir)")
    ap.add_argument("--rust-log", default="warn,queen-pg=info,queen_pg=info,boot=info,shutdown=info")
    args = ap.parse_args()
    known = {n: (fn, d) for n, fn, d in SCENARIOS}
    wanted = []
    for x in args.scenarios or [n for n, _, _ in SCENARIOS]:
        for y in x.split(","):
            y = y.strip()
            if not y:
                continue
            if y not in known:
                ap.error(f"unknown scenario {y!r}; known: {', '.join(known)}")
            wanted.append(y)
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(1, 10**9)
    if not os.access(args.bin, os.X_OK):
        print(f"no broker binary at {args.bin}: build it with (cd server && cargo build --bin queen)", file=sys.stderr)
        return 2
    made_work = args.work is None
    args.work = os.path.abspath(args.work or tempfile.mkdtemp(prefix="queen-pgconn-"))
    os.makedirs(args.work, exist_ok=True)
    h = Harness(args)
    h.say(f"pgconn e2e: seed {args.seed} (rerun: --seed {args.seed}), scale {args.scale}, run id {h.rid}")
    h.say(f"  binary {args.bin} (built {time.strftime('%Y-%m-%d %H:%M', time.localtime(os.path.getmtime(args.bin)))}); work {args.work}")
    try:
        facts = h.pg.preflight()
    except PgError as e:
        h.say(f"PostgreSQL not usable: {e}")
        return 2
    h.say(f"  postgres {h.pg.describe()}: {facts}")
    gone = leftovers(h)
    if gone:
        h.say(f"  dropped leftovers of earlier runs: {gone}")
    h.pg.create_schema(h.schema)
    free = shutil.disk_usage(args.work).free
    h.say(f"  schema {h.schema}; {free >> 20} MB free on the work disk")
    if free < 1 << 30:
        h.say("  WARNING: under 1 GB free; the brokers refuse writes when the disk fills")
    results = []
    started = time.time()
    aborted = None
    try:
        for name in wanted:
            fn, desc = known[name]
            h.say(f"--- scenario {name}: {desc}")
            s = Scn(h, name)
            t0 = time.time()
            status, line = "FAIL", ""
            try:
                line = fn(h, s)
                status = "PASS"
            except Skip as e:
                status, line = "SKIP", str(e)
            except Unsupported as e:
                status, line = "ABORT", str(e)
                aborted = str(e)
            except Failed as e:
                line = str(e)
            except PgError as e:
                line = f"postgres: {e}"
            except Exception as e:  # noqa: BLE001 — a harness bug is reported, not hidden
                line = f"harness error {type(e).__name__}: {e}\n{traceback.format_exc()}"
            finally:
                try:
                    s.cleanup()
                except Exception as e:  # noqa: BLE001
                    s.cleanup_log.append(f"cleanup failed: {e}")
            dur = time.time() - t0
            results.append((name, status, dur, line, s))
            h.say(f"{status} {name} ({dur:.0f}s): {line}")
            if s.cleanup_log:
                h.say(f"  cleanup: {s.cleanup_log}")
            if aborted:
                break
    finally:
        h.kill_all()
        if not args.keep:
            h.pg.drop_schema(h.schema)
        leaked = h.pg.slots_like(f"queen\\_pgc\\_{h.rid}\\_%")
        if leaked:
            h.say(f"WARNING: slots left on the server: {leaked}")
    h.say("")
    h.say("=" * 30 + " pgconn e2e summary " + "=" * 30)
    h.say(f"{'scn':<4} {'result':<6} {'time':>6}  evidence")
    for name, status, dur, line, _ in results:
        first = line.splitlines()[0] if line else ""
        h.say(f"{name:<4} {status:<6} {dur:>5.0f}s  {first[:400]}")
    notes = [(n, x) for n, _, _, _, s in results for x in s.notes]
    findings = [(n, x) for n, _, _, _, s in results for x in s.findings]
    if notes:
        h.say("notes (spec deviations that cost no data):")
        for n, x in notes:
            h.say(f"  ({n}) {x}")
    if findings:
        h.say("findings beyond PLAN §7:")
        for n, x in findings:
            h.say(f"  ({n}) {x}")
    failed = [r for r in results if r[1] == "FAIL"]
    h.say(
        f"{sum(1 for r in results if r[1] == 'PASS')} passed, {len(failed)} failed, "
        f"{sum(1 for r in results if r[1] == 'SKIP')} skipped in {time.time() - started:.0f}s; seed {args.seed}; "
        f"logs in {os.path.join(args.work, 'logs')}"
    )
    if aborted:
        h.say(f"ABORTED: {aborted}")
        return 2
    if not args.keep and not failed:
        if made_work:
            shutil.rmtree(args.work, ignore_errors=True)
        else:
            shutil.rmtree(os.path.join(args.work, "data"), ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
