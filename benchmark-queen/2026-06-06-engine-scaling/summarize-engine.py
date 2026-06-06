#!/usr/bin/env python3
"""Summarize the engine-scaling sweep into a decision table + verdict.

For each regime it prints, per (NUM_WORKERS, SIDE) cell:
  push/s (PG n_tup_ins, ground truth), del/s, commits/s, broker vCPU, PG vCPU,
  PG active backends, WAL MB/s, and event-loop lag (evl max / p95 ms).

Then a verdict per regime:
  pg-bound     -> push/s ~flat as engine count falls AND evl stays low
                  => one push/ack engine is enough; scale slots, not engines.
  engine-bound -> push/s drops at low engine count AND evl climbs while PG has
                  headroom => multiple (then partition-sharded) push engines help.

Usage: python3 summarize-engine.py [out_dir]   (default ./out)
"""
import os
import re
import sys
import glob

WARMUP_ROWS = 2          # drop initial samples (pool warm-up, first batches)
EVL_FLAT_MS = 5.0        # evl below this is considered "loop not behind"
FLAT_FRAC = 0.90         # push at min engines >= FLAT_FRAC * push at max engines => flat


def pct(vals, p):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def fnum(s):
    try:
        if s is None or s == "" or s == "NA":
            return None
        return float(s)
    except ValueError:
        return None


def parse_goload(path):
    """Return (pushed, popped, push_err, pop_err) from the [final] line, if any."""
    if not os.path.exists(path):
        return None
    txt = open(path, encoding="utf-8", errors="replace").read()
    m = re.search(r"\[final\]\s+pushed=(\d+)\s+popped=(\d+)\s+pushErr=(\d+)\s+popErr=(\d+)", txt)
    if not m:
        return None
    return tuple(int(x) for x in m.groups())


def load_cell(tsv):
    rows = []
    with open(tsv, encoding="utf-8", errors="replace") as f:
        header = f.readline()  # noqa: F841
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 10:
                continue
            rows.append(parts)
    if len(rows) <= WARMUP_ROWS + 1:
        return None
    body = rows[WARMUP_ROWS:]

    def col(i):
        return [fnum(r[i]) for r in body]

    ts = col(0)
    elapsed = (ts[-1] - ts[0]) if (ts[0] and ts[-1]) else None
    if not elapsed or elapsed <= 0:
        return None

    def rate(i):
        a, b = fnum(body[0][i]), fnum(body[-1][i])
        if a is None or b is None:
            return None
        return (b - a) / elapsed

    broker = [v for v in col(1) if v is not None]
    pgcpu = [v for v in col(3) if v is not None]
    active = [v for v in col(4) if v is not None]
    evl = [v for v in col(9) if v is not None]

    return {
        "elapsed": elapsed,
        "push_s": rate(6),                  # n_tup_ins delta / s
        "del_s": rate(7),                   # n_tup_del delta / s
        "commit_s": rate(5),                # xact_commit delta / s
        "wal_mb_s": (rate(8) / 1e6) if rate(8) is not None else None,
        "broker_vcpu": (sum(broker) / len(broker)) if broker else None,
        "pg_vcpu": (sum(pgcpu) / len(pgcpu)) if pgcpu else None,
        "pg_active": (sum(active) / len(active)) if active else None,
        "evl_max": max(evl) if evl else None,
        "evl_p95": pct(evl, 95) if evl else None,
    }


def f(v, nd=1):
    return "n/a" if v is None else f"{v:,.{nd}f}"


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "out"
    if not os.path.isdir(out_dir):
        print(f"no such dir: {out_dir}")
        sys.exit(1)

    regimes = sorted(d for d in os.listdir(out_dir) if os.path.isdir(os.path.join(out_dir, d)))
    if not regimes:
        print(f"no regime subdirs under {out_dir}")
        sys.exit(1)

    for regime in regimes:
        cells = []
        for tsv in sorted(glob.glob(os.path.join(out_dir, regime, "*.tsv"))):
            base = os.path.basename(tsv)
            m = re.match(r"w(\d+)_s(\d+)\.tsv$", base)
            if not m:
                continue
            w, side = int(m.group(1)), int(m.group(2))
            data = load_cell(tsv)
            if not data:
                continue
            gl = parse_goload(tsv[:-4] + ".goload.log")
            data["w"], data["side"] = w, side
            data["pop_s"] = (gl[1] / data["elapsed"]) if gl else None
            data["push_err"] = gl[2] if gl else None
            data["pop_err"] = gl[3] if gl else None
            cells.append(data)

        if not cells:
            continue
        cells.sort(key=lambda c: (c["side"], c["w"]))

        print(f"\n=== regime: {regime} ===")
        hdr = ("W", "SIDE", "push/s", "pop/s", "del/s", "commit/s",
               "brkCPU", "pgCPU", "pgActive", "WAL MB/s", "evlMax", "evlP95", "errs")
        print("  " + "".join(s.rjust(11) for s in hdr))
        for c in cells:
            errs = "-" if c["push_err"] is None else f'{c["push_err"]}/{c["pop_err"]}'
            line = (
                str(c["w"]), str(c["side"]), f(c["push_s"], 0), f(c["pop_s"], 0),
                f(c["del_s"], 0), f(c["commit_s"], 0), f(c["broker_vcpu"], 2),
                f(c["pg_vcpu"], 2), f(c["pg_active"], 1), f(c["wal_mb_s"], 1),
                f(c["evl_max"], 0), f(c["evl_p95"], 0), errs,
            )
            print("  " + "".join(s.rjust(11) for s in line))

        # Verdict over the engine-count (W) sweep at the fixed/most-common SIDE.
        from collections import Counter
        common_side = Counter(c["side"] for c in cells).most_common(1)[0][0]
        wsweep = sorted((c for c in cells if c["side"] == common_side), key=lambda c: c["w"])
        if len(wsweep) >= 2:
            lo, hi = wsweep[0], wsweep[-1]  # fewest vs most engines
            verdict, why = "inconclusive", []
            if lo["push_s"] and hi["push_s"]:
                flat = lo["push_s"] >= FLAT_FRAC * hi["push_s"]
                evl_low = (lo["evl_max"] is None) or (lo["evl_max"] < EVL_FLAT_MS)
                if flat and evl_low:
                    verdict = "PG-BOUND  -> one push/ack engine is enough (scale slots, not engines)"
                elif not flat and (lo["evl_max"] is not None and (hi["evl_max"] is None or lo["evl_max"] > max(EVL_FLAT_MS, (hi["evl_max"] or 0)))):
                    verdict = "ENGINE-BOUND -> multiple (then partition-sharded) push engines help"
                why.append(f"push@W={lo['w']}={f(lo['push_s'],0)}/s vs push@W={hi['w']}={f(hi['push_s'],0)}/s "
                           f"({(lo['push_s']/hi['push_s']*100 if hi['push_s'] else 0):.0f}% of max)")
                why.append(f"evl_max@W={lo['w']}={f(lo['evl_max'],0)}ms vs @W={hi['w']}={f(hi['evl_max'],0)}ms")
                why.append(f"pgCPU@W={lo['w']}={f(lo['pg_vcpu'],1)} pgActive={f(lo['pg_active'],1)} (SIDE={common_side})")
            print(f"  VERDICT [{regime} @ SIDE={common_side}]: {verdict}")
            for w in why:
                print(f"           - {w}")


if __name__ == "__main__":
    main()
