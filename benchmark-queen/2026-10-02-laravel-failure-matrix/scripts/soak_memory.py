#!/usr/bin/env python3
"""The soak lanes' memory samples and events as CSV, for the docs figure.

Usage:
  soak_memory.py [RAW_DIR]   (default: ../raw next to this script)

Reads `RAW_DIR/soak/soak/<engine>.json` and writes `RAW_DIR/soak-memory.csv`
(one row per sample: the master's resident memory and the median worker's)
and `RAW_DIR/soak-events.csv` (worker kills and the deploy). Both use seconds
since the dispatch started.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ENGINES = ("horizon", "queen-php", "queen-rust")
DISPATCH = re.compile(r"^dispatching ")


def main() -> int:
    raw = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "raw"
    samples, events = [], []
    for engine in ENGINES:
        path = raw / "soak" / "soak" / f"{engine}.json"
        if not path.exists():
            continue
        lane = json.loads(path.read_text())
        timeline = lane.get("timeline") or []
        start = next((t for t, note in timeline if DISPATCH.match(note)), 0.0)
        for sample in (lane.get("extra") or {}).get("memory") or []:
            samples.append([engine, sample["t"], sample.get("master_rss_mib"), sample.get("worker_rss_median_mib"),
                            sample.get("workers")])
        for t, note in timeline:
            if note.startswith("SIGKILL worker"):
                events.append([engine, round(t - start), "worker killed"])
            elif note.startswith("rolling restart: stopped"):
                events.append([engine, round(t - start), "deploy"])

    def write(name: str, header: list[str], rows: list[list]) -> None:
        with (raw / name).open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            for row in rows:
                writer.writerow(["" if v is None else (round(v, 1) if isinstance(v, float) else v) for v in row])

    write("soak-memory.csv", ["engine", "t_s", "master_rss_mib", "worker_rss_median_mib", "workers"], samples)
    write("soak-events.csv", ["engine", "t_s", "event"], events)
    print(f"{len(samples)} samples, {len(events)} events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
