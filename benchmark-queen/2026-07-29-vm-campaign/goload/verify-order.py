#!/usr/bin/env python3
"""Offline correctness verifier for goload -mode app -verify-log.

Reads the per-consumer-group TSV logs (cg*.tsv) and the producer manifest
(manifest.tsv) written by goload's app mode and checks, per
(consumer-group, partition):

  1. ORDER     first-delivery order == push order (seq 0,1,2,..). Catches the
               cursor-skip / reorder class (the 0.16 fix). A "seq > expected"
               event is a violation (skip/reorder/loss).
  2. ZERO-LOSS every pushed seq (0..maxSeq from the manifest) was delivered to
               each consumer group at least once.
  3. COMPLETE  (--full) every non-poison seq reached a terminal "ok" (completed)
               status; poison seqs failed every delivery (-> DLQ).

Delivery is at-least-once: redeliveries (seq < expected) are expected and OK,
as long as they stay ordered. Poison seqs are still *delivered* in order (then
DLQ'd after retryLimit), so they are NOT delivery gaps.

Usage:
  python3 verify-order.py <verify-log-dir> [--full]

Exit code 0 = PASS, 1 = FAIL (order violation or loss detected).

NOTE: to verify zero-loss you must let the consumers fully drain (run long
enough / until the queue is empty). Partitions that are merely "short" at stop
are reported separately from genuine order violations.
"""

import sys
import os
import glob


def read_manifest(path):
    """partition -> dict(count, maxSeq, poison). Partitions are disjoint per
    producer, so each appears once."""
    man = {}
    if not os.path.exists(path):
        return man
    with open(path) as f:
        header = f.readline()
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            p, count, mx, poison = parts[0], int(parts[1]), int(parts[2]), int(parts[3])
            man[p] = {"count": count, "maxSeq": mx, "poison": poison}
    return man


def verify_cg(path, full):
    """Stream one cg*.tsv. Returns per-partition stats keyed by partition."""
    # streaming order model: expected next first-delivery seq per partition
    expected = {}      # part -> next expected seq
    dups = {}          # part -> redelivery count (seq < expected)
    viols = {}         # part -> violation count (seq > expected)
    sample = []        # up to 20 violation samples
    # --full only:
    delivered = {}     # part -> set of seqs ever delivered
    completed = {}     # part -> set of seqs with an "ok" status
    poisoned = {}      # part -> set of seqs seen "poison"
    cg_name = None

    with open(path) as f:
        f.readline()  # header
        for line in f:
            row = line.rstrip("\n").split("\t")
            if len(row) < 6:
                continue
            cg_name = row[1]
            part = row[2]
            try:
                seq = int(row[3])
            except ValueError:
                continue
            status = row[5]
            if seq < 0:
                continue  # stage-2 / unsequenced

            e = expected.get(part, 0)
            if seq == e:
                expected[part] = e + 1
            elif seq < e:
                dups[part] = dups.get(part, 0) + 1
            else:  # seq > e: skip / reorder / loss
                viols[part] = viols.get(part, 0) + 1
                if len(sample) < 20:
                    sample.append(f"{cg_name} part={part} expected={e} got={seq} gap={seq - e}")
                expected[part] = seq + 1

            if full:
                delivered.setdefault(part, set()).add(seq)
                if status == "ok":
                    completed.setdefault(part, set()).add(seq)
                elif status == "poison":
                    poisoned.setdefault(part, set()).add(seq)

    return {
        "cg": cg_name or os.path.basename(path),
        "expected": expected,
        "dups": dups,
        "viols": viols,
        "sample": sample,
        "delivered": delivered,
        "completed": completed,
        "poisoned": poisoned,
    }


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    full = "--full" in sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(2)
    d = args[0]

    man = read_manifest(os.path.join(d, "manifest.tsv"))
    total_pushed = sum(v["count"] for v in man.values())
    print(f"manifest: {len(man)} partitions, {total_pushed} messages pushed"
          + (f", {sum(v['poison'] for v in man.values())} poison" if man else ""))

    cg_files = sorted(glob.glob(os.path.join(d, "cg*.tsv")))
    if not cg_files:
        print(f"no cg*.tsv files in {d}")
        sys.exit(2)

    overall_fail = False
    for path in cg_files:
        r = verify_cg(path, full)
        cg = r["cg"]
        total_viol = sum(r["viols"].values())
        total_dup = sum(r["dups"].values())
        parts_seen = set(r["expected"].keys())

        # loss / short: per manifest partition, expected_final should reach maxSeq+1
        missing_parts = []   # never delivered to this CG at all
        short_parts = []     # delivered but did not reach maxSeq (pending-at-stop OR loss)
        for p, info in man.items():
            need = info["maxSeq"] + 1
            got = r["expected"].get(p, 0)
            if p not in parts_seen:
                missing_parts.append(p)
            elif got < need:
                short_parts.append((p, got, need))

        status = "PASS"
        if total_viol > 0 or missing_parts:
            status = "FAIL"
            overall_fail = True

        print(f"\n=== {cg} ===  [{status}]")
        print(f"  partitions delivered : {len(parts_seen)} / {len(man)} (manifest)")
        print(f"  ORDER violations     : {total_viol}  (seq>expected: skip/reorder/loss)")
        print(f"  redeliveries (dups)  : {total_dup}  (seq<expected: at-least-once, OK)")
        print(f"  partitions missing   : {len(missing_parts)}  (never delivered to this CG)")
        print(f"  partitions short     : {len(short_parts)}  (not drained to maxSeq -> pending-at-stop or loss)")
        for s in r["sample"]:
            print(f"    ! {s}")
        if missing_parts[:5]:
            print(f"    missing e.g.: {missing_parts[:5]}")
        if short_parts[:5]:
            print(f"    short e.g.:   {[f'{p}:{g}/{n}' for p, g, n in short_parts[:5]]}")

        if full:
            # completion accounting: non-poison seqs should be completed
            comp_ok = 0
            comp_missing = 0
            poison_total = 0
            for p, info in man.items():
                mx = info["maxSeq"]
                comp = r["completed"].get(p, set())
                pois = r["poisoned"].get(p, set())
                poison_total += len(pois)
                for s in range(mx + 1):
                    if s in pois and s not in comp:
                        continue  # poison -> DLQ, expected not-completed
                    if s in comp:
                        comp_ok += 1
                    else:
                        comp_missing += 1
            print(f"  [full] completed     : {comp_ok}  not-completed(non-poison): {comp_missing}  poison-deliveries: {poison_total}")
            if comp_missing > 0:
                print("         (non-poison messages without a terminal 'ok' -> likely pending at stop; drain longer)")

    print("\n" + ("RESULT: FAIL (order violation or partition loss)" if overall_fail
                  else "RESULT: PASS (no order violations, every manifest partition delivered)"))
    sys.exit(1 if overall_fail else 0)


if __name__ == "__main__":
    main()
