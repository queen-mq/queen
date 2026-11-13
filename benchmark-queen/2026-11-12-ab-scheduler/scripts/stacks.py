#!/usr/bin/env python3
"""stacks.py <planner-script.txt.gz> : read `perf script -F tid,period,ip,sym,dso` samples of one thread and print
(1) the share of samples under each step of the planner cycle, (2) self time by function, (3) for the allocator and
memcpy samples, the first queen frames above them (who frees, who copies)."""
import gzip, re, sys
from collections import Counter

ALLOC = re.compile(r"^(mi_|_mi_|__memmove|__memcpy|memcpy|memmove|__memset|bytes::bytes::shared_drop)")
STEPS = [  # (label, substring of a frame's symbol), the first that matches from the root down names the sample
    ("landing (land_async / join / unfold)", ("land_async", "PendingLanding", "unfold_listed", "unfold_parts_here")),
    ("advance (catalog unfold, entries)", ("KeptOverlay>::advance", "unfold_catalog")),
    ("push_entry", ("KeptOverlay>::push_entry",)),
    ("footprints", ("Footprints<",)),
    ("groups on this thread", ("scheduler::plan_group", "scheduler::work")),
    ("merge", ("scheduler::merge", "absorb_catalog", "absorb_shard", "file_spill")),
    ("assemble / numbering", ("scheduler::assemble", "Numbering")),
    ("plan_parallel (other)", ("scheduler::plan_parallel",)),
    ("plan_commands (other)", ("scheduler::plan_commands",)),
    ("plan_cycle_blocking (other)", ("plan_cycle_blocking",)),
]


def frames_of(block):
    out = []
    for l in block[1:]:
        l = l.strip()
        m = re.match(r"^([0-9a-f]+)\s+(.*?)\s+\((.*)\)$", l)
        if m:
            out.append((m.group(2), m.group(3)))
    return out


def short(sym):
    sym = re.sub(r"<queen::rsm::store::heed_store::Heed(Read|Store)>", "", sym)
    sym = re.sub(r"::<[^<>]*(<[^<>]*(<[^<>]*>[^<>]*)*>[^<>]*)*>$", "", sym)
    return sym[:110]


def main(path):
    blocks, cur = [], []
    with gzip.open(path, "rt", errors="replace") as f:
        for l in f:
            if l.strip() == "":
                if cur:
                    blocks.append(cur)
                cur = []
            else:
                cur.append(l.rstrip("\n"))
    if cur:
        blocks.append(cur)
    n = 0
    steps, selfs, who_alloc, who_copy = Counter(), Counter(), Counter(), Counter()
    for b in blocks:
        fr = frames_of(b)
        if not fr:
            continue
        n += 1
        leaf = fr[0][0]
        selfs[short(leaf)] += 1
        label = "unattributed (stack cut)"
        for lab, keys in STEPS:
            if any(any(k in s for k in keys) for s, _ in fr):
                label = lab
                break
        if not any("plan_cycle_blocking" in s for s, _ in fr) and label == "unattributed (stack cut)":
            label = "outside plan_cycle_blocking (thread loop, waits)"
        steps[label] += 1
        if ALLOC.match(leaf) or leaf.startswith("[unknown]") or re.match(r"^0x", leaf) or "libc" in fr[0][1]:
            callers = [short(s) for s, d in fr[1:] if "queen" in d and not ALLOC.match(s)][:3]
            key = " <- ".join(callers) if callers else "(no queen frame)"
            if "memmove" in leaf or "memcpy" in leaf or "libc" in fr[0][1]:
                who_copy[key] += 1
            else:
                who_alloc[key] += 1
    print(f"{n} samples")
    print("\n== where the planner thread is, by step (share of samples)")
    for k, v in steps.most_common():
        print(f"  {100 * v / n:5.1f}%  {k}")
    print("\n== self time by function (top 25)")
    for k, v in selfs.most_common(25):
        print(f"  {100 * v / n:5.1f}%  {k}")
    print("\n== allocator samples (free/malloc), by the first queen frames above them (top 15)")
    for k, v in who_alloc.most_common(15):
        print(f"  {100 * v / n:5.1f}%  {k}")
    print("\n== libc samples (memcpy and friends), by the first queen frames above them (top 10)")
    for k, v in who_copy.most_common(10):
        print(f"  {100 * v / n:5.1f}%  {k}")


main(sys.argv[1])
