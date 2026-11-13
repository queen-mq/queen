#!/usr/bin/env python3
"""stacks2.py <script.txt.gz> [comm] [focus...] : read `perf script` samples (frame-pointer stacks; `comm` in the
header or not) and print, for the samples of threads named `comm` (all when omitted): the share each function is on
the stack (inclusive) and at the top (self), and for each focus substring, the callers above the samples whose leaf
matches it (who frees, who copies)."""
import gzip, re, sys
from collections import Counter

FRAME = re.compile(r"^([0-9a-f]+)\s+(.*?)\s+\((.*)\)$")


def short(sym):
    sym = re.sub(r"<queen::rsm::store::heed_store::Heed(Read|Store)>", "", sym)
    sym = re.sub(r"<queen::rsm::replicator::node::NodeReplicator<[^>]*>>", "", sym)
    out, depth = [], 0
    for ch in sym:  # drop generic arguments
        if ch == "<":
            depth += 1
            if depth == 1 and out and out[-1] == ":":
                out.pop(); out.pop()
            if depth >= 1:
                continue
        if ch == ">":
            depth -= 1
            continue
        if depth == 0:
            out.append(ch)
    s = "".join(out)
    return s[:120] if s else sym[:120]


def samples(path, comm):
    cur = []
    with gzip.open(path, "rt", errors="replace") as f:
        for l in f:
            if l.strip() == "":
                if cur:
                    yield cur
                cur = []
            else:
                cur.append(l.rstrip("\n"))
    if cur:
        yield cur


def main():
    path = sys.argv[1]
    comm = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "-" else None
    focus = sys.argv[3:]
    n = 0
    incl, selfc = Counter(), Counter()
    who = {f: Counter() for f in focus}
    for b in samples(path, comm):
        head = b[0].strip()
        if comm and not head.startswith(comm):
            continue
        fr = []
        for l in b[1:]:
            m = FRAME.match(l.strip())
            if m:
                fr.append((short(m.group(2)), m.group(3)))
        if not fr:
            continue
        n += 1
        selfc[fr[0][0]] += 1
        for s in set(s for s, _ in fr):
            incl[s] += 1
        for f in focus:
            if f in fr[0][0] or (f == "libc" and "libc" in fr[0][1]):
                callers = [s for s, d in fr[1:] if "queen" in d and not s.startswith(("mi_", "_mi_"))][:4]
                who[f][" <- ".join(callers) if callers else "(none)"] += 1
    print(f"{n} samples{' of ' + comm if comm else ''}")
    print("\n== inclusive (on the stack), top 45")
    for k, v in incl.most_common(45):
        print(f"  {100 * v / n:5.1f}%  {k}")
    print("\n== self (at the top), top 25")
    for k, v in selfc.most_common(25):
        print(f"  {100 * v / n:5.1f}%  {k}")
    for f in focus:
        tot = sum(who[f].values())
        print(f"\n== {f}: {100 * tot / max(n, 1):.1f}% of samples, by callers (top 12)")
        for k, v in who[f].most_common(12):
            print(f"  {100 * v / n:5.1f}%  {k}")


main()
