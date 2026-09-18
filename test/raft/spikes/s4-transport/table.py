#!/usr/bin/env python3
"""Render results.jsonl as the markdown tables used in RESULTS-*.md.

    python3 table.py results/laptop/results.jsonl            # one row per run
    python3 table.py results/laptop-interleaved/results.jsonl --agg
                                                             # one row per config,
                                                             # median over rounds
"""
import json
import statistics
import sys

ROWS = [
    ("offered msg/s", lambda d: f'{d["rate_msgs"]:,}'),
    ("achieved msg/s", lambda d: f'{d["achieved_msgs_s"]:,.0f}'),
    ("p50 us", lambda d: d["rt_send_p50_us"]),
    ("p99 us", lambda d: d["rt_send_p99_us"]),
    ("p99.9 us", lambda d: d["rt_send_p999_us"]),
    ("max us", lambda d: d["rt_send_max_us"]),
    ("leader cores", lambda d: f'{d["leader_cpu_cores"]:.3f}'),
    ("recv cores", lambda d: f'{d["recv_cpu_cores"]:.3f}'),
    ("leader us/msg", lambda d: f'{d["leader_cpu_us_per_msg"]:.2f}'),
    ("recv us/msg", lambda d: f'{d["recv_cpu_us_per_msg"]:.2f}'),
    ("B/msg wire", lambda d: f'{d["wire_bytes_per_msg"]:.1f}'),
    ("conns open", lambda d: d.get("leader_conns_open", d.get("leader_conns"))),
    ("conns opened", lambda d: d.get("leader_conns_opened", d.get("leader_conns"))),
    ("errors", lambda d: d["cmds_bad"]),
]


def label(d):
    return "{}{}{} @{}k/{}conn".format(
        d["transport"],
        "+tls" if d["tls"] else "",
        "+mac" if d["mac"] else "",
        d["rate_msgs"] // 1000,
        d["conns_cfg"],
    )


def emit(rows, extra=()):
    hdr = ["config"] + [n for n, _ in ROWS] + list(extra)
    print("| " + " | ".join(hdr) + " |")
    print("|" + "---|" * len(hdr))
    for d, tail in rows:
        print("| " + " | ".join([label(d)] + [str(f(d)) for _, f in ROWS] + list(tail)) + " |")


def aggregate(rows):
    groups = {}
    for d in rows:
        groups.setdefault(label(d), []).append(d)
    out = []
    for lab, g in groups.items():
        med = lambda k: statistics.median(x[k] for x in g)
        mean = lambda k: statistics.fmean(x[k] for x in g)
        a = dict(g[0])
        a["achieved_msgs_s"] = mean("achieved_msgs_s")
        a["rt_send_p50_us"] = round(med("rt_send_p50_us"))
        a["rt_send_p99_us"] = round(med("rt_send_p99_us"))
        a["rt_send_p999_us"] = round(med("rt_send_p999_us"))
        a["rt_send_max_us"] = max(x["rt_send_max_us"] for x in g)
        a["leader_cpu_cores"] = mean("leader_cpu_cores")
        a["recv_cpu_cores"] = mean("recv_cpu_cores")
        a["leader_cpu_us_per_msg"] = mean("leader_cpu_us_per_msg")
        a["recv_cpu_us_per_msg"] = mean("recv_cpu_us_per_msg")
        a["wire_bytes_per_msg"] = mean("wire_bytes_per_msg")
        a["leader_conns_open"] = round(med("leader_conns_open"))
        a["leader_conns_opened"] = round(med("leader_conns_opened"))
        a["cmds_bad"] = sum(x["cmds_bad"] for x in g)
        out.append((a, (str(len(g)),)))
    return out


def main(argv):
    path = next((a for a in argv if not a.startswith("-")), "results/laptop/results.jsonl")
    rows = [json.loads(l) for l in open(path) if l.strip()]
    if "--agg" in argv:
        emit(aggregate(rows), extra=("rounds",))
    else:
        emit([(d, ()) for d in rows])


if __name__ == "__main__":
    main(sys.argv[1:])
