#!/usr/bin/env python3
"""make_data.py: the dashboard's data files, from the runs' own summary lines.

Reads ../runs/<tag>.out (the last line, as matrix.sh writes it), the backlog runs' benchmark.log and
the facade failover runs, and writes omb_points.csv, omb_backlog.csv and facade_failover.csv here.
Nothing is typed by hand except what each tag is (the TAGS table below).
"""
import csv
import json
import os
import re

H = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(H, "..", "runs")

# tag -> (family, workload, system, config, build, partitions, topics, bytes, producers, target msg/s)
# system: queen = native driver, queen_kafka_api = Queen's Kafka facade with OMB's driver-kafka, kafka = Kafka 4.3.1
F1 = "Fixed rate, 1 topic, 1 KB"
F3 = "Max rate"
F4 = "1M msg/s on 10 topics, 100 B"
F2 = "Many topics, 500k msg/s, 1 KB"
TAGS = {
    # family 1
    "native-nr2-p100-200k": (F1, "100 partitions", "queen", "4 producers", "nr2", 100, 1, 1024, 4, 200_000),
    "native-nr0-p100-500k": (F1, "100 partitions", "queen", "4 producers", "nr0", 100, 1, 1024, 4, 500_000),
    "native-nr0-p100-1000k": (F1, "100 partitions", "queen", "4 producers", "nr0", 100, 1, 1024, 4, 1_000_000),
    "native-nr2-p100-8p8c-1000k": (F1, "100 partitions", "queen", "8 producers", "nr2", 100, 1, 1024, 8, 1_000_000),
    "native-nr2-p100-2000k": (F1, "100 partitions", "queen", "4 producers", "nr2", 100, 1, 1024, 4, 2_000_000),
    "native-nr2-p10000-200k": (F1, "10,000 partitions", "queen", "4 producers", "nr2", 10_000, 1, 1024, 4, 200_000),
    "native-nr2-p10000-500k": (F1, "10,000 partitions", "queen", "4 producers", "nr2", 10_000, 1, 1024, 4, 500_000),
    "native-nr2-p10000-1000k": (F1, "10,000 partitions", "queen", "4 producers", "nr2", 10_000, 1, 1024, 4, 1_000_000),
    "native-nr2-p10000-2000k": (F1, "10,000 partitions", "queen", "4 producers", "nr2", 10_000, 1, 1024, 4, 2_000_000),
    "facade-nr6-p100-500k": (F1, "100 partitions", "queen_kafka_api", "4 producers", "nr6", 100, 1, 1024, 4, 500_000),
    "facade-nr6-p100-1000k": (F1, "100 partitions", "queen_kafka_api", "4 producers", "nr6", 100, 1, 1024, 4, 1_000_000),
    "facade-nr0-p100-500k": (F1, "100 partitions", "queen_kafka_api", "before the fixes", "nr0", 100, 1, 1024, 4, 500_000),
    "facade-nr4-p100-1000k": (F1, "100 partitions", "queen_kafka_api", "before the fixes", "nr4", 100, 1, 1024, 4, 1_000_000),
    "kafka-p100-200k": (F1, "100 partitions", "kafka", "4 producers", "kafka", 100, 1, 1024, 4, 200_000),
    "kafka-p100-500k": (F1, "100 partitions", "kafka", "4 producers", "kafka", 100, 1, 1024, 4, 500_000),
    "kafka-p100-1000k": (F1, "100 partitions", "kafka", "4 producers", "kafka", 100, 1, 1024, 4, 1_000_000),
    "kafka-p100-8p8c-1000k": (F1, "100 partitions", "kafka", "8 producers", "kafka", 100, 1, 1024, 8, 1_000_000),
    "kafka-p100-16p16c-1000k": (F1, "100 partitions", "kafka", "16 producers", "kafka", 100, 1, 1024, 16, 1_000_000),
    "kafka-p100-1000k-b128k": (F1, "100 partitions", "kafka", "4 producers, 128 KB batches", "kafka", 100, 1, 1024, 4, 1_000_000),
    "kafka-p100-2000k": (F1, "100 partitions", "kafka", "4 producers", "kafka", 100, 1, 1024, 4, 2_000_000),
    "kafka-p10000-200k": (F1, "10,000 partitions", "kafka", "4 producers", "kafka", 10_000, 1, 1024, 4, 200_000),
    "kafka-p10000-500k": (F1, "10,000 partitions", "kafka", "4 producers", "kafka", 10_000, 1, 1024, 4, 500_000),
    # family 3
    "native-nr2-max-1t1p-1p1c-100b": (F3, "1 partition, 100 B", "queen", "pop autopilot", "nr2", 1, 1, 100, 1, 0),
    "native-nr2-max-1t1p-1p1c-100b-b1000": (F3, "1 partition, 100 B", "queen", "popBatch 1,000", "nr2", 1, 1, 100, 1, 0),
    "native-nr2-max-1t1p-1p1c-100b-b10000": (F3, "1 partition, 100 B", "queen", "popBatch 10,000", "nr2", 1, 1, 100, 1, 0),
    "kafka-max-1t1p-1p1c-100b": (F3, "1 partition, 100 B", "kafka", "stock", "kafka", 1, 1, 100, 1, 0),
    "native-nr2-max-1t1p-1p1c-1kb": (F3, "1 partition, 1 KB", "queen", "pop autopilot", "nr2", 1, 1, 1024, 1, 0),
    "native-nr2-max-1t1p-1p1c-1kb-b1000": (F3, "1 partition, 1 KB", "queen", "popBatch 1,000", "nr2", 1, 1, 1024, 1, 0),
    "native-nr2-max-1t1p-1p1c-1kb-b10000": (F3, "1 partition, 1 KB", "queen", "popBatch 10,000", "nr2", 1, 1, 1024, 1, 0),
    "kafka-max-1t1p-1p1c-1kb": (F3, "1 partition, 1 KB", "kafka", "stock", "kafka", 1, 1, 1024, 1, 0),
    "native-nr2-max-1t1p-1p1c-64kb": (F3, "1 partition, 64 KB", "queen", "pop autopilot", "nr2", 1, 1, 65536, 1, 0),
    "native-nr2-max-1t1p-1p1c-64kb-b1000": (F3, "1 partition, 64 KB", "queen", "popBatch 1,000", "nr2", 1, 1, 65536, 1, 0),
    "native-nr2-max-1t1p-1p1c-64kb-b10000": (F3, "1 partition, 64 KB", "queen", "popBatch 10,000", "nr2", 1, 1, 65536, 1, 0),
    "kafka-max-1t1p-1p1c-64kb": (F3, "1 partition, 64 KB", "kafka", "stock", "kafka", 1, 1, 65536, 1, 0),
    "native-nr2-max-1t100p-100p100c-100b": (F3, "100 partitions, 100 B", "queen", "pop autopilot", "nr2", 100, 1, 100, 100, 0),
    "kafka-max-1t100p-100p100c-100b": (F3, "100 partitions, 100 B", "kafka", "stock", "kafka", 100, 1, 100, 100, 0),
    "native-nr2-max-1t100p-100p100c-1kb": (F3, "100 partitions, 1 KB", "queen", "pop autopilot", "nr2", 100, 1, 1024, 100, 0),
    "kafka-max-1t100p-100p100c-1kb": (F3, "100 partitions, 1 KB", "kafka", "stock", "kafka", 100, 1, 1024, 100, 0),
    "native-nr2-max-1t100p-100p100c-64kb": (F3, "100 partitions, 64 KB", "queen", "pop autopilot", "nr2", 100, 1, 65536, 100, 0),
    "kafka-max-1t100p-100p100c-64kb": (F3, "100 partitions, 64 KB", "kafka", "stock", "kafka", 100, 1, 65536, 100, 0),
    "native-nr2-max-10t1p-1kb": (F3, "10 topics × 1 partition, 1 KB", "queen", "pop autopilot", "nr2", 1, 10, 1024, 10, 0),
    "kafka-max-10t1p-1kb": (F3, "10 topics × 1 partition, 1 KB", "kafka", "stock", "kafka", 1, 10, 1024, 10, 0),
    # family 4
    "native-nr2-1m-10t-1p-100b": (F4, "1 partition a topic", "queen", "pop autopilot", "nr2", 1, 10, 100, 10, 1_000_000),
    "native-nr2-1m-10t-1p-100b-b10000": (F4, "1 partition a topic", "queen", "popBatch 10,000", "nr2", 1, 10, 100, 10, 1_000_000),
    "kafka-1m-10t-1p-100b": (F4, "1 partition a topic", "kafka", "stock", "kafka", 1, 10, 100, 10, 1_000_000),
    "native-nr2-1m-10t-16p-100b": (F4, "16 partitions a topic", "queen", "pop autopilot", "nr2", 16, 10, 100, 10, 1_000_000),
    "kafka-1m-10t-16p-100b": (F4, "16 partitions a topic", "kafka", "stock", "kafka", 16, 10, 100, 10, 1_000_000),
    # family 2
    "native-nr2s16-t100": (F2, "100 topics", "queen", "pop autopilot", "nr2", 1, 100, 1024, 4, 500_000),
    "native-nr2s16-t100-b10000": (F2, "100 topics", "queen", "popBatch 10,000", "nr2", 1, 100, 1024, 4, 500_000),
    "native-nr2s16-t1k": (F2, "1,000 topics", "queen", "pop autopilot", "nr2", 1, 1000, 1024, 4, 500_000),
    "native-nr2s16-t1k-b10000": (F2, "1,000 topics", "queen", "popBatch 10,000", "nr2", 1, 1000, 1024, 4, 500_000),
    "native-nr2s16-t10k": (F2, "10,000 topics", "queen", "pop autopilot", "nr2", 1, 10_000, 1024, 4, 500_000),
    "native-nr2s16-t100k": (F2, "100,000 topics", "queen", "pop autopilot", "nr2", 1, 100_000, 1024, 4, 500_000),
    "kafka-t100": (F2, "100 topics", "kafka", "stock", "kafka", 1, 100, 1024, 4, 500_000),
    "kafka-t1k": (F2, "1,000 topics", "kafka", "stock", "kafka", 1, 1000, 1024, 4, 500_000),
}
NOTES = {
    "kafka-p10000-200k": "Kafka 4.3 refused OMB's single CreateTopics of 10,000 partitions; not a speed measurement",
    "kafka-p10000-500k": "Kafka 4.3 refused OMB's single CreateTopics of 10,000 partitions; not a speed measurement",
    "kafka-t1k": "OMB's Kafka driver failed in the warm-up: a worker running ~1,333 producers answered HTTP 500",
    "kafka-p100-1000k": "OMB's stock kafka-throughput.yaml (1 MB batches) with 4 producers: the producers were the limit",
}
# a run whose rate the producers met but whose consumers did not keep up throughout
OVERRIDE = {
    "native-nr2s16-t100": ("no", "a topic or two fell behind: the backlog grew about 6k msg/s, e2e p99 35 s"),
}
NUM = r"([0-9][0-9,]*(?:\.[0-9]+)?)"


def num(s):
    return float(s.replace(",", ""))


def rate(s):
    v = num(s[:-1]) if s.endswith("k") else num(s)
    return v * 1000 if s.endswith("k") else v


def parse(line):
    m = re.search(r"pub ([0-9,.]+k?)/s cons ([0-9,.]+k?)/s err ([0-9,]+)", line)
    if not m:
        return None
    out = {"pub_msgs": rate(m.group(1)), "cons_msgs": rate(m.group(2)), "errors": num(m.group(3))}
    m = re.search(r"pub p50/p99/p99\.9 %s/%s/%s ms" % (NUM, NUM, NUM), line)
    out.update(pub_p50=num(m.group(1)), pub_p99=num(m.group(2)), pub_p999=num(m.group(3)))
    m = re.search(r"e2e p50/p99/p99\.9 %s/%s/%s ms" % (NUM, NUM, NUM), line)
    out.update(e2e_p50=num(m.group(1)), e2e_p99=num(m.group(2)), e2e_p999=num(m.group(3)))
    m = re.search(r"brokers %s cores" % NUM, line)
    out["broker_cores"] = num(m.group(1))
    m = re.search(r"disk write ([0-9/]+) MB/s", line)
    out["disk_mb_s"] = max(int(x) for x in m.group(1).split("/")) if m else ""
    return out


cols = ["tag", "family", "workload", "system", "config", "build", "partitions", "topics", "msg_bytes", "producers",
        "target_msgs", "pub_msgs", "cons_msgs", "carried", "errors", "pub_p50", "pub_p99", "pub_p999", "e2e_p50",
        "e2e_p99", "e2e_p999", "broker_cores", "disk_mb_s", "note"]
rows = []
for tag, (fam, wl, sysname, cfg, build, parts, topics, size, prods, target) in TAGS.items():
    line = open(os.path.join(R, tag + ".out")).read().strip().splitlines()[-1]
    p = parse(line)
    row = dict(tag=tag, family=fam, workload=wl, system=sysname, config=cfg, build=build, partitions=parts,
               topics=topics, msg_bytes=size, producers=prods, target_msgs=target, note=NOTES.get(tag, ""))
    if p is None:
        row.update(carried="no")
    else:
        row.update(p)
        if target:
            ok = p["pub_msgs"] >= 0.98 * target and p["cons_msgs"] >= 0.98 * target
            row["carried"] = "yes" if ok else "no"
        else:
            row["carried"] = ""
    if tag in OVERRIDE:
        row["carried"], row["note"] = OVERRIDE[tag]
    rows.append(row)
with open(os.path.join(H, "omb_points.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in cols})

# backlog: 100 GB of 1 KB built with the consumers stopped, then drained (OMB's own lines)
bl = [("native-nr2-backlog-1t-16p", "queen", 16, ""), ("kafka-backlog-1t-16p-ret", "kafka", 16, "log.retention.ms=-1"),
      ("kafka-backlog-1t-1p-ret", "kafka", 1, "log.retention.ms=-1")]
with open(os.path.join(H, "omb_backlog.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["tag", "system", "partitions", "build_s", "drain_s", "backlog_messages", "note"])
    for tag, sysname, parts, note in bl:
        log = open(os.path.join(R, tag, "benchmark.log")).read()
        b = float(re.search(r"Completed backlog build in ([0-9.]+) s", log).group(1))
        d = float(re.search(r"Completed backlog draining in ([0-9.]+) s", log).group(1))
        mx = re.search(r"backlog max ([0-9,]+)", open(os.path.join(R, tag + ".out")).read().splitlines()[-1]).group(1)
        w.writerow([tag, sysname, parts, round(b, 1), round(d, 1), int(mx.replace(",", "")), note])
    w.writerow(["native-nr2-backlog-1t-1p", "queen", 1, "", "", "",
                "not run: one partition consumes 65k msg/s with the default pops, below the 100k msg/s producer"])

# the Kafka facade across a leader change (failover/run.sh on nr6)
with open(os.path.join(H, "facade_failover.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["how", "acked", "lost", "duplicated", "reordered"])
    for tag, how in [("failover-kill-nr6", "kill -9 of the raft leader"), ("failover-stop-nr6", "graceful stop of the raft leader")]:
        v = json.load(open(os.path.join(R, tag, "verify.json")))
        w.writerow([how, v["acked"], v["lost"], v["duplicates"], v["reorders"]])
print(len(rows), "points")
