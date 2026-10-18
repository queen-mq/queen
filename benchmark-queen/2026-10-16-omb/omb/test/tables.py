#!/usr/bin/env python3
# tables.py: the matrix's rows as markdown tables, Queen native beside Kafka, from ../../runs/<tag>/ (scripts/summary.py).
import json, os, sys, subprocess
H = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(H, 'scripts'))
import summary  # noqa: E402

def errors(d):
    try:
        j = json.load(open(os.path.join(d, 'result.json')))
    except Exception:
        return None
    return int(round(sum(j.get('publishErrorRate', []) or [0]) * 10))

def cell(tag):
    d = os.path.join(H, 'runs', tag)
    if not os.path.isdir(d):
        return None
    r = summary.row(d)
    if 'error' in r:
        return {'tag': tag, 'missing': r['error']}
    r['errors'] = errors(d)
    return r

ms = lambda v: '-' if v is None else (f"{v:.1f}" if v < 100 else f"{v:,.0f}")
def line(label, sysname, r):
    if r is None:
        return f"| {label} | {sysname} | not run | | | | | |"
    if 'missing' in r:
        return f"| {label} | {sysname} | {r['missing']} | | | | | |"
    disk = '/'.join(f"{x:.0f}" for x in r['disk_w_mbs']) or '-'
    return (f"| {label} | {sysname} | {r['pub_rate']/1e3:,.0f}k / {r['cons_rate']/1e3:,.0f}k | {ms(r['pub50'])} / {ms(r['pub99'])} / {ms(r['pub999'])} "
            f"| {ms(r['e2e50'])} / {ms(r['e2e99'])} / {ms(r['e2e999'])} | {r['broker_cores']:.1f} | {disk} | {r['errors']} |")

HEAD = "| Point | System | Pub / cons msg/s | Pub p50/p99/p99.9 ms | E2E p50/p99/p99.9 ms | Broker cores (sum of 3) | Disk write MB/s per node | Pub errors |\n|---|---|---|---|---|---|---|---|"
families = {
 '1': [(f"p{p} {r}", n, k) for p in (100, 10000) for r, n, k in [
        ('200k', f'native-nr2-p{p}-200k', f'kafka-p{p}-200k'),
        ('500k', 'native-nr0-p100-500k' if p == 100 else f'native-nr2-p{p}-500k', 'kafka-l1-p100-500k' if p == 100 else f'kafka-p{p}-500k'),
        ('1M', 'native-nr0-p100-1000k' if p == 100 else f'native-nr2-p{p}-1000k', f'kafka-p{p}-1000k'),
        ('2M', f'native-nr2-p{p}-2000k', f'kafka-p{p}-2000k')]],
 '3': [(w, f'native-nr2-max-{w}', f'kafka-max-{w}') for w in ('1t1p-1p1c-100b', '1t1p-1p1c-1kb', '1t1p-1p1c-64kb', '1t100p-100p100c-100b', '1t100p-100p100c-1kb', '1t100p-100p100c-64kb', '10t1p-1kb')],
 '3b': [(f"{w} popBatch {b}", f'native-nr2-max-{w}-b{b}', None) for w in ('1t1p-1p1c-100b', '1t1p-1p1c-1kb', '1t1p-1p1c-64kb') for b in (1000, 10000)],
 '1b': [('p100 1M 8p 8c', 'native-nr2-p100-8p8c-1000k', 'kafka-p100-8p8c-1000k'), ('p100 1M 16p 16c', None, 'kafka-p100-16p16c-1000k'), ('p100 1M 4p 4c, Kafka batch.size 128 KB', None, 'kafka-p100-1000k-b128k')],
 '4b': [('10t-1p-100b popBatch 10000', 'native-nr2-1m-10t-1p-100b-b10000', None)],
 '5b': [('1t-16p retention off', None, 'kafka-backlog-1t-16p-ret'), ('1t-1p retention off', None, 'kafka-backlog-1t-1p-ret')],
 '4': [(w, f'native-nr2-1m-{w}', f'kafka-1m-{w}') for w in ('10t-1p-100b', '10t-16p-100b')],
 '5': [(w, f'native-nr2-backlog-{w}', f'kafka-backlog-{w}') for w in ('1t-1p', '1t-16p')],
 '2': [(f"{t} topics", f'native-nr2s16-t{t}', f'kafka-t{t}') for t in ('100', '1k', '10k', '100k')],
 '2b': [(f"{t} topics popBatch 10000", f'native-nr2s16-t{t}-b10000', None) for t in ('100', '1k')],
}
for fam in sys.argv[1:] or ['1', '1b', '3', '3b', '4', '4b', '5', '5b', '2', '2b']:
    print(f"\n### Family {fam}\n\n{HEAD}")
    for label, n, k in families[fam]:
        if n:
            print(line(label, 'Queen native', cell(n)))
        if k:
            print(line(label, 'Kafka', cell(k)))
