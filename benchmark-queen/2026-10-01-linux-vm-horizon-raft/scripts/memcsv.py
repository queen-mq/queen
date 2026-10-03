"""CSV tables: soak memory per fifth of the run, and per-process memory mid-run.
Usage: memcsv.py soak|procs RESULTS_ROOT"""
import collections
import csv
import glob
import json
import statistics
import sys
mode, root = sys.argv[1], sys.argv[2]
out = csv.writer(sys.stdout, lineterminator='\n')
MiB = 2 ** 20
def samples(path):
    return [d for d in (json.loads(line) for line in open(path)) if d.get('type') == 'sample']


def median_mib(processes, key):
    return round(statistics.median([(p.get(key) or 0) for p in processes]) / MiB, 1)


if mode == 'soak':
    out.writerow(['engine', 'target', 'measure', 'fifth_1_mib', 'fifth_2_mib', 'fifth_3_mib', 'fifth_4_mib', 'fifth_5_mib'])
    for engine in ('horizon', 'queen-rust'):
        series = collections.defaultdict(list)
        for d in samples(glob.glob(f'{root}/load/soak/*/{engine}/fixed/r01/stats.jsonl')[0]):
            for t in d['targets']:
                ps = t.get('processes') or []
                pss = sum((p.get('pss_bytes') or 0) for p in ps)
                series[(t['label'], 'cgroup_memory')].append(((t.get('cgroup') or {}).get('memory') or {}).get('current_bytes') or 0)
                series[(t['label'], 'process_anon_rss')].append(sum((p.get('rss_anon_bytes') or 0) for p in ps))
                if pss:
                    series[(t['label'], 'process_pss')].append(pss)
        for (label, measure), xs in sorted(series.items()):
            k = max(1, len(xs) // 5)
            out.writerow([engine, label, measure] + [round(statistics.median(xs[i:i + k]) / MiB, 1) for i in range(0, k * 5, k)])
else:
    out.writerow(['lane', 'engine', 'role', 'processes', 'rss_mib_each', 'private_mib_each', 'pss_mib_each', 'pss_mib_total'])
    for lane in ('drain-32', 'drain-64'):
        for engine in ('horizon', 'queen-rust'):
            data = samples(glob.glob(f'{root}/drain/{lane}/*/{engine}/fixed/r01/stats.jsonl')[0])
            mid = data[len(data) // 2:len(data) // 2 + 5]
            rows = collections.defaultdict(list)
            for d in mid:
                for t in d['targets']:
                    if t['label'] == 'app':
                        for p in t.get('processes') or []:
                            rows[p.get('role')].append(p)
            for role in ('orchestrator', 'worker', 'lease_renewer'):
                ps = rows.get(role)
                if not ps:
                    continue
                out.writerow([lane, engine, role, round(len(ps) / len(mid)), median_mib(ps, 'rss_bytes'),
                              median_mib(ps, 'private_bytes'), median_mib(ps, 'pss_bytes'),
                              round(sum((p.get('pss_bytes') or 0) for p in ps) / len(mid) / MiB, 1)])
