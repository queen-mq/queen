#!/usr/bin/env python3
"""padmin.py <command> [options] — the Pulsar admin REST calls of the harness. Runs ON a broker node (pc.sh calls it):
the private IPs are not reachable from the Mac. Commands:
  ns-setup --admin URL [--cluster bench --tenant bench --ns ns --bundles 48 --e 3 --qw 3 --qa 2 --mark-delete-rate 1.0]
           tenant + namespace (bundles), persistence, retention 0/0, all read back and checked (= ns-show)
  ns-show  --admin URL                    the namespace's bundles / persistence / retention, checked
  brokers  --admin URL                    active brokers and /admin/v2/brokers/health of each
  bookies  --bookie-http URL              writable / read-only bookies (bookie http API, metadata from ZooKeeper)
  proof    --admin URL [--topic T]        E/Qw/Qa of a real ledger of the namespace (internalStats?metadata=true); with no
           topic in the namespace it creates a probe topic + subscription (the managed ledger opens a ledger) and prints
           "PROBE <topic>" for pc.sh to delete after the bookkeeper-shell check; last line "LEDGER <id>"
  delete   --admin URL --topic T          delete a (probe) topic
  balance  --admin URL [--fix] [--tol 0.10] [--max-moves 24]
           topics (partitions) and bundles per broker for the namespace. topic -> bundle uses the broker's own rule
           (CRC32 of the full topic name, NamespaceBundleFactory), checked against the lookup API on a sample. Bundles
           that hold topics but have no owner yet are assigned first (one lookup each). --fix unloads, from the heaviest
           broker, the bundle whose move brings it and the lightest broker closest to the mean, with destinationBroker =
           the lightest; repeats until every broker is within ±tol of the mean or no whole-bundle move helps.
Output lines never start with "[final]" and never contain "cpu_total=" (report.py finds run files by content)."""
import argparse, bisect, json, random, re, sys, time, urllib.error, urllib.parse, urllib.request, zlib
from concurrent.futures import ThreadPoolExecutor


def ts():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(*a):
    print(f"[{ts()}]", *a, flush=True)


class HttpErr(Exception):
    def __init__(self, code, body, url):
        super().__init__(f"HTTP {code} {url}: {body}")
        self.code, self.body, self.url = code, body, url


def call(method, url, body=None, timeout=60, raw=False, hops=4):
    """one REST call; follows 307s itself (urllib refuses to for PUT/POST/DELETE)"""
    data, headers = None, {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read()
    except urllib.error.HTTPError as e:
        loc = e.headers.get("Location")
        if e.code in (301, 302, 307, 308) and loc and hops > 0:
            return call(method, urllib.parse.urljoin(url, loc), body, timeout, raw, hops - 1)
        raise HttpErr(e.code, e.read().decode(errors="replace")[:300], url)
    txt = b.decode(errors="replace")
    if raw:
        return txt
    return json.loads(txt) if txt.strip() else None


def local(topic):
    return topic.split("/")[-1]


# ---------------------------------------------------------------- ns-setup
def ns_setup(a):
    ns = f"{a.tenant}/{a.ns}"
    for what, method, path, body in (
            ("tenant", "PUT", f"/admin/v2/tenants/{a.tenant}", {"adminRoles": [], "allowedClusters": [a.cluster]}),
            ("namespace", "PUT", f"/admin/v2/namespaces/{ns}", {"bundles": {"numBundles": a.bundles}})):
        try:
            call(method, a.admin + path, body)
            log(f"created {what} {a.tenant if what == 'tenant' else ns}")
        except HttpErr as e:
            if e.code != 409:
                raise
            log(f"{what} exists (409)")
    call("POST", f"{a.admin}/admin/v2/namespaces/{ns}/persistence",
         {"bookkeeperEnsemble": a.e, "bookkeeperWriteQuorum": a.qw, "bookkeeperAckQuorum": a.qa,
          "managedLedgerMaxMarkDeleteRate": a.mark_delete_rate})
    call("POST", f"{a.admin}/admin/v2/namespaces/{ns}/retention", {"retentionTimeInMinutes": 0, "retentionSizeInMB": 0})
    return ns_show(a)


def ns_show(a):
    ns = f"{a.tenant}/{a.ns}"
    b = call("GET", f"{a.admin}/admin/v2/namespaces/{ns}/bundles") or {}
    p = call("GET", f"{a.admin}/admin/v2/namespaces/{ns}/persistence") or {}
    r = call("GET", f"{a.admin}/admin/v2/namespaces/{ns}/retention") or {}
    got = (b.get("numBundles"), p.get("bookkeeperEnsemble"), p.get("bookkeeperWriteQuorum"), p.get("bookkeeperAckQuorum"))
    print(f"namespace {ns}: bundles={got[0]} persistence=E{got[1]}/Qw{got[2]}/Qa{got[3]} "
          f"markDeleteRate={p.get('managedLedgerMaxMarkDeleteRate')} retention={r.get('retentionTimeInMinutes', 0)}min/"
          f"{r.get('retentionSizeInMB', 0)}MB")
    if got != (a.bundles, a.e, a.qw, a.qa):
        print(f"MISMATCH: wanted bundles={a.bundles} E{a.e}/Qw{a.qw}/Qa{a.qa}")
        return 1
    return 0


# ---------------------------------------------------------------- brokers / bookies
def brokers(a):
    lst = sorted(call("GET", f"{a.admin}/admin/v2/brokers/{a.cluster}") or [])
    out = []
    for b in lst:
        try:
            h = call("GET", f"http://{b}/admin/v2/brokers/health", raw=True, timeout=30).strip()
        except Exception as e:  # noqa: BLE001 - print whatever went wrong
            h = f"ERR({str(e)[:80]})"
        out.append(f"{b}={h}")
    print(f"brokers active={len(lst)} " + " ".join(out))
    return 0 if lst and all(x.endswith("=ok") for x in out) else 1


def bookies(a):
    rw = call("GET", f"{a.bookie_http}/api/v1/bookie/list_bookies?type=rw&print_hostnames=false") or {}
    ro = call("GET", f"{a.bookie_http}/api/v1/bookie/list_bookies?type=ro&print_hostnames=false") or {}
    print(f"bookies rw={len(rw)} ro={len(ro)} writable=[{' '.join(sorted(rw))}]" + (f" readonly=[{' '.join(sorted(ro))}]" if ro else ""))
    return 0


# ---------------------------------------------------------------- proof
def proof(a):
    ns = f"{a.tenant}/{a.ns}"
    topic, probe = a.topic, None
    if not topic:
        parts = call("GET", f"{a.admin}/admin/v2/persistent/{ns}/partitioned") or []
        if parts:
            topic = sorted(parts)[0] + "-partition-0"
        else:
            plain = [t for t in (call("GET", f"{a.admin}/admin/v2/persistent/{ns}") or [])
                     if not local(t).startswith(("__", "probe-health-"))]
            if plain:
                topic = sorted(plain)[0]
            else:
                probe = topic = f"persistent://{ns}/probe-health-{int(time.time())}"
                call("PUT", f"{a.admin}/admin/v2/persistent/{ns}/{local(topic)}")
                call("PUT", f"{a.admin}/admin/v2/persistent/{ns}/{local(topic)}/subscription/probe")
                print(f"PROBE {topic}")
    st = None
    for _ in range(20):
        st = call("GET", f"{a.admin}/admin/v2/persistent/{ns}/{local(topic)}/internalStats?metadata=true")
        if st and st.get("ledgers"):
            break
        time.sleep(0.5)
    led = (st or {}).get("ledgers") or []
    if not led:
        print(f"proof {topic}: no ledger yet (state={(st or {}).get('state')})")
        return 1
    L = led[-1]
    md = str(L.get("metadata") or "")
    m = re.search(r"ensembleSize=(\d+), writeQuorumSize=(\d+), ackQuorumSize=(\d+)", md)
    ens = re.search(r"ensembles=\{0=\[([^\]]*)\]", md)
    q = f"ensembleSize={m[1]} writeQuorumSize={m[2]} ackQuorumSize={m[3]}" if m else f"metadata={md[:200]!r}"
    print(f"proof {topic}: ledger {L.get('ledgerId')} ({len(led)} ledgers, state={st.get('state')}) {q}"
          + (f" ensemble=[{ens[1]}]" if ens else "") + " (broker internalStats?metadata=true)")
    print(f"LEDGER {L.get('ledgerId')}")
    return 0 if (m and (int(m[1]), int(m[2]), int(m[3])) == (a.e, a.qw, a.qa)) else 1


def delete(a):
    t = a.topic
    ns = "/".join(t.split("://")[-1].split("/")[:2])
    call("DELETE", f"{a.admin}/admin/v2/persistent/{ns}/{local(t)}?force=true")
    print(f"deleted {t}")
    return 0


# ---------------------------------------------------------------- balance
def bundles_of(admin, ns):
    b = call("GET", f"{admin}/admin/v2/namespaces/{ns}/bundles")
    bnd = b["boundaries"]
    return [int(x, 16) for x in bnd], [f"{bnd[i]}_{bnd[i + 1]}" for i in range(len(bnd) - 1)]


def bundle_of(name, bounds, names):
    h = zlib.crc32(name.encode()) & 0xFFFFFFFF          # Guava Hashing.crc32() == zlib crc32, padToLong = unsigned
    return names[min(max(bisect.bisect_right(bounds, h) - 1, 0), len(names) - 1)]


def ns_topics(admin, ns):
    parts = call("GET", f"{admin}/admin/v2/persistent/{ns}/partitioned") or []

    def nparts(t):
        return t, (call("GET", f"{admin}/admin/v2/persistent/{ns}/{local(t)}/partitions") or {}).get("partitions", 0)

    with ThreadPoolExecutor(16) as ex:
        res = list(ex.map(nparts, parts))
    names = [f"{t}-partition-{i}" for t, n in res for i in range(n)]
    pset = set(parts)
    for t in call("GET", f"{admin}/admin/v2/persistent/{ns}") or []:
        if "-partition-" in t or t in pset or local(t).startswith(("__", "probe-health-")):
            continue
        names.append(t)
    return names, len(parts)


def ownership(admin, cluster, ns):
    brokers = sorted(call("GET", f"{admin}/admin/v2/brokers/{cluster}") or [])
    own = {}
    for b in brokers:
        for k in call("GET", f"http://{b}/admin/v2/brokers/{cluster}/{b}/ownedNamespaces") or {}:
            if k.startswith(ns + "/"):
                own[k[len(ns) + 1:]] = b
    return brokers, own


def balance(a):
    ns = f"{a.tenant}/{a.ns}"
    bounds, names = bundles_of(a.admin, ns)
    topics, npart = ns_topics(a.admin, ns)
    if not topics:
        print(f"balance {ns}: no topics")
        return 0
    tb = {}
    for t in topics:
        tb.setdefault(bundle_of(t, bounds, names), []).append(t)
    # the local hash must agree with the broker's lookup, else the counts below mean nothing
    sample = random.Random(7).sample(topics, min(4, len(topics)))
    agree = 0
    for t in sample:
        got = call("GET", f"{a.admin}/lookup/v2/topic/persistent/{ns}/{local(t)}/bundle", raw=True).strip().strip('"')
        agree += got == bundle_of(t, bounds, names)
    brokers, own = ownership(a.admin, a.cluster, ns)
    unowned = [b for b in tb if b not in own]
    if unowned:   # bundles that hold topics but were never looked up: one lookup each assigns them
        for b in unowned:
            try:
                call("GET", f"{a.admin}/lookup/v2/topic/persistent/{ns}/{local(tb[b][0])}")
            except HttpErr as e:
                log(f"lookup {tb[b][0]}: {e}")
        time.sleep(2)
        brokers, own = ownership(a.admin, a.cluster, ns)

    def counts():
        c = {b: 0 for b in brokers}
        nb = {b: 0 for b in brokers}
        for bd, br in own.items():
            if br in nb:
                nb[br] += 1
        for bd, ts_ in tb.items():
            if bd in own and own[bd] in c:
                c[own[bd]] += len(ts_)
        return c, nb

    def show(title):
        c, nb = counts()
        mean = len(topics) / max(len(brokers), 1)
        devs = {b: (c[b] - mean) / mean for b in brokers}
        print(f"{title} {ns}: {len(topics)} topics ({npart} partitioned) in {len(tb)} of {len(names)} bundles, "
              f"{len(brokers)} brokers, mean {mean:.1f} topics/broker; hash check {agree}/{len(sample)} lookups agree")
        for b in brokers:
            per = sorted((len(tb.get(bd, [])) for bd, br in own.items() if br == b), reverse=True)
            print(f"  {b:22s} bundles={nb[b]:3d} topics={c[b]:7d} ({devs[b]*100:+6.1f}%)  per-bundle max/min="
                  f"{per[0] if per else 0}/{per[-1] if per else 0}")
        orphan = sum(len(v) for bd, v in tb.items() if bd not in own)
        mx = max(abs(d) for d in devs.values()) if devs else 0
        print(f"  unowned bundles with topics: {sum(1 for bd in tb if bd not in own)} ({orphan} topics); "
              f"max deviation {mx*100:.1f}% (target ±{a.tol*100:.0f}%): {'OK' if mx <= a.tol and not orphan else 'OUT'}")
        return mx, c

    mx, c = show("balance")
    if not a.fix or mx <= a.tol:
        return 0 if mx <= a.tol else 3
    failed_dest = 0
    for move in range(1, a.max_moves + 1):
        c, _ = counts()
        mean = len(topics) / len(brokers)
        if max(abs(v - mean) / mean for v in c.values()) <= a.tol:
            break
        hi = max(brokers, key=lambda b: c[b])
        lo = min(brokers, key=lambda b: c[b])
        spread0 = c[hi] - c[lo]
        best = None
        for bd, br in own.items():
            n = len(tb.get(bd, []))
            if br != hi or n == 0:
                continue
            nc = dict(c)
            nc[hi] -= n
            nc[lo] += n
            spread = max(nc.values()) - min(nc.values())
            if spread < spread0 and (best is None or spread < best[0]):
                best = (spread, bd, n)
        if not best:
            print(f"  fix: no whole-bundle move from {hi} to {lo} narrows the spread ({spread0} topics): stop")
            break
        _, bd, n = best
        try:
            call("PUT", f"http://{hi}/admin/v2/namespaces/{ns}/{bd}/unload?destinationBroker={urllib.parse.quote(lo)}")
        except HttpErr as e:
            print(f"  fix: unload {bd} failed: {e}")
            break
        landed = None
        for _ in range(30):
            try:
                call("GET", f"http://{lo}/lookup/v2/topic/persistent/{ns}/{local(tb[bd][0])}")
            except HttpErr:
                pass
            brokers, own = ownership(a.admin, a.cluster, ns)
            landed = own.get(bd)
            if landed:
                break
            time.sleep(0.5)
        print(f"  fix move {move}: bundle {bd} ({n} topics) {hi} -> {lo} (destinationBroker): landed on {landed}")
        if landed != lo:
            failed_dest += 1
            if failed_dest >= 2:
                print("  fix: destinationBroker not honoured twice: stop")
                break
    mx, _ = show("balance after fix")
    return 0 if mx <= a.tol else 3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["ns-setup", "ns-show", "brokers", "bookies", "proof", "delete", "balance"])
    ap.add_argument("--admin", default="http://127.0.0.1:8080")
    ap.add_argument("--bookie-http", default="http://127.0.0.1:8000")
    ap.add_argument("--cluster", default="bench")
    ap.add_argument("--tenant", default="bench")
    ap.add_argument("--ns", default="ns")
    ap.add_argument("--bundles", type=int, default=48)
    ap.add_argument("--e", type=int, default=3)
    ap.add_argument("--qw", type=int, default=3)
    ap.add_argument("--qa", type=int, default=2)
    ap.add_argument("--mark-delete-rate", type=float, default=1.0)   # the broker default (managedLedgerDefaultMarkDeleteRateLimit)
    ap.add_argument("--topic")
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--tol", type=float, default=0.10)
    ap.add_argument("--max-moves", type=int, default=24)
    a = ap.parse_args()
    fn = {"ns-setup": ns_setup, "ns-show": ns_show, "brokers": brokers, "bookies": bookies, "proof": proof, "delete": delete,
          "balance": balance}[a.cmd]
    try:
        sys.exit(fn(a))
    except HttpErr as e:
        print(f"padmin {a.cmd}: {e}")
        sys.exit(2)
    except (urllib.error.URLError, OSError) as e:
        print(f"padmin {a.cmd}: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
