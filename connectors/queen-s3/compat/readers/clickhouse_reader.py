"""ClickHouse against the sample lake, through `clickhouse-local` in the
`clickhouse/clickhouse-server` image.

Runs on the HOST and shells out to docker per query — unlike the DuckDB and
dataframe scripts, which run inside their container — because the image has no
Python and the check must stay the same code for every reader.

The plan's recipe (§7) is `s3('…/*.jsonl.zst', 'JSONEachRow')`; `file()` is the
same reader over a mounted directory, and it is the half that can be measured
without an object store in the loop.

Two things this measures that no other reader in the lane does:

* **schema inference on the JSONL envelope.** Left to itself ClickHouse infers
  `payload` as a `Tuple(...)` of the union of the keys it sampled, which is not
  a JSON document any more: `payload IS NULL` is then false for every row and
  the 14 null payloads vanish. The incantation the matrix records passes the
  structure explicitly and keeps `payload` a `Nullable(String)`.
* **hive partitioning**, which recent ClickHouse turns on by itself: `queue`,
  `dt` and `hour` arrive as columns with no argument at all. That is the only
  way the queue name is knowable here — it is the `queue=` path key and no row
  repeats it (plan §6.3, §6.4) — and it works even with the structure spelled
  out below, which lists the file's five fields only.
"""

import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import _common as C  # noqa: E402

READER = "clickhouse"
IMAGE = os.environ.get("QUEEN_S3_CH_IMAGE", "clickhouse/clickhouse-server:latest")
CONTAINER = "queen-s3-compat-clickhouse"

# The JSONL envelope, spelled out: the file's five fields. `queue` is NOT one of
# them (it is the path key, and hive partitioning supplies it). `offset` is a
# keyword in ClickHouse and needs backticks everywhere it appears.
JSONL_STRUCT = (
    "partition String, `offset` Int64, transactionId String, "
    "ts String, payload Nullable(String)"
)
ENTITY = {"orders": "cust 42/eu", "audit": "région-eu"}


def ch(samples, sql):
    """One `clickhouse-local --query`. Returns stdout; raises with the tail of
    stderr, which is where ClickHouse puts the interesting half."""
    out = subprocess.run(
        [
            "docker", "run", "--rm", "--name", CONTAINER,
            "-v", "%s:/samples:ro" % samples,
            "--entrypoint", "clickhouse-local", IMAGE,
            "--query", sql,
        ],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(
            (out.stderr.strip().splitlines() or ["exit %d" % out.returncode])[-1][:400]
        )
    return out.stdout


def rows_json(samples, sql):
    return [json.loads(line) for line in ch(samples, sql).splitlines() if line.strip()]


def source(prefix, fmt, ext, explicit=True):
    glob = "/samples/%s/queue=*/dt=*/hour=*/*.%s" % (prefix, ext)
    if fmt == "parquet":
        return "file('%s', 'Parquet')" % glob
    if explicit:
        return "file('%s', 'JSONEachRow', '%s')" % (glob, JSONL_STRUCT)
    return "file('%s', 'JSONEachRow')" % glob


def version(samples):
    try:
        return ch(samples, "SELECT version()").strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def main():
    if len(sys.argv) != 2:
        print("usage: clickhouse_reader.py <samples-dir>", file=sys.stderr)
        return 2
    samples = os.path.abspath(sys.argv[1])
    exp = C.load_expected(os.path.join(samples, "expected.json"))
    v = version(samples)

    for prefix, meta in sorted(exp["prefixes"].items()):
        fmt, comp, ext = meta["format"], meta["compression"], meta["ext"]
        src = source(prefix, fmt, ext)
        incantation = src.replace("/samples/%s/" % prefix, "<prefix>/")
        verdict, detail = "ok", ""
        try:
            for queue in sorted(exp["queues"]):
                rows = rows_json(
                    samples,
                    "SELECT partition, `offset` FROM %s WHERE queue = '%s' "
                    "ORDER BY partition, `offset` FORMAT JSONCompactEachRow"
                    % (src, queue),
                )
                nulls = int(
                    ch(
                        samples,
                        "SELECT countIf(payload IS NULL) FROM %s WHERE queue = '%s'"
                        % (src, queue),
                    ).strip()
                )
                bad = C.check_queue(exp, queue, [(r[0], r[1]) for r in rows], nulls)
                if bad:
                    verdict, detail = bad, "queue=%s" % queue
                    break
            if verdict == "ok":
                bad = entity_history(samples, exp, prefix, fmt, ext)
                if bad:
                    verdict, detail = bad, "the plan §7 entity-history query"
        except Exception as e:  # noqa: BLE001
            verdict, detail = "fail:%s" % str(e)[:200], str(e)[:400]
        C.emit(READER, v, fmt, comp, verdict, incantation, detail)

    extras(samples, exp, v)
    return 0


def entity_history(samples, exp, prefix, fmt, ext):
    """One entity's history with payload fields decoded, in offset order."""
    src = source(prefix, fmt, ext)
    if fmt == "parquet":
        txn, ts = "transaction_id", "toUnixTimestamp64Micro(ts)"
    else:
        txn = "transactionId"
        ts = "toUnixTimestamp64Micro(parseDateTime64BestEffort(ts, 6, 'UTC'))"
    for queue in sorted(exp["queues"]):
        rows = rows_json(
            samples,
            "SELECT partition, `offset`, %s, %s, JSONExtractString(payload, 'type'), "
            "JSONExtractInt(payload, 'amount'), "
            "JSONExtractString(JSONExtractRaw(payload, 'customer'), 'name'), "
            "JSONExtractString(payload, 'note') FROM %s WHERE queue = '%s' AND "
            "partition = '%s' ORDER BY `offset` LIMIT 3 FORMAT JSONCompactEachRow"
            % (txn, ts, src, queue, ENTITY[queue].replace("'", "''")),
        )
        got = {
            (r[0], r[1]): {
                "partition": r[0],
                "offset": r[1],
                "transactionId": r[2],
                "tsMicros": r[3],
                "type": r[4],
                "amount": r[5],
                "customerName": r[6],
                "note": r[7],
            }
            for r in rows
        }
        bad = C.check_spot(exp, queue, got)
        if bad:
            return bad
    return None


def extras(samples, exp, v):
    try:
        desc = ch(
            samples,
            "DESCRIBE %s FORMAT JSONCompactEachRow" % source("jsonl-zstd", "jsonl", "jsonl.zst", explicit=False),
        )
        types = {json.loads(l)[0]: json.loads(l)[1] for l in desc.splitlines() if l.strip()}
        C.note(
            READER,
            "JSONEachRow schema inference with NO structure argument types payload as "
            "%s and ts as %s, and adds the hive keys %s by itself; a Tuple payload is "
            "not NULL for a null record, so the null count comes out as 0. Pass the "
            "structure to keep payload a Nullable(String)."
            % (
                types.get("payload", "?").split("(")[0],
                types.get("ts", "?"),
                [k for k in ("queue", "dt", "hour") if k in types],
            ),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "schema inference probe failed: %s" % e)

    try:
        n = int(
            ch(
                samples,
                "SELECT count() FROM file('/samples/parquet-zstd/queue=*/dt=*/hour=*/"
                "*.parquet', 'Parquet') WHERE hour = 10",
            ).strip()
        )
        want = sum(
            o["records"]
            for o in exp["prefixes"]["parquet-zstd"]["objects"]
            if o["k"] == exp["windows"][0]["k"]
        )
        C.note(
            READER,
            "the automatic hive column `hour` filters: hour=10 gives %d rows, expected "
            "%d — %s" % (n, want, "ok" if n == want else "WRONG"),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "hive filter probe failed: %s" % e)

    try:
        t = ch(
            samples,
            "SELECT toString(toTypeName(ts)) FROM file('/samples/parquet-zstd/queue=*/"
            "dt=*/hour=*/*.parquet', 'Parquet') LIMIT 1",
        ).strip().replace("\\'", "'")
        C.note(READER, "Parquet ts arrives as %s — the TIMESTAMP(MICROS,UTC) annotation survives" % t)
    except Exception as e:  # noqa: BLE001
        C.note(READER, "parquet ts probe failed: %s" % e)


if __name__ == "__main__":
    sys.exit(main())
