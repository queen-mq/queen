"""DuckDB against the sample lake — the reader plan §7's first recipe names.

Runs inside `queen-s3-compat-python`. For every (format, compression) it asks
DuckDB three questions:

1. **exactness** — `ORDER BY partition, "offset"` over the whole prefix, checked
   against `expected.json` (count per queue and per partition, NULL payloads,
   digest in the returned order);
2. **hive partitioning** — the `queue=/dt=/hour=` keys as columns. `queue` is
   the interesting one: it is ONLY a path key (plan §6.3, §6.4), so every query
   below that says `WHERE queue = ?` is reading a value DuckDB recovered from
   the directory name, and without `hive_partitioning = true` there would be no
   such column at all;
3. **the entity-history query** of plan §7: one partition's records in offset
   order with a field pulled out of the payload.

`offset` is a reserved word in DuckDB and must be quoted everywhere.
"""

import sys

sys.path.insert(0, "/readers")

import importlib.metadata as md  # noqa: E402

import duckdb  # noqa: E402

import _common as C  # noqa: E402

READER = "duckdb"
VERSION = md.version("duckdb")
ENTITY = {"orders": "cust 42/eu", "audit": "région-eu"}


def source(prefix, fmt, comp, ext, hive=True):
    """The FROM clause, and the string MATRIX.md prints as the incantation."""
    glob = "%s/%s/queue=*/dt=*/hour=*/*.%s" % (C.SAMPLES, prefix, ext)
    hive_arg = ", hive_partitioning = true" if hive else ""
    if fmt == "parquet":
        # No compression argument: a Parquet file names its codec per column
        # chunk in its own footer, so `snappy` and `zstd` are the same call.
        return "read_parquet('%s'%s)" % (glob, hive_arg)
    return "read_json_auto('%s', format = 'newline_delimited', compression = '%s'%s)" % (
        glob,
        duckdb_compression(comp),
        hive_arg,
    )


def duckdb_compression(comp):
    """DuckDB's `FileCompressionType` enum has no `none`: the value is spelled
    `uncompressed` (or `auto`), and passing `'none'` is a NotImplementedException,
    not a no-op. Measured on 1.5.5 and recorded here rather than worked around
    silently."""
    return "uncompressed" if comp == "none" else comp


def json_columns_source(prefix, ext, comp):
    """The explicit-schema form. Needed for the entity query on JSONL: under
    `read_json_auto` the payload is inferred as a DuckDB STRUCT, and `->>` is a
    JSON operator — the schema has to say JSON for plan §7's query to work
    unchanged across formats.

    `columns` lists the FILE's five fields only. `queue` is not one of them: it
    comes from `hive_partitioning = true`, and naming it here would ask DuckDB
    for a JSON field that no line has."""
    glob = "%s/%s/queue=*/dt=*/hour=*/*.%s" % (C.SAMPLES, prefix, ext)
    return (
        "read_json('%s', format = 'newline_delimited', compression = '%s', "
        "hive_partitioning = true, columns = {partition: 'VARCHAR', "
        "\"offset\": 'BIGINT', transactionId: 'VARCHAR', ts: 'VARCHAR', payload: 'JSON'})"
    ) % (glob, duckdb_compression(comp))


def main():
    exp = C.load_expected()
    con = duckdb.connect()
    queues = sorted(exp["queues"])

    for prefix, meta in exp["prefixes"].items():
        fmt, comp, ext = meta["format"], meta["compression"], meta["ext"]
        src = source(prefix, fmt, comp, ext)
        incantation = src.replace(C.SAMPLES + "/" + prefix, "<prefix>")
        verdict, detail = "ok", ""
        try:
            for queue in queues:
                rows = con.execute(
                    'SELECT partition, "offset" FROM %s WHERE queue = ? '
                    'ORDER BY partition, "offset"' % src,
                    [queue],
                ).fetchall()
                nulls = con.execute(
                    "SELECT count(*) FROM %s WHERE queue = ? AND payload IS NULL" % src,
                    [queue],
                ).fetchone()[0]
                bad = C.check_queue(exp, queue, rows, nulls)
                if bad:
                    verdict, detail = bad, "queue=%s" % queue
                    break
            if verdict == "ok":
                bad = entity_history(con, exp, prefix, fmt, comp, ext)
                if bad:
                    verdict, detail = bad, "the plan §7 entity-history query"
            if verdict == "ok":
                bad = hive_columns(con, exp, src)
                if bad:
                    verdict, detail = bad, "hive_partitioning = true"
        except Exception as e:  # noqa: BLE001 - the verdict IS the exception
            verdict = "fail:%s" % type(e).__name__
            detail = str(e).replace("\n", " ")[:400]
        C.emit(READER, VERSION, fmt, comp, verdict, incantation, detail)

    extras(con, exp)


def entity_history(con, exp, prefix, fmt, comp, ext):
    """Plan §7: `SELECT partition, offset, ts, payload->>'type' … WHERE partition
    = ? ORDER BY offset` — one entity's exact history, with a payload field
    decoded. Compared against `expected.json`'s spot rows."""
    if fmt == "parquet":
        src = source(prefix, fmt, comp, ext)
        ts_expr = "epoch_us(ts)"
        # The envelope is snake_case in Parquet and camelCase in JSONL
        # (plan §6.4). A reader that moves between the two formats renames one
        # column, and this lane says which.
        txn = "transaction_id"
    else:
        src = json_columns_source(prefix, ext, comp)
        # ts is the broker's ISO text in JSONL; parse it back to microseconds so
        # both formats are checked against the same expected number.
        ts_expr = "epoch_us(CAST(ts AS TIMESTAMP))"
        txn = "transactionId"
    for queue in sorted(exp["queues"]):
        rows = con.execute(
            'SELECT partition, "offset", %s, %s, payload->>\'type\', '
            "CAST(payload->>'amount' AS BIGINT), payload->>'$.customer.name', "
            "payload->>'note' FROM %s WHERE queue = ? AND partition = ? "
            'ORDER BY "offset" LIMIT 3' % (txn, ts_expr, src),
            [queue, ENTITY[queue]],
        ).fetchall()
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


def hive_columns(con, exp, src):
    """The `queue=/dt=/hour=` keys must come back as columns AND agree with the
    data. `queue` is the interesting one: it is a key and nothing else, so this
    is the only place the queue name can come from."""
    rows = con.execute(
        "SELECT queue, CAST(dt AS VARCHAR), CAST(hour AS VARCHAR), count(*) "
        "FROM %s GROUP BY 1, 2, 3 ORDER BY 1, 3" % src
    ).fetchall()
    hours = sorted({r[2] for r in rows})
    want_hours = sorted(exp["hive"]["hour"])
    if [h.lstrip("0") or "0" for h in hours] != [h.lstrip("0") or "0" for h in want_hours]:
        return "fail:hive hour columns %r != %r" % (hours, want_hours)
    if sorted({r[0] for r in rows}) != sorted(exp["hive"]["queue"]):
        return "fail:hive queue column %r" % sorted({r[0] for r in rows})
    if sum(r[3] for r in rows) != exp["records"]:
        return "fail:hive total %d" % sum(r[3] for r in rows)
    return None


def extras(con, exp):
    """Observations that are not per-cell verdicts."""
    pq = "%s/parquet-zstd/queue=orders/dt=2026-09-04/hour=10/*.parquet" % C.SAMPLES
    try:
        meta = con.execute(
            "SELECT DISTINCT created_by, num_row_groups, format_version "
            "FROM parquet_file_metadata('%s')" % pq
        ).fetchall()
        # Two pairs now: `queen.envelope=1` and the queue name, which is what
        # keeps ONE object self-describing after someone copies it out of the
        # `queue=` layout.
        kv = con.execute(
            "SELECT key, value FROM parquet_kv_metadata('%s')" % pq
        ).fetchall()
        C.note(
            READER,
            "parquet footer as DuckDB sees it: created_by=%r row_groups=%s "
            "format_version=%s key_value=%s"
            % (
                meta[0][0],
                meta[0][1],
                meta[0][2],
                [(k.decode(), v.decode()) for k, v in kv],
            ),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "parquet metadata functions failed: %s" % e)

    try:
        t = con.execute(
            "SELECT typeof(ts), typeof(payload) FROM read_json_auto('%s/jsonl-zstd/"
            "queue=*/dt=*/hour=*/*.jsonl.zst', compression = 'zstd') LIMIT 1"
            % C.SAMPLES
        ).fetchone()
        C.note(
            READER,
            "read_json_auto infers ts as %s (the trailing Z is dropped, so it is NOT "
            "timezone-aware) and payload as %s — pass columns={...payload: 'JSON'} to "
            "keep `->>` working on it" % (t[0], t[1].split("(")[0]),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "type probe failed: %s" % e)

    try:
        n = con.execute(
            "SELECT count(*) FROM read_parquet('%s/parquet-zstd/queue=*/dt=*/hour=*/"
            "*.parquet', hive_partitioning = true) WHERE hour = 10" % C.SAMPLES
        ).fetchone()[0]
        want = sum(
            o["records"]
            for o in exp["prefixes"]["parquet-zstd"]["objects"]
            if o["k"] == exp["windows"][0]["k"]
        )
        C.note(
            READER,
            "hive pruning on hour=10 returns %d rows (expected %d): %s"
            % (n, want, "ok" if n == want else "WRONG"),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "hive pruning probe failed: %s" % e)


if __name__ == "__main__":
    main()
