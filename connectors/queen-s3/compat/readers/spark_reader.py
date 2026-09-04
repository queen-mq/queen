"""Apache Spark (PySpark, local mode) against the sample lake.

Submitted with `spark-submit` inside the official `apache/spark` image, which is
the only way to get a JVM, Hadoop's `CompressionCodecFactory` and parquet-java
into the same process as the check.

Two Spark-specific things the matrix records:

* **the two zstds are not the same zstd.** Parquet's zstd is decoded by
  parquet-java's own pure-JVM binding and works; JSONL's `.zst` goes through
  Hadoop's `ZStandardCodec`, which is a JNI wrapper around libhadoop — present as
  a class, useless without the native build. The exact message is in the verdict.
* **partition discovery on `queue=`.** The queue name is only ever the path key
  (plan §6.3, §6.4) — no row repeats it — so `spark.read` over the prefix root
  is what turns it back into a column, for JSON exactly as for Parquet. This is
  the layout Spark itself writes with `partitionBy("queue", "dt", "hour")`, and
  reading it back is measured here, not assumed.

The session timezone is pinned to UTC. It is not a detail: `ts` is the broker's
`clock_timestamp()` in UTC, and Spark's `to_timestamp` on the JSONL text reads it
in the SESSION zone — on a machine in Europe/Rome the same object would come back
two hours out.
"""

import sys
import traceback

sys.path.insert(0, "/readers")

import _common as C  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

READER = "spark"
ENTITY = {"orders": "cust 42/eu", "audit": "région-eu"}
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


def main():
    spark = (
        SparkSession.builder.appName("queen-s3-compat")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    v = spark.version
    exp = C.load_expected()

    for prefix, meta in sorted(exp["prefixes"].items()):
        fmt, comp = meta["format"], meta["compression"]
        base = "%s/%s" % (C.SAMPLES, prefix)
        call = (
            "spark.read.parquet('<prefix>')"
            if fmt == "parquet"
            else "spark.read.json('<prefix>')"
        )
        verdict, detail = "ok", ""
        try:
            df = (
                spark.read.parquet(base)
                if fmt == "parquet"
                else spark.read.json(base)
            )
            # Partition discovery has to have found the keys: `queue` is a
            # column ONLY because Spark read it out of the directory name.
            missing = [c for c in ("queue", "dt", "hour") if c not in df.columns]
            if missing:
                raise RuntimeError("partition discovery lost %s" % missing)
            for queue in sorted(exp["queues"]):
                sub = df.filter(F.col("queue") == queue)
                rows = [
                    (r["partition"], r["offset"])
                    for r in sub.orderBy("partition", "offset")
                    .select("partition", "offset")
                    .collect()
                ]
                nulls = sub.filter(F.col("payload").isNull()).count()
                bad = C.check_queue(exp, queue, rows, nulls)
                if bad:
                    verdict, detail = bad, "queue=%s" % queue
                    break
            if verdict == "ok":
                bad = entity_history(exp, df, fmt)
                if bad:
                    verdict, detail = bad, "the plan §7 entity-history query"
        except Exception as e:  # noqa: BLE001
            verdict, detail = classify(e), one_line(e)
        C.emit(READER, v, fmt, comp, verdict, call, detail)

    extras(spark, exp)
    spark.stop()


def entity_history(exp, df, fmt):
    """One entity's history in offset order, with payload fields decoded.

    The two formats need different expressions and that IS the finding: in
    Parquet `payload` is a JSON *string* and wants `get_json_object`, in JSONL
    Spark infers it as a nested struct and wants field access. A query written
    for one does not run against the other."""
    if fmt == "parquet":
        cols = [
            F.col("transaction_id").alias("txn"),
            F.unix_micros(F.col("ts")).alias("tsm"),
            F.get_json_object("payload", "$.type").alias("type"),
            F.get_json_object("payload", "$.amount").cast("bigint").alias("amount"),
            F.get_json_object("payload", "$.customer.name").alias("cname"),
            F.get_json_object("payload", "$.note").alias("note"),
        ]
    else:
        cols = [
            F.col("transactionId").alias("txn"),
            F.unix_micros(F.to_timestamp(F.col("ts"), TS_FORMAT)).alias("tsm"),
            F.col("payload.type").alias("type"),
            F.col("payload.amount").cast("bigint").alias("amount"),
            F.col("payload.customer.name").alias("cname"),
            F.col("payload.note").alias("note"),
        ]
    for queue in sorted(exp["queues"]):
        rows = (
            df.filter((F.col("queue") == queue) & (F.col("partition") == ENTITY[queue]))
            .select(F.col("partition"), F.col("offset"), *cols)
            .orderBy("offset")
            .limit(3)
            .collect()
        )
        got = {
            (r["partition"], r["offset"]): {
                "partition": r["partition"],
                "offset": r["offset"],
                "transactionId": r["txn"],
                "tsMicros": r["tsm"],
                "type": r["type"],
                "amount": r["amount"],
                "customerName": r["cname"],
                "note": r["note"],
            }
            for r in rows
        }
        bad = C.check_spot(exp, queue, got)
        if bad:
            return bad
    return None


def extras(spark, exp):
    try:
        loaded = spark._jvm.org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded()
        C.note(
            READER,
            "hadoop native code loaded: %s — org.apache.hadoop.io.compress.ZStandardCodec "
            "is on the classpath either way, so `.jsonl.zst` fails at DECOMPRESSION, not "
            "at codec lookup. Parquet+zstd in the same JVM is unaffected: parquet-java "
            "does not go through the Hadoop codec." % loaded,
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "native codec probe failed: %s" % one_line(e))

    try:
        df = spark.read.parquet("%s/parquet-zstd" % C.SAMPLES)
        n = df.filter(F.col("hour") == 10).count()
        want = sum(
            o["records"]
            for o in exp["prefixes"]["parquet-zstd"]["objects"]
            if o["k"] == exp["windows"][0]["k"]
        )
        C.note(
            READER,
            "partition discovery gives %s; pruning on hour=10 returns %d rows, expected "
            "%d — %s" % (df.columns, n, want, "ok" if n == want else "WRONG"),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "partition pruning probe failed: %s" % one_line(e))

    try:
        t = dict(spark.read.json("%s/jsonl-none" % C.SAMPLES).dtypes)
        C.note(
            READER,
            "JSON inference types ts as %s (a string: Spark does not infer timestamps "
            "from JSON unless you ask) and payload as a nested struct, so a query "
            "written against the Parquet payload string does not run unchanged"
            % t.get("ts"),
        )
    except Exception as e:  # noqa: BLE001
        C.note(READER, "json inference probe failed: %s" % one_line(e))


def one_line(e):
    text = "%s: %s" % (type(e).__name__, e)
    return " / ".join(text.splitlines())[:400]


def classify(e):
    """A missing native codec is `unsupported`: the object is valid — four other
    readers in this lane decode the same bytes — and this JVM simply has no zstd.
    Everything else is a `fail`."""
    text = str(e)
    if "native zStandard library not available" in text or "zStandard" in text:
        return (
            "unsupported:hadoop ZStandardCodec needs libhadoop built with zstd; this "
            "image has none (native code not loaded)"
        )
    if "Codec" in text and "not found" in text:
        return "unsupported:%s" % one_line(e)[:150]
    return "fail:%s" % one_line(e)[:200]


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)
