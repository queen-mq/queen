"""Polars, PyArrow and pandas against the sample lake — the three libraries a
notebook actually starts with, one call each.

Not a query engine between them, so the bar is different from DuckDB's: read the
prefix, sort by `(partition, offset)`, and be exact. Each gets its own row in
MATRIX.md because they fail in different places.

**Where the queue name comes from.** It is not in the rows: the sink writes it
only as the `queue=<esc>` key of the path (plan §6.3, §6.4), the way Spark and
Hive write a `partitionBy` key. So every reader here has to get it from the
layout, and the two ways it can are exactly what this row measures:

* **partition discovery** — PyArrow's dataset reader (which `pandas` and
  `polars` also use for Parquet) turns `queue=` into a column by itself. Since
  the column stopped being in the file, `pq.read_table('<prefix>')` and
  `pd.read_parquet('<prefix>')` — the two calls a notebook types first — work:
  before, the discovered key was a `dictionary<string>` and the file's own
  column a `string`, and the schemas would not merge.
* **one prefix per queue** — for JSONL, where none of the three does partition
  discovery over a glob, the reader opens `<prefix>/queue=<name>/…` and the
  queue is the directory it asked for. That is the "a table per queue" shape
  Hive and Athena want anyway.

Where the obvious call fails, the script tries a short ladder of alternatives and
reports the FIRST one that works as the cell's incantation, with the failures as
notes: the question this lane answers is "with which incantation", and "none"
would be a different answer from "this one".
"""

import glob as globmod
import sys

sys.path.insert(0, "/readers")

import importlib.metadata as md  # noqa: E402

import _common as C  # noqa: E402


def version(dist):
    try:
        return md.version(dist)
    except Exception:  # noqa: BLE001
        return "unknown"


def data_glob(prefix, ext, queue="*"):
    """One (format, compression) prefix, one queue (or all of them)."""
    return "%s/%s/queue=%s/dt=*/hour=*/*.%s" % (C.SAMPLES, prefix, queue, ext)


def files(prefix, ext, queue="*"):
    return sorted(globmod.glob(data_glob(prefix, ext, queue)))


def shown(call, prefix=None):
    """The incantation as MATRIX.md prints it: the sample lake's own directory
    collapsed to `<prefix>`, so the line can be pasted at a bucket."""
    if prefix:
        call = call.replace("%s/%s" % (C.SAMPLES, prefix), "<prefix>")
    return call.replace(C.SAMPLES + "/", "")


# ---------------------------------------------------------------------------
# The check, over columnar output turned into rows
# ---------------------------------------------------------------------------


def judge(exp, recs, sorter):
    """`recs` is [{queue, partition, offset, payload_is_null}] in whatever order
    the reader produced. `sorter` sorts one queue's rows the way that LIBRARY
    sorts — the ordering under test is the library's, never Python's."""
    for queue in sorted(exp["queues"]):
        mine = [r for r in recs if r["queue"] == queue]
        rows = sorter(mine)
        nulls = sum(1 for r in mine if r["payload_is_null"])
        bad = C.check_queue(exp, queue, rows, nulls)
        if bad:
            return "%s (queue=%s)" % (bad, queue)
    return None


def ladder(reader, fmt, comp, prefix, candidates, sorter, exp):
    """Try each `(label, thunk)` in turn — a thunk returns the records already
    tagged with their queue — and the first that both loads and passes is the
    cell's verdict. Every failure before it becomes a note."""
    failures = []
    for label, thunk in candidates:
        try:
            recs = thunk()
        except Exception as e:  # noqa: BLE001
            failures.append((label, one_line(e)))
            continue
        try:
            bad = judge(exp, recs, sorter)
        except Exception as e:  # noqa: BLE001
            failures.append((label, one_line(e)))
            continue
        for flabel, ferr in failures:
            C.note(reader, "%s / %s: `%s` FAILS — %s" % (fmt, comp, shown(flabel, prefix), ferr))
        C.emit(reader, version(reader), fmt, comp, bad or "ok", shown(label, prefix), "")
        return
    detail = "; ".join("%s -> %s" % (shown(l, prefix), e) for l, e in failures)
    verdict = classify_all(failures)
    C.emit(reader, version(reader), fmt, comp, verdict, shown(candidates[0][0], prefix), detail)


# ---------------------------------------------------------------------------
# polars
# ---------------------------------------------------------------------------


def run_polars(exp):
    import polars as pl

    def from_frame(df, queue=None):
        """`queue` given = the frame is one queue's prefix; otherwise the frame
        has a `queue` column, discovered from the path."""
        queues = [queue] * df.height if queue else df["queue"].to_list()
        return [
            {"queue": q, "partition": p, "offset": o, "payload_is_null": n}
            for q, p, o, n in zip(
                queues,
                df["partition"].to_list(),
                df["offset"].to_list(),
                df["payload"].is_null().to_list(),
            )
        ]

    def sorter(rows):
        sub = pl.DataFrame(
            {
                "partition": [r["partition"] for r in rows],
                "offset": [r["offset"] for r in rows],
            }
        ).sort(["partition", "offset"])
        return list(zip(sub["partition"].to_list(), sub["offset"].to_list()))

    for prefix, meta in sorted(exp["prefixes"].items()):
        fmt, comp, ext = meta["format"], meta["compression"], meta["ext"]
        root = "%s/%s/" % (C.SAMPLES, prefix)
        if fmt == "parquet":
            # The prefix ROOT, not a glob: polars walks it and turns `queue=`,
            # `dt=` and `hour=` into columns. A glob of the data files reads the
            # same rows but discovers no keys, and the queue would be lost.
            cands = [
                (
                    "pl.read_parquet('%s')" % root,
                    lambda r=root: from_frame(pl.read_parquet(r)),
                )
            ]
        else:
            cands = [
                (
                    "pl.read_ndjson('%s')  # per queue" % data_glob(prefix, ext, "<queue>"),
                    lambda p=prefix, e=ext: [
                        rec
                        for q in sorted(exp["queues"])
                        for rec in from_frame(pl.read_ndjson(data_glob(p, e, q)), q)
                    ],
                )
            ]
        ladder("polars", fmt, comp, prefix, cands, sorter, exp)

    try:
        cols = pl.read_parquet("%s/parquet-zstd/" % C.SAMPLES).columns
        C.note(
            "polars",
            "read_parquet over the prefix ROOT gives %s — `queue`, `dt` and `hour` are "
            "the path keys, discovered; over a `queue=*/dt=*/hour=*/*.parquet` GLOB the "
            "same call returns the five file columns and no keys at all" % (cols,),
        )
    except Exception as e:  # noqa: BLE001
        C.note("polars", "read_parquet over the prefix root FAILS: %s" % one_line(e))

    try:
        cols = pl.read_ndjson(
            data_glob("jsonl-zstd", "jsonl.zst"), include_file_paths="src"
        ).columns
        C.note(
            "polars",
            "read_ndjson does no partition discovery; one glob over every queue plus "
            "include_file_paths='src' gives %s, and the queue is a regex away from `src`"
            % (cols,),
        )
    except Exception as e:  # noqa: BLE001
        C.note("polars", "include_file_paths probe FAILS: %s" % one_line(e))


# ---------------------------------------------------------------------------
# pyarrow
# ---------------------------------------------------------------------------


def run_pyarrow(exp):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as pads
    import pyarrow.json as paj
    import pyarrow.parquet as pq

    def from_table(table, queue=None):
        queues = [queue] * table.num_rows if queue else table["queue"].to_pylist()
        return [
            {"queue": q, "partition": p, "offset": o, "payload_is_null": n}
            for q, p, o, n in zip(
                queues,
                table["partition"].to_pylist(),
                table["offset"].to_pylist(),
                pc.is_null(table["payload"]).to_pylist(),
            )
        ]

    def sorter(rows):
        t = pa.table(
            {
                "partition": [r["partition"] for r in rows],
                "offset": [r["offset"] for r in rows],
            }
        )
        t = t.take(
            pc.sort_indices(
                t, sort_keys=[("partition", "ascending"), ("offset", "ascending")]
            )
        )
        return list(zip(t["partition"].to_pylist(), t["offset"].to_pylist()))

    def read_json_by_queue(prefix, ext, comp):
        """One queue's files at a time, concatenated: the queue is the prefix
        that was asked for, which is where the sink puts it."""
        out = []
        for queue in sorted(exp["queues"]):
            tables = []
            for f in files(prefix, ext, queue):
                if comp == "none":
                    tables.append(paj.read_json(f))
                else:
                    codec = "zstd" if comp == "zstd" else "gzip"
                    with pa.CompressedInputStream(pa.OSFile(f, "rb"), codec) as s:
                        tables.append(paj.read_json(s))
            out += from_table(pa.concat_tables(tables, promote_options="default"), queue)
        return out

    for prefix, meta in sorted(exp["prefixes"].items()):
        fmt, comp, ext = meta["format"], meta["compression"], meta["ext"]
        d = "%s/%s" % (C.SAMPLES, prefix)
        if fmt == "parquet":
            cands = [
                # What a notebook types first. It reads the keys as
                # dictionary<string> columns, which is why the sink must not
                # also put `queue` in the file: two types for one name do not
                # merge.
                ("pq.read_table('%s')" % d, lambda d=d: from_table(pq.read_table(d))),
                (
                    "pads.dataset('%s', partitioning='hive').to_table()" % d,
                    lambda d=d: from_table(
                        pads.dataset(d, partitioning="hive").to_table()
                    ),
                ),
            ]
        else:
            codec = "zstd" if comp == "zstd" else ("gzip" if comp == "gzip" else None)
            per_file = (
                "pa.json.read_json(f)"
                if codec is None
                else "pa.json.read_json(pa.CompressedInputStream(pa.OSFile(f), '%s'))" % codec
            )
            cands = [
                # The dataset reader does JSON too, and then the path keys come
                # back exactly as they do for Parquet — but its compression
                # detection stops at gzip.
                (
                    "pads.dataset('%s', format='json', partitioning='hive').to_table()" % d,
                    lambda d=d: from_table(
                        pads.dataset(d, format="json", partitioning="hive").to_table()
                    ),
                ),
                (
                    per_file + "  # per queue prefix, then pa.concat_tables",
                    lambda p=prefix, e=ext, c=comp: read_json_by_queue(p, e, c),
                ),
            ]
        ladder("pyarrow", fmt, comp, prefix, cands, sorter, exp)


# ---------------------------------------------------------------------------
# pandas
# ---------------------------------------------------------------------------


def run_pandas(exp):
    import pandas as pd

    def from_frame(df, queue=None):
        queues = [queue] * len(df) if queue else df["queue"].tolist()
        return [
            {"queue": q, "partition": p, "offset": int(o), "payload_is_null": bool(n)}
            for q, p, o, n in zip(
                queues,
                df["partition"].tolist(),
                df["offset"].tolist(),
                df["payload"].isna().tolist(),
            )
        ]

    def sorter(rows):
        sub = pd.DataFrame(rows).sort_values(["partition", "offset"], kind="stable")
        return list(zip(sub["partition"].tolist(), sub["offset"].tolist()))

    for prefix, meta in sorted(exp["prefixes"].items()):
        fmt, comp, ext = meta["format"], meta["compression"], meta["ext"]
        d = "%s/%s" % (C.SAMPLES, prefix)
        if fmt == "parquet":
            # pandas hands the directory to pyarrow's dataset reader, so it gets
            # the same discovered `queue`/`dt`/`hour` columns.
            cands = [
                ("pd.read_parquet('%s')" % d, lambda d=d: from_frame(pd.read_parquet(d)))
            ]
        else:
            cands = [
                (
                    "pd.concat(pd.read_json(f, lines=True) for f in glob('%s'))  # per queue"
                    % data_glob(prefix, ext, "<queue>"),
                    lambda p=prefix, e=ext: [
                        rec
                        for q in sorted(exp["queues"])
                        for rec in from_frame(
                            pd.concat(
                                [pd.read_json(f, lines=True) for f in files(p, e, q)],
                                ignore_index=True,
                            ),
                            q,
                        )
                    ],
                )
            ]
        ladder("pandas", fmt, comp, prefix, cands, sorter, exp)

    try:
        dtypes = {
            c: str(t) for c, t in pd.read_parquet("%s/parquet-zstd" % C.SAMPLES).dtypes.items()
        }
        C.note(
            "pandas",
            "read_parquet over the prefix gives the path keys as `category` columns: "
            "%s — a filter on `queue` is a filter on the directory, not on the file"
            % ({k: v for k, v in dtypes.items() if k in ("queue", "dt", "hour")},),
        )
    except Exception as e:  # noqa: BLE001
        C.note("pandas", "dtype probe FAILS: %s" % one_line(e))


# ---------------------------------------------------------------------------


def one_line(e):
    return ("%s: %s" % (type(e).__name__, e)).replace("\n", " ")[:300]


def classify_all(failures):
    """A missing optional codec is `unsupported` — the library never claimed to
    do it, and installing one package is the whole answer. Anything else is a
    `fail`: the reader is there and it did not read what the sink wrote."""
    text = " ".join(e for _, e in failures).lower()
    if "importerror" in text or "install" in text or "not installed" in text:
        return "unsupported:%s" % failures[-1][1][:150]
    return "fail:%s" % failures[-1][1][:200]


def main():
    exp = C.load_expected()
    run_polars(exp)
    run_pyarrow(exp)
    run_pandas(exp)


if __name__ == "__main__":
    main()
