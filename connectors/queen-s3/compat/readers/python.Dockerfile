# The container three readers share: DuckDB, Polars and pandas+pyarrow.
#
# Versions are deliberately NOT pinned. This lane's claim is "these readers, at
# these versions, on this day", and the version is a MEASUREMENT that every
# script prints and MATRIX.md records — a pin here would freeze the answer to
# whatever was current when the file was written and quietly stop testing the
# readers people actually install.
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb polars pyarrow pandas
WORKDIR /work
