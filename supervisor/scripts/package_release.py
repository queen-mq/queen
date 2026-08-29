#!/usr/bin/env python3
"""Create a byte-reproducible Queen supervisor release archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import re
import sys
import tarfile
from pathlib import Path


ASSETS = (
    ("LICENSE.md", 0o644),
    ("queen-supervisor", 0o755),
    ("queen-supervisor.service.example", 0o644),
)
SEMVER = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package(
    staging: Path,
    output: Path,
    version: str,
    operating_system: str,
    arch: str,
    source_date_epoch: int,
) -> Path:
    if not SEMVER.fullmatch(version):
        raise ValueError("version must be a stable X.Y.Z semantic version")
    if arch not in {"amd64", "arm64"}:
        raise ValueError("arch must be amd64 or arm64")
    if operating_system not in {"linux", "darwin"}:
        raise ValueError("os must be linux or darwin")
    if source_date_epoch < 0:
        raise ValueError("SOURCE_DATE_EPOCH must not be negative")

    inputs: list[tuple[Path, str, int]] = []
    for name, mode in ASSETS:
        path = staging / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"missing regular release input: {path}")
        inputs.append((path, name, mode))

    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"queen-supervisor-{version}-{operating_system}-{arch}.tar.gz"
    with archive.open("wb") as raw:
        # An empty original filename and mtime=0 normalize the gzip header.
        with gzip.GzipFile(filename="", mode="wb", compresslevel=9, mtime=0, fileobj=raw) as zipped:
            with tarfile.open(fileobj=zipped, mode="w|", format=tarfile.GNU_FORMAT) as tar:
                for path, name, mode in inputs:
                    info = tarfile.TarInfo(name=name)
                    info.size = path.stat().st_size
                    info.mode = mode
                    info.mtime = source_date_epoch
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    with path.open("rb") as stream:
                        tar.addfile(info, stream)

    sidecar = archive.with_name(f"{archive.name}.sha256")
    sidecar.write_text(f"{file_sha256(archive)}  {archive.name}\n", encoding="ascii")
    return archive


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--os", required=True)
    parser.add_argument("--arch", required=True)
    parser.add_argument("--source-date-epoch", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        package(
            args.staging,
            args.output,
            args.version,
            args.os,
            args.arch,
            args.source_date_epoch,
        )
    except (OSError, ValueError) as error:
        print(f"package-release: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
