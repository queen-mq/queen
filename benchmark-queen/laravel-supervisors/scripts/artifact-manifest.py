#!/usr/bin/env python3
"""Create a sorted SHA-256 inventory for a completed benchmark campaign."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path


SCHEMA = "queen.laravel-supervisors.artifact-manifest/v1"
CHUNK_BYTES = 1024 * 1024


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(root: Path, output: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    if not root.is_dir():
        raise ValueError("campaign root must be a directory")

    output = output.resolve(strict=False)
    try:
        output.relative_to(root)
    except ValueError as exception:
        raise ValueError("manifest output must be inside the campaign root") from exception

    files: list[dict[str, object]] = []
    for path in sorted(root.rglob("*"), key=lambda candidate: candidate.as_posix()):
        if path == output or path.name.startswith(f".{output.name}."):
            continue
        if path.is_symlink():
            raise ValueError(f"campaign artifact must not be a symlink: {path.relative_to(root)}")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        stat = path.stat()
        files.append(
            {
                "path": relative,
                "bytes": stat.st_size,
                "sha256": sha256(path),
            }
        )

    return {
        "schema": SCHEMA,
        "algorithm": "sha256",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "root": root.name,
        "file_count": len(files),
        "files": files,
    }


def atomic_write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()

    atomic_write(arguments.output, build_manifest(arguments.root, arguments.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
