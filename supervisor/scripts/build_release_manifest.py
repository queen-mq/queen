#!/usr/bin/env python3
"""Build the canonical, detached-signature-friendly supervisor manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote


PLATFORMS = (
    ("linux", "amd64", "x86_64-unknown-linux-musl"),
    ("linux", "arm64", "aarch64-unknown-linux-musl"),
    ("darwin", "amd64", "x86_64-apple-darwin"),
    ("darwin", "arm64", "aarch64-apple-darwin"),
)
SEMVER = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(version: str, repository: str, dist: Path) -> dict[str, object]:
    if not SEMVER.fullmatch(version):
        raise ValueError("version must be a stable X.Y.Z semantic version")
    if not REPOSITORY.fullmatch(repository):
        raise ValueError("repository must have the form owner/name")

    tag = f"supervisor/v{version}"
    release_base = (
        f"https://github.com/{repository}/releases/download/{quote(tag, safe='')}"
    )
    artifacts: list[dict[str, str]] = []

    for operating_system, arch, target in PLATFORMS:
        filename = f"queen-supervisor-{version}-{operating_system}-{arch}.tar.gz"
        archive = dist / filename
        if not archive.is_file():
            raise FileNotFoundError(f"release archive not found: {archive}")
        artifacts.append(
            {
                "target": target,
                "os": operating_system,
                "arch": arch,
                "filename": filename,
                "url": f"{release_base}/{filename}",
                "sha256": sha256(archive),
            }
        )

    return {
        "schema_version": 1,
        "name": "queen-supervisor",
        "version": version,
        "release_tag": tag,
        "artifacts": artifacts,
    }


def canonical_json(document: dict[str, object]) -> bytes:
    # No build timestamp is intentional: identical archives and inputs must
    # produce byte-identical manifests that can be signed as detached blobs.
    return (
        json.dumps(
            document,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("ascii")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--dist", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = canonical_json(build_manifest(args.version, args.repository, args.dist))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(payload)
    except (OSError, ValueError) as error:
        print(f"build-release-manifest: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
