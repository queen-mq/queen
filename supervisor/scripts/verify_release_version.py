#!/usr/bin/env python3
"""Fail closed when the supervisor release versions drift apart."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path


TAG = re.compile(r"^supervisor/v((?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))$")
PHP_VERSION = re.compile(
    r"\b(?:public\s+)?const\s+VERSION\s*=\s*['\"]([^'\"]+)['\"]\s*;"
)


def package_version(document: dict[str, object], package: str) -> str:
    packages = document.get("package")
    if not isinstance(packages, list):
        raise ValueError("Cargo.lock has no package list")
    matches = [
        item.get("version")
        for item in packages
        if isinstance(item, dict) and item.get("name") == package
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        raise ValueError(f"Cargo.lock must contain exactly one {package} package")
    return matches[0]


def client_version(path: Path) -> str:
    match = PHP_VERSION.search(path.read_text(encoding="utf-8"))
    if match is None:
        raise ValueError(f"cannot find SupervisorBinary::VERSION in {path}")
    return match.group(1)


def verify(tag: str, cargo_toml: Path, cargo_lock: Path, client_php: Path) -> str:
    match = TAG.fullmatch(tag)
    if match is None:
        raise ValueError("tag must match supervisor/vX.Y.Z (stable SemVer only)")
    version = match.group(1)

    manifest = tomllib.loads(cargo_toml.read_text(encoding="utf-8"))
    crate_version = manifest.get("package", {}).get("version")
    lock_version = package_version(
        tomllib.loads(cargo_lock.read_text(encoding="utf-8")), "queen-supervisor"
    )
    expected = {
        "release tag": version,
        "supervisor/Cargo.toml": crate_version,
        "supervisor/Cargo.lock": lock_version,
        "Laravel SupervisorBinary::VERSION": client_version(client_php),
    }
    if any(value != version for value in expected.values()):
        rendered = ", ".join(f"{label}={value!r}" for label, value in expected.items())
        raise ValueError(f"supervisor release version mismatch: {rendered}")
    return version


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--cargo-toml", type=Path, default=Path("supervisor/Cargo.toml"))
    parser.add_argument("--cargo-lock", type=Path, default=Path("supervisor/Cargo.lock"))
    parser.add_argument(
        "--client-php",
        type=Path,
        default=Path(
            "clients/client-laravel/src/Laravel/Supervisor/Binary/SupervisorBinary.php"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        version = verify(args.tag, args.cargo_toml, args.cargo_lock, args.client_php)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"verify-release-version: {error}", file=sys.stderr)
        return 1
    print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
