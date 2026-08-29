from __future__ import annotations

import hashlib
import importlib.util
import json
import tarfile
import tempfile
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1]


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


manifest_script = load("build_release_manifest")
package_script = load("package_release")
version_script = load("verify_release_version")


class ReleaseManifestTest(unittest.TestCase):
    def test_manifest_is_canonical_complete_and_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dist = Path(temporary)
            for operating_system, arch, _target in manifest_script.PLATFORMS:
                (dist / f"queen-supervisor-1.2.3-{operating_system}-{arch}.tar.gz").write_bytes(
                    f"{operating_system}-{arch}".encode()
                )

            first = manifest_script.canonical_json(
                manifest_script.build_manifest("1.2.3", "queen-mq/queen", dist)
            )
            second = manifest_script.canonical_json(
                manifest_script.build_manifest("1.2.3", "queen-mq/queen", dist)
            )
            self.assertEqual(first, second)
            self.assertTrue(first.endswith(b"\n"))

            document = json.loads(first)
            self.assertEqual(1, document["schema_version"])
            self.assertEqual("supervisor/v1.2.3", document["release_tag"])
            self.assertEqual(
                [
                    ("linux", "amd64"),
                    ("linux", "arm64"),
                    ("darwin", "amd64"),
                    ("darwin", "arm64"),
                ],
                [(a["os"], a["arch"]) for a in document["artifacts"]],
            )
            self.assertIn("supervisor%2Fv1.2.3", document["artifacts"][0]["url"])
            self.assertEqual(
                hashlib.sha256(b"linux-amd64").hexdigest(),
                document["artifacts"][0]["sha256"],
            )

    def test_manifest_rejects_missing_architecture(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dist = Path(temporary)
            (dist / "queen-supervisor-1.2.3-linux-amd64.tar.gz").touch()
            with self.assertRaises(FileNotFoundError):
                manifest_script.build_manifest("1.2.3", "queen-mq/queen", dist)


class ReleaseVersionTest(unittest.TestCase):
    def write_metadata(self, root: Path, versions: tuple[str, str, str]) -> tuple[Path, Path, Path]:
        cargo, lock, client = root / "Cargo.toml", root / "Cargo.lock", root / "SupervisorBinary.php"
        cargo.write_text(f'[package]\nname="queen-supervisor"\nversion="{versions[0]}"\n')
        lock.write_text(
            f'[[package]]\nname="queen-supervisor"\nversion="{versions[1]}"\n'
        )
        client.write_text(
            "<?php final class SupervisorBinary { "
            f"public const VERSION = '{versions[2]}'; }}"
        )
        return cargo, lock, client

    def test_versions_must_all_match(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.write_metadata(Path(temporary), ("1.2.3", "1.2.3", "1.2.3"))
            self.assertEqual("1.2.3", version_script.verify("supervisor/v1.2.3", *paths))

    def test_client_version_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.write_metadata(Path(temporary), ("1.2.3", "1.2.3", "1.2.4"))
            with self.assertRaisesRegex(ValueError, "version mismatch"):
                version_script.verify("supervisor/v1.2.3", *paths)


class ReleaseArchiveTest(unittest.TestCase):
    def test_archive_metadata_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging"
            staging.mkdir()
            binary = staging / "queen-supervisor"
            binary.write_bytes(b"binary")
            binary.chmod(0o755)
            (staging / "queen-supervisor.service.example").write_text("unit\n")
            (staging / "LICENSE.md").write_text("license\n")

            archives = []
            for name in ("one", "two"):
                output = root / name
                archives.append(
                    package_script.package(
                        staging,
                        output,
                        "1.2.3",
                        "linux",
                        "amd64",
                        1700000000,
                    )
                )

            self.assertEqual(archives[0].read_bytes(), archives[1].read_bytes())
            with tarfile.open(archives[0]) as archive:
                members = archive.getmembers()
                self.assertEqual(
                    ["LICENSE.md", "queen-supervisor", "queen-supervisor.service.example"],
                    [member.name for member in members],
                )
                self.assertTrue(all(member.uid == 0 and member.gid == 0 for member in members))
                self.assertTrue(all(member.mtime == 1700000000 for member in members))


if __name__ == "__main__":
    unittest.main()
