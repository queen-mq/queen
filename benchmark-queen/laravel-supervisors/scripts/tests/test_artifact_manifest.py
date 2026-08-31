from __future__ import annotations

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "artifact-manifest.py"
SPEC = importlib.util.spec_from_file_location("artifact_manifest", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load {SCRIPT}")
artifact_manifest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact_manifest)


class ArtifactManifestTest(unittest.TestCase):
    def test_manifest_is_sorted_and_excludes_itself(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "z.txt").write_text("z\n", encoding="utf-8")
            (root / "nested").mkdir()
            (root / "nested" / "a.json").write_text("{}\n", encoding="utf-8")
            output = root / "artifact-manifest.json"

            manifest = artifact_manifest.build_manifest(root, output)

            self.assertEqual(2, manifest["file_count"])
            self.assertEqual(
                ["nested/a.json", "z.txt"],
                [entry["path"] for entry in manifest["files"]],
            )
            self.assertEqual(
                hashlib.sha256(b"z\n").hexdigest(),
                manifest["files"][1]["sha256"],
            )

    def test_manifest_rejects_symlinked_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.write_text("data", encoding="utf-8")
            (root / "link").symlink_to(target)

            with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                artifact_manifest.build_manifest(root, root / "manifest.json")

    def test_output_must_stay_inside_campaign(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "campaign"
            root.mkdir()

            with self.assertRaisesRegex(ValueError, "inside the campaign root"):
                artifact_manifest.build_manifest(root, Path(directory) / "manifest.json")


if __name__ == "__main__":
    unittest.main()
