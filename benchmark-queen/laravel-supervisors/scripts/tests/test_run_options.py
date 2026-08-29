from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "run.sh"


class RunOptionsTest(unittest.TestCase):
    def test_publishable_rejects_no_build_before_host_inspection(self) -> None:
        environment = {
            "PATH": "/usr/bin:/bin",
        }
        result = subprocess.run(
            [
                "/bin/bash",
                str(SCRIPT),
                "--qualification",
                "publishable",
                "--no-build",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "--qualification publishable cannot be combined with --no-build",
            result.stderr,
        )
        self.assertNotIn("Docker daemon", result.stderr)


if __name__ == "__main__":
    unittest.main()
