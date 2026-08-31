from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "container-isolation.py"
SPEC = importlib.util.spec_from_file_location("container_isolation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
isolation = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = isolation
SPEC.loader.exec_module(isolation)


def container(identifier: str, name: str, project: str | None = None) -> dict:
    labels = (
        {}
        if project is None
        else {
            "com.docker.compose.project": project,
            "com.docker.compose.service": name.split("-", 1)[0],
        }
    )
    return {
        "Id": identifier,
        "Name": f"/{name}",
        "Image": f"sha256:{identifier}",
        "Created": "2026-08-29T00:00:00Z",
        "Config": {"Image": "fixture:latest", "Labels": labels},
        "State": {
            "Running": True,
            "Status": "running",
            "StartedAt": "2026-08-29T00:00:01Z",
        },
    }


def start_event(identifier: str, name: str, project: str | None = None) -> dict:
    attributes = {"name": name, "image": "fixture:latest"}
    if project is not None:
        attributes.update(
            {
                "com.docker.compose.project": project,
                "com.docker.compose.service": name.split("-", 1)[0],
            }
        )
    return {
        "Type": "container",
        "Action": "start",
        "status": "start",
        "id": identifier,
        "from": "fixture:latest",
        "Actor": {"ID": identifier, "Attributes": attributes},
        "time": 1_777_000_000,
        "timeNano": 1_777_000_000_000_000_000,
    }


class ContainerIsolationTest(unittest.TestCase):
    def test_empty_daemon_is_isolated(self) -> None:
        result = isolation.evaluate_inventory(
            [], "bench-project", "monitor", "lane-start", False
        )
        self.assertTrue(result["gate_passed"])
        self.assertEqual("isolated", result["qualification"])

    def test_project_and_monitor_are_owned(self) -> None:
        result = isolation.evaluate_inventory(
            [
                container("a" * 64, "app", "bench-project"),
                container("b" * 64, "monitor"),
            ],
            "bench-project",
            "monitor",
            "pre-dispatch",
            False,
            allowed_services=("app",),
        )
        self.assertTrue(result["gate_passed"])
        self.assertEqual(2, result["owned_count"])
        self.assertEqual(0, result["foreign_count"])

    def test_unexpected_compose_tool_is_foreign(self) -> None:
        result = isolation.evaluate_inventory(
            [container("e" * 64, "producer-1", "bench-project")],
            "bench-project",
            "monitor",
            "pre-dispatch",
            False,
            allowed_services=("app", "redis"),
        )
        self.assertFalse(result["gate_passed"])
        self.assertEqual("producer-1", result["foreign"][0]["name"])

    def test_foreign_container_fails_closed(self) -> None:
        result = isolation.evaluate_inventory(
            [container("c" * 64, "database", "another-project")],
            "bench-project",
            "monitor",
            "lane-start",
            False,
        )
        self.assertFalse(result["gate_passed"])
        self.assertFalse(result["isolated"])
        self.assertEqual("database", result["foreign"][0]["name"])

    def test_override_is_explicitly_diagnostic(self) -> None:
        result = isolation.evaluate_inventory(
            [container("d" * 64, "database")],
            "bench-project",
            "monitor",
            "pre-dispatch",
            True,
        )
        self.assertTrue(result["gate_passed"])
        self.assertFalse(result["isolated"])
        self.assertEqual(
            "diagnostic_foreign_container_override", result["qualification"]
        )

    def test_exact_id_mode_rejects_same_project_one_off_or_replacement(self) -> None:
        allowed_id = "a" * 64
        unexpected_id = "b" * 64
        result = isolation.evaluate_start_events(
            [
                start_event(allowed_id, "producer", "bench-project"),
                start_event(unexpected_id, "producer-run-123", "bench-project"),
            ],
            "bench-project",
            "monitor",
            allowed_services=("producer",),
            allowed_container_ids=(allowed_id,),
        )

        self.assertEqual(1, result["owned_count"])
        self.assertEqual(1, result["foreign_count"])
        self.assertEqual(unexpected_id, result["foreign"][0]["id"])

    def test_start_event_catches_a_transient_foreign_container(self) -> None:
        # The container need not remain in the final inventory: its daemon
        # start event is sufficient evidence that it overlapped the window.
        result = isolation.evaluate_start_events(
            [start_event("c" * 64, "transient-build")],
            "bench-project",
            "monitor",
            allowed_container_ids=("a" * 64,),
        )

        self.assertEqual(1, result["record_count"])
        self.assertEqual(1, result["foreign_count"])

    def test_allowed_container_start_is_a_restart_not_an_overridable_event(self) -> None:
        allowed_id = "a" * 64
        starts = isolation.evaluate_start_events(
            [start_event(allowed_id, "app", "bench-project")],
            "bench-project",
            "monitor",
            allowed_container_ids=(allowed_id,),
        )
        decision = isolation.evaluate_watch_decision(
            {"foreign_count": 0},
            starts,
            {"foreign_count": 0},
            True,
            [],
        )

        self.assertTrue(decision["restart_detected"])
        self.assertFalse(decision["gate_passed"])
        self.assertEqual(
            "allowed_container_restart_detected", decision["qualification"]
        )

    def test_continuous_watch_fails_on_transient_start_and_stops_cleanly(self) -> None:
        allowed_id = "a" * 64
        foreign_id = "f" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            docker = fake_bin / "docker"
            docker.write_text(
                """#!/usr/bin/env python3
import json
import sys
import time

allowed = "a" * 64
foreign = "f" * 64
arguments = sys.argv[1:]
if arguments[:2] == ["container", "ls"]:
    print(allowed)
elif arguments[:2] == ["container", "inspect"]:
    print(json.dumps([{
        "Id": allowed,
        "Name": "/app",
        "Image": "sha256:" + allowed,
        "Created": "2026-08-29T00:00:00Z",
        "Config": {"Image": "fixture:latest", "Labels": {}},
        "State": {
            "Running": True,
            "Status": "running",
            "StartedAt": "2026-08-29T00:00:01Z",
        },
    }]))
elif arguments and arguments[0] == "events":
    print(json.dumps({
        "Type": "container",
        "Action": "start",
        "status": "start",
        "id": foreign,
        "from": "foreign:latest",
        "Actor": {"ID": foreign, "Attributes": {"name": "transient"}},
        "time": 1777000000,
        "timeNano": 1777000000000000000,
    }), flush=True)
    while True:
        time.sleep(0.05)
else:
    raise SystemExit(9)
""",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            output = root / "watch.json"
            ready = root / "ready.json"
            stop = root / "stop"
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--phase",
                    "measurement",
                    "--project",
                    "bench-project",
                    "--allowed-container",
                    allowed_id,
                    "--output",
                    str(output),
                    "--watch-until",
                    str(stop),
                    "--ready-file",
                    str(ready),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if ready.exists() and output.exists():
                    current = json.loads(output.read_text(encoding="utf-8"))
                    if current["start_events"]["foreign_count"] == 1:
                        break
                time.sleep(0.02)
            else:
                process.kill()
                process.communicate(timeout=5)
                self.fail("continuous watch did not record the start event")

            stop.write_text("stop\n", encoding="utf-8")
            _, stderr = process.communicate(timeout=5)
            result = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(2, process.returncode, stderr)
        self.assertFalse(result["active"])
        self.assertTrue(result["foreign_detected"])
        self.assertFalse(result["gate_passed"])
        self.assertEqual(foreign_id, result["start_events"]["foreign"][0]["id"])


if __name__ == "__main__":
    unittest.main()
