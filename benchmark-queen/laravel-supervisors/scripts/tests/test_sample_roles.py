from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[1]


def load_script(name: str):
    path = SCRIPTS / name
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sample = load_script("sample.py")


class SampleRoleTest(unittest.TestCase):
    def test_lease_renewal_php_helper_has_a_dedicated_role(self) -> None:
        command = (
            "/usr/local/bin/php -d display_errors=stderr -r "
            "require $argv[1]; \\Queen\\Laravel\\Queue\\LeaseRenewalWorker::main(); "
            "/workspace/vendor/autoload.php"
        )

        self.assertEqual("lease-renewer", sample.classify_process(command, "app"))

    def test_worker_marker_wins_over_incidental_helper_text(self) -> None:
        command = "php artisan queue:work queen --name=lease-renewer"

        self.assertEqual("worker", sample.classify_process(command, "app"))

    def test_docker_init_does_not_inherit_its_child_role(self) -> None:
        command = "/sbin/docker-init -- php artisan queue:work queen"

        self.assertEqual("app", sample.classify_process(command, "app"))


if __name__ == "__main__":
    unittest.main()
