import json
import subprocess
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).parents[3]
GATE = ROOT / ".github/scripts/check-ci-gate.py"


class CiGateTest(unittest.TestCase):
    def run_gate(self, needs):
        return subprocess.run(
            ["python3", str(GATE)],
            input=needs if isinstance(needs, str) else json.dumps(needs),
            text=True,
            capture_output=True,
            check=False,
        )

    def test_only_successful_verify_and_runtime_jobs_pass(self):
        result = self.run_gate({"verify": {"result": "success"}, "runtime": {"result": "success"}})

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_failure_cancelled_skipped_and_missing_results_fail_closed(self):
        for status in ("failure", "cancelled", "skipped", None):
            with self.subTest(status=status):
                verify = {} if status is None else {"result": status}
                result = self.run_gate({"verify": verify, "runtime": {"result": "success"}})

                self.assertNotEqual(result.returncode, 0)

    def test_missing_or_unexpected_required_jobs_fail_closed(self):
        cases = (
            {"verify": {"result": "success"}},
            {"runtime": {"result": "success"}},
            {"verify": {"result": "success"}, "runtime": {"result": "success"}, "sca": {"result": "success"}},
        )
        for needs in cases:
            with self.subTest(needs=needs):
                self.assertNotEqual(self.run_gate(needs).returncode, 0)

    def test_malformed_result_json_fails_closed(self):
        self.assertNotEqual(self.run_gate("{").returncode, 0)

    def test_ci_and_release_workflows_gate_merge_upload_and_publication(self):
        ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
        release = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())

        self.assertEqual(set(ci["jobs"]["ci-ok"]["needs"]), {"verify", "runtime"})
        self.assertEqual(ci["jobs"]["ci-ok"]["if"], "always()")
        self.assertIn("actions/checkout@", str(ci["jobs"]["ci-ok"]["steps"]))
        self.assertIn("./gradlew check", str(ci["jobs"]["verify"]["steps"]))
        self.assertIn("./gradlew deployTest", str(ci["jobs"]["verify"]["steps"]))
        self.assertEqual(ci["jobs"]["runtime"]["strategy"]["matrix"]["java"], [8, 11, 17, 21, 25])
        verify_steps = str(ci["jobs"]["verify"]["steps"])
        for required in ("Scan secrets", "Scan resolved dependencies"):
            self.assertIn(required, verify_steps)
        self.assertIn("runtimeMatrixTest", str(ci["jobs"]["runtime"]["steps"]))
        self.assertEqual(release["jobs"]["gate"]["needs"], "preflight")
        self.assertEqual(release["jobs"]["gate"]["uses"], "./.github/workflows/ci.yml")
        self.assertEqual(set(release["jobs"]["release"]["needs"]), {"preflight", "gate"})
        self.assertNotIn("if", release["jobs"]["release"])
        steps = [step.get("name") for step in release["jobs"]["release"]["steps"]]
        self.assertLess(steps.index("Upload to Central Portal"), steps.index("Wait for VALIDATED and run deploy test harness (b)"))
        self.assertLess(steps.index("Wait for VALIDATED and run deploy test harness (b)"), steps.index("Create draft GitHub Release"))
        preflight_steps = str(release["jobs"]["preflight"]["steps"])
        self.assertIn("check-release-tag.sh", preflight_steps)
        self.assertIn("check-release-secrets.sh", preflight_steps)


if __name__ == "__main__":
    unittest.main()
