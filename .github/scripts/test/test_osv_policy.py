import json
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "check-osv-policy.py"


def finding(severity, fixed=None):
    events = ([{"introduced": "0"}] if fixed is None else [{"introduced": "0"}, {"fixed": fixed}])
    return {
        "id": f"TEST-{severity}",
        "database_specific": {"severity": severity},
        "affected": [{"ranges": [{"events": events}]}],
    }


class OsvPolicyTest(unittest.TestCase):
    def run_report(self, vulnerabilities, sources=None):
        paths = sources or [
            "strand-netty/gradle.lockfile",
            "build-logic/gradle.lockfile",
            "deploy-test/gradle-classpath/gradle.lockfile",
            "deploy-test/gradle-modulepath/gradle.lockfile",
            "deploy-test/maven-classpath/target/bom.json",
            "deploy-test/maven-modulepath/target/bom.json",
        ]
        results = [{"source": {"path": path}, "packages": [{"package": {"name": "sample", "version": "1"}, "vulnerabilities": vulnerabilities}]} for path in paths]
        report = {"results": results}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as file:
            json.dump(report, file)
            file.flush()
            return subprocess.run(["python3", str(SCRIPT), file.name], capture_output=True, text=True)

    def test_critical_fails_with_or_without_fix(self):
        for fix in (None, "2"):
            with self.subTest(fix=fix):
                result = self.run_report([finding("CRITICAL", fix)])
                self.assertEqual(result.returncode, 1)
                self.assertIn("TEST-CRITICAL", result.stdout)

    def test_high_fails_only_when_fixed_version_exists(self):
        self.assertEqual(self.run_report([finding("HIGH")]).returncode, 0)
        result = self.run_report([finding("HIGH", "2")])
        self.assertEqual(result.returncode, 1)
        self.assertIn("TEST-HIGH", result.stdout)

    def test_cvss_scores_map_to_the_agreed_severity_thresholds(self):
        vulnerability = {"id": "TEST-CVSS", "severity": [{"type": "CVSS_V3", "score": "9.8"}]}
        self.assertEqual(self.run_report([vulnerability]).returncode, 1)

    def test_lower_and_unclassified_are_reported_without_failure(self):
        result = self.run_report([finding("MEDIUM", "2"), {"id": "TEST-UNKNOWN"}])
        self.assertEqual(result.returncode, 0)
        self.assertIn("MEDIUM", result.stdout)
        self.assertIn("UNKNOWN", result.stdout)

    def test_osv_severity_names_map_to_policy_levels(self):
        self.assertEqual(self.run_report([finding("MODERATE", "2")]).returncode, 0)
        self.assertEqual(self.run_report([finding("IMPORTANT", "2")]).returncode, 1)

    def test_missing_or_invalid_input_fails(self):
        result = subprocess.run(["python3", str(SCRIPT), "/missing/report.json"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)

    def test_missing_scan_source_fails(self):
        result = self.run_report([], sources=["strand-netty/gradle.lockfile"])
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
