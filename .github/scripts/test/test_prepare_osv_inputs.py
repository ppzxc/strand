import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "prepare-osv-inputs.py"
REQUIRED_INPUTS = [
    "strand-api/gradle.lockfile",
    "strand-core/gradle.lockfile",
    "strand-nio/gradle.lockfile",
    "strand-netty/gradle.lockfile",
    "strand-conformance/gradle.lockfile",
    "strand-examples/gradle.lockfile",
    "strand-integration-test/gradle.lockfile",
    "strand-benchmark/gradle.lockfile",
    "strand-load/gradle.lockfile",
    "build-logic/gradle.lockfile",
    "gradle/verification-metadata.xml",
    "deploy-test/gradle-classpath/gradle.lockfile",
    "deploy-test/gradle-modulepath/gradle.lockfile",
    "deploy-test/gradle-classpath/gradle/verification-metadata.xml",
    "deploy-test/gradle-modulepath/gradle/verification-metadata.xml",
    "deploy-test/maven-classpath/target/bom.json",
    "deploy-test/maven-modulepath/target/bom.json",
]


class PrepareOsvInputsTest(unittest.TestCase):
    def test_stages_required_inputs_without_generated_pom_caches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "workspace"
            target = root / "osv-inputs"
            for index, relative in enumerate(REQUIRED_INPUTS):
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"input-{index}")
            generated_pom = source / "build/tmp/test/caches/unrelated.pom"
            generated_pom.parent.mkdir(parents=True)
            generated_pom.write_text("generated cache")

            result = subprocess.run(
                ["python3", str(SCRIPT), str(source), str(target)],
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            staged = {
                path.relative_to(target).as_posix()
                for path in target.rglob("*")
                if path.is_file()
            }
            self.assertEqual(staged, set(REQUIRED_INPUTS))
            self.assertFalse((target / "build/tmp/test/caches/unrelated.pom").exists())

    def test_missing_required_input_fails_before_staging_any_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "workspace"
            target = root / "osv-inputs"
            missing = REQUIRED_INPUTS[-1]
            for index, relative in enumerate(REQUIRED_INPUTS[:-1]):
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"input-{index}")

            result = subprocess.run(
                ["python3", str(SCRIPT), str(source), str(target)],
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn(missing, result.stderr)
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
