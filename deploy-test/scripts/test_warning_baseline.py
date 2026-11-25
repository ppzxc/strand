import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from check_warning_baseline import matches_baseline, parse_warnings


class WarningBaselineTest(unittest.TestCase):
    def test_parses_gradle_and_maven_modulepath_diagnostics(self):
        root = Path("/repo")
        gradle = (
            "/repo/deploy-test/shared/module/module-info.java:2: warning: "
            "[requires-automatic] requires directive for an automatic module"
        )
        maven = (
            "[WARNING] /repo/deploy-test/shared/module/module-info.java:[2,35] "
            "[requires-automatic] requires directive for an automatic module"
        )
        expected = {
            "compileJava|main|11|deploy-test/shared/module/module-info.java:2|"
            "requires-automatic|requires directive for an automatic module"
        }

        self.assertEqual(parse_warnings(gradle, root), expected)
        self.assertEqual(parse_warnings(maven, root), expected)

    def test_added_warning_does_not_match_baseline(self):
        self.assertFalse(matches_baseline({"known", "new"}, {"known"}))

    def test_warning_at_another_location_is_not_ignored(self):
        output = "/repo/deploy-test/shared/src/Unexpected.java:9: warning: [NewRule] new finding"
        self.assertEqual(
            parse_warnings(output, Path("/repo")),
            {"compileJava|main|11|deploy-test/shared/src/Unexpected.java:9|NewRule|new finding"},
        )

    def test_missing_warning_does_not_match_baseline(self):
        self.assertFalse(matches_baseline(set(), {"known"}))

    def test_clean_consumer_matches_empty_baseline(self):
        self.assertTrue(matches_baseline(set(), set()))


if __name__ == "__main__":
    unittest.main()
