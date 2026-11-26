#!/usr/bin/env python3
"""Compare consumer javac diagnostics with the reviewed exact warning inventory."""

import json
import re
import sys
from pathlib import Path

WARNING = re.compile(
    r"(?:\[WARNING\] )?(?P<path>[^\s:]+\.java)"
    r"(?::(?P<gradle_line>\d+):|:\[(?P<maven_line>\d+),\d+\])\s*"
    r"(?:warning: )?\[(?P<rule>[^]]+)\] (?P<message>.*)$"
)


def parse_warnings(output: str, root: Path) -> set[str]:
    warnings = set()
    for line in output.splitlines():
        is_diagnostic = "warning:" in line or ("[WARNING] " in line and ".java:[" in line)
        match = WARNING.search(line)
        if not match:
            if is_diagnostic:
                warnings.add(f"UNPARSED|{line.strip()}")
            continue
        path = Path(match.group("path"))
        try:
            relative = path.resolve().relative_to(root.resolve()).as_posix()
        except ValueError:
            continue
        line_number = match.group("gradle_line") or match.group("maven_line")
        rule = match.group("rule") or "<no-rule>"
        warnings.add(f"compileJava|main|11|{relative}:{line_number}|{rule}|{match.group('message').strip()}")
    return warnings


def matches_baseline(actual: set[str], expected: set[str]) -> bool:
    return actual == expected


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: check_warning_baseline.py <consumer> <output.log>", file=sys.stderr)
        return 2
    consumer, output_path = sys.argv[1:]
    root = Path(__file__).resolve().parents[2]
    baseline_path = Path(__file__).with_name("warning-baseline.json")
    baseline = json.loads(baseline_path.read_text())
    if consumer not in baseline["consumers"]:
        print(f"no warning baseline entry for {consumer}", file=sys.stderr)
        return 2
    actual = parse_warnings(Path(output_path).read_text(errors="replace"), root)
    expected = set(baseline["consumers"][consumer])
    if not matches_baseline(actual, expected):
        print(f"javac warning baseline mismatch for {consumer}", file=sys.stderr)
        for item in sorted(actual - expected):
            print(f"  unexpected: {item}", file=sys.stderr)
        for item in sorted(expected - actual):
            print(f"  missing: {item}", file=sys.stderr)
        return 1
    print(f"javac warning baseline matched: {consumer} ({len(actual)} diagnostics)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
