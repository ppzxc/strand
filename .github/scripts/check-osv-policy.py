#!/usr/bin/env python3
"""Apply strand's OSV severity and fix-availability gate."""

import json
import sys
from pathlib import Path


def fixed_versions(vulnerability):
    versions = set()
    for affected in vulnerability.get("affected", []):
        for version_range in affected.get("ranges", []):
            versions.update(event["fixed"] for event in version_range.get("events", []) if "fixed" in event)
    return sorted(versions)


def severity(vulnerability):
    sources = [vulnerability.get("database_specific", {}), vulnerability.get("ecosystem_specific", {})]
    for source in sources:
        value = source.get("severity")
        if isinstance(value, str):
            value = value.upper()
            if value in {"CRITICAL", "HIGH", "MEDIUM", "LOW"}:
                return value
            if value == "IMPORTANT":
                return "HIGH"
            if value == "MODERATE":
                return "MEDIUM"
        if isinstance(value, (int, float)):
            return severity_from_score(value)
    for item in vulnerability.get("severity", []):
        try:
            return severity_from_score(float(item["score"]))
        except (KeyError, TypeError, ValueError):
            continue
    return "UNKNOWN"


def severity_from_score(score):
    if score >= 9:
        return "CRITICAL"
    if score >= 7:
        return "HIGH"
    if score >= 4:
        return "MEDIUM"
    return "LOW"


def main(path):
    try:
        report = json.loads(Path(path).read_text(encoding="utf-8"))
        results = report["results"]
        if not isinstance(results, list) or not results:
            raise ValueError("OSV report has no scan results")
        sources = {result.get("source", {}).get("path", "") for result in results}
        required_sources = {
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
        }
        missing = sorted(source for source in required_sources if not any(source in path for path in sources))
        if missing:
            raise ValueError(f"OSV report is missing required inputs: {', '.join(missing)}")
        if not any(package for result in results for package in result.get("packages", [])):
            raise ValueError("OSV report contains no scanned packages")
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        print(f"Invalid or missing OSV report: {error}", file=sys.stderr)
        return 2

    blocked = False
    scanned = 0
    for result in results:
        source = result.get("source", {}).get("path", "unknown source")
        for package in result.get("packages", []):
            for vulnerability in package.get("vulnerabilities", []):
                scanned += 1
                level = severity(vulnerability)
                fixes = fixed_versions(vulnerability)
                identifier = vulnerability.get("id", "unknown id")
                details = package.get("package", {})
                print(f"{level}\t{identifier}\t{details.get('name', 'unknown package')}@{details.get('version', 'unknown version')}\t{','.join(fixes) or 'no fix'}\t{source}")
                blocked |= level == "CRITICAL" or (level == "HIGH" and bool(fixes))

    if scanned == 0:
        print("No vulnerabilities found")
    return 1 if blocked else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {Path(sys.argv[0]).name} <osv-json-report>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
