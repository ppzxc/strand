# Issue 487 SCA baseline

- Target SHA: `3a61f8a1f9d1dfdaa771e07502d0ec247b5066df`
- Date: 2026-11-25
- Rule: `SEC-015`
- Inputs: resolved Gradle lockfiles and CycloneDX SBOMs from both Maven consumers.
- Command: `osv-scanner scan source --recursive --no-ignore --all-packages --format json --verbosity error /src`
- Scanner: `ghcr.io/google/osv-scanner:v2.7.1@sha256:a70ffffc2bfd33482953e5d6b64c6fcdb30239717a07b0f0d3ab2859ce3e4cde`
- Policy: `python3 .github/scripts/check-osv-policy.py <osv-report.json>`
- Result: blocked as required by policy.
- Critical finding: `GHSA-c4c3-7fpv-j4q5`, `io.netty:netty-handler:4.1.101.Final`; fixes listed as `4.1.137.Final` and `4.2.17.Final`.
- High findings also affect Netty 4.1.101.Final and Guava versions in the build graph.
- Error-path probe: scanning a missing path returned exit `127` with a diagnostic; the CI gate fails on that status or any scanner stderr.

The gate fails CRITICAL findings regardless of fixes and HIGH findings when a fix exists. This baseline is evidence that the gate blocks a known vulnerable dependency graph. Dependency upgrades remain outside ticket 487.
