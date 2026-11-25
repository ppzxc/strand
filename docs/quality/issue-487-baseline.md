# Issue 487 SCA baseline

- Target SHA: `2976312326ee99aae709ad1171f9372461761f39`
- Date: 2026-11-25
- Rule: `SEC-015`
- Inputs: resolved Gradle lockfiles and CycloneDX SBOMs from both Maven consumers.
- Command: `osv-scanner scan source --recursive --no-ignore --format json --verbosity error /src`
- Policy: `python3 .github/scripts/check-osv-policy.py <osv-report.json>`
- Result: blocked as required by policy.
- Critical finding: `GHSA-c4c3-7fpv-j4q5`, `io.netty:netty-handler:4.1.101.Final`; fixes listed as `4.1.137.Final` and `4.2.17.Final`.
- High findings also affect Netty 4.1.101.Final and Guava versions in the build graph.

The gate fails CRITICAL findings regardless of fixes and HIGH findings when a fix exists. This baseline is evidence that the gate blocks a known vulnerable dependency graph. Dependency upgrades remain outside ticket 487.
