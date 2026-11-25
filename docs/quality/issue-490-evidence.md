# Issue 490 evidence — PR and release gates

## Scope and owner

- Target code SHA: `b04718d`
- Owner: build and release
- Rules: TOOL-001, TOOL-006, SEC-003, SEC-015, VER-001, VER-013, VER-015
- Gate: Fast on every PR; existing runtime matrix on JDK 8, 11, 17, 21, and 25; reusable Fast gate on release tags

## Acceptance evidence

1. `.github/workflows/pr.yml` calls the reusable CI workflow for every PR to `main`. Its `verify` job runs `check`, secret scanning, the independent Gradle/Maven consumer harness, and SCA; the `runtime` job runs `runtimeMatrixTest` for JDK 8/11/17/21/25. The active `main` ruleset (ID `26113647`) requires `ci / ci-ok`.
2. `ci-ok` runs with `if: always()` and needs both `verify` and `runtime`. `.github/scripts/check-ci-gate.py` requires exactly those job results and fails unless each result is `success`. The `test_ci_gate.py` fixtures cover success, failure, cancellation, skipped, missing result, missing/unexpected job, and malformed JSON. The release job needs both successful preflight and the reusable CI gate, so failure, cancellation, or absence prevents upload.
3. Release preflight checks that the tag is a stable `X.Y.Z` matching `gradle.properties` and that required secrets are present. Release compilation uses JDK 25; managed deployment modules compile with `--release 8`, and the runtime matrix checks supported JVMs. After upload, the workflow waits for that deployment to become `VALIDATED`, runs `deploy-test/run.sh` against its deployment-scoped download URL, then creates a draft GitHub Release. It does not publish the Portal deployment or make the GitHub Release public.
4. Existing Integration/Deep evidence comes from #489's conformance vectors, Netty WebSocket tests, and resource-release integration tests. The current #487 SCA baseline contains CRITICAL Netty and HIGH findings; the required policy gate correctly fails until maintainers remediate them. No dependency upgrade is included in this ticket.

## Reproduction

- `python3 .github/scripts/test/test_ci_gate.py` — PASS, five tests
- `python3 .github/scripts/test/test_osv_policy.py` — PASS, seven tests
- `python3 -c 'import yaml; yaml.safe_load(open(".github/workflows/ci.yml")); yaml.safe_load(open(".github/workflows/release.yml"))'` — PASS
- `.github/scripts/check-release-tag.sh 0.2.0` — PASS
- `.github/scripts/check-release-tag.sh 999.999.999` — FAIL as expected
- `git diff --check` — PASS
- The Bats release tests are wired into CI. Local Bats was unavailable, so they were not run here.
- No upload or publication was performed. Workflow dependency checks and VALIDATED ordering were tested from YAML; release execution requires live Central credentials and a tag.

## Limit

The SCA gate is intentionally red on the current dependency baseline. A passing `ci / ci-ok` result requires remediation of the recorded findings.
