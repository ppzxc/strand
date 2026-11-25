# Issue #484 evidence

- Owner: build
- Rules: STYLE-001–003, STYLE-011
- Implementation SHA: `f8623b7`
- Baseline SHA: `f36cc7a`
- Commands: `./gradlew check`, `./gradlew deployTest`, `./gradlew -p build-logic spotlessApply`
- Result: root `check` passed 89 tasks; `deployTest` passed four Gradle/Maven consumers at version `0.2.0`.
- Failure evidence: a temporary invalid test class made `deployTest` fail at `checkJavaComments` with `ClassNamingConventions`; the consumer harness did not run.
- Focused evidence: PMD tests cover allowed license and API Javadoc, forbidden implementation/toggle comments, missing public Javadoc, internal-package exclusion, package-info Javadoc, regular/static wildcard imports, naming, and formatting.
- Baseline: 320 existing findings at `f36cc7a` (319 missing public API Javadocs, one non-allowlisted comment). The gate fails on findings outside this frozen path/line list.
- Limit: GitHub CI was not run locally. Existing Javadoc gaps stay in the frozen baseline until removed.
