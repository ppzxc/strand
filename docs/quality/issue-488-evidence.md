# Issue #488 evidence — generated-test reproducibility

- Owner: build and test owners
- Rules: `TST-001`, `TST-003`; boundary coverage also maps to `TST-004`
- Implementation commit: `68f216a80a94227d36ac113a4e7911e82a20e36f` (2026-11-25 19:19:24 Asia/Seoul)
- Integration commit: `e687797225ccf45b63486dd89b1577f1619786e0`
- Environment: JDK 25, Gradle wrapper

| Check | Command | Result |
|---|---|---|
| Fixed-seed generated sample and ArchUnit registration rule | `./gradlew :strand-core:test --tests '*Generated*Test' --console=plain` | PASS; generated failure is reproducible with `@Seed(12345L)` |
| Formatting | `./gradlew :strand-core:spotlessApply` | PASS |
| Existing boundary tests | API/core tests for null, empty, maximum length/limits, and integer boundaries | PASS; existing tests retained |

The test command timestamp was not captured. No random failure requiring promotion to an explicit example was found. The full root `check` was not run for this ticket; later `./gradlew clean check` passed on `08e0ebba71e8c2576dcac048eba2e62b872216c7` after the integrated changes.
