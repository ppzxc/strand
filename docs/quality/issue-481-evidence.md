# Issue 481 evidence

Owner: ppzxc
Rules: ARC-001–003, STYLE-005, AGT-005; policy §9.4
Code SHA: `8e3ccaf`
Baseline inspection SHA: `caba924`
Environment: JDK 25, Gradle 9.9.2, PMD 7.29.0, 2026-11-25 Asia/Seoul

| Command | Result | Evidence |
|---|---|---|
| `./gradlew :strand-core:spotlessApply` | PASS | formatted touched Java sources |
| `./gradlew :strand-core:test --tests '*PolicyExceptionExpiryTest' --tests '*CoreArchitectureTest' --console=plain` | PASS | expired date, next-day expiry, method and repeatable annotations; frozen core boundary rules |
| `./gradlew :strand-core:test --tests '*ArchUnitBaselinePolicyTest' --console=plain` | PASS | rejects store auto-creation and missing/renamed baseline rules |
| `./gradlew :build-logic:test --tests '*PolicyException*Test' --console=plain` | PASS | PMD fixtures cover valid, missing, mismatched, duplicate, extra, and multiple suppression pairs; rule priority is 1 |
| `./gradlew checkPolicyExceptions --console=plain` | PASS | scans maintained repository Java; 31 pre-existing missing pairs match frozen baseline |
| `./gradlew check --dry-run --console=plain` | PASS | root `check` schedules `checkPolicyExceptions` |

The initial ArchUnit store contains zero violations at the baseline. `freeze.store.default.allowStoreCreation=false` makes missing or renamed baseline entries fail; changing the checked-in store requires reviewed source control changes. No existing architectural violations required freezing.

`pmd-suppression-baseline.txt` records the 31 existing violations as `module/source-set path:line`. Every baseline diagnostic is an unpaired Java `@SuppressWarnings` declaration. The checker reads this file only; it cannot regenerate the baseline. Removing an old violation does not cancel a new finding because the task compares exact finding sets.

Incomplete acceptance criteria:

- No source suppression currently needs an exception. `PolicyException` is public in `strand-api` so all Java modules can use the same CLASS-retained declaration.
- Independent approval and minimum exception scope are represented by `approvedBy`, declaration-only annotation targets, and per-rule pairing. GitHub designated-reviewer enforcement and reapproval history were not exercised locally; keep #481 open until protected-branch evidence confirms them.
- Severity downgrades and broad scope exclusions need reviewed build changes. Controlled tests guard the fixed PMD priority, source inventory, and ArchUnit rule inventory; GitHub approval enforcement was not exercised locally.
