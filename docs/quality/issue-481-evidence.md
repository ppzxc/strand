# Issue 481 evidence

Owner: ppzxc
Rules: ARC-001–003, STYLE-005, AGT-005; policy §9.4
Code SHA: `d61d58d`
Baseline inspection SHA: `c6d25ab`
Environment: JDK 25, Gradle 9.9.2, PMD 7.29.0, 2026-11-25 Asia/Seoul

| Command | Result | Evidence |
|---|---|---|
| `./gradlew :strand-core:spotlessApply` | PASS | formatted touched Java sources |
| `./gradlew :build-logic:test --tests '*PolicyException*Test' --console=plain` | PASS | PMD fixtures cover valid, missing, mismatched, duplicate, extra, and multiple suppression pairs; tests preserve duplicate same-line findings and priority 1 |
| `./gradlew checkPolicyExceptions --console=plain` | PASS | scans maintained Java sources and all project/build-logic production class directories; 32 pre-existing missing pairs match diagnostic baseline |
| Temporary expired `@PolicyException` type in `strand-nio/src/main` + `./gradlew checkPolicyExceptions --console=plain` | EXPECTED FAIL | production class scan failed on `ExpiredPolicyExceptionProbe` with `reviewBy=2000-01-01`; deleting the probe restored PASS |
| `./gradlew check --dry-run --console=plain` | PASS | root `check` schedules every Java `compileJava` task before `checkPolicyExceptions` |
| `./gradlew check --console=plain` | FAIL, unrelated | concurrent integration run exhausted observation capacity; `strand-netty:spotlessJavaCheck` also failed and is being fixed by ticket #483 |

The initial ArchUnit store contains zero violations at the baseline. `freeze.store.default.allowStoreCreation=false` makes missing or renamed baseline entries fail; changing the checked-in store requires reviewed source control changes. No existing architectural violations required freezing.

`pmd-suppression-baseline.txt` records 32 existing diagnostics as `module/source-set path:line:diagnostic`. Same-line findings have distinct rule diagnostics, and the task compares lists with multiplicity, so a new suppression or duplicate at an existing line is not masked by the baseline. The checker reads this file only; it cannot regenerate the baseline. Removing an old violation does not cancel a new finding.

The expiry rule imports every maintained project's compiled `main` classes plus build-logic `main` classes. It excludes test fixtures and generated/vendor sources.

Incomplete acceptance criteria:

- No source suppression currently needs an exception. `PolicyException` is public in `strand-api` so all Java modules can use the same CLASS-retained declaration.
- Independent approval and minimum exception scope are represented by `approvedBy`, declaration-only annotation targets, and per-rule pairing. GitHub designated-reviewer enforcement and reapproval history were not exercised locally; keep #481 open until protected-branch evidence confirms them.
- Severity downgrades and broad scope exclusions need reviewed build changes. Controlled tests guard the fixed PMD priority, source inventory, and ArchUnit rule inventory; GitHub approval enforcement was not exercised locally.

GitHub read-only branch protection lookup for `main` returned HTTP 404. The repository does not currently enforce protected-branch review, so independent approval, designated reviewer, and reapproval acceptance remain unmet. Repository settings were not changed.
