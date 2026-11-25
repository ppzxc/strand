# Issue 481 evidence

Owner: ppzxc
Rules: ARC-001–003, STYLE-005, AGT-005; policy §9.4
Code SHA: `95a4bd7`
Environment: JDK 25, Gradle 9.9.2, 2026-11-25 Asia/Seoul

| Command | Result | Evidence |
|---|---|---|
| `./gradlew :strand-core:spotlessApply` | PASS | formatted touched Java sources |
| `./gradlew :strand-core:test --tests '*PolicyExceptionExpiryTest' --tests '*CoreArchitectureTest' --console=plain` | PASS | expired date, next-day expiry, method and repeatable annotations; frozen core boundary rules |
| `./gradlew :strand-core:test --tests '*ArchUnitBaselinePolicyTest' --console=plain` | PASS | rejects store auto-creation and missing/renamed baseline rules |

The initial ArchUnit store contains zero violations at the baseline. `freeze.store.default.allowStoreCreation=false` makes missing or renamed baseline entries fail; changing the checked-in store requires reviewed source control changes. No existing architectural violations required freezing.

Incomplete acceptance criteria:

- PMD does not yet enforce the `@SuppressWarnings` and same-declaration `@PolicyException` pair. Ticket #484 owns PMD setup and custom rules; this branch adds the annotation contract and expiry checker only.
- No source suppression currently needs an exception. `PolicyException` is public in `strand-api` so all Java modules can use the same CLASS-retained declaration.
- Severity downgrades and scope exclusions remain PR review responsibilities; this repository has no local severity or exception configuration yet. GitHub approval enforcement was not exercised locally.
