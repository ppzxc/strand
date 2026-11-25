# Issue #491 rule audit and residual gaps

Owner: build and module owners
Source: `docs/specification/java-engineering-quality.md` 1.3.3, 152 rule rows
Recorded: 2026-11-25, Asia/Seoul

## Scope and verification map

| Scope | Applied checks and evidence | Owner |
|---|---|---|
| Deployed modules: `strand-api`, `strand-core`, `strand-nio`, `strand-netty`; main/test/test fixtures | Java conventions, required Error Prone checks, compiler warning collection, `NullAway` on deployed `main`, style/Javadoc and suppression scans; issues #480–485 evidence | Build and module owners |
| Internal Java modules: examples, conformance, integration tests, load harness, benchmark | Shared Java conventions and Spotless; root `check`; conformance and integration tests; runtime matrix excludes examples and build-logic by design | Build and module owners |
| Included `build-logic` | Spotless, PMD rule tests, TestKit failure propagation, dependency and compatibility tests; root `check` includes its tests and formatting | Build owner |
| Independent consumers | Gradle and Maven, classpath and module path; Error Prone, warning baseline, dependency allowlist, and `deployTest` | Build owner |
| Protocol and transport paths | Conformance vectors, Netty WebSocket, NIO/Netty rendered logs, and resource release; issue #489 and #485 evidence | Core and transport owners |
| CI and release | PR reusable CI, weekly scheduled CI, JDK 8/11/17/21/25 runtime matrix, secret and SCA scans, release preflight, publish gate, and post-publish consumer harness; issue #490 evidence | Build and release owners |

The profile decisions are unchanged: DDD/hexagonal, Spring, and MyBatis profiles are inactive because this repository has no matching adopted architecture or framework stack. No implementation framework was added to activate them. The deployed Java 8 boundary and JDK 25 build toolchain remain in force.

## Rule-family coverage

| Rule IDs | Rule family | Evidence pointer | Audit result |
|---|---|---|---|
| `ARC-005`, `TOOL-001` | Architecture, resolved dependency boundaries | `issue-480-evidence.md` | Automated checks and controlled forbidden-dependency samples exist. |
| `ARC-001–003`, `STYLE-005`, `AGT-005` | Suppressions, expiry, baseline protection | `issue-481-evidence.md` | Same-declaration PMD checks, production class expiry scan, and multiplicity-aware baselines run from root `check`. |
| `STYLE-009` | DTO scope and immutability | `issue-482-evidence.md` | Approved DTOs and Java 8 exceptions are checked; output arrays are copied. |
| `TOOL-001`, `TOOL-006–007`, `NUL-001–002`, `STYLE-008` | Compiler, NullAway, nullness imports | `issue-483-evidence.md` | Required diagnostics fail closed; deploy `main` is the only NullAway scope; NUL-002 rejects non-JSpecify nullness imports. |
| `STYLE-001–003`, `STYLE-011` | Style, comments, public API Javadoc | `issue-484-evidence.md` | PMD and Spotless gates have normal/violation fixtures and a frozen legacy baseline. |
| `LOG-001–002`, `SEC-010` | Logging and transport privacy | `issue-485-evidence.md` | LOG-001/LOG-002 exact-shape PMD rules and rendered NIO/Netty log tests pass. |
| `SEC-003` | Secrets | issue #486 comments | Gitleaks is wired into PR/release CI. |
| `SEC-015` | Dependency vulnerabilities | `issue-487-baseline.md` | OSV scans resolved inputs and blocks the current vulnerable baseline. |
| `TST-001`, `TST-003–004`, `VER-005–006`, `VER-009`, `VER-011` | Generated tests, protocol, resource lifecycle | `issue-488` implementation/evidence, `issue-489-evidence.md` | Fixed-seed and deterministic boundary tests cover applicable execution paths. |
| `TOOL-001`, `TOOL-006`, `SEC-003`, `SEC-015`, `VER-001`, `VER-013`, `VER-015` | CI gates and remaining acceptance evidence | `issue-490-evidence.md`, issue #492 | Gate wiring is in #490; owner/SHA/environment/evidence reconciliation is in #492. |

## Remaining gaps

1. **Known SCA findings:** the gate correctly fails on CRITICAL Netty 4.1.101.Final and fixable HIGH findings. Dependency upgrades were outside #487. This is a remediation blocker, not a missing scanner or threshold implementation.
2. **Live release evidence:** no artifact was published to Central during this audit. The release workflow requires its gate, a `VALIDATED` deployment, and the post-upload consumer harness before a human publishes. Ticket #490 documents controlled gate evidence without publishing a test release.
3. **Conditional formal/property tools:** PIT, TLA+, and specialist property tools have no target or acceptance contract. They remain optional under the policy and were not introduced.
4. **Human approval controls:** the public repository is maintained solo. The main ruleset remains at zero required reviews; independent internal review is recorded for policy exceptions instead of claiming a human GitHub approval.
5. **Rule status ledger:** the 152 policy rows remain authoritative in the source policy and issue #479 decisions. Ticket evidence files link implemented rules to commands and results; ticket #492 assembles final per-rule and §13 acceptance evidence.

## Custom-rule operation

PMD is pinned at 7.29.0. PMD-backed root tasks declare Java sources and baselines as inputs, declare no outputs, and are not marked cacheable; Gradle therefore reruns these checks rather than reusing cached findings. Unit/TestKit fixtures cover allowed input, forbidden input, and policy-specific exceptions. The weekly CI schedule runs `./gradlew check` and the runtime matrix. A PMD/formatter version change must rerun the focused fixtures, `./gradlew clean check`, and `deploy-test/run.sh <version>` before promotion; do not regenerate frozen baselines as part of that check.

No additional verification contract was added: existing Gradle check, TestKit, PMD samples, warning baselines, and weekly CI provide reproducible pass/fail evidence for the applicable mandatory rules. The release and SCA limits above remain explicit in this register.

## Commands and result

| Command | Result |
|---|---|
| `./gradlew :build-logic:test --tests '*JavaStyleCheckTest' --console=plain` | PASS. Includes standalone root `check` fixture without a nested included build, exact baseline match, removal/new-finding separation, and duplicate same-location findings. Before the fix, the new test failed to compile; after removing the hard included-build dependency, fixture tests initially exposed the missing PMD baseline input, which is now supplied. |
| `./gradlew checkJavaComments checkPolicyExceptions --console=plain` | PASS. Custom PMD checks reran; no task output or cache entry is declared. |
| `./gradlew clean check --console=plain` | PASS on `08e0ebba71e8c2576dcac048eba2e62b872216c7` (2026-11-25); `BUILD SUCCESSFUL` in 2m 41s, 99 actionable tasks (61 executed, 23 from cache, 15 up-to-date). |
| `deploy-test/run.sh 0.2.0` | Four consumer builds passed per issues #480/#483/#484. |
| Weekly `.github/workflows/ci.yml` | Scheduled; this workspace cannot claim a hosted workflow run. |
