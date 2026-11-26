# Java Quality SPEC 적용 결과

## 프로젝트 현황

- Policy: Java AI Engineering Quality Specification 1.3.3; source SHA-256 `36be76e7ecbe0b7f3985ab073f0daec0f5e642eab9e3e077b377b7ddf42bff6c`.
- Target code SHA: `2ed58f6a6db465a5ae78ccc5924b22658ea53670` (`integrate/479-java-quality`, PR #493 head).
- Environment: JDK 25, Gradle 9.9.2. Consumer reports use Gradle/Maven and record their own command and environment.
- Modules: four deployment modules, internal examples/conformance/integration/load/benchmark modules, included `build-logic`, and four independent Gradle/Maven consumers.
- Profiles: DDD/hexagonal, Spring, and MyBatis remain inactive per applicability decisions; no framework was added.
- Evidence date: 2026-11-26, Asia/Seoul. Most prior reports record commit time, not test execution time. Missing test timestamps are marked as unrecorded.

## Latest SCA remediation

Ticket #495 resolved the CRITICAL Netty and HIGH build-logic baseline described in the historical findings below. Netty minimum is now `4.1.141.Final`; build-logic selects Guava `33.7.2-jre`. PR #493 hosted run [45727363940](https://github.com/ppzxc/strand/actions/runs/45727363940) passed `ci / verify`, `ci / ci-ok`, and JDK 8/11/17/21/25. Its OSV artifact scanned 17 inputs / 1,097 packages with zero findings, scanner exit 0, and policy exit 0. Details: [#495 remediation evidence](issue-495-remediation-evidence.md). #492 remains open for 84 applicable rule rows without rule-specific execution evidence. Maven Central deployment validation remains a pre-publication condition for a future release.

## §13 Acceptance status

| # | Criterion | Status | Evidence and limits |
|---:|---|---|---|
| 1 | Technology stack and profiles | PASS locally | `issue-491-gap-register.md`; deployed Java 8 / JDK 25, active and inactive profiles mapped. |
| 2 | 152 rule records | PARTIAL | This report indexes all 152 source IDs. 36 rows link to dedicated issue evidence; 29 rows have inactive-profile, optional-rule, or conditional dispositions; 3 rows identify deleted/transferred IDs; 84 applicable rows still lack rule-specific execution evidence. |
| 3 | Architecture and types first | PARTIAL | #480 architecture/dependency samples and #482 DTO/immutability tests pass. Full design review of all API/state invariants is not recorded. |
| 4 | Working CI | PASS | #490 tests required-result success/fail/cancel/missing gate behavior and active `ci / ci-ok` rule. Earlier run `45648947339` correctly blocked on the then-known SCA baseline. After #495, run `45727363940` passed `ci / verify`, `ci / ci-ok`, OSV, and all five runtime jobs. |
| 5 | Custom rule tests | PASS locally | #481/#483/#484/#485 provide normal, violation, and allowed-exception fixtures. See linked evidence. |
| 6 | Required rules are ERROR | PASS locally | #483 Error Prone violation samples fail in managed and Gradle/Maven consumer builds; #490 fail-closed gate fixtures pass. |
| 7 | Suppression pair and expiry | PASS locally | #481 PMD pairing, production class expiry scan, and duplicate-preserving baseline tests pass. Internal reviewer: `review481`. |
| 8 | Author-enforced policy | PARTIAL | Static gates cover DTO, nullness, style, suppression and logging rules. Manual decisions use internal review task IDs; no human GitHub approval is claimed. |
| 9 | Per-commit evidence | PARTIAL | Evidence reports record target commits, environments, commands, and outcomes. Test execution timestamps are absent from some older reports. |
| 10 | Failure and regression loop | PARTIAL | PR #493 runs `45634765483`, `45641400310`, and `45645491691` exposed OSV scan scoping, missing `ci-ok` checkout, and a JDK 17 test race. #494, commit `7e70064`, and commit `c1bc5c3` fixed them; the latest hosted run `45648947339` reran successfully apart from the expected SCA policy failure. |
| 11 | Defect regression prevention | PASS locally | #482 array-copy, #485 log-redaction, #481 expiry/baseline, and #483 compiler/style regressions have passing focused tests. |
| 12 | Baseline protection | PASS locally | #481/#483/#484 multiset or exact baselines reject new/duplicate findings; frozen ArchUnit baseline disallows auto-creation. |
| 13 | Lint promotion | PASS locally | Required Error Prone and custom PMD rules have normal/violation coverage before root check enforcement. Internal review IDs are listed in §11. |
| 14 | Working automation | PASS locally | Gradle tasks, `deployTest`, workflow checks, OSV, Gitleaks, and release gates have reproducible commands in evidence reports. |
| 15 | Constraint disclosure | PASS | Resolved SCA findings and remaining missing timestamps and rule evidence are listed with owners or follow-up conditions. Maven Central deployment validation is recorded as a pre-publication condition for the next release. |

## Applied result format (§11.2)

### Applicability decisions

- The source has 152 current and historical rule rows. The index below preserves every row ID and points to supporting evidence or marks the row unverified. Audit correction: source rows ARC-001–004 and ARC-006–012 are DDD/hexagonal profile rules and are not applicable while that profile is inactive. Older #481/#491 summaries list ARC-001–003 beside baseline work; those rules do not describe the generic suppression baseline, so this report does not count them as verified.
- Deleted/transferred IDs remain historical entries; they are not treated as current requirements.
- Inactive Spring/MyBatis profile rules use the applicability decision in `issue-491-gap-register.md`.
- Internal reviewer task IDs: `review481` (#481 expiry/baseline), `review482` (#482 DTO scope), and `review485final` (#485 logging). These are internal AI review records, not human GitHub approvals.

### Actual findings

| Severity | Rule | Finding | Reproduction/result | Prevention / follow-up |
|---|---|---|---|---|
| CRITICAL | `SEC-015` | Historical finding: OSV found `io.netty:netty-handler@4.1.101.Final` in resolved dependencies. | Original local scan on 17 required inputs exited 1; policy gate failed as specified. #495 upgrades Netty to `4.1.141.Final`; latest hosted scan reports zero findings. | Resolved in #495; see `issue-495-remediation-evidence.md`. |
| HIGH | `SEC-015` | Historical finding: OSV found fixable HIGH findings in build-logic. | Original scan result is recorded in `issue-487-baseline.md`. #495 pins build-logic Guava to `33.7.2-jre`; latest hosted scan reports zero findings. | Resolved in #495; same 17-input scanner and policy gate now pass. |
| INFO | `TOOL-006` | Four existing `requires-automatic` warnings remain in the two modulepath consumers. | Exact consumer warning baseline passes; new/moved/missing diagnostics fail. | Keep line/rule/message baseline reviewed; remove warnings when module metadata is corrected. |
| INFO | `STYLE-011` | 320 existing PMD style/Javadoc findings are frozen. | #484's existing baseline passes; new findings fail. | Reduce baseline in reviewed changes; do not regenerate automatically. |
| INFO | `STYLE-005` | 32 legacy suppression-pair findings are frozen. | #481 PMD baseline compares diagnostic multiplicities; new or duplicate same-location findings fail. | Add only independently reviewed, narrow, dated exceptions. |

Actual hosted CI/PR incidents: run `45634765483` failed OSV extraction on generated Gradle TestKit POM caches. Issue #494 stages exactly the 17 declared inputs and rejects missing or workspace-escaping paths. Run `45638715862` scanned 17 sources / 1,104 packages with zero extraction errors, then failed severity policy on the known baseline. Run `45641400310` then found that `ci-ok` lacked checkout; commit `7e70064` added checkout and a regression assertion. Run `45643237410` confirmed `ci-ok` reads `needs` and fails with `verify` because OSV still blocks. Run `45645491691` exposed a JDK 17 test race: the test observed the request reservation released before the byte reservation; commit `c1bc5c3` waits for both counters, passed 10 local reruns, and passed in run `45648947339`. Ticket #495 upgraded the vulnerable dependencies without weakening the OSV policy. Run `45727363940` passed the full CI and OSV gate. Its first Gradle attempt missed Netty branch coverage by 0.01; the forced local rerun and hosted retry passed without code changes, so a low-probability coverage flake remains possible.

### Checks performed

| Command / report | Execution | Result | Evidence |
|---|---|---|---|
| `./gradlew clean check --console=plain` | PASS on `08e0ebba71e8c2576dcac048eba2e62b872216c7`; 2026-11-25; JDK 25 / Gradle 9.9.2 | `BUILD SUCCESSFUL` in 2m 41s; 99 tasks (61 executed, 23 from cache, 15 up-to-date). | `issue-491-gap-register.md` |
| `deploy-test/run.sh 0.2.0` | PASS in #480/#483/#484 evidence; exact per-run time not captured | Four Gradle/Maven classpath/modulepath consumers pass. | `issue-480-evidence.md`, `issue-483-evidence.md`, `issue-484-evidence.md` |
| OSV v2.7.1 required-input scan | PASS as a scan; gate outcome is expected failure; exact scan time not captured | 17 required inputs scanned; CRITICAL/HIGH findings cause exit 1. | `issue-487-baseline.md` |
| `python3 .github/scripts/test/test_ci_gate.py` | PASS; 5 tests | Success passes; failure/cancel/skipped/missing/unexpected/malformed results fail. | `issue-490-evidence.md` |
| `python3 .github/scripts/test/test_osv_policy.py` | PASS; 7 tests | Threshold and input/error behavior pass. | `issue-490-evidence.md` |
| Gitleaks Bats tests | NOT RUN locally | Bats unavailable; workflow installs and runs the tests in hosted CI. | `issue-486-evidence.md` |
| GitHub hosted PR CI #493 | PARTIAL / expected SCA failure; run `45648947339`, target SHA `c1bc5c390c3968ec20817d6fe692d37530392bc2`, merge SHA `e94134bed274bfbd069b03df4686c9b423ffb79d`, started `2026-11-25T15:49:03Z`, completed `2026-11-25T15:56:07Z` | `./gradlew check`, CodeQL, Bats, OSV policy/staging/gate tests, deployTest, and JDK 8/11/17/21/25 pass. OSV scans exactly 17 staged sources (1,104 packages, 168 findings), with 5 CRITICAL and 64 HIGH rows; `ci / verify` fails on policy, and `ci / ci-ok` executes successfully then fails closed because `verify` failed. | [Run 45648947339](https://github.com/ppzxc/strand/actions/runs/45648947339); artifact `osv-scan-report`; #494 |
| GitHub hosted PR CI #493, after #495 | PASS on head `2ed58f6a6db465a5ae78ccc5924b22658ea53670`, run `45727363940`, merge SHA `18ad513ed6d827025157d2939a2451b8718d4141`; completed `2026-11-26T00:34:04Z` | `./gradlew check`, Bats, policy/staging/gate tests, deployTest, OSV, `ci / ci-ok`, and JDK 8/11/17/21/25 pass. OSV reports 17 inputs / 1,097 packages / zero findings; scanner and policy exit 0. The initial `check` attempt missed branch coverage by 0.01; hosted retry passed. | [Run 45727363940](https://github.com/ppzxc/strand/actions/runs/45727363940); artifact `osv-scan-report`; [#495 evidence](issue-495-remediation-evidence.md) |
| GitHub weekly workflow | NOT RUN | No weekly run evidence from this workspace. | `issue-490-evidence.md` |
| Central release / post-upload validation | NOT RUN | No artifact published; release YAML orders `VALIDATED` deployment test before public release. | `issue-490-evidence.md` |

### Gaps, exceptions, and review conditions

- **Unverified rule rows:** 84 applicable IDs lack rule-specific execution evidence. Four optional `MAY` rows and one conditional rule are recorded with applicability dispositions; inactive profiles and historical IDs remain separately classified. See the per-ID index and `issue-491-gap-register.md`.
- **Actual CI/PR incidents:** #493 runs `45634765483` and `45641400310` exposed the OSV cache-scan issue and missing `ci-ok` checkout. #494 and commit `7e70064` fix those defects. Run `45645491691` exposed a flaky JDK 17 reservation assertion; commit `c1bc5c3` fixes it, with 10 local focused reruns and hosted JDK 17 success in `45648947339`. Run `45727363940` confirms the upgraded dependencies pass the same OSV gate. Its first `check` attempt missed the coverage threshold; the retry passed without source changes.
- **SCA remediation:** complete in #495. Keep SEC-015 blocking for future findings; no waiver or threshold reduction was added.
- **Live release:** release owner must record upload, `VALIDATED` deployment, and post-upload consumer result before publication; no release was made here. Hosted PR CI is recorded above.
- **Human review:** repository is solo-maintained; main ruleset remains at zero required approvals. Internal AI review records above are not human approval.
- **Automatic merge:** not enabled. Policy prerequisites and protected-check evidence are not fully demonstrated; no automatic merge change is proposed.
- **Recheck:** rerun `./gradlew clean check --console=plain`, `deploy-test/run.sh <version>`, OSV scan, Gitleaks Bats tests, and runtime matrix at the target SHA. Keep any unresolved result and timestamp in the §13 ledger.

### 152-row rule index

Each linked report contains the owner, rule IDs, commands, results, target commit, and environment when captured. Missing fields remain explicitly unverified; do not infer test timestamps from commit timestamps.

| Rule ID | Source section | Status | Owner | Command/result/SHA/environment/time/report |
|---|---|---|---|---|
| `CBD-001` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-002` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-003` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-004` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-005` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-006` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-007` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-008` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-009` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CBD-010` | 1. Codebase: 코드와 아키텍처 자체로 오류 예방 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ARC-001` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-001 |
| `ARC-002` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-002 |
| `ARC-003` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-003 |
| `ARC-004` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-004 |
| `ARC-005` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-480-evidence.md, issue-491-gap-register.md |
| `ARC-006` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-006 |
| `ARC-007` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-007 |
| `ARC-008` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-008 |
| `ARC-009` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-009 |
| `ARC-010` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-010 |
| `ARC-011` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-011 |
| `ARC-012` | 1.1 아키텍처 선택 프로파일: DDD/헥사고날 | 미적용 프로파일; DDD/헥사고날 미채택 | 빌드·모듈 담당 | issue-491-gap-register.md, source row ARC-012 |
| `OBJ-001` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-002` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-003` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-004` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-005` | 2.1 객체·타입·동등성·컬렉션 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `OBJ-006` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-007` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-008` | 2.1 객체·타입·동등성·컬렉션 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `OBJ-009` | 2.1 객체·타입·동등성·컬렉션 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `OBJ-010` | 2.1 객체·타입·동등성·컬렉션 | 증거 연결; 필수 Error Prone 체크를 ERROR로 설정하고 컴파일 검사 실행 | `strand-build`, 소비자 빌드 담당 | issue-483-evidence.md, issue-491-gap-register.md |
| `OBJ-011` | 2.1 객체·타입·동등성·컬렉션 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `OBJ-012` | 2.1 객체·타입·동등성·컬렉션 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-001` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-002` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-003` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-004` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-005` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-006` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-007` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-008` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-009` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-010` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `ERR-011` | 2.2 예외 및 실패 처리 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-001` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-002` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-003` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-004` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-005` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-006` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-007` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-008` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-009` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-010` | 2.3 동시성·스레드·성능 | 이력 ID; 원문 행에서 삭제·이관 상태 확인 | 정책 소유자 | java-engineering-quality.md |
| `CON-011` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `CON-012` | 2.3 동시성·스레드·성능 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `STYLE-001` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-484-evidence.md, issue-491-gap-register.md |
| `STYLE-002` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-484-evidence.md, issue-491-gap-register.md |
| `STYLE-003` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-484-evidence.md, issue-491-gap-register.md |
| `STYLE-004` | 2.4 코드 스타일·유지보수·기계적 검사 | 이력 ID; 원문 행에서 삭제·이관 상태 확인 | 정책 소유자 | java-engineering-quality.md |
| `STYLE-005` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-481-evidence.md, issue-491-gap-register.md |
| `STYLE-006` | 2.4 코드 스타일·유지보수·기계적 검사 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `STYLE-007` | 2.4 코드 스타일·유지보수·기계적 검사 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `STYLE-008` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-483-evidence.md, issue-491-gap-register.md |
| `STYLE-009` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-482-evidence.md, issue-491-gap-register.md |
| `STYLE-010` | 2.4 코드 스타일·유지보수·기계적 검사 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `STYLE-011` | 2.4 코드 스타일·유지보수·기계적 검사 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-484-evidence.md, issue-491-gap-register.md |
| `STYLE-012` | 2.4 코드 스타일·유지보수·기계적 검사 | 이력 ID; 원문 행에서 삭제·이관 상태 확인 | 정책 소유자 | java-engineering-quality.md |
| `STYLE-013` | 2.4 코드 스타일·유지보수·기계적 검사 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `STYLE-014` | 2.4 코드 스타일·유지보수·기계적 검사 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `NUL-001` | 2.5 Nullness·결정성·로깅 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-483-evidence.md, issue-491-gap-register.md |
| `NUL-002` | 2.5 Nullness·결정성·로깅 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-483-evidence.md, issue-491-gap-register.md |
| `NUL-003` | 2.5 Nullness·결정성·로깅 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `DET-001` | 2.5 Nullness·결정성·로깅 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `DET-002` | 2.5 Nullness·결정성·로깅 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `LOG-001` | 2.5 Nullness·결정성·로깅 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-485-evidence.md, issue-491-gap-register.md |
| `LOG-002` | 2.5 Nullness·결정성·로깅 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-485-evidence.md, issue-491-gap-register.md |
| `LOG-003` | 2.5 Nullness·결정성·로깅 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-001` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-002` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-003` | 3. Security: Java 보안 코드 규칙 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-486-evidence.md, issue-490-evidence.md, issue-491-gap-register.md |
| `SEC-004` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-005` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-006` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-007` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-008` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-009` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-010` | 3. Security: Java 보안 코드 규칙 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-491-gap-register.md |
| `SEC-011` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-012` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-013` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-014` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `SEC-015` | 3. Security: Java 보안 코드 규칙 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-487-baseline.md, issue-490-evidence.md, issue-491-gap-register.md |
| `SEC-016` | 3. Security: Java 보안 코드 규칙 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `MB-001` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `MB-002` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `MB-003` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `MB-004` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `MB-005` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `MB-006` | 3.1 데이터 접근 선택 프로파일: MyBatis | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-001` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-002` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-003` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-004` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-005` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-006` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `SPR-007` | 4. Framework: Spring 선택 프로파일 | 미적용 프로파일; 저장소 기술 스택 조사 참조 | 빌드·모듈 담당 | issue-491-gap-register.md |
| `VER-001` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-490-evidence.md, issue-491-gap-register.md |
| `VER-002` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-003` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-004` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-005` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-489-evidence.md, issue-491-gap-register.md |
| `VER-006` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-489-evidence.md, issue-491-gap-register.md |
| `VER-007` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-008` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-009` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-489-evidence.md, issue-491-gap-register.md |
| `VER-010` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-011` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-489-evidence.md, issue-491-gap-register.md |
| `VER-012` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-013` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-490-evidence.md, issue-491-gap-register.md |
| `VER-014` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `VER-015` | 5. Runtime Verification: 실행·회귀 검증 요구사항 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-490-evidence.md, issue-491-gap-register.md |
| `FORM-001` | 6. Formal Verification 및 상태 기반 검증 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `FORM-002` | 6. Formal Verification 및 상태 기반 검증 | 미적용; MAY 선택 사항이며 상태 모델 도입 근거 없음 | 빌드·모듈 담당 | issue-491-gap-register.md; issue-491 §6 |
| `FORM-003` | 6. Formal Verification 및 상태 기반 검증 | 미적용; MAY 선택 사항이며 분산 시스템/TLA+ 검증 대상 없음 | 빌드·모듈 담당 | issue-491-gap-register.md; issue-491 §6 |
| `FORM-004` | 6. Formal Verification 및 상태 기반 검증 | 미적용; MAY 선택 사항이며 형식 증명 도입 대상 없음 | 빌드·모듈 담당 | issue-491-gap-register.md; issue-491 §6 |
| `FORM-005` | 6. Formal Verification 및 상태 기반 검증 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `FORM-006` | 6. Formal Verification 및 상태 기반 검증 | 조건부; 형식 검증 도입 시 모델과 구현 연결 근거 필요; 현재 도입 없음 | 빌드·모듈 담당 | issue-491-gap-register.md; issue-491 §6 |
| `TST-001` | 6.1 생성 데이터 기반 검증 정책 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-488-evidence.md, issue-491-gap-register.md |
| `TST-002` | 6.1 생성 데이터 기반 검증 정책 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TST-003` | 6.1 생성 데이터 기반 검증 정책 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-488-evidence.md, issue-491-gap-register.md |
| `TST-004` | 6.1 생성 데이터 기반 검증 정책 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-488-evidence.md, issue-489-evidence.md, issue-491-gap-register.md |
| `TST-005` | 6.1 생성 데이터 기반 검증 정책 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TST-006` | 6.1 생성 데이터 기반 검증 정책 | 미적용; MAY 선택 사항이며 최소 반례 축소 도구를 채택하지 않음 | 테스트 담당 | issue-488-evidence.md |
| `AGT-001` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-002` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-003` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-004` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-005` | 7.1 최소 AI 작업 계약 | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-481-evidence.md, issue-491-gap-register.md |
| `AGT-006` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-007` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-008` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `AGT-009` | 7.1 최소 AI 작업 계약 | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TOOL-001` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-480-evidence.md, issue-483-evidence.md, issue-486-evidence.md, issue-490-evidence.md, issue-491-gap-register.md |
| `TOOL-002` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TOOL-003` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TOOL-004` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TOOL-005` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
| `TOOL-006` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-483-evidence.md, issue-490-evidence.md, issue-491-gap-register.md |
| `TOOL-007` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 증거 연결; 상세 판정은 보고서 참조 | 연결된 보고서의 Owner 필드 | issue-483-evidence.md, issue-491-gap-register.md |
| `TOOL-008` | 8.1 기본 툴체인 최소화 및 책임 이관 (작성자 강제 정책) | 미확인; 규칙별 실행·통과 증거 연결 없음 | §13 담당; 행별 지정 미완료 | issue-491-gap-register.md |
