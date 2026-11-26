# Issue #495 — Netty·Guava OSV 수정 증거

## 결과

- 대상 코드 SHA: `18bd48259cebcf3e1f91132385b0ca4d1966c1b9` (`integrate/479-java-quality`)
- 호스팅 검증 head SHA: `0e807a77d9acc9a62382c87ed5ed16c804131d81`; PR 검증 merge SHA: `2e41918876e88180cb47a369c7a8cf056211f2ea`.
- 실행일: 2026-11-26, Asia/Seoul. OSV 실행: `2026-11-25T23:59:20Z`–`2026-11-25T23:59:22Z`.
- 환경: Linux 5.14, aarch64; Gradle 9.9.1; Red Hat OpenJDK 25.0.4.1; 런타임 매트릭스 JDK 8/11/17/21/25.
- 담당: 빌드·모듈 담당.
- 규칙: `SEC-015`.

## 수정

- 공개 소비자 컴파일 하한, 내부 테스트와 예제, Gradle/Maven 소비자 네 곳의 Netty를 `4.1.141.Final`로 맞췄다. 제품 빌드 기본값은 루트 `gradle.properties` 한 곳에서 관리한다.
- build-logic에 `com.google.guava:guava:33.7.2-jre`를 직접 추가했다. `dependencyInsight`에서 이전 `33.4.8-jre`와 `33.7.1-jre` 대신 수정 버전이 선택됨을 확인했다.
- 실행 시 생성하는 lockfile·verification metadata·Maven SBOM을 다시 해석해 OSV 입력으로 스캔했다. 저장소는 생성 잠금 파일을 추적하지 않는다.

## 실행 증거

로컬 검증은 모두 2026-11-26 KST, 위 대상 코드 트리에서 실행했다.

| 명령 | 환경 | 결과 |
|---|---|---|
| `./gradlew -p build-logic dependencyInsight --dependency com.google.guava:guava --configuration runtimeClasspath --console=plain` | Gradle 9.9.1, JDK 25 | PASS; `33.7.2-jre` 선택, 이전 두 버전의 충돌 경로가 대체됨. |
| `./gradlew :build-logic:test --console=plain` | Gradle 9.9.1, JDK 25 | PASS, 3m 35s. |
| `./gradlew :strand-netty:test :strand-netty:testWithoutCodec :strand-integration-test:test :strand-conformance:test -PtestJavaVersion=8 -PnettyVersion=4.1.141.Final --console=plain` | Linux aarch64, JDK 8 toolchain | PASS, 3m 3s. |
| `./gradlew deployTest --console=plain` | Gradle·Maven, classpath·modulepath, JDK 25 | PASS, 네 소비자. |
| `./gradlew clean check --console=plain` | Gradle 9.9.1, Red Hat JDK 25.0.4.1 | PASS, 4m 34s. |
| `./gradlew runtimeMatrixTest -PtestJavaVersion=8 --console=plain`; same command with `11`, `17`, `21`, `25` | JDK 8/11/17/21/25 | PASS, 각 버전 실행. |
| `osv-scanner scan source --recursive --no-ignore --all-packages --format json --verbosity error /src` | OSV-Scanner v2.7.1, image digest `sha256:a70ffffc2bfd33482953e5d6b64c6fcdb30239717a07b0f0d3ab2859ce3e4cde`; 17 staged inputs | PASS; 1,097 packages, zero findings, scanner exit 0, policy exit 0, stderr empty. |

| [PR #493 hosted `ci / verify`](https://github.com/ppzxc/strand/actions/runs/45725935219) | Head `0e807a77d9acc9a62382c87ed5ed16c804131d81`; started `2026-11-26T00:03:26Z`, completed `2026-11-26T00:13:04Z` | PASS; `./gradlew check`, script/policy tests, four consumer builds, OSV scan and report upload. OSV artifact `osv-scan-report`: 17 inputs, 1,097 packages, zero findings; scanner exit 0, policy exit 0. |
| [PR #493 hosted `ci / ci-ok`](https://github.com/ppzxc/strand/actions/runs/45725935219) | `2026-11-26T00:13:00Z`; needs: verify=success, runtime=success | PASS. |
| [PR #493 hosted CodeQL](https://github.com/ppzxc/strand/actions/runs/45725935218) | Head `0e807a77d9acc9a62382c87ed5ed16c804131d81`; started `2026-11-26T00:03:26Z`, completed `2026-11-26T00:08:22Z` | PASS. |

PR #493 required checks pass on the recorded head SHA. The run's OSV artifact is retained by GitHub Actions.
