---
ticket: 14
label: wayfinder:research
checked: 2026-10-29
---

# R8 테스트 스택의 Java 8 지원

확인일: 2026-10-29. 근거는 1차 자료만 사용했다. Maven Central(repo1.maven.org)에서 jar와 POM을 직접 받아 클래스 파일 major 버전(바이트 6-7)과 MANIFEST를 읽었다. 공식 문서(assertj.github.io, docs.junit.org), GitHub 릴리스 노트, 각 저장소의 POM과 CI 워크플로를 함께 봤다. JDK 8에서 테스트를 실제로 돌려 보지는 않았다(범위 밖).

## 결론

| 질문 | 답 | 근거 |
|---|---|---|
| G1 테스트 스택(JUnit 5.14.x + AssertJ 3.x)이 Java 8 런타임에서 성립하는가 | **성립한다.** jar 메타데이터 기준으로 전 구성요소가 Java 8 이하 바이트코드다 | 아래 1~3절 |
| AssertJ 3.x 최신 릴리스 | **3.27.7** (2026-01-24). 3.x 브랜치는 `3.27.8-SNAPSHOT`으로 계속 유지보수 중 | Maven Central 메타데이터, GitHub 릴리스 |
| 3.27.7 클래스 파일 major | **52 (Java 8)**, 842개 전부. MR-JAR `META-INF/versions/9/`에는 `module-info.class`(53) 1개만 있다 | jar 직접 검사 |
| 공식 문서의 최소 Java | "AssertJ Core requires Java 8 or higher." | assertj.github.io/doc 2.4.1절 |
| 3.x 안에 Java 8 지원 마지막 버전이 따로 있나 | **없다.** 3.x 전체가 Java 8 기준이다(3.27.7 소스 POM `java.version` 8). Java 17로 올라간 것은 **4.x**(4.0.0-M1, major 61)다 | 소스 POM, 4.0.0-M1 릴리스 노트 |
| AssertJ 전이 의존성(byte-buddy) | 3.27.7이 끌어오는 byte-buddy 1.18.3은 base major **49 (Java 5)**. 최신 1.18.16도 base 52(Java 8). Java 8 런타임에 문제 없음 | jar 직접 검사, byte-buddy README |
| JUnit 5.14.4 | jupiter-api/engine, platform-engine/commons/launcher 모두 major 52. opentest4j·apiguardian은 50(Java 6). 공식 문서 "JUnit 5 requires Java 8 (or higher) at runtime." | jar 직접 검사, docs.junit.org |
| 대안이 필요한가 | 필요 없다. 다만 **AssertJ 메이저를 3으로 고정**해야 한다. 4.x로 올라가면 Java 8에서 깨진다 | 4.0.0-M1 jar major 61 |

## 1. AssertJ Core

### 1.1 버전 현황

- Maven Central `org.assertj:assertj-core` 메타데이터: `<release>4.0.0-M1</release>`, 3.x 최신은 3.27.7. 메타데이터의 `<release>`가 마일스톤을 가리킨다(이 값을 Gradle 동적 버전이나 업그레이드 도구가 어떻게 해석하는지는 미확인). 출처: https://repo1.maven.org/maven2/org/assertj/assertj-core/maven-metadata.xml
- 디렉터리 날짜: 3.27.7 2026-01-24, 4.0.0-M1 2025-03-09. 출처: https://repo1.maven.org/maven2/org/assertj/assertj-core/
- GitHub 릴리스: v3.27.7이 "Latest"(2026-01-24), v4.0.0-M1은 "Pre-release"(2025-03-09). 3.27.7은 XXE 취약점 수정(CVE-2026-24400) 포함, Byte Buddy 1.18.3과 JUnit BOM 5.14.1로 올렸다. 출처: https://github.com/assertj/assertj/releases/tag/assertj-build-3.27.7
- 공식 문서 페이지는 3.27.7을 현재 버전으로 안내한다("Last updated 2026-10-24"). 출처: https://assertj.github.io/doc/

### 1.2 3.27.7 jar 메타데이터

`assertj-core-3.27.7.jar` (sha1 `2f4f64f054c9d618d4b1d89e7611559f5e2cfff7`, Central의 `.sha1`과 일치)

- base 클래스: major 52가 842개, 다른 버전 없음.
- `Multi-Release: true`. `META-INF/versions/9/`에는 `module-info.class`(major 53)와 `OSGI-INF/MANIFEST.MF`뿐이다. Java 8 JVM은 MR 경로를 읽지 않으므로 영향 없다.
- `Require-Capability: osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=1.8))"`.
- 배포 POM(flatten된 POM)의 의존성: `net.bytebuddy:byte-buddy:1.18.3` (compile). 나머지(junit 4.13.2, hamcrest 3.0, junit-jupiter-api 5.14.1, opentest4j 1.3.0)는 모두 `provided` + `optional`이라 전이되지 않는다. 출처: https://repo1.maven.org/maven2/org/assertj/assertj-core/3.27.7/assertj-core-3.27.7.pom
- 배포 POM에는 컴파일러 설정이 없다. 소스 POM(태그 `assertj-build-3.27.7`)의 `assertj-parent/pom.xml` 19~23행: `java.version` 8, `maven.compiler.release` `${java.version}`. 즉 `--release 8`로 빌드되어 JDK 8 API 시그니처 검사도 컴파일러가 했다. 출처: https://raw.githubusercontent.com/assertj/assertj/assertj-build-3.27.7/assertj-parent/pom.xml

### 1.3 3.x와 4.x의 경계

- `3.x` 브랜치: 루트 `pom.xml` 버전 `3.27.8-SNAPSHOT`, 114행 `java.version` 8, 116행 `maven.compiler.release`. 마지막 커밋 2026-10-24("Backport: ..."). 3.x 라인은 Java 8 기준으로 백포트를 받고 있다. 출처: https://raw.githubusercontent.com/assertj/assertj/3.x/pom.xml
- `main` 브랜치: `4.0.0-SNAPSHOT`, 루트 `pom.xml` 114행 `java.version` 17. 출처: https://raw.githubusercontent.com/assertj/assertj/main/pom.xml
- 4.0.0-M1 릴리스 노트: "This release is binary incompatible with the previous major version.", Breaking Changes "Raise required Java version to 17 #3447". 출처: https://github.com/assertj/assertj/releases/tag/assertj-build-4.0.0-M1
- `assertj-core-4.0.0-M1.jar`: base 클래스 major 61이 833개, `osgi.ee` version=17. jar 직접 검사로 확인했다.
- 따라서 "3.x 중 Java 8을 지원하는 마지막 버전"은 따로 없다. 3.x 전체가 Java 8 최소이고, 경계는 3 → 4 메이저다.

### 1.4 AssertJ 3.x의 CI는 JDK 8에서 돌지 않는다

`3.x` 브랜치 `.github/workflows/main.yml`의 JDK: 25(기본, 다중 OS), 27·28(jdk.java.net early access), 21(Kotlin 통합 테스트). 다른 워크플로(binary-compatibility, release, pitest)도 25다. JDK 8 런타임 잡은 없다. 출처: https://raw.githubusercontent.com/assertj/assertj/3.x/.github/workflows/main.yml

의미: Java 8 호환은 `--release 8` 컴파일(바이트코드와 API 시그니처)로만 보장되고, JDK 8 JVM에서의 실제 동작은 upstream이 검증하지 않는다. strand의 JDK 8 테스트 런타임 잡이 사실상 이 조합의 첫 검증이 된다.

## 2. byte-buddy (AssertJ의 유일한 compile 의존성)

| 버전 | base major | MR-JAR | 비고 |
|---|---|---|---|
| 1.18.3 (AssertJ 3.27.7이 선언) | 49 (Java 5), 3046개 | `versions/9/`: 52 3046개 + `module-info` 53, `versions/24/`: 68 25개 | `Build-Jdk-Spec: 1.8` |
| 1.18.16 (2026-10-23, 최신) | 52 (Java 8), 3050개 | `versions/9/`: `module-info` 53, `versions/24/`: 68 25개 | `Build-Jdk-Spec: 1.8` |

- `versions/24/`의 major 68 클래스는 Java 24 이상 JVM에서만 로드된다. Java 8~21 런타임에는 base 클래스가 쓰인다.
- byte-buddy README 호환 표: `1.18.7` 최소 JVM 8, `1.18.7-jdk5` 최소 JVM 5, `1.17.0`까지는 최소 JVM 5. 출처: https://raw.githubusercontent.com/raphw/byte-buddy/master/README.md ("Java version compatibility")
- release notes 1.18.7(2026-03-01): "Introduce new versioning concept with *-jdk5* suffix for backwards-compatible jar and Java 8 baseline for regular jar." 출처: https://raw.githubusercontent.com/raphw/byte-buddy/master/release-notes.md
- 결론: 다른 의존성(예: Mockito)이 byte-buddy를 더 높은 1.18.x로 끌어올려도 base는 Java 8이라 Java 8 런타임에서 성립한다. byte-buddy jar는 ASM을 `net/bytebuddy/jar/asm/` 아래로 셰이딩해 포함한다(1.18.3 jar에 해당 경로 항목 102개). 1.18.3 POM의 의존성은 jna, jna-platform, findbugs-annotations, jsr305 넷뿐이고 모두 `provided`라 전이 의존성이 없다.

## 3. JUnit 5.14.4

jar 직접 검사(2026-04-26 릴리스, 5.x 최신):

| 아티팩트 | base major | 비고 |
|---|---|---|
| junit-jupiter-api 5.14.4 | 52 ×201, 루트 `module-info.class` 53 ×1 | `osgi.ee` 1.8 |
| junit-jupiter-engine 5.14.4 | 52 ×161, `module-info` 53 | `osgi.ee` 1.8 |
| junit-platform-engine 1.14.4 | 52 ×171, `module-info` 53 | `osgi.ee` 1.8 |
| junit-platform-commons 1.14.4 | 52 ×77, `module-info` 53, MR `versions/9/` 53 ×5 | `Multi-Release: true` |
| junit-platform-launcher 1.14.4 | 52 ×128, `module-info` 53 | `osgi.ee` 1.8 |
| opentest4j 1.3.0 | 50 ×8, `module-info` 53 | `osgi.ee` 1.6 |
| apiguardian-api 1.1.2 | 50 ×2, `module-info` 53 | `osgi.ee` 1.6 |

- 루트에 있는 `module-info.class`(53)는 Java 8 JVM이 클래스로 로드하지 않는다(이름이 유효한 클래스 이름이 아니라 classpath 조회 대상이 되지 않는다). 이 동작 자체는 JDK 8에서 실행해 확인하지 않았다(미확인과 한계 참고).
- JUnit 공식 문서(5.14.4 overview): "JUnit 5 requires Java 8 (or higher) at runtime." 출처: https://docs.junit.org/5.14.4/overview.html
- 참고: JUnit 최신은 6.1.3(2026-08-07). G1에서 확인한 대로 6.x는 major 61이므로 Java 8 테스트 런타임에는 5.14.x 고정이 필요하다. 출처: https://repo1.maven.org/maven2/org/junit/jupiter/junit-jupiter-api/maven-metadata.xml

## 4. 대안 현황 (AssertJ가 Java 8을 지원하므로 참고용)

| 대안 | 최신 | Java 8 런타임 | 비고 |
|---|---|---|---|
| AssertJ 3.27.x 고정 | 3.27.7 | 가능(major 52) | 권장안. 3.x 유지보수 진행 중(1.3절) |
| Google Truth | 1.4.5 (2025-09-10) | jar는 major 52 | compile 의존성이 많다: guava 33.4.3-android(52), **junit:junit 4.13.2(49)**, asm 9.8(49), jspecify, error_prone_annotations, auto-value-annotations. JUnit 4가 테스트 classpath에 들어온다. 빌드는 `source/target 1.8`(`--release` 아님, `Build-Jdk-Spec: 11`)이고 animal-sniffer는 Android API 23 시그니처로만 검사한다. 부모 POM 주석: "In principle, it would make sense to *also* test compatibility with JDK 1.8 ... " (TODO). 공식 문서의 최소 Java 명시는 미확인 |
| Hamcrest | 3.0 (2024-08-01) | 가능(major 52, `osgi.ee` 1.8) | 메타데이터 lastUpdated 2024-08-01 이후 새 릴리스 없음 |
| JUnit `Assertions`만 사용 | 5.14.4 | 가능(major 52) | 추가 의존성 없음. 표현력은 위 라이브러리보다 낮다(이번 조사는 기능 비교를 하지 않았다) |

출처:
- Truth: https://repo1.maven.org/maven2/com/google/truth/truth/1.4.5/truth-1.4.5.pom, https://repo1.maven.org/maven2/com/google/truth/truth-parent/1.4.5/truth-parent-1.4.5.pom (270~295행), https://github.com/google/truth/releases/tag/v1.4.5
- Hamcrest: https://repo1.maven.org/maven2/org/hamcrest/hamcrest/maven-metadata.xml, `hamcrest-3.0.jar`

## 5. strand에 대한 의미

- G1 결정 5(JUnit 5.14.x + AssertJ 3.x, 메이저 고정)는 그대로 성립한다. G1에 남아 있던 "AssertJ의 바이트코드 버전은 아직 확인하지 않았다"는 이 조사로 해소된다: 3.27.7은 major 52.
- 버전 카탈로그에서 AssertJ를 3.x로 막아 두어야 한다. 4.x(major 61)는 Java 8 테스트 런타임에서 `UnsupportedClassVersionError`가 난다. Maven Central 메타데이터 `<release>`가 이미 4.0.0-M1이다. 동적 버전이나 자동 업그레이드 도구가 이를 고를지는 미확인이므로 정확한 버전으로 고정하는 편이 안전하다.
- 재검토 조건 후보: AssertJ 3.x 유지보수 종료(현재 3.x 브랜치 활동 중이며 종료 공지는 찾지 못했다).

## 미확인과 한계

- **JDK 8에서의 실제 실행은 하지 않았다.** 결론은 클래스 파일 major, OSGi EE, `--release 8` 빌드 설정, 공식 문서 문구에 근거한다. 바이트코드가 52여도 리플렉션이나 `Class.forName`으로 Java 9 이상 API를 찾는 코드가 런타임에 실패할 가능성은 이 방법으로 배제할 수 없다. AssertJ upstream CI도 JDK 8을 돌리지 않는다(1.4절). 부트스트랩 때 JDK 8 테스트 잡으로 확인한다.
- 루트 `module-info.class`가 Java 8에서 무해하다는 점은 JDK 8에서 직접 확인하지 않았다.
- AssertJ 3.x의 유지보수 종료 시점이나 지원 정책 문서는 찾지 못했다. 미확인.
- AssertJ 4.0 정식 릴리스 시점은 미확인. 4.0.0-M1 이후 마일스톤은 Central에 없다(2026-10-29 기준).
- Google Truth의 공식 최소 Java 버전 문구는 미확인. jar major와 POM만 봤다.
- Hamcrest 3.0의 컴파일 설정(`--release` 여부)은 확인하지 않았다.
- Maven 메타데이터 `<release>`가 4.0.0-M1일 때 Gradle `latest.release`나 Dependabot 등이 어떤 버전을 고르는지는 미확인.
- Gradle 의존성 해석 결과(다른 테스트 의존성과 섞였을 때 byte-buddy 최종 버전 등)는 이 저장소에 빌드가 없어 확인하지 않았다.
