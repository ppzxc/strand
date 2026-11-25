# Lauren Tan의 에이전트 환경 방법론으로 점검한 Strand

2026-11-25, 커밋 `4ba4e5f` 기준. [사용자가 공유한 한국어 해설 영상](https://www.youtube.com/watch?v=ueA116hO_ho)의 여섯 절을 체크리스트로 사용했다. 발표자 발언과 슬라이드 맥락은 [AI Engineer 원본 영상](https://www.youtube.com/watch?v=O_zcc6Xl12g), [주최 측 발표·슬라이드 페이지](https://ai.engineer/talks/O_zcc6Xl12g-all-code-is-slop-except-mine), [주최 측 라이브 자동 자막](https://ai.engineer/api/code/reading-download/transcripts/lauren-tan.md)과 대조했다. 본문 표의 시간은 사용자가 제공한 한국어 해설 영상의 인덱스다. 해당 업로드의 자막은 직접 확보하지 못했다. 원본은 17분 31초이며 자동 자막은 라이브 스트림의 시간으로 표기된다.

## 판정

**Strand는 코드베이스·정적 분석·실행 검증에 이미 투자했다. 다음 병목은 경합 검증의 재현성과 성공 증거의 전달이다.** 특히 요청이 처음 들어오는 순간, 쓰기 진행과 취소가 교차하는 순간, 연결이 닫히는 순간에 회귀가 있었다. 이 경로에는 실제 전송 테스트가 이미 있다. 다음 투자는 관련 테스트를 다시 손쉽게 실행하고, 실패한 이벤트 순서를 보존하고, 성공한 결과도 PR에서 읽을 수 있게 만드는 것이다. [#403 첫 요청 경쟁](https://github.com/ppzxc/strand/pull/403), [#404 동시 CLOSE·간헐 실패](https://github.com/ppzxc/strand/pull/404), [#405 종료 순서·설정 검증](https://github.com/ppzxc/strand/pull/405)

| 피라미드 순서 | 현재 판정 | 다음 확인 지점 |
|---|---|---|
| 1. 코드베이스 | 적용. core, 공용 앱 API, NIO, Netty, 도구의 책임이 드러난다. | 모듈 내부에서도 잘못된 스레드·자원 소유권을 표현하기 어려운지 확인한다. |
| 2. 정적 분석 | 적용. 컴파일·의존성·호환성 예방 장치가 있다. | 전송 내부 패키지 경계와 실행 컨텍스트에는 별도 증거가 필요하다. |
| 3. 검증 | 런타임은 상당히 적용. 정형 모델과 체계적인 이벤트 순서 생성은 확인되지 않았다. | 요구사항 → 테스트·벡터 → 관측 결과를 연결한다. |
| 4. 산문 | 얇은 에이전트 안내와 도메인 문서를 사용한다. | 실제 진입점과 어긋난 읽기 경로를 정비한다. |
| 5. 사람 리뷰 | CI 증거와 계약·성능 검토가 일부 PR에 보인다. | PR에서 의도와 관측 결과를 함께 확인할 수 있게 한다. |

## 1. 신뢰와 생산성

| 발표 항목 | Strand 점검 |
|---|---|
| 01:49 생산성을 크게 높이면서 품질 유지·향상 | Strand에서 가치가 있는 품질 지표는 Java 버전·API 호환성, 장기 연결 생존성, 결과 확정성, 지연·자원 사용이다. JDK 매트릭스와 부하 하네스가 이미 측정 수단을 제공한다. 변경량·머지 시간과 함께 재수정·릴리스 후 결함·간헐 테스트 빈도를 추적한 저장소 기준선은 이번 점검에서 확인하지 못했다. [CI](../../.github/workflows/ci.yml), [장기 연결 ADR](../adr/0004-장기-연결-안정성-포지션.md), [부하 비교](transport-load-comparison.md) |
| 02:10 에이전트 환경에 투자해 신뢰 형성 | Gradle Wrapper, 규격과 실행 벡터, 실제 소켓·TLS 검증, 외부 소비자 하네스까지 제공한다. 에이전트가 혼자 실행할 수 있는 환경이라는 점에서 이미 적용됐다. [루트 빌드](../../build.gradle.kts), [배포 검증](../../deploy-test/run.sh) |
| 02:41 사람이 문제를 찾는 루프에서 에이전트가 자체 검증하는 루프로 | #442는 실제 실행에서 측정 도구의 JMX·히스토그램 초기화가 p99를 부풀리는 원인을 발견하고 수정한 사례다. #443은 에이전트 작성 표시와 검증 결과, 녹색 CI가 확인된다. 이 두 사례로 반복 가능한 경로를 확인했다. 전체 PR의 검증 주체나 품질 추이는 확인하지 않았다. [#442](https://github.com/ppzxc/strand/pull/442), [#443](https://github.com/ppzxc/strand/pull/443) |

## 2. 다섯 계층의 역할

| 발표 항목 | Strand 점검 |
|---|---|
| 02:58–03:16 코드베이스·정적 분석은 함께 작동하는 결정론적 제약 | `strand-core`는 프로토콜 엔드포인트, `strand-api`는 전송 독립 앱 API, NIO·Netty는 전송 책임을 맡는다. Gradle 허용 목록과 컴파일 검사가 이를 보강한다. 실행 중의 이벤트 루프 소유권은 별도 테스트가 맡는다. [모듈 구성](../../settings.gradle.kts), [라이브러리 검사](../../build-logic/src/main/kotlin/strand.library-conventions.gradle.kts), [Netty 소유권 테스트](../../strand-netty/src/test/java/io/github/ppzxc/strand/netty/NettyStrandEventLoopOwnershipTest.java) |
| 03:38 검증은 버그·불변식·사용자 의도를 확인 | 규격의 요구사항 ID와 적합성 벡터, 공개 API 통합 테스트가 있다. 벡터로 관측하지 못하는 요구사항에는 관측 불가·형식 미지원 사유가 있다. 새 요구의 의도는 관측 가능한 인수 조건과 실제 결과를 연결할 때 드러난다. [규격](../specification/STRAND_SPECIFICATION.md), [적합성 검사](../../strand-conformance/src/test/java/io/github/ppzxc/strand/conformance/SpecCoverageTest.java), [벡터 제외 목록](../../strand-conformance/src/test/resources/uncovered-requirements.txt) |
| 03:54 규칙·스킬·AGENTS.md는 판단이 필요한 부분에 얇게 | 루트 `AGENTS.md`는 이슈 구조·라벨·도메인 문서·규격 작성 위치만 안내한다. `GLOSSARY.md`에는 공용 어휘, ADR에는 설계 결정이 있다. 반복 실행 경로는 이미 Gradle과 하네스에 있으므로 해당 진입점을 짧게 연결하는 편이 맞다. [AGENTS.md](../../AGENTS.md), [용어집](../../GLOSSARY.md), [ADR](../adr/) |
| 03:00 / 12:37 사람 리뷰 | CI가 기계적으로 확인할 수 있는 항목을 집계한다. 사람은 공개 동작 변경, 요구의 의미, 배포 판단, 성능 결과의 해석을 맡을 수 있다. #443에는 GitHub의 제출된 리뷰가 0건이었고 GitHub ruleset도 기본 필수 승인 수가 0이다. 외부 리뷰나 병합 주체는 이 기록에서 판별할 수 없다. [CI](../../.github/workflows/ci.yml), [#443](https://github.com/ppzxc/strand/pull/443), [main 적용 규칙](https://api.github.com/repos/ppzxc/strand/rules/branches/main) |

## 3. 코드베이스와 결정론적 제약

| 발표 항목 | Strand 점검 |
|---|---|
| 04:32 입력 품질과 재현성 | Strand의 기존 예제·테스트·규격이 다음 변경의 입력이다. 요구사항 ID, 필수 벡터, 삭제된 ID 관리, TestKit 회귀 테스트에 이미 반복 가능한 규칙이 들어 있다. 반면 루트 README의 ‘아직 코드는 없습니다’는 공개 구현·검증 경로가 존재하는 현재 상태와 어긋난다. [README](../../README.md), [벡터 리소스](../../strand-conformance/src/test/resources/), [빌드 검증 테스트](../../build-logic/src/test/kotlin/) |
| 05:32–06:20 정원 가꾸기, 가드레일을 토대로 사용 | 구 연결 드라이버 제거(#397), 테스트 시나리오 정리 및 전역 static 통로 래퍼 제거(#404), 전송 방식 분리(#420)가 실제 정리 사례다. 유사한 NIO/Netty 코드의 공통화는 동일한 계약·소유권을 확인한 부분부터 검토할 만하다. 전송 API와 이벤트 루프는 각자 다른 진입점을 갖는다. [#397](https://github.com/ppzxc/strand/pull/397), [#404](https://github.com/ppzxc/strand/pull/404), [#420](https://github.com/ppzxc/strand/pull/420) |
| 06:56 Dune처럼 아키텍처와 프레임워크에 경험을 인코딩 | `Endpoint`와 전송 엔진의 구동 계약, 앱 공통 API, Gradle convention plugin이 이미 해당 역할을 한다. core에는 TCP·WebSocket 지식이 없어야 한다는 ADR과 이름·메시지 형태 검사가 있다. 이 검사는 일부 식별자와 시그니처를 보므로 전체 소유권이나 패키지 의존성의 증거로 확대 해석하기 어렵다. [core ADR](../adr/0006-core는-사용-환경을-모른다.md), [CoreKnowsNoUsageEnvironmentTest](../../strand-core/src/test/java/io/github/ppzxc/strand/core/internal/CoreKnowsNoUsageEnvironmentTest.java) |
| 07:24 `useEffect`처럼 오용하기 쉬운 기능을 제한 | React 금지는 UI 없는 Strand에 해당하지 않는다. 적용 후보는 확인된 동시성 오용의 표면을 좁히는 것이다. 기존 요청 준비·연결 소유권 테스트를 기준으로 새 임의 지연이나 이벤트 루프의 블로킹 대기가 문제를 숨기는 경우를 찾고, 전송 계층의 정해진 완료·스케줄링 경로로 유도한다. 제품의 프로토콜 기한, 종료 대기, 정당한 테스트 관측 지연은 용도를 보고 판단한다. [#403](https://github.com/ppzxc/strand/pull/403), [Netty ConnectionEngine](../../strand-netty/src/main/java/io/github/ppzxc/strand/netty/ConnectionEngine.java), [NIO StageCompletion](../../strand-nio/src/main/java/io/github/ppzxc/strand/nio/StageCompletion.java) |
| 08:06 Electron main/renderer처럼 디렉터리와 그래프로 경계를 강제 | 이름이 분명한 모듈과 resolve된 compile/runtime 의존성 허용 목록이 있다. Netty는 컴파일 단계에서 앱이 제공하는 Netty를 사용한다. 모듈 경계와 별개로 공개 패키지→내부 구현 방향, 허용된 core 구동 진입점, 소유 스레드에는 서로 다른 검사가 필요하다. `internal` 이름만으로 Java 패키지 접근이 강제되지는 않는다. [허용 목록 구현](../../build-logic/src/main/kotlin/VerifyDependencyAllowlist.kt), [core 내부 ADR](../adr/0003-core-구현은-internal-패키지에-둔다.md), [Netty 구동 구조](../diagrams/core-netty.html), [NIO 구동 구조](../diagrams/core-nio.html) |
| 09:18 정당화와 과도한 집착 | 구체적인 예외 사유와 실제 검사를 짝지을 수 있다. 예를 들어 벡터 제외 목록은 벡터가 해당 요구를 덮으면 오래된 제외 항목을 검사로 잡고, 일부 항목은 대체 테스트를 명시한다. 타이밍 관련 설명은 실제 이벤트나 실패 테스트와 대조한다. 저장소의 주석이 에이전트의 변명에서 나왔다는 출처 판단은 하지 않았다. [벡터 제외 목록](../../strand-conformance/src/test/resources/uncovered-requirements.txt), [SpecCoverageTest](../../strand-conformance/src/test/java/io/github/ppzxc/strand/conformance/SpecCoverageTest.java) |
| 10:42 / 11:59 주석 99%, 세 타이머와 pstack `no-comments` | 발표 사례의 `setTimeout`, `requestAnimationFrame`, `requestIdleCallback`은 Cursor에서 명령형·선언형 갱신의 충돌을 미뤘고, 주석으로 정당화된 복제가 GC와 지연으로 이어졌다. 해설의 65,824→1,227은 발표 사례의 수치다. Strand에서는 **우회 구현을 정당화하는 주석을 금지한다.** 예를 들어 핸들러 준비 신호를 기다려야 하는 경로에 임의 지연을 추가한 뒤 “이 정도면 준비된다”는 주석으로 남기는 코드는 준비 완료를 직접 관측하도록 고치고 회귀 테스트로 고정한다. 공개 API Javadoc, 전송 소유권·이벤트 순서 불변식, 외부 계약의 근거를 설명하는 주석은 허용한다. 리뷰에서는 주석과 코드가 함께 드러내는 우회 동작을 판단한다. 반복되는 위반을 구조적으로 판별할 수 있으면 해당 동작을 린트로 막고, 실행 순서에 따라 드러나는 위반은 회귀 테스트로 막는다. [주최 측 발표 정리](https://ai.engineer/talks/O_zcc6Xl12g-all-code-is-slop-except-mine#how-a-timer-workaround-became-a-pattern), [pstack](https://github.com/cursor/plugins/tree/main/pstack), [Javadoc 검사](../../build-logic/src/main/kotlin/strand.library-conventions.gradle.kts) |
| 11:18 / 12:06 근본 원인으로 이끄는 린트와 자율 병합 | Spotless, main 컴파일 경고 검사, Error Prone, NullAway/JSpecify, 의존성 허용 목록, Javadoc, JaCoCo 90% 라인·분기 기준, 호환성 검사를 가진다. 예컨대 POM 설명 오류는 고칠 파일과 속성을 알려준다. 이번 로컬 실행에서 네 라이브러리의 `japicmp`와 conformance의 `baselineVectorTest`가 모두 SKIPPED였다. NIO·Netty의 API 비교는 빌드에서 명시적으로 비활성화되어 있다. 새 제약의 실패 메시지에는 위반 지점, 예상 소유자·API, 수정 방향을 넣는다. 해설의 린트 7→167은 발표 사례의 증가 추이다. [라이브러리 convention](../../build-logic/src/main/kotlin/strand.library-conventions.gradle.kts), [API 호환성](../../build-logic/src/main/kotlin/strand.api-compat-conventions.gradle.kts), [NIO 설정](../../strand-nio/build.gradle.kts), [Netty 설정](../../strand-netty/build.gradle.kts) |

## 4. 검증 전략

| 발표 항목 | Strand 점검 |
|---|---|
| 12:37 검증 산출물 중심 리뷰 | 이미 JUnit XML·HTML, 적합성 경로별 결과, JMH, 부하 JSON·표, 상태 다이어그램이 있다. 일반 CI는 실패 때만 XML·HTML을 업로드한다. #442는 성공 증거와 공유 머신 측정의 한계를 본문에 남겼고, #443은 수치와 재현 명령을 이슈에 연결했다. 일관되게 재사용할 산출물은 변경된 요구사항 ID, 명령·환경, 예상→관측, 결과 경로, 실패한 순서다. UI 영상·스크린샷의 역할을 여기서는 그 자료가 맡는다. [CI](../../.github/workflows/ci.yml), [적합성 리포트](../../strand-conformance/src/test/java/io/github/ppzxc/strand/conformance/VectorConformanceTest.java), [#442](https://github.com/ppzxc/strand/pull/442), [#443](https://github.com/ppzxc/strand/pull/443) |
| 13:01 실행과 디버깅 | `check`, 적합성 러너, loopback TCP/TLS·WebSocket 테스트, Maven·Gradle 외부 소비자, 부하 하네스가 있다. Chrome DevTools 대신 적합성 trace, JUnit, JFR/async-profiler, 실제 협상 결과와 부하 JSON이 해당 역할을 한다. 원하는 실행 깊이를 선택할 수 있다. [적합성 빌드](../../strand-conformance/build.gradle.kts), [통합 검증](../../strand-integration-test/), [배포 검증](../../deploy-test/run.sh), [load CLI](../../strand-load/README.md) |
| 13:15 사용자 관점 Feature Map | 용어집·스펙·모듈 README와 다이어그램은 분산된 지도를 이룬다. 한 진입점에서 ‘연결하고 인증’, ‘보내고 응답·기한 확인’, ‘취소·재시도 판단’, ‘과부하·종료’, ‘TCP/TLS·WS 선택’, ‘라이브러리 소비’와 각 공개 API·테스트를 바로 연결하는 지도는 찾지 못했다. 루트 README에서 기존 문서와 실행 대상을 연결할 수 있다. [용어집](../../GLOSSARY.md), [NIO 사용법](../../strand-nio/README.md), [Netty 사용법](../../strand-netty/README.md), [예제](../../strand-examples/) |
| 13:35 CLI·드라이버 재사용 | `EndpointDriver`를 core·Netty 테스트와 적합성 러너에서 공유하며 실행 경로를 선택할 수 있다. `LoadCli`는 `run/matrix/table/stall/soak`를 제공한다. 반복 작업은 이 도구를 호출하면 된다. 스킬에서 사용자 의도에 맞는 명령과 증거를 찾는 짧은 안내는 비용 절감에 도움이 될 수 있으며 토큰 절감량은 측정하지 않았다. [EndpointDriver](../../strand-core/src/testFixtures/java/io/github/ppzxc/strand/core/internal/EndpointDriver.java), [적합성 빌드](../../strand-conformance/build.gradle.kts), [LoadCli](../../strand-load/README.md) |
| 14:18 TLA+ 상태 검증 | 규격과 상태도, close-races 벡터, 종료·결과 테스트가 있다. 추적 파일에서 TLA+ 모델은 확인되지 않았다. 첫 모델 후보는 ‘요청 쓰기 진행 × 취소 × 상대/로컬 종료 × 결과 통지’다. 결과 확정성이 쓰기 증거와 일치하고 최종 결과가 한 번만 전달되는지를 확인한다. 모델의 반례는 대응하는 Java 실행 순서로 재현할 수 있을 때 검증 가치가 커진다. [종료·재시도 규격](../specification/STRAND_SPECIFICATION.md), [종료 경쟁 벡터](../../strand-conformance/vectors/close-races.json), [결과 확정성 상태도](../diagrams/outcome-certainty-state.html), [#404](https://github.com/ppzxc/strand/pull/404) |
| 14:28 Lean 알고리즘 증명 | 추적 파일에서 Lean 증명은 확인되지 않았다. 발표의 가상화 떨림은 Strand의 대상이 아니다. 순수 알고리즘의 증명이 필요해지면 stream ID 단조성·범위 또는 포화 산술의 경계 같은 좁은 후보가 있다. 현재는 규격과 기존 경계 테스트에 근거해 비용을 판단할 단계다. [64비트 stream ID ADR](../adr/0005-64비트-stream-id.md), [포화 산술 테스트](../../strand-nio/src/test/java/io/github/ppzxc/strand/nio/SaturatingTest.java) |
| 14:49 / 16:43 정형 검증의 역할과 의도 확인 | 모델은 가능한 순서와 불변식의 의미를 리뷰할 수 있게 한다. 벡터·공개 API 통합 테스트는 구현에서 실제로 관측되는 결과를 확인한다. 종료 직전 어느 결과를 사용자에게 노출할지부터 합의해야 한다. 통과한 모델은 실제 소켓, 스레드 소유권, 자원 회수 검증과 함께 해석한다. [종료 순서 수정](https://github.com/ppzxc/strand/pull/405), [실제 전송 효과 검증](../../strand-nio/src/test/java/io/github/ppzxc/strand/nio/RealTransportOutcomeTest.java) |
| 15:02 / 15:15 속성 기반 테스트와 UI 퍼징 | Bombadil·UI Fuzz가 대상으로 하는 제품 UI는 없다. `EndpointStreamSplitTest`는 고정 seed 여러 개로 분할 입력과 한 번에 넣은 입력이 같음을 검사한다. `EndpointTicketOrderTest`와 `EndpointOutboxOrderTest`는 seed 시나리오의 결과를 golden으로 고정한다. `OwnRequestLedgerTest`는 난수로 상태 전이를 실행하고 별도의 기대 상태와 비교한다. 다음 후보는 기존 드라이버에서 제한된 **정상** 요청·취소·종료 이벤트 순서를 생성해 결과 1회 전달·조각 크기 독립성·종료 후 진행 여부를 검사하고 seed와 순서를 보존하는 것이다. 범용 생성·축소 프레임워크나 정기 퍼징 잡은 찾지 못했다. [분할 속성](../../strand-core/src/test/java/io/github/ppzxc/strand/core/internal/EndpointStreamSplitTest.java), [티켓 순서](../../strand-core/src/test/java/io/github/ppzxc/strand/core/internal/EndpointTicketOrderTest.java), [출력 순서](../../strand-core/src/test/java/io/github/ppzxc/strand/core/internal/EndpointOutboxOrderTest.java), [요청 원장 상태 검증](../../strand-core/src/test/java/io/github/ppzxc/strand/core/internal/OwnRequestLedgerTest.java) |
| 15:41 / 16:00 검증 토큰 투자 | Cindy Sridharan의 약 70%와 Tan의 높은 검증 토큰 비중은 사례다. Strand의 토큰 분포 자료는 확인하지 못했다. 검증 투자를 판단할 지표는 재현 없이 실패한 테스트, 수정 후 재발한 경합, PR 증거 재실행 시간, 오판을 줄인 정도다. CI 실패 리포트 보존과 #442의 실제 측정 경험을 출발점으로 삼을 수 있다. [발표 출처](https://ai.engineer/talks/O_zcc6Xl12g-all-code-is-slop-except-mine), [CI](../../.github/workflows/ci.yml), [#442](https://github.com/ppzxc/strand/pull/442) |
| 16:52 pstack `state-chart` | 연결 단계와 요청 결과 확정성 상태도, core/NIO/Netty 구조도가 이미 있다. 변경 PR에서 같은 상태 어휘를 쓰고 실제 테스트 전이와 연결하면 사용자 관점 검토에 활용할 수 있다. 현재 HTML 다이어그램과 코드의 동기화를 검사하는 워크플로는 찾지 못했다. [연결 상태도](../diagrams/core-connection-phase.html), [확정성 상태도](../diagrams/outcome-certainty-state.html), [#421](https://github.com/ppzxc/strand/pull/421) |

## 5. 안쪽 루프부터 바깥 루프까지

| 발표 항목 | Strand 점검 |
|---|---|
| 19:53 / 20:42 새 안티패턴부터 방지 | 의존성·NullAway·컴파일 검사에 이미 이 접근이 적용됐다. 다음 출발점은 최근 반복된 요청 준비·종료 중 한 사례를 골라 기존 회귀 테스트가 문제 변경에서 실패하는지 확인하고, 검출하지 못하는 순서를 재현 가능한 테스트로 덮는 것이다. 구조적 위반은 작고 검증 가능한 검사로 만든다. [#403](https://github.com/ppzxc/strand/pull/403), [#404](https://github.com/ppzxc/strand/pull/404), [빌드 검사 테스트](../../build-logic/src/test/kotlin/) |
| 19:53 / 20:42 기존 부채 정리 | 검사가 생긴 뒤 같은 패턴의 과거 사례를 정리한다. 대기·스케줄링 코드는 쓰기 진행·이벤트 루프·프로토콜 기한의 의미를 확인해 정리 대상을 좁힌다. 문서 쪽에서 당장 확인된 부채는 설치·실행 경로를 안내하지 못하는 루트 README다. [README](../../README.md), [#404](https://github.com/ppzxc/strand/pull/404) |
| 20:04 / 20:11 검증을 탄탄히, 안쪽 루프 선행 | 코드 변경→관련 벡터·테스트→실제 전송 증거→CI의 경로가 있다. 속도를 높이려면 기존 JUnit·벡터 결과를 읽기 쉬운 짧은 증거로 먼저 연결하고 실제 전송·부하 증거는 변경 범위에 맞춰 선택한다. 간헐 실패가 재시도로만 해결되는 사례와 측정 환경의 영향을 검증 비용으로 본다. [CI](../../.github/workflows/ci.yml), [#357](https://github.com/ppzxc/strand/pull/357), [부하 비교](transport-load-comparison.md) |
| 20:08 / 20:54 검증 위에 이벤트 트리거 확장 | PR, 태그, Dependabot, CodeQL·Scorecard 워크플로가 있다. 태그 릴리스는 CI→Central VALIDATED→해당 배포 ID에서 소비자 검증→서명·SBOM→GitHub draft 순서이며 공개는 사람이 한다. 발표의 커넥터·루틴·프로젝트처럼 이슈/피드백/Slack/알림이 에이전트 업무를 시작하거나 결과를 되돌려주는 설정은 추적된 워크플로에서 확인되지 않았다. 외부 연결 여부는 확인 범위 밖이다. [워크플로](../../.github/workflows/), [배포 의존성 업데이트](../../.github/dependabot.yml), [릴리스](../../.github/workflows/release.yml) |
| 21:16 릴리스마다 새 빌드 검증→수정 PR→병합→보고 | 현재 Strand의 대응 단계는 실제 업로드한 빌드의 소비자 검증이다. 원본 발표는 릴리스 트리거, Cursor 프로젝트, 사용자 방식 실행, 수정 PR·자동 병합을 확인해 준다. Slack 보고는 사용자 제공 해설의 예시로 포함했다. 릴리스별 순서 탐색·자동 수정 PR·자동 병합·Slack 보고는 로컬 설정에서 확인되지 않았다. GitHub 설정상 `allow_auto_merge=false`이며 활성 main ruleset에는 PR과 `ci / ci-ok`가 필수다. 기본 승인 수는 0, 최신 base의 엄격 확인 옵션은 꺼져 있다. GitHub 기본 자동 병합 설정만으로 실제 병합 주체까지 판단할 수는 없다. 검증 산출물의 일관성이 확보된 뒤 격리된 빌드 검증과 보고부터 바깥 루프를 확장할 수 있다. [릴리스](../../.github/workflows/release.yml), [저장소 설정 API](https://api.github.com/repos/ppzxc/strand), [main 적용 규칙](https://api.github.com/repos/ppzxc/strand/rules/branches/main) |

## 6. 지금 시작하는 네 가지

1. **22:14 반복 교정을 실행 가능한 검사로 옮긴다.** 첫 후보는 #403의 첫 요청·핸들러 준비 순서나 #404의 동시 종료 중 하나다. 기존 테스트로 결함 커밋을 잡을 수 있는지, 실패 출력에 검증할 사건의 순서가 드러나는지 확인한다. 정적 검사와 런타임 회귀 테스트 중 위반을 직접 볼 수 있는 장치를 쓴다.
2. **22:22 새 유입을 막은 뒤 기존 예를 정리한다.** 최초 확인된 위반에 검사나 계약 테스트를 연결하고, 저장소 안에서 같은 동작을 하는 우회 코드를 찾아 검사 결과와 함께 고친다. 문서의 오래된 셋업 안내도 실제 모듈·예제 링크로 연결할 수 있다.
3. **22:25 에이전트에 실행 경로를 주고 리뷰어에 관측 결과를 준다.** 상태 관련 PR 하나에 규격 ID, 실행 경로, 명령, 이벤트 순서, 예상·실제 결과를 붙인다. 기존 벡터 리포트와 Gradle 테스트 결과부터 사용한다. 성능 관련 PR은 변경 전후 같은 조건의 수치와 JDK·환경·측정 반복 수를 남긴다.
4. **22:34 피라미드를 위에서부터 적용한다.** API·모듈 경계, 실행 검사, 검증 증거의 순서로 투자한다. 99% 주석 감축, 167개 린트, 70% 토큰, 0.5% 리버트는 발표 사례다. 편집 관행·정형 모델·병합 권한을 바꿀 판단은 Strand에서 재현 가능한 증거를 쌓은 뒤 내린다.

## 성과를 판단하는 기준

Tan은 환경에 약 6개월 투자한 뒤 발표 전 한 달에 개인 약 3천 PR, 팀 약 1만 1천 PR, 10월 약 227명 기여, 안정 릴리스 하루 약 한 번, PR 리버트 비율 약 0.5%를 보고했다. 여기에 시작, 타이핑, 채팅 전환 같은 사용자 동작의 개선을 함께 제시했다. 리버트는 후속 수정으로 해결한 결함을 세지 않는다. Strand에서는 PR 처리량을 장기 연결 실패, 결과 확정성 회귀, 재수정·간헐 실패, Java/API 호환성, 실제 전송 p99·자원 사용과 함께 보는 것이 이 방법론에 대응한다. [발표 결과와 슬라이드](https://ai.engineer/talks/O_zcc6Xl12g-all-code-is-slop-except-mine), [Strand 부하 비교의 측정 한계](transport-load-comparison.md)

## 이번 점검에서 재현한 결과

- 로컬 JDK 25.0.4.1에서 `./gradlew check --console=plain`이 3분 47초에 통과했다. 83개 실행 가능 태스크 중 36개 실행, 39개 캐시 복원, 8개 최신 상태였다. JDK 8·11·17·21 매트릭스, Bats, 배포 하네스는 이번 로컬 명령에 포함되지 않았다. #443의 PR에서는 해당 매트릭스와 `ci / verify`가 모두 성공했다. [#443](https://github.com/ppzxc/strand/pull/443), [CI](../../.github/workflows/ci.yml)
- 로컬 적합성 통합 경로는 **304개 통과, 실패 0, 미지원 0**이었다. 개별 경로는 core 300개, NIO 1개, Netty 19개를 통과시켰으며 일부 벡터는 경로 간 중복된다. 통합 결과는 각 벡터를 지원하는 경로에서 통과했음을 뜻한다. 세 전송 경로 각각에서 전체를 실행했다는 의미로 읽으면 테스트 범위를 과대평가하게 된다. 적합성 모듈 JUnit 집계는 759개 중 실패 0, 건너뜀 1이었다. [통합 결과 검사](../../strand-conformance/src/test/java/io/github/ppzxc/strand/conformance/VectorConformanceTest.java)
- `strand-load` 35개와 `build-logic` TestKit 49개가 실패 없이 완료됐다. 이전 릴리스와의 비교인 `japicmp` 및 `baselineVectorTest`는 SKIPPED였다. 현재 빌드와 소비자 계약의 검증 범위를 과거 릴리스 호환성 결과로 확대할 수 없다. [호환성 convention](../../build-logic/src/main/kotlin/strand.api-compat-conventions.gradle.kts), [벡터 비교 convention](../../build-logic/src/main/kotlin/strand.vector-compat-conventions.gradle.kts)
