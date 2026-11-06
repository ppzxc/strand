---
status: proposed
---

# core 구현은 internal 패키지에 둔다

`strand-core`의 상태 기계 구현(`Endpoint`, `EndpointConfig`, `Event`, `OutboundMessage`, `MessageKind`, `Frames`)은 `io.github.ppzxc.strand.core.internal`에 둔다. 공개 패키지 `io.github.ppzxc.strand.core`에는 `StrandVersion`만 있다. 패키지 이름이 코드 밖에서는 드러나지 않는 약속이라 결정을 남긴다.

## 결정

- `internal`의 클래스는 공개 API가 아니다. 하위 호환을 약속하지 않는다.
- `strand-conformance`의 러너가 직접 호출해야 하므로 일부 클래스는 `public`이다. `public`이어도 약속 대상이 아니다.
- 호환성 검사(japicmp)는 `*.internal`과 `*.internal.*`을 제외한다(`strand.api-compat-conventions.gradle.kts`).
- `module-info.java`는 없다. 모듈 수준에서 `internal`을 막지 않고, 패키지 이름으로만 구분한다.

## 맥락

구현(#102~#112, PR #116)은 공개 API 결정(#113)보다 먼저 들어갔고, PR #116은 "공개 API는 아직 확정하지 않았다"고 적었다. 구현을 먼저 두고 공개 표면을 나중에 좁히는 순서라, 확정되지 않은 시그니처가 호환성 검사에 걸리지 않게 `internal`에 둔 것으로 보인다. 이 맥락은 PR 본문과 빌드 설정에서 추론한 것이다. 당시 결정 기록은 없다.

## Considered Options

- 처음부터 공개 패키지에 둔다: 1.0 이전에 시그니처를 바꿀 때마다 호환성 검사와 충돌한다.
- `module-info.java`로 `internal`을 `exports`에서 뺀다: 모듈패스 소비자에게는 더 강하게 숨길 수 있다. Java 8 기준(ADR 0001)에서는 쓸 수 없고, 적합성 러너가 같은 모듈 밖에서 접근해야 한다. 지금은 채택하지 않았다.

## 남은 일

공개 API(#113)가 `internal`의 어느 부분을 공개할지는 이 문서가 정하지 않는다. 승격하거나 `module-info`를 도입할 때 이 문서를 갱신한다.
