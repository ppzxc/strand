---
status: accepted
---

# Java 8 기준선 유지

2026년 기준으로 생태계는 Java 8을 떠나는 중이다(JUnit 6은 Java 17 바이트코드, javac 21/25는 `--release 8`에 폐기 경고, Google OSS 정책의 Java 8 보장은 최소 2026-09까지). 그래도 strand는 `examples`를 뺀 전 모듈을 `--release 8`로 유지하고, 대외적으로 Java 8 이상 지원을 선언한다. core는 런타임 외부 의존성이 0이라 생태계의 Java 8 이탈이 런타임에 닿지 않고, 영향은 빌드·테스트 도구에만 미치며 이는 toolchain(JDK 25)과 JUnit 5.14.x·AssertJ 3.x 고정으로 막는다. 첫 사용 환경(strand over WebSocket, strand over TCP) 중 strand over TCP 시스템은 Java 8에 남아 있을 수 있고, Oracle·Temurin·Corretto·Azul 모두 Java 8 런타임을 2030-12까지 지원한다.

## Considered Options

- Java 11 또는 17로 상향: 빌드는 단순해지지만 첫 목표가 겨냥한 레거시 사용자층을 잃는다.

## Consequences

다음 중 하나가 생기면 기준선을 다시 결정한다.

1. 고정한 컴파일 JDK의 후속 LTS에서 `--release 8`이 제거된다.
2. JUnit 5.x 유지보수가 끝난다.
3. 2030-12 벤더 지원 종료에 이른다.

근거: [G1 타깃·빌드 구조 재결정](https://github.com/ppzxc/strand/issues/9), [R1 Java 8 타깃·JDK 툴체인](https://github.com/ppzxc/strand/issues/2), [R8 테스트 스택의 Java 8 지원](https://github.com/ppzxc/strand/issues/14).
