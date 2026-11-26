# Strand

Strand는 서버 간 장기 연결에서 여러 요청과 응답을 한 연결로 다중화하는 Java 라이브러리입니다. 연결별로 요청을 동시에 처리하고, TCP 또는 WebSocket을 통해 통신합니다.

## Java 지원

라이브러리 모듈은 Java 8 이상을 지원합니다.

## 전송과 사용 안내

| 모듈 | 지원 전송 | 사용 안내 |
| --- | --- | --- |
| `strand-nio` | TCP, TLS | [NIO 안내](strand-nio/README.md) |
| `strand-netty` | TCP, WebSocket | [Netty 안내](strand-netty/README.md) |

Netty 모듈의 의존성 및 전송 선택 방법은 Netty 안내를 참고하세요. 실행 가능한 예제는 [`strand-examples`](strand-examples/src/main/java/io/github/ppzxc/strand/examples/)에서 볼 수 있습니다.

## 저장소 문서

- [AGENTS.md](AGENTS.md): 에이전트 작업 규칙
- [GLOSSARY.md](GLOSSARY.md): 도메인 용어
- [docs/adr/](docs/adr/): 아키텍처 결정 기록
- [docs/agents/](docs/agents/): 이슈 트래커, triage 라벨, 도메인 문서 규칙
