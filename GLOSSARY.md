# strand 도메인 용어집 (Domain Glossary)

strand는 고성능 다중화 이진 메시지 교환 규격(Specification)과 그 규격을 구현한 Java 라이브러리다. 본 문서는 프로젝트 전반에서 통일되게 사용하는 어휘의 언어 규율을 규정한다.

규격 용어의 정의는 규격 1.3절이 정본이고, 1.3절에 없는 규격 항목은 해당 절이 정본이다. 이 문서는 정의를 다시 쓰지 않고 정본으로 링크하며, 피해야 할 표현(`_Avoid_`)만 정한다. 규격 문서 자체와 규격에 없는 모듈 용어(`core`, 전송 모듈, 어댑터)만 여기서 정의한다.

---

## 1. 규격 및 프로토콜 핵심 (Specification & Protocol Core)

**규격 (Specification)**:
strand가 정의하는 바이트 인코딩·디코딩 및 통신 상태 규칙을 담은 문서(`docs/specification/STRAND_SPECIFICATION.md`). 언어 중립적이며 Java 구현체는 여러 가능한 구현 중 하나다.
_Avoid_: 프로토콜 스펙, 와이어 프로토콜 스펙, 프로토콜 문서

**연결 서문 (Connection Preface)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 핸드셰이크 매직, 프리페이스(단독 비표준 표기), 매직 쿠키

**가닥 (Strand)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 스레드, 경량 스레드, 파이버

**스트림 (Stream)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 채널, 소켓 세션, 파이프

**엔드포인트 (Endpoint)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 피어(단독 표기), 노드(물리 장비와 혼동 주의)

**개시자 (Initiator)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 요청자(문맥 불명확), 호출자

**수신자 (Receiver)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 피요청자, 피호출자

---

## 2. 프레임 및 통신 계층 (Framing & Wire Elements)

**프레임 (Frame)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 패킷(L3/L4와 혼동 주의), 메시지(프레임과 페이로드를 구별할 것)

**WebSocket 메시지 (WebSocket Message) / WebSocket Close**:
정의: [규격 1.2절](docs/specification/STRAND_SPECIFICATION.md#12-공식-사용-환경)
_Avoid_: 메시지(단독 표기. 프레임과 구별할 것), WebSocket 프레임(RFC 6455의 프레임과 혼동 주의), 웹소켓 종료 프레임

**헤더 (Header)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 프레임 프리픽스, 메타 헤더

**제어 프레임 (Control Frame)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 시스템 프레임, 관리 프레임

**애플리케이션 프레임 (Application Frame)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 데이터 프레임(HTTP/2와 혼동 주의), 비즈니스 프레임

**확장 영역 (Extension Area)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 필수 확장, 헤더 옵션, 트레일러

**번호 공간 (Stream ID Space)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 개시자 비트, 일련번호, 방향 비트

**본문 (Payload)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 바디, 데이터

---

## 3. 수명주기 및 상태 관리 (Lifecycle & State Management)

**파라미터 협상 (HELLO)**:
정의: [규격 3.2절](docs/specification/STRAND_SPECIFICATION.md#32-hello-및-파라미터-협상)
_Avoid_: 핸드셰이크 프레임, 설정 프레임, SETTINGS

**생존 확인 (PING)**:
정의: [규격 3.4절](docs/specification/STRAND_SPECIFICATION.md#34-생존-확인-ping)
_Avoid_: 하트비트, 킵얼라이브

**연결 종료 (CLOSE)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 정상 종료, GOAWAY, 세션 종료

**종료 중인 연결 (Closing)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: draining, 유예, graceful shutdown

**스트림 중단 (RESET)**:
정의: [규격 4.4절](docs/specification/STRAND_SPECIFICATION.md#44-스트림-중단-reset)
_Avoid_: RST_STREAM, 취소 프레임

**기능 비트 (Capabilities Bitset)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 피처 플래그, 옵션 비트

**배압 (Backpressure)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 흐름 제어(Flow Control - 윈도우 기반과 구분), 스로틀링

**동시 스트림 수 (Concurrent Streams)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 미응답 바이트, 크레딧, 윈도우

**대기 중인 스트림 (Pending Stream)**:
정의: [규격 1.3절](docs/specification/STRAND_SPECIFICATION.md#13-용어)
_Avoid_: 유휴 스트림, 유예 상태, 진행 중인 스트림

---

## 4. 아키텍처 및 모듈 역할 (Architecture & Modules)

**core (코어)**:
규격을 구현한 모듈 (`strand-core`). 바이트 입출력(`byte[]`, `ByteBuffer`) 및 프레이밍, 상태 검사를 수행하며 런타임에 외부 의존성이 없고 스레드나 I/O 소켓 상태를 갖지 않는다.
_Avoid_: 코덱 모듈, 엔진

**전송 모듈 (Transport Module)**:
연결 수명주기와 스레드를 관리하며 `core`를 실제 네트워크 소켓 위에서 구동하는 모듈 (예: JDK NIO 기반 `strand-nio`).
_Avoid_: 트랜스포트 어댑터, 전송 어댑터

**어댑터 (Adapter)**:
`core`를 Netty, Protocol Buffers, gRPC 등 특정 외부 프레임워크나 라이브러리에 연결해 주는 브릿지 모듈 (예: `strand-netty`, `strand-protobuf`).
_Avoid_: 바인딩, 플러그인

**사용 환경 (Usage Environment)**:
정의: [규격 1.2절](docs/specification/STRAND_SPECIFICATION.md#12-공식-사용-환경). 모듈이 아니다
_Avoid_: WebSocket 어댑터, TCP 어댑터, WebSocket 바인딩

**적합성 벡터 (Conformance Vectors)**:
정의: [적합성 벡터 형식](docs/specification/STRAND_CONFORMANCE_VECTORS.md)
_Avoid_: conformance 벡터, 테스트 데이터, 픽스처, 골든 파일
