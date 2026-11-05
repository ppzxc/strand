# Strand 적합성 벡터 형식

* **문서 상태 (Status)**: 초안 (Draft)
* **벡터 형식 버전 (Format version)**: 1
* **작성일자 (Date)**: 2026-11-05
* **편집자 (Editor)**: 조정하 (@ppzxc)
* **저장소 (Repository)**: ppzxc/strand

---

이 문서는 규범이다. 와이어 규격(`STRAND_SPECIFICATION.md`)의 규범 참조이며, 벡터 형식 버전 1을 정의한다. 다른 언어로 구현하는 사람이 이 문서와 JSON Schema 파일(`conformance/schema/vector-format-1.schema.json`)만 보고 벡터를 쓰고 러너를 만들 수 있도록, 벡터의 의미를 여기서 정의한다. JSON Schema는 이 문서의 구조를 기계가 검사하게 만든 것이다. 둘이 어긋나면 이 문서가 우선한다. id 유일성, `next_stream_id`의 홀짝, `transport`와 단계 `kind`의 조합, 상대 HELLO의 ACK 전 `ping` 단계 금지(이 문서 8.1절 3번), 번호 할당 전제, 가상 시계처럼 스키마로 표현할 수 없는 규칙은 이 문서에만 있다.

이 문서에서 **러너**는 벡터를 읽어 테스트 대상 구현에 입력을 넣고 출력을 기대 결과와 비교하는 프로그램이고, **단계**는 시나리오의 한 항목이다. 두 말은 이 문서 안에서만 쓴다.

절 번호 표기: "이 문서 n절"은 이 문서의 절이고, 그냥 "n.n절"은 와이어 규격(`STRAND_SPECIFICATION.md`)의 절이다. 벡터의 `spec` 필드도 와이어 규격의 절만 가리킨다.

## 1. 모델

모든 벡터는 시나리오 하나다. 시나리오는 초기 상태(`setup`)와 단계(`steps`) 목록으로 이뤄진다. 프레임 하나를 디코딩해 보는 벡터도 단계가 하나뿐인 시나리오다. 그래서 러너와 구현의 접점도 하나다.

러너가 관측하는 것은 **구현이 송신한 와이어 프레임**과 **애플리케이션 경계 이벤트**, 두 가지뿐이다. 스트림 상태나 동시 스트림 수 같은 내부 상태는 직접 단언하지 않는다. 대신 다음처럼 간접 관측으로 확인한다.

| 확인하려는 내부 상태 | 간접 관측 |
| --- | --- |
| 끝남 (대기 중이 아닌 스트림) | 끝난 스트림으로 늦게 도착한 프레임이 조용히 버려지는지(`send: []`) |
| 대기 → 끝남 전이(동시 스트림 수 반환) | 전이 사건 직후 새 요청이 다시 수락되는지 |
| 거절된 요청이 동시 스트림 수에 들지 않음 | 거절 직후 경계에서 새 요청이 수락되는지 |

## 2. 파일 구조와 진화 규칙

벡터 파일의 최상위는 아래 두 필드다.

| 필드 | 의미 |
| --- | --- |
| `format` | 형식 버전. 정수. 이 문서는 `1`을 정의한다 |
| `vectors` | 벡터 배열 |

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "ping-is-acknowledged",
      "description": "PING을 받으면 같은 8바이트에 ACK를 붙여 돌려준다",
      "spec": ["3.4"],
      "setup": { "role": "server", "established": true },
      "steps": [
        {
          "kind": "recv",
          "hex": "00000014 00020000 00000000 00000000 0102030405060708",
          "send": [{ "type": "PING", "flags": ["ACK"], "payload": "0102030405060708" }],
          "events": []
        }
      ]
    }
  ]
}
```

진화 규칙은 다음과 같다.

1. 같은 `format` 안에서는 필드와 단계 `kind`를 추가만 할 수 있다. 추가하는 필드에는 기본값이 있어야 한다. 그러므로 기본값을 쓰는 옛 벡터는 새 로더가 계속 읽을 수 있다. 와이어 규격 0.1.0 확정 전의 형식 1에는 이 규칙을 적용하지 않는다.
2. 로더는 자기가 아는 것보다 높은 `format`과 모르는 필드를 거부한다(MUST).

## 3. 벡터의 공통 필드

| 필드 | 필수 | 의미 |
| --- | --- | --- |
| `id` | 예 | 같은 파일 안에서 유일한 kebab-case 이름(`[a-z0-9]+(-[a-z0-9]+)*`) |
| `description` | 예 | 무엇을 확인하는지 적은 한 문장 이상의 설명 |
| `spec` | 예 | 근거가 되는 규격 절 번호 문자열의 배열. 예: `["2.2.2", "5.1"]`. 가장 구체적인 하위 절 번호를 쓴다(예: `2.2`가 아니라 `2.2.2`). 비어 있으면 안 된다. 이슈 번호는 쓰지 않는다 |
| `setup` | 예 | 초기 상태(이 문서 4절) |
| `steps` | 예 | 단계 목록(이 문서 5절). 하나 이상 |

벡터 파일은 규격 영역별로 나눈다. `framing`, `handshake`, `streams`, `backpressure`, `extensions`, `close` 같은 이름을 쓴다. 테스트 이름은 `파일명/id`다(파일명에서 `.json`은 뺀다).

## 4. 초기 상태: `setup`

| 필드 | 필수 | 의미 | 기본값 |
| --- | --- | --- | --- |
| `role` | 예 | 테스트 대상 구현의 역할. `client` 또는 `server` | — |
| `local_hello` | 아니오 | 구현이 보내는 HELLO의 파라미터(3.2.1절의 이름) | 규격 기본값 |
| `peer_hello` | 아니오 | 러너가 상대 **엔드포인트**(`role`과 반대 역할)로서 보내는 HELLO의 파라미터 | 규격 기본값 |
| `established` | 예 | 연결을 수립한 상태에서 시작하는지 | — |
| `config` | 아니오 | 설정 항목(이 문서 8.2절) | 항목별 기본값 |
| `transport` | 아니오 | 전송 계층. `tcp` 또는 `websocket`. 바이트를 넣는 단계가 `tcp`에서는 `recv`, `websocket`에서는 `recv_message`로 갈린다(이 문서 5절) | `tcp` |
| `next_stream_id` | 아니오 | 구현이 자기 번호 공간에서 다음에 쓸 스트림 번호. 1 이상 2^31-1 이하이고, `role`이 `client`이면 홀수, `server`이면 짝수 | `role`이 `client`이면 1, `server`이면 2 |
| `known_types` | 아니오 | 구현이 아는 애플리케이션 `type`의 목록. 256 이상 정수의 배열이고 같은 값을 두 번 쓸 수 없다 | 256 이상의 모든 `type` |

`established`가 `true`이면 러너가 표준 수립 순서(3.3절)를 실행하고, 성공을 확인한 뒤 구현의 출력과 이벤트를 비운 다음 첫 단계를 시작한다. 표준 수립 순서에서 서버 역할 구현은 연결 서문을 받은 즉시 HELLO를 보내고, 클라이언트 역할 구현은 연결 서문에 이어 HELLO를 보낸다. 러너는 상대 HELLO에 `ACK`를 보내고 구현의 `ACK`를 받아야 수립을 마친 것으로 본다. `transport`가 `websocket`이면 연결 서문과 각 프레임을 바이너리 메시지 하나씩으로 주고받는다. 이때 `local_hello`와 `peer_hello`로는 수립이 성공하는 값만 쓴다. `false`이면 연결이 막 열린 맨 바이트 상태에서 시작한다. 연결 서문과 첫 프레임 검사를 확인하는 벡터가 이 쪽이다. 이 경우 구현이 연결이 열리자마자 내보내는 출력(클라이언트 역할의 연결 서문 등)은 첫 단계의 `send`에 합쳐 비교한다.

`peer_hello`는 필드 이름(식별자)이고, 가리키는 것은 용어집의 **엔드포인트** 중 테스트 대상이 아닌 쪽, 곧 러너가 흉내 내는 상대 엔드포인트가 보내는 HELLO다. `role`이 `server`이면 클라이언트 HELLO이고 `client`이면 서버 HELLO다. 반대로 `local_hello`는 테스트 대상 구현 자신의 HELLO다. 용어집은 "피어"의 단독 표기를 피하라고 하므로 이 문서의 설명문에서는 "상대 엔드포인트"라고 쓰고, `peer_hello`라는 이름만 형식 호환을 위해 유지한다.

"이미 열린 스트림" 같은 중간 상태는 선언하지 않는다. 필요하면 앞 단계에서 `request`나 `recv`로 만든다.

## 5. 단계

단계는 `kind`로 종류를 구분하며, 모든 단계에 기대 결과 `send`와 `events`(이 문서 6절)가 반드시 있다.

| `kind` | 필드 | 의미 |
| --- | --- | --- |
| `recv` | `hex` | `transport`가 `tcp`인 벡터에서만 쓴다. 구현에 바이트를 넣는다. 소문자 hex이고 공백·줄바꿈은 허용하며, 모두 지운 뒤 자릿수가 짝수여야 한다. 프레임 경계와 무관하게 아무 위치에서 잘라도 된다. 형식이 깨진 바이트도 쓸 수 있다 |
| `recv_message` | `opcode`, `hex` | `transport`가 `websocket`인 벡터에서만 쓴다. WebSocket 메시지 하나를 구현에 넣는다. `opcode`는 `binary` 또는 `text`이고, `hex`는 `recv`의 `hex`와 같은 표기다 |
| `ping` | `payload` | 애플리케이션이 이 `payload`로 PING을 보내게 한다. `payload`는 공백 없는 16자리 소문자 hex(8바이트)다 |
| `request` | `type`, `extensions`, `payload` | 애플리케이션이 요청을 보내게 한다. `type`은 256 이상 정수, `extensions`는 `{tag, value}` 배열(기본값 빈 배열), `payload`는 hex(기본값 빈 값) |
| `respond` | `stream_id`, `status`, `extensions`, `payload` | 애플리케이션이 받은 요청에 응답하게 한다. `status`는 2.5.1절의 이름 |
| `cancel` | `stream_id` | 애플리케이션이 자기가 보낸 요청을 취소하게 한다 |
| `close` | `error_code` | 애플리케이션이 연결을 닫게 한다. `error_code`는 2.5.2절의 이름 |
| `advance` | `ms` | 가상 시계를 `ms`밀리초 앞으로 보낸다(이 문서 8.3절). 1 이상 정수 |
| `disconnect` | — | 상대 엔드포인트가 CLOSE 없이 전송 계층을 끊게 한다(TCP EOF·RST, WebSocket close). 필드가 없다 |

`stream_id`는 10진수 정수로 쓴다(서버가 연 첫 스트림은 `2`).

## 6. 기대 결과

### 6.1 `send`: 송신 프레임

`send`는 단계가 실행되는 동안 구현이 보낸 프레임의 목록이다. 프레임은 필드 단위 구조로 쓰고, 아래 표의 규범 필드만 비교한다. 아무것도 보내지 않으면 `send: []`다. `transport`가 `websocket`이면 러너는 구현이 보낸 메시지마다 프레임이 정확히 하나인지 확인한다.

| 프레임 (`type`) | 비교하는 필드 | 비교하지 않는 필드 |
| --- | --- | --- |
| `PREFACE` | 8바이트 연결 서문 전체 | — |
| `HELLO` | `flags`(`ACK` 여부), `stream_id`(0), `params`(빠진 값은 규격 기본값으로 채워 비교. `ACK`이면 payload가 비어 있어야 함), 확장 영역(비어 있어야 함) | — |
| `PING` | `flags`(`ACK` 여부), `stream_id`(0), `payload`(공백 없는 16자리 hex), 확장 영역(비어 있어야 함) | — |
| `CLOSE` | `stream_id`(0), `error_code`, `flags`(0), 확장 영역(비어 있어야 함) | `reason` |
| `RESET` | `stream_id`, `error_code`, `flags`(0), 확장 영역(비어 있어야 함) | — |
| 애플리케이션 프레임 (`type` 256 이상 정수) | `type`, `flags`(0), `stream_id`, `status`(응답 프레임, 곧 `stream_id`가 상대 엔드포인트의 번호 공간인 프레임만. 생략하면 `OK`), `extensions`(tag별 값. 순서는 비교하지 않음), `payload`(생략하면 빈 값) | 요청 프레임의 `status` |

모든 프레임에서 `length`와 `reserved`는 비교하지 않는다. 비교 대상이지만 쓰지 않은 필드는 위 표의 기본값(빈 `flags`, 빈 확장 영역, 빈 `payload`)과 비교한다.

### 6.2 `events`: 애플리케이션 경계 이벤트

| `event` | 필드 | 뜻 |
| --- | --- | --- |
| `request_received` | `stream_id`, `type` | 상대가 연 스트림의 새 요청이 애플리케이션에 전달됨 |
| `result` | `stream_id`, `status` | 자기가 보낸 요청의 결과가 애플리케이션에 전달됨. 상대가 보낸 `status`인지 로컬에서 만든 `status`인지는 구분하지 않는다 |
| `closed` | — | 연결이 닫혔음이 애플리케이션에 전달됨 |

`status`는 2.5.1절의 이름이다. 17~255의 예약 값은 정수로 쓴다.

스트림을 열기 전에 로컬에서 실패한 요청(예: CLOSE를 보냈거나 받은 뒤의 새 요청, 3.5절)은 `stream_id`가 없으므로 `result` 이벤트가 아니다. 이런 단계의 `events`는 빈 목록이다.

## 7. 비교 규칙

1. 단계마다 `send` 목록과 `events` 목록은 각각 순서까지 포함해 완전히 일치해야 한다. 기대 목록에 없는 출력이나 이벤트가 있으면 실패다.
2. `send`와 `events` 두 목록 사이의 순서는 비교하지 않는다.
3. `send: []`는 구현이 아무것도 보내지 않았다는 뜻이다. 그러므로 조용한 폐기와 와이어 무응답을 검증할 수 있다.
4. 아무것도 보내지 않고 연결을 닫는 경우(예: 연결 서문 불일치)는 `send: []`와 `events: [{ "event": "closed" }]`로 쓴다.

## 8. 전제 조건과 설정

### 8.1 전제 조건

벡터를 실행하는 동안 테스트 대상 구현은 다음을 만족한다. 벡터는 이를 전제로 쓴다.

1. 구현은 자기 번호 공간에서 번호를 `next_stream_id`부터 연속으로 할당한다. 클라이언트는 1, 3, 5, ...(홀수)이고 서버는 2, 4, 6, ...(짝수)이다. 번호 건너뛰기를 허용하는 2.2.5절은 바뀌지 않는다. 이 전제 덕분에 벡터 작성자는 응답을 담은 `recv`의 `stream_id`를 미리 적을 수 있다.
2. `known_types`가 없으면 구현은 256 이상의 모든 `type`을 아는 것으로 다루고 payload를 해석하지 않은 채 전달한다. 있으면 목록에 없는 `type`은 모르는 것이다(4.1절, 5.1절 5번 (나)).
3. 상대 HELLO에 `ACK`를 보내기 전에 `ping` 단계를 쓰는 벡터는 없다.

### 8.2 설정 항목

적합한 구현은 적합성 실행 중에 아래 항목을 모두 설정할 수 있어야 한다(MUST). 건너뛰는 항목은 없다. 설정은 `setup`의 `local_hello`, `config`, `next_stream_id`, `known_types`로 한다. `local_hello`의 파라미터는 구현이 보내는 HELLO의 값으로 설정할 수 있어야 한다.

| 항목 | 위치 | 의미 | 기본값 |
| --- | --- | --- | --- |
| `max_frame_size`, `max_concurrent_streams`, `capabilities` | `local_hello` | 3.2.1절의 파라미터 | 3.2.1절의 기본값 |
| `ack_timeout_ms` | `config` | HELLO나 PING을 보내고 `ACK`를 기다리는 시간(확인 대기, 3.3절). 둘이 같은 값을 쓴다. 규격은 값을 정하지 않으므로 이 기본값은 적합성 실행의 설정일 뿐이다 | 5000 |
| `next_stream_id` | `setup` | 구현이 자기 번호 공간에서 다음에 쓸 스트림 번호 | `role`이 `client`이면 1, `server`이면 2 |
| `known_types` | `setup` | 구현이 아는 애플리케이션 `type`의 목록 | 256 이상의 모든 `type` |

### 8.3 가상 시계

시간은 `advance` 단계로만 흐른다(MUST). 구현은 시계를 주입받을 수 있어야 하며, 적합성 실행 중에는 실제 시간을 보지 않는다. 시계는 `setup` 시작 시점에 0이다. 표준 수립 순서와 단계 안의 처리는 시간이 걸리지 않는다. 시간 한도에 정확히 닿은 순간의 동작은 규격이 정하지 않았으므로, 벡터는 한도 직전과 한도를 지난 뒤만 쓴다.

## 9. 검증 범위

벡터는 MUST와 MUST NOT 동작만 검증한다. SHOULD 동작은 다루지 않는다. 예를 들어 `timeout` 초과 시 `DEADLINE_EXCEEDED` 응답은 벡터가 없다. 선택 동작 때문에 적합성 판정이 갈리지 않게 하려는 원칙이다.

payload 해석은 애플리케이션 계층의 몫이므로 벡터는 payload를 해석하는 동작(해석 실패에 대한 `INVALID_ARGUMENT` 응답 등)을 검증하지 않는다(4.1절).

## 10. 예시 (비규범)

아래 벡터는 형식을 보여 주는 예시다. 규격 본문이 요구하는 동작은 본문이 정하며, 이 예시는 그 일부를 어떻게 관측하는지를 보여 줄 뿐이다. 긴 반복 바이트는 줄여 쓰는 표기 `<00x16352>`(`00`을 16,352번 반복)로 나타냈다. 실제 벡터 파일에는 풀어서 쓴다.

### 10.1 동시 스트림 수의 경계, 거절 배제, 반환 후 재수락

`max_concurrent_streams`를 2로 두고, 서버 역할 구현이 상대의 요청을 받는다. 대기 중인 요청이 `n`개일 때 새 요청은 `n + 1`이 한도(2)와 같으면 수락하고 1 크면 거절한다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "concurrent-streams-boundary-refuse-and-return",
      "description": "n + 1이 max_concurrent_streams와 같으면 수락하고 1 크면 REFUSED로 거절한다. 거절된 요청은 동시 스트림 수에 들지 않고, 응답을 보내면 그 자리만큼 다시 수락한다",
      "spec": ["4.5", "5.1"],
      "setup": {
        "role": "server",
        "established": true,
        "local_hello": { "max_concurrent_streams": 2 }
      },
      "steps": [
        {
          "kind": "recv",
          "hex": "0000000c 01010000 00000001 00000000",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 1, "type": 257 }]
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010000 00000003 00000000",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 3, "type": 257 }]
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010000 00000005 00000000",
          "send": [{ "type": "RESET", "stream_id": 5, "error_code": "REFUSED" }],
          "events": []
        },
        {
          "kind": "respond",
          "stream_id": 1,
          "status": "OK",
          "send": [{ "type": 257, "stream_id": 1, "status": "OK" }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01010000 00000007 00000000",
          "send": [],
          "events": [{ "event": "request_received", "stream_id": 7, "type": 257 }]
        }
      ]
    }
  ]
}
```

두 번째 단계는 `n + 1`이 한도와 같은 경계 수락이고, 세 번째 단계는 1 큰 경계 거절이다. 네 번째 단계에서 응답으로 대기 중인 스트림이 하나 끝나 `n`이 줄었고, 다섯 번째 단계에서 새 요청이 다시 수락된다. 거절된 5번이 `n`에 들었다면 다섯 번째 단계도 거절됐을 것이므로, 이 단계는 거절한 요청이 세어지지 않음도 보인다.

### 10.2 응답 `type` 불일치

응답 `type`이 요청 `type`과 다르면 구현은 와이어로 아무것도 보내지 않고, 애플리케이션에만 로컬에서 만든 `INTERNAL` 결과를 넘긴다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "response-type-mismatch",
      "description": "응답 type이 요청 type과 다르면 아무것도 보내지 않고 애플리케이션에 INTERNAL을 넘긴다",
      "spec": ["4.1", "5.1"],
      "setup": { "role": "client", "established": true },
      "steps": [
        {
          "kind": "request",
          "type": 257,
          "send": [{ "type": 257, "stream_id": 1 }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 01020000 00000001 00000000",
          "send": [],
          "events": [{ "event": "result", "stream_id": 1, "status": "INTERNAL" }]
        }
      ]
    }
  ]
}
```

### 10.3 가상 시계와 확인 대기 시간 초과

서버 역할 구현은 연결 서문을 확인한 즉시 자기 HELLO를 보내고(클라이언트 HELLO를 기다리지 않는다), 클라이언트 HELLO를 받으면 `ACK`로 답한다. 이후 상대의 `ACK`가 오지 않는다. 대기 시간(1,000밀리초) 직전에는 아무것도 보내지 않고, 지나면 `CLOSE(PROTOCOL_ERROR)`를 보낸다.

```json
{
  "format": 1,
  "vectors": [
    {
      "id": "hello-ack-timeout",
      "description": "서버가 연결 서문을 확인한 즉시 HELLO를 보내고, 그 HELLO에 대한 ACK가 오지 않은 채 대기 시간이 지나면 CLOSE(PROTOCOL_ERROR)를 보내고 닫는다",
      "spec": ["3.3"],
      "setup": {
        "role": "server",
        "established": false,
        "config": { "ack_timeout_ms": 1000 }
      },
      "steps": [
        {
          "kind": "recv",
          "hex": "c0de5354 52300000",
          "send": [{ "type": "HELLO" }],
          "events": []
        },
        {
          "kind": "recv",
          "hex": "0000000c 00010000 00000000 00000000",
          "send": [{ "type": "HELLO", "flags": ["ACK"] }],
          "events": []
        },
        {
          "kind": "advance",
          "ms": 999,
          "send": [],
          "events": []
        },
        {
          "kind": "advance",
          "ms": 2,
          "send": [{ "type": "CLOSE", "error_code": "PROTOCOL_ERROR" }],
          "events": [{ "event": "closed" }]
        }
      ]
    }
  ]
}
```
