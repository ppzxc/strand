---
related: 153
label: wayfinder:research
checked: 2026-11-08
---

# 연결 인증 설계의 표준·업계 관행 교차 검증

확인일: 2026-11-08. 잠정 승인된 Strand 연결 인증 설계 8개 항목을 표준(RFC, OASIS)과 널리 쓰이는 프로토콜(Kafka, PostgreSQL, NATS, gRPC, FIX, SMPP)의 1차 문서와 하나씩 대조했다. 인용은 원문 그대로 옮기고 절 번호(필요하면 쪽수)를 달았다. 원문에 없는 해석은 **(추론)**으로 표시했다. 선행 조사 `research/finance-sms-protocol-reliability` 브랜치의 `docs/research/finance-sms-protocol-reliability.md`(이하 "선행 조사") 7번 항목(인증 위치)은 다시 다루지 않고, 이 문서는 그 위에서 설계 결정 단위로 검증한다. 관련 티켓은 #153(인증 위치)이다.

대조 대상인 현재 규격은 `docs/specification/STRAND_SPECIFICATION.md`(0.1.0 초안)이다. 특히 3.1~3.3절(연결 서문, HELLO, 수립 순서와 ACK 관문), 3.5절(CLOSE), 2.5.2절(error\_code), 6.1~6.2절(보안, 멱등 키)을 본다. 규격은 고치지 않았다.

## 검증 대상 설계 (잠정 승인안)

1. 연결 수립 단계에 프로토콜 수준 인증 관문을 둔다. 인증이 끝나기 전에는 어느 쪽도 PING, RESET, 애플리케이션 프레임(서버가 먼저 보내는 요청 포함)을 보내지도 받지도 않는다. 검증된 신원은 연결 수명 동안 연결에 묶이고, 멱등 키를 나누는 기준인 "인증된 상대"가 된다.
2. 담는 형식은 `mechanism` 이름(짧은 ASCII)과 불투명한 `credentials` 바이트열이다. 메커니즘 내용(익명, 비밀번호, 토큰)은 규격 밖이고, 받을 메커니즘은 서버가 정한다(SASL과 비슷).
3. 새 제어 프레임 `AUTH`로 한 번 왕복한다. 클라이언트는 ACK HELLO를 보낸 뒤 AUTH를 보내고, 서버는 성공하면 빈 `AUTH(flags=ACK)`, 실패하면 CLOSE로 답한다. 여러 단계의 challenge-response는 없다.
4. 프로토콜 안에서는 클라이언트만 인증한다. 서버는 채널(TLS 서버 인증서)이 인증한다.
5. 연결마다 한 번만 인증하고 재인증은 없다. 자격 증명이 만료되거나 취소되면 서버가 `CLOSE(UNAUTHENTICATED)`를 보낼 수 있고, 클라이언트는 새 연결로 옮긴다.
6. 실패는 새 error\_code `UNAUTHENTICATED`와 빈 reason을 담은 CLOSE로 알린다(조용히 끊지 않는다). 서버가 AUTH를 기다리는 시간은 로컬 타임아웃이고, 만료되면 같은 코드로 닫는다.
7. 채널 요건: 기밀성, 무결성, 서버 인증을 조건 없이 MUST로 요구한다(신뢰할 수 없는 네트워크에 한정하지 않음). 클라이언트 인증서는 선택이다. 각 구간이 이 성질을 만족하면 TLS를 종료하는 중간자도 허용한다. 기본값으로 TLS 1.3을 SHOULD로 둔다.
8. TLS ALPN 식별자를 정의하지 않는다. 8바이트 연결 서문 `STR0`이 이미 프로토콜을 식별한다고 본다.

## 출처와 신뢰도

| 약칭 | 문서 | 입수 경로 | 비고 |
|---|---|---|---|
| RFC4422 | RFC 4422, *Simple Authentication and Security Layer (SASL)*, 2006 | https://www.rfc-editor.org/rfc/rfc4422.txt | Standards Track. 부록 A가 EXTERNAL 메커니즘 정의(규범) |
| RFC4505 | RFC 4505, *Anonymous SASL Mechanism* | https://www.rfc-editor.org/rfc/rfc4505.txt | |
| RFC4616 | RFC 4616, *The PLAIN SASL Mechanism* | https://www.rfc-editor.org/rfc/rfc4616.txt | |
| RFC5056 | RFC 5056, *On the Use of Channel Bindings to Secure Channels* | https://www.rfc-editor.org/rfc/rfc5056.txt | |
| RFC5802 | RFC 5802, *SCRAM SASL and GSS-API Mechanisms* | https://www.rfc-editor.org/rfc/rfc5802.txt | RFC 9266이 갱신 |
| RFC7628 | RFC 7628, *A Set of SASL Mechanisms for OAuth* (OAUTHBEARER) | https://www.rfc-editor.org/rfc/rfc7628.txt | |
| RFC9266 | RFC 9266, *Channel Bindings for TLS 1.3* | https://www.rfc-editor.org/rfc/rfc9266.txt | `tls-exporter` 채널 바인딩 |
| RFC8446 | RFC 8446, *TLS 1.3* | https://www.rfc-editor.org/rfc/rfc8446.txt | |
| RFC9325 | RFC 9325 (BCP 195), *Recommendations for Secure Use of TLS and DTLS*, 2022 | https://www.rfc-editor.org/rfc/rfc9325.txt | |
| RFC7301 | RFC 7301, *TLS ALPN Extension* | https://www.rfc-editor.org/rfc/rfc7301.txt | |
| IANA-ALPN | IANA *TLS ALPN Protocol IDs* 레지스트리 | https://www.iana.org/assignments/tls-extensiontype-values/alpn-protocol-ids.csv | 2026-11-08 내려받음 |
| RFC9113 | RFC 9113, *HTTP/2* | https://www.rfc-editor.org/rfc/rfc9113.txt | |
| RFC6455 | RFC 6455, *The WebSocket Protocol* | https://www.rfc-editor.org/rfc/rfc6455.txt | |
| RFC9440 | RFC 9440, *Client-Cert HTTP Header Field*, 2023 | https://www.rfc-editor.org/rfc/rfc9440.txt | **Informational**(표준 아님) |
| MQTT5 | MQTT Version 5.0, OASIS Standard, 2019-03-07 | https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html | 규범 문장 번호 `[MQTT-x.y.z-n]`로 인용 |
| AMQP1-SEC | OASIS AMQP 1.0 Part 5: Security | http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html | |
| KIP-43 / KIP-152 / KIP-368 | Apache Kafka 개선 제안 | https://cwiki.apache.org/confluence/display/KAFKA/ (각 KIP 페이지) | KIP-368 상태 "Adopted (in 2.2)", KIP-152는 SaslAuthenticate 도입 |
| KAFKA-PROTO | Apache Kafka 4.1 문서, *Protocol* | https://kafka.apache.org/41/design/protocol/ | 페이지에 "older version (4.1)" 표기. 더 새 판이 있다 |
| PG18 | PostgreSQL 18 문서 54.2 *Message Flow*, 54.3 *SASL Authentication* | https://www.postgresql.org/docs/current/protocol-flow.html , .../sasl-authentication.html | `current`가 18을 가리킴(2026-11-08) |
| NATS-PROTO | NATS 문서, *Client Protocol* | https://docs.nats.io/reference/reference-protocols/nats-protocol | |
| NATS-SRV | nats-server `server/client.go` | https://github.com/nats-io/nats-server/blob/59bf235aea63881477dd485af3579be2ffc32b0c/server/client.go | 커밋 `59bf235`(2026-11-06) 기준 소스 |
| GRPC-AUTH | gRPC 문서, *Authentication* | https://grpc.io/docs/guides/auth/ | |
| FIXSL | FIX Session Layer Technical Specification, June 2020 | https://www.fixtrading.org/wp-content/uploads/download-manager-files/FIX_Session_Layer_June_2020.pdf | 선행 조사와 같은 판 |
| SMPP34 | SMPP v3.4, Issue 1.2 | https://www.world-text.com/docs/SMPPV3.4.pdf (미러) | 선행 조사와 같은 미러 |
| OWASP-AUTHN | OWASP *Authentication Cheat Sheet* | https://github.com/OWASP/CheatSheetSeries/blob/684d6d9d9f1a0c680ad30219f46ae4e2debf9440/cheatsheets/Authentication_Cheat_Sheet.md | 커밋 `684d6d9`(2026-10-04) |

## 결론: 항목별 판정

판정 기준. **표준 부합**: 같은 방식을 규범으로 정한 표준이나 널리 쓰이는 프로토콜이 있고 큰 반례가 없다. **부분 부합**: 선례는 있지만 같은 목적(장기 연결, SASL식 일반성 등)을 가진 주요 표준이 다른 길을 택했거나, 설계에 빠진 요소가 있다. **이탈**: 현행 표준 지침이 반대 방향을 권한다.

| # | 설계 항목 | 판정 | 핵심 근거 |
|---|---|---|---|
| 1 | 수립 단계 인증 관문, 서버발 트래픽까지 막음, 신원을 연결에 묶음 | **표준 부합** | MQTT `[MQTT-3.2.0-1]`, `[MQTT-3.1.2-30]`. SMPP 3.4 §2.3 표(`deliver_sm`, `enquire_link`는 BOUND 상태에서만). FIX §4.3.1, §4.3.10. AMQP Part 5 §5.3(SASL 계층이 AMQP `open`보다 먼저). Kafka 프로토콜 "SASL Authentication Sequence". PostgreSQL 54.2.1. RFC 4422 §2(신원은 서버가 결정) |
| 2 | `mechanism` 이름 + 불투명 `credentials`, 서버가 허용 메커니즘 결정 | **부분 부합** | 형식은 RFC 4422 §3.3, §4 3)a, AMQP `sasl-init`, Kafka `SaslHandshake`/`SaslAuthenticate`, MQTT Authentication Method/Data와 같다. 빠진 것: 메커니즘 공지 수단(RFC 4422 §4 2) SHOULD, AMQP §5.3.2 MUST), 이름 문법(RFC 4422 §3.1), 인가 신원 규정(§4 4)) |
| 3 | AUTH 한 번 왕복, challenge-response 없음 | **부분 부합** | 단일 메시지 메커니즘(PLAIN RFC 4616 §2, ANONYMOUS RFC 4505 §2, EXTERNAL RFC 4422 부록 A.1, OAUTHBEARER 성공 경로 RFC 7628 §3)과 단순 B2B 로그인(FIX Logon, SMPP bind, NATS CONNECT, MQTT 기본 인증)은 한 번 왕복이다. 그러나 SASL을 받는 프로토콜(AMQP, Kafka, PostgreSQL, MQTT 확장 인증)은 모두 여러 단계를 지원하고, SCRAM(RFC 5802 §5.2: client-first, 성공 시 추가 데이터)은 들어가지 못한다 |
| 4 | 프로토콜 안에서는 클라이언트만 인증, 서버는 TLS가 인증 | **표준 부합** | RFC 8446 §1("server side ... always authenticated; the client side is optionally authenticated"). MQTT §5.4.3. SMPP, FIX, Kafka, NATS, PostgreSQL 기본 흐름 모두 같다. 보강 수단으로 채널 바인딩(RFC 5802 §6, RFC 9266, PostgreSQL SCRAM-SHA-256-PLUS)이 있다 |
| 5 | 연결당 1회 인증, 재인증 없음, 만료·취소 시 서버가 CLOSE | **부분 부합** | 기본값으로는 RFC 4422 §3.8과 같고, NATS(만료 시 `-ERR 'User Authentication Expired'` 후 끊음), PostgreSQL, AMQP, FIX도 재인증이 없다. 다중화 프로토콜에서는 TLS 사후 인증을 피하라는 지침도 있다(RFC 9325 §3.6 → RFC 9113 §9.2.3). 그러나 장기 연결을 이유로 재인증을 넣은 Kafka KIP-368과 MQTT §4.12.1이 있고, KIP-368은 재인증 없이 서버가 끊기만 하는 방식을 명시적으로 기각했다 |
| 6 | `CLOSE(UNAUTHENTICATED)`, 빈 reason, 타임아웃도 같은 코드 | **표준 부합** (보완 필요) | 명시적 실패 신호: Kafka KIP-152, AMQP `sasl-outcome`, PostgreSQL ErrorResponse, NATS `-ERR 'Authorization Violation'`, MQTT 0x86/0x87(MAY). 이유를 숨김: RFC 4422 §3.6, OWASP. 반례: MQTT §3.1.4 비규범 주석과 FIX §4.6.4는 조용히 끊기를 권한다. 빠진 것: 인증 시스템 장애(일시적)와 자격 증명 거부(영구적)의 구분(AMQP `sys-temp`/`auth`, RFC 4422 §3.6) |
| 7 | 기밀성·무결성·서버 인증을 조건 없이 MUST, 중간자 허용, TLS 1.3 SHOULD | **표준 부합** (보완 필요) | FIX §4.3.1(TCP면 FIXS MUST), RFC 7628 §3("TLS MUST be used for OAUTHBEARER"), RFC 4616 §1, RFC 9325 §3.1.1, §5.1. MQTT, SMPP보다 엄격하다. 빠진 것: 호스트 이름 검증(RFC 9325 §7.1), 0-RTT 금지(RFC 9325 §3.10, RFC 8446 부록 E.5), TLS 사후 인증·재협상 금지(RFC 9325 §3.6), 평문 대체 금지(RFC 9325 §3.2). "TLS 1.3 SHOULD"는 BCP 195를 인용하는 편이 정확하다(1.2 MUST 지원) |
| 8 | ALPN 식별자 정의 안 함 | **이탈** | RFC 9325 §3.8 "Protocol developers are strongly encouraged to register an ALPN identifier ... This applies both to new protocols". HTTP/2는 서문이 있는데도 TLS 위에서는 ALPN이 MUST다(RFC 9113 §3.3, §10.2). PostgreSQL은 직접 TLS 모드에서 ALPN `postgresql`을 요구한다(PG18 54.2.10). MQTT, PostgreSQL, IMAP, XMPP가 IANA에 등록돼 있다 |

---

## 1. 수립 단계 인증 관문

### 1.1 표준과 업계 관행

**인증 전에는 상대가 먼저 보내는 트래픽도 막는다.**

- MQTT 5.0 §3.2: "The Server MUST send a CONNACK with a 0x00 (Success) Reason Code before sending any Packet other than AUTH [MQTT-3.2.0-1]." 서버발 PUBLISH, PINGRESP 등은 인증 성공 전에 나갈 수 없다.
- MQTT 5.0 §3.1.2.11.9: "If a Client sets an Authentication Method in the CONNECT, the Client MUST NOT send any packets other than AUTH or DISCONNECT packets until it has received a CONNACK packet [MQTT-3.1.2-30]." 확장 인증을 쓰면 클라이언트도 PINGREQ를 포함해 아무것도 미리 보내지 못한다.
- 같은 규격 §3.1.4는 확장 인증이 없을 때 클라이언트가 CONNACK 전에 보내는 것을 허용하되, 거절되면 서버가 처리하지 않게 한다: "If the Server rejects the CONNECT, it MUST NOT process any data sent by the Client after the CONNECT packet except AUTH packets [MQTT-3.1.4-6]."
- SMPP 3.4 §2.3 표(17~18쪽): `deliver_sm`(SMSC가 ESME로 보내는 요청)의 "Required SMPP Session State"는 `BOUND_RX`, `BOUND_TRX`뿐이고, `enquire_link`도 `BOUND_TX`, `BOUND_RX`, `BOUND_TRX`에서만 쓴다. bind 전(`OPEN`)에 쓸 수 있는 PDU는 bind 계열과 `outbind`뿐이다. 즉 서버발 요청과 생존 확인이 모두 인증 뒤다.
- FIX §4.3.1: "The Logon(35=A) request message must always be the first message transmitted over a FIX connection. If the acceptor receives anything other than a valid Logon(35=A) request message, an error should be logged and the transport layer connection terminated without Logout(35=5) processing." §4.3.10: "The initiator should not transmit any application message until the the Logon(35=A) acknowledgement has been received from the acceptor".
- AMQP 1.0 Part 5 §5.3 그림 5.4: SASL 헤더 교환과 SASL 협상이 끝난 뒤에야 AMQP 헤더와 `open`이 오간다. `sasl-outcome` 정의(§5.3.3.5): "Upon successful completion of the SASL dialog the security layer has been established, and the peers MUST exchange protocol headers to either start a nested security layer, or to establish the AMQP connection."
- Kafka 프로토콜 문서 "SASL Authentication Sequence": "If authentication succeeds, subsequent packets are handled as Kafka API requests. Otherwise, the client connection is closed."
- PostgreSQL 18 §54.2.1: 인증 주기가 "rejecting the connection attempt (ErrorResponse), or sending AuthenticationOk"로 끝나고, 그 뒤 ReadyForQuery에서야 "Start-up is completed. The frontend can now issue commands."

**인증 전에 허용되는 예외는 "프로토콜 협상"뿐이다.** Kafka는 인증 전에 `ApiVersionsRequest`를 받는다: "a broker returns its full list of supported ApiKeys and versions regardless of current authentication state (e.g., before SASL authentication on an SASL listener ...)"(KAFKA-PROTO, Retrieving Supported API versions). NATS는 서버가 접속 직후 `INFO`를 먼저 보낸다(NATS-PROTO, INFO). Strand의 HELLO가 AUTH보다 먼저 오가는 구조는 이 예외와 같은 모양이다.

**신원은 서버가 정하고 연결에 묶인다.** RFC 4422 §2: "The server is responsible for verifying the client's credentials and verifying that the identity it associates with the client's credentials (e.g., the authentication identity) is allowed to act as the authorization identity." §3.4.1: 인가 신원 문자열이 없거나 비어 있으면 "the client is requesting to act as the identity the server associates with the client's credentials." MQTT §5.4.2도 인증 성공 뒤 인가를 확인하고, 특히 "the implementation should check that the Client is authorized to use the Client Identifier as this gives access to the MQTT Session State"라고 적는다. 신원이 세션 상태의 열쇠가 된다는 점에서 Strand가 멱등 키를 인증된 상대별로 나누는 것과 같은 구조다.

### 1.2 대안 비교: 채널만 쓰는 인증, 애플리케이션 payload 로그인

- 채널(mTLS)만으로 인증: MQTT §5.4.12.3 "TLS Client authentication can be used in addition to – or in place of – MQTT Client authentication". 허용은 되지만 MQTT도 프로토콜 안 인증을 같이 둔다. OWASP-AUTHN "TLS Client Authentication"은 "if the client is behind an enterprise proxy that performs SSL/TLS decryption, this will break certificate authentication"이라고 적는다. 설계 7번이 TLS 종료 중간자를 허용하므로, 채널 인증만으로는 신원이 서버까지 오지 못할 수 있다. HTTP 쪽 우회책인 RFC 9440은 Informational 문서다.
- 애플리케이션 메시지에 로그인: MQTT §5.4.1 "An implementation might allow for authentication where the credentials are sent in an Application Message"라며 허용만 한다. gRPC는 이 길(호출 단위 자격 증명)을 택했다. "Call credentials, which are attached to a call"이고 "The result will send the authentication data associated with the composed CallCredentials with every call made on the channel"(GRPC-AUTH, Credential types). gRPC는 서버가 먼저 요청을 보내지 않고 연결 단위 멱등 범위도 없으므로 Strand와 전제가 다르다 **(추론)**.

### 1.3 판정과 위험

**표준 부합.** 서버발 요청과 PING까지 인증 전에 막는 것은 MQTT(`[MQTT-3.2.0-1]`, `[MQTT-3.1.2-30]`)와 SMPP 3.4 §2.3 표에 직접적인 선례가 있다.

위험과 차이:
- 현재 규격 3.3절의 송신 규칙은 "서버는 클라이언트 HELLO에 ACK를 보낸 직후부터 보낼 수 있다"이다. 인증 관문이 생기면 서버의 송신 시작점이 "AUTH(ACK) 송신 뒤"로 바뀌므로 3.3절 송신·수신 규칙과 수신 측 연결 오류 목록(5.1절)을 함께 고쳐야 한다 **(추론)**.
- 클라이언트가 AUTH와 첫 요청을 이어 보내는 파이프라이닝을 허용할지 정해야 한다. MQTT는 확장 인증을 쓸 때는 금지하고(`[MQTT-3.1.2-30]`), 기본 인증에서는 허용하되 거절 시 버린다(`[MQTT-3.1.4-6]`). 설계 1번은 전자와 같다.
- #153 메모처럼 "인증된 상대"가 반드시 있어야 멱등 범위가 선다. 그런데 설계 2번은 익명 메커니즘을 허용한다. RFC 4505 §5(Security Considerations): "The trace information is not authenticated, so it can be falsified." 익명 연결에서 멱등 범위를 무엇으로 할지는 규격이 정해야 한다 **(추론)**.

## 2. 담는 형식: `mechanism` + 불투명 `credentials`

### 2.1 표준과 업계 관행

- RFC 4422 §3.3: "The client sends a message that contains the name of the mechanism to the server." §4 3)a: 시작 메시지는 "MUST contain a field for carrying the name of the mechanism selected by the client", "SHOULD contain an optional field for carrying an initial response", 그 필드는 "MUST be capable of carrying arbitrary sequences of octets (including zero-length sequences and sequences containing zero-valued octets)."
- AMQP Part 5 §5.3.3.2 `sasl-init`: `mechanism`(symbol, 필수), `initial-response`("A block of opaque data passed to the security mechanism"), `hostname`.
- Kafka: `SaslHandshake Request => mechanism`, `SaslAuthenticate Request => auth_bytes`("The SASL authentication bytes from the client, as defined by the SASL mechanism")(KAFKA-PROTO).
- MQTT §3.1.2.11.9~10: Authentication Method(UTF-8 문자열)와 Authentication Data(Binary Data). §4.12 비규범 주석: "The Authentication Method is commonly a SASL mechanism, and using such a registered name aids interchange. However, the Authentication Method is not constrained to using registered SASL mechanisms."
- PostgreSQL 18 §54.3: "the server sends an AuthenticationSASL message. It includes a list of SASL authentication mechanisms that the server can accept ... The client selects one of the supported mechanisms from the list, and sends a SASLInitialResponse message".
- 메커니즘 중립성: RFC 4422 §4 끝 "In general, protocol specifications SHOULD be mechanism neutral."

**메커니즘 이름 문법.** RFC 4422 §3.1: "SASL mechanisms are named by character strings, from 1 to 20 characters in length, consisting of ASCII uppercase letters, digits, hyphens, and/or underscores." (`sasl-mech = 1*20mech-char`)

**메커니즘 공지.** RFC 4422 §3.2: "Commonly, a protocol will specify that the server advertises supported and available mechanisms to the client". §4 2): "A protocol SHOULD specify a facility through which the client may discover, both before initiation of the SASL exchange and after installing security layers negotiated by the exchange, the names of the SASL mechanisms that the server makes available to the client." AMQP §5.3.2는 MUST다: "The peer acting as the SASL server MUST announce supported authentication mechanisms using the sasl-mechanisms frame." Kafka는 실패할 때 목록을 돌려준다: "If the requested mechanism is not enabled in the server, the server responds with the list of supported mechanisms and closes the client connection." 반면 FIX §4.3.10은 "the authentication method chosen by counterparty agreement"로 사전 합의에 맡긴다.

**다운그레이드.** RFC 4422 §3.2: "the mechanism negotiation is not protected by the subsequent authentication exchange and hence is subject to downgrade attacks if not protected by other means." §6: "In some cases, it is appropriate to use data integrity protective services external to SASL (e.g., TLS) to protect against downgrade attacks in SASL." 설계 7번이 무결성을 MUST로 두므로 이 위험은 채널이 막는다 **(추론)**.

### 2.2 RFC 4422 §4 프로토콜 요건 대조

Strand가 "SASL 프로필"이라고 주장하려면 RFC 4422 §4가 요구하는 8가지를 모두 정해야 한다. 주장하지 않는다면 "SASL과 비슷한 형식일 뿐 SASL 프로필이 아니다"라고 밝히는 편이 오해를 막는다 **(추론)**.

| RFC 4422 §4 요건 | 설계 상태 |
|---|---|
| 1) GSSAPI 서비스 이름 | 없음 |
| 2) 메커니즘 협상 수단(SHOULD: 교환 전·후 목록 조회) | 없음. 서버가 정한다고만 함 |
| 3)a 시작 메시지: 메커니즘 이름 MUST, 초기 응답 SHOULD, 빈 응답과 응답 없음 구분 MUST | 이름과 `credentials`가 있음. 단일 왕복이라 "응답 없음"이 없으니 구분 문제는 생기지 않음 **(추론)** |
| 3)b challenge/response 메시지 | 없음(설계 3번) |
| 3)c 결과 메시지, 성공 시 추가 데이터 SHOULD | `AUTH(ACK)` 빈 payload. 추가 데이터 없음 |
| 4) 비어 있지 않은 인가 신원 문자열의 문법·정규화 MUST | 없음 |
| 5) 중단 수단 | CLOSE |
| 6) 보안 계층이 적용되는 위치 | 해당 없음(보안 계층 없는 메커니즘만 가정) |
| 7) TLS와 보안 계층의 적용 순서 MUST | 없음. "SASL 보안 계층은 쓰지 않는다"(§4 7)e에 해당)를 명시하면 됨 **(추론)** |
| 8) 다중 인증 지원 여부 | 연결당 1회(설계 5번) |

### 2.3 판정과 위험

**부분 부합.** 이름 + 불투명 바이트라는 형식 자체는 SASL, AMQP, Kafka, MQTT, PostgreSQL과 같다. 차이는 세 가지다.
- 메커니즘 공지 수단이 없다. B2B 사전 합의(FIX §4.3.10)로 충분하다고 볼 수 있으나, RFC 4422는 SHOULD, AMQP는 MUST이고 Kafka와 PostgreSQL도 목록을 알려 준다. 공지가 없으면 클라이언트는 잘못된 메커니즘으로 실패했는지, 자격 증명이 틀렸는지 구분할 수 없다(6번과 연결).
- 이름 문법을 "짧은 ASCII"로만 두면 `PLAIN`, `EXTERNAL`, `OAUTHBEARER`, `SCRAM-SHA-256` 같은 등록 이름과 대소문자·길이 규칙이 어긋날 수 있다. MQTT도 등록 이름 사용이 "aids interchange"라고 본다.
- "인증된 상대"가 인증 신원인지 인가 신원인지 정하지 않았다. 멱등 키 분할 기준은 클라이언트가 주장한 문자열이 아니라 서버가 정한 신원이어야 한다(RFC 4422 §2) **(추론)**.

## 3. AUTH 한 번 왕복

### 3.1 SASL이 정의하는 교환 길이

RFC 4422 §3은 challenge-response를 반드시 요구하지 않는다. 초기 응답 필드가 있으면 "the exchange is shortened by one round-trip": `C: Request authentication exchange + Initial response` / `<additional challenge/response messages>` / `S: Outcome`. 추가 challenge가 0번이면 한 번 왕복이다. §5 2)는 "SASL mechanisms SHOULD be designed to minimize the number of challenges and responses necessary to complete the exchange."

**한 번 왕복에 들어가는 메커니즘:**
- PLAIN, RFC 4616 §2: "The mechanism consists of a single message, a string of [UTF-8] encoded [Unicode] characters, from the client to the server."
- ANONYMOUS, RFC 4505 §2: "The mechanism consists of a single message from the client to the server."
- EXTERNAL, RFC 4422 부록 A.1: "There are no additional challenges and responses. Hence, the server is to return the outcome of the authentication exchange."
- OAUTHBEARER, RFC 7628 §3 성공 경로: "1. Client sends a valid and correct initial client response. 2. Server responds with a successful authentication."

**들어가지 않는 메커니즘과 경로:**
- SCRAM, RFC 5802 §5.2: "2a) SCRAM is a client-first mechanism. 2b) SCRAM sends additional data with success." 교환은 `client-first` → `server-first`(salt, nonce, 반복 횟수) → `client-final`(proof) → `server-final`(서버 서명)이다(§5 예시). 서버 challenge 한 번이 꼭 필요하고, 클라이언트가 서버를 확인하는 `server-final`은 성공 결과에 실려야 한다. 설계 3번은 challenge도 없고 `AUTH(ACK)`가 비어 있으므로 두 조건 모두 막힌다.
- Kerberos(GS2-KRB5)는 MQTT §4.12 비규범 예시에서 AUTH를 세 번 더 주고받는다.
- OAUTHBEARER 실패 경로, RFC 7628 §3: 실패 시 "the server sends an error result; the client MUST then send an additional message to the server in order to allow the server to finish the exchange". §3.2.2의 오류 결과는 `status`, `scope`, `openid-configuration`을 담은 JSON이다. 설계는 실패를 빈 reason CLOSE로 끝내므로 이 정보는 전달되지 않는다.

### 3.2 다른 프로토콜의 선택

| 프로토콜 | 교환 길이 | 근거 |
|---|---|---|
| AMQP 1.0 | 여러 단계(`sasl-challenge`/`sasl-response` "zero or more times") | Part 5 §5.3.2 그림 5.6 |
| Kafka | 여러 단계. v1부터 `SaslAuthenticate`로 감쌈 | KAFKA-PROTO "SASL Authentication Sequence" |
| PostgreSQL | SASL은 여러 단계, 그 밖의 방식은 "at most one request and one response" | PG18 §54.2.1 |
| MQTT 5.0 | 기본 인증(Username/Password)은 1회, 확장 인증은 AUTH 반복 | §4.12 |
| NATS | CONNECT 1회. 서버가 `INFO`에 `nonce`를 미리 주고 클라이언트가 CONNECT의 `sig`로 서명 | NATS-PROTO INFO·CONNECT 표 |
| FIX | Logon 1회 | FIXSL §4.3.10 |
| SMPP | bind 1회 | SMPP34 §4.1 |

PostgreSQL은 SCRAM을 이렇게 처리한다: "In the Initial Client response field, the message contains the SCRAM client-first-message ... Server sends an AuthenticationSASLContinue message ... Client sends a SASLResponse message ... Server sends an AuthenticationSASLFinal message, with the SCRAM server-final-message, followed immediately by an AuthenticationOk message."(PG18 §54.3.1)

**서버 nonce를 인사말에 미리 싣는 방식.** NATS는 서버가 먼저 보내는 `INFO`에 `nonce`를 넣고, 클라이언트는 "In case the server has responded with a nonce on INFO, then a NATS client must use this field to reply with the signed nonce"(CONNECT `sig`). 서버가 먼저 말하는 단계를 challenge로 써서 challenge-response 서명을 한 번 왕복에 넣는다. Strand도 서버 HELLO가 클라이언트 HELLO를 기다리지 않고 나가므로(3.2.2절 3번) 같은 자리에 nonce를 실을 수 있다 **(추론)**. 다만 현재 HELLO payload는 `(identifier u16, value u32)` 반복이라 임의 바이트열을 담을 수 없다(3.2절). 확장 영역(2.4절)은 제어 프레임에서 따로 정해야 한다(2.4.3절).

### 3.3 판정과 위험

**부분 부합.** 한 번 왕복은 단순 B2B 로그인 프로토콜(FIX, SMPP, NATS, MQTT 기본 인증)에서는 표준적이고, 등록 SASL 메커니즘 중 PLAIN, ANONYMOUS, EXTERNAL, OAUTHBEARER(성공 경로)를 담을 수 있다. 하지만 설계 2번이 "SASL과 비슷한" 일반 형식을 내세우는 만큼, SASL을 받는 주요 프로토콜(AMQP, Kafka, PostgreSQL, MQTT 확장 인증)이 모두 여러 단계를 지원한다는 점과 어긋난다.

위험:
- 비밀번호를 보내지 않는 비밀번호 인증(SCRAM)과 SCRAM-PLUS의 채널 바인딩을 쓸 수 없다. 비밀번호 기반이면 PLAIN처럼 비밀번호 원문이 서버(와 7번의 TLS 종료 중간자)까지 간다 **(추론)**.
- `AUTH(ACK)`의 payload를 "비어 있음"으로 고정하면, 나중에 성공 결과에 추가 데이터(RFC 4422 §3.6, SCRAM `server-final`, Kafka `session_lifetime_ms`)를 싣는 확장이 형태 위반이 된다. 지금 규격의 ACK HELLO도 "payload가 비어 있지 않음"을 연결 오류로 둔다(3.2.5절). 같은 규칙을 AUTH에 그대로 적용하면 확장 여지가 사라진다 **(추론)**.

## 4. 클라이언트만 프로토콜 안에서 인증

### 4.1 표준과 업계 관행

- RFC 8446 §1: "Authentication: The server side of the channel is always authenticated; the client side is optionally authenticated."
- MQTT §5.4.3: "The MQTT protocol is not trust symmetrical. When using basic authentication, there is no mechanism for the Client to authenticate the Server. ... Where TLS is used, TLS Certificates sent from the Server can be used by the Client to authenticate the Server."
- SMPP 5.0 §2.10(선행 조사 1.7): SSL/TLS로 "first establishing a secure and authenticated connection before commencing the SMPP session". FIX §4.3: "acceptance with optional authentication of the initiator by the acceptor". Kafka, NATS, PostgreSQL 기본 흐름도 클라이언트만 프로토콜 안에서 증명한다.
- 상호 인증을 프로토콜 안에서 하는 예외: SCRAM의 `server-final`(RFC 5802 §3 "the client authenticates the server by computing the ServerSignature"), MQTT §5.4.3 "Some forms of extended authentication do allow for mutual authentication."

### 4.2 채널과 프로토콜 인증의 결합(채널 바인딩)

RFC 5056 초록: "The concept of channel binding allows applications to establish that the two end-points of a secure channel at one network layer are the same as at a higher layer by binding authentication at the higher layer to the channel at the lower layer." RFC 9266은 TLS 1.3용 `tls-exporter`를 정의하고 "updates the default channel binding to the new binding for versions of TLS greater than 1.2". PostgreSQL은 SCRAM-SHA-256-PLUS에서 이것을 쓴다: "SCRAM with channel binding prevents such man-in-the-middle attacks by mixing the signature of the server's certificate into the transmitted password hash."(PG18 §54.3.1)

### 4.3 판정과 위험

**표준 부합.** 서버 인증을 TLS에 맡기고 클라이언트만 프로토콜 안에서 증명하는 구조는 RFC 8446의 기본 모델이고 MQTT, SMPP, FIX, Kafka, NATS, PostgreSQL의 기본 흐름과 같다.

위험:
- 서버 인증이 전적으로 TLS 인증서 검증에 달리므로, 호스트 이름 검증(RFC 9325 §7.1, 7번 참조)을 빼먹으면 bearer 토큰이나 비밀번호가 가짜 서버로 간다.
- 서버가 먼저 요청을 보내는 Strand에서, 클라이언트 쪽 멱등 범위의 "인증된 상대"는 서버의 TLS 신원이다. 7번처럼 중간자가 TLS를 종료하면 클라이언트가 확인하는 것은 중간자의 인증서다. 한 인증서 뒤에 서버가 여럿 있으면 클라이언트 입장에서 그 서버들이 하나의 "인증된 상대"로 합쳐진다 **(추론)**.
- 채널 바인딩은 TLS가 양 끝에서 끊기지 않을 때만 성립하므로 7번의 중간자 허용과 함께 쓸 수 없다 **(추론)**.

## 5. 연결당 1회 인증, 재인증 없음

### 5.1 1회 인증을 기본으로 하는 쪽

- RFC 4422 §3.8: "Unless explicitly permitted in the protocol (as stated in the protocol's technical specification), only one successful SASL authentication exchange may occur in a protocol session."
- NATS: 재인증 프로토콜이 없다. 서버는 사용자 JWT 만료 시각에 타이머를 걸고(`setExpiration`, NATS-SRV 1381행 부근), 만료되면 `c.sendErrAndDebug("User Authentication Expired")` 후 `c.closeConnection(AuthenticationExpired)`(NATS-SRV `authExpired`, 2579행 부근). 설계 5번과 같은 방식이다.
- PostgreSQL, AMQP 1.0, FIX: 인증은 수립 단계에서 한 번이다(PG18 §54.2.1, AMQP Part 5 §5.3, FIXSL §4.3.10). 이 문서들에서 재인증 절차는 찾지 못했다.
- 다중화 프로토콜과 TLS 사후 인증: RFC 9325 §3.6 "In the context of protocols that multiplex requests over a single connection (such as HTTP/2), post-handshake authentication has the same problems as TLS 1.2 renegotiation. Multiplexed protocols SHOULD follow the advice provided for HTTP/2 in Section 9.2.3 of [RFC9113]." RFC 9113 §9.2.3: "HTTP/2 servers MUST NOT send post-handshake TLS 1.3 CertificateRequest messages." 이는 TLS 계층 재인증에 대한 지침이지만, 진행 중인 여러 요청이 어느 신원에 속하는지가 흐려진다는 문제는 애플리케이션 계층 재인증에도 비슷하게 생긴다 **(추론)**.

### 5.2 장기 연결 때문에 재인증을 넣은 쪽

- Kafka KIP-368(Adopted in 2.2), Motivation: "Kafka connections are long-lived, so there is no ability to change the bearer token associated with a particular connection. Allowing SASL connections to periodically re-authenticate would resolve this." 보안상 동기도 든다. 계정을 끊으려면 "the current requirement is to remove all authorizations (i.e. remove all ACLs). This is necessary because of the long-lived nature of the connections."
- 설정 `connections.max.reauth.ms`: "The default value is 0, which means there is effectively no maximum communicated ..., server-side kill of expired connections is disabled". 양수로 두면 "the server will disconnect any SASL connection that does not re-authenticate and subsequently uses the connection for any purpose other than re-authentication at any point beyond the communicated expiration point". 유휴 연결은 끊지 않는다: "if a connection sits idle, it will not be closed – something unrelated to re-authentication must traverse the connection before a disconnect will occur."
- 만료 시각을 클라이언트에 알린다: `SaslAuthenticate Response (Version: 1) => error_code error_message auth_bytes session_lifetime_ms`, "session_lifetime_ms: Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur."(KAFKA-PROTO)
- **설계 5번과 같은 방식을 기각했다.** KIP-368 Rejected Alternatives, "Brute-Force Server-Side Kill": "We could kill the connection from the server side instead, when the token expires. But in this case, if there is no ability for the client to re-authenticate to avoid the killing of the connection in the first place, then we still have all of the harsh approach disadvantages mentioned above." 앞 항목에서 말한 단점은 "having connections pulled out from underneath the client will introduce latency while the client reconnects; it introduces non-trivial resource utilization on both the client and server as TLS is renegotiated; and it forces the client to periodically 'recover' from what essentially looks like a failure scenario." 또 "Delaying Support for Brokers Killing Connections" 항목에서 "re-authentication without this feature doesn't really improve security (because it can't be enforced)"라며 서버 측 강제 종료와 재인증을 함께 넣었다.
- MQTT 5.0 §4.12.1: 클라이언트가 CONNACK 뒤 언제든 `AUTH`(Reason Code 0x19)로 재인증할 수 있다. "During this re-authentication sequence, the flow of other packets between the Client and Server can continue using the previous authentication." 실패하면 "the Client or Server SHOULD send DISCONNECT with an appropriate Reason Code ..., and MUST close the Network Connection [MQTT-4.12.1-2]." 비규범 주석: 서버는 사용자 이름 변경 같은 재인증을 거절해 범위를 제한할 수 있다.

### 5.3 판정과 위험

**부분 부합.** "연결당 1회"는 SASL 기본값이고 NATS가 같은 방식(만료 시 오류를 보내고 끊음)을 운영한다. 그러나 Strand와 같은 목표(장기 연결)를 가진 Kafka와 MQTT는 재인증을 넣었고, Kafka는 "재인증 없는 서버 측 종료"를 명시적으로 기각했다.

Strand 고유의 위험(규격 3.5절과의 상호작용):
- `UNAUTHENTICATED`는 `NO_ERROR`가 아니므로 오류 CLOSE다. 규격 3.5절에 따르면 오류 CLOSE를 보내거나 받으면 "즉시 전송 계층을 닫는다(MUST). 대기 중인 스트림이 남아 있어도 기다리지 않는다." 대기 중인 내 요청은 모두 로컬 `UNAVAILABLE`이 된다. 자격 증명 만료라는 예정된 사건 때문에, 이미 처리 중인 비멱등 요청(송금, 문자 발송)의 결과가 알 수 없는 상태가 된다. 선행 조사 1번 항목(결과 확정)이 다룬 문제를 프로토콜이 스스로 만드는 셈이다 **(추론)**.
- Kafka는 두 가지로 이를 피한다. 만료 시각을 미리 알리고(`session_lifetime_ms`), 만료 뒤 연결이 *쓰일 때만* 끊는다. 설계에는 만료 시각을 알리는 수단이 없어 클라이언트가 미리 새 연결을 만들 수 없다.
- 반대로 취소(revocation)는 즉시 끊어야 하는 경우가 많다. KIP-368이 동기로 든 "계정을 디렉터리에서 비활성화해 접근을 끊기"가 이것이다. 만료와 취소를 같은 코드·같은 종료 방식으로 다룰 필요는 없다 **(추론)**.

## 6. 실패 신호: `CLOSE(UNAUTHENTICATED)`, 빈 reason

### 6.1 명시적 실패 코드를 보내는 쪽

- **Kafka KIP-152.** 이전 동작: "broker closes the client connection if SASL authentication fails without providing feedback to the client ... Producers and consumers retry, attempting to create successful connections, treating authentication failures as transient failures." 목표: "Authentication failures should be treated as non-retriable exceptions rather than transient failures." 변경: "In the case of authentication failure, a SaslAuthenticate response will be sent with error_code indicating authentication failure and empty sasl_auth_bytes before the connection is closed by the broker." 현재 오류 코드 `SASL_AUTHENTICATION_FAILED 58 ... SASL Authentication failed.`(KAFKA-PROTO). 조용한 종료가 실제 운영에서 일으킨 문제와 그 해결을 보여 주는 가장 직접적인 근거다.
- AMQP Part 5 §5.3.3.6 `sasl-code`: `ok 0`, `auth 1` "Connection authentication failed due to an unspecified problem with the supplied credentials", `sys 2`, `sys-perm 3` "unlikely to be corrected without intervention", `sys-temp 4` "due to a transient system error". 지원하지 않는 메커니즘은 "it MUST close the connection with the authentication-failure close-code"(§5.3.3.2).
- PostgreSQL: 실패 시 ErrorResponse, "The connection attempt has been rejected. The server then immediately closes the connection."(PG18 §54.2.1)
- NATS: `-ERR` 표에 "Authorization Violation: Client failed to authenticate to the server with credentials specified in the CONNECT message"와 "Authorization Timeout: Client took too long to authenticate to the server after establishing a connection (default 1 second)"가 있고 둘 다 복구 불가(No)다(NATS-PROTO). 서버 소스에서 타임아웃은 `c.sendErrAndDebug("Authentication Timeout")` 후 종료한다(NATS-SRV `authTimeout`, 2574행 부근). 실패와 타임아웃의 메시지는 다르지만 둘 다 "인증 실패, 복구 불가"로 분류된다.
- MQTT: CONNACK 이유 코드 `0x86 Bad User Name or Password`, `0x87 Not authorized`, `0x8C Bad authentication method`(§3.2.2.2 표 3-1). 다만 보내는 것은 MAY다(§3.1.4 "it MAY send an appropriate CONNACK response with a Reason Code of 0x80 or greater").
- SMPP 3.4 §5.1.3 표 5-2: `ESME_RBINDFAIL 0x0D Bind Failed`, `ESME_RINVPASWD 0x0E Invalid Password`, `ESME_RINVSYSID 0x0F Invalid System ID`(112~113쪽).
- FIX §4.3.10: "The session acceptor should send a Logout(35=5) message to reject FIX connection initiation and provide a reason for the rejection in the Text(58) field and then immediately terminate the transport layer connection." `SessionStatus(1409)`에는 `5 Invalid username or password`, `6 Account locked`, `8 Password expired` 등이 있다(§11.12, 81~82쪽).

### 6.2 정보를 숨기거나 조용히 끊는 쪽

- RFC 4422 §3.6: "It is also important that the server can be configured such that the outcome message will not distinguish between a valid user with invalid credentials and an invalid user."
- OWASP-AUTHN "Authentication Responses": "an application must respond with a generic error message regardless of whether: The user ID or password was incorrect. The account does not exist. The account is locked or disabled." "Error Codes and URLs"는 응답 코드 차이도 정보 누출이 된다고 경고한다.
- MQTT §3.1.4 비규범 주석: "If they fail, it is suggested that the Server does not send a CONNACK at all, as this could alert a potential attacker to the presence of the MQTT Server and encourage such an attacker to launch a denial of service or password-guessing attack."
- FIX §4.6.4: BeginString, SenderCompID, TargetCompID, IP 주소가 잘못된 Logon은 "immediately terminated by terminating the transport layer cnnection without sending a Logout(35=5) message ... This logon attempt could be an unauthorized access attempt. Terminating the connection without transmitting a Logout(35=5) message avoids divulging information about the SenderCompID(49)/TargetCompID(56) and the version of FIX being used".

### 6.3 일시적 실패와 영구적 실패의 구분

RFC 4422 §3.6: "The outcome message provided by the server can provide a way for the client to distinguish between errors that are best dealt with by re-prompting the user for her credentials, errors that are best dealt with by telling the user to try again later, and errors where the user must contact a system administrator for resolution". AMQP의 `auth`와 `sys-temp`, MQTT의 `0x87 Not authorized`와 `0x89 Server busy`, FIX의 `7 Logons are not allowed at this time`이 이 구분에 해당한다.

### 6.4 판정과 위험

**표준 부합(보완 필요).** "실패를 하나의 일반 코드로 알리고 이유는 숨긴다"는 Kafka(KIP-152), AMQP, PostgreSQL, NATS, SASL §3.6, OWASP를 합친 방향과 같다. 조용히 끊으라는 MQTT 주석과 FIX §4.6.4는 공개 인터넷에서 서버 존재 자체를 숨기려는 목적이다. Strand는 서문과 HELLO를 이미 주고받은 뒤에 AUTH를 보므로, 이 시점에서 조용히 끊어도 숨길 것이 거의 없다 **(추론)**.

위험:
- 인증 백엔드(LDAP, IdP, HSM)가 장애일 때도 `UNAUTHENTICATED`를 보내면, KIP-152 취지대로 이 코드를 "재시도 불가"로 다루는 클라이언트는 장애가 끝나도 다시 붙지 않는다. 반대로 재시도 가능으로 다루면 잘못된 자격 증명으로 무한히 재시도한다. 이 구분이 없다.
- 타임아웃에 같은 코드를 쓰는 것은 NATS("Authorization Timeout"도 복구 불가 인증 오류로 분류)와 맞는다. 다만 현재 규격의 확인 대기 초과는 `PROTOCOL_ERROR`(3.3절)라서, 같은 "기다리다 만료" 사건에 두 코드가 생긴다. 의도를 규격에 적어 둘 필요가 있다 **(추론)**.
- 규격 2.5.2절은 모르는 코드(6\~)를 `INTERNAL_ERROR`로 취급한다. 새 코드를 6으로 정의하면 구현 간 버전 차이가 있을 때 받는 쪽이 이를 내부 오류로 볼 수 있다. 0.1.0 초안 단계라 실제 위험은 작다 **(추론)**.

## 7. 채널 요건

### 7.1 표준과 업계 관행

**채널 보호를 무조건 요구하는 쪽.**
- FIX §4.3.1: "FIX implementations that use the TCP/IP protocol as the transport layer must use the FIX-over-TLS (FIXS) specification".
- RFC 7628 §3: "for the two mechanisms specified in this document, TLS MUST be used for OAUTHBEARER to protect the bearer token".
- RFC 4616 §1(PLAIN): "The PLAIN mechanism should not be used without adequate data security protection as this mechanism affords no integrity or confidentiality protections itself." 같은 절: "Specifications for IETF protocols that indicate that this mechanism is an applicable authentication mechanism MUST mandate that implementations support an strong data security service, such as TLS."
- gRPC: "In general this mechanism must be used as well as SSL/TLS on the channel ... most gRPC language implementations will not let you send credentials on an unencrypted channel."(GRPC-AUTH)
- RFC 9325 §5.1: "Typically, all communication between TLS clients and TLS servers requires all three of the above security services."(기밀성, 무결성, 인증)

**선택으로 두는 쪽.** MQTT §5.4.12는 "Clear communication profile"부터 보안 프로필을 나열한다. SMPP 5.0 §2.10은 권고만 한다(선행 조사). 현재 Strand 6.1절도 "신뢰할 수 없는 네트워크를 지나는 환경이라면 ... 써야 한다(SHOULD)"로 이쪽에 가깝다.

**TLS 버전.** RFC 9325 §3.1.1: "Implementations MUST support TLS 1.2", "Implementations SHOULD support TLS 1.3 and, if implemented, MUST prefer to negotiate TLS 1.3 over earlier versions of TLS." 1.0과 1.1은 MUST NOT. RFC 9113 §9.2: "Implementations of HTTP/2 MUST use TLS version 1.2 or higher ... The general TLS usage guidance in [TLSBCP] SHOULD be followed".

**설계에 빠진 TLS 세부 요건.**
- 호스트 이름 검증, RFC 9325 §7.1: "Host name validation is security-critical for all common TLS use cases. Without it, TLS ensures that the certificate is valid and guarantees possession of the private key but does not ensure that the connection terminates at the desired endpoint." AMQP §5.2: "The TLS client MUST validate the certificate presented by the TLS server."
- 0-RTT, RFC 9325 §3.10: "applications MUST avoid this feature unless an explicit specification exists for the application protocol in question to clarify when 0-RTT is appropriate and secure." RFC 8446 부록 E.5: 재생 가능한 0-RTT 데이터의 위험으로 "Duplication of actions which cause side effects (e.g., purchasing an item or transferring money)"를 든다. Strand의 대상 도메인(금융, 문자)과 직결된다.
- TLS 사후 인증·재협상, RFC 9325 §3.6(5.1절 인용). RFC 9113 §9.2.1은 TLS 1.2 재협상도 금지한다.
- 평문 대체 금지, RFC 9325 §3.2: "implementations MUST provide a way for administrators to set a strict local policy that forbids use of plaintext in the absence of a negotiated TLS channel".

**TLS 종료 중간자.** 표준 문서가 "구간마다 보호하면 된다"를 직접 규정한 예는 찾지 못했다. 관련 사실은 다음과 같다.
- OWASP-AUTHN: TLS를 복호화하는 프록시는 클라이언트 인증서 인증을 깨뜨린다(1.2절 인용).
- RFC 9440 초록: "HTTP extension header fields that allow a TLS terminating reverse proxy (TTRP) to convey the client certificate information of a mutually authenticated TLS connection to the origin server". Informational 문서다.
- AMQP §5.2, §5.3.3.2: SNI와 `sasl-init`의 `hostname`을 "AMQP proxies"가 백엔드 선택과 자격 증명 도메인 결정에 쓸 수 있다고 적는다. 프록시를 전제한 설계다.
- RFC 6455 §10.5: "The WebSocket server can use any client authentication mechanism available to a generic HTTP server, such as cookies, HTTP authentication, or TLS authentication." Strand의 1급 사용 환경인 WebSocket(규격 1.2절)에서는 HTTP 핸드셰이크 단계 인증과 그 앞단의 TLS 종료 프록시가 흔하다 **(추론)**.

### 7.2 판정과 위험

**표준 부합(보완 필요).** 조건 없는 MUST는 FIX, OAUTHBEARER, gRPC 구현 관행과 같고 MQTT, SMPP보다 엄격하다. 클라이언트 인증서를 선택으로 두는 것도 RFC 8446 §1, MQTT §5.4.12.3과 맞는다.

위험과 차이:
- "TLS 1.3 SHOULD"만 적으면 BCP 195의 나머지(1.2 지원 MUST, 1.0·1.1 금지, 1.3 우선 MUST)가 빠진다.
- 위 네 가지(호스트 이름 검증, 0-RTT, 사후 인증, 평문 대체)가 없다. 특히 0-RTT로 AUTH나 비멱등 요청이 실리면 재생 공격에 노출된다.
- 중간자를 허용하면 PLAIN·토큰 자격 증명이 중간자에서 평문으로 보이고(4.3절), EXTERNAL식 클라이언트 인증서 신원은 서버까지 오지 않는다. 역설적으로 이것이 프로토콜 안 AUTH가 필요한 이유이기도 하다 **(추론)**.

## 8. ALPN 식별자를 정의하지 않음

### 8.1 표준 지침

- RFC 9325 §3.8: "TLS implementations (both client- and server-side) MUST support the Application-Layer Protocol Negotiation (ALPN) extension. In order to prevent 'cross-protocol' attacks resulting from failure to ensure that a message intended for use in one protocol cannot be mistaken for a message for use in another protocol, servers are advised to strictly enforce the behavior prescribed in Section 3.2 of [RFC7301]". 이어서 "Protocol developers are strongly encouraged to register an ALPN identifier for their protocols. This applies both to new protocols and to well-established protocols".
- RFC 7301 §3.2: "In the event that the server supports no protocols that the client advertises, then the server SHALL respond with a fatal 'no_application_protocol' alert." 레지스트리는 "Expert Review" 정책이다(§6).
- **서문이 있어도 ALPN을 요구한 선례.** HTTP/2는 24바이트 연결 서문이 있지만 RFC 9113 §3.3: "HTTP/2 connections over TLS MUST use protocol negotiation in TLS [TLS-ALPN]." §3.4는 서문을 "a final confirmation of the protocol in use"라고 부른다. §10.2: "Completing a TLS handshake with an ALPN identifier for HTTP/2 can be considered sufficient protection against cross-protocol attacks. ALPN provides a positive indication that a server is willing to proceed with HTTP/2, which prevents attacks on other TLS-based protocols." 반면 평문 서문에 대해서는 "contains a string that is designed to confuse HTTP/1.1 servers, but no special protection is offered for other protocols."
- PostgreSQL 18 §54.2.10: 직접 TLS 모드에서 "the client is required to use the ALPN extension defined by RFC 7301 to protect against protocol confusion attacks. The PostgreSQL protocol is 'postgresql' as registered at IANA TLS ALPN Protocol IDs registry." PostgreSQL도 자체 시작 메시지가 있는 프로토콜이다.
- IANA-ALPN 레지스트리에 등록된 메시징·데이터베이스 프로토콜: `mqtt`(OASIS MQTT), `postgresql`, `imap`, `pop3`, `xmpp-client`, `xmpp-server`, `ftp`, `coap`, `sunrpc` 등.

**ALPN 없이 자체 헤더만 쓰는 쪽.** AMQP 1.0은 `"AMQP" %d2`/`%d3`/`%d0` 헤더로 계층을 구분하고 별도 TLS 포트(`amqps`, 5671)를 둔다(Part 5 §5.2, §5.2.1). Kafka, NATS, FIX, SMPP 문서에도 ALPN 식별자 규정이 없다. 이들은 모두 RFC 9325(2022) 이전에 설계됐다.

**WebSocket.** strand over WebSocket에서는 TLS ALPN이 HTTP의 것(`http/1.1`, `h2`)이므로 Strand 식별자를 ALPN에 둘 수 없다. 대응물은 WebSocket 하위 프로토콜(`Sec-WebSocket-Protocol`, RFC 6455 §1.9 "Subprotocols Using the WebSocket Protocol")이다.

### 8.2 판정과 위험

**이탈.** RFC 9325는 새 프로토콜에 ALPN 등록을 강하게 권하고, "서문이 이미 식별한다"는 근거는 HTTP/2와 PostgreSQL이라는 두 선례에서 받아들여지지 않았다. RFC 9113 §10.2가 밝히듯 서문은 TLS 핸드셰이크가 끝난 *뒤* 앱 데이터로 확인되므로, 인증서를 공유하는 다른 TLS 서비스로 연결이 넘어가는 교차 프로토콜 공격(ALPACA, RFC 9325 §3.7~3.8)을 핸드셰이크 단계에서 막지 못한다.

완화 요소 **(추론)**: Strand 클라이언트는 서버 HELLO를 검증하고 ACK를 보낸 뒤에만 AUTH를 보내므로, 자격 증명이 Strand가 아닌 서버로 새는 경로는 좁다. 반대 방향(다른 프로토콜 클라이언트가 Strand 서버에 붙음)은 서문 검사(3.1절)에서 조용히 끊긴다. 남는 위험은 다른 프로토콜의 서버를 향한 공격에 Strand 클라이언트가 이용되는 경우와, 같은 포트에서 여러 프로토콜을 받는 운영 환경에서 TLS 단계 라우팅이 불가능한 점이다.

---

## 결정에 주는 시사점

아래는 근거에서 나온 관찰이며 결정이 아니다. 번호는 설계 항목 번호와 맞췄다.

1. **(1, 5) 만료는 graceful 종료로, 수명은 미리 알린다.** `CLOSE(UNAUTHENTICATED)`는 오류 CLOSE라서 규격 3.5절에 따라 대기 중인 스트림을 버리고 즉시 닫는다. 자격 증명 *만료*처럼 예정된 사건에는 `NO_ERROR` CLOSE(종료 중 상태로 마저 처리)를 쓰고, 클라이언트가 새 연결을 먼저 연 뒤 옛 연결을 비우게(make-before-break) 하는 방안을 검토할 만하다. 그러려면 `AUTH(ACK)`에 남은 유효 시간을 실어야 한다(Kafka `session_lifetime_ms`). `UNAUTHENTICATED` 즉시 종료는 *취소*나 인증 실패에만 쓰는 구분이 가능하다.
2. **(5) 재인증을 넣지 않는다면 근거를 남긴다.** KIP-368과 MQTT §4.12.1은 장기 연결을 이유로 재인증을 넣었다. 넣지 않는다면 1번 시사점(graceful 회전)이 그 대체물이라는 점, 그리고 다중화 연결에서 신원이 바뀌는 문제(RFC 9325 §3.6)를 피한다는 점을 함께 적어 두는 편이 좋다. 넣는다면 MQTT처럼 "재인증 중에도 기존 신원으로 흐름 유지, 신원 변경 거절"이 참고 모델이다.
3. **(3) `AUTH(ACK)` payload를 확장 가능하게 둔다.** ACK HELLO처럼 "비어 있지 않으면 연결 오류"로 고정하면, 수명 정보(1번)나 SASL 추가 데이터(SCRAM `server-final`)를 나중에 실을 수 없다. 형식을 정의하되 이번 판에서는 빈 값만 쓰거나, 모르는 내용은 무시하는 규칙이 대안이다. 여러 단계 교환이 나중에 필요할 때를 위해 `flags`나 기능 비트(capabilities)에 자리를 남기는 것도 같은 맥락이다.
4. **(3, 4) 단일 왕복이 담는 범위를 명시한다.** PLAIN, ANONYMOUS, EXTERNAL, OAUTHBEARER(성공 경로)는 들어가고 SCRAM, GSSAPI, OAUTHBEARER 오류 상세는 들어가지 않는다고 적어 두면 구현자가 기대를 맞출 수 있다. NATS식 서버 nonce 서명을 한 번 왕복에 넣고 싶다면 서버 HELLO가 바이트열을 싣는 방법(확장 영역 등)을 정해야 한다 **(추론)**.
5. **(2) 메커니즘 이름 문법을 SASL과 맞춘다.** RFC 4422 §3.1(1\~20자, `A-Z0-9-_`)을 따르면 등록 SASL 이름을 그대로 쓸 수 있다.
6. **(2) 메커니즘 공지 여부를 정한다.** 사전 합의(FIX식)로 갈지, 서버가 목록을 알리게 할지(AMQP MUST, RFC 4422 SHOULD, Kafka는 실패 시 목록 반환) 정해야 한다. 공지하지 않는다면 "메커니즘은 사전 합의로 정한다"를 규격에 적는다.
7. **(2) SASL 프로필 여부를 밝힌다.** RFC 4422 §4의 8개 요건(2.2절 표)을 다 채울 생각이 없다면 "SASL과 비슷한 형식이지만 SASL 프로필은 아니다"를 적는 편이 오해를 막는다. 채운다면 최소한 인가 신원 규정(§4 4)), 보안 계층 미사용(§4 7)e)을 적어야 한다.
8. **(1, 6.2절 규격) "인증된 상대"의 정의를 고정한다.** 서버 측에서는 클라이언트가 주장한 문자열이 아니라 서버가 자격 증명으로부터 정한 신원(RFC 4422 §2)이어야 한다. 클라이언트 측(서버발 요청의 멱등 범위)에서는 서버의 TLS 신원인데, TLS 종료 중간자가 있으면 중간자의 신원이 된다는 점을 적어 둘 필요가 있다 **(추론)**.
9. **(1, 2) 익명 메커니즘과 멱등 범위의 관계를 정한다.** #153이 요구하는 "인증된 상대가 반드시 존재"와 익명 허용이 충돌한다. 익명 연결에도 서버가 분할 신원을 부여할지, 익명 연결에서는 멱등 키를 쓰지 않을지 정해야 한다 **(추론)**.
10. **(6) 일시적 실패를 다른 코드로 보낸다.** 인증 백엔드 장애는 `UNAUTHENTICATED`가 아니라 기존 `INTERNAL_ERROR`나 `OVERLOAD`로 보내고, `UNAUTHENTICATED`는 "재시도해도 소용없음"으로 정의하는 방안이 AMQP `auth`/`sys-temp`, RFC 4422 §3.6, KIP-152와 맞는다. 사용자 없음, 비밀번호 틀림, 계정 잠김은 구분하지 않는다(RFC 4422 §3.6, OWASP).
11. **(6) 타임아웃 코드의 의도를 적는다.** 확인 대기 초과는 `PROTOCOL_ERROR`, AUTH 대기 초과는 `UNAUTHENTICATED`인 이유를 적어 둔다. NATS도 인증 타임아웃을 인증 오류로 분류한다.
12. **(7) TLS 요건은 BCP 195를 인용하고 빠진 네 가지를 더한다.** "TLS 1.3 SHOULD" 대신 RFC 9325 준수를 요구하고, 호스트 이름(서버 신원) 검증 MUST(§7.1), 0-RTT 사용 금지(§3.10, RFC 8446 부록 E.5), TLS 사후 인증·재협상 금지(§3.6, RFC 9113 §9.2.1·§9.2.3), 평문 대체 금지(§3.2)를 명시할 만하다. 0-RTT는 금융·문자 도메인의 중복 실행 위험과 직결된다.
13. **(7) WebSocket 환경의 채널 요건을 따로 적는다.** `wss`와 TLS 종료 프록시가 흔하고, HTTP 핸드셰이크 단계 인증(RFC 6455 §10.5)과 Strand AUTH의 관계(둘 다 쓸지, AUTH만 신원으로 인정할지)가 정해져 있지 않다.
14. **(8) ALPN 식별자를 정의하고 WebSocket 하위 프로토콜 이름도 정한다.** RFC 9325 §3.8을 따라 TLS용 ALPN 식별자(예: 버전을 담은 `strand/0` 같은 형태, 이름은 미정)를 정하고, 서버는 ALPN이 오면 일치를 강제하며(RFC 7301 §3.2), WebSocket에서는 `Sec-WebSocket-Protocol` 토큰을 정하는 방안이다. IANA 등록은 Expert Review라 부담이 작다. 정의하지 않는다면 HTTP/2와 PostgreSQL 선례를 반박하는 근거가 필요하다.
15. **(4, 7) 채널 바인딩은 중간자 허용과 함께 쓸 수 없음을 인지한다.** SCRAM-PLUS나 `tls-exporter`(RFC 9266) 같은 결합은 TLS가 끝에서 끝까지 이어질 때만 성립한다. 중간자 허용을 유지하면 bearer 자격 증명이 중간자에서 보인다는 점을 보안 고려사항에 적어 둘 만하다.
