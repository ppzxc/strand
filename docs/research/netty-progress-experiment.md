# Netty 진행 경계 예비 실험

이 문서는 #315의 진행 중 기록이다. 아래 결과는 지원 조합 확정이나 티켓 수용 기준 통과를 뜻하지 않는다.

## 실행 환경과 절차

- Netty `netty-transport` 4.1.101.Final, `NioSocketChannel` 하위 클래스, 출력 handler 없는 pipeline.
- Linux aarch64 5.14.0-687.42.1.el9_8, Red Hat OpenJDK 25.0.4.1, JDK TLS 1.2.
- `./gradlew :strand-integration-test:test --tests '*NettyWriteBoundaryTest' --tests '*TlsBoundaryTest' --tests '*NettyOutboundBoundaryTest'`.
- 테스트에 포함한 localhost PKCS#12 키는 시험용으로만 사용한다.

## 관찰과 한계

- 읽지 않는 실제 loopback 상대와 제한된 소켓 버퍼에서 Java `SocketChannel.write`의 부분 및 0 반환을 관찰했고 별도 연결에서 전체 쓰기와 수신을 확인했다.
- JDK `SSLEngine.wrap`의 평문 소비량과 암호문 실제 소켓 쓰기량을 별도로 관찰하고 실제 TLS 서버가 평문을 수신했다. 읽기를 멈춘 TLS 상대에서는 평문 소비 후 암호문 부분·0 쓰기도 관찰했다. 별도의 교환에서 암호문을 실험용 `doWrite` 경로로 보내 TLS 서버 수신을 확인했다.
- 별도의 ByteBuf 전용 `doWrite` 실험 채널에서 0 쓰기 뒤 미시작 출력 철회, 부분 쓰기 뒤 출력 순서, 종료 후 버퍼 참조 해제를 관찰했다. 포화된 채널의 `close().sync()` 직후에는 버퍼가 아직 해제되지 않은 경우가 있었고, 이 실험에서 이벤트 루프에 후속 작업을 실행한 뒤 해제됐다. 이는 일반적인 정리 완료 보장이 아니다.
- 실험 채널은 생산용 직렬 구동, 임의 handler 호환을 검증하지 않았다. TLS 탐침은 handshake 동안 이벤트 루프에서 대기하므로 제품 구현이 아니다.
- Strand core의 출력 유효성, 실제 RESET 순서, TLS 출력 예약·철회, 모든 경합에서의 정리 완료와 ByteBuf 소유권은 확인하지 않았다. 이 항목을 통과하기 전에는 이 조합을 지원 후보로 확정하지 않는다.
