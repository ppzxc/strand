# Netty TCP·TLS의 정확한 쓰기 경계 구현 가능성

연구일: 2026-11-17. 대상: [Netty TCP·TLS의 정확한 쓰기 경계 구현 가능성](https://github.com/ppzxc/strand/issues/310). 제품 구현·실행 검증 없이 공식 소스와 규격을 대조했다. Netty 근거는 아래 고정 커밋의 4.1 개발 브랜치이며 출시 버전 보장을 뜻하지 않는다. 구현할 때 실제 선택한 릴리스에서 재확인해야 한다.

## 결론

일반 `ChannelFuture`와 outbound handler만으로 승인된 정확한 쓰기 경계를 충족한다고 약속할 수 없다. TCP에는 하위 쓰기를 통제하는 전용 채널 구현 후보가 있지만 검증된 완성 경로는 아니다. TLS에는 그 외에 **평문을 TLS에 넘긴 경계와 암호문을 소켓에 쓴 경계 중 어느 것이 규격의 전송 계층 전달인가**라는 미해결 의미가 있다. 이 연구는 규격이나 기존 결정을 바꾸지 않는다.

## 유지할 기준

[규격 3.5절과 4.4절](https://github.com/ppzxc/strand/blob/9f7eb037b00565f03709c5a688bbd84f98cb87e5/docs/specification/STRAND_SPECIFICATION.md)은 취소·기한 만료에서 첫 쓰기 전후, 연결이 끝날 때 마지막 바이트 전달 전후를 구별한다. [승인된 core 내부 구동 계약](https://github.com/ppzxc/strand/issues/304#issuecomment-6940710104)은 출력 수거·첫 쓰기·전체 전달을 구별하고, 0바이트를 쓰기 시작으로 치지 않으며, 유효성 확인→비차단 쓰기→진행 보고 사이에 다른 core 명령을 처리하지 않는다. [준비·종료 결정](https://github.com/ppzxc/strand/issues/305#issuecomment-6940908095)은 물리적 정리와 결과 보존의 완료를 구별한다.

## 확인된 Netty 동작

`ChannelOutboundBuffer.addFlush()`는 실제 소켓 쓰기 전에 Promise를 취소 불가능하게 만든다. 따라서 flush했지만 아직 0바이트인 항목을 Future 취소로 철회할 수 없다. `currentProgress()`는 현재 항목 진행량이고, `removeBytes(n)`는 진행과 읽기 위치를 갱신하며 완성 항목을 제거한다. `remove()`는 버퍼 해제와 Promise 성공을 동반한다. 이 구조는 일반 앱용 큐 API가 아니라 전송 구현용 내부 자료구조다. [출력 버퍼 소스](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/transport/src/main/java/io/netty/channel/ChannelOutboundBuffer.java#L143-L175), [진행·제거](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/transport/src/main/java/io/netty/channel/ChannelOutboundBuffer.java#L250-L410).

`NioSocketChannel.doWrite()`는 하나 또는 여러 ByteBuffer에 `SocketChannel.write()`를 호출한다. 양수 결과를 얻어야 `removeBytes`로 반영하고, 0이면 쓰기 가능 통지를 기다린다. 여러 항목을 한 번에 쓰는 경로가 있으므로 `doWriteBytes()`만 덮어써서는 모든 쓰기를 가로채지 못한다. [NIO 소켓 소스](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/transport/src/main/java/io/netty/channel/socket/nio/NioSocketChannel.java#L359-L448).

`SslHandler.write()`는 평문을 내부 대기열에 넣는다. `wrap()`은 평문을 묶거나 나누어 소비하고, 남은 평문을 다시 대기열에 넣으며 암호문 출력에 Promise를 연결한다. 그러므로 평문 write 접수·평문 소비·암호문 소켓 진행은 다른 사건이다. `wrapDataSize` 설정만으로 모든 경계 문제를 해결했다고 볼 수 없다. [SslHandler 소스](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/handler/src/main/java/io/netty/handler/ssl/SslHandler.java#L807-L940).

Java `SSLEngine`은 I/O를 수행하지 않으며 평문·네트워크 데이터 사이의 변환을 담당한다. 호출자가 네트워크 운송을 제공한다. 공개 API에 일반적인 wrap 롤백은 없다. 따라서 이미 소비·암호화한 요청을 소켓 쓰기 0이라는 이유로 제거한 뒤 같은 TLS 연결을 계속 써도 안전하다는 보장은 없다. 마지막 문장은 API 분리에서 도출한 설계 제약이며 특정 TLS 구현에서 불가능함을 증명한 것은 아니다. [Java 8 SSLEngine 공식 문서](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLEngine.html).

## 가능한 TCP 설계 후보와 제약

다음은 소스가 보여 주는 확장 지점에 근거한 **미구현 후보**다. Netty의 일반 사용자 파이프라인 연동보다 책임이 크다.

1. 지원 범위를 전용 NIO 채널과 허용된 파이프라인으로 제한한다. `doWrite`에서 실제 소켓 쓰기를 직접 제어한다. 임의 native transport·메시지 변환 handler 호환은 약속하지 않는다.
2. 출력 식별자와 프레임의 바이트 구간을 보존한다. 처음에는 한 프레임씩 쓰는 방식으로 gathering 경계 계산을 단순화할 수 있다. 배치한다면 반환 바이트 수를 각 프레임에 정확히 배분한다.
3. 0바이트 상태의 항목은 매 쓰기 시도 직전에 core 유효성을 재확인한다. 철회된 항목은 하위 쓰기에서 제외하고 자원·Promise를 일관되게 정리한다. 공개 `Future.cancel()`을 철회 수단으로 사용하지 않는다.
4. 양수 쓰기 반환 직후 core 진행을 보고하고, 그 다음 Promise 완료·앱에 보이는 사건을 발생시킨다. 완료 리스너 재진입도 명령 직렬화 밖에서 관리한다. 부분 쓰기를 시작한 프레임은 연결이 유지되면 끝까지 쓰고 RESET을 뒤에 둔다.
5. 0바이트에서는 OP_WRITE 재등록·재시도, 예외에서는 마지막으로 확인된 진행과 연결 종료를 처리한다. 종료 시 버퍼 해제, pending bytes, Promise 통지, flush 재진입 방지 등 Netty 전송 책임도 유지해야 한다. 이는 상위 handler에 몇 줄을 추가하는 작업이 아니다.

이 후보의 근거는 `NioSocketChannel`의 protected `doWrite`와 `javaChannel`, 출력 버퍼 전송용 메서드, [AbstractNioByteChannel의 incompleteWrite](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/transport/src/main/java/io/netty/channel/nio/AbstractNioByteChannel.java#L253-L314), [AbstractChannel의 flush·오류 경로](https://github.com/netty/netty/blob/ce334fda03fcabc233823b5321e59b03f604da82/transport/src/main/java/io/netty/channel/AbstractChannel.java#L898-L959)다. 현재 소스에서 확장 지점이 보인다는 사실과 완성 구현의 적합성은 구별한다.

## TLS의 선택 지점

| 해석 후보 | 필요한 확인 | 현재 판정 |
| --- | --- | --- |
| TLS가 수락·소비한 평문 진행을 전송 계층 전달로 정의 | TLS 진입 큐 접수와 실제 소비를 구별할 수 있는 구동 방식, 첫·마지막 소비의 동기 보고, 기존 규격·결정과의 정합성 | 사용자 설계 결정과 TLS 전용 구현 조사 필요. 단순 `SslHandler.write()` 접수를 이 경계로 바꾸면 안 됨 |
| 암호문의 실제 소켓 전달을 기준으로 유지 | 프레임→TLS 출력의 대응, TLS가 이미 소비했지만 소켓은 0바이트인 요청의 안전한 철회, 레코드 순서·암호 상태 유지 | stock SslHandler에서 충족하는 경로를 확인하지 못함. 별도 TLS 구동 설계·실증 필요 |

암호문 전체 쓰기 성공을 관찰할 수 있더라도 평문 프레임별 첫 쓰기 전 철회 문제가 자동 해결되지는 않는다. 암호문 일부가 전달된 뒤 실패하면 마지막 바이트 전달 여부도 프레임별로 입증해야 한다. 실패한 Future 하나만으로 부분 진행을 추정하거나, 모른다는 이유로 규격이 요구하는 처리 안 됨을 결과 모름으로 바꾸면 안 된다.

평문 경계를 택할 수 있는지, 그리고 그것이 기존 결정의 수정인지 명료화인지는 후속 grilling의 질문이다. 연구자가 묵시적으로 선택하지 않는다. 채널 보안 요구를 빼거나 TLS를 제품에서 제외하는 우회도 이 연구가 승인하지 않는다.

## 구현 전에 요구할 검증

설계 후보를 선택한 뒤, 구현 티켓의 적합성 검증에 다음을 포함해야 한다.

- flush 뒤 첫 쓰기 0, 그 사이 취소·만료, 다음 쓰기 전에 철회되어 상대에 요청이 도착하지 않는 경우.
- 첫 양수·부분·마지막 쓰기 각 지점의 취소·만료와 연결 끊김; 결과 정확히 한 번 보존 및 RESET 순서.
- 한 번의 gathering write가 여러 프레임을 가로지르는 경우와 Promise 리스너 재진입.
- TLS 평문 대기, 부분 소비, 여러 레코드·프레임 혼합, 암호문 0·부분·전체 전달, 실패·종료 중 해제.
- 실제 선택한 Netty 릴리스와 TLS provider에서 지원 제약 검증. 단위 모의 호출만으로 실제 쓰기 경계를 입증했다고 하지 않는다.

현재 연구는 소스 수준의 구현 후보와 불확실성을 식별했다. 컴파일·네트워크 실험·성능 측정·제공 가능성 확정은 수행하지 않았다. 후속 결정은 [Netty Future 연동과 ByteBuf 소유권](https://github.com/ppzxc/strand/issues/306)에서 사용자와 진행한다.
