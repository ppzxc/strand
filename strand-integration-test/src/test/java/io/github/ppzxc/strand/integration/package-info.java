/**
 * 실제 TCP 위에서 전송 모듈({@code strand-nio}, {@code strand-netty})의 제품 공개 API가 클라이언트·서버 {@code Endpoint}를 맞물려 돌리는지 확인하는 통합 테스트.
 *
 * <p>시나리오는 {@code @TransportTest}로 nio와 netty에서 각각 한 번씩 돈다(본문 한 벌, {@code Transport} 매개변수). 호출 이름이 {@code [nio] ...},
 * {@code [netty] ...}라 리포트에 전송별 결과가 따로 나온다. 전송을 감싼 중립 모양은 {@code Wire}이고 구현은 {@code NioWire}, {@code NettyWire}다. 설정 값은 두 전송이 같다
 * (netty도 {@code closeFinishDeadline}을 1초로 둔다. 양쪽이 동시에 CLOSE를 보내면 마무리 기한까지 기다릴 수 있어서다).
 *
 * <p>nio에서만 도는 시나리오({@code [nio 전용]}). 엔진의 테스트 전용 통로 감싸기 지점({@code PassageWrapping})이 nio 엔진에만 있어서 netty에는 같은 관찰·조절 수단이 없다.
 * <ul>
 *   <li>{@code CloseRaceIntegrationTest.noErrorCloseWaitsForPendingResponse}: 서버가 CLOSE를 처리했음을 통로가 읽은 바이트 수로 확인한 뒤 응답한다.
 *   <li>{@code CloseRaceIntegrationTest.crossedCloseWhileClosingSendsOneCloseEach}: 두 통로의 쓰기를 붙잡아(deferWrites) CLOSE를 엇갈리게 하고, 통로에 쓴 CLOSE 프레임 수를 센다.
 *   <li>{@code FrameSizeIntegrationTest.pingIssuedBehindLargeRequestIsWrittenFirst}: 통로의 깨움을 붙잡고(holdWakeups) 통로에 쓴 프레임 순서를 본다.
 *   <li>{@code TransportFailureIntegrationTest.requestNeverWrittenEndsAsNotProcessed}: 통로 쓰기 용량을 0으로 막고 쓴 프레임 수를 본다.
 *   <li>{@code TransportFailureIntegrationTest.framesAreReassembledFromOneByteReads}: 통로가 한 번에 1바이트씩만 읽게 한다(splitReads).
 * </ul>
 *
 * <p>양쪽에서 돌되 스트림 번호(통로가 읽은 프레임의 {@code stream_id})를 확인하는 부분만 nio에서 한다. netty에서는 이 부분을 건너뛰고 응답 매칭과 결과까지 본다:
 * {@code BidirectionalIntegrationTest}, {@code RequestResponseIntegrationTest.eachOverlappingRequestGetsItsOwnResponse},
 * {@code MultiClientIntegrationTest.concurrentClientsEachGetTheirOwnResponse}, {@code RequestVolumeIntegrationTest}.
 *
 * <p>확인하는 것: 연결 수립(규격 3.1~3.3절), 요청·응답의 스트림 매칭(4.2절), 연결 종료(3.5절)가 소켓 위에서 어긋나지 않는지.
 *
 * <p>여러 클라이언트가 한 서버에 동시에 붙어도 연결끼리 섞이지 않는지(4.2절), CLOSE 없이 TCP가 끊기거나
 * 프레임이 쪼개져 읽혀도 규격대로 끝나고 조립되는지(2.1절, 3.5절)도 본다.
 *
 * <p>서버가 먼저 내는 요청과 클라이언트 요청이 한 연결에서 섞이지 않는지(4.2절), 대기 요청이 있는 채
 * CLOSE를 보내거나 양쪽이 동시에 CLOSE를 보내도 규격대로 끝나는지(3.5절), 요청을 이어 보내도 번호와
 * 응답이 어긋나지 않는지(4.2절)도 본다.
 *
 * <p>컨포먼스 벡터가 {@code Endpoint} 하나를 바이트 단위로 검증하는 것과 달리, 여기서는 전송 모듈이 두 {@code Endpoint}를 실제 연결로 이어
 * 돌리는 쪽의 동작을 본다. 연결을 돌리는 루프는 이 모듈에 없다. 연결은 제품 API로만 맺고 돌리며, 쓰기 시점을 조절하거나 오간 프레임과 바이트를 세야 하는 시험은 엔진의 테스트 전용 통로 감싸기 지점({@code PassageWrapping})에 끼운 통로({@code ControlledPassage})로 본다. 제품 연결은 AUTH 관문을 통과해야 앱에 보이므로 관문 전 상태는 날것의 소켓으로 맞춘다.
 *
 * <p>확인하지 않는 것: 프레임 필드 검증과 오류 코드는 {@code strand-conformance}가 맡는다. 네트워크 지연, 부하, 보안, WebSocket 전송도 범위
 * 밖이다.
 *
 * <p>작성 규칙: 포트는 0에 바인딩하고, 대기는 {@code sleep} 없이 알림과 Future, 상한 시간 대기로 한다. 변경을 알려 주지 않는 관찰 지점 카운터만 상한이 있는 짧은 {@code parkNanos} 폴링으로 본다. 새 시나리오는 무엇을 예방하는지 Javadoc과
 * {@code @DisplayName}에 적는다.
 */
package io.github.ppzxc.strand.integration;
