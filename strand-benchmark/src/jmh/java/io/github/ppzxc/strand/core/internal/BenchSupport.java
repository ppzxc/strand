package io.github.ppzxc.strand.core.internal;

/**
 * 벤치마크 공용 도우미. {@code Frames}가 패키지 전용이라 벤치마크 소스도 같은 패키지에 둔다(클래스패스 실행이고 {@code module-info}가 없다).
 */
final class BenchSupport {

    /** 벤치마크가 쓰는 애플리케이션 요청 type. */
    static final int REQUEST_TYPE = 0x0100;
    /** 받으면 확장 영역 검사만 하고 무시하는 예약 제어 type(규격 2.2.2절). 이벤트도 송신도 만들지 않는다. */
    static final int RESERVED_TYPE = 0x0006;

    private BenchSupport() {}

    /** 두 엔드포인트가 서문, HELLO, AUTH 관문까지 마친 상태로 만든다. 송신 대기열과 이벤트는 비운다. */
    static Endpoint[] establish(EndpointConfig config) {
        Endpoint client = Endpoint.client(config);
        Endpoint server = Endpoint.server(config);
        for (int round = 0; round < 6; round++) {
            pump(client, server);
            pump(server, client);
        }
        if (!client.drainEvents().stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED)) {
            throw new IllegalStateException("클라이언트가 AUTH 관문을 통과하지 못했다");
        }
        server.drainEvents();
        return new Endpoint[] {client, server};
    }

    /** {@code from}이 쌓은 메시지를 {@code to}에 넣는다. */
    static void pump(Endpoint from, Endpoint to) {
        for (OutboundMessage message : from.drainMessages()) {
            to.receive(message.bytes());
        }
    }

    /** 길이 필드를 포함해 정확히 {@code size}바이트인, 예약 type 프레임 하나. */
    static byte[] reservedFrame(int size) {
        return Frames.control(RESERVED_TYPE, 0, new byte[size - Frames.HEADER_SIZE]);
    }
}
