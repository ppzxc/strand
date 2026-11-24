package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;

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
            answerAuthentication(server);
            pump(server, client);
        }
        if (!drainEvents(client).stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED)) {
            throw new IllegalStateException("클라이언트가 AUTH 관문을 통과하지 못했다");
        }
        drainEvents(server);
        return new Endpoint[] {client, server};
    }

    /** {@code from}이 쌓은 메시지를 {@code to}에 넣는다. */
    static void pump(Endpoint from, Endpoint to) {
        for (OutboundMessage message : drainMessages(from)) {
            receive(to, message.bytes());
        }
    }

    /** 서버가 위임한 인증 판단을 통과로 입력하고 핸들러가 준비됐음을 알린다. */
    static void answerAuthentication(Endpoint server) {
        for (Event event : drainEvents(server)) {
            if (event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
            }
        }
    }

    /** 바이트를 모두 넣는다(진행 보고 방식의 한도 있는 입력). */
    static void receive(Endpoint endpoint, byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(
                    bytes,
                    offset,
                    bytes.length - offset,
                    Integer.MAX_VALUE,
                    CollectionSpace.of(Integer.MAX_VALUE, Integer.MAX_VALUE));
            if (progress.consumed() == 0) {
                return;
            }
            offset += progress.consumed();
        }
    }

    /** 쌓인 출력을 순서대로 꺼내 모두 쓴 것으로 보고한다. */
    static List<OutboundMessage> drainMessages(Endpoint endpoint) {
        List<OutboundMessage> drained = new ArrayList<>();
        EndpointOutput output;
        while ((output = endpoint.nextOutput()) != null) {
            drained.add(output.message());
            endpoint.outputProgress(output.id(), output.length());
        }
        return drained;
    }

    static List<Event> drainEvents(Endpoint endpoint) {
        return endpoint.drainEvents(Integer.MAX_VALUE);
    }

    /** {@code server}가 받아 둔 요청의 스트림 번호를 도착 순서대로 꺼내 {@code order}로 정렬한다. 이벤트는 비운다. */
    static long[] receivedStreamIds(Endpoint server, ResponseOrder order) {
        List<Long> ids = new ArrayList<>();
        for (Event event : drainEvents(server)) {
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                ids.add(event.streamId());
            }
        }
        long[] result = new long[ids.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ids.get(order == ResponseOrder.OLDEST_FIRST ? i : result.length - 1 - i);
        }
        return result;
    }

    /** 클라이언트가 요청 {@code count}개를 내고 서버가 모두 받아 이벤트로 올린 상태를 만든다. 반환은 {@code {client, server}}. */
    static Endpoint[] establishWithPendingRequests(int count) {
        Endpoint[] pair = establish(EndpointConfig.defaults().maxConcurrentStreams(count));
        for (int i = 0; i < count; i++) {
            pair[0].request(REQUEST_TYPE, new byte[0]);
        }
        pump(pair[0], pair[1]);
        return pair;
    }

    /** 가상 시계를 밀리초 단위로 앞으로 보내는 나노초 시계. */
    static final class Clock {
        private long elapsedNanos;

        void advance(Endpoint endpoint, long ms) {
            elapsedNanos += ms * Deadlines.NANOS_PER_MILLI;
            endpoint.advanceTo(elapsedNanos);
        }
    }

    /** 길이 필드를 포함해 정확히 {@code size}바이트인, 예약 type 프레임 하나. */
    static byte[] reservedFrame(int size) {
        return Frames.control(RESERVED_TYPE, 0, new byte[size - Frames.HEADER_SIZE]);
    }
}
