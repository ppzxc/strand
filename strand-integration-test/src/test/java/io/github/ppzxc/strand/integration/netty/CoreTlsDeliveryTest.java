package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** 실제 Netty NIO 채널과 JDK SSLEngine·TLS 서버에서 TLS 평문 소비량과 암호문 쓰기량을 구별해 보고하는 경계를 검증한다(#315). */
class CoreTlsDeliveryTest {
    private static final byte[] PING = {1, 2, 3, 4, 5, 6, 7, 8};

    private static int sum(List<Integer> values) {
        return values.stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * PING을 하나씩 보내 "마지막 PING의 평문은 모두 소비했지만 그 암호문은 소켓에 나가지 못하고 대기열에 남은" 상태를 만든다. 커널 버퍼가 늦게
     * 풀려 쓰기 0이 일시적일 수 있으므로, 30ms 뒤에도 실제 쓴 양이 변하지 않고 암호문이 남아 있을 때만 막힘으로 본다.
     */
    private static void blockSocket(TlsFixture fixture) throws Exception {
        for (int i = 0; i < 100_000; i++) {
            int zerosBefore = fixture.onLoop(fixture.driver::zeroCiphertextWrites);
            fixture.driver.command(() -> fixture.endpoint.ping(PING));
            boolean stuck = fixture.onLoop(() -> fixture.driver.zeroCiphertextWrites() > zerosBefore
                    && fixture.driver.queuedCiphertextBytes() > 0
                    && !fixture.driver.hasPendingOutput());
            if (!stuck) continue;
            int written = sum(fixture.onLoop(fixture.driver::ciphertextWrites));
            Thread.sleep(30);
            if (sum(fixture.onLoop(fixture.driver::ciphertextWrites)) == written
                    && fixture.onLoop(fixture.driver::queuedCiphertextBytes) > 0
                    && !fixture.onLoop(fixture.driver::hasPendingOutput)) {
                return;
            }
        }
        throw new AssertionError("socket never stably blocked");
    }

    /** 한 시나리오. 커널이 쓰기 막힘을 일시적으로 풀어 전제가 깨졌으면 false를 돌려주고, 전제가 맞은 실행에서 단언이 어긋나면 던진다. */
    private interface Scenario {
        boolean run(TlsFixture fixture) throws Exception;
    }

    private static void whenSocketStaysBlocked(String protocol, int reservedPackets, Scenario scenario)
            throws Exception {
        for (int attempt = 0; attempt < 10; attempt++) {
            try (TlsFixture fixture = new TlsFixture(protocol, reservedPackets)) {
                if (scenario.run(fixture)) return;
            }
        }
        throw new AssertionError("쓰기 막힘이 유지되는 실행을 얻지 못했다");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void plaintextConsumptionIsReportedPerFrameAndDiffersFromCiphertextWrites(String protocol) throws Exception {
        try (TlsFixture fixture = new TlsFixture(protocol, 2)) {
            byte[] payload = new byte[40_000];
            Arrays.fill(payload, (byte) 5);
            int frameLength = 4 + 16 + payload.length;
            fixture.driver.command(() -> fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, payload));
            byte[] wire = fixture.read(frameLength);
            assertThat(OpenEndpoint.typeOf(wire)).isEqualTo(OpenEndpoint.REQUEST_TYPE);
            List<Integer> plain = fixture.onLoop(fixture.driver::plaintextReports);
            assertThat(plain).as("프레임 하나가 여러 wrap에 나뉘어 소비된다").hasSizeGreaterThan(1);
            assertThat(plain.get(0)).isPositive().isLessThan(frameLength);
            assertThat(sum(plain)).isEqualTo(frameLength);
            assertThat(sum(fixture.onLoop(fixture.driver::ciphertextWrites)))
                    .as("암호문 쓰기량은 평문 소비량과 같은 값이 아니다")
                    .isGreaterThan(frameLength);
            fixture.closeAndSettle();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void partialPlaintextConsumptionFinishesTheFrameBeforeReset(String protocol) throws Exception {
        try (TlsFixture fixture = new TlsFixture(protocol, 2)) {
            byte[] payload = new byte[512 * 1024];
            Arrays.fill(payload, (byte) 9);
            int frameLength = 4 + 16 + payload.length;
            AtomicLong ticket = new AtomicLong();
            fixture.driver.command(() -> ticket.set(fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, payload)));
            // 상대가 읽지 않으면 암호문 소켓 쓰기가 막혀 평문 소비가 프레임 중간에서 멈춘다.
            for (int i = 0; i < 2000 && fixture.onLoop(fixture.driver::budgetBlocks) == 0; i++) {
                Thread.sleep(2);
            }
            List<Integer> plain = fixture.onLoop(fixture.driver::plaintextReports);
            assertThat(fixture.onLoop(fixture.driver::budgetBlocks)).isPositive();
            assertThat(fixture.onLoop(fixture.driver::zeroCiphertextWrites)).isPositive();
            assertThat(fixture.onLoop(fixture.driver::partialCiphertextWrites))
                    .as("암호문이 일부만 실제로 쓰인 적이 있다")
                    .isPositive();
            assertThat(sum(plain)).isPositive().isLessThan(frameLength);

            fixture.driver.command(() -> fixture.endpoint.cancelRequest(ticket.get()));
            List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
            assertThat(events.get(0).certainty()).hasValue(Certainty.UNKNOWN);

            byte[] wire = fixture.read(frameLength + 24);
            List<byte[]> frames = OpenEndpoint.frames(wire);
            assertThat(frames).hasSize(2);
            assertThat(frames.get(0)).hasSize(frameLength);
            assertThat(OpenEndpoint.typeOf(frames.get(1))).isEqualTo(OpenEndpoint.FRAME_TYPE_RESET);
            fixture.closeAndSettle();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void withdrawalBeforeAnyPlaintextIsConsumedLeavesNoFrameOnTheWire(String protocol) throws Exception {
        // 예약 한도가 패킷 하나이므로 소켓이 막히면 암호문이 남은 채 다음 프레임의 소비량은 0이다.
        whenSocketStaysBlocked(protocol, 1, fixture -> {
            blockSocket(fixture);
            // 요청 접수, 0 보고, 철회를 이벤트 루프 한 작업으로 묶어 그 사이에 소켓 상태가 바뀌지 않게 한다.
            boolean withdrawnAtZero = fixture.onLoop(() -> {
                long ticket = fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, new byte[100]);
                fixture.driver.pump();
                List<Integer> reports = fixture.driver.plaintextReports();
                if (reports.get(reports.size() - 1) != 0 || fixture.driver.budgetBlocks() == 0) return false;
                fixture.endpoint.cancelRequest(ticket);
                return true;
            });
            if (!withdrawnAtZero) return false;
            List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
            assertThat(events).hasSize(1);
            assertThat(events.get(0).certainty()).hasValue(Certainty.NOT_PROCESSED);

            fixture.driver.closeGracefully();
            List<byte[]> frames = OpenEndpoint.frames(fixture.readToEnd());
            assertThat(frames)
                    .extracting(OpenEndpoint::typeOf)
                    .as("PING만 도착하고 철회한 요청 프레임은 없다")
                    .containsOnly(2);
            assertThat(frames).allMatch(OpenEndpoint::isComplete);
            fixture.channel.closeFuture().sync();
            fixture.onLoop(() -> null);
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
            return true;
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void lastPlaintextConsumptionIsNotTlsOrTransportCleanup(String protocol) throws Exception {
        int frameLength = 4 + 16 + 1000;
        whenSocketStaysBlocked(protocol, 2, fixture -> {
            blockSocket(fixture);
            // 예약 한도에 여유가 있어 프레임의 마지막 평문까지 소비했지만 이 프레임의 암호문은 소켓에 한 바이트도 나가지 않은 상태를 한 작업으로 잡는다.
            int[] state = fixture.onLoop(() -> {
                int writtenBefore = sum(fixture.driver.ciphertextWrites());
                fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, new byte[1000]);
                fixture.driver.pump();
                List<Integer> plain = fixture.driver.plaintextReports();
                int written = sum(fixture.driver.ciphertextWrites());
                return new int[] {
                    plain.get(plain.size() - 1),
                    written - writtenBefore,
                    fixture.driver.queuedCiphertextBytes(),
                    fixture.driver.tlsOutboundOpen() ? 1 : 0,
                    fixture.channel.isOpen() ? 1 : 0
                };
            });
            if (state[0] != frameLength || state[1] != 0) return false;
            assertThat(state[2]).isGreaterThan(frameLength);
            assertThat(state[3]).as("TLS 송신 방향이 아직 열려 있다").isEqualTo(1);
            assertThat(state[4]).as("실제 통로가 아직 열려 있다").isEqualTo(1);

            fixture.driver.closeGracefully();
            List<byte[]> frames = OpenEndpoint.frames(fixture.readToEnd());
            assertThat(frames.get(frames.size() - 1)).hasSize(frameLength);
            assertThat(OpenEndpoint.typeOf(frames.get(frames.size() - 1))).isEqualTo(OpenEndpoint.REQUEST_TYPE);
            fixture.channel.closeFuture().sync();
            fixture.onLoop(() -> null);
            assertThat(fixture.onLoop(fixture.driver::tlsOutboundDone)).isTrue();
            assertThat(fixture.channel.isOpen()).isFalse();
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
            List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
            assertThat(events).extracting(Event::kind).contains(Event.Kind.RESULT, Event.Kind.CLOSED);
            return true;
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void abortWithUnsentCiphertextReleasesEveryBufferAndResultIsUnknown(String protocol) throws Exception {
        int frameLength = 4 + 16 + 1000;
        whenSocketStaysBlocked(protocol, 2, fixture -> {
            blockSocket(fixture);
            boolean consumedButUnsent = fixture.onLoop(() -> {
                fixture.endpoint.request(OpenEndpoint.REQUEST_TYPE, new byte[1000]);
                fixture.driver.pump();
                List<Integer> plain = fixture.driver.plaintextReports();
                return plain.get(plain.size() - 1) == frameLength && fixture.driver.queuedCiphertextBytes() > 0;
            });
            if (!consumedButUnsent) return false;
            fixture.closeAndSettle();
            List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
            Event result = events.stream()
                    .filter(e -> e.kind() == Event.Kind.RESULT)
                    .findFirst()
                    .get();
            assertThat(result.certainty()).hasValue(Certainty.UNKNOWN);
            assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
            return true;
        });
    }
}
