package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CoreTlsDeliveryTest {
    private static final byte[] PING = {1, 2, 3, 4, 5, 6, 7, 8};

    private static int sum(List<Integer> values) {
        return values.stream().mapToInt(Integer::intValue).sum();
    }

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

        whenSocketStaysBlocked(protocol, 1, fixture -> {
            blockSocket(fixture);

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
