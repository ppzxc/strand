package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * 바이트 스트림 입력은 어디서 어떻게 잘라 넣어도 같은 결과여야 한다(규격 1.2절, {@code Endpoint.receive}의 계약). 같은 입력 바이트를 통째로
 * 넣은 결과를 기준으로, 모든 분할 지점, 1바이트씩, 고정 시드의 무작위 조각, 조각마다 호출자 배열을 덮어쓰는 경우를 견준다.
 *
 * <p>통째로 넣은 결과 자체도 골든 마스터({@code stream-split-golden.txt})와 견준다. 분할끼리만 견주면 모든 경로에서 같게 틀린 구현을 놓친다.
 * 보는 것은 {@code receive}와 {@code drainMessages}, {@code drainEvents}뿐이다. 골든을 다시 만드는 방법은 {@link Golden}을 본다.
 */
class EndpointStreamSplitTest {

    private static final String GOLDEN_RESOURCE = "stream-split-golden.txt";
    private static final int REQUEST_TYPE = 0x0100;
    private static final int RESERVED_TYPE = 0x0006;
    private static final int MAX_FRAME_SIZE = 1_048_576;
    /** 이 크기 이하의 입력만 모든 분할 지점을 돈다. */
    private static final int EXHAUSTIVE_LIMIT = 2000;

    /** 입력 하나와 그것을 받는 엔드포인트. */
    private static final class Case {
        final Supplier<Endpoint> endpoint;
        final byte[] stream;

        Case(Supplier<Endpoint> endpoint, byte[] stream) {
            this.endpoint = endpoint;
            this.stream = stream;
        }
    }

    private static Supplier<Endpoint> server(EndpointConfig config) {
        return () -> Endpoint.server(config);
    }

    /** 서버 쪽 시점에서 서문과 HELLO 교환, AUTH 통과까지의 입력. */
    private static byte[] establishing(EndpointConfig peer) {
        return join(
                Frames.preface(),
                Frames.hello(peer),
                Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]),
                Frames.auth("ANONYMOUS", new byte[0]));
    }

    private static Map<String, Case> cases() {
        EndpointConfig defaults = EndpointConfig.defaults();
        EndpointConfig big = EndpointConfig.defaults().maxFrameSize(MAX_FRAME_SIZE);
        byte[] established = establishing(defaults);
        Map<String, Case> cases = new LinkedHashMap<>();
        cases.put(
                "valid-server",
                new Case(
                        server(defaults),
                        join(
                                established,
                                Frames.request(REQUEST_TYPE, 1, new byte[0]),
                                Frames.control(FrameType.PING, 0, new byte[] {1, 2, 3, 4, 5, 6, 7, 8}),
                                Frames.request(REQUEST_TYPE, 3, filled(300, 7)),
                                Frames.control(RESERVED_TYPE, 0, new byte[0]),
                                Frames.request(REQUEST_TYPE, 5, filled(5000, 9)),
                                Frames.reset(3, ErrorCode.CANCEL),
                                Frames.request(REQUEST_TYPE, 7, filled(1, 1)),
                                Frames.close(ErrorCode.NO_ERROR, 0))));
        cases.put(
                "bad-length-then-request",
                new Case(
                        server(defaults), join(established, Frames.request(REQUEST_TYPE, 1, filled(40, 3)), new byte[] {
                            0, 0, 0, 5, 0x01, 0x00, 0, 0, 0, 0
                        })));
        // CLOSE는 길이가 어긋나도 받은 CLOSE로 보고 CLOSE 없이 닫는다. type을 읽을 때까지 기다려야 도착 단위와 무관하다(3.5절).
        cases.put(
                "bad-length-close",
                new Case(
                        server(defaults), join(established, Frames.request(REQUEST_TYPE, 1, filled(10, 2)), new byte[] {
                            0, 0, 0, 5, 0x00, 0x03, 0, 0, 0, 0
                        })));
        cases.put("oversized-length-request", new Case(server(defaults), join(established, new byte[] {
            0, 0x20, 0, 0, 0x01, 0x00, 0, 0, 0, 0
        })));
        cases.put("oversized-length-close", new Case(server(defaults), join(established, new byte[] {
            0, 0x20, 0, 0, 0x00, 0x03, 0, 0, 0, 0
        })));
        byte[] badPreface = Frames.preface();
        badPreface[3] = 0x00;
        cases.put("bad-preface", new Case(server(defaults), join(badPreface, Frames.hello(defaults))));
        cases.put(
                "first-frame-not-hello",
                new Case(
                        server(defaults),
                        join(
                                Frames.preface(),
                                Frames.control(FrameType.PING, 0, new byte[8]),
                                Frames.hello(defaults))));
        cases.put("first-frame-tiny-length", new Case(server(defaults), join(Frames.preface(), new byte[] {
            0, 0, 0, 3, 0x00, 0x01, 0, 0
        })));
        cases.put(
                "client-role",
                new Case(
                        () -> Endpoint.client(defaults),
                        join(
                                Frames.hello(defaults),
                                Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]),
                                Frames.control(FrameType.AUTH, Flags.ACK, new byte[0]),
                                Frames.request(REQUEST_TYPE, 2, filled(50, 4)),
                                Frames.control(FrameType.PING, 0, new byte[8]),
                                Frames.request(REQUEST_TYPE, 4, filled(2000, 5)))));
        // 128 KiB 이상 큰 프레임 뒤에 작은 프레임. 기본 최대 프레임 크기(64 KiB)로는 쌓은 버퍼를 놓는 분기에 닿지 못한다.
        cases.put(
                "big-frame-then-small",
                new Case(
                        server(big),
                        join(
                                establishing(big),
                                Frames.request(REQUEST_TYPE, 1, filled(131_072, 5)),
                                Frames.control(FrameType.PING, 0, new byte[8]),
                                Frames.request(REQUEST_TYPE, 3, filled(70_000, 6)),
                                Frames.request(REQUEST_TYPE, 5, filled(10, 7)),
                                Frames.close(ErrorCode.NO_ERROR, 0))));
        return cases;
    }

    private static byte[] filled(int size, int value) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] join(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    /** 이벤트와 송신 메시지를 문자열로 줄인다. */
    private static String observe(Endpoint endpoint) {
        StringBuilder out = new StringBuilder();
        for (Event event : endpoint.drainEvents()) {
            out.append("event ")
                    .append(event.kind())
                    .append(' ')
                    .append(event.streamId())
                    .append(' ')
                    .append(event.status())
                    .append(' ')
                    .append(event.type())
                    .append('\n');
        }
        for (OutboundMessage message : endpoint.drainMessages()) {
            out.append("message ")
                    .append(message.kind())
                    .append(' ')
                    .append(Arrays.toString(message.bytes()))
                    .append('\n');
        }
        return out.toString();
    }

    private static String receivedAs(Case c, List<Integer> sizes, boolean overwriteAfterEach) {
        Endpoint endpoint = c.endpoint.get();
        int at = 0;
        for (int size : sizes) {
            byte[] chunk = Arrays.copyOfRange(c.stream, at, at + size);
            endpoint.receive(chunk);
            if (overwriteAfterEach) {
                Arrays.fill(chunk, (byte) 0xFF);
            }
            at += size;
        }
        return observe(endpoint);
    }

    private static String receivedWhole(Case c) {
        return receivedAs(c, Collections.singletonList(c.stream.length), false);
    }

    private static List<Integer> oneByteEach(int length) {
        List<Integer> sizes = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            sizes.add(1);
        }
        return sizes;
    }

    private static List<Integer> randomSizes(int length, long seed, int maxPiece) {
        Random random = new Random(seed);
        List<Integer> sizes = new ArrayList<>();
        int remaining = length;
        while (remaining > 0) {
            int size = Math.min(remaining, 1 + random.nextInt(random.nextBoolean() ? 8 : maxPiece));
            sizes.add(size);
            remaining -= size;
        }
        return sizes;
    }

    /** 입력이 크면 모든 지점을 돌 수 없으니, 헤더와 서문, 프레임 크기 경계 근처 지점만 고른다. */
    private static List<Integer> splitPoints(int length) {
        List<Integer> points = new ArrayList<>();
        if (length <= EXHAUSTIVE_LIMIT) {
            for (int point = 1; point < length; point++) {
                points.add(point);
            }
            return points;
        }
        int[] around = {1, 3, 4, 5, 6, 7, 8, 9, 19, 20, 21, 24, 65_536, 131_072, 131_073};
        for (int offset : new int[] {0, 40, 60}) {
            for (int k : around) {
                int point = offset + k;
                if (point > 0 && point < length) {
                    points.add(point);
                }
            }
        }
        points.add(length / 2);
        points.add(length - 1);
        return points;
    }

    private static void assertSplitInvariant(Case c) {
        String whole = receivedWhole(c);
        int length = c.stream.length;
        for (int point : splitPoints(length)) {
            assertThat(receivedAs(c, Arrays.asList(point, length - point), false))
                    .as("둘로 나눈 지점 %d", point)
                    .isEqualTo(whole);
        }
        if (length <= EXHAUSTIVE_LIMIT) {
            assertThat(receivedAs(c, oneByteEach(length), false)).as("1바이트씩").isEqualTo(whole);
        }
        int maxPiece = length <= EXHAUSTIVE_LIMIT ? 700 : 5000;
        for (long seed = 1; seed <= 20; seed++) {
            List<Integer> sizes = randomSizes(length, seed, maxPiece);
            assertThat(receivedAs(c, sizes, false)).as("무작위 조각 시드 %d", seed).isEqualTo(whole);
            assertThat(receivedAs(c, sizes, true))
                    .as("조각마다 호출자 배열을 덮어써도, 시드 %d", seed)
                    .isEqualTo(whole);
        }
    }

    @Test
    void everyCaseGivesSameResultWhereverItIsSplit() {
        for (Map.Entry<String, Case> entry : cases().entrySet()) {
            try {
                assertSplitInvariant(entry.getValue());
            } catch (AssertionError e) {
                throw new AssertionError(entry.getKey() + ": " + e.getMessage(), e);
            }
        }
    }

    @Test
    void casesReachTheirBranches() {
        Map<String, Case> cases = cases();
        assertThat(receivedWhole(cases.get("valid-server")))
                .contains("event REQUEST_RECEIVED 1 ")
                .contains("event REQUEST_RECEIVED 5 ");
        for (String closing : new String[] {
            "bad-length-then-request",
            "bad-length-close",
            "oversized-length-request",
            "oversized-length-close",
            "bad-preface",
            "first-frame-not-hello",
            "first-frame-tiny-length"
        }) {
            assertThat(receivedWhole(cases.get(closing))).as(closing).contains("event CLOSED");
        }
        assertThat(receivedWhole(cases.get("client-role"))).contains("event REQUEST_RECEIVED 2 ");
        assertThat(receivedWhole(cases.get("big-frame-then-small")))
                .contains("event REQUEST_RECEIVED 1 ")
                .contains("event REQUEST_RECEIVED 3 ")
                .contains("event REQUEST_RECEIVED 5 ");
    }

    @Test
    void wholeStreamResultMatchesGoldenMaster() {
        List<String> actual = new ArrayList<>();
        for (Map.Entry<String, Case> entry : cases().entrySet()) {
            actual.add(entry.getKey() + " " + Golden.sha256(receivedWhole(entry.getValue())));
        }
        assertThat(actual).isEqualTo(Golden.lines(GOLDEN_RESOURCE, actual));
    }
}
