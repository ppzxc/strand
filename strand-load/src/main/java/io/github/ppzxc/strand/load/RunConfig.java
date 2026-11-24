package io.github.ppzxc.strand.load;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** {@code run} 명령의 설정. 실행 전에 모두 검증해 잘못된 설정으로 긴 실행을 헛돌리지 않는다. */
final class RunConfig {
    /** 설정 오류. 종료 상태 2로 끝난다. */
    static final class Invalid extends Exception {
        private static final long serialVersionUID = 1L;

        Invalid(String message) {
            super(message);
        }
    }

    static final Set<String> TRANSPORTS = Set.of("nio", "netty");

    final String transport;
    final int connections;
    final int bodyBytes;
    final int inflight;
    final Duration warmup;
    final Duration measure;
    final String output; // null이면 표준 출력
    final Duration chaosKillServerAfter; // null이면 사용하지 않는다(하네스 자체 시험용)

    private RunConfig(Map<String, String> options) throws Invalid {
        this.transport = options.getOrDefault("--transport", "nio");
        if (!TRANSPORTS.contains(transport)) {
            throw new Invalid("--transport 값 '" + transport + "'을(를) 지원하지 않는다. 지원: " + new ArrayList<>(TRANSPORTS));
        }
        this.connections = positiveInt(options, "--connections", 1);
        this.bodyBytes = positiveInt(options, "--body-bytes", 64);
        this.inflight = positiveInt(options, "--inflight", 8);
        this.warmup = duration(options, "--warmup", Duration.ofSeconds(30), true);
        this.measure = duration(options, "--measure", Duration.ofSeconds(60), false);
        this.output = options.get("--output");
        String chaos = options.get("--chaos-kill-server-after");
        this.chaosKillServerAfter = chaos == null ? null : parseDuration("--chaos-kill-server-after", chaos, false);
    }

    /** {@code --이름 값} 쌍을 검증한다. 모르는 옵션이나 값 없는 옵션은 거절한다. */
    static RunConfig parse(List<String> args) throws Invalid {
        Set<String> known = Set.of(
                "--transport",
                "--connections",
                "--body-bytes",
                "--inflight",
                "--warmup",
                "--measure",
                "--output",
                "--chaos-kill-server-after");
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.size(); i += 2) {
            String name = args.get(i);
            if (!known.contains(name)) {
                throw new Invalid(
                        "모르는 옵션 " + name + ". 지원: " + known.stream().sorted().toList());
            }
            if (i + 1 >= args.size()) {
                throw new Invalid(name + " 값이 없다");
            }
            options.put(name, args.get(i + 1));
        }
        return new RunConfig(options);
    }

    private static int positiveInt(Map<String, String> options, String name, int fallback) throws Invalid {
        String raw = options.get(name);
        if (raw == null) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(raw);
            if (value <= 0) {
                throw new Invalid(name + "은(는) 1 이상이어야 한다: " + raw);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new Invalid(name + "은(는) 정수여야 한다: " + raw);
        }
    }

    private static Duration duration(Map<String, String> options, String name, Duration fallback, boolean zeroAllowed)
            throws Invalid {
        String raw = options.get(name);
        return raw == null ? fallback : parseDuration(name, raw, zeroAllowed);
    }

    /** {@code 500ms}, {@code 30s}, {@code 2m} 형식만 받는다. */
    static Duration parseDuration(String name, String raw, boolean zeroAllowed) throws Invalid {
        long multiplierMillis;
        String number;
        if (raw.endsWith("ms")) {
            multiplierMillis = 1;
            number = raw.substring(0, raw.length() - 2);
        } else if (raw.endsWith("s")) {
            multiplierMillis = 1_000;
            number = raw.substring(0, raw.length() - 1);
        } else if (raw.endsWith("m")) {
            multiplierMillis = 60_000;
            number = raw.substring(0, raw.length() - 1);
        } else {
            throw new Invalid(name + " 시간 형식이 아니다(예: 500ms, 30s, 2m): " + raw);
        }
        try {
            long value = Long.parseLong(number);
            if (value < 0 || (value == 0 && !zeroAllowed)) {
                throw new Invalid(name + "은(는) " + (zeroAllowed ? "0 이상" : "양수") + "여야 한다: " + raw);
            }
            return Duration.ofMillis(Math.multiplyExact(value, multiplierMillis));
        } catch (NumberFormatException | ArithmeticException e) {
            throw new Invalid(name + " 시간 형식이 아니다(예: 500ms, 30s, 2m): " + raw);
        }
    }

    /** 자식 프로세스에 그대로 넘기는 인자. */
    List<String> childArgs() {
        return Arrays.asList(
                "--transport",
                transport,
                "--connections",
                String.valueOf(connections),
                "--body-bytes",
                String.valueOf(bodyBytes),
                "--inflight",
                String.valueOf(inflight),
                "--warmup",
                warmup.toMillis() + "ms",
                "--measure",
                measure.toMillis() + "ms");
    }
}
