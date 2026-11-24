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
    final boolean openLoop;
    /** 개방형 도착률(요청/초, 전체 연결 합). 절대값이면 rateFraction은 0이다. */
    final double rate;
    /** {@code --rate 50%}처럼 포화 처리량의 비율로 준 경우 0~1. 아니면 0이다. */
    final double rateFraction;

    final int repetitions;
    /** 이미 잰 포화 처리량(요청/초). 0이면 없다. 행렬 실행이 같은 셀의 폐쇄형 결과를 개방형에 넘길 때 쓴다. */
    final double saturation;
    /** 켠 상태가 주 비교다. {@code --plaintext}는 하네스 한정 대조군이며 제품 설정으로 노출되지 않는다. */
    final boolean tls;
    /** 자식 프로세스 전용: 진행자가 만든 키 저장소 경로. */
    final String keystore;

    final Duration serverDelay; // 하네스 자체 시험용: 서버가 응답을 이만큼 미룬다.
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
        String model = options.getOrDefault("--load-model", "closed");
        if (!model.equals("closed") && !model.equals("open")) {
            throw new Invalid("--load-model 값 '" + model + "'을(를) 지원하지 않는다. 지원: closed, open");
        }
        this.openLoop = model.equals("open");
        String rawRate = options.get("--rate");
        if (openLoop && rawRate == null) {
            throw new Invalid("--load-model open에는 --rate(요청/초 또는 포화 처리량의 비율 예: 50%)가 필요하다");
        }
        if (!openLoop && rawRate != null) {
            throw new Invalid("--rate는 --load-model open에서만 쓴다");
        }
        double parsedRate = 0;
        double parsedFraction = 0;
        if (rawRate != null) {
            try {
                if (rawRate.endsWith("%")) {
                    parsedFraction = Double.parseDouble(rawRate.substring(0, rawRate.length() - 1)) / 100.0;
                    if (!(parsedFraction > 0 && parsedFraction <= 1.0)) {
                        throw new Invalid("--rate 비율은 0 초과 100% 이하여야 한다: " + rawRate);
                    }
                } else {
                    parsedRate = Double.parseDouble(rawRate);
                    if (!(parsedRate > 0)) {
                        throw new Invalid("--rate는 양수여야 한다: " + rawRate);
                    }
                }
            } catch (NumberFormatException e) {
                throw new Invalid("--rate는 숫자 또는 비율(예: 50%)이어야 한다: " + rawRate);
            }
        }
        this.rate = parsedRate;
        this.rateFraction = parsedFraction;
        this.repetitions = positiveInt(options, "--repetitions", 3);
        String sat = options.get("--saturation");
        double parsedSaturation = 0;
        if (sat != null) {
            try {
                parsedSaturation = Double.parseDouble(sat);
            } catch (NumberFormatException e) {
                throw new Invalid("--saturation은 숫자여야 한다: " + sat);
            }
            if (!(parsedSaturation > 0)) {
                throw new Invalid("--saturation은 양수여야 한다: " + sat);
            }
        }
        this.saturation = parsedSaturation;
        String delay = options.get("--server-delay");
        this.serverDelay = delay == null ? Duration.ZERO : parseDuration("--server-delay", delay, true);
        this.tls = !options.containsKey("--plaintext");
        this.keystore = options.get("--keystore");
        if (tls && keystore == null && options.containsKey("--child")) {
            throw new Invalid("자식 프로세스에 --keystore가 없다");
        }
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
                "--chaos-kill-server-after",
                "--load-model",
                "--rate",
                "--repetitions",
                "--server-delay",
                "--saturation",
                "--plaintext",
                "--keystore",
                "--child");
        Map<String, String> options = new HashMap<>();
        Set<String> flags = Set.of("--plaintext", "--child");
        for (int i = 0; i < args.size(); i++) {
            String name = args.get(i);
            if (!known.contains(name)) {
                throw new Invalid(
                        "모르는 옵션 " + name + ". 지원: " + known.stream().sorted().toList());
            }
            if (flags.contains(name)) {
                options.put(name, "true");
                continue;
            }
            if (i + 1 >= args.size()) {
                throw new Invalid(name + " 값이 없다");
            }
            options.put(name, args.get(++i));
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

    /** 개방형에서 미완료 요청이 쌓여도 자원 한도 때문에 거절되지 않도록 넉넉히 잡는 창 크기. */
    int effectiveInflight() {
        return openLoop ? Math.max(inflight, 1024) : inflight;
    }

    /**
     * 자식 프로세스에 넘기는 인자. 도착률 비율은 진행자가 포화 처리량으로 풀어 절대값으로 넘긴다.
     *
     * @param resolvedRate 개방형이면 요청/초, 폐쇄형이면 무시
     * @param closedOverride true면 설정과 무관하게 폐쇄형으로 돌린다(포화 처리량 측정용)
     */
    List<String> childArgs(double resolvedRate, boolean closedOverride, String keystorePath) {
        List<String> args = new ArrayList<>(Arrays.asList(
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
                measure.toMillis() + "ms",
                "--server-delay",
                serverDelay.toMillis() + "ms"));
        args.add("--child");
        if (tls) {
            args.addAll(List.of("--keystore", keystorePath));
        } else {
            args.add("--plaintext");
        }
        if (openLoop && !closedOverride) {
            args.addAll(List.of("--load-model", "open", "--rate", Double.toString(resolvedRate)));
        }
        return args;
    }
}
