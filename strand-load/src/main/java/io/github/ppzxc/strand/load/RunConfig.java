package io.github.ppzxc.strand.load;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class RunConfig {

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

    final double rate;

    final double rateFraction;

    final int repetitions;

    final double saturation;

    final boolean tls;

    final String keystore;

    final boolean stall;

    final boolean soak;

    final String binding;

    final String loadgenTransport;

    final Duration sampleInterval;

    final Duration idlePing;
    final Duration ackTimeout;
    final Duration maxWait;

    final boolean jfr;

    final String asyncProfiler;

    final String profileDir;

    final Duration serverDelay;
    final String output;
    final Duration chaosKillServerAfter;

    private RunConfig(Map<String, String> options) throws Invalid {
        this.transport = options.getOrDefault("--transport", "nio");
        if (!TRANSPORTS.contains(transport)) {
            throw new Invalid("--transport 값 '" + transport + "'을(를) 지원하지 않는다. 지원: " + new ArrayList<>(TRANSPORTS));
        }
        this.connections = positiveInt(options, "--connections", 1);
        this.stall = options.containsKey("--stall");
        this.soak = options.containsKey("--soak");
        this.binding = options.getOrDefault("--binding", "tcp");
        if (!binding.equals("tcp") && !binding.equals("websocket")) {
            throw new Invalid("--binding 값 '" + binding + "'을(를) 지원하지 않는다. 지원: tcp, websocket");
        }
        this.loadgenTransport = options.getOrDefault("--loadgen-transport", "nio");
        if (!loadgenTransport.equals("nio") && !loadgenTransport.equals("netty")) {
            throw new Invalid("--loadgen-transport 값 '" + loadgenTransport + "'을(를) 지원하지 않는다. 지원: nio, netty");
        }
        if (binding.equals("websocket") && !(transport.equals("netty") && loadgenTransport.equals("netty"))) {
            throw new Invalid(
                    "--binding websocket은 서버와 부하 생성기 모두 netty여야 한다(--transport netty --loadgen-transport netty, ADR 0007)");
        }
        this.sampleInterval = duration(options, "--sample-interval", Duration.ofSeconds(10), false);

        this.bodyBytes = positiveInt(options, "--body-bytes", stall ? 65_536 : 64);
        this.inflight = positiveInt(options, "--inflight", stall ? 32 : 8);
        this.idlePing = duration(options, "--idle-ping", Duration.ofSeconds(30), true);
        this.ackTimeout = duration(options, "--ack-timeout", Duration.ofSeconds(5), false);
        this.maxWait = duration(options, "--max-wait", Duration.ofSeconds(60), false);
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
                    if (!(parsedRate > 0) || Double.isInfinite(parsedRate)) {
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
            if (!(parsedSaturation > 0) || Double.isInfinite(parsedSaturation)) {
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
        this.jfr = options.containsKey("--jfr");
        this.asyncProfiler = options.get("--async-profiler");
        this.profileDir = options.getOrDefault("--profile-dir", "strand-load-profiles");
        if (jfr && ModuleLayer.boot().findModule("jdk.jfr").isEmpty()) {
            throw new Invalid("--jfr: 이 JVM에는 JFR(jdk.jfr 모듈)이 없다. JFR이 있는 JDK로 실행한다");
        }
        if (asyncProfiler != null && !java.nio.file.Files.isRegularFile(java.nio.file.Paths.get(asyncProfiler))) {
            throw new Invalid("--async-profiler 라이브러리를 찾을 수 없다: " + asyncProfiler
                    + " (async-profiler는 선택 사항이다. 받은 libasyncProfiler 경로를 주거나 옵션을 뺀다)");
        }
        String unavailable = ResourceSampler.unavailableReason();
        if (unavailable != null) {
            throw new Invalid(unavailable);
        }
        this.output = options.get("--output");
        String chaos = options.get("--chaos-kill-server-after");
        this.chaosKillServerAfter = chaos == null ? null : parseDuration("--chaos-kill-server-after", chaos, false);
    }

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
                "--stall",
                "--soak",
                "--binding",
                "--loadgen-transport",
                "--sample-interval",
                "--idle-ping",
                "--ack-timeout",
                "--max-wait",
                "--jfr",
                "--profile-dir",
                "--async-profiler",
                "--plaintext",
                "--keystore",
                "--child");
        Map<String, String> options = new HashMap<>();
        Set<String> flags = Set.of("--plaintext", "--child", "--jfr", "--stall", "--soak");
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

    int requestLimit() {
        return 2 * effectiveInflight();
    }

    int effectiveInflight() {
        return openLoop ? Math.max(inflight, 1024) : inflight;
    }

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
                serverDelay.toMillis() + "ms",
                "--idle-ping",
                idlePing.toMillis() + "ms",
                "--ack-timeout",
                ackTimeout.toMillis() + "ms"));
        if (stall) {
            args.add("--stall");
        }
        args.addAll(List.of("--binding", binding, "--loadgen-transport", loadgenTransport));
        if (soak) {
            args.addAll(List.of("--soak", "--sample-interval", sampleInterval.toMillis() + "ms"));
        }
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
