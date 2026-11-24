package io.github.ppzxc.strand.load;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * nio·netty 전송 부하 하네스의 진입점(#422). 이 진입점이 하네스의 유일한 시험 seam이다.
 *
 * <pre>
 * matrix   [--extended] [--output matrix.json]   핵심 행렬(8셀) 한 명령 실행
 * stall    [--transport nio|netty] [--idle-ping 1s] [--ack-timeout 2s]   읽지 않는 상대를 붙여 감지 시간 측정
 * table    --input matrix.json                   행렬 결과 JSON에서 표 생성
 * run      --transport nio --connections 1 --body-bytes 64 --warmup 30s --measure 60s --output result.json
 * server   (내부용) 서버 프로세스 역할
 * loadgen  (내부용) 부하 생성기 프로세스 역할
 * </pre>
 *
 * 종료 상태: 0 성공, 1 실행 실패(프로세스 비정상 종료 등), 2 설정 오류.
 */
public final class LoadCli {
    private LoadCli() {}

    public static void main(String[] args) {
        int exit = execute(args, System.out, System.err);
        // 자식 프로세스 역할에서 남은 비데몬 스레드가 있어도 확실히 끝낸다.
        System.exit(exit);
    }

    /** 프로세스를 끝내지 않고 종료 상태를 돌려준다. 시험이 이 메서드를 부른다. */
    public static int execute(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0) {
            err.println("사용법: run|server|loadgen [옵션]. 자세한 옵션은 strand-load/README.md");
            return 2;
        }
        String command = args[0];
        List<String> rest = Arrays.asList(args).subList(1, args.length);
        try {
            switch (command) {
                case "run":
                    new Orchestrator(RunConfig.parse(rest), out).run();
                    return 0;
                case "matrix":
                    Matrix.parse(rest, out).run();
                    return 0;
                case "stall": {
                    java.util.List<String> withFlag = new java.util.ArrayList<>(rest);
                    withFlag.add("--stall");
                    new Orchestrator(RunConfig.parse(withFlag), out).runStall();
                    return 0;
                }
                case "soak":
                    return soak(rest, out);
                case "table":
                    return table(rest, out, err);
                case "server":
                    return ServerRole.run(RunConfig.parse(withoutPort(rest)), out);
                case "loadgen":
                    return LoadGenRole.run(RunConfig.parse(withoutPort(rest)), portOf(rest), out);
                default:
                    err.println("모르는 명령 '" + command + "'. 지원: run, matrix, stall, soak, table");
                    return 2;
            }
        } catch (RunConfig.Invalid e) {
            err.println("설정 오류: " + e.getMessage());
            return 2;
        } catch (Orchestrator.RunFailed e) {
            err.println("실행 실패: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err.println("실행 실패: " + e);
            return 1;
        }
    }

    /**
     * {@code soak [--duration 1h | --long] [--rate 1000] ...}: 일정 부하로 오래 돌린다. 기본 1시간, {@code --long}은 8시간이다. {@code --plan}은 설정만 JSON으로 낸다.
     */
    private static int soak(List<String> args, PrintStream out) throws Exception {
        java.util.List<String> translated = new java.util.ArrayList<>();
        boolean plan = false;
        boolean longRun = false;
        String duration = null;
        String output = null;
        String rate = "1000";
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            switch (arg) {
                case "--plan":
                    plan = true;
                    break;
                case "--long":
                    longRun = true;
                    break;
                case "--duration":
                    duration = args.get(++i);
                    break;
                case "--rate":
                    rate = args.get(++i);
                    break;
                default:
                    translated.add(arg);
                    break;
            }
        }
        if (duration == null) {
            duration = longRun ? "480m" : "60m";
        }
        translated.addAll(java.util.List.of(
                "--load-model", "open", "--rate", rate, "--repetitions", "1", "--measure", duration, "--soak"));
        RunConfig config = RunConfig.parse(translated);
        if (plan) {
            com.google.gson.JsonObject json = new com.google.gson.JsonObject();
            com.google.gson.JsonObject c = new com.google.gson.JsonObject();
            c.addProperty("durationMillis", config.measure.toMillis());
            c.addProperty("sampleIntervalMillis", config.sampleInterval.toMillis());
            c.addProperty("rateTargetPerSecond", config.rate);
            c.addProperty("transport", config.transport);
            json.add("config", c);
            if (config.output == null) {
                out.println(json);
            } else {
                java.nio.file.Files.writeString(java.nio.file.Paths.get(config.output), json + System.lineSeparator());
            }
            return 0;
        }
        new Orchestrator(config, out).runSoak();
        return 0;
    }

    /** {@code table --input <matrix.json>}: 행렬 결과 JSON에서 표를 만든다. */
    private static int table(List<String> args, PrintStream out, PrintStream err) throws java.io.IOException {
        if (args.size() != 2 || !args.get(0).equals("--input")) {
            err.println("사용법: table --input <matrix.json>");
            return 2;
        }
        java.nio.file.Path input = java.nio.file.Paths.get(args.get(1));
        if (!java.nio.file.Files.isRegularFile(input)) {
            err.println("설정 오류: 입력 파일이 없다: " + input);
            return 2;
        }
        com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(java.nio.file.Files.readString(input))
                .getAsJsonObject();
        out.print(TableReport.markdown(json));
        return 0;
    }

    /** 자식 프로세스 전용 {@code --port} 옵션은 설정 검증에서 뺀다. */
    private static List<String> withoutPort(List<String> args) {
        List<String> result = new java.util.ArrayList<>(args);
        int index = result.indexOf("--port");
        if (index >= 0) {
            result.remove(index + 1);
            result.remove(index);
        }
        return result;
    }

    private static int portOf(List<String> args) {
        return Integer.parseInt(args.get(args.indexOf("--port") + 1));
    }
}
