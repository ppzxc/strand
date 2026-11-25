package io.github.ppzxc.strand.load;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public final class LoadCli {
    private LoadCli() {}

    public static void main(String[] args) {
        int exit = execute(args, System.out, System.err);

        System.exit(exit);
    }

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
                    duration = valueOf(args, ++i, arg);
                    break;
                case "--rate":
                    rate = valueOf(args, ++i, arg);
                    break;
                default:
                    translated.add(arg);
                    break;
            }
        }
        if (rate.endsWith("%")) {

            throw new RunConfig.Invalid("soak의 --rate는 요청/초 절대값이어야 한다(비율 불가): " + rate);
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

    private static String valueOf(List<String> args, int index, String name) throws RunConfig.Invalid {
        if (index >= args.size()) {
            throw new RunConfig.Invalid(name + " 값이 없다");
        }
        return args.get(index);
    }

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
        if (!json.has("status") || !json.get("status").getAsString().equals("OK")) {
            err.println("설정 오류: 측정 결과가 아닌 파일이다(--plan 출력 등). status=" + json.get("status"));
            return 2;
        }
        out.print(TableReport.markdown(json));
        return 0;
    }

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
