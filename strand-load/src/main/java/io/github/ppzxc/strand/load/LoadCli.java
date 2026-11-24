package io.github.ppzxc.strand.load;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * nio·netty 전송 부하 하네스의 진입점(#422). 이 진입점이 하네스의 유일한 시험 seam이다.
 *
 * <pre>
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
                case "server":
                    return ServerRole.run(RunConfig.parse(withoutPort(rest)), out);
                case "loadgen":
                    return LoadGenRole.run(RunConfig.parse(withoutPort(rest)), portOf(rest), out);
                default:
                    err.println("모르는 명령 '" + command + "'. 지원: run");
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
