package strand.deploytest;

import io.github.ppzxc.strand.core.StrandVersion;

/** 배포된 core를 받아 쓰는 외부 소비자. 실행 중인 core 버전이 기대 버전과 같은지 확인한다. */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: Main <기대 버전>");
            System.exit(2);
        }
        String expected = args[0];
        String actual = StrandVersion.get();
        System.out.println("expected=" + expected);
        System.out.println("actual=" + actual);
        if (!expected.equals(actual)) {
            System.err.println("버전 불일치");
            System.exit(1);
        }
        System.out.println("OK");
    }
}
