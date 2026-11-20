package strand.deploytest;

import io.github.ppzxc.strand.core.StrandVersion;
import java.io.File;
import java.net.URISyntaxException;
import java.util.jar.JarFile;

/** 배포된 core, nio, netty를 받아 쓰는 외부 소비자. 실행 중인 세 모듈의 버전이 기대 버전과 같은지 확인한다. */
public final class Main {

    /** nio는 공개 API가 모두 internal 패키지라 컴파일 의존 없이 이름으로만 찾는다. */
    private static final String NIO_PROBE_CLASS = "io.github.ppzxc.strand.nio.internal.TcpPassage";

    /** netty는 nio와 같은 이유로 이름으로만 찾는다. Netty 타입을 함께 읽어, Netty가 경로에 없으면 실패한다. */
    private static final String NETTY_PROBE_CLASS = "io.github.ppzxc.strand.netty.NettyStrandClient";

    private static final String NETTY_TYPE = "io.netty.channel.EventLoopGroup";

    private Main() {}

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: Main <core 기대 버전> <nio 기대 버전> <netty 기대 버전>");
            System.exit(2);
        }
        boolean ok = check("", args[0], StrandVersion.get());
        ok &= check("strand-nio ", args[1], jarVersion(NIO_PROBE_CLASS, "nio"));
        ok &= check("strand-netty ", args[2], nettyVersion());
        if (!ok) {
            System.err.println("버전 불일치");
            System.exit(1);
        }
        System.out.println("OK");
    }

    private static boolean check(String label, String expected, String actual) {
        System.out.println(label + "expected=" + expected);
        System.out.println(label + "actual=" + actual);
        return expected.equals(actual);
    }

    private static String nettyVersion() {
        try {
            Class.forName(NETTY_TYPE, false, Main.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            return "(Netty 타입을 찾지 못함: " + NETTY_TYPE + ". 소비자가 Netty를 의존하지 않았거나 경로에 없음)";
        }
        return jarVersion(NETTY_PROBE_CLASS, "netty");
    }

    /** 탐침 클래스를 담은 jar 매니페스트의 Implementation-Version. 찾지 못하면 사유 문자열을 돌려줘 불일치로 실패시킨다. */
    private static String jarVersion(String probeClass, String name) {
        try {
            Class<?> probe = Class.forName(probeClass, false, Main.class.getClassLoader()); // 초기화 없이 위치만 찾는다
            File jar = new File(probe.getProtectionDomain().getCodeSource().getLocation().toURI());
            try (JarFile jarFile = new JarFile(jar)) {
                if (jarFile.getManifest() == null) {
                    return "(" + name + " jar에 매니페스트 없음)";
                }
                String version = jarFile.getManifest().getMainAttributes().getValue("Implementation-Version");
                return version == null ? "(Implementation-Version 없음)" : version;
            }
        } catch (ClassNotFoundException e) {
            return "(" + name + " 탐침 클래스를 찾지 못함: " + probeClass + ". " + name + "가 경로에 없거나 클래스 이름이 바뀜)";
        } catch (java.io.IOException | URISyntaxException | RuntimeException e) {
            return "(" + name + " jar 매니페스트를 읽지 못함: " + e + ")";
        }
    }
}
