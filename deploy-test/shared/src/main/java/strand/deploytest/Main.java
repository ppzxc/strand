package strand.deploytest;

import io.github.ppzxc.strand.core.StrandVersion;
import java.io.File;
import java.net.URISyntaxException;
import java.util.jar.JarFile;

public final class Main {

    private static final String NIO_PROBE_CLASS = "io.github.ppzxc.strand.nio.internal.TcpPassage";

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

    private static String jarVersion(String probeClass, String name) {
        try {
            Class<?> probe = Class.forName(probeClass, false, Main.class.getClassLoader());
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
