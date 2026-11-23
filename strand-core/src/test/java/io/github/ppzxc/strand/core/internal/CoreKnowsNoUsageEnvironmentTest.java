package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * core는 사용 환경(TCP인지 WebSocket인지)을 모른다. core의 타입과 메서드, 열거 상수 어디에도 WebSocket이라는 개념이 없고, 입력은 스트림 입력과 단위
 * 입력 둘뿐이다(ADR 0006).
 */
class CoreKnowsNoUsageEnvironmentTest {

    private static List<Class<?>> coreClasses() throws Exception {
        File root = new File(Endpoint.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
        String packageName = Endpoint.class.getPackage().getName();
        File directory = new File(root, packageName.replace('.', '/'));
        List<Class<?>> classes = new ArrayList<>();
        for (File file : directory.listFiles()) {
            String name = file.getName();
            if (name.endsWith(".class") && !name.equals("package-info.class")) {
                classes.add(Class.forName(packageName + "." + name.replace(".class", "")));
            }
        }
        return classes;
    }

    private static boolean mentionsUsageEnvironment(String name) {
        return name.toLowerCase(java.util.Locale.ROOT).contains("websocket");
    }

    @Test
    void noTypeNameMentionsWebSocket() throws Exception {
        assertThat(coreClasses())
                .extracting(Class::getName)
                .noneMatch(CoreKnowsNoUsageEnvironmentTest::mentionsUsageEnvironment);
    }

    @Test
    void noMethodOrConstantNameMentionsWebSocket() throws Exception {
        List<String> names = new ArrayList<>();
        for (Class<?> type : coreClasses()) {
            for (Method method : type.getDeclaredMethods()) {
                names.add(type.getSimpleName() + "." + method.getName());
            }
            for (Class<?> nested : type.getDeclaredClasses()) {
                if (nested.isEnum()) {
                    for (Object constant : nested.getEnumConstants()) {
                        names.add(nested.getSimpleName() + "." + constant);
                    }
                }
            }
        }
        assertThat(names).isNotEmpty().noneMatch(CoreKnowsNoUsageEnvironmentTest::mentionsUsageEnvironment);
    }

    @Test
    void outboundMessageHasOnlyBytes() {
        assertThat(OutboundMessage.class.getDeclaredClasses()).isEmpty();
        assertThat(OutboundMessage.class.getDeclaredMethods())
                .extracting(Method::getName)
                .doesNotContain("kind");
    }
}
