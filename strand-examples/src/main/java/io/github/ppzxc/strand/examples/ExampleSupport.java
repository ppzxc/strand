package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.Credentials;
import java.nio.charset.StandardCharsets;

final class ExampleSupport {

    static final int DEMO_REQUEST_TYPE = 256;

    static final int STATUS_OK = 0;

    static final long TIMEOUT_SECONDS = 5;

    static final String AUTH_MECHANISM = "BEARER";

    static final byte[] AUTH_CREDENTIALS = "demo-token".getBytes(StandardCharsets.UTF_8);

    private ExampleSupport() {}

    static Credentials credentials() {
        return Credentials.of(AUTH_MECHANISM, AUTH_CREDENTIALS);
    }

    static void handleResult(RequestResult result) {
        switch (result.certainty()) {
            case RESPONSE:
                System.out.println("  -> 응답을 받았다. status=" + result.status() + " body="
                        + new String(result.payload(), StandardCharsets.UTF_8));
                break;
            case NOT_PROCESSED:
                System.out.println("  -> 처리되지 않았다. 다시 보내도 안전하다");
                break;
            case UNKNOWN:
            default:
                System.out.println("  -> 처리 여부를 알 수 없다. 멱등 키 없이 다시 보내지 않는다");
                break;
        }
    }

    static String arg(String[] args, int index, String fallback) {
        return args.length > index ? args[index] : fallback;
    }
}
