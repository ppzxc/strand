package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/** 블로킹 {@link Socket}으로 바이트를 보내고 한 번 읽는다. 사용법: {@code BlockingTcpExample host port} */
public final class BlockingTcpExample {

    private BlockingTcpExample() {}

    public static void main(String[] args) throws IOException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        System.out.println("strand core " + StrandVersion.get());
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            // 여기서 core가 인코딩한 바이트를 보낸다. 규격 구현 전이라 자리표시 바이트를 보낸다.
            out.write(new byte[] {0x01});
            out.flush();
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            System.out.println("수신 " + n + " 바이트");
        }
    }
}
