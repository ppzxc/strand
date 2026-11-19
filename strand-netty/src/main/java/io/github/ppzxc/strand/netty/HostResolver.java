package io.github.ppzxc.strand.netty;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** 호스트 이름을 주소 목록으로 해석한다. 블로킹할 수 있으므로 앱용 실행기에서 부른다. */
@FunctionalInterface
interface HostResolver {
    /** 시스템 해석기. */
    HostResolver SYSTEM = InetAddress::getAllByName;

    InetAddress[] resolve(String host) throws UnknownHostException;
}
