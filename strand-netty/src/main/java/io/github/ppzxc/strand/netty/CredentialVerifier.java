package io.github.ppzxc.strand.netty;

@FunctionalInterface
public interface CredentialVerifier {

    AuthenticationResult verify(Credentials credentials) throws Exception;
}
