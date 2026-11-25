package io.github.ppzxc.strand.nio;

@FunctionalInterface
public interface CredentialVerifier {

    AuthenticationResult verify(Credentials credentials) throws Exception;
}
