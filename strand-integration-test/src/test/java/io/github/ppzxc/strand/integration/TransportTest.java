package io.github.ppzxc.strand.integration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest(name = "[{0}] {displayName}")
@EnumSource(Transport.class)
@interface TransportTest {}
