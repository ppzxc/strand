package io.github.ppzxc.strand.integration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 같은 시험 본문을 {@link Transport}마다 한 번씩 돌린다. 시험 메서드는 {@link Transport} 매개변수 하나를 받는다. 호출 이름은 {@code [nio] 표시 이름}과
 * {@code [netty] 표시 이름}이라 리포트에 전송별 결과가 따로 나온다.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest(name = "[{0}] {displayName}")
@EnumSource(Transport.class)
@interface TransportTest {}
