package io.github.ppzxc.strand.quality;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.domain.JavaAnnotation;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMember;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.stream.Stream;

public final class PolicyExceptionArchRule {
    private static final String EXCEPTION_NAME = "io.github.ppzxc.strand.api.PolicyException";
    private static final String EXCEPTIONS_NAME = "io.github.ppzxc.strand.api.PolicyExceptions";

    private PolicyExceptionArchRule() {}

    public static void check(JavaClasses importedClasses, LocalDate today) {
        classes().should(notHaveExpiredExceptions(today)).check(importedClasses);
    }

    private static ArchCondition<JavaClass> notHaveExpiredExceptions(LocalDate today) {
        return new ArchCondition<JavaClass>("not have expired @PolicyException annotations") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                Stream.concat(
                                Stream.of(item.getAnnotations()),
                                Stream.concat(
                                        item.getMethods().stream().map(JavaMember::getAnnotations),
                                        Stream.concat(
                                                item.getConstructors().stream().map(JavaMember::getAnnotations),
                                                item.getFields().stream().map(JavaMember::getAnnotations))))
                        .flatMap(annotations -> annotations.stream())
                        .flatMap(PolicyExceptionArchRule::policyExceptions)
                        .forEach(annotation -> checkReviewDate(item, annotation, today, events));
            }
        };
    }

    private static Stream<JavaAnnotation<?>> policyExceptions(JavaAnnotation<?> annotation) {
        if (annotation.getRawType().getName().equals(EXCEPTION_NAME)) {
            return Stream.of(annotation);
        }
        if (annotation.getRawType().getName().equals(EXCEPTIONS_NAME)) {
            Object values = annotation.get("value").get();
            return Arrays.stream((JavaAnnotation<?>[]) values);
        }
        return Stream.empty();
    }

    private static void checkReviewDate(
            JavaClass item, JavaAnnotation<?> annotation, LocalDate today, ConditionEvents events) {
        String reviewBy = annotation.get("reviewBy").get().toString();
        LocalDate date = LocalDate.parse(reviewBy);
        boolean expired = today.isAfter(date);
        events.add(new SimpleConditionEvent(
                item, !expired, item.getName() + " has expired @PolicyException reviewBy=" + reviewBy));
    }
}
