package io.github.ppzxc.strand.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Records an approved, narrow, time-limited exception to a quality rule. */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Repeatable(PolicyExceptions.class)
public @interface PolicyException {
  /** Policy or tool rule identifier. */
  String rule();

  /** Reason the exception is safe. */
  String reason();

  /** Independent approver. */
  String approvedBy();

  /** ISO-8601 date; the exception expires at 00:00 UTC the following day. */
  String reviewBy();
}
