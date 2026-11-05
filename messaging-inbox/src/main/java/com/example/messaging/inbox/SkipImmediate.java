package com.example.messaging.inbox;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotated exceptions skip the immediate level and go straight to the delayed level.
 * Typical use: HTTP 429/503 and rate limits, where retrying in 100 ms only makes it worse.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface SkipImmediate {
}
