package com.example.messaging.inbox.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a bean method as the handler of {@code messaging.receivers.<receiver>}. Accepted shapes, with {@code T} the
 * payload type (deserialized with Jackson; {@code String} and {@code byte[]} get it raw):
 *
 * <pre>{@code
 * void on(T payload)
 * void on(T payload, InboxMessage message)
 * void on(TypedMessage<T> message)
 * HandlerResult on(...)   // any of the above, with an explicit result (ADR-0014)
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageListener {

    /** Name in {@code messaging.receivers}. */
    String receiver();
}
