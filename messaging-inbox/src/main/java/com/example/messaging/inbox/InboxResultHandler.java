package com.example.messaging.inbox;

/** Handler with an explicit result (ADR-0014). {@code null} counts as {@link HandlerResult#ack()}. */
@FunctionalInterface
public interface InboxResultHandler<T> {

    HandlerResult handle(TypedMessage<T> message) throws Exception;

    static <T> InboxResultHandler<T> of(InboxHandler<T> handler) {
        return message -> {
            handler.handle(message);
            return HandlerResult.ack();
        };
    }
}
