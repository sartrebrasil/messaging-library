package com.example.messaging.inbox;

/** Handler that returns normally on success, which equals {@link HandlerResult#ack()} (ADR-0014). */
@FunctionalInterface
public interface InboxHandler<T> {

    void handle(TypedMessage<T> message) throws Exception;
}
