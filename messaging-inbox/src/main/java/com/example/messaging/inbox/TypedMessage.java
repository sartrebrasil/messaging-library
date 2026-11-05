package com.example.messaging.inbox;

import java.util.Objects;

/** An {@link InboxMessage} with its payload already deserialized: what handlers receive (ADR-0003). */
public record TypedMessage<T>(InboxMessage message, T payload) {

    public TypedMessage {
        Objects.requireNonNull(message, "message");
    }
}
