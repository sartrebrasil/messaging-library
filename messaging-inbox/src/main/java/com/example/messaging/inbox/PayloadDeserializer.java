package com.example.messaging.inbox;

import java.lang.reflect.Type;

/**
 * Turns {@link InboxMessage#payload()} into the handler's type (ADR-0003). The starter provides one on the
 * application's Jackson mapper.
 */
@FunctionalInterface
public interface PayloadDeserializer {

    /** @throws PayloadDeserializationException when the payload cannot be read as {@code type} */
    Object deserialize(InboxMessage message, Type type);

    @SuppressWarnings("unchecked")
    default <T> TypedMessage<T> typed(InboxMessage message, Class<T> type) {
        return new TypedMessage<>(message, (T) deserialize(message, type));
    }
}
