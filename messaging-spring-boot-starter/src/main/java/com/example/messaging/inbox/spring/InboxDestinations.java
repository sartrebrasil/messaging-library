package com.example.messaging.inbox.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.spring.MessagingProperties.Endpoint;

import java.util.List;

/**
 * Where receivers get their senders and receivers, by address. The default goes to {@code MessagingDestinations}
 * (messaging-lib starter); a bean of this type replaces it (tests use one over {@code InMemoryMessaging}).
 *
 * <p>{@code name} is the metrics tag and health key: the receiver name, or {@code <receiver>.dead-letter},
 * {@code <receiver>.failed} and {@code <receiver>.retry.<delay>} for the copies. {@code provider} {@code null} means
 * the only provider.
 */
public interface InboxDestinations {

    /** A new receiver per call; the inbox closes it. */
    MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter);

    /** One sender per name; the inbox never closes it. */
    MessageSender sender(String name, String provider, Endpoint endpoint);

    /** Problems of an address, without opening a connection; {@code key} is the full property path. */
    default List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
        return List.of();
    }
}
