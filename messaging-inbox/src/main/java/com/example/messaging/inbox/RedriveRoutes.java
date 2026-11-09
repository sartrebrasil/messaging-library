package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.redrive.Source;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Where a receiver's redrive reads and writes.
 *
 * @param deadLetterReceivers {@code null} (with {@code deadLetter}) when the receiver has no DLQ
 * @param main                {@code null} when the main destination cannot take a redrive (a topic with a
 *                            subscription: sending would reach every subscription)
 * @param unavailable         sources that cannot be read, with the reason
 */
public record RedriveRoutes(Supplier<MessageReceiver> failedReceivers, MessageSender failed,
                            Supplier<MessageReceiver> deadLetterReceivers, MessageSender deadLetter,
                            MessageSender main, Map<Source, String> unavailable) {

    public RedriveRoutes {
        unavailable = Map.copyOf(unavailable);
    }
}
