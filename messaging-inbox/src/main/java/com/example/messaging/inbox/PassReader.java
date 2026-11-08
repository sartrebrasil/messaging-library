package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.ReceivedMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Reads a destination one message at a time until an end condition (a dead-letter pass, a redrive): a message enqueued
 * at or after {@code runStart} (a copy made during the run), a key already seen, {@code limit} messages read, an empty
 * receive, a stop or a receive failure. The message that ended the run goes back with {@code nack(0)}. Never throws.
 */
final class PassReader {

    private static final System.Logger LOG = System.getLogger(PassReader.class.getName());

    private final MessageReceiver receiver;
    private final Instant runStart;
    private final int limit;
    private final Function<ReceivedMessage, String> key;
    private final StopSignal stop;
    private final Duration pollWait;
    private final String consumer;
    private final Set<String> seen = new HashSet<>();
    private int read;
    private PassEnd end;

    PassReader(MessageReceiver receiver, Instant runStart, int limit, Function<ReceivedMessage, String> key,
               StopSignal stop, Duration pollWait, String consumer) {
        this.receiver = receiver;
        this.runStart = runStart;
        this.limit = limit;
        this.key = key;
        this.stop = stop;
        this.pollWait = pollWait;
        this.consumer = consumer;
    }

    /** The next message, or empty once the run ended ({@link #end()} says why). */
    Optional<ReceivedMessage> next() {
        if (end != null) {
            return Optional.empty();
        }
        if (stop.stopping()) {
            return ended(PassEnd.STOPPING);
        }
        List<ReceivedMessage> received;
        try {
            received = receiver.receive(1, pollWait);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + consumer + ": receive failed; run ended", e);
            return ended(PassEnd.RECEIVE_FAILED);
        }
        if (received.isEmpty()) {
            return ended(PassEnd.EMPTY);
        }
        ReceivedMessage message = received.getFirst();
        PassEnd ending = endingAt(message);
        if (ending != null) {
            giveBack(message);
            return ended(ending);
        }
        read++;
        return Optional.of(message);
    }

    /** Why the run ended; {@code null} while it goes on. */
    PassEnd end() {
        return end;
    }

    /** Messages handed out by {@link #next()}. */
    int read() {
        return read;
    }

    private Optional<ReceivedMessage> ended(PassEnd why) {
        end = why;
        return Optional.empty();
    }

    private PassEnd endingAt(ReceivedMessage message) {
        if (message.enqueuedAt() != null && !message.enqueuedAt().isBefore(runStart)) {
            return PassEnd.NEWER_THAN_PASS;
        }
        if (!seen.add(key.apply(message))) {
            return PassEnd.REPEATED_KEY;
        }
        return read >= limit ? PassEnd.MAX_PER_PASS : null;
    }

    private void giveBack(ReceivedMessage message) {
        try {
            receiver.nack(message, Duration.ZERO);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + consumer + ": nack of message " + message.messageId()
                    + " failed at the end of a run; it comes back when the lease expires", e);
        }
    }
}
