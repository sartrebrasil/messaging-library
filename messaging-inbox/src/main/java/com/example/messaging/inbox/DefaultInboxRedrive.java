package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.redrive.Filter;
import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.FailureAttribute;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Redrive over a {@link PassReader} (ADR-0012): what passes the filter goes to the target with the attempts reset and
 * without {@code inbox_failure}; the rest, and everything in a dry run, goes unchanged to the end of the source. One run
 * per receiver and source at a time on this instance.
 */
public final class DefaultInboxRedrive implements InboxRedrive {

    private static final System.Logger LOG = System.getLogger(DefaultInboxRedrive.class.getName());
    private static final Duration POLL_WAIT = Duration.ofSeconds(2);
    private static final String NONE = "none";

    private final Map<String, RedriveRoutes> routes;
    private final Clock clock;
    private final Duration pollWait;
    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final EnvelopeReader envelopes = new EnvelopeReader(EnvelopeMode.AUTO);

    public DefaultInboxRedrive(Map<String, RedriveRoutes> routes, Clock clock) {
        this(routes, clock, POLL_WAIT);
    }

    DefaultInboxRedrive(Map<String, RedriveRoutes> routes, Clock clock, Duration pollWait) {
        this.routes = Map.copyOf(routes);
        this.clock = clock;
        this.pollWait = pollWait;
    }

    /** The receivers and where their redrive reads and writes. */
    public Map<String, RedriveRoutes> routes() {
        return routes;
    }

    @Override
    public RedriveReport redrive(RedriveRequest request) {
        RedriveRoutes route = routes.get(request.consumer());
        if (route == null) {
            throw new NoSuchElementException("unknown receiver: " + request.consumer());
        }
        Supplier<MessageReceiver> sourceReceivers = sourceReceivers(request, route);
        MessageSender sourceSender = request.source() == Source.FAILED ? route.failed() : route.deadLetter();
        MessageSender target = target(request, route);
        String key = request.consumer() + "/" + request.source();
        if (!running.add(key)) {
            throw new IllegalStateException("a redrive of " + request.source().name().toLowerCase() + " of receiver "
                    + request.consumer() + " is in progress");
        }
        try {
            return run(request, sourceReceivers, sourceSender, target);
        } finally {
            running.remove(key);
        }
    }

    private static Supplier<MessageReceiver> sourceReceivers(RedriveRequest request, RedriveRoutes route) {
        String reason = route.unavailable().get(request.source());
        if (reason != null) {
            throw new IllegalArgumentException("receiver " + request.consumer() + ": "
                    + request.source().name().toLowerCase() + " cannot be read: " + reason);
        }
        if (request.source() == Source.DEAD_LETTER && route.deadLetterReceivers() == null) {
            throw new IllegalArgumentException("receiver " + request.consumer() + " has no dead-letter destination");
        }
        return request.source() == Source.FAILED ? route.failedReceivers() : route.deadLetterReceivers();
    }

    private static MessageSender target(RedriveRequest request, RedriveRoutes route) {
        if (request.target() == Target.DEAD_LETTER) {
            if (route.deadLetter() == null) {
                throw new IllegalArgumentException("receiver " + request.consumer()
                        + " has no dead-letter destination");
            }
            return route.deadLetter();
        }
        if (route.main() == null) {
            throw new IllegalArgumentException("receiver " + request.consumer() + " reads a subscription: a redrive "
                    + "to the main destination would publish to every subscription of the topic");
        }
        return route.main();
    }

    private RedriveReport run(RedriveRequest request, Supplier<MessageReceiver> sourceReceivers,
                              MessageSender sourceSender, MessageSender target) {
        String redriveId = UUID.randomUUID().toString();
        Instant runStart = clock.instant();
        long start = System.nanoTime();
        long interval = 1_000_000_000L / request.maxPerSecond();
        long nextMove = start;
        Map<String, Integer> byException = new TreeMap<>();
        Map<String, Integer> byFingerprint = new TreeMap<>();
        int moved = 0;
        int skipped = 0;
        int errors = 0;
        PassEnd end;
        LOG.log(System.Logger.Level.INFO, "Redrive {0} started: receiver {1}, {2} to {3}, filter {4}, dryRun {5}",
                redriveId, request.consumer(), name(request.source()), name(request.target()), request.filter(),
                request.dryRun());
        try (MessageReceiver receiver = sourceReceivers.get()) {
            PassReader reader = new PassReader(receiver, runStart, request.limit(), this::keyOf, new StopSignal(),
                    pollWait, request.consumer());
            for (var next = reader.next(); next.isPresent(); next = reader.next()) {
                ReceivedMessage message = next.get();
                Optional<FailureAttribute> failure = FailureAttribute.parse(
                        message.attributes().get(FailureAttribute.NAME));
                byException.merge(failure.map(FailureAttribute::exception).orElse(NONE), 1, Integer::sum);
                byFingerprint.merge(failure.map(FailureAttribute::fingerprint).orElse(NONE), 1, Integer::sum);
                boolean move = !request.dryRun() && passes(request.filter(), message, failure);
                if (move) {
                    // at most maxPerSecond: time spent skipping is no credit for a burst
                    for (long wait = nextMove - System.nanoTime(); wait > 0; wait = nextMove - System.nanoTime()) {
                        LockSupport.parkNanos(wait);
                    }
                    nextMove = System.nanoTime() + interval;
                }
                if (copy(receiver, message, move ? target : sourceSender, move, redriveId)) {
                    if (move) {
                        moved++;
                    } else {
                        skipped++;
                    }
                } else {
                    errors++;
                }
            }
            end = reader.end();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Redrive " + redriveId + ": source of receiver "
                    + request.consumer() + " unavailable; run ended", e);
            end = PassEnd.RECEIVE_FAILED;
        }
        LOG.log(System.Logger.Level.INFO, "Redrive {0} ended ({1}) after {2} ms: receiver {3}, {4} to {5}, filter "
                        + "{6}, dryRun {7}: {8} moved, {9} skipped, {10} errors", redriveId, name(end),
                Duration.ofNanos(System.nanoTime() - start).toMillis(), request.consumer(), name(request.source()),
                name(request.target()), request.filter(), request.dryRun(), moved, skipped, errors);
        return new RedriveReport(redriveId, end, moved, skipped, errors, byException, byFingerprint);
    }

    private String keyOf(ReceivedMessage message) {
        try {
            return envelopes.read(message, "redrive").id(); // the destination does not change the id
        } catch (RuntimeException e) {
            return message.messageId();
        }
    }

    private static boolean passes(Filter filter, ReceivedMessage message, Optional<FailureAttribute> failure) {
        if (filter.exception() != null
                && !filter.exception().equals(failure.map(FailureAttribute::exception).orElse(NONE))) {
            return false;
        }
        if (filter.fingerprint() != null
                && !filter.fingerprint().equals(failure.map(FailureAttribute::fingerprint).orElse(NONE))) {
            return false;
        }
        if (filter.reason() != null
                && (message.deadLetter() == null || !message.deadLetter().reason().startsWith(filter.reason()))) {
            return false;
        }
        Instant enqueuedAt = message.enqueuedAt();
        if (filter.enqueuedBefore() != null && (enqueuedAt == null || !enqueuedAt.isBefore(filter.enqueuedBefore()))) {
            return false;
        }
        return filter.enqueuedAfter() == null || (enqueuedAt != null && enqueuedAt.isAfter(filter.enqueuedAfter()));
    }

    /** Attempts reset, no {@code inbox_failure}, no dead-letter origin. */
    private static OutgoingMessage moved(ReceivedMessage message, String redriveId) {
        return Copies.build(message, AttemptState.redriven(redriveId), null).message()
                .withDeduplicationId(deduplicationId(message, redriveId));
    }

    /** Back to the end of the source as it was. */
    private static OutgoingMessage unchanged(ReceivedMessage message, String redriveId) {
        return OutgoingMessage.copyOf(message).withDeadLetter(message.deadLetter())
                .withDeduplicationId(deduplicationId(message, redriveId));
    }

    /**
     * New for the entity, stable within the run: with the source id, Service Bus duplicate detection (MessageId =
     * deduplicationId) would drop a copy the main queue or the source already saw, and the ack would lose it.
     */
    private static String deduplicationId(ReceivedMessage message, String redriveId) {
        return Copies.deduplicationId(redriveId + ":" + message.messageId());
    }

    /**
     * Builds the copy, sends it, then acks; when the copy cannot be built (a message from outside the lib that breaks
     * the sending rules) or sent, the message goes back to the source and {@code false} is returned.
     */
    private static boolean copy(MessageReceiver receiver, ReceivedMessage message, MessageSender sender, boolean move,
                                String redriveId) {
        try {
            sender.send(move ? moved(message, redriveId) : unchanged(message, redriveId));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Redrive " + redriveId + ": copy of message " + message.messageId()
                    + " failed; it stays in the source", e);
            try {
                receiver.nack(message, Duration.ZERO);
            } catch (RuntimeException nack) {
                LOG.log(System.Logger.Level.WARNING, "Redrive " + redriveId + ": nack of message "
                        + message.messageId() + " failed; it comes back when the lease expires", nack);
            }
            return false;
        }
        try {
            receiver.ack(message);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Redrive " + redriveId + ": ack of message " + message.messageId()
                    + " failed after its copy; it comes back when the lease expires and may be copied twice", e);
        }
        return true;
    }

    private static String name(Enum<?> value) {
        return value.name().toLowerCase();
    }
}
