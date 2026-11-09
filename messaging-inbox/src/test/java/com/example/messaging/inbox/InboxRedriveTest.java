package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.SendResult;
import com.example.messaging.inbox.redrive.Filter;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.FailureAttribute;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Redrive (ADR-0012) over the in-memory provider, synchronously. */
class InboxRedriveTest {

    private static final String MAIN = "orders";
    private static final String DLQ = "orders-dlq";
    private static final String FAILED = "orders-failed";

    private final InMemoryMessaging messaging = new InMemoryMessaging();

    InboxRedriveTest() {
        messaging.createQueue(MAIN, QueueOptions.defaults());
        messaging.createQueue(DLQ, QueueOptions.defaults().withLease(Duration.ofSeconds(5)));
        messaging.createQueue(FAILED, QueueOptions.defaults().withLease(Duration.ofSeconds(5)));
    }

    private RedriveRoutes routes(MessageSender main, Map<Source, String> unavailable) {
        return new RedriveRoutes(() -> messaging.receiver(FAILED), messaging.queueSender(FAILED),
                () -> messaging.receiver(DLQ), messaging.queueSender(DLQ), main, unavailable);
    }

    private DefaultInboxRedrive redrive(RedriveRoutes routes) {
        return new DefaultInboxRedrive(Map.of("billing", routes), Clock.systemUTC(), Duration.ofMillis(20));
    }

    private DefaultInboxRedrive redrive() {
        return redrive(routes(messaging.queueSender(MAIN), Map.of()));
    }

    private static String envelope(String id) {
        return "{\"messageId\":\"" + id + "\",\"eventType\":\"E\",\"payload\":\"hi\"}";
    }

    private void fail(String queue, String id, Throwable cause, String reason) {
        OutgoingMessage message = OutgoingMessage.ofText(envelope(id))
                .withAttribute(AttemptState.ATTRIBUTE, "l2=5;l3=3")
                .withAttribute("tenant", "acme")
                .withDeadLetter(new DeadLetterInfo("src-" + id, reason));
        if (cause != null) {
            message = message.withAttribute(FailureAttribute.NAME,
                    FailureAttribute.next(null, "billing", MAIN, cause, Instant.now()).format());
        }
        messaging.queueSender(queue).send(message);
    }

    private void fail(String id, Throwable cause) {
        fail(FAILED, id, cause, cause.getClass().getName() + ": " + cause.getMessage());
    }

    /** The run starts after what was enqueued before it. */
    private static void settle() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static RedriveRequest request(Source source, Target target, Filter filter, boolean dryRun) {
        return new RedriveRequest("billing", source, target, 100, filter, 1000, dryRun);
    }

    private static RedriveRequest failedToMain(Filter filter) {
        return request(Source.FAILED, Target.MAIN, filter, false);
    }

    private List<ReceivedMessage> drain(String queue) {
        return messaging.receiver(queue).receive(100, Duration.ZERO);
    }

    private static Filter exception(String exception) {
        return new Filter(exception, null, null, null, null);
    }

    @Test
    void failedToMainResetsAttempts() {
        fail("m-1", new IllegalStateException("down"));
        settle();

        RedriveReport report = redrive().redrive(failedToMain(Filter.NONE));

        assertEquals(1, report.moved());
        assertEquals(0, report.skipped());
        assertEquals(PassEnd.EMPTY, report.end());
        ReceivedMessage moved = drain(MAIN).getFirst();
        assertEquals("l2=0;l3=0;redrive=" + report.redriveId(), moved.attributes().get(AttemptState.ATTRIBUTE));
        assertNull(moved.attributes().get(FailureAttribute.NAME));
        assertEquals("acme", moved.attributes().get("tenant"));
        assertEquals(envelope("m-1"), moved.bodyAsString());
        assertEquals(0, messaging.size(FAILED));
    }

    @Test
    void failedToDeadLetter() {
        fail("m-1", new IllegalStateException("down"));
        settle();

        RedriveReport report = redrive().redrive(request(Source.FAILED, Target.DEAD_LETTER, Filter.NONE, false));

        assertEquals(1, report.moved());
        assertEquals(1, drain(DLQ).size());
        assertEquals(0, messaging.size(FAILED));
        assertEquals(0, messaging.size(MAIN));
    }

    @Test
    void deadLetterToMain() {
        fail(DLQ, "m-1", new IllegalStateException("down"), "down");
        settle();

        RedriveReport report = redrive().redrive(request(Source.DEAD_LETTER, Target.MAIN, Filter.NONE, false));

        assertEquals(1, report.moved());
        assertEquals(1, drain(MAIN).size());
        assertEquals(0, messaging.size(DLQ));
    }

    @Test
    void filterByException() {
        fail("m-1", new IllegalStateException("down"));
        fail("m-2", new UnsupportedOperationException("nope"));
        settle();

        RedriveReport report = redrive().redrive(failedToMain(exception("java.lang.UnsupportedOperationException")));

        assertEquals(1, report.moved());
        assertEquals(1, report.skipped());
        assertEquals(envelope("m-2"), drain(MAIN).getFirst().bodyAsString());
        assertEquals(envelope("m-1"), drain(FAILED).getFirst().bodyAsString());
    }

    @Test
    void filterByFingerprint() {
        IllegalStateException wanted = new IllegalStateException("down");
        String fingerprint = FailureAttribute.next(null, "billing", MAIN, wanted, Instant.now()).fingerprint();
        fail("m-1", wanted);
        fail("m-2", new IllegalStateException("other") {
        });
        settle();

        RedriveReport report = redrive().redrive(failedToMain(new Filter(null, fingerprint, null, null, null)));

        assertEquals(1, report.moved());
        assertEquals(envelope("m-1"), drain(MAIN).getFirst().bodyAsString());
    }

    @Test
    void filterByReasonPrefix() {
        fail(FAILED, "m-1", new IllegalStateException("down"), "rejected: bad tax id");
        fail(FAILED, "m-2", new IllegalStateException("down"), "retries_exhausted");
        settle();

        RedriveReport report = redrive().redrive(failedToMain(new Filter(null, null, "rejected", null, null)));

        assertEquals(1, report.moved());
        assertEquals(envelope("m-1"), drain(MAIN).getFirst().bodyAsString());
    }

    @Test
    void filterByDate() {
        fail("m-1", new IllegalStateException("down"));
        settle();
        Instant between = Instant.now();
        settle();
        fail("m-2", new IllegalStateException("down"));
        settle();

        RedriveReport report = redrive().redrive(failedToMain(new Filter(null, null, null, between, null)));

        assertEquals(1, report.moved());
        assertEquals(envelope("m-1"), drain(MAIN).getFirst().bodyAsString());
    }

    @Test
    void aMessageWithoutEnqueuedAtNeverPassesADateFilter() {
        fail("m-1", new IllegalStateException("down"));
        settle();
        MessageReceiver noEnqueuedAt = new InboxConsumerTest.Delegating(messaging.receiver(FAILED)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                return super.receive(maxMessages, maxWait).stream().map(m -> new ReceivedMessage(m.messageId(),
                        m.body(), m.attributes(), m.contentType(), m.orderingKey(), m.deliveryCount(), null,
                        m.receivedAt(), m.leaseExpiresAt(), m.traceparent(), m.deadLetter(), m.handle())).toList();
            }
        };
        RedriveRoutes routes = new RedriveRoutes(() -> noEnqueuedAt, messaging.queueSender(FAILED), null, null,
                messaging.queueSender(MAIN), Map.of());

        RedriveReport report = redrive(routes).redrive(
                failedToMain(new Filter(null, null, null, null, Instant.EPOCH)));

        assertEquals(0, report.moved());
        assertEquals(1, report.skipped());
        assertEquals(PassEnd.REPEATED_KEY, report.end());
    }

    @Test
    void skippedGoToTheEndAndTheRunGoesOn() {
        fail("m-1", new IllegalStateException("down"));
        fail("m-2", new IllegalStateException("down"));
        fail("m-3", new UnsupportedOperationException("nope"));
        settle();

        RedriveReport report = redrive().redrive(failedToMain(exception("java.lang.UnsupportedOperationException")));

        assertEquals(1, report.moved());
        assertEquals(2, report.skipped());
        assertEquals(PassEnd.NEWER_THAN_PASS, report.end());
        assertEquals(List.of(envelope("m-1"), envelope("m-2")),
                drain(FAILED).stream().map(ReceivedMessage::bodyAsString).toList());
    }

    @Test
    void skippedCopiesKeepTheFailure() {
        fail(FAILED, "m-1", new IllegalStateException("down"), "rejected: bad tax id");
        settle();

        redrive().redrive(failedToMain(exception("none")));

        ReceivedMessage kept = drain(FAILED).getFirst();
        assertNotNull(kept.attributes().get(FailureAttribute.NAME));
        assertEquals("l2=5;l3=3", kept.attributes().get(AttemptState.ATTRIBUTE));
        assertEquals("rejected: bad tax id", kept.deadLetter().reason());
    }

    @Test
    void dryRunMovesNothingAndGroups() {
        fail("m-1", new IllegalStateException("down"));
        fail("m-2", new IllegalStateException("down"));
        fail(FAILED, "m-3", null, "from outside the inbox");
        settle();

        RedriveReport report = redrive().redrive(request(Source.FAILED, Target.MAIN, Filter.NONE, true));

        assertEquals(0, report.moved());
        assertEquals(3, report.skipped());
        assertEquals(Map.of("java.lang.IllegalStateException", 2, "none", 1), report.byException());
        assertEquals(2, report.byFingerprint().size());
        assertEquals(1, report.byFingerprint().get("none"));
        assertEquals(0, messaging.size(MAIN));
        assertEquals(3, messaging.size(FAILED));
    }

    @Test
    void aMessageFromOutsideTheInboxIsMoved() {
        fail(FAILED, "m-1", null, "from outside the inbox");
        settle();

        RedriveReport report = redrive().redrive(failedToMain(exception("none")));

        assertEquals(1, report.moved());
        assertEquals(Map.of("none", 1), report.byException());
    }

    @Test
    void limitEndsTheRun() {
        for (int i = 0; i < 5; i++) {
            fail("m-" + i, new IllegalStateException("down"));
        }
        settle();

        RedriveReport report = redrive().redrive(
                new RedriveRequest("billing", Source.FAILED, Target.MAIN, 2, Filter.NONE, 1000, false));

        assertEquals(2, report.moved());
        assertEquals(PassEnd.MAX_PER_PASS, report.end());
        assertEquals(3, messaging.size(FAILED));
    }

    @Test
    void maxPerSecondPaces() {
        for (int i = 0; i < 5; i++) {
            fail("m-" + i, new IllegalStateException("down"));
        }
        settle();
        long start = System.nanoTime();

        RedriveReport report = redrive().redrive(
                new RedriveRequest("billing", Source.FAILED, Target.MAIN, 100, Filter.NONE, 10, false));

        assertEquals(5, report.moved());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() >= 400);
    }

    @Test
    void copyFailureCountsAndGoesOn() {
        fail("m-1", new IllegalStateException("down"));
        settle();
        MessageSender broken = new MessageSender() {
            @Override
            public SendResult send(OutgoingMessage message) {
                throw new IllegalStateException("broker down");
            }

            @Override
            public void checkAccess() {
            }

            @Override
            public Capabilities capabilities() {
                return messaging.queueSender(MAIN).capabilities();
            }

            @Override
            public void close() {
            }
        };

        RedriveReport report = redrive(routes(broken, Map.of())).redrive(failedToMain(Filter.NONE));

        assertEquals(0, report.moved());
        assertEquals(1, report.errors());
        assertEquals(PassEnd.REPEATED_KEY, report.end());
        assertEquals(1, messaging.size(FAILED));
    }

    @Test
    void invalidRequests() {
        assertThrows(IllegalArgumentException.class, () -> request(Source.DEAD_LETTER, Target.DEAD_LETTER,
                Filter.NONE, false));
        assertThrows(IllegalArgumentException.class, () -> new RedriveRequest("billing", Source.FAILED,
                Target.MAIN, 0, Filter.NONE, 10, false));
        assertThrows(IllegalArgumentException.class, () -> new RedriveRequest("billing", Source.FAILED,
                Target.MAIN, 10_001, Filter.NONE, 10, false));
        assertThrows(IllegalArgumentException.class, () -> new RedriveRequest("billing", Source.FAILED,
                Target.MAIN, 10, Filter.NONE, 0, false));

        RedriveRoutes noDeadLetter = new RedriveRoutes(() -> messaging.receiver(FAILED),
                messaging.queueSender(FAILED), null, null, messaging.queueSender(MAIN), Map.of());
        assertThrows(IllegalArgumentException.class, () -> redrive(noDeadLetter).redrive(
                request(Source.FAILED, Target.DEAD_LETTER, Filter.NONE, false)));
        assertThrows(IllegalArgumentException.class, () -> redrive(noDeadLetter).redrive(
                request(Source.DEAD_LETTER, Target.MAIN, Filter.NONE, false)));

        IllegalArgumentException subscription = assertThrows(IllegalArgumentException.class,
                () -> redrive(routes(null, Map.of())).redrive(failedToMain(Filter.NONE)));
        assertTrue(subscription.getMessage().contains("subscription"), subscription.getMessage());

        IllegalArgumentException unreadable = assertThrows(IllegalArgumentException.class,
                () -> redrive(routes(messaging.queueSender(MAIN), Map.of(Source.FAILED, "a topic cannot be read")))
                        .redrive(failedToMain(Filter.NONE)));
        assertTrue(unreadable.getMessage().contains("a topic cannot be read"), unreadable.getMessage());
    }

    @Test
    void unknownConsumer() {
        assertThrows(NoSuchElementException.class, () -> redrive().redrive(
                new RedriveRequest("shipping", Source.FAILED, Target.MAIN, 10, Filter.NONE, 10, false)));
    }

    @Test
    void concurrentRunIsRejected() throws Exception {
        fail("m-1", new IllegalStateException("down"));
        settle();
        CountDownLatch receiving = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MessageReceiver slow = new InboxConsumerTest.Delegating(messaging.receiver(FAILED)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                receiving.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.receive(maxMessages, maxWait);
            }
        };
        DefaultInboxRedrive redrive = redrive(new RedriveRoutes(() -> slow, messaging.queueSender(FAILED),
                () -> messaging.receiver(DLQ), messaging.queueSender(DLQ), messaging.queueSender(MAIN), Map.of()));

        CompletableFuture<RedriveReport> first = CompletableFuture.supplyAsync(
                () -> redrive.redrive(failedToMain(Filter.NONE)));
        assertTrue(receiving.await(5, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> redrive.redrive(failedToMain(Filter.NONE)));
        assertDoesNotThrow(() -> redrive.redrive(request(Source.DEAD_LETTER, Target.MAIN, Filter.NONE, true)));
        release.countDown();

        assertEquals(1, first.get(5, TimeUnit.SECONDS).moved());
        assertEquals(0, redrive.redrive(failedToMain(Filter.NONE)).moved());
    }

    /** Sends to {@code delegate}, recording each copy and when it went out. */
    private static final class Capturing implements MessageSender {
        final MessageSender delegate;
        final List<OutgoingMessage> sent = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<Long> at = new java.util.concurrent.CopyOnWriteArrayList<>();

        Capturing(MessageSender delegate) {
            this.delegate = delegate;
        }

        @Override
        public SendResult send(OutgoingMessage message) {
            sent.add(message);
            at.add(System.nanoTime());
            return delegate.send(message);
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Service Bus duplicate detection compares MessageId = deduplicationId: a copy with the source id would be dropped
     * by an entity that already saw it (the original publish, the failed copy). Review C1.
     */
    @Test
    void copiesGetADeduplicationIdOfTheirOwn() {
        Capturing main = new Capturing(messaging.queueSender(MAIN));
        Capturing failed = new Capturing(messaging.queueSender(FAILED));
        fail("m-1", new IllegalStateException("down"));
        fail("m-2", new UnsupportedOperationException("nope"));
        settle();
        List<String> sources = new java.util.ArrayList<>();
        MessageReceiver recording = new InboxConsumerTest.Delegating(messaging.receiver(FAILED)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                List<ReceivedMessage> got = super.receive(maxMessages, maxWait);
                got.forEach(m -> sources.add(m.messageId()));
                return got;
            }
        };
        RedriveRoutes routes = new RedriveRoutes(() -> recording, failed, null, null, main, Map.of());

        redrive(routes).redrive(failedToMain(exception("java.lang.IllegalStateException")));

        assertEquals(1, main.sent.size());
        assertEquals(1, failed.sent.size());
        assertFalse(sources.contains(main.sent.getFirst().deduplicationId()), main.sent.toString());
        assertFalse(sources.contains(failed.sent.getFirst().deduplicationId()), failed.sent.toString());
        assertNotNull(main.sent.getFirst().deduplicationId());
    }

    /** A message that cannot be copied (empty body from outside the lib) is an error, not the end of the run (I1). */
    @Test
    void anUncopiableMessageIsAnErrorAndTheRunGoesOn() {
        fail("m-1", new IllegalStateException("down"));
        settle();
        MessageReceiver emptyBody = new InboxConsumerTest.Delegating(messaging.receiver(FAILED)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                return super.receive(maxMessages, maxWait).stream().map(m -> new ReceivedMessage(m.messageId(),
                        new byte[0], m.attributes(), m.contentType(), m.orderingKey(), m.deliveryCount(),
                        m.enqueuedAt(), m.receivedAt(), m.leaseExpiresAt(), m.traceparent(), m.deadLetter(),
                        m.handle())).toList();
            }
        };
        RedriveRoutes routes = new RedriveRoutes(() -> emptyBody, messaging.queueSender(FAILED), null, null,
                messaging.queueSender(MAIN), Map.of());

        RedriveReport report = redrive(routes).redrive(failedToMain(Filter.NONE));

        assertEquals(1, report.errors());
        assertEquals(PassEnd.REPEATED_KEY, report.end());
        assertEquals(1, messaging.size(FAILED));
    }

    /** Time spent skipping is not credit for a burst of moves (I4). */
    @Test
    void skippingDoesNotLetMovesBurst() {
        fail("m-1", new IllegalStateException("down"));
        fail("m-2", new IllegalStateException("down"));
        fail("m-3", new UnsupportedOperationException("nope"));
        fail("m-4", new UnsupportedOperationException("nope"));
        settle();
        MessageReceiver slow = new InboxConsumerTest.Delegating(messaging.receiver(FAILED)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                settleFor(300);
                return super.receive(maxMessages, maxWait);
            }
        };
        Capturing main = new Capturing(messaging.queueSender(MAIN));
        RedriveRoutes routes = new RedriveRoutes(() -> slow, messaging.queueSender(FAILED), null, null, main,
                Map.of());

        redrive(routes).redrive(new RedriveRequest("billing", Source.FAILED, Target.MAIN, 100,
                exception("java.lang.UnsupportedOperationException"), 2, false));

        assertEquals(2, main.sent.size());
        long gap = Duration.ofNanos(main.at.get(1) - main.at.get(0)).toMillis();
        assertTrue(gap >= 450, "second move after " + gap + " ms, maxPerSecond=2");
    }

    private static void settleFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
