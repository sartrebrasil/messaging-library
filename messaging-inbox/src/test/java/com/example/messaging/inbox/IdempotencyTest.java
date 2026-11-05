package com.example.messaging.inbox;

import com.example.idempotency.model.IdempotencyKeyId;
import com.example.idempotency.model.IdempotencyRecord;
import com.example.idempotency.model.IdempotencyStatus;
import com.example.idempotency.spi.IdempotencyStore;
import com.example.idempotency.store.InMemoryIdempotencyStore;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The idempotency step of the pipeline (ADR-0004). */
class IdempotencyTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final IdempotencyKeyId KEY = new IdempotencyKeyId("m-1", "billing");

    @SkipImmediate
    static class Down extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(CLOCK);
    private final AtomicInteger calls = new AtomicInteger();

    @Test
    void successCompletesWithTheCompletedTtlAndTheNextDeliveryIsADuplicate() {
        var p = pipeline(store, m -> null);

        assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), p.handle(outbox("a"), "q", 0));
        assertEquals(new Outcome.Ack(Outcome.AckReason.DUPLICATE), p.handle(outbox("a"), "q", 0));

        assertEquals(1, calls.get());
        IdempotencyRecord stored = store.find(KEY).orElseThrow();
        assertEquals(IdempotencyStatus.COMPLETED, stored.status());
        assertEquals(NOW.plus(Duration.ofDays(7)), stored.expiresAt());
    }

    @Test
    void everyOtherOutcomeReleasesTheKey() {
        assertInstanceOf(Outcome.Retry.class, pipeline(store, m -> {
            throw new Down();
        }).handle(outbox("a"), "q", 0));
        assertTrue(store.find(KEY).isEmpty());

        assertInstanceOf(Outcome.Failed.class, pipeline(store, m -> HandlerResult.reject("no")).handle(outbox("a"), "q", 0));
        assertTrue(store.find(KEY).isEmpty());

        assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), pipeline(store, m -> null).handle(outbox("a"), "q", 0));
    }

    @Test
    void anErrorFromTheHandlerReleasesTheKeyAndPropagates() {
        var p = pipeline(store, m -> {
            throw new AssertionError("bug");
        });

        assertThrows(AssertionError.class, () -> p.handle(outbox("a"), "q", 0));
        assertTrue(store.find(KEY).isEmpty());
    }

    @Test
    void sameIdWithAnotherPayloadIsAConflict() {
        pipeline(store, m -> null).handle(outbox("a"), "q", 0);

        Outcome outcome = pipeline(store, m -> null).handle(outbox("b"), "q", 0);

        assertEquals(Outcome.FailureReason.IDEMPOTENCY_CONFLICT, assertInstanceOf(Outcome.Failed.class, outcome).reason());
        assertEquals(1, calls.get());
    }

    @Test
    void heldByAnotherInstanceRetriesWithoutSpendingAnAttempt() {
        store.reserve(IdempotencyRecord.inProgress(KEY, "other", fingerprint("a"), NOW, NOW.plusSeconds(60)));

        Outcome outcome = pipeline(store, m -> null).handle(outbox("a"), "q", 0);

        var retry = assertInstanceOf(Outcome.Retry.class, outcome);
        assertEquals(Outcome.RetryReason.IN_PROGRESS, retry.reason());
        assertEquals(AttemptState.initial(), retry.next());
        assertEquals(Duration.ofSeconds(1), retry.delay());
        assertEquals(0, calls.get());
        assertEquals("other", store.find(KEY).orElseThrow().owner(), "the holder keeps the key");
    }

    @Test
    void heldByAnotherInstanceAfterTheLastDelayedAttemptGoesToTheDeadLetterDestination() {
        store.reserve(IdempotencyRecord.inProgress(KEY, "other", fingerprint("a"), NOW, NOW.plusSeconds(60)));

        Outcome outcome = pipeline(store, m -> null).handle(received(body("a"), Map.of("inbox_attempts", "l2=3;l3=0")), "q", 0);

        assertInstanceOf(Outcome.DeadLetter.class, outcome);
    }

    @Test
    void aFailingStoreIsARetryableFailure() {
        IdempotencyStore down = new Delegating(store) {
            @Override
            public Optional<IdempotencyRecord> reserve(IdempotencyRecord candidate) {
                throw new IllegalStateException("redis down");
            }
        };

        var retry = assertInstanceOf(Outcome.Retry.class, pipeline(down, m -> null).handle(outbox("a"), "q", 0));

        assertEquals(Outcome.RetryReason.FAILURE, retry.reason());
        assertEquals(new AttemptState(1, 0), retry.next());
        assertEquals(0, calls.get());
    }

    @Test
    void noAckWithoutComplete() {
        IdempotencyStore failingComplete = new Delegating(store) {
            @Override
            public boolean complete(IdempotencyKeyId id, String owner, String type, byte[] payload, Instant expiresAt) {
                throw new IllegalStateException("redis down");
            }
        };

        Outcome outcome = pipeline(failingComplete, m -> null).handle(outbox("a"), "q", 0);

        assertInstanceOf(Outcome.Retry.class, outcome);
        assertTrue(store.find(KEY).isEmpty(), "the reservation is released");
    }

    @Test
    void concurrentDeliveriesRunTheHandlerOnce() throws Exception {
        var shared = new InMemoryIdempotencyStore();
        var inHandler = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var p = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, m -> {
                    calls.incrementAndGet();
                    inHandler.countDown();
                    finish.await(5, TimeUnit.SECONDS);
                    return null;
                }, policy(), d -> { }, () -> 0.0,
                new InboxIdempotency(shared, Duration.ofMinutes(20), Duration.ofDays(7), Clock.systemUTC()));

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<Outcome> first = pool.submit(() -> p.handle(outbox("a"), "q", 0));
            assertTrue(inHandler.await(5, TimeUnit.SECONDS));
            Outcome second = p.handle(outbox("a"), "q", 0);
            finish.countDown();

            assertEquals(Outcome.RetryReason.IN_PROGRESS, assertInstanceOf(Outcome.Retry.class, second).reason());
            assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), first.get());
        }
        assertEquals(1, calls.get());
        assertEquals(new Outcome.Ack(Outcome.AckReason.DUPLICATE), p.handle(outbox("a"), "q", 0));
    }

    private Pipeline<String> pipeline(IdempotencyStore s, InboxResultHandler<String> handler) {
        return new Pipeline<>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, m -> {
                    calls.incrementAndGet();
                    return handler.handle(m);
                }, policy(), d -> { }, () -> 0.0,
                new InboxIdempotency(s, Duration.ofMinutes(20), Duration.ofDays(7), CLOCK));
    }

    private static RetryPolicy policy() {
        return new RetryPolicy(BackoffSpec.none(), new BackoffSpec(3, Duration.ofSeconds(1), 2, Duration.ofMinutes(1), 0),
                true, 3, List.of(), List.of(), List.of());
    }

    private static String body(String payload) {
        return "{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"" + payload + "\"}";
    }

    private static String fingerprint(String payload) {
        return IdempotencyGuard.sha256(("\"" + payload + "\"").getBytes());
    }

    private static ReceivedMessage outbox(String payload) {
        return received(body(payload), Map.of());
    }

    private static ReceivedMessage received(String body, Map<String, String> attributes) {
        return PipelineTest.received(body, attributes, 1);
    }

    /** Store that forwards everything; tests override one method. */
    static class Delegating implements IdempotencyStore {
        private final IdempotencyStore delegate;

        Delegating(IdempotencyStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<IdempotencyRecord> reserve(IdempotencyRecord candidate) {
            return delegate.reserve(candidate);
        }

        @Override
        public boolean complete(IdempotencyKeyId id, String owner, String resultType, byte[] resultPayload,
                                Instant expiresAt) {
            return delegate.complete(id, owner, resultType, resultPayload, expiresAt);
        }

        @Override
        public boolean release(IdempotencyKeyId id, String owner) {
            return delegate.release(id, owner);
        }

        @Override
        public Optional<IdempotencyRecord> find(IdempotencyKeyId id) {
            return delegate.find(id);
        }
    }
}
