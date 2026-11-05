package com.example.messaging.inbox;

import com.example.idempotency.exception.IdempotencyInProgressException;
import com.example.idempotency.model.IdempotencyKeyId;
import com.example.idempotency.model.IdempotencyRecord;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.RetryPolicy;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Idempotency around one delivery (ADR-0004): {@code reserve -> handler -> complete} on success,
 * {@code reserve -> handler -> release} on every other outcome, before the effect on the broker. Key = envelope id,
 * scope = consumer, fingerprint = SHA-256 of the payload.
 *
 * <p>Kept apart from {@link Pipeline} so idempotency-core, an optional dependency, is loaded only by consumers that
 * use it.</p>
 */
final class IdempotencyGuard {

    private static final System.Logger LOG = System.getLogger(IdempotencyGuard.class.getName());

    private final String consumer;
    private final InboxIdempotency idempotency;
    private final RetryPolicy policy;
    private final DoubleSupplier random;

    IdempotencyGuard(String consumer, InboxIdempotency idempotency, RetryPolicy policy, DoubleSupplier random) {
        this.consumer = consumer;
        this.idempotency = idempotency;
        this.policy = policy;
        this.random = random;
    }

    /**
     * @param run          the handler and the retry levels
     * @param storeFailure outcome for a failure of the store itself (a retryable failure)
     */
    Outcome apply(InboxMessage inbox, AttemptState state, Supplier<Outcome> run,
                  Function<RuntimeException, Outcome> storeFailure) {
        IdempotencyKeyId id = new IdempotencyKeyId(inbox.id(), consumer);
        String fingerprint = sha256(inbox.payload());
        Instant now = idempotency.clock().instant();
        IdempotencyRecord candidate = IdempotencyRecord.inProgress(id, fingerprint, now,
                now.plus(idempotency.reservationTtl()));
        Optional<IdempotencyRecord> existing;
        try {
            existing = idempotency.store().reserve(candidate);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + consumer + ": idempotency store failed on reserve of "
                    + id, e);
            return storeFailure.apply(e);
        }
        if (existing.isPresent()) {
            return held(existing.get(), fingerprint, inbox, state);
        }
        String owner = candidate.owner();
        Outcome outcome;
        try {
            outcome = run.get();
        } catch (RuntimeException | Error e) {
            release(id, owner);
            throw e;
        }
        if (!(outcome instanceof Outcome.Ack)) {
            release(id, owner);
            return outcome;
        }
        try {
            if (!idempotency.store().complete(id, owner, null, null,
                    idempotency.clock().instant().plus(idempotency.completedTtl()))) {
                // the reservation expired and another instance took the key: both ran the handler
                LOG.log(System.Logger.Level.WARNING, "Consumer {0}: reservation of {1} was lost before complete; "
                        + "raise the reservation TTL above max-processing", consumer, id);
            }
        } catch (RuntimeException e) {
            // no ack without complete (ADR-0004)
            LOG.log(System.Logger.Level.WARNING, "Consumer " + consumer + ": idempotency store failed on complete of "
                    + id, e);
            release(id, owner);
            return storeFailure.apply(e);
        }
        return outcome;
    }

    /** Another delivery holds the key, or already completed it. */
    private Outcome held(IdempotencyRecord current, String fingerprint, InboxMessage inbox, AttemptState state) {
        if (current.fingerprint() != null && !current.fingerprint().equals(fingerprint)) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: message {1} reuses an id with a different payload",
                    consumer, inbox.id());
            return new Outcome.Failed(Outcome.FailureReason.IDEMPOTENCY_CONFLICT, state, null, null);
        }
        if (current.isCompleted()) {
            return new Outcome.Ack(Outcome.AckReason.DUPLICATE);
        }
        var inProgress = new IdempotencyInProgressException(current.id(), false);
        if (state.delayed() >= policy.delayed().maxAttempts()) {
            // reached only under broker-delay, where every redelivery counts (ADR-0004)
            return policy.deadLetterEnabled()
                    ? new Outcome.DeadLetter(state, inProgress)
                    : new Outcome.Failed(Outcome.FailureReason.RETRIES_EXHAUSTED, state, inProgress, null);
        }
        Duration delay = policy.delayed().delayFor(state.delayed() + 1, random);
        return new Outcome.Retry(delay, state, inProgress, Outcome.RetryReason.IN_PROGRESS);
    }

    private void release(IdempotencyKeyId id, String owner) {
        try {
            idempotency.store().release(id, owner);
        } catch (RuntimeException e) {
            // the key stays IN_PROGRESS until the reservation TTL; redeliveries meanwhile are in_progress
            LOG.log(System.Logger.Level.WARNING, "Consumer " + consumer + ": idempotency store failed on release of "
                    + id, e);
        }
    }

    static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
