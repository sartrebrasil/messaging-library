package com.example.messaging.inbox;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.ExceptionClassifier;
import com.example.messaging.inbox.retry.FailureAttribute;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/**
 * Copies that the inbox sends: retry queue, DLQ and {@code failed} (ADR-0002). Body, user attributes,
 * {@code contentType}, {@code orderingKey} and {@code traceparent} are the originals; {@code deduplicationId} is the
 * source {@code messageId}; the {@code inbox_*} attributes are rewritten within the 16-attribute budget.
 */
final class Copies {

    /**
     * @param attemptsDropped {@code inbox_attempts} did not fit: the copy cannot carry the count
     * @param budgetExceeded  at least one {@code inbox_*} attribute was left out
     */
    record Copy(OutgoingMessage message, boolean attemptsDropped, boolean budgetExceeded) {
    }

    private Copies() {
    }

    /**
     * @param failure {@code null} for a retry-queue copy; the DLQ and {@code failed} copies carry it
     * @throws IllegalArgumentException when the received message breaks the sending rules (empty body, invalid
     *                                  {@code orderingKey}): it can never be copied
     */
    static Copy build(ReceivedMessage message, AttemptState attempts, Failure failure) {
        Map<String, String> attributes = new HashMap<>();
        message.attributes().forEach((k, v) -> {
            if (!k.startsWith("inbox_")) {
                attributes.put(k, v);
            }
        });
        int room = OutgoingMessage.MAX_ATTRIBUTES - attributes.size();
        boolean attemptsDropped = room < 1;
        boolean failureDropped = failure != null && room < 2;
        if (!attemptsDropped) {
            attributes.put(AttemptState.ATTRIBUTE, attempts.toAttribute());
        }
        OutgoingMessage copy = OutgoingMessage.copyOf(message).withAttributes(attributes)
                .withDeduplicationId(deduplicationId(message.messageId()));
        if (failure != null) {
            if (!failureDropped) {
                copy = copy.withAttribute(FailureAttribute.NAME, FailureAttribute.next(
                        message.attributes().get(FailureAttribute.NAME), failure.consumer(), failure.destination(),
                        failure.cause(), failure.now()).format());
            }
            copy = copy.withDeadLetter(new DeadLetterInfo(message.messageId(), failure.reason()));
        }
        return new Copy(copy, attemptsDropped, attemptsDropped || failureDropped);
    }

    /** What the DLQ and {@code failed} copies record about the failure. */
    record Failure(String consumer, String destination, Throwable cause, String reason, Instant now) {

        /**
         * @param detail the handler's own reason (a {@code Reject}), which leads the text; {@code null} otherwise
         */
        static Failure of(String consumer, String destination, Throwable cause, String fallbackReason, String detail,
                          boolean includeExceptionMessage, Instant now) {
            String reason;
            if (cause == null) {
                reason = detail != null ? fallbackReason + ": " + detail : fallbackReason;
            } else {
                Throwable root = ExceptionClassifier.rootCause(cause);
                reason = includeExceptionMessage && root.getMessage() != null
                        ? root.getClass().getName() + ": " + root.getMessage()
                        : root.getClass().getName();
                if (detail != null) {
                    reason = fallbackReason + ": " + detail + " (" + reason + ")";
                }
            }
            return new Failure(consumer, destination, cause, reason, now);
        }
    }

    /** The source id when the providers accept it; otherwise a stable SHA-256 of it (64 hex characters). */
    static String deduplicationId(String messageId) {
        try {
            OutgoingMessage.ofText("x").withDeduplicationId(messageId);
            return messageId;
        } catch (IllegalArgumentException e) {
            try {
                byte[] hash = MessageDigest.getInstance("SHA-256").digest(messageId.getBytes(StandardCharsets.UTF_8));
                return HexFormat.of().formatHex(hash);
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException(impossible);
            }
        }
    }
}
