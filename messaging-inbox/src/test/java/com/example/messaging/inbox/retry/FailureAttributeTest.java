package com.example.messaging.inbox.retry;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FailureAttributeTest {

    private static final Instant FIRST = Instant.parse("2026-10-07T10:00:00Z");
    private static final Instant NOW = Instant.parse("2026-10-07T13:00:00Z");

    @Test
    void keepsFirstFailureAndUsesRootCause() {
        var cause = new IllegalStateException(new java.net.SocketTimeoutException());
        var first = FailureAttribute.next(null, "billing-orders", "orders", cause, FIRST);
        var next = FailureAttribute.next(first.format(), "billing-orders", "orders", cause, NOW);

        assertEquals(FIRST, next.firstFailureAt());
        assertEquals("java.net.SocketTimeoutException", next.exception());
        assertEquals(ErrorFingerprint.of(cause), next.fingerprint());
        assertEquals(Optional.of(next), FailureAttribute.parse(next.format()));
    }

    @Test
    void withoutCause() {
        var attr = FailureAttribute.next(null, "c", "d", null, NOW);
        assertEquals("consumer=c;destination=d;exception=none;first_failure_at=2026-10-07T13:00:00Z", attr.format());
        assertNull(attr.fingerprint());
    }

    @Test
    void valueFitsTheAttributeLimit() {
        String longName = "d".repeat(2000);
        var attr = FailureAttribute.next(null, "billing-orders", longName, new IllegalStateException(), NOW);

        assertEquals(FailureAttribute.MAX_LENGTH, attr.format().length());
        assertEquals("IllegalStateException", attr.exception());
        assertEquals("billing-orders", attr.consumer());
        assertEquals(Optional.of(attr), FailureAttribute.parse(attr.format()));
    }

    @Test
    void invalidValues() {
        assertEquals(Optional.empty(), FailureAttribute.parse("consumer=c;first_failure_at=bad"));
        assertEquals(Optional.empty(), FailureAttribute.parse("garbage"));
        assertThrows(IllegalArgumentException.class, () -> FailureAttribute.next(null, "a;b", "d", null, NOW));
    }
}
