package com.example.messaging.inbox.redrive;

import java.time.Instant;

/**
 * Which messages a redrive moves; every field is optional and they combine with AND. {@code exception} and
 * {@code fingerprint} match the {@code inbox_failure} attribute ({@code none} for a message without it); {@code reason}
 * is a prefix of the dead-letter reason; the dates bound {@code enqueuedAt} (a message without it never passes).
 */
public record Filter(String exception, String fingerprint, String reason, Instant enqueuedBefore,
                     Instant enqueuedAfter) {

    public static final Filter NONE = new Filter(null, null, null, null, null);
}
