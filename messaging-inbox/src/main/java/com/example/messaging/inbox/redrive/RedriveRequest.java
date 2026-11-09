package com.example.messaging.inbox.redrive;

import java.util.Objects;

/**
 * @param limit        messages read, moved or not: 1..{@value #MAX_LIMIT}
 * @param filter       {@code null} = {@link Filter#NONE}
 * @param maxPerSecond moved messages per second, at least 1
 * @param dryRun       reads and groups, moves nothing
 */
public record RedriveRequest(String consumer, Source source, Target target, int limit, Filter filter,
                             int maxPerSecond, boolean dryRun) {

    public static final int MAX_LIMIT = 10_000;

    public RedriveRequest {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT + ": " + limit);
        }
        if (maxPerSecond < 1) {
            throw new IllegalArgumentException("maxPerSecond must be at least 1: " + maxPerSecond);
        }
        if (source == Source.DEAD_LETTER && target == Target.DEAD_LETTER) {
            throw new IllegalArgumentException("source and target are both the dead-letter destination");
        }
        filter = filter == null ? Filter.NONE : filter;
    }
}
