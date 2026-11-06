package com.example.messaging.inbox.spring;

import com.example.idempotency.spi.IdempotencyStore;
import com.example.idempotency.store.InMemoryIdempotencyStore;
import com.example.messaging.inbox.InboxIdempotency;
import org.springframework.beans.factory.ListableBeanFactory;

import java.time.Clock;
import java.time.Duration;

/** Idempotency wiring, kept apart because idempotency-core is optional. */
final class IdempotencySupport {

    private static final System.Logger LOG = System.getLogger(IdempotencySupport.class.getName());

    private IdempotencySupport() {
    }

    /** @return {@code null} with a problem message when there is no store bean */
    static InboxIdempotency of(String consumer, ListableBeanFactory beans, Duration reservationTtl,
                               Duration completedTtl, Clock clock) {
        IdempotencyStore store = beans.getBeanProvider(IdempotencyStore.class).getIfUnique();
        if (store == null) {
            return null;
        }
        if (store instanceof InMemoryIdempotencyStore) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: the idempotency store is in memory and does not "
                    + "deduplicate across instances; set idempotency.store (jdbc, redis or mongo)", consumer);
        }
        return new InboxIdempotency(store, reservationTtl, completedTtl, clock);
    }
}
