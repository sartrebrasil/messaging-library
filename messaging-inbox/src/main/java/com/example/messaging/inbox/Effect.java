package com.example.messaging.inbox;

/** What the consumer did on the broker for one delivery (ADR-0009). */
public enum Effect {
    ACKED,
    /** The {@code ack} found the lease expired: the message comes back (a duplicate). */
    ACK_LEASE_EXPIRED,
    RELEASED,
    REDELIVERY_DELAYED,
    RETRY_QUEUED,
    DEAD_LETTERED,
    FAILED,
    /** A broker call failed; the message comes back by {@code nack(0)} or when the lease expires. */
    EFFECT_FAILED,
    /** The copy can never be built from this message; released, the native redrive policy ends the loop. */
    COPY_IMPOSSIBLE
}
