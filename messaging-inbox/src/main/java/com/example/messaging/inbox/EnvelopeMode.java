package com.example.messaging.inbox;

/** How a consumer reads the envelope (ADR-0003, {@code envelope: auto|cloudevents|outbox|raw}). */
public enum EnvelopeMode {
    /** CloudEvents when {@code ce_id} is present, else the outbox body when it has the outbox fields, else raw. */
    AUTO,
    CLOUDEVENTS,
    OUTBOX,
    RAW
}
