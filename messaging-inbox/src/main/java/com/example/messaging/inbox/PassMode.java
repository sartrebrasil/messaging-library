package com.example.messaging.inbox;

/**
 * Where a delivery is processed: the main destination (levels 1 and 2), or a slow pass over the DLQ (level 3, ADR-0002:
 * level 1, no level 2). A parameter of {@link Pipeline#process}, never state: the pipeline is shared with the workers of
 * the main destination.
 */
public enum PassMode { MAIN, DEAD_LETTER }
