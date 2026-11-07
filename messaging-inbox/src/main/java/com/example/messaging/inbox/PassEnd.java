package com.example.messaging.inbox;

/** Why a level-3 pass over the DLQ ended (ADR-0002); the {@code end} tag of {@code inbox.dead.letter.passes}. */
public enum PassEnd { EMPTY, NEWER_THAN_PASS, REPEATED_KEY, MAX_PER_PASS, STOPPING, RECEIVE_FAILED }
