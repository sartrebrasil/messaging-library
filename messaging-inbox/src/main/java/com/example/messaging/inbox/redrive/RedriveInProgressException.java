package com.example.messaging.inbox.redrive;

/** Já existe um redrive da mesma origem do mesmo receiver em andamento (HTTP 409 no endpoint). */
public final class RedriveInProgressException extends IllegalStateException {

    public RedriveInProgressException(String message) {
        super(message);
    }
}
