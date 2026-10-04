package com.example.messaging;

/** A fila, o tópico ou a subscription não existe. */
public class DestinationNotFoundException extends MessagingException {

    public DestinationNotFoundException(String provider, String message, Throwable cause) {
        super(provider, message, cause, false);
    }
}
