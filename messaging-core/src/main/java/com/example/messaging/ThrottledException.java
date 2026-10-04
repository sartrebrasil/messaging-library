package com.example.messaging;

/** O provedor recusou por limite de taxa ou de cota. Tentar de novo depois faz sentido. */
public class ThrottledException extends MessagingException {

    public ThrottledException(String provider, String message, Throwable cause) {
        super(provider, message, cause, true);
    }
}
