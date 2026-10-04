package com.example.messaging;

/** As credenciais não têm permissão para a operação no destino. */
public class AccessDeniedException extends MessagingException {

    public AccessDeniedException(String provider, String message, Throwable cause) {
        super(provider, message, cause, false);
    }
}
