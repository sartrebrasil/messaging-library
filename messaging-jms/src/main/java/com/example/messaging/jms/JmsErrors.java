package com.example.messaging.jms;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessagingException;
import com.example.messaging.ThrottledException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.ResourceAllocationException;

import java.io.IOException;

/** Converte erros do JMS nas exceções da lib (ADR-0007, ADR-0009). */
final class JmsErrors {

    static final String PROVIDER = "jms";

    private JmsErrors() {
    }

    static MessagingException map(String operation, JMSException e) {
        String message = operation + ": " + e.getMessage();
        return switch (e) {
            case InvalidDestinationException ignored -> new DestinationNotFoundException(PROVIDER, message, e);
            case JMSSecurityException ignored -> new AccessDeniedException(PROVIDER, message, e);
            case ResourceAllocationException ignored -> new ThrottledException(PROVIDER, message, e);
            default -> new MessagingException(PROVIDER, message, e, isConnectionFailure(e));
        };
    }

    /**
     * Para confirmações: session ou consumer fechados ({@link jakarta.jms.IllegalStateException})
     * significam que a mensagem já voltou ao broker.
     */
    static MessagingException mapSettle(String operation, JMSException e) {
        if (e instanceof jakarta.jms.IllegalStateException) {
            return new LeaseExpiredException(PROVIDER, operation + ": " + e.getMessage(), e);
        }
        return map(operation, e);
    }

    /** Queda de conexão chega como {@link IOException} na causa; tentar de novo faz sentido. */
    private static boolean isConnectionFailure(JMSException e) {
        for (Throwable cause = e.getCause() != null ? e.getCause() : e.getLinkedException(); cause != null;
             cause = cause.getCause()) {
            if (cause instanceof IOException) {
                return true;
            }
        }
        return false;
    }
}
