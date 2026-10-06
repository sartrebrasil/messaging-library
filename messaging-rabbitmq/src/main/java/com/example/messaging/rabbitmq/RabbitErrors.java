package com.example.messaging.rabbitmq;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;

import java.util.concurrent.TimeoutException;

/**
 * Converte erros do {@code amqp-client} nas exceções da lib (ADR-0007, ADR-0009). O broker
 * informa o erro fechando o channel (ou a conexão) com um reply code.
 */
final class RabbitErrors {

    static final String PROVIDER = "rabbitmq";

    private static final int NOT_FOUND = 404;
    private static final int ACCESS_REFUSED = 403;
    private static final int PRECONDITION_FAILED = 406;

    private RabbitErrors() {
    }

    static MessagingException map(String operation, Exception e) {
        ShutdownSignalException shutdown = shutdown(e);
        if (shutdown == null) {
            // IOException sem fechamento (rede) e timeout de confirm: tentar de novo faz sentido
            return new MessagingException(PROVIDER, operation + ": " + e.getMessage(), e,
                    e instanceof java.io.IOException || e instanceof TimeoutException);
        }
        int code = replyCode(shutdown.getReason());
        String text = replyText(shutdown.getReason());
        String message = operation + ": " + code + " " + text;
        return switch (code) {
            case NOT_FOUND -> new DestinationNotFoundException(PROVIDER, message, e);
            case ACCESS_REFUSED -> new AccessDeniedException(PROVIDER, message, e);
            case PRECONDITION_FAILED -> text.contains("max size")
                    ? new MessageTooLargeException(PROVIDER, message, e)
                    : new MessagingException(PROVIDER, message, e, false);
            // fechamento pelo broker ou pela rede (connection forced, 320; internal error, 541)
            default -> new MessagingException(PROVIDER, message, e, !shutdown.isInitiatedByApplication());
        };
    }

    /**
     * Para confirmações: com o channel fechado, o broker já devolveu a mensagem; um tag desconhecido
     * (406) também significa que ela não é mais deste receiver.
     */
    static MessagingException mapSettle(String operation, Exception e) {
        ShutdownSignalException shutdown = shutdown(e);
        if (shutdown != null && (replyCode(shutdown.getReason()) == PRECONDITION_FAILED
                || !(shutdown.getReason() instanceof AMQP.Channel.Close))) {
            return new LeaseExpiredException(PROVIDER, operation + ": " + shutdown.getMessage(), e);
        }
        return map(operation, e);
    }

    private static ShutdownSignalException shutdown(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof ShutdownSignalException shutdown) {
                return shutdown;
            }
        }
        return null;
    }

    private static int replyCode(Method reason) {
        return switch (reason) {
            case AMQP.Channel.Close close -> close.getReplyCode();
            case AMQP.Connection.Close close -> close.getReplyCode();
            case null, default -> 0;
        };
    }

    private static String replyText(Method reason) {
        return switch (reason) {
            case AMQP.Channel.Close close -> close.getReplyText();
            case AMQP.Connection.Close close -> close.getReplyText();
            case null, default -> "";
        };
    }
}
