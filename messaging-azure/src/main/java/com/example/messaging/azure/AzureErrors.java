package com.example.messaging.azure;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.servicebus.ServiceBusException;
import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.ThrottledException;

/** Converte erros do Service Bus nas exceções da lib (ADR-0007). */
final class AzureErrors {

    static final String PROVIDER = "azure";

    private AzureErrors() {
    }

    static MessagingException map(String operation, RuntimeException e) {
        String message = operation + ": " + e.getMessage();
        if (e instanceof ServiceBusException sb) {
            return switch (sb.getReason().toString()) {
                case "MESSAGING_ENTITY_NOT_FOUND" -> new DestinationNotFoundException(PROVIDER, message, e);
                case "UNAUTHORIZED" -> new AccessDeniedException(PROVIDER, message, e);
                case "SERVICE_BUSY", "QUOTA_EXCEEDED" -> new ThrottledException(PROVIDER, message, e);
                case "MESSAGE_LOCK_LOST", "SESSION_LOCK_LOST" -> new LeaseExpiredException(PROVIDER, message, e);
                case "MESSAGE_SIZE_EXCEEDED" -> new MessageTooLargeException(PROVIDER, message, e);
                default -> e.getCause() instanceof AmqpException amqp
                        ? map(operation, amqp, message)
                        : new MessagingException(PROVIDER, message, e, sb.isTransient());
            };
        }
        if (e instanceof AmqpException amqp) {
            return map(operation, amqp, message);
        }
        return new MessagingException(PROVIDER, message, e, false);
    }

    private static MessagingException map(String operation, AmqpException e, String message) {
        AmqpErrorCondition condition = e.getErrorCondition();
        if (condition == AmqpErrorCondition.NOT_FOUND) {
            return new DestinationNotFoundException(PROVIDER, message, e);
        }
        if (condition == AmqpErrorCondition.UNAUTHORIZED_ACCESS) {
            return new AccessDeniedException(PROVIDER, message, e);
        }
        if (condition == AmqpErrorCondition.SERVER_BUSY_ERROR || condition == AmqpErrorCondition.RESOURCE_LIMIT_EXCEEDED) {
            return new ThrottledException(PROVIDER, message, e);
        }
        if (condition == AmqpErrorCondition.MESSAGE_LOCK_LOST || condition == AmqpErrorCondition.SESSION_LOCK_LOST) {
            return new LeaseExpiredException(PROVIDER, message, e);
        }
        return new MessagingException(PROVIDER, message, e, e.isTransient());
    }
}
