package com.example.messaging.azure;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.AttributeCodec;
import com.example.messaging.spi.ReservedAttributes;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

/**
 * Mensagem no formato do Service Bus (ADR-0004): corpo binário nativo, {@code contentType} no
 * campo nativo, atributos como application properties (sem empacotar), {@code orderingKey} como
 * {@code SessionId} e {@code deduplicationId} como {@code MessageId}.
 */
final class ServiceBusCodec {

    private ServiceBusCodec() {
    }

    static ServiceBusMessage encode(OutgoingMessage message, boolean sessions, long maxBytes) {
        long size = message.body().length;
        for (Map.Entry<String, String> attribute : message.attributes().entrySet()) {
            size += attribute.getKey().length() + attribute.getValue().length();
        }
        if (size > maxBytes) {
            throw new MessageTooLargeException(AzureErrors.PROVIDER, "Mensagem com " + size + " bytes; limite "
                    + maxBytes, null);
        }
        ServiceBusMessage result = new ServiceBusMessage(BinaryData.fromBytes(message.body()));
        result.getApplicationProperties().putAll(message.attributes());
        if (message.contentType() != null) {
            result.setContentType(message.contentType());
        }
        if (message.traceparent() != null) {
            result.getApplicationProperties().put(ReservedAttributes.TRACEPARENT, message.traceparent());
        }
        if (message.deadLetter() != null) {
            result.getApplicationProperties().put(ReservedAttributes.DEAD_LETTER, message.deadLetter().toAttributeValue());
        }
        // MessageId é o que a duplicate detection compara; sem deduplicationId, um id novo
        result.setMessageId(message.deduplicationId() != null ? message.deduplicationId() : UUID.randomUUID().toString());
        if (sessions) {
            result.setSessionId(message.orderingKey());
        }
        return result;
    }

    /**
     * @param handle handle opaco do receiver
     */
    static ReceivedMessage decode(ServiceBusReceivedMessage message, Instant receivedAt, Object handle) {
        Map<String, String> properties = new HashMap<>();
        message.getApplicationProperties().forEach((key, value) -> {
            if (value != null) {
                properties.put(key, value.toString());
            }
        });
        DeadLetterInfo deadLetter = null;
        if (message.getDeadLetterReason() != null) {
            deadLetter = new DeadLetterInfo(message.getMessageId(), message.getDeadLetterReason());
        } else if (properties.containsKey(ReservedAttributes.DEAD_LETTER)) {
            deadLetter = DeadLetterInfo.fromAttributeValue(properties.get(ReservedAttributes.DEAD_LETTER));
        }
        return new ReceivedMessage(message.getMessageId(), message.getBody().toBytes(),
                AttributeCodec.fromNative(properties), message.getContentType(), message.getSessionId(),
                OptionalInt.of(deliveryCount(message, properties)),
                message.getEnqueuedTime() == null ? null : message.getEnqueuedTime().toInstant(), receivedAt,
                message.getLockedUntil() == null ? receivedAt : message.getLockedUntil().toInstant(),
                properties.get(ReservedAttributes.TRACEPARENT), deadLetter, handle);
    }

    /**
     * O SDK Java começa em 0 (F0); a lib normaliza para 1. Numa cópia de {@code RESCHEDULE}, soma as
     * entregas da original gravadas em {@code delivery_count}.
     */
    static int deliveryCount(ServiceBusReceivedMessage message, Map<String, String> properties) {
        int count = (int) message.getDeliveryCount() + 1;
        String previous = properties.get(ReservedAttributes.DELIVERY_COUNT);
        return previous == null ? count : count + Integer.parseInt(previous);
    }
}
