package com.example.messaging.rabbitmq;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.AttributeCodec;
import com.example.messaging.spi.ReservedAttributes;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

/**
 * Mensagem no formato AMQP 0-9-1 (ADR-0009): corpo binário nativo, {@code contentType} e
 * {@code messageId} nas properties, atributos como headers (sem empacotar). O broker não gera
 * {@code message-id}: o sender grava um UUID. {@code orderingKey} e {@code deduplicationId} são
 * ignorados: o RabbitMQ não tem ordem por chave nem deduplicação no envio.
 */
final class RabbitCodec {

    static final String DELIVERY_COUNT = "x-delivery-count";
    static final String FIRST_DEATH_REASON = "x-first-death-reason";

    private RabbitCodec() {
    }

    record Encoded(AMQP.BasicProperties properties, byte[] body) {
    }

    static Encoded encode(OutgoingMessage message, long maxBytes) {
        long size = message.body().length;
        for (Map.Entry<String, String> attribute : message.attributes().entrySet()) {
            size += attribute.getKey().length() + attribute.getValue().length();
        }
        if (size > maxBytes) {
            throw new MessageTooLargeException(RabbitErrors.PROVIDER, "Mensagem com " + size + " bytes; limite "
                    + maxBytes, null);
        }
        Map<String, Object> headers = new HashMap<>(message.attributes());
        if (message.traceparent() != null) {
            headers.put(ReservedAttributes.TRACEPARENT, message.traceparent());
        }
        if (message.deadLetter() != null) {
            headers.put(ReservedAttributes.DEAD_LETTER, message.deadLetter().toAttributeValue());
        }
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .messageId(UUID.randomUUID().toString())
                .contentType(message.contentType())
                .deliveryMode(2)
                .timestamp(new Date())
                .headers(headers)
                .build();
        return new Encoded(properties, message.body());
    }

    static ReceivedMessage decode(Delivery delivery, Instant receivedAt, Instant leaseExpiresAt, Object handle) {
        AMQP.BasicProperties properties = delivery.getProperties();
        Map<String, String> headers = new HashMap<>();
        Object deliveryCount = null;
        Object firstDeathReason = null;
        if (properties.getHeaders() != null) {
            for (Map.Entry<String, Object> header : properties.getHeaders().entrySet()) {
                // x-* são do broker (x-delivery-count, x-death, x-first-death-*)
                if (header.getKey().startsWith("x-")) {
                    if (DELIVERY_COUNT.equals(header.getKey())) {
                        deliveryCount = header.getValue();
                    } else if (FIRST_DEATH_REASON.equals(header.getKey())) {
                        firstDeathReason = header.getValue();
                    }
                } else if (header.getValue() != null) {
                    // strings chegam como LongString
                    headers.put(header.getKey(), header.getValue().toString());
                }
            }
        }
        String deadLetter = headers.get(ReservedAttributes.DEAD_LETTER);
        DeadLetterInfo deadLetterInfo = deadLetter != null ? DeadLetterInfo.fromAttributeValue(deadLetter)
                : firstDeathReason != null ? new DeadLetterInfo(null, firstDeathReason.toString()) : null;
        // mensagem de outro produtor pode vir sem message-id
        String messageId = properties.getMessageId() != null ? properties.getMessageId() : UUID.randomUUID().toString();
        return new ReceivedMessage(messageId, delivery.getBody(), AttributeCodec.fromNative(headers),
                properties.getContentType(), null, deliveryCount(delivery, deliveryCount),
                properties.getTimestamp() == null ? null : properties.getTimestamp().toInstant(), receivedAt,
                leaseExpiresAt, headers.get(ReservedAttributes.TRACEPARENT), deadLetterInfo, handle);
    }

    /**
     * Quorum queue: {@code x-delivery-count} conta as entregas anteriores e falta na primeira (F9).
     * Classic queue: só há {@code redelivered}, então a contagem é 1 na primeira e desconhecida depois.
     */
    private static OptionalInt deliveryCount(Delivery delivery, Object header) {
        if (header instanceof Number previous) {
            return OptionalInt.of(previous.intValue() + 1);
        }
        return delivery.getEnvelope().isRedeliver() ? OptionalInt.empty() : OptionalInt.of(1);
    }
}
