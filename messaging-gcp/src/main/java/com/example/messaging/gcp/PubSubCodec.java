package com.example.messaging.gcp;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.AttributeCodec;
import com.example.messaging.spi.ReservedAttributes;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Mensagem no formato do Pub/Sub (ADR-0004): corpo binário nativo, atributos nativos (até 100;
 * sem empacotar), {@code content_type}, {@code traceparent} e {@code dead_letter} como atributos
 * reservados e {@code orderingKey} nativo. {@code deduplicationId} não existe no envio.
 */
final class PubSubCodec {

    /** 10 MB de dados por mensagem. */
    static final long MAX_MESSAGE_BYTES = 10_000_000;

    private PubSubCodec() {
    }

    static PubsubMessage encode(OutgoingMessage message) {
        PubsubMessage.Builder builder = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(message.body()))
                .putAllAttributes(message.attributes());
        if (message.contentType() != null) {
            builder.putAttributes(ReservedAttributes.CONTENT_TYPE, message.contentType());
        }
        if (message.traceparent() != null) {
            builder.putAttributes(ReservedAttributes.TRACEPARENT, message.traceparent());
        }
        if (message.deadLetter() != null) {
            builder.putAttributes(ReservedAttributes.DEAD_LETTER, message.deadLetter().toAttributeValue());
        }
        if (message.orderingKey() != null) {
            builder.setOrderingKey(message.orderingKey());
        }
        PubsubMessage result = builder.build();
        long size = result.getData().size();
        for (Map.Entry<String, String> attribute : result.getAttributesMap().entrySet()) {
            size += attribute.getKey().length() + attribute.getValue().length();
        }
        if (size > MAX_MESSAGE_BYTES) {
            throw new MessageTooLargeException(GcpErrors.PROVIDER, "Mensagem com " + size + " bytes; limite "
                    + MAX_MESSAGE_BYTES, null);
        }
        return result;
    }

    /**
     * @param deliveryAttempt 0 quando a subscription não tem dead letter policy
     */
    static ReceivedMessage decode(PubsubMessage message, int deliveryAttempt, Instant receivedAt, Duration lease,
                                  Object handle) {
        Map<String, String> attributes = message.getAttributesMap();
        String deadLetter = attributes.get(ReservedAttributes.DEAD_LETTER);
        return new ReceivedMessage(message.getMessageId(), message.getData().toByteArray(),
                AttributeCodec.fromNative(attributes), attributes.get(ReservedAttributes.CONTENT_TYPE),
                message.getOrderingKey().isEmpty() ? null : message.getOrderingKey(),
                deliveryAttempt > 0 ? OptionalInt.of(deliveryAttempt) : OptionalInt.empty(),
                message.hasPublishTime()
                        ? Instant.ofEpochSecond(message.getPublishTime().getSeconds(), message.getPublishTime().getNanos())
                        : null,
                receivedAt, receivedAt.plus(lease), attributes.get(ReservedAttributes.TRACEPARENT),
                deadLetter == null ? null : DeadLetterInfo.fromAttributeValue(deadLetter), handle);
    }
}
