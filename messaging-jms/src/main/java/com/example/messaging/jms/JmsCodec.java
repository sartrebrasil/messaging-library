package com.example.messaging.jms;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.AttributeCodec;
import com.example.messaging.spi.ReservedAttributes;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Mensagem no formato JMS (ADR-0009): corpo em {@link BytesMessage}, atributos como string
 * properties (sem empacotar), {@code contentType}, {@code traceparent} e {@code dead_letter} como
 * atributos reservados, {@code orderingKey} como {@code JMSXGroupID}. {@code deduplicationId} vai
 * como o dialeto deduplica ({@code _AMQ_DUPL_ID} no Artemis) ou é ignorado (Classic).
 */
final class JmsCodec {

    static final String GROUP_ID = "JMSXGroupID";
    static final String DELIVERY_COUNT = "JMSXDeliveryCount";

    private JmsCodec() {
    }

    static BytesMessage encode(Session session, JmsDialect dialect, OutgoingMessage message, long maxBytes)
            throws JMSException {
        long size = message.body().length;
        for (Map.Entry<String, String> attribute : message.attributes().entrySet()) {
            size += attribute.getKey().length() + attribute.getValue().length();
        }
        if (size > maxBytes) {
            throw new MessageTooLargeException(JmsErrors.PROVIDER, "Mensagem com " + size + " bytes; limite "
                    + maxBytes, null);
        }
        BytesMessage result = session.createBytesMessage();
        result.writeBytes(message.body());
        for (Map.Entry<String, String> attribute : message.attributes().entrySet()) {
            result.setStringProperty(attribute.getKey(), attribute.getValue());
        }
        if (message.contentType() != null) {
            result.setStringProperty(ReservedAttributes.CONTENT_TYPE, message.contentType());
        }
        if (message.traceparent() != null) {
            result.setStringProperty(ReservedAttributes.TRACEPARENT, message.traceparent());
        }
        if (message.deadLetter() != null) {
            result.setStringProperty(ReservedAttributes.DEAD_LETTER, message.deadLetter().toAttributeValue());
        }
        if (message.orderingKey() != null) {
            result.setStringProperty(GROUP_ID, message.orderingKey());
        }
        if (message.deduplicationId() != null) {
            dialect.deduplicate(result, message.deduplicationId());
        }
        return result;
    }

    /**
     * Cópia para reentrega ({@code nack} com atraso), com as entregas já feitas em
     * {@code delivery_count}. Sem {@code deduplicationId}: o Artemis descartaria a cópia.
     */
    static BytesMessage copy(Session session, JmsDialect dialect, ReceivedMessage message) throws JMSException {
        OutgoingMessage outgoing = new OutgoingMessage(message.body(), message.attributes(), message.contentType(),
                message.orderingKey(), null, message.traceparent(), message.deadLetter());
        BytesMessage copy = encode(session, dialect, outgoing, Long.MAX_VALUE);
        copy.setStringProperty(ReservedAttributes.DELIVERY_COUNT,
                Integer.toString(message.deliveryCount().orElse(1)));
        return copy;
    }

    static ReceivedMessage decode(Message message, Instant receivedAt, Instant leaseExpiresAt, Object handle)
            throws JMSException {
        Map<String, String> properties = new HashMap<>();
        for (Enumeration<?> names = message.getPropertyNames(); names.hasMoreElements(); ) {
            String name = (String) names.nextElement();
            Object value = message.getObjectProperty(name);
            if (value != null && !isBrokerProperty(name)) {
                properties.put(name, value.toString());
            }
        }
        String deadLetter = properties.get(ReservedAttributes.DEAD_LETTER);
        long timestamp = message.getJMSTimestamp();
        return new ReceivedMessage(message.getJMSMessageID(), body(message), AttributeCodec.fromNative(properties),
                properties.get(ReservedAttributes.CONTENT_TYPE), message.getStringProperty(GROUP_ID),
                OptionalInt.of(deliveryCount(message, properties)),
                timestamp == 0 ? null : Instant.ofEpochMilli(timestamp), receivedAt, leaseExpiresAt,
                properties.get(ReservedAttributes.TRACEPARENT),
                deadLetter == null ? null : DeadLetterInfo.fromAttributeValue(deadLetter), handle);
    }

    /** {@code JMS*}/{@code JMSX*} do JMS e {@code _AMQ*}/{@code __AMQ*} internas do Artemis (F8). */
    private static boolean isBrokerProperty(String name) {
        return name.startsWith("JMS") || name.startsWith("_AMQ") || name.startsWith("__AMQ");
    }

    /** {@code JMSXDeliveryCount} (1 na primeira entrega), somado às entregas de antes da cópia. */
    private static int deliveryCount(Message message, Map<String, String> properties) throws JMSException {
        int count = message.propertyExists(DELIVERY_COUNT) ? message.getIntProperty(DELIVERY_COUNT) : 1;
        String previous = properties.get(ReservedAttributes.DELIVERY_COUNT);
        return previous == null ? count : count + Integer.parseInt(previous);
    }

    /** {@link TextMessage} vem de produtores sem a lib; vira UTF-8. */
    private static byte[] body(Message message) throws JMSException {
        if (message instanceof TextMessage text) {
            return text.getText() == null ? new byte[0] : text.getText().getBytes(StandardCharsets.UTF_8);
        }
        byte[] body = message.getBody(byte[].class);
        return body == null ? new byte[0] : body;
    }
}
