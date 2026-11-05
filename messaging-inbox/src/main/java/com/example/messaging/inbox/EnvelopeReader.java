package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Reads the envelope of a {@link ReceivedMessage} into an {@link InboxMessage} (ADR-0003): CloudEvents binary in
 * the attributes, the outbox-library JSON body, or raw.
 *
 * <p>In {@link EnvelopeMode#AUTO}, a message is CloudEvents when it has {@code ce_id}, outbox when its body is a
 * JSON object with {@code messageId}, {@code eventType} and {@code payload}, raw otherwise. Once a format is
 * recognized, a broken field (bad {@code ce_time} or {@code occurredAt}, non-integer version) fails with
 * {@link PayloadDeserializationException} instead of falling back to raw, because raw would change the id.</p>
 *
 * <p>{@code headers} are the transport attributes plus the outbox {@code headers}; the outbox value wins, like the
 * {@code traceparent} (ADR-0007). {@code inbox_*} attributes are left out.</p>
 */
public final class EnvelopeReader {

    private static final String CE = "ce_";
    private static final String INBOX = "inbox_";
    private static final Pattern TRACEPARENT = Pattern.compile("[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    private final EnvelopeMode mode;

    public EnvelopeReader(EnvelopeMode mode) {
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    public EnvelopeMode mode() {
        return mode;
    }

    /**
     * @param destination consumer destination the message was read from
     * @throws PayloadDeserializationException when the configured format cannot be read
     */
    public InboxMessage read(ReceivedMessage message, String destination) {
        Map<String, String> attributes = message.attributes();
        Delivery delivery = Delivery.of(AttemptState.from(attributes.get(AttemptState.ATTRIBUTE),
                message.deliveryCount().orElse(0)));
        return switch (mode) {
            case CLOUDEVENTS -> cloudEvents(message, destination, delivery);
            case OUTBOX -> outbox(message, destination, delivery, parseOutbox(message, true));
            case RAW -> raw(message, destination, delivery);
            case AUTO -> {
                if (attributes.containsKey(CE + "id")) {
                    yield cloudEvents(message, destination, delivery);
                }
                OutboxJson.Body body = parseOutbox(message, false);
                yield body == null ? raw(message, destination, delivery) : outbox(message, destination, delivery, body);
            }
        };
    }

    private InboxMessage cloudEvents(ReceivedMessage message, String destination, Delivery delivery) {
        Map<String, String> a = message.attributes();
        String id = a.get(CE + "id");
        String type = a.get(CE + "type");
        if (id == null || type == null) {
            throw new PayloadDeserializationException("CloudEvents message " + message.messageId()
                    + " without ce_id or ce_type", null);
        }
        return new InboxMessage(id, type, a.get(CE + "source"), instant(a.get(CE + "time"), message),
                a.get(CE + "subject"), a.get(CE + "aggregatetype"), version(a.get(CE + "aggregateversion"), message),
                traceparent(null, message), headers(a, Map.of()), message.body(), destination, delivery);
    }

    /** {@code null} when not an outbox body and {@code required} is false. */
    private static OutboxJson.Body parseOutbox(ReceivedMessage message, boolean required) {
        OutboxJson.Body body;
        try {
            body = OutboxJson.parse(message.bodyAsString());
        } catch (IllegalArgumentException e) {
            if (required) {
                throw new PayloadDeserializationException("message " + message.messageId()
                        + " is not an outbox body", e);
            }
            return null;
        }
        if (body.strings().get("messageId") == null || body.strings().get("eventType") == null
                || body.payload() == null) {
            if (required) {
                throw new PayloadDeserializationException("outbox body of message " + message.messageId()
                        + " without messageId, eventType or payload", null);
            }
            return null;
        }
        return body;
    }

    private InboxMessage outbox(ReceivedMessage message, String destination, Delivery delivery, OutboxJson.Body body) {
        Map<String, String> s = body.strings();
        return new InboxMessage(s.get("messageId"), s.get("eventType"), null, instant(s.get("occurredAt"), message),
                s.get("aggregateId"), s.get("aggregateType"), version(body.aggregateVersion(), message),
                traceparent(body.headers().get("traceparent"), message), headers(message.attributes(), body.headers()),
                body.payload().getBytes(StandardCharsets.UTF_8), destination, delivery);
    }

    private InboxMessage raw(ReceivedMessage message, String destination, Delivery delivery) {
        return new InboxMessage(message.messageId(), null, null, null, null, null, null, traceparent(null, message),
                headers(message.attributes(), Map.of()), message.body(), destination, delivery);
    }

    /** ADR-0007 (revised 2026-11-05): the outbox body first, then the transport; invalid values are skipped. */
    private static String traceparent(String fromBody, ReceivedMessage message) {
        if (fromBody != null && TRACEPARENT.matcher(fromBody).matches()) {
            return fromBody;
        }
        String transport = message.traceparent();
        return transport != null && TRACEPARENT.matcher(transport).matches() ? transport : null;
    }

    private static Map<String, String> headers(Map<String, String> attributes, Map<String, String> fromBody) {
        Map<String, String> headers = new HashMap<>();
        attributes.forEach((k, v) -> {
            if (!k.startsWith(INBOX)) {
                headers.put(k, v);
            }
        });
        headers.putAll(fromBody);
        return headers;
    }

    private static Instant instant(String value, ReceivedMessage message) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new PayloadDeserializationException("message " + message.messageId() + " with invalid time '"
                    + value + "'", e);
        }
    }

    private static Long version(String value, ReceivedMessage message) {
        if (value == null || value.equals("null")) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new PayloadDeserializationException("message " + message.messageId()
                    + " with invalid aggregate version '" + value + "'", e);
        }
    }
}
