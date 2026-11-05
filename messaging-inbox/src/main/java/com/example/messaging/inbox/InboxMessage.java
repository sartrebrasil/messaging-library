package com.example.messaging.inbox;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * A received message with its envelope read (ADR-0003). {@code id} is the logical id: the same on every copy for
 * CloudEvents and outbox messages, the native id in raw mode.
 *
 * <p>The {@code payload} array is not copied: do not modify it.</p>
 *
 * @param id               {@code ce_id}, outbox {@code messageId} or the native id (raw)
 * @param type             {@code ce_type} or outbox {@code eventType}; {@code null} in raw mode
 * @param source           {@code ce_source}; {@code null} otherwise
 * @param time             {@code ce_time} or outbox {@code occurredAt}; {@code null} when absent
 * @param subject          {@code ce_subject} or outbox {@code aggregateId}; {@code null} when absent
 * @param aggregateType    {@code ce_aggregatetype} or outbox {@code aggregateType}; {@code null} when absent
 * @param aggregateVersion {@code ce_aggregateversion} or outbox {@code aggregateVersion}; {@code null} when absent
 * @param traceparent      W3C context to continue (ADR-0007); {@code null} when absent
 * @param headers          transport attributes and outbox {@code headers}, without {@code inbox_*}
 * @param payload          event payload: the outbox {@code payload} member, the whole body otherwise
 * @param destination      consumer destination the message was read from
 * @param delivery         position in the retry levels
 */
public record InboxMessage(String id,
                           String type,
                           String source,
                           Instant time,
                           String subject,
                           String aggregateType,
                           Long aggregateVersion,
                           String traceparent,
                           Map<String, String> headers,
                           byte[] payload,
                           String destination,
                           Delivery delivery) {

    public InboxMessage {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(delivery, "delivery");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public InboxMessage withDelivery(Delivery newDelivery) {
        return new InboxMessage(id, type, source, time, subject, aggregateType, aggregateVersion, traceparent, headers,
                payload, destination, newDelivery);
    }

    /** Shows the payload size, not its content. */
    @Override
    public String toString() {
        return "InboxMessage[id=" + id + ", type=" + type + ", source=" + source + ", time=" + time
                + ", subject=" + subject + ", aggregateType=" + aggregateType + ", aggregateVersion=" + aggregateVersion
                + ", traceparent=" + traceparent + ", headers=" + headers + ", payload=" + payload.length + " bytes"
                + ", destination=" + destination + ", delivery=" + delivery + "]";
    }
}
