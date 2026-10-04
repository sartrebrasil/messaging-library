package com.example.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Mensagem entregue por {@link MessageReceiver#receive}. Só dados: confirmar, devolver e
 * renovar são operações do receiver que a entregou (ADR-0002).
 *
 * @param messageId      id do provedor
 * @param body           bytes originais (Base64 do SQS/SNS já desfeito)
 * @param attributes     atributos do usuário (pacote desfeito, reservados removidos)
 * @param contentType    campo nativo ou atributo {@code content_type}; {@code null} se ausente
 * @param orderingKey    {@code MessageGroupId}, {@code SessionId} ou {@code orderingKey}; {@code null} se ausente
 * @param deliveryCount  1 na primeira entrega; vazio quando o provedor não conta (Pub/Sub sem dead letter policy)
 * @param enqueuedAt     entrada no provedor
 * @param receivedAt     momento do receive, base de {@link #leaseDuration()}
 * @param leaseExpiresAt vencimento do lease calculado no receive
 * @param traceparent    contexto W3C de quem publicou; {@code null} se ausente
 * @param deadLetter     origem, quando a mensagem foi lida de uma DLQ; {@code null} nos outros casos
 * @param handle         referência nativa opaca, de uso exclusivo do adapter
 */
public record ReceivedMessage(String messageId,
                              byte[] body,
                              Map<String, String> attributes,
                              String contentType,
                              String orderingKey,
                              OptionalInt deliveryCount,
                              Instant enqueuedAt,
                              Instant receivedAt,
                              Instant leaseExpiresAt,
                              String traceparent,
                              DeadLetterInfo deadLetter,
                              Object handle) {

    public ReceivedMessage {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(body, "body");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        deliveryCount = deliveryCount == null ? OptionalInt.empty() : deliveryCount;
        Objects.requireNonNull(receivedAt, "receivedAt");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
    }

    /** Corpo decodificado como UTF-8. */
    public String bodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }

    /** Duração do lease do destino, como observada no receive. */
    public Duration leaseDuration() {
        return Duration.between(receivedAt, leaseExpiresAt);
    }

    /** Mostra o tamanho do corpo, não o conteúdo. */
    @Override
    public String toString() {
        return "ReceivedMessage[messageId=" + messageId + ", body=" + body.length + " bytes, attributes=" + attributes
                + ", contentType=" + contentType + ", orderingKey=" + orderingKey + ", deliveryCount=" + deliveryCount
                + ", enqueuedAt=" + enqueuedAt + ", leaseExpiresAt=" + leaseExpiresAt + ", deadLetter=" + deadLetter + "]";
    }
}
