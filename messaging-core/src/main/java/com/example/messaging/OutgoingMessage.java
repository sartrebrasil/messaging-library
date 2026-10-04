package com.example.messaging;

import com.example.messaging.spi.ReservedAttributes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Mensagem a enviar. A construção valida as regras que os três provedores aceitam sem
 * transformar (ADR-0004), então o erro aparece antes de qualquer chamada ao SDK:
 *
 * <ul>
 *   <li>{@code body} não vazio (o SQS exige 1 byte; o Pub/Sub recusa mensagem vazia).</li>
 *   <li>Até {@value #MAX_ATTRIBUTES} atributos; chave {@code [a-z_][a-z0-9_]*} com até
 *       {@value #MAX_KEY_LENGTH} caracteres, sem prefixo {@code goog} e fora de
 *       {@link ReservedAttributes}; valor ASCII imprimível com até {@value #MAX_VALUE_LENGTH} bytes.</li>
 *   <li>{@code orderingKey} e {@code deduplicationId}: ASCII imprimível sem espaço, até
 *       {@value #MAX_ID_LENGTH} caracteres (limite do SQS e do Service Bus).</li>
 *   <li>{@code traceparent}: formato W3C.</li>
 * </ul>
 *
 * <p>O array de {@code body} não é copiado: não o modifique depois de construir a mensagem.</p>
 *
 * @param body            conteúdo; a serialização é de quem envia
 * @param attributes      atributos do usuário
 * @param contentType     informativo; decide se o corpo vai como texto ou Base64 no SQS/SNS
 * @param orderingKey     chave de ordem (ADR-0006); {@code null} sem ordem
 * @param deduplicationId id de deduplicação do provedor; {@code null} sem deduplicação
 * @param traceparent     contexto W3C; normalmente preenchido pelo starter
 */
public record OutgoingMessage(byte[] body,
                              Map<String, String> attributes,
                              String contentType,
                              String orderingKey,
                              String deduplicationId,
                              String traceparent) {

    public static final int MAX_ATTRIBUTES = 16;
    public static final int MAX_KEY_LENGTH = 64;
    public static final int MAX_VALUE_LENGTH = 1024;
    public static final int MAX_ID_LENGTH = 128;

    private static final Pattern KEY = Pattern.compile("[a-z_][a-z0-9_]*");
    private static final Pattern PRINTABLE = Pattern.compile("[\\x20-\\x7E]*");
    private static final Pattern ID = Pattern.compile("[\\x21-\\x7E]+");
    private static final Pattern TRACEPARENT = Pattern.compile("[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");

    public OutgoingMessage {
        Objects.requireNonNull(body, "body");
        if (body.length == 0) {
            throw new IllegalArgumentException("Corpo vazio: os provedores exigem ao menos 1 byte");
        }
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        validateAttributes(attributes);
        if (contentType != null && (contentType.isBlank() || !PRINTABLE.matcher(contentType).matches())) {
            throw new IllegalArgumentException("contentType inválido: '" + contentType + "'");
        }
        validateId("orderingKey", orderingKey);
        validateId("deduplicationId", deduplicationId);
        if (traceparent != null && !TRACEPARENT.matcher(traceparent).matches()) {
            throw new IllegalArgumentException("traceparent fora do formato W3C: '" + traceparent + "'");
        }
    }

    public static OutgoingMessage of(byte[] body) {
        return new OutgoingMessage(body, Map.of(), null, null, null, null);
    }

    /** Texto em UTF-8 com {@code contentType} {@code text/plain; charset=utf-8}. */
    public static OutgoingMessage ofText(String text) {
        return new OutgoingMessage(text.getBytes(StandardCharsets.UTF_8), Map.of(), "text/plain; charset=utf-8",
                null, null, null);
    }

    public OutgoingMessage withAttribute(String key, String value) {
        Map<String, String> copy = new HashMap<>(attributes);
        copy.put(key, value);
        return withAttributes(copy);
    }

    public OutgoingMessage withAttributes(Map<String, String> newAttributes) {
        return new OutgoingMessage(body, newAttributes, contentType, orderingKey, deduplicationId, traceparent);
    }

    public OutgoingMessage withContentType(String newContentType) {
        return new OutgoingMessage(body, attributes, newContentType, orderingKey, deduplicationId, traceparent);
    }

    public OutgoingMessage withOrderingKey(String newOrderingKey) {
        return new OutgoingMessage(body, attributes, contentType, newOrderingKey, deduplicationId, traceparent);
    }

    public OutgoingMessage withDeduplicationId(String newDeduplicationId) {
        return new OutgoingMessage(body, attributes, contentType, orderingKey, newDeduplicationId, traceparent);
    }

    public OutgoingMessage withTraceparent(String newTraceparent) {
        return new OutgoingMessage(body, attributes, contentType, orderingKey, deduplicationId, newTraceparent);
    }

    private static void validateAttributes(Map<String, String> attributes) {
        if (attributes.size() > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException("Mais de " + MAX_ATTRIBUTES + " atributos: " + attributes.size());
        }
        attributes.forEach((key, value) -> {
            if (key.length() > MAX_KEY_LENGTH || !KEY.matcher(key).matches() || key.startsWith("goog")) {
                throw new IllegalArgumentException("Chave de atributo inválida: '" + key
                        + "' (use [a-z_][a-z0-9_]*, até " + MAX_KEY_LENGTH + " caracteres, sem prefixo goog)");
            }
            if (ReservedAttributes.isReserved(key)) {
                throw new IllegalArgumentException("Chave de atributo reservada pela lib: '" + key + "'");
            }
            if (value.length() > MAX_VALUE_LENGTH || !PRINTABLE.matcher(value).matches()) {
                throw new IllegalArgumentException("Valor do atributo '" + key
                        + "' fora de ASCII imprimível ou acima de " + MAX_VALUE_LENGTH + " bytes");
            }
        });
    }

    private static void validateId(String name, String value) {
        if (value != null && (value.length() > MAX_ID_LENGTH || !ID.matcher(value).matches())) {
            throw new IllegalArgumentException(name + " inválido: use ASCII imprimível sem espaço, até "
                    + MAX_ID_LENGTH + " caracteres");
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof OutgoingMessage that
                && Arrays.equals(body, that.body)
                && attributes.equals(that.attributes)
                && Objects.equals(contentType, that.contentType)
                && Objects.equals(orderingKey, that.orderingKey)
                && Objects.equals(deduplicationId, that.deduplicationId)
                && Objects.equals(traceparent, that.traceparent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(body), attributes, contentType, orderingKey, deduplicationId, traceparent);
    }

    /** Mostra o tamanho do corpo, não o conteúdo. */
    @Override
    public String toString() {
        return "OutgoingMessage[body=" + body.length + " bytes, attributes=" + attributes
                + ", contentType=" + contentType + ", orderingKey=" + orderingKey
                + ", deduplicationId=" + deduplicationId + ", traceparent=" + traceparent + "]";
    }
}
