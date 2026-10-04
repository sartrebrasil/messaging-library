package com.example.messaging;

import java.util.Objects;

/**
 * Origem de uma mensagem que chegou à DLQ por {@link MessageReceiver#deadLetter}.
 *
 * @param originalMessageId id da mensagem na fila de origem; {@code null} quando o provedor não informa
 * @param reason            motivo passado a {@code deadLetter}
 */
public record DeadLetterInfo(String originalMessageId, String reason) {

    /** Valor máximo do atributo reservado {@code dead_letter} (limite do Pub/Sub). */
    public static final int MAX_ATTRIBUTE_LENGTH = 1024;

    public DeadLetterInfo {
        Objects.requireNonNull(reason, "reason");
    }

    /**
     * {@code <originalMessageId>;<reason>}, com o motivo reduzido a ASCII imprimível e truncado para
     * o valor caber em {@value #MAX_ATTRIBUTE_LENGTH} bytes.
     */
    public String toAttributeValue() {
        String prefix = (originalMessageId == null ? "" : originalMessageId) + ";";
        String ascii = reason.replaceAll("[^\\x20-\\x7E]", "?");
        int room = Math.max(0, MAX_ATTRIBUTE_LENGTH - prefix.length());
        return prefix + (ascii.length() > room ? ascii.substring(0, room) : ascii);
    }

    /** Inverso de {@link #toAttributeValue()}. */
    public static DeadLetterInfo fromAttributeValue(String value) {
        int separator = value.indexOf(';');
        if (separator < 0) {
            return new DeadLetterInfo(null, value);
        }
        String id = value.substring(0, separator);
        return new DeadLetterInfo(id.isEmpty() ? null : id, value.substring(separator + 1));
    }
}
