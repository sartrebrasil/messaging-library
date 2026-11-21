package com.example.messaging.spi;

import java.util.Set;

/**
 * Atributos que a lib grava por conta própria (ADR-0004). Quem envia não pode usá-los, e o
 * receiver os tira de {@link com.example.messaging.ReceivedMessage#attributes()}.
 */
public final class ReservedAttributes {

    /** {@code contentType} onde o provedor não tem campo próprio. */
    public static final String CONTENT_TYPE = "content_type";
    /** Contexto W3C de trace. */
    public static final String TRACEPARENT = "traceparent";
    /** Cópia enviada à DLQ pela lib: {@code <messageId original>;<motivo>}. */
    public static final String DEAD_LETTER = "dead_letter";
    /** Contador de entregas do reenvio agendado no Service Bus ({@code RESCHEDULE}). */
    public static final String DELIVERY_COUNT = "delivery_count";
    /** Atributos do usuário empacotados em JSON quando não cabem como atributos nativos. */
    public static final String ATTRIBUTES = "attributes";

    public static final Set<String> ALL = Set.of(CONTENT_TYPE, TRACEPARENT, DEAD_LETTER, DELIVERY_COUNT, ATTRIBUTES);

    private ReservedAttributes() {
    }

    /** Número de entregas gravado por outro produtor ou pela lib; valor ilegível conta como 0. */
    public static int parseCount(String value) {
        try {
            return Math.max(0, Integer.parseInt(value.trim()));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static boolean isReserved(String key) {
        return ALL.contains(key);
    }
}
