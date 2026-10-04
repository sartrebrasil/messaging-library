package com.example.messaging;

import java.util.Map;

/** Resultado de {@link MessageReceiver#ackAll}: ids das mensagens que não foram confirmadas e o motivo. */
public record AckResult(Map<String, MessagingException> failures) {

    public AckResult {
        failures = Map.copyOf(failures);
    }

    public boolean isSuccess() {
        return failures.isEmpty();
    }
}
