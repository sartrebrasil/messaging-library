package com.example.messaging;

import java.util.Map;

/**
 * Resultado de {@link MessageSender#sendAll}. As chaves são as posições na lista enviada; cada
 * posição aparece em exatamente um dos dois mapas.
 */
public record BatchSendResult(Map<Integer, SendResult> sent, Map<Integer, MessagingException> failures) {

    public BatchSendResult {
        sent = Map.copyOf(sent);
        failures = Map.copyOf(failures);
    }

    public boolean isSuccess() {
        return failures.isEmpty();
    }
}
