package com.example.messaging;

/**
 * O lease da mensagem venceu ou foi perdido: outra entrega pode já estar em andamento.
 * Confirmar de novo não adianta; quem consome precisa ser idempotente.
 */
public class LeaseExpiredException extends MessagingException {

    public LeaseExpiredException(String provider, String message, Throwable cause) {
        super(provider, message, cause, false);
    }
}
