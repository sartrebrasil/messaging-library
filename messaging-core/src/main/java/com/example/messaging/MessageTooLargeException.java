package com.example.messaging;

/**
 * A mensagem passa do limite do provedor ({@link Capabilities#maxMessageBytes()}). Os adapters
 * lançam antes de chamar o SDK.
 */
public class MessageTooLargeException extends MessagingException {

    public MessageTooLargeException(String provider, String message, Throwable cause) {
        super(provider, message, cause, false);
    }
}
