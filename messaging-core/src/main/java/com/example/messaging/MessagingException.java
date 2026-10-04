package com.example.messaging;

/**
 * Falha no provedor de mensageria. Subclasses indicam os casos que o chamador costuma tratar.
 *
 * <p>O SDK de cada provedor já repete as falhas transitórias. O que chega aqui é o que sobrou;
 * {@link #retryable()} diz se tentar de novo faz sentido. A exceção original do SDK fica em
 * {@link #getCause()}.</p>
 */
public class MessagingException extends RuntimeException {

    private final String provider;
    private final boolean retryable;

    public MessagingException(String provider, String message, Throwable cause, boolean retryable) {
        super(message, cause);
        this.provider = provider;
        this.retryable = retryable;
    }

    /** Nome do provedor ({@code aws}, {@code azure}, {@code gcp}, {@code memory}). */
    public String provider() {
        return provider;
    }

    /** {@code true} para timeout, erro 5xx e throttling; {@code false} para erro de validação ou permissão. */
    public boolean retryable() {
        return retryable;
    }
}
