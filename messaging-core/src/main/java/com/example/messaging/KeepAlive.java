package com.example.messaging;

/**
 * Renovação automática do lease de uma mensagem, criada por {@link MessageReceiver#keepAlive}.
 * {@link #close()} para a renovação e nunca lança.
 */
public interface KeepAlive extends AutoCloseable {

    /** {@code true} se uma renovação falhou: o lease pode ter vencido e o {@code ack} tende a falhar. */
    boolean lost();

    @Override
    void close();
}
