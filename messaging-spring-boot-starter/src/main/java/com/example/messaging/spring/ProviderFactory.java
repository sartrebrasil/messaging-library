package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;

/**
 * Cria senders e receivers de um provedor configurado, com os clientes nativos que ele mesmo
 * criou (ou os beans da aplicação). Uma implementação por tipo, carregada só quando o adapter
 * está no classpath.
 */
interface ProviderFactory extends AutoCloseable {

    /** {@code null} quando o destino não tem endereço de envio. */
    MessageSender sender(String name, MessagingProperties.Destination destination);

    /**
     * {@code null} quando o destino não tem endereço de recebimento.
     *
     * @param deadLetterSender sender do destino {@code dead-letter}; {@code null} sem DLQ
     */
    MessageReceiver receiver(String name, MessagingProperties.Destination destination, MessageSender deadLetterSender);

    /** Fecha só os clientes que a fábrica criou; os beans da aplicação são de quem os declarou. */
    @Override
    void close();
}
