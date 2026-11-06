package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;

/**
 * Os clientes nativos de um provider configurado (criados pelo starter ou beans da aplicação) e os
 * senders e receivers sobre eles. Uma implementação por tipo, em {@code com.example.messaging.spring.<provedor>},
 * criada só quando o adapter está no classpath.
 */
public interface ProviderConnection extends AutoCloseable {

    /** {@code null} quando o destino não tem endereço de envio. */
    MessageSender sender(String name, MessagingProperties.Destination destination);

    /**
     * {@code null} quando o destino não tem endereço de recebimento.
     *
     * @param deadLetterSender sender do destino {@code dead-letter}; {@code null} sem DLQ
     */
    MessageReceiver receiver(String name, MessagingProperties.Destination destination, MessageSender deadLetterSender);

    /** Fecha só os clientes que a conexão criou; os beans da aplicação são de quem os declarou. */
    @Override
    void close();
}
