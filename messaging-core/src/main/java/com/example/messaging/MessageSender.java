package com.example.messaging;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Envio para uma fila ou um tópico, ligado a um único destino na construção. Thread-safe.
 *
 * <p>Falhas lançam {@link MessagingException} ou uma subclasse. Uso depois de {@link #close()}
 * lança {@link IllegalStateException}.</p>
 */
public interface MessageSender extends AutoCloseable {

    /**
     * Envia e espera a confirmação do provedor.
     *
     * @throws MessageTooLargeException a mensagem passa de {@link Capabilities#maxMessageBytes()}
     * @throws IllegalArgumentException o destino exige {@code orderingKey} (SQS/SNS FIFO, Service Bus com sessions)
     */
    SendResult send(OutgoingMessage message);

    /**
     * Envia várias mensagens. Não é atômico: cada posição pode falhar sozinha. O padrão chama
     * {@link #send} uma a uma; os adapters agrupam em lotes do tamanho do provedor.
     */
    default BatchSendResult sendAll(List<OutgoingMessage> messages) {
        Map<Integer, SendResult> sent = new HashMap<>();
        Map<Integer, MessagingException> failures = new HashMap<>();
        for (int i = 0; i < messages.size(); i++) {
            try {
                sent.put(i, send(messages.get(i)));
            } catch (MessagingException e) {
                failures.put(i, e);
            }
        }
        return new BatchSendResult(sent, failures);
    }

    /**
     * Confirma que o destino existe e que as credenciais alcançam, sem enviar nada. Base do
     * health check.
     */
    void checkAccess();

    Capabilities capabilities();

    /** Libera recursos do adapter. Não fecha o cliente nativo recebido na construção. */
    @Override
    void close();
}
