package com.example.messaging;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Recebimento por pull com lease de uma fila ou subscription, ligado a um único destino na
 * construção (ADR-0002). Thread-safe; no Service Bus com sessions, chamadas concorrentes de
 * {@link #receive} na mesma instância são serializadas.
 *
 * <p>Regras comuns:</p>
 * <ul>
 *   <li>A mensagem fica invisível para outros consumidores até {@link ReceivedMessage#leaseExpiresAt()}.
 *       Sem confirmação, volta a ser entregue.</li>
 *   <li>{@code ack}, {@code nack}, {@code extendLease} e {@code deadLetter} com lease vencido lançam
 *       {@link LeaseExpiredException} onde o provedor informa
 *       ({@link Capabilities#reportsLeaseExpiredOnAck()}). Quem consome precisa ser idempotente.</li>
 *   <li>Mensagem recebida por outro receiver lança {@link IllegalArgumentException}.</li>
 *   <li>Uso depois de {@link #close()} lança {@link IllegalStateException}.</li>
 * </ul>
 */
public interface MessageReceiver extends AutoCloseable {

    /**
     * Bloqueia até ter ao menos uma mensagem ou até {@code maxWait}. Lista vazia não é erro.
     * {@code maxMessages} acima do limite do provedor é reduzido ao limite.
     *
     * @throws IllegalArgumentException {@code maxMessages < 1} ou {@code maxWait} negativo
     */
    List<ReceivedMessage> receive(int maxMessages, Duration maxWait);

    /** Confirma o processamento: a mensagem não volta a ser entregue. */
    void ack(ReceivedMessage message);

    /**
     * Confirma várias mensagens. Não é atômico. O padrão chama {@link #ack} uma a uma; os
     * adapters agrupam em lotes do tamanho do provedor.
     */
    default AckResult ackAll(List<ReceivedMessage> messages) {
        Map<String, MessagingException> failures = new HashMap<>();
        for (ReceivedMessage message : messages) {
            try {
                ack(message);
            } catch (MessagingException e) {
                failures.put(message.messageId(), e);
            }
        }
        return new AckResult(failures);
    }

    /**
     * Devolve para nova entrega depois de {@code redeliverAfter} ({@link Duration#ZERO} = imediato).
     * Sem {@link Capabilities#delayedRedelivery()}, o atraso é ignorado e a devolução é imediata
     * (Service Bus em {@code ABANDON}).
     *
     * @throws IllegalArgumentException atraso negativo, ou acima de {@link Capabilities#maxRedeliveryDelay()}
     *                                  quando o adapter respeita atraso
     */
    void nack(ReceivedMessage message, Duration redeliverAfter);

    /** Estende o lease em pelo menos {@code lease} a partir de agora. */
    void extendLease(ReceivedMessage message, Duration lease);

    /**
     * Renova o lease em segundo plano, quando falta 1/3 dele, até {@link KeepAlive#close()},
     * até {@code ack}/{@code nack}/{@code deadLetter} da mensagem ou até {@code maxTotal}.
     *
     * <pre>{@code
     * try (KeepAlive lease = receiver.keepAlive(message, Duration.ofMinutes(30))) {
     *     process(message);
     *     receiver.ack(message);
     * }
     * }</pre>
     *
     * @throws IllegalArgumentException {@code maxTotal} não positivo
     */
    KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal);

    /**
     * Move para a dead letter queue. Nativo no Service Bus; nos outros provedores, cópia para o
     * sender de DLQ configurado e depois {@code ack} (ADR-0005).
     *
     * @throws UnsupportedOperationException sem dead letter nativo nem sender de DLQ configurado
     */
    void deadLetter(ReceivedMessage message, String reason);

    /**
     * Confirma que o destino existe e que as credenciais alcançam, sem consumir mensagens. Base do
     * health check.
     */
    void checkAccess();

    Capabilities capabilities();

    /**
     * Para os {@link KeepAlive}, libera a session (se houver) e não confirma nada: mensagens
     * pendentes voltam quando o lease vence. Não fecha o cliente nativo recebido na construção.
     */
    @Override
    void close();
}
