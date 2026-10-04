package com.example.messaging;

import java.time.Duration;

/**
 * O que o adapter suporta com a configuração que recebeu (ADR-0007). Não diz o que o destino tem
 * configurado na nuvem (FIFO, sessions, duplicate detection): isso é do Terraform.
 *
 * @param maxMessageBytes          corpo + atributos, como o provedor conta
 * @param maxBatchSize             mensagens por chamada de lote ao provedor
 * @param delayedRedelivery        {@link MessageReceiver#nack} respeita o atraso
 * @param maxRedeliveryDelay       maior atraso aceito por {@code nack}; {@link Duration#ZERO} sem atraso
 * @param nativeDeadLetter         {@link MessageReceiver#deadLetter} é do provedor, não cópia + ack
 * @param orderedDelivery          entrega em ordem por {@code orderingKey}
 * @param publisherDeduplication   o provedor deduplica por {@code deduplicationId}
 * @param reportsLeaseExpiredOnAck {@code ack} com lease vencido lança {@link LeaseExpiredException}
 *                                 em vez de responder sucesso em silêncio
 */
public record Capabilities(long maxMessageBytes,
                           int maxBatchSize,
                           boolean delayedRedelivery,
                           Duration maxRedeliveryDelay,
                           boolean nativeDeadLetter,
                           boolean orderedDelivery,
                           boolean publisherDeduplication,
                           boolean reportsLeaseExpiredOnAck) {
}
