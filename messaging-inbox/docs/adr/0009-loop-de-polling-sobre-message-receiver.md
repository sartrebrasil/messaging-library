# ADR-0009: `inbox-messaging`: loop de polling sobre `MessageReceiver`

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-11-05 (esperas no `StopSignal`, sem interrupt; `Error` não mata o worker)
- Revisão: 2026-10-07 (`Release`; `hold` e nível 3 no core; orçamento do `keepAlive` por `receive`;
  batch acima do `maxBatchSize` com `max-wait`)

## Contexto

O ADR-0001 troca os containers Spring de SQS, RabbitMQ e JMS pela messaging-lib.
A messaging-lib só oferece pull síncrono com lease (`receive`, `ack`, `nack`,
`extendLease`, `keepAlive`, `deadLetter`) e deixa de propósito para quem
consome o loop, a concorrência e o shutdown (ADR-0001 dela).

Fatos da messaging-lib que moldam o loop:

- `receive(maxMessages, maxWait)` bloqueia até ter mensagem ou até `maxWait`.
  Lista vazia não é erro.
- `receiver(nome)` do `MessagingDestinations` devolve uma instância nova a
  cada chamada, compartilhando o cliente nativo. No Service Bus com sessions,
  cada instância segura uma session.
- `keepAlive(message, maxTotal)` renova o lease em segundo plano;
  `KeepAlive.lost()` avisa quando uma renovação falhou.
- `close()` do receiver não confirma nada: as mensagens pendentes voltam
  quando o lease vence.
- No Java 21, o `receive` dos clientes JMS prende a carrier thread de uma
  virtual thread durante o `maxWait` (ADR-0009 da messaging-lib).

## Decisão

### Workers

- Cada consumer tem `concurrency` workers (padrão 1). Cada worker é uma
  platform thread com o **próprio** receiver (`receiver(nome)`).
- Um receiver por worker serve para todos os provedores: no Service Bus com
  sessions, `concurrency` é o número de sessions processadas em paralelo.
- Platform thread e não virtual thread, por causa do pinning do JMS no
  Java 21. Os workers são poucos (um por unidade de paralelismo), então o custo
  é baixo. Virtual threads ficam para quando o baseline for Java 24+.

### Loop

```
enquanto rodando:
    se circuit breaker aberto: espera fechar ou meio abrir (ADR-0010)
    msgs = receiver.receive(max-messages-per-poll, poll-wait)
    para cada m em msgs: keepAlive(m, orçamento)         // aberto logo após o receive
    para cada m em msgs, em sequência:                   // ou o batch inteiro (ADR-0011)
        outcome = pipeline.handle(m)
        aplicar(outcome)
        fecha o keepAlive de m
```

- `max-messages-per-poll` padrão 1. Acima de 1, as mensagens do mesmo poll são
  processadas em sequência no mesmo worker, todas com o lease renovado desde o
  receive. Paralelismo vem de `concurrency`, não do tamanho do poll.
- `max-processing` (padrão 15 min) é o tempo máximo de processamento de uma
  entrega. Com `hold`, soma-se a ele os atrasos máximos do nível 2. O TTL de
  reserva da idempotência precisa ser maior que esse valor (ADR-0004).
- O orçamento do `keepAlive` (`maxTotal`) conta a espera da mensagem atrás das
  outras do mesmo `receive`: `max-processing` vezes o número de mensagens que
  rodam em sequência antes dela, inclusive ela. Sem lanes, é o tamanho do
  `receive`; com `key-lanes` (ADR-0016), o tamanho da maior lane. No batch,
  `max-wait` mais `max-processing`, que vale para o lote inteiro (ADR-0011).
- No RabbitMQ, o `consumer_timeout` da fila precisa ser maior que o orçamento
  (ADR-0009 da messaging-lib).

### Aplicação do `Outcome`

| `Outcome` | Efeito |
|---|---|
| `Ack` | `receiver.ack(m)` |
| `Retry`, `broker-delay` | `receiver.nack(m, delay)` |
| `Retry`, `retry-queues` | `sender(faixa).send(cópia)`, depois `ack(m)` |
| `Retry`, `hold` | Nenhum no broker: o core espera `delay` e roda o pipeline de novo, no mesmo worker (ADR-0001); o adapter só mantém o `keepAlive` |
| `DeadLetter` | `sender(dead-letter).send(cópia)`, depois `ack(m)`. O core já transformou em `Failed` quando não há `dead-letter` (ADR-0002) |
| `Failed` | `sender(failed).send(cópia)`, depois `ack(m)` |
| `Release` | `receiver.nack(m, 0)` |

- As cópias seguem o ADR-0002: envio antes do `ack`, `deduplicationId` =
  `messageId` de origem, atributos `inbox_*`.
- A inbox não usa `receiver.deadLetter()`: a DLQ da inbox é um destino comum
  (ADR-0002), e a `failed` também.
- Falha no envio da cópia ou no `nack`: a mensagem não é confirmada, volta por
  `nack(0)` (ou pelo vencimento do lease, se o `nack` também falhar) e
  `inbox_effect_failed_total` incrementa.
- `LeaseExpiredException` no `ack`: log de aviso e métrica. A reentrega vira
  `duplicate` pela idempotência. Sem idempotência, o handler roda de novo.
- `KeepAlive.lost()` antes do `ack`: o `ack` é tentado mesmo assim. Onde o
  provedor não informa lease vencido (SQS, Pub/Sub sem exactly-once), não há
  como saber se ele valeu.

### Erros do `receive`

- `MessagingException` com `retryable = true`: o worker espera um backoff
  (1 s, x2, máx. 30 s, jitter 0.2) e tenta de novo.
- `retryable = false` (`AccessDeniedException`, `DestinationNotFoundException`):
  mesmo backoff, com máx. 1 min, e log de erro a cada tentativa. O worker não
  morre: credencial e destino costumam ser corrigidos sem restart.
- Métrica `inbox_effect_failed_total{effect="receive"}`.

### Nível 3

- O agendamento e a lógica da passada são do core (ADR-0001, ADR-0002): a cada
  `retry.dead-letter.interval`, se a passada anterior terminou e o circuit
  breaker está fechado, o core pede ao adapter que busque na DLQ e decide o
  fim da passada.
- No `inbox-messaging`, a passada usa `retry.dead-letter.concurrency` workers,
  cada um com o próprio receiver da DLQ, e o mesmo loop.
- Entre passadas, os receivers da DLQ ficam abertos e parados (sem
  `receive`), sem mensagem pendente.

### Shutdown

A inbox é um `SmartLifecycle` que para antes do `MessagingDestinations`.

1. Para de chamar `receive`. As mensagens que chegarem de um `receive` já em
   andamento voltam por `Release` (`outcome=released`).
2. Interrompe as esperas de `hold` e de backoff do nível 1: a mensagem volta
   por `Release`. Em provedores que contam entregas, isso gasta uma tentativa
   (ADR-0002).
3. Espera as execuções do handler em andamento até `shutdown-timeout` (padrão
   30 s).
4. Fecha os receivers. O que não terminou volta ao broker quando o lease vence.

Revisão de 2026-11-05: as esperas (backoff do nível 1, `hold`, backoff do
`receive`) dormem num `StopSignal` do consumer, não com `Thread.interrupt()`.
O passo 2 acorda só as esperas; um handler no meio de uma chamada JDBC ou
HTTP nunca é interrompido pelo shutdown. Uma falha inesperada numa mensagem,
inclusive `Error` (`StackOverflowError`, `AssertionError`), devolve a mensagem
por `Release` e o worker continua; só `OutOfMemoryError` encerra o worker, com
log de erro.

Um `receive` bloqueado não é interrompível em todos os clientes, então o
shutdown pode levar até `poll-wait` a mais. `poll-wait` (padrão 20 s) é
limitado ao `shutdown-timeout`.

### Validação no startup

Lidas as `Capabilities` do receiver e dos senders:

| Combinação | Resultado |
|---|---|
| `ordering: strict` com `orderedDelivery = false` | Falha |
| `delayed.strategy: broker-delay` com `delayedRedelivery = false` | Falha |
| `delayed.max` explícito acima de `maxRedeliveryDelay` | Falha |
| `retry-queues` sem filas ou com `strict` | Falha |
| `failed` ausente | Falha |
| `batch.max-size` maior que `maxBatchSize` do receiver | O `max-messages-per-poll` é reduzido ao limite. Com `batch.max-wait > 0`, o lote junta vários `receive` até `max-size` (ADR-0011); com `max-wait = 0`, o lote fica no limite. Registra no log |
| TTL de reserva da idempotência menor que `max-processing` (+ `hold`) | Falha |

O startup registra no log, por consumer, a estratégia do nível 2 escolhida e o
motivo.

## Consequências

- Os seis provedores da messaging-lib usam o mesmo loop, testado uma vez com o
  `InMemoryMessaging` do `messaging-testkit` e depois no contrato com cada
  emulador.
- Paralelismo custa um receiver e uma thread por worker. No Service Bus com
  sessions, cada worker também segura uma session.
- A inbox não reconecta: a reconexão é do cliente nativo, como na messaging-lib.
