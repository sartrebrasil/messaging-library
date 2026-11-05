# ADR-0007: Observabilidade (métricas e tracing)

- Status: aceito (2026-10-08)
- Data: 2026-10-02
- Revisão: 2026-10-07 (nível 3, `failed`, circuit breaker, batch, redrive, stale check)
- Revisão: 2026-10-07 (`discarded`, `expired` e `rejected`, ADRs 0014 e 0015)
- Revisão: 2026-10-07 (`circuit_open`; `released` sem circuit breaker; `retry_delayed` sem exigir nível 1)
- Revisão: 2026-11-05 (body da outbox primeiro na procura do `traceparent`; gauge `inbox_last_receive_age_seconds`)

## Contexto

Quem opera um consumer precisa responder, sem abrir log: quantas mensagens
chegaram, quantas foram processadas, quantas estão em retry e em qual nível,
quantas foram para a DLQ e por quê. A outbox-library já usa Micrometer com
nomes `outbox_*` e proíbe labels de alta cardinalidade; a inbox segue o mesmo
padrão.

## Decisão

Toda entrega passa pela Micrometer Observation `inbox.consume`, que gera as
métricas abaixo e o span de trace. Micrometer é opcional: sem
`MeterRegistry` no contexto, nada é registrado.

### Métricas

| Métrica | Tipo | Tags | Quando incrementa |
|---|---|---|---|
| `inbox_messages_received_total` | counter | `consumer`, `destination` | Toda mensagem entregue pelo broker, inclusive reentregas |
| `inbox_messages_processed_total` | counter | `consumer`, `destination`, `outcome` | Resultado final de cada entrega |
| `inbox_retry_total` | counter | `consumer`, `destination`, `level`, `strategy`, `exception` | Cada retry agendado |
| `inbox_dead_letter_total` | counter | `consumer`, `destination`, `exception` | Cada mensagem enviada à DLQ (entrada no nível 3) |
| `inbox_failed_total` | counter | `consumer`, `destination`, `reason`, `exception` | Cada mensagem enviada à `failed` |
| `inbox_processing_duration` | timer | `consumer`, `destination`, `outcome` | Tempo do pipeline por entrega, incluindo retries do nível 1 e esperas do `hold` |
| `inbox_message_age` | timer | `consumer`, `destination` | De `ce_time`/`occurredAt` até o início do consumo. Só registra quando a mensagem traz o horário |
| `inbox_dead_letter_pass_total` | counter | `consumer`, `end` | Cada passada do nível 3 terminada; `end` = `caught_up`, `max_per_pass`, `circuit_open`, `shutdown` |
| `inbox_circuit_state` | gauge | `circuit` | 0 fechado, 1 meio aberto, 2 aberto (ADR-0010) |
| `inbox_circuit_transitions_total` | counter | `circuit`, `to` | Cada mudança de estado do circuit breaker |
| `inbox_batch_size` | distribution summary | `consumer` | Mensagens entregues ao handler por batch (ADR-0011) |
| `inbox_redrive_total` | counter | `consumer`, `source`, `target`, `result` | Cada mensagem movida pelo redrive; `result` = `moved`, `skipped`, `error` (ADR-0012) |
| `inbox_last_receive_age_seconds` | gauge | `consumer` | Segundos desde o último `receive` concluído, com mensagem ou vazio. Sobe quando o consumer para de ler, mesmo com a fila vazia (incidente T-88/B20 do engine-tax) |
| `inbox_effect_failed_total` | counter | `consumer`, `effect` | Falha ao chamar o broker; `effect` = `receive`, `ack`, `nack`, `copy` |
| `inbox_retry_skipped_total` | counter | `consumer`, `reason` | Nível 2 pulado; `reason` = `delivery_count_unavailable`, `redelivery_limit` |
| `inbox_stale_race_total` | counter | `consumer` | `advance` do stale check não gravou porque outra mensagem gravou versão maior durante o handler (ADR-0013) |
| `inbox_attribute_budget_exceeded_total` | counter | `consumer` | Cópia sem atributos da inbox por causa do limite de 16 atributos (ADR-0002) |

### Valores das tags

`outcome`:

| Valor | Significado |
|---|---|
| `success` | Handler concluiu, mensagem confirmada |
| `duplicate` | Idempotência detectou que já foi processada; confirmada sem chamar o handler |
| `stale` | Stale check: versão do aggregate menor ou igual à última processada; confirmada sem chamar o handler (ADR-0013) |
| `expired` | Mais velha que `expiration.max-age`; confirmada sem chamar o handler (ADR-0015) |
| `discarded` | Handler devolveu `Discard`; confirmada (ADR-0014) |
| `retry_delayed` | Reentrega com atraso pela estratégia do nível 2: nível 1 esgotado, `SkipImmediate` ou `RetryAfter` (ADR-0014) |
| `retry_dead_letter` | Falhou numa passada do nível 3; voltou para o fim da DLQ |
| `in_progress` | Outra instância está processando a mesma mensagem; reentrega com atraso |
| `circuit_open` | O circuito abriu com a mensagem em mãos; reentrega com atraso pela estratégia do nível 2 (ADR-0010) |
| `dead_letter` | Enviada à DLQ |
| `failed` | Enviada à `failed` |
| `released` | Devolvida com `nack(0)` sem processar: shutdown, fim de passada do nível 3. Onde o provedor conta entregas, a reentrega conta como tentativa (ADR-0002) |

Com `hold`, uma entrega física pode ter várias execuções; o `processed` conta
uma vez, no resultado final.

`level`:

| Valor | Significado |
|---|---|
| `immediate` | Retry em memória (nível 1, ADR-0002) |
| `delayed` | Reentrega com atraso (nível 2, ADR-0002) |
| `dead_letter` | Reenfileiramento no fim da DLQ (nível 3, ADR-0002) |

`strategy` (só no nível 2): `broker_delay`, `retry_queues`, `hold`; `none`
nos outros níveis.

`reason` (só em `inbox_failed_total`):

| Valor | Significado |
|---|---|
| `non_retryable` | Exceção classificada como `NonRetryable`; não passou por retry |
| `retries_exhausted` | Esgotou os níveis 1, 2 e 3 |
| `deserialization` | Envelope ou payload não pôde ser lido |
| `idempotency_conflict` | Mesmo id de mensagem com payload diferente (ADR-0004) |
| `rejected` | Handler devolveu `Reject` (ADR-0014) |
| `expired` | Expirada com `expiration.action: failed` (ADR-0015) |

`exception`: nome simples da classe da exceção (`SocketTimeoutException`),
ou `none`. A cardinalidade fica limitada às exceções existentes no código.

### Regras

- Proibido usar como tag: id da mensagem, aggregateId, traceId ou qualquer
  valor vindo do payload.
- `inbox_messages_received_total` menos a soma de
  `inbox_messages_processed_total` mostra mensagens em andamento ou perdidas
  num crash.
- Cada entrega gera exatamente um incremento de `received` e um de
  `processed`. Retries do nível 1 contam só em `inbox_retry_total`.

### Exemplos de consulta (PromQL)

```promql
# Mensagens que foram para a failed sem retry, por consumer
sum by (consumer) (rate(inbox_failed_total{reason="non_retryable"}[5m]))

# Circuit breakers abertos
inbox_circuit_state == 2

# Retries por nível
sum by (consumer, level) (rate(inbox_retry_total[5m]))

# Taxa de sucesso
sum(rate(inbox_messages_processed_total{outcome="success"}[5m]))
  / sum(rate(inbox_messages_processed_total[5m]))
```

### Tracing

Objetivo: a mensagem que chega com contexto de trace continua o mesmo trace.
Com a outbox-library, enqueue, publish e consume aparecem num trace só, e
tudo o que o handler fizer também.

1. **Formatos lidos:** W3C `traceparent`, `tracestate` e `baggage`. Ordem de
   procura (revisão de 2026-11-05):
   1. mapa `headers` do body da outbox (ADR-0003);
   2. extensão CloudEvents;
   3. header nativo do broker (`ReceivedMessage.traceparent`).

   O body vem primeiro porque guarda o contexto de quem gravou o evento, que
   é o trace que o consumo continua. O do transporte é o do publisher da
   outbox, que pode ter aberto um trace próprio. É a ordem que o
   `QueueConsumer` do engine-tax já usava.

   A messaging-lib só propaga `traceparent` (ADR-0004 dela). Nos provedores
   dela, `tracestate` e `baggage` só chegam pelo body da outbox.
2. **Reuso do contexto:** o span `CONSUMER` (`<destination> process`) é
   **filho** do contexto recebido, com o mesmo trace id do produtor.
3. **Contexto corrente:** o span fica ativo (scope aberto) durante todo o
   handler, incluindo os retries do nível `immediate`. Chamadas HTTP
   instrumentadas, JDBC, `KafkaTemplate` e `SpringOutboxWriter` herdam o
   trace sem código na aplicação.
4. **Retry e DLQ:** toda cópia (retry topic, `retry-queues`, DLQ, nível 3,
   `failed`, redrive) leva o `traceparent` original, não o do span de
   consumo, para manter o trace do produtor. Na reentrega por `nack` os
   atributos já são preservados. Cada nova tentativa vira um span novo no
   mesmo trace, com atributos `inbox.attempt` e `inbox.level`.
5. **Mensagem sem trace:** o consumo inicia um trace novo (root).
6. **Sem span duplicado:** o `inbox-kafka` desliga a observation nativa do
   container. No `inbox-messaging`, o `MeteredMessageReceiver` da
   messaging-lib só mede `receive`/`ack`, não abre span de processamento
   (ADR-0008 da messaging-lib). Fica só o span da inbox.
7. **Batch:** um span `<destination> process` por batch, com um span link
   para o contexto de cada mensagem, e não span filho (ADR-0011).
8. **Logs:** `traceId` e `spanId` no MDC durante o handler (vem pronto com
   Micrometer Tracing).

Atributos do span, no padrão OpenTelemetry de mensageria:
`messaging.system`, `messaging.destination.name`, `messaging.message.id`,
`messaging.consumer.group.name` (= consumer), mais `inbox.outcome`,
`inbox.attempt` e `inbox.level`. O id da mensagem pode aparecer no span,
nunca em métrica.

Fora do escopo do MVP: `AWSTraceHeader` (X-Ray) no SQS e propagação B3
(Zipkin).

Teste obrigatório (F2 em diante): handler que grava na outbox, e o consumer
seguinte recebe a mensagem com o mesmo trace id da mensagem original.

## Consequências

- Alertas típicos (DLQ crescendo, retry `delayed` alto, idade da mensagem
  subindo) ficam possíveis sem código na aplicação.
- O README traz um dashboard de exemplo com as consultas acima.
