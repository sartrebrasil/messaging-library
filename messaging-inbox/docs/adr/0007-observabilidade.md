# ADR-0007: Observabilidade (métricas e tracing)

- Status: aceito (2026-10-08)
- Data: 2026-10-02
- Revisão: 2026-10-07 (nível 3, `failed`, circuit breaker, batch, redrive, stale check)
- Revisão: 2026-10-07 (`discarded`, `expired` e `rejected`, ADRs 0014 e 0015)
- Revisão: 2026-10-07 (`circuit_open`; `released` sem circuit breaker; `retry_delayed` sem exigir nível 1)
- Revisão: 2026-11-05 (body da outbox primeiro na procura do `traceparent`; gauge `inbox_last_receive_age_seconds`)
- Revisão: 2026-11-05 (implementação no starter: nomes Micrometer, `ConsumerObserver`)
- Revisão: 2026-11-13 (convenção de nomes e renomeações, correções de `outcome`, métricas de diagnóstico, timers das
  observations; o que ainda não existe fica marcado como planejado)

## Contexto

Quem opera um consumer precisa responder, sem abrir log: quantas mensagens
chegaram, quantas foram processadas, quantas estão em retry e em qual nível,
quantas foram para a DLQ e por quê. A outbox-library já usa Micrometer com
nomes `outbox_*` e proíbe labels de alta cardinalidade; a inbox segue o mesmo
padrão.

## Decisão

Cada execução do pipeline passa pela Micrometer Observation `inbox.consume`,
que gera o span de trace e dois timers (seção "Timers das observations"). As
métricas da tabela abaixo são registradas pelo `ConsumerObserver` do starter,
com tags próprias. Micrometer é opcional: sem `MeterRegistry` no contexto,
nada é registrado.

Linhas marcadas **planejada** descrevem o que as features ainda não escritas
(circuit breaker, batch, stale check, expiração) vão emitir; hoje o código não
as registra.

### Métricas

| Métrica | Tipo | Tags | Quando incrementa |
|---|---|---|---|
| `inbox_messages_received_total` | counter | `consumer`, `destination` | Toda mensagem entregue pelo broker, inclusive reentregas |
| `inbox_messages_processed_total` | counter | `consumer`, `destination`, `outcome` | Resultado final de cada entrega |
| `inbox_messages_retried_total` | counter | `consumer`, `destination`, `level`, `strategy`, `exception` | Cada retry agendado |
| `inbox_messages_dead_lettered_total` | counter | `consumer`, `destination`, `exception` | Cada mensagem enviada à DLQ (entrada no nível 3) |
| `inbox_messages_failed_total` | counter | `consumer`, `destination`, `reason`, `exception` | Cada mensagem enviada à `failed` |
| `inbox_processing_duration_seconds` | timer | `consumer`, `destination`, `outcome` | Tempo do pipeline por entrega, incluindo retries do nível 1 e esperas do `hold` |
| `inbox_message_age_seconds` | timer | `consumer`, `destination` | De `ce_time`/`occurredAt` até o início do consumo. Só registra quando a mensagem traz o horário |
| `inbox_dead_letter_passes_total` | counter | `consumer`, `end` | Cada passada do nível 3 terminada; `end` = `empty`, `newer_than_pass`, `repeated_key`, `max_per_pass`, `stopping`, `receive_failed` (os valores de `PassEnd`) |
| `inbox_circuit_state` | gauge | `circuit` | **Planejada (F6).** 0 fechado, 1 meio aberto, 2 aberto (ADR-0010) |
| `inbox_circuit_transitions_total` | counter | `circuit`, `to` | **Planejada (F6).** Cada mudança de estado do circuit breaker |
| `inbox_batch_size` | distribution summary | `consumer` | **Planejada (ADR-0011).** Mensagens entregues ao handler por batch (ADR-0011) |
| `inbox_messages_redriven_total` | counter | `consumer`, `source`, `target`, `result` | Cada mensagem movida pelo redrive; `result` = `moved`, `skipped`, `error` (ADR-0012) |
| `inbox_redrives_ended_total` | counter | `consumer`, `source`, `target`, `end` | Cada execução de redrive terminada, por como terminou (`end` = os valores de `PassEnd`, ex.: `empty`, `max_per_pass`, `receive_failed`). Pedido recusado (receiver desconhecido, já em execução) não conta: o `consumer` é entrada do chamador |
| `inbox_messages_in_flight` | gauge | `consumer`, `destination` | Entregues pelo broker e ainda sem `ack`, `nack` ou cópia. Substitui a conta `received - processed`, que não sobrevive a um restart dos counters |
| `inbox_handler_duration_seconds` | timer | `consumer`, `destination`, `outcome` | Uma chamada do handler (cada retry imediato é uma); `outcome` = `success` ou `error` (lançou exceção). Separa lentidão da aplicação da lentidão do broker |
| `inbox_idempotency_duration_seconds` | timer | `consumer`, `destination`, `operation`, `outcome` | Uma chamada ao `IdempotencyStore`; `operation` = `reserve`, `complete`, `release`; `outcome` = `success` ou `error` |
| `inbox_delivery_attempts` | distribution summary | `consumer`, `destination`, `level` | Tentativas já gastas no nível em que a entrega estava (0 na primeira), por entrega. Mostra quantas rodadas uma mensagem costuma precisar |
| `inbox_hold_wait_duration_seconds` | timer | `consumer`, `destination` | Espera planejada de cada `hold` em memória (o `delay` do retry) |
| `inbox_keepalives_failed_total` | counter | `consumer`, `destination` | `keepAlive` que não pôde começar: a mensagem é processada sem renovar o lease |
| `inbox_failures_unexpected_total` | counter | `consumer`, `destination`, `stage`, `exception` | Falha que ninguém esperava (`Error`, bug); `stage` = `lane`, `delivery`, `dead_letter_pass`. Antes só havia log ERROR |
| `inbox_redrive_duration_seconds` | timer | `consumer`, `source`, `target` | Uma execução de redrive, do pedido ao relatório |
| `inbox_last_receive_age_seconds` | gauge | `consumer` | Segundos desde o último `receive` concluído, com mensagem ou vazio. Sobe quando o consumer para de ler, mesmo com a fila vazia (incidente T-88/B20 do engine-tax) |
| `inbox_effects_failed_total` | counter | `consumer`, `destination`, `effect` | Falha ao chamar o broker; `effect` = `receive`, `ack`, `nack`, `copy` |
| `inbox_retries_skipped_total` | counter | `consumer`, `destination`, `reason` | Nível 2 pulado; `reason` = `delivery_count_unavailable`, `redelivery_limit` |
| `inbox_stale_race_total` | counter | `consumer` | **Planejada (ADR-0013).** `advance` do stale check não gravou porque outra mensagem gravou versão maior durante o handler (ADR-0013) |
| `inbox_attributes_budget_exceeded_total` | counter | `consumer`, `destination` | Cópia sem atributos da inbox por causa do limite de 16 atributos (ADR-0002) |

### Convenção de nomes (revisão de 2026-11-13)

Vale para os meters `inbox.*` e `messaging.*` do starter; a outbox-library segue o mesmo padrão.

- Prefixo `inbox.` para o consumo e `messaging.` para chamadas ao broker.
- Counter: `<prefixo>.<entidade no plural>.<evento no particípio>` (`inbox.messages.retried`,
  `inbox.retries.skipped`). Exportado com `_total`.
- Timer: termina em `.duration` (`messaging.send.duration`); idade termina em `.age`. Exportado com `_seconds`.
- Tags e valores de tag em `snake_case` minúsculo; nada vindo do payload.
- Todo meter leva `description`; timer, summary e gauge, a unidade.
- `destination` é o nome lógico do receiver (nunca o endereço) e tem o mesmo valor nas métricas `inbox_*` e
  `messaging_*` do mesmo receiver, para cruzá-las.
- A passada do nível 3 usa o `consumer` do receiver que ela serve, e o `destination` `<receiver>.dead-letter`
  (o nome do receiver da DLQ no transporte). Por isso todo meter por entrega leva `consumer` e `destination`: sem o
  segundo, a passada e o loop principal cairiam na mesma série.
- `exception` é o nome simples da classe, ou `none`, tanto na inbox quanto nos timers de `messaging.*`; `outcome`
  fica para o resultado (`success`, `error`, `partial` no transporte; os valores da tabela abaixo na inbox).
- `messaging.system` (só nos spans) é o tipo do provider, o mesmo valor da tag `provider` dos meters do transporte
  (`aws`, `azure`, `gcp`, `activemq-classic`, `artemis`, `rabbitmq`), e não os valores do OpenTelemetry: `aws` cobre
  SQS e SNS, então `aws_sqs` estaria errado para o SNS. `messaging` quando o tipo não é conhecido.
- Os nomes ficam fixos pelo teste do starter: renomear exige mudar o teste e esta tabela juntos.

### Valores das tags

`outcome`:

| Valor | Significado |
|---|---|
| `success` | Handler concluiu, mensagem confirmada |
| `duplicate` | Idempotência detectou que já foi processada; confirmada sem chamar o handler |
| `stale` | **Planejado.** Stale check: versão do aggregate menor ou igual à última processada; confirmada sem chamar o handler (ADR-0013) |
| `expired` | **Planejado.** Mais velha que `expiration.max-age`; confirmada sem chamar o handler (ADR-0015) |
| `discarded` | Handler devolveu `Discard`; confirmada (ADR-0014) |
| `retry_delayed` | Reentrega com atraso pela estratégia do nível 2: nível 1 esgotado, `SkipImmediate` ou `RetryAfter` (ADR-0014) |
| `retry_dead_letter` | Falhou numa passada do nível 3; voltou para o fim da DLQ. Conta em `inbox_messages_retried_total{level="dead_letter"}`, e não em `inbox_messages_dead_lettered_total` |
| `in_progress` | Outra instância está processando a mesma mensagem; reentrega com atraso |
| `circuit_open` | **Planejado.** O circuito abriu com a mensagem em mãos; reentrega com atraso pela estratégia do nível 2 (ADR-0010) |
| `dead_letter` | Enviada à DLQ |
| `failed` | Enviada à `failed` |
| `lease_expired` | O trabalho e a cópia (se havia) foram feitos, mas o lease venceu antes do `ack`: a mensagem volta como duplicata. Não é `success` |
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

`reason` (só em `inbox_messages_failed_total`):

| Valor | Significado |
|---|---|
| `non_retryable` | Exceção classificada como `NonRetryable`; não passou por retry |
| `retries_exhausted` | Esgotou os níveis 1, 2 e 3 |
| `deserialization` | Envelope ou payload não pôde ser lido |
| `idempotency_conflict` | Mesmo id de mensagem com payload diferente (ADR-0004) |
| `rejected` | Handler devolveu `Reject` (ADR-0014) |
| `expired` | **Planejado.** Expirada com `expiration.action: failed` (ADR-0015) |

`exception`: nome simples da classe da exceção (`SocketTimeoutException`),
ou `none`. A cardinalidade fica limitada às exceções existentes no código.

### Regras

- Proibido usar como tag: id da mensagem, aggregateId, traceId ou qualquer
  valor vindo do payload.
- `inbox_messages_in_flight` mostra as mensagens em andamento. `inbox_messages_received_total` menos a soma de
  `inbox_messages_processed_total` serve para ver as perdidas num crash, mas só dentro da vida dos counters.
- Cada entrega gera exatamente um incremento de `received` e um de
  `processed`. Retries do nível 1 contam só em `inbox_messages_retried_total`.

### Exemplos de consulta (PromQL)

```promql
# Mensagens que foram para a failed sem retry, por consumer
sum by (consumer) (rate(inbox_messages_failed_total{reason="non_retryable"}[5m]))

# Retries por nível
sum by (consumer, level) (rate(inbox_messages_retried_total[5m]))

# Mensagens em andamento por consumer
sum by (consumer) (inbox_messages_in_flight)

# Tempo médio do handler (p95 pede percentile histogram no registry)
sum by (consumer) (rate(inbox_handler_duration_seconds_sum[5m]))
  / sum by (consumer) (rate(inbox_handler_duration_seconds_count[5m]))

# Lease vencida antes do ack: duplicatas a caminho
sum by (consumer) (rate(inbox_messages_processed_total{outcome="lease_expired"}[5m]))

# Falhas que ninguém esperava, por onde foram pegas
sum by (consumer, stage) (increase(inbox_failures_unexpected_total[1h]))

# Redrives que pararam por falha de receive
increase(inbox_redrives_ended_total{end="receive_failed"}[1h])

# Circuit breakers abertos (planejada, F6)
inbox_circuit_state == 2

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
   mesmo trace, com atributos `inbox.attempt` e `level`.
5. **Mensagem sem trace:** o consumo inicia um trace novo (root).
6. **Sem span duplicado:** o `inbox-kafka` desliga a observation nativa do
   container. No `inbox-messaging`, o `MeteredMessageReceiver` da
   messaging-lib só mede `receive`/`ack`, não abre span de processamento
   (ADR-0008 da messaging-lib). Fica só o span da inbox.
7. **Batch (planejado, ADR-0011):** um span `<destination> process` por batch, com um span link
   para o contexto de cada mensagem, e não span filho (ADR-0011).
8. **Logs:** `traceId` e `spanId` no MDC durante o handler (vem pronto com
   Micrometer Tracing).

Atributos do span, no padrão OpenTelemetry de mensageria:
`messaging.system`, `messaging.destination.name`, `messaging.message.id`,
`messaging.consumer.group.name` (= consumer), mais `inbox.attempt`. O id da
mensagem pode aparecer no span, nunca em métrica.

Revisão de 2026-11-13: o span também leva `consumer`, `destination`, `level` e
`outcome`, que são as tags dos timers (abaixo) e substituem `inbox.level` e
`inbox.outcome`. Os atributos OpenTelemetry são chaves de alta cardinalidade
do Micrometer, que nenhum meter usa; as curtas são de baixa cardinalidade, e
são elas que o `DefaultMeterObservationHandler` transforma em tags. Assim o
mesmo código serve ao backend de tracing (nomes OTel) e ao PromQL (nomes dos
meters `inbox_*`).

### Timers das observations (revisão de 2026-11-13)

Com um `ObservationRegistry` que tenha o `DefaultMeterObservationHandler` (o
padrão do Boot com Actuator), a observation também gera meters. Eles seguem o
nome da observation e ficam fora da regra do sufixo `.duration`:

| Meter | Tipo | Tags | Observação |
|---|---|---|---|
| `inbox_consume_seconds` | timer | `consumer`, `destination`, `level`, `outcome`, `error` | Uma por **execução** do pipeline, não por entrega: um `hold` gera várias, e cada tentativa do nível 1 fica dentro da mesma. Para tempo por entrega, use `inbox_processing_duration_seconds` |
| `inbox_consume_active_seconds` | long task timer | `consumer`, `destination`, `level`, `outcome` | Execuções em andamento. O `outcome` só é conhecido no fim, então aqui vale sempre `released` |

`error` é a tag que o Micrometer põe sozinha (nome simples da exceção que
escapou do pipeline, ou `none`); é o que os meters `inbox_*` chamam de
`exception`, e não dá para renomeá-la sem trocar o handler. Como o handler da
inbox devolve um `Outcome` em vez de lançar, `error` só é diferente de `none`
quando o próprio pipeline quebra.

Fora do escopo do MVP: `AWSTraceHeader` (X-Ray) no SQS e propagação B3
(Zipkin).

Teste obrigatório (F2 em diante): handler que grava na outbox, e o consumer
seguinte recebe a mensagem com o mesmo trace id da mensagem original.

## Revisão de 2026-11-05: implementação

- O core expõe o gancho `ConsumerObserver` (só JDK): `received`,
  `execution` (em volta de cada execução do pipeline, depois da leitura do
  envelope), `immediateRetry`, `held`, `delivered` (uma vez por entrega, depois
  do efeito) e `receiveFailed`. Exceção do observer é registrada no log e
  descartada: observar nunca muda o destino de uma mensagem.
- O starter implementa o gancho com Micrometer. Os meters usam nomes com ponto
  (`inbox.messages.received`, `inbox.processing.duration`,
  `inbox.last.receive.age` com unidade `seconds`...), que o Prometheus exporta
  como `inbox_messages_received_total`, `inbox_processing_duration_seconds` e
  `inbox_last_receive_age_seconds`. A tabela acima usa os nomes exportados; o
  timer de duração ganha o sufixo `_seconds`.
- `outcome` vem do efeito quando ele difere da decisão: `broker-delay` sem
  `deliveryCount` vira `dead_letter`; efeito que falhou ou cópia impossível,
  `released`.
- `inbox.consume` é uma observation `CONSUMER` com o `InboxMessage` como
  carrier: `traceparent` de `InboxMessage.traceparent()`, `tracestate` e
  `baggage` dos headers. Fica aberta durante os retries do nível 1; cada
  execução do `hold` abre uma nova.
- `inbox_effects_failed_total{effect}`: `receive` vem do `receiveFailed`; os outros vêm da chamada que
  falhou de fato, que o `OutcomeApplier` informa no `Report` (`ack`, `nack`, `copy`). Até 2026-11-13 o valor
  era deduzido do `Outcome` e errava, por exemplo, num `ack` que falhou depois de uma cópia bem-sucedida.

## Consequências

- Alertas típicos (DLQ crescendo, retry `delayed` alto, idade da mensagem
  subindo) ficam possíveis sem código na aplicação.
- O README traz um dashboard de exemplo com as consultas acima.
