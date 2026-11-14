# Métricas da messaging-library e da messaging-inbox

Referência de **todas** as métricas que o `messaging-spring-boot-starter` registra: 22 da inbox, 8 do transporte e as
4 que o Micrometer gera sozinho das observations. As decisões e o porquê estão no
[ADR-0007 da inbox](../messaging-inbox/docs/adr/0007-observabilidade.md) e no
[ADR-0008 da lib](adr/0008-spring-boot-e-observabilidade.md); este arquivo descreve **o que existe hoje**. Os nomes,
tipos e tags são travados por testes (veja [Como mudar](#como-mudar)).

Os nomes, tipos e séries exportadas foram conferidos em um scrape real de um `PrometheusMeterRegistry`, **exceto**
`inbox_last_receive_age_seconds`, que precisa do consumer inteiro e só o `ObservabilityTest` confere. As consultas
PromQL da seção de exemplos **não** foram executadas contra um Prometheus.

## Índice

| Meter (Micrometer) | Prometheus | Tipo | Tags | Seção |
|---|---|---|---|---|
| `inbox.messages.received` | `inbox_messages_received_total` | counter | `consumer`, `destination` | [Entrega](#entrega) |
| `inbox.messages.processed` | `inbox_messages_processed_total` | counter | `consumer`, `destination`, `outcome` | [Entrega](#entrega) |
| `inbox.processing.duration` | `inbox_processing_duration_seconds` | timer | `consumer`, `destination`, `outcome` | [Entrega](#entrega) |
| `inbox.messages.in.flight` | `inbox_messages_in_flight` | gauge | `consumer`, `destination` | [Entrega](#entrega) |
| `inbox.delivery.attempts` | `inbox_delivery_attempts` | summary | `consumer`, `destination`, `level` | [Entrega](#entrega) |
| `inbox.message.age` | `inbox_message_age_seconds` | timer | `consumer`, `destination` | [Entrega](#entrega) |
| `inbox.messages.retried` | `inbox_messages_retried_total` | counter | `consumer`, `destination`, `level`, `strategy`, `exception` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.messages.dead.lettered` | `inbox_messages_dead_lettered_total` | counter | `consumer`, `destination`, `exception` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.messages.failed` | `inbox_messages_failed_total` | counter | `consumer`, `destination`, `reason`, `exception` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.retries.skipped` | `inbox_retries_skipped_total` | counter | `consumer`, `destination`, `reason` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.attributes.budget.exceeded` | `inbox_attributes_budget_exceeded_total` | counter | `consumer`, `destination` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.dead.letter.passes` | `inbox_dead_letter_passes_total` | counter | `consumer`, `end` | [Retry, DLQ e failed](#retry-dlq-e-failed) |
| `inbox.handler.duration` | `inbox_handler_duration_seconds` | timer | `consumer`, `destination`, `outcome` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.idempotency.duration` | `inbox_idempotency_duration_seconds` | timer | `consumer`, `destination`, `operation`, `outcome` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.hold.wait.duration` | `inbox_hold_wait_duration_seconds` | timer | `consumer`, `destination` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.effects.failed` | `inbox_effects_failed_total` | counter | `consumer`, `destination`, `effect` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.failures.unexpected` | `inbox_failures_unexpected_total` | counter | `consumer`, `destination`, `stage`, `exception` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.keepalives.failed` | `inbox_keepalives_failed_total` | counter | `consumer`, `destination` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.last.receive.age` | `inbox_last_receive_age_seconds` | gauge | `consumer` | [Diagnóstico](#diagnóstico-do-consumer) |
| `inbox.messages.redriven` | `inbox_messages_redriven_total` | counter | `consumer`, `source`, `target`, `result` | [Redrive](#redrive) |
| `inbox.redrives.ended` | `inbox_redrives_ended_total` | counter | `consumer`, `source`, `target`, `end` | [Redrive](#redrive) |
| `inbox.redrive.duration` | `inbox_redrive_duration_seconds` | timer | `consumer`, `source`, `target` | [Redrive](#redrive) |
| `messaging.send.duration` | `messaging_send_duration_seconds` | timer | `provider`, `destination`, `batch`, `outcome`, `exception` | [Transporte](#transporte) |
| `messaging.receive.duration` | `messaging_receive_duration_seconds` | timer | `provider`, `destination`, `outcome`, `exception` | [Transporte](#transporte) |
| `messaging.ack.duration` | `messaging_ack_duration_seconds` | timer | `provider`, `destination`, `batch`, `outcome`, `exception` | [Transporte](#transporte) |
| `messaging.nack.duration` | `messaging_nack_duration_seconds` | timer | `provider`, `destination`, `outcome`, `exception` | [Transporte](#transporte) |
| `messaging.dead.letter.duration` | `messaging_dead_letter_duration_seconds` | timer | `provider`, `destination`, `outcome`, `exception` | [Transporte](#transporte) |
| `messaging.receive.messages` | `messaging_receive_messages` | summary | `provider`, `destination` | [Transporte](#transporte) |
| `messaging.messages.failed` | `messaging_messages_failed_total` | counter | `provider`, `destination`, `operation`, `exception` | [Transporte](#transporte) |
| `messaging.message.size` | `messaging_message_size_bytes` | summary | `provider`, `destination` | [Transporte](#transporte) |
| `inbox.consume` | `inbox_consume_seconds` | timer (observation) | `consumer`, `destination`, `level`, `outcome`, `error` | [Observations](#observations) |
| `inbox.consume.active` | `inbox_consume_active_seconds` | long task timer | `consumer`, `destination`, `level`, `outcome` | [Observations](#observations) |
| `messaging.publish` | `messaging_publish_seconds` | timer (observation) | `provider`, `destination`, `error` | [Observations](#observations) |
| `messaging.publish.active` | `messaging_publish_active_seconds` | long task timer | `provider`, `destination` | [Observations](#observations) |

## Como ler

**Nomes.** O código registra com pontos (`inbox.messages.received`); o Prometheus troca `.` por `_` e acrescenta o
sufixo do tipo:

| Tipo | Séries exportadas |
|---|---|
| counter | `<nome>_total` |
| timer | `<nome>_seconds_count`, `_seconds_sum` e o gauge `_seconds_max` (tipo `summary`, **sem buckets**; para p95 é preciso ligar *percentile histogram* no registry) |
| summary | `<nome>_count`, `_sum` e o gauge `_max`; com unidade, `<nome>_<unidade>_count` (`messaging_message_size_bytes_count`) |
| gauge | `<nome>` (com unidade, `<nome>_<unidade>`) |
| long task timer | `<nome>_seconds_count` (execuções em andamento), `_sum`, `_max` |

**Convenção** (ADR-0007): prefixo `inbox.` para o consumo e `messaging.` para chamadas ao broker; counter =
`<entidade no plural>.<evento no particípio>`; timer termina em `.duration` e idade em `.age`; tags e valores em
`snake_case` minúsculo; nada vindo do payload; todo meter tem `description`.

**O que liga cada coisa.**
- Métricas da inbox: precisam de um `MeterRegistry` no contexto. Sem ele, nada é registrado. Os spans precisam de um
  `ObservationRegistry`. Os dois são opcionais e independentes.
- Transporte: o sender é instrumentado com `MeterRegistry` **ou** `ObservationRegistry`; o receiver, só com
  `MeterRegistry`.

**Quando a série aparece.** Counters e timers só existem depois do primeiro evento. Existem desde o início:
`inbox_messages_in_flight` e `inbox_last_receive_age_seconds` (criados com o consumer) e `messaging_receive_messages`
(criado com o receiver). `inbox_messages_redriven_total` é registrado mesmo com 0 a cada execução. Use `or vector(0)`
ou `absent()` onde a ausência importa.

**Tags comuns.**

| Tag | Valor |
|---|---|
| `consumer` | Nome do receiver em `messaging.receivers.<nome>`. A passada do nível 3 usa o mesmo valor do receiver que ela serve |
| `destination` | Nome **lógico** do receiver, nunca o endereço (URL de fila, ARN). Nas métricas da inbox é o nome do receiver; nas passadas do nível 3, `<receiver>.dead-letter`. É o mesmo valor que o transporte usa, e é a chave para cruzar `inbox_*` com `messaging_*` |
| `provider` | Tipo do provider: `aws`, `azure`, `gcp`, `activemq-classic`, `artemis`, `rabbitmq` |
| `exception` | Nome simples da classe da exceção (`SocketTimeoutException`), ou `none`. Na inbox é a **causa raiz**; no transporte, a exceção que o adapter lançou (um `MessagingException` específico) |

Cardinalidade: todas as tags têm conjunto limitado pela configuração ou pelo código. É proibido usar id de mensagem,
`aggregateId`, `traceId` ou qualquer valor do payload.

---

## Inbox

### Entrega

**`inbox.messages.received`**: mensagens entregues pelo broker, **inclusive reentregas** e as lidas pela passada do
nível 3 (nesse caso com `destination=<receiver>.dead-letter`). Incrementa antes de qualquer processamento.

**`inbox.messages.processed`**: uma vez por entrega, com o resultado final (`outcome`, valores
[abaixo](#valores-das-tags)). Com `hold`, uma entrega física tem várias execuções e conta uma vez, no fim.

**`inbox.processing.duration`**: da primeira execução ao efeito no broker (`ack`, `nack` ou cópia), incluindo os retries
do nível 1 e as esperas do `hold`. Tem as mesmas tags do `processed`, então `_count` do timer == `processed_total`.

**`inbox.messages.in.flight`** (gauge): mensagens entregues e ainda sem efeito no broker. Sobe no `received`, desce
no `delivered`. Vale por consumer **e destination**, então a passada e o loop principal têm séries separadas. É a
forma correta de ver "em andamento": `received - processed` não sobrevive a um restart dos counters. O consumer é
recriado a cada start, e o gauge do anterior é substituído.

**`inbox.delivery.attempts`** (summary): tentativas **já gastas** no nível em que a entrega estava, 0 na primeira.
`level` é o nível da entrega (`immediate`, `delayed`, `dead_letter`). Não é registrado quando o envelope não pôde ser
lido (`deserialization`).

**`inbox.message.age`**: do horário do evento (`ce_time` ou `occurredAt` da outbox) ao início do consumo. Só registra
quando a mensagem traz o horário e a idade não é negativa (relógios dessincronizados são descartados).

### Retry, DLQ e failed

**`inbox.messages.retried`**: uma contagem por retry agendado.

| `level` | `strategy` | Quando |
|---|---|---|
| `immediate` | `none` | Cada nova execução em memória do nível 1 |
| `delayed` | `hold` | Cada espera em memória do `hold` |
| `delayed` | `broker_delay` | `nack` com atraso aplicado (`REDELIVERY_DELAYED`) |
| `delayed` | `retry_queues` | Cópia para a fila de retry (`RETRY_QUEUED`) |
| `dead_letter` | `none` | Falha numa passada do nível 3: a mensagem volta ao fim da DLQ |

**`inbox.messages.dead.lettered`**: cópias enviadas à DLQ, ou seja, **entradas** no nível 3. Só conta no loop
principal; o retorno ao fim da DLQ numa passada conta em `retried{level="dead_letter"}`.

**`inbox.messages.failed`**: cópias enviadas à fila `failed`. `reason` ([valores](#valores-das-tags)). Quando não há DLQ
configurada, o esgotamento dos retries vai direto para `failed` com `reason=retries_exhausted`.

**`inbox.retries.skipped`**: o nível 2 foi pulado e a mensagem foi para a DLQ ou `failed` sem esgotá-lo.
`reason=delivery_count_unavailable` (o receiver não informa contagem de entregas) ou `redelivery_limit` (o broker
recusou o atraso, como o teto de 12 h do SQS).

**`inbox.attributes.budget.exceeded`**: uma cópia saiu sem os atributos `inbox_*` por falta de espaço (limite de 16
atributos por mensagem, ADR-0002). Sem eles a cópia perde a contagem de tentativas; por isso uma cópia de retry-queue
nessa situação vai para a DLQ.

**`inbox.dead.letter.passes`**: passadas do nível 3 terminadas, por `end` ([valores](#valores-das-tags)). Só existe
para o observer da passada e não tem `destination`.

### Diagnóstico do consumer

**`inbox.handler.duration`**: **uma chamada** do handler; cada retry imediato é uma chamada. `outcome` é `error` se
a chamada lançou exceção e `success` se retornou, inclusive com `Reject` ou `Discard`. Separa lentidão da aplicação
da lentidão do broker (compare com `inbox.processing.duration`).

**`inbox.idempotency.duration`**: uma chamada ao `IdempotencyStore`. `operation` = `reserve`, `complete` ou
`release`; `outcome` = `success` ou `error`. Só existe nos receivers com idempotência ligada.

**`inbox.hold.wait.duration`**: a espera **planejada** de cada `hold` em memória (o `delay` do retry). Difere da
medida só se a espera for interrompida pelo shutdown.

**`inbox.effects.failed`**: chamada ao broker que falhou. `effect`:

| `effect` | Chamada |
|---|---|
| `receive` | O `receive` falhou; o worker espera e tenta de novo |
| `ack` | O `ack` falhou |
| `nack` | O `nack` falhou (atraso do `broker_delay`, ou o `nack(0)` de uma liberação) |
| `copy` | O envio da cópia (DLQ, `failed`, retry queue) falhou, ou a cópia é impossível de montar |
| `unknown` | Não deveria ocorrer: efeito falho sem chamada identificada |

Um `ack` que falha depois de uma cópia bem-sucedida conta como `ack`, não `copy`.

**`inbox.failures.unexpected`**: falha que ninguém esperava (um `Error`, um bug), onde o worker sobreviveu. `stage` =
`lane` (a lane de `key-lanes` falhou), `delivery` (a entrega foi liberada) ou `dead_letter_pass` (a mensagem foi para
`failed`). Antes só havia log ERROR. O valor deveria ser sempre 0.

**`inbox.keepalives.failed`**: o `keepAlive` de uma mensagem não pôde começar, e ela foi processada **sem renovação de
lease**. Não cobre o lease que se perde depois de renovado.

**`inbox.last.receive.age`** (gauge, segundos): tempo desde o último `receive` concluído, com mensagem ou vazio. Sobe
quando o consumer **para de ler**, mesmo com a fila vazia (incidente T-88/B20 do engine-tax). É `NaN` até o primeiro
`receive` terminar. Só tem `consumer`; não existe para a passada.

### Redrive

O `consumer` vem do pedido. Pedidos recusados (receiver desconhecido, redrive já em execução, configuração errada)
**não** são contados: o valor seria entrada do chamador.

**`inbox.messages.redriven`**: mensagens lidas por uma execução, por `result`: `moved`, `skipped` (lida e devolvida ao
fim da origem: filtrada, ou *dry run*) ou `error` (a cópia falhou; a mensagem fica na origem). Registrado mesmo com 0.
`source` = `dead_letter` ou `failed`; `target` = `main` ou `dead_letter`.

**`inbox.redrives.ended`**: execuções terminadas, por `end` ([valores](#valores-das-tags)). `receive_failed` é a
execução cortada por falha de leitura, que de outro modo só pareceria pequena.

**`inbox.redrive.duration`**: do pedido ao relatório.

---

## Transporte

Vale para todo sender e receiver criado pelo `MessagingDestinations`, inclusive os que a inbox usa. `destination` é o
nome do destino em `messaging.senders` ou `messaging.receivers`. Os destinos que a inbox cria para si aparecem com
estes nomes: `<receiver>` (leitura), `<receiver>.dead-letter`, `<receiver>.failed`, `<receiver>.retry.<n>` e
`<receiver>.redrive`.

| Meter | Cobre |
|---|---|
| `messaging.send.duration` | `send` e `sendAll` do sender |
| `messaging.receive.duration` | `receive` |
| `messaging.ack.duration` | `ack` e `ackAll` |
| `messaging.nack.duration` | `nack` |
| `messaging.dead.letter.duration` | `deadLetter` |

Tags dos timers:
- `outcome`: `success`; `error` (a chamada lançou, ou, em lote, todos os itens falharam); `partial` (lote em que só
  parte dos itens falhou).
- `exception`: classe lançada, ou `none`.
- `batch` (só em `send` e `ack`): `true` para `sendAll` e `ackAll`, `false` para o unitário.

**`messaging.receive.messages`** (summary): mensagens devolvidas por cada `receive`. Um `receive` vazio registra 0, então
a média cai quando a fila esvazia.

**`messaging.messages.failed`**: **uma contagem por mensagem** cuja chamada falhou. `operation` = `send`, `ack`, `nack`
ou `dead_letter`. Num lote, cada item que falhou conta 1; se a chamada inteira lança, contam todas as mensagens dela.
Complementa os timers, que contam chamadas.

**`messaging.message.size`** (summary, bytes): tamanho do corpo de cada mensagem enviada, medido **antes** do envio,
então inclui as que falharam. Serve para alertar antes do `MessageTooLargeException`.

---

## Observations

Com um `ObservationRegistry` e o `DefaultMeterObservationHandler` (padrão do Boot com Actuator), as observations
também viram meters. Eles têm o nome da observation e ficam fora da regra do sufixo `.duration`.

| Meter | Granularidade | Observação |
|---|---|---|
| `inbox.consume` | Uma por **execução** do pipeline | Não é por entrega: um `hold` gera várias, e os retries do nível 1 ficam dentro de uma. Para tempo por entrega, use `inbox.processing.duration` |
| `inbox.consume.active` | Execuções em andamento | O `outcome` só é conhecido no fim, então aqui vale sempre `released` |
| `messaging.publish` | Um por `send` ou `sendAll` | Um `sendAll` é uma observation só |
| `messaging.publish.active` | Envios em andamento | |

`error` é a tag que o **Micrometer** põe sozinho (nome simples da exceção que escapou, ou `none`). É o que os meters
explícitos chamam de `exception`, e não dá para renomeá-la sem trocar o handler. Como o pipeline devolve um `Outcome`
em vez de lançar, `error` em `inbox.consume` só é diferente de `none` quando o próprio pipeline quebra.

O span carrega os atributos do OpenTelemetry (`messaging.system`, `messaging.destination.name`,
`messaging.consumer.group.name`, `messaging.message.id`, `inbox.attempt`) como chaves de alta cardinalidade, que
nenhum meter usa, e as tags curtas (`consumer`, `destination`, `level`, `outcome`) como baixa cardinalidade.
`messaging.system` é o tipo do provider, o mesmo da tag `provider`.

---

## Valores das tags

### `outcome` da inbox

Resultado final de uma entrega. Quando mais de um se aplica, vale a primeira linha da lista de precedência:
`lease_expired` > `dead_letter` / `retry_dead_letter` > `failed` > `released` (efeito falho, cópia impossível ou
liberação) > o que o handler decidiu.

| Valor | Significado |
|---|---|
| `success` | Handler concluiu e a mensagem foi confirmada |
| `duplicate` | A idempotência viu a mensagem já processada; confirmada sem chamar o handler |
| `discarded` | O handler devolveu `Discard`; confirmada |
| `retry_delayed` | Reentrega com atraso pelo nível 2 (nível 1 esgotado, `SkipImmediate` ou `RetryAfter`) |
| `in_progress` | Outra instância processa a mesma mensagem; reentrega com atraso, sem gastar tentativa |
| `retry_dead_letter` | Falhou numa passada do nível 3; voltou ao fim da DLQ |
| `dead_letter` | Enviada à DLQ |
| `failed` | Enviada à `failed` |
| `lease_expired` | O trabalho e a cópia (se havia) foram feitos, mas o lease venceu antes do `ack`: a mensagem volta como duplicata. **Não é `success`** |
| `released` | Devolvida com `nack(0)`: shutdown, fim de passada, efeito que falhou, ou cópia impossível. Onde o broker conta entregas, a volta conta como tentativa |

Planejados, ainda não emitidos: `stale` (ADR-0013), `expired` (ADR-0015), `circuit_open` (ADR-0010).

### Outras tags da inbox

| Tag | Valores |
|---|---|
| `level` (`retried`) | `immediate` (nível 1), `delayed` (nível 2), `dead_letter` (nível 3) |
| `level` (`delivery.attempts`, `consume`) | `immediate`, `delayed`, `dead_letter`: o nível da entrega |
| `strategy` | `none`, `hold`, `broker_delay`, `retry_queues` (a estratégia do nível 2 que se aplicou) |
| `reason` (`failed`) | `non_retryable`, `retries_exhausted`, `deserialization`, `idempotency_conflict`, `rejected` (planejado: `expired`) |
| `reason` (`retries.skipped`) | `delivery_count_unavailable`, `redelivery_limit` |
| `effect` | `receive`, `ack`, `nack`, `copy`, `unknown` |
| `stage` | `lane`, `delivery`, `dead_letter_pass` |
| `operation` (idempotência) | `reserve`, `complete`, `release` |
| `outcome` (`handler`, `idempotency`) | `success`, `error` |
| `end` (passes e redrive) | `empty`, `newer_than_pass`, `repeated_key`, `max_per_pass`, `stopping`, `receive_failed` |
| `result` (`redriven`) | `moved`, `skipped`, `error` |
| `source` / `target` | `dead_letter`, `failed` / `main`, `dead_letter` |

### Tags do transporte

| Tag | Valores |
|---|---|
| `outcome` | `success`, `error`, `partial` |
| `batch` | `true`, `false` |
| `operation` (`messages.failed`) | `send`, `ack`, `nack`, `dead_letter` |

---

## Relações que valem sempre

- Cada entrega gera **um** `received` e **um** `processed`; `inbox_processing_duration_seconds_count` tem a mesma
  contagem do `processed_total`, com as mesmas tags.
- `inbox_messages_in_flight` **nunca fica negativo** em operação normal. Um valor negativo indica um `delivered` sem o
  `received` correspondente: é bug, não carga.
- Os retries do nível 1 contam só em `retried{level="immediate"}`, nunca em `processed`.
- `dead.lettered` só cresce no loop principal. A volta ao fim da DLQ de uma passada soma em
  `retried{level="dead_letter"}` e em `processed{outcome="retry_dead_letter"}`.
- `inbox_delivery_attempts_count` ≤ `processed_total`: não é registrado quando o envelope não pôde ser lido.
- `inbox_messages_redriven_total{result="moved"}` e `retried`/`processed` são independentes: o redrive não passa pelo
  loop de consumo.

## Consultas

```promql
# Entregas por resultado
sum by (consumer, outcome) (rate(inbox_messages_processed_total[5m]))

# Taxa de sucesso
sum(rate(inbox_messages_processed_total{outcome="success"}[5m]))
  / sum(rate(inbox_messages_processed_total[5m]))

# Mensagens em andamento
sum by (consumer) (inbox_messages_in_flight)

# Retries por nível e estratégia
sum by (consumer, level, strategy) (rate(inbox_messages_retried_total[5m]))

# Mensagens que foram para a failed sem retry
sum by (consumer) (rate(inbox_messages_failed_total{reason="non_retryable"}[5m]))

# Tempo médio do handler (p95 pede percentile histogram)
sum by (consumer) (rate(inbox_handler_duration_seconds_sum[5m]))
  / sum by (consumer) (rate(inbox_handler_duration_seconds_count[5m]))

# Quanto do tempo da entrega é o handler (o resto é idempotência e broker)
sum by (consumer) (rate(inbox_handler_duration_seconds_sum[5m]))
  / sum by (consumer) (rate(inbox_processing_duration_seconds_sum[5m]))

# Atraso entre o evento e o consumo
sum by (consumer) (rate(inbox_message_age_seconds_sum[5m]))
  / sum by (consumer) (rate(inbox_message_age_seconds_count[5m]))

# Duplicatas a caminho (lease vencido antes do ack)
sum by (consumer) (rate(inbox_messages_processed_total{outcome="lease_expired"}[5m]))

# Falhas que ninguém esperava, por onde foram pegas
sum by (consumer, stage) (increase(inbox_failures_unexpected_total[1h]))

# Chamadas ao broker que falharam, por tipo
sum by (consumer, effect) (rate(inbox_effects_failed_total[5m]))

# Mensagens por falha no transporte, por operação e exceção
sum by (destination, operation, exception) (rate(messaging_messages_failed_total[5m]))

# Cruzar a inbox com o transporte pelo mesmo receiver
sum by (destination) (rate(inbox_messages_received_total[5m]))
  / sum by (destination) (rate(messaging_receive_messages_sum[5m]))

# Redrives que pararam por falha de receive
increase(inbox_redrives_ended_total{end="receive_failed"}[1h])
```

### Alertas sugeridos

| Condição | Sinal de |
|---|---|
| `inbox_last_receive_age_seconds` acima de alguns `poll-wait` | Worker parado: não lê mais, mesmo com a fila vazia |
| `inbox_failures_unexpected_total` acima de 0 | Bug ou `Error` pego pelo worker |
| `inbox_messages_failed_total` crescendo | Mensagens indo para a `failed` |
| `inbox_effects_failed_total{effect="receive"}` crescendo | Broker inacessível ou credencial inválida |
| `inbox_messages_processed_total{outcome="lease_expired"}` subindo | `max-processing` ou lease curtos para o handler |
| `inbox_messages_in_flight` parado em valor alto | Consumer travado dentro do handler |
| `inbox_keepalives_failed_total` crescendo | Mensagens sem renovação de lease |
| `messaging_messages_failed_total` crescendo | Broker recusando envio, ack ou nack |
| `inbox_message_age_seconds` subindo | Consumo atrasado em relação ao evento |

## Limites e armadilhas

- **Timers e summaries sem buckets.** Só `_count`, `_sum` e `_max`: dá média e máximo, não percentil.
- **`inbox_consume_*` não é por entrega** e o `outcome` do `.active` é sempre `released`.
- **`error` ≠ `exception`.** As observations usam `error`, os meters explícitos usam `exception`.
- **`inbox.hold.wait.duration` é a espera planejada**, não a medida.
- **`messaging.message.size` conta o que falhou**, porque mede antes do envio.
- **`inbox.keepalives.failed` não vê** o lease que se perde depois de renovado.
- **`inbox.message.age`** descarta idade negativa; sem horário na mensagem, não registra.
- **Falha do observer não afeta a mensagem**: uma exceção ao registrar uma métrica é logada e descartada, e a mensagem
  segue seu caminho. Nesse caso a métrica daquela entrega pode faltar.
- **`inbox_messages_in_flight` por OOM**: um `OutOfMemoryError` no worker pode deixar o gauge subir sem voltar.

## Planejadas

Descritas nos ADRs, ainda **não** registradas porque as features não existem:

| Meter | Depende de |
|---|---|
| `inbox.circuit.state`, `inbox.circuit.transitions` | circuit breaker (F6, ADR-0010) |
| `inbox.batch.size` | handler em batch (ADR-0011) |
| `inbox.stale.races`, outcome `stale` | stale check (ADR-0013) |
| outcome `expired` e reason `expired` | expiração por idade (ADR-0015) |
| profundidade da fila e da DLQ | capability nova no core e implementação por adapter |

## Como mudar

1. Os nomes, tipos e chaves de tag dos meters explícitos ficam em `MeterNamesTest` (duas cópias: `inbox.spring` e
   `spring`); os das observations, em `ObservationMetersTest` e no teste do `messaging.publish`. Mudar um nome quebra o
   teste de propósito.
2. Mudou um nome, tag ou valor? Atualize o teste, o [ADR-0007](../messaging-inbox/docs/adr/0007-observabilidade.md), o
   README e este arquivo juntos.
3. Meter novo: siga a convenção acima, dê `description` (o teste exige) e, se for por entrega, as tags `consumer` e
   `destination`.
