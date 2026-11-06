# F3: contrato da inbox nos seis provedores

- Data: 2026-11-06
- Fase: F3 do `messaging-inbox/docs/plan.md`
- Abordagem escolhida: contrato abstrato no test-jar da `messaging-inbox`, subclasse por broker no módulo de cada adapter

## Objetivo

Provar a semântica da inbox (`InboxConsumer` direto, sem Spring) sobre cada broker real ou emulado: nível 1, as
três estratégias do nível 2 onde o broker suporta, cópias para `failed` e DLQ comum, `ack` depois da cópia, lease
mantido e `stop`. Medir o limite de entregas da DLQ nativa de cada broker e documentar.

Hoje a inbox só está provada sobre o `InMemoryMessaging` (`InboxConsumerTest`) e, pelo starter, sobre o SQS
(`SqsLocalStackTest`).

## Fora do escopo

- Caminho de configuração do Spring por provedor (o mapeamento de endereço já tem `EndpointsTest`).
- Idempotência e `key-lanes`: independem do broker e estão cobertos no InMemory.
- Nível 3 e redrive (F5); circuit breaker, batch, stale check, expiração (F6).
- Validar no startup o limite nativo contra o orçamento da inbox (exigiria APIs de administração do broker).
- Ordenação com sessions/FIFO: já tem contrato na messaging-lib.

## Contrato

`com.example.messaging.inbox.contract.InboxContract`, abstrato, em `messaging-inbox/src/test/java`, publicado no
test-jar da `messaging-inbox` (`maven-jar-plugin`, goal `test-jar`).

### O que a subclasse fornece

```java
protected abstract MessageSender newSender();            // envia ao destino principal
protected abstract MessageReceiver newReceiver();        // recebe do destino principal; instância nova por chamada
protected abstract MessageSender failedSender();
protected abstract MessageReceiver failedReceiver();     // para conferir as cópias
protected abstract MessageSender deadLetterSender();
protected abstract MessageReceiver deadLetterReceiver();
protected abstract Duration lease();                     // lease do destino principal
protected List<RetryQueue> retryQueues();                // null (padrão): pula retry-queues
protected MessageReceiver retryQueueReceiver();          // recebe da banda de retryQueues()
protected Integer nativeDeliveryLimit();                 // null: sem limite padrão
protected boolean delayCopies();                         // atraso do nível 2 é por cópia (RESCHEDULE, SCHEDULED)
protected Duration delayedInitial();                     // atraso mínimo aceito pelo broker; padrão 1 s
protected Duration settle();                             // teto das esperas; padrão 15 s
```

O contrato monta o `InboxConsumer` como o `InboxConsumerTest` (`ConsumerSettings`, `Pipeline`, `Destinations`,
`StopSignal`) e o fecha no fim de cada teste. Configuração de retry do contrato: nível 1 com 3 tentativas a 10 ms,
nível 2 com 2 tentativas a `delayedInitial()`. Cada teste marca as mensagens com um atributo de execução; mensagens de
outros testes encontradas no destino recebem `ack` e são descartadas. Os destinos são drenados antes de cada teste.
Esperas são ativas, com teto `settle()`; nenhum caso depende de tempo exato.

### Casos

Cada um é pulado (`Assumptions`) quando a capability ou o destino não existe.

| Caso | Condição | Prova |
|---|---|---|
| `successIsAcked` | sempre | handler conclui; principal vazio; nenhuma cópia em `failed`/DLQ |
| `immediateRetryThenSuccess` | sempre | falha nas 2 primeiras execuções, conclui na 3ª; uma entrega só no broker |
| `nonRetryableGoesToFailed` | sempre | uma execução; cópia em `failed` com `inbox_failure` e `dead_letter`; original com `ack` |
| `rejectCarriesTheReason` | sempre | `HandlerResult.reject("x")` chega a `failed` com o motivo |
| `brokerDelayThenDeadLetter` | `delayedRedelivery` | nível 2 devolve ao broker com atraso; contagem por `inbox_attempts`/`deliveryCount`; esgota na DLQ comum |
| `holdRetriesWithoutReturning` | sempre | nível 2 em memória com `keepAlive`; a mensagem não reaparece no broker durante a espera; esgota na DLQ |
| `retryQueuesCopyAndAck` | `retryQueues() != null` | cópia na banda com `inbox_attempts`; original com `ack` |
| `slowHandlerKeepsTheLease` | sempre | handler de ~1,5 × `lease()`; nenhuma reentrega |
| `stopReleasesUnstarted` | sempre | `stop` devolve (`nack`) as mensagens do poll que não começaram |
| `nativeDeliveryLimitIsWhatTheBrokerDoes` | `nativeDeliveryLimit() != null` | `nack(0)` em sequência (teto 30); a mensagem some depois de exatamente `nativeDeliveryLimit()` entregas; e, sem `delayCopies()`, o limite é maior que 6 (orçamento padrão do `broker-delay`: 1 + 5) |

## Matriz

| Subclasse (módulo) | Principal | `failed`, DLQ, banda | Nível 2 | Lease |
|---|---|---|---|---|
| `SqsInboxContractTest` (aws) | fila standard nova (`LocalStack.queue`) | filas novas | broker-delay, hold, retry-queues | 2 s |
| `ServiceBusInboxContractTest` (azure) | fila nova `inbox`, receiver `RESCHEDULE` | filas novas `inbox-failed`, `inbox-dlq`, `inbox-retry` | broker-delay, hold, retry-queues | 5 s |
| `ServiceBusSubscriptionInboxContractTest` (azure) | `contract-topic/contract-sub` | as mesmas filas novas | hold, retry-queues | 5 s |
| `PubSubInboxContractTest` (gcp) | tópico/subscription novos (`topicWithSubscription`) | tópicos/subscriptions novos | broker-delay, hold, retry-queues | 10 s |
| `ActiveMqClassicInboxContractTest` (jms) | fila nova, `Redelivery.SCHEDULED` (container com `schedulerSupport`) | filas novas | broker-delay, hold, retry-queues | 5 s |
| `ActiveMqArtemisInboxContractTest` (jms) | fila nova (`ActiveMqArtemis.queue`), `SCHEDULED` | filas novas | broker-delay, hold, retry-queues | 5 s |
| `RabbitMqInboxContractTest` (rabbitmq) | fila quorum nova (`RabbitMq.queue(name, "quorum")`) | filas novas | hold, retry-queues | 5 s |

- Filas próprias da inbox em todas as subclasses, separadas das do `MessagingContract`.
- Só o Service Bus muda entidades fixas: quatro filas novas no `service-bus-config.json` (o emulator não cria em
  runtime). Os demais criam no `@BeforeAll`, pelos helpers existentes.
- A banda de retry tem atraso igual ao lease; o teste lê a cópia da banda, não a consome como segundo consumer.

## DLQ nativa

O orçamento de entregas que o broker vê até a inbox decidir, com os padrões da lib (nível 1: 3; nível 2: 5):
`broker-delay` 6 (mais lease vencido ou `stop`), `hold` 1, `retry-queues` 1 no original. Com o limite nativo abaixo
disso, o broker tira a mensagem antes da inbox mandar a cópia com `inbox_failure`.

Valores esperados (o contrato confirma e corrige):

| Broker | Limite padrão | Depois do limite |
|---|---|---|
| SQS | nenhum (redrive policy por fila) | DLQ da redrive policy |
| Service Bus | `MaxDeliveryCount` 10 (o emulator usa 10) | DLQ nativa da entidade |
| Pub/Sub | nenhum (dead-letter policy; mínimo 5) | tópico de dead letter |
| ActiveMQ Classic | `maximumRedeliveries` 6 (política do cliente) | `ActiveMQ.DLQ` |
| Artemis | `max-delivery-attempts` 10 | endereço `DLQ` |
| RabbitMQ 4 (quorum) | `delivery-limit` 20 | descartada, ou DLX |

No Classic e no Service Bus o `broker-delay` é por cópia (`SCHEDULED`, `RESCHEDULE`), e a contagem recomeça; o
contrato não exige folga sobre 6 nesses casos (`delayCopies()`).

Os valores medidos vão para uma tabela no README da lib (item da F7) e para a ADR-0002 da inbox.

## Build

- No build padrão, como os contratos de hoje: `@Testcontainers(disabledWithoutDocker = true)` e o container
  compartilhado por JVM de cada helper.
- `messaging-inbox/pom.xml`: `maven-jar-plugin` com goal `test-jar`.
- `messaging-aws`, `-azure`, `-gcp`, `-jms`, `-rabbitmq`: dependências de teste `messaging-inbox` e
  `messaging-inbox` (`type` `test-jar`), versão `${project.version}`. O reactor passa a compilar a inbox antes dos
  adapters. Nenhum código de produção muda.
- Estimativa: 40 a 90 s por subclasse, ~7 min a mais no build completo (hoje ~8 min). Se pesar, mover para um
  profile depois; não agora.

## Pronto quando

1. As 7 subclasses verdes; casos pulados só por capability ausente.
2. Tabela de DLQ nativa medida no README e na ADR-0002.
3. `mvn clean install` da raiz verde.
4. F3 marcada como feita em `messaging-inbox/docs/plan.md`.

## Riscos

- Emulator do Service Bus com falha intermitente no `complete` da DLQ nativa (registrado no plano da lib). O
  contrato não lê a DLQ nativa do Service Bus; as cópias vão para filas comuns.
- Pub/Sub com ack deadline mínimo de 10 s deixa a subclasse mais lenta.
- Política de redelivery do ActiveMQ Classic é do cliente: a medição vale para a `ConnectionFactory` dos testes; uma
  aplicação com outra política terá outro limite (documentar).
