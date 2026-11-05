# ADR-0001: Adapters sobre a messaging-lib e o spring-kafka

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-10-07 (troca os containers Spring de SQS, RabbitMQ e JMS pela messaging-lib)
- Revisão: 2026-10-07 (SPI do adapter síncrono; circuit breaker sai do core, ADR-0010)
- Revisão: 2026-10-07 (`Outcome.Release`; `hold` e passada do nível 3 no core)

## Contexto

A inbox-library precisa consumir de Kafka, SQS, SNS (via SQS), Service Bus,
Pub/Sub, RabbitMQ e ActiveMQ (Classic e Artemis) com a mesma semântica de
retry, backoff, DLQ e idempotência.

A versão de 2026-10-01 deste ADR escolheu um adapter por broker sobre o
container Spring de cada cliente (spring-kafka, spring-cloud-aws-sqs,
spring-amqp, spring-jms). Desde então a messaging-lib passou a cobrir SQS,
SNS, Service Bus, Pub/Sub, ActiveMQ e RabbitMQ atrás de uma API única de pull
com lease (`MessageReceiver`, `MessageSender`, `Capabilities`). O ADR-0001 da
messaging-lib deixa de propósito com a inbox o listener, o polling, a
concorrência, o retry e a idempotência.

Kafka continua fora da messaging-lib: é log com offset, não fila com lease
(ADR-0002 da messaging-lib).

Opções avaliadas:

| Opção | Prós | Contras |
|---|---|---|
| Spring Cloud Stream | Abstração pronta, binders plugáveis | Binders sólidos só para Kafka e Rabbit; SQS, Service Bus e JMS sem binder oficial mantido; retry/DLQ diferentes por binder (a abstração vaza) |
| Apache Camel | EIPs prontos, idempotent consumer | Grande, DSL própria, foge do modelo Spring Boot dos consumidores |
| Um container Spring por broker | Containers maduros cuidam de conexão, concorrência e ack | Cinco adapters com semânticas diferentes de ack, atraso e DLQ; Pub/Sub e Service Bus exigiriam Spring Cloud GCP/Azure |
| **messaging-lib para filas + spring-kafka para Kafka** | Um adapter cobre seis provedores; as diferenças já estão em `Capabilities`; o contrato da messaging-lib já roda nos emuladores | O loop de polling, a concorrência e o shutdown passam a ser código da inbox (ADR-0009) |

## Decisão

- `inbox-core` é Java puro (sem Spring): envelope, pipeline, middlewares,
  política de retry, classificação de erro, gancho do circuit breaker
  (`ConsumptionGate`, ADR-0010), stale check e o SPI do adapter.
- Dois adapters:

  | Módulo | Base | Provedores |
  |---|---|---|
  | `inbox-messaging` | `messaging-core` (`MessageReceiver`/`MessageSender`) | SQS, SNS → SQS, Service Bus, Pub/Sub, ActiveMQ Classic e Artemis, RabbitMQ |
  | `inbox-kafka` | spring-kafka (listener container) | Kafka |

- `inbox-messaging` depende só de `messaging-core`. Os adapters de cada
  provedor (`messaging-aws`, `messaging-azure`, ...) entram pela aplicação, e
  os destinos vêm do `MessagingDestinations` do starter da messaging-lib
  (ADR-0008 da messaging-lib).
- Cada adapter só traduz: mensagem nativa em `InboxMessage` e `Outcome` do
  pipeline em efeito no broker (ack, reentrega, cópia para DLQ ou `failed`).
- Toda política é do core, inclusive o que espera ou repete: a espera do
  `hold`, a passada do nível 3 (agendamento, condições de fim, reenfileiramento)
  e o redrive. O adapter oferece as operações básicas (buscar, `ack`,
  `nack(atraso)`, enviar cópia para um destino, pausar e retomar a busca), e o
  core decide quando usá-las. Assim o nível 3 e o redrive são escritos uma vez
  e valem para os dois adapters.
- O pipeline devolve um `Outcome`; quem executa o efeito é o adapter. O core
  não conhece broker.

  ```java
  public sealed interface Outcome {
      record Ack() implements Outcome {}       // sucesso, duplicata, stale, expirada, descartada
      record Retry(Duration delay, Reason reason) implements Outcome {}  // nível 2, in_progress, circuito aberto
      record DeadLetter(Throwable cause) implements Outcome {}  // entra no nível 3 ou volta para o fim da DLQ
      record Failed(Throwable cause, FailureReason reason) implements Outcome {}  // falha definitiva
      record Release() implements Outcome {}   // nack(0) sem processar: shutdown, fim de passada
  }
  ```

- O SPI do adapter e o pipeline são **síncronos**: o handler roda na thread do
  worker, e o pipeline devolve `Outcome`, não `CompletionStage<Outcome>`. Os
  dois backends são síncronos (o `receive` da messaging-lib e a thread do
  listener do spring-kafka), e a estratégia `hold` segura a thread de
  propósito. Paralelismo vem de `concurrency` (ADR-0009). Virtual threads, a
  partir do Java 24 (JEP 491 resolve o pinning do JMS), dão o ganho de um
  modelo assíncrono sem mudar o código.
- As decisões que dependem do provedor são tomadas uma vez, no startup, a
  partir de `Capabilities` (estratégia do nível 2, ordem, limites de atraso).
- Um contrato único (`BrokerAdapterContract` no `inbox-testkit`) roda contra
  os dois adapters. O `inbox-messaging` roda o contrato com cada provedor da
  messaging-lib, nos mesmos emuladores que a messaging-lib já usa.

## Alternativas descartadas

- SPI assíncrono (`CompletionStage`), como na acme-messaging, incorporada a
  este projeto: só compensa com adapters assíncronos nativos. Sobre a
  messaging-lib e o spring-kafka, seria uma ponte de thread para
  `CompletionStage` sem ganho de vazão e com stack traces piores. Volta a ser
  avaliado se um provedor, medido, não aguentar o volume com platform threads.
- Modelo push (`MessageSink` e `CreditGate` da acme-messaging): a messaging-lib
  só oferece pull.

## Consequências

- Um provedor novo na messaging-lib (OCI Queue, por exemplo) funciona na inbox
  sem módulo novo, desde que passe no contrato da messaging-lib.
- Pelas `Capabilities`, a inbox vê o que cada provedor faz de diferente. Não
  existe `if (provider == ...)` no `inbox-messaging`.
- O polling deixa de ser de um container maduro. Concorrência, lease e shutdown
  ficam no ADR-0009 e têm teste próprio no contrato.
- O Kafka continua com spring-kafka: rebalance, commit e pausa de partição não
  são reimplementados.
- Inspiração conceitual: pipeline de middlewares do MassTransit/Watermill.
