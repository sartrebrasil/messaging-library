# F3: contrato da inbox nos seis provedores — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Um `InboxContract` abstrato, publicado no test-jar da `messaging-inbox`, verde sobre o InMemory e sobre os
seis provedores (7 subclasses), com o limite da DLQ nativa medido e documentado.

**Architecture:** O contrato monta um `InboxConsumer` real (sem Spring) sobre os `MessageSender`/`MessageReceiver`
que a subclasse fornece, no mesmo molde do `InboxConsumerTest`. Cada adapter ganha dependência de teste da inbox e do
test-jar dela e estende o contrato reusando o helper de container que já tem. Nenhum código de produção muda.

**Tech Stack:** Java 21, Maven, JUnit 5, Testcontainers (LocalStack 4.14.0, Service Bus emulator, Pub/Sub emulator,
ActiveMQ Classic 6, Artemis 2.40.0, RabbitMQ 4.1).

**Spec:** `docs/superpowers/specs/2026-11-06-f3-contrato-inbox-design.md`

## Global Constraints

- Nenhuma mudança em `src/main` de nenhum módulo; só testes, poms, `service-bus-config.json` e docs.
- Toda subclasse de broker: `@Testcontainers(disabledWithoutDocker = true)` e o helper de container existente.
- Filas/tópicos próprios da inbox (prefixo `inbox-`), nunca os do `MessagingContract`.
- Retry do contrato: nível 1 `BackoffSpec(2, 10ms, 1.0, 10ms, 0)` (3 execuções); nível 2 `BackoffSpec(2, delayedInitial(), 1.0, delayedInitial(), 0)`; `nonRetryable = [IllegalArgumentException]`. Nos casos de nível 2, nível 1 é `BackoffSpec.none()`.
- Orçamento padrão do `broker-delay` usado na conferência: `DEFAULT_BROKER_DELAY_DELIVERIES = 6` (1 + 5).
- Branch `feat/f3-contrato-inbox`; sem merge sem pedido.

## Review Focus

1. Mensagem de um teste anterior ainda no destino (lease vencido, cópia atrasada) → o teste seguinte não a conta: drenagem no `@BeforeEach` e filtro pelo atributo `inbox_contract_run`. Coberto pelo `InMemoryInboxContractTest` rodando os casos em ordem aleatória (Task 1).
2. Broker que devolve a mensagem por lease vencido durante o `hold` → o caso `holdRetriesWithoutReturning` precisa falhar, não passar por sorte: a espera do hold é > `lease()` (Task 1, assert de nenhuma reentrega com `deliveryCount` 1 no fim).
3. Limite nativo maior que o teto de 30 `nack`s (RabbitMQ 20 está perto) → falha dizendo "teto atingido", não um assert enganoso (Task 1).
4. Subclasse sem `retryQueues()` → `retryQueuesCopyAndAck` pulado (`Assumptions`), nunca vermelho (Task 1, InMemory roda com e sem).
5. Container que não sobe → toda a classe é pulada sem Docker, e com Docker o erro é do container, não NPE no `@BeforeAll` (Tasks 2–6, helpers existentes).

---

### Task 1: `InboxContract` no test-jar, verde sobre o InMemory

**Files:**
- Create: `messaging-inbox/src/test/java/com/example/messaging/inbox/contract/InboxContract.java`
- Create: `messaging-inbox/src/test/java/com/example/messaging/inbox/contract/InMemoryInboxContractTest.java`
- Modify: `messaging-inbox/pom.xml` (plugin `maven-jar-plugin`, execution `test-jar`)

**Interfaces:**
- Produces (abstratos): `MessageSender newSender()`, `MessageReceiver newReceiver()`, `MessageSender failedSender()`, `MessageReceiver failedReceiver()`, `MessageSender deadLetterSender()`, `MessageReceiver deadLetterReceiver()`, `Duration lease()`.
- Produces (com padrão): `List<RetryQueue> retryQueues()` → `null`; `MessageReceiver retryQueueReceiver()` → `null`; `Integer nativeDeliveryLimit()` → `null`; `boolean delayCopies()` → `false`; `Duration delayedInitial()` → `1s`; `Duration settle()` → `15s`.
- Produces: `public static final String RUN_ATTRIBUTE = "inbox_contract_run"`, `public static final int DEFAULT_BROKER_DELAY_DELIVERIES = 6`.
- Artefato: `com.example:messaging-inbox:${project.version}:test-jar`.

- [ ] **Step 1:** `InMemoryInboxContractTest extends InboxContract` com um `InMemoryMessaging` por teste e filas `inbox-main`, `inbox-failed`, `inbox-dlq`, `inbox-retry`; `retryQueues()` = `[new RetryQueue(lease(), queueSender("inbox-retry"))]`; `lease()` = 2 s. Classe aninhada `WithoutRetryQueues` (mesmo setup, `retryQueues()` = `null`) para o Review Focus 4. `@TestMethodOrder(MethodOrderer.Random.class)` no contrato.
- [ ] **Step 2:** `mvn -pl messaging-inbox test -Dtest='InMemoryInboxContractTest*'` → FAIL (compilação: `InboxContract` inexistente).
- [ ] **Step 3:** `InboxContract` com os casos da spec. O consumer é montado como `InboxConsumerTest.consumer(...)` (`ConsumerSettings("contract", "inbox-main", 1, 1, 1s, 3 × lease(), 5s, RELAXED, strategy, true)`, `Pipeline` com deserializer de `String`, `Destinations(this::newReceiver, failedSender(), deadLetterSender(), bands)`), parado no `@AfterEach`. Asserts por caso:
  - `successIsAcked`: handler chamado 1×; depois de `settle()`/3 o principal não entrega nada com o `RUN_ATTRIBUTE`; `failed` e DLQ vazios.
  - `immediateRetryThenSuccess`: 3 chamadas, todas com `delivery().level() == IMMEDIATE`; nenhuma cópia.
  - `nonRetryableGoesToFailed`: 1 chamada; cópia em `failed` com `inbox_failure` contendo `exception=java.lang.IllegalArgumentException` e `deadLetter().originalMessageId()` igual ao id enviado; principal vazio.
  - `rejectCarriesTheReason`: `HandlerResult.reject("not today")`; cópia em `failed` com `inbox_failure` contendo `reason=rejected` e `not today`.
  - `brokerDelayThenDeadLetter` (`assumeTrue(receiver.capabilities().delayedRedelivery())`): estratégia `BROKER_DELAY`; 3 chamadas; cópia na DLQ com `inbox_attempts` = `l2=2;l3=0`.
  - `holdRetriesWithoutReturning`: estratégia `HOLD`, nível 2 com atraso = `lease()` (a espera passa do lease); 3 chamadas; todas vistas com o mesmo `messageId` e nenhuma com `deliveryCount() > 1`; cópia na DLQ.
  - `retryQueuesCopyAndAck` (`assumeTrue(retryQueues() != null)`): estratégia `RETRY_QUEUES`; cópia em `retryQueueReceiver()` com `inbox_attempts` = `l2=1;l3=0` e sem `inbox_failure`; principal vazio.
  - `slowHandlerKeepsTheLease`: handler dorme `lease() × 3/2`; 1 chamada; nenhuma reentrega até `settle()`/3 depois.
  - `stopReleasesUnstarted`: `maxMessagesPerPoll` 3, handler bloqueado na 1ª mensagem; `stop()`; as 2 não iniciadas voltam ao principal (recebidas de novo por `newReceiver()` com `deliveryCount() >= 2`, quando o broker informa).
  - `nativeDeliveryLimitIsWhatTheBrokerDoes` (`assumeTrue(nativeDeliveryLimit() != null)`): envia 1; laço `receive` + `nack(msg, ZERO)` contando entregas até um `receive` vazio por `settle()`/3 ou 30 entregas; `fail("teto de 30 entregas atingido")` no teto; `assertEquals(nativeDeliveryLimit(), entregas)`; sem `delayCopies()`, `assertTrue(nativeDeliveryLimit() > DEFAULT_BROKER_DELAY_DELIVERIES)`.
  - `@BeforeEach`: drena principal, `failed`, DLQ e banda (`ack` de tudo por até `settle()`/3).
- [ ] **Step 4:** mesmo comando → PASS; `nativeDeliveryLimitIsWhatTheBrokerDoes` e `brokerDelayThenDeadLetter` conforme as capabilities do InMemory (pulados ou verdes, nunca vermelhos); `retryQueuesCopyAndAck` pulado só em `WithoutRetryQueues`.
- [ ] **Step 5:** `mvn -pl messaging-inbox install` → o jar `messaging-inbox-0.1.0-SNAPSHOT-tests.jar` em `target/`.
- [ ] **Step 6:** `git commit -m "test(inbox): InboxContract no test-jar, verde sobre o InMemory"`

### Task 2: SQS

**Files:**
- Modify: `messaging-aws/pom.xml` (test deps `messaging-inbox` e `messaging-inbox` `<type>test-jar</type>`, ambos `${project.version}`, `test`)
- Create: `messaging-aws/src/test/java/com/example/messaging/aws/SqsInboxContractTest.java`

**Interfaces:**
- Consumes: Task 1 (`InboxContract`); `LocalStack.start()`, `LocalStack.queue(name)`, `LocalStack.sqs()`, `LocalStack.LEASE` (2 s).

- [ ] **Step 1:** Subclasse com filas `inbox-main`, `inbox-failed`, `inbox-dlq`, `inbox-retry` criadas no `@BeforeAll`; receivers como `SqsLocalStackContractTest.newReceiver()`; `retryQueues()` = banda `inbox-retry` com `delay` = `lease()`; `nativeDeliveryLimit()` = `null`; `delayedInitial()` = 1 s.
- [ ] **Step 2:** `mvn -pl messaging-aws test -Dtest=SqsInboxContractTest` → PASS (o contrato já existe; vermelho aqui é defeito real a investigar com superpowers:systematic-debugging, não ajuste de assert).
- [ ] **Step 3:** `git commit -m "test(aws): contrato da inbox no SQS"`

### Task 3: Service Bus (fila e subscription)

**Files:**
- Modify: `messaging-azure/src/test/resources/service-bus-config.json` (filas `inbox`, `inbox-failed`, `inbox-dlq`, `inbox-retry`: `LockDuration` `PT5S`, `MaxDeliveryCount` 10, sem sessions)
- Modify: `messaging-azure/pom.xml` (mesmas test deps da Task 2)
- Create: `messaging-azure/src/test/java/com/example/messaging/azure/ServiceBusInboxContractTest.java`
- Create: `messaging-azure/src/test/java/com/example/messaging/azure/ServiceBusSubscriptionInboxContractTest.java`

**Interfaces:**
- Consumes: Task 1; `ServiceBusEmulator.builder()`, `ServiceBusEmulator.LEASE` (5 s); receivers como em `ServiceBusRescheduleContractTest` (fila, `RESCHEDULE`) e `ServiceBusSubscriptionContractTest` (`contract-topic`/`contract-sub`).

- [ ] **Step 1:** `ServiceBusInboxContractTest`: principal `inbox` com `RESCHEDULE`; `nativeDeliveryLimit()` = 10; `delayCopies()` = `true`; banda `inbox-retry`.
- [ ] **Step 2:** `ServiceBusSubscriptionInboxContractTest`: principal `contract-topic`/`contract-sub` (sender no tópico); `nativeDeliveryLimit()` = 10; `delayCopies()` = `false`; mesmas filas de cópia.
- [ ] **Step 3:** `mvn -pl messaging-azure test -Dtest='ServiceBus*InboxContractTest'` → PASS (`brokerDelayThenDeadLetter` pulado na subscription).
- [ ] **Step 4:** `mvn -pl messaging-azure test` → os contratos antigos continuam verdes com as entidades novas no emulator.
- [ ] **Step 5:** `git commit -m "test(azure): contrato da inbox no Service Bus (fila e subscription)"`

### Task 4: Pub/Sub

**Files:**
- Modify: `messaging-gcp/pom.xml` (test deps)
- Create: `messaging-gcp/src/test/java/com/example/messaging/gcp/PubSubInboxContractTest.java`

**Interfaces:**
- Consumes: Task 1; `PubSubEmulator.start()`, `topicWithSubscription(topic, subscription, false)`, `sender(topic)`, `receiver(subscription, options)`, `PubSubEmulator.LEASE` (10 s).

- [ ] **Step 1:** Subclasse com pares tópico/subscription `inbox-main`, `inbox-failed`, `inbox-dlq`, `inbox-retry`; `nativeDeliveryLimit()` = `null`; `delayedInitial()` = 1 s; `settle()` = 30 s.
- [ ] **Step 2:** `mvn -pl messaging-gcp test -Dtest=PubSubInboxContractTest` → PASS.
- [ ] **Step 3:** `git commit -m "test(gcp): contrato da inbox no Pub/Sub"`

### Task 5: ActiveMQ Classic e Artemis

**Files:**
- Modify: `messaging-jms/pom.xml` (test deps)
- Create: `messaging-jms/src/test/java/com/example/messaging/jms/ActiveMqClassicInboxContractTest.java`
- Create: `messaging-jms/src/test/java/com/example/messaging/jms/ActiveMqArtemisInboxContractTest.java`

**Interfaces:**
- Consumes: Task 1; `ActiveMqClassic.start()/connection()/queue(name)`, `ActiveMqArtemis.start()/connection()/queue(name)`; receivers como `ActiveMq*ContractTest.newReceiver()` (`JmsMessageReceiver(connection, dialect, queue, LEASE, 10, Redelivery.SCHEDULED, ...)`), lease 5 s.

- [ ] **Step 1:** Classic: `nativeDeliveryLimit()` = 6 (política do cliente), `delayCopies()` = `true`. Artemis: `nativeDeliveryLimit()` = 10, `delayCopies()` = `true`. Ambas com banda `inbox-retry`.
- [ ] **Step 2:** `mvn -pl messaging-jms test -Dtest='ActiveMq*InboxContractTest'` → PASS. Se o limite medido divergir do declarado, corrigir o valor declarado e registrar o medido (o contrato mede; a tabela da spec é expectativa).
- [ ] **Step 3:** `git commit -m "test(jms): contrato da inbox no ActiveMQ Classic e no Artemis"`

### Task 6: RabbitMQ

**Files:**
- Modify: `messaging-rabbitmq/pom.xml` (test deps)
- Create: `messaging-rabbitmq/src/test/java/com/example/messaging/rabbitmq/RabbitMqInboxContractTest.java`

**Interfaces:**
- Consumes: Task 1; `RabbitMq.start()/connection()/queue(name, "quorum")`; receiver como `RabbitMqContractTest.newReceiver()` com lease 5 s.

- [ ] **Step 1:** Subclasse com filas quorum `inbox-main`, `inbox-failed`, `inbox-dlq`, `inbox-retry`; `nativeDeliveryLimit()` = 20; `delayCopies()` = `false`.
- [ ] **Step 2:** `mvn -pl messaging-rabbitmq test -Dtest=RabbitMqInboxContractTest` → PASS (`brokerDelayThenDeadLetter` pulado). Mesmo tratamento da Task 5 se o limite medido divergir.
- [ ] **Step 3:** `git commit -m "test(rabbitmq): contrato da inbox no RabbitMQ"`

### Task 7: documentação e build completo

**Files:**
- Modify: `README.md` (seção nova "DLQ nativa e o orçamento da inbox": a tabela medida da spec, com a coluna "medido pelo contrato" e o orçamento por estratégia)
- Modify: `messaging-inbox/docs/adr/0002-retry-em-tres-niveis.md` (revisão 2026-11-06: valores medidos e a regra "limite nativo > 6 no `broker-delay` sem cópia")
- Modify: `messaging-inbox/docs/plan.md` (linha da F3: Status "Feita em <data>: 7 subclasses verdes" e o tempo do build)

- [ ] **Step 1:** Escrever as três mudanças com os valores efetivamente medidos nas Tasks 2–6.
- [ ] **Step 2:** `mvn clean install` na raiz → BUILD SUCCESS; anotar no `plan.md` o tempo total e o de cada subclasse (linhas `Tests run ... Time elapsed` do log).
- [ ] **Step 3:** `git commit -m "docs: DLQ nativa medida e F3 feita"`
