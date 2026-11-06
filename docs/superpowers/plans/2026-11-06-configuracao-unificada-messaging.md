# Configuração unificada em `messaging.*` — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Configurar um listener com `providers` + `receivers` (e `senders` para envio) sob o prefixo único
`messaging`, eliminando `messaging.destinations`, `inbox.*` e `inbox.brokers`, e migrar o engine-tax.

**Architecture:** O endereço sai do destino nomeado e passa para quem usa: cada receiver e cada sender declara
`provider` + `destination` (+ `subscription`, `kind`, `options`). O starter converte esse endereço no
`MessagingProperties.Destination` que os `ProviderConnection` já consomem, então os adapters e a validação por tipo
(`validateShape`) continuam valendo. `MessagingProperties` (sempre presente) liga `providers` e `senders`;
`InboxProperties` (só com a messaging-inbox) liga `receivers`, `receiver-defaults` e `receivers-enabled`, cada raiz
em modo estrito via `Binder` + `NoUnboundElementsBindHandler`.

**Tech Stack:** Java, Spring Boot 4 (autoconfigure, `Binder`), Maven, JUnit 5, AssertJ, Testcontainers (LocalStack,
Artemis, RabbitMQ).

**Spec:** a seção "Especificação" abaixo (decidida na conversa de 2026-11-06; não há documento separado).

## Especificação

```yaml
messaging:
  providers:
    aws-sa: { type: aws, region: sa-east-1 }
    azure:  { type: azure, connection-string: ... }

  receivers-enabled: ${RECEIVERS_ENABLED:true}     # interruptor geral; padrão true

  receiver-defaults:                               # tudo de um receiver, menos endereços
    provider: aws-sa                               # opcional
    concurrency: 4
    key-lanes: 10
    retry: { immediate: { max-attempts: 3 }, non-retryable: [com.acme.InvalidOrderException] }
    max-messages-per-poll: 10                      # antes configuration.*
    poll-wait: 20s
    max-processing: 10m
    shutdown-timeout: 15s

  receivers:
    lifecycle-received:
      enabled: ${LIFECYCLE_RECEIVED_ENABLED:true}  # padrão true
      provider: aws-sa
      destination: ${LIFECYCLE_RECEIVED_QUEUE_URL} # obrigatório
      dead-letter: lifecycle-received-dlq          # opcional
      failed: ${LIFECYCLE_RECEIVED_FAILED_QUEUE_URL} # obrigatório (ADR-0002)
    orders-processed:
      provider: azure
      destination: orders
      subscription: billing                        # presente => destination é tópico
      dead-letter: orders-dlq
      failed: orders-failed
      options: { sessions: true, redelivery: reschedule }

  senders:
    lifecycle-sender:
      provider: aws-sa
      destination: ${OUTCOME_STATUS_QUEUE_URL}
      kind: queue                                  # queue (padrão) | topic
      options: { max-message-bytes: 262144 }
```

Regras:

1. Um receiver roda só se `receivers-enabled` **e** o `enabled` dele forem `true`. Receiver desligado: configuração
   validada no startup, nenhuma conexão aberta, handler (`@MessageListener` ou bean pelo nome) aceito sem erro.
2. Sender não tem `enabled`.
3. `provider`: receiver > `receiver-defaults.provider` > o único provider existente. Sem nenhum dos três:
   `messaging.receivers.<n>.provider é obrigatório com mais de um provider`. Sender: `provider` > o único provider.
4. `dead-letter`, `failed` e `retry.delayed.queues[].destination` são endereços no provider do receiver, com a
   mesma regra de formato de `destination` (sem `subscription`, `kind: queue`, sem `options`).
5. Formato do endereço por tipo de provider:

   | Tipo | Receber (`destination` [+ `subscription`]) | Enviar (`destination` + `kind`) |
   |---|---|---|
   | aws | `https://…` = queue-url; senão nome de fila (`GetQueueUrl` no startup). `subscription` é erro | `arn:aws:sns:…` = topic-arn; `https://…` = queue-url; senão nome de fila. `kind: topic` sem ARN é erro |
   | azure, activemq-classic, artemis, rabbitmq | com `subscription`: topic + subscription; senão queue | `kind: topic` = topic; senão queue |
   | gcp | `destination` = subscription. `subscription` é erro | `destination` = topic. `kind: queue` explícito é erro |

6. `options` (tipado, todos opcionais): `require`, `sessions`, `redelivery`, `max-message-bytes`, `ack-deadline`,
   `ordered`, `exactly-once`, `dead-letter-queue`, `lease`, `routing-key`, `routing-by-ordering-key`,
   `confirm-timeout`. A aplicabilidade por tipo é a do `validateShape` atual (mensagens "não se aplica a este
   provedor").
7. Chaves desconhecidas em `messaging.receivers`, `messaging.receiver-defaults`, `messaging.senders` e
   `messaging.providers` derrubam o startup.
8. Chaves antigas derrubam o startup com mensagem de migração: `messaging.destinations.*`, `inbox.enabled`,
   `inbox.defaults.*`, `inbox.consumers.*`, `inbox.brokers.*`.
9. `@MessageListener(receiver = "<nome>")` substitui `@InboxListener(consumer = …)`; `InboxListener` é removida.
10. Proibidos em `receiver-defaults` (são de um receiver só): `destination`, `subscription`, `dead-letter`, `failed`,
    `options`, `enabled`, `retry.delayed.queues`.

## Global Constraints

- Mensagens de erro: português no pacote `com.example.messaging.spring` e inglês em `com.example.messaging.inbox.spring` (como hoje), sempre com o caminho completo da chave (`messaging.receivers.<n>.…`).
- Métricas não mudam de nome (`inbox.*`, `messaging.*`). A tag `destination` passa a ser o **nome do receiver/sender** (nunca a URL), e as cópias usam `<receiver>.dead-letter`, `<receiver>.failed` e `<receiver>.retry.<delay>`. Os testes de alerta do engine-tax (`ops/prometheus/alerts_test.yml`) dependem de `destination="events-status"`.
- `messaging-inbox`, `idempotency-core` e Jackson continuam `<optional>` no starter: nenhum tipo de `com.example.messaging.inbox` pode aparecer em classes do pacote `com.example.messaging.spring`.
- Adapters (`messaging-aws`, `-azure`, `-gcp`, `-jms`, `-rabbitmq`) não mudam.
- Branch `refactor/messaging-config-unificada` na messaging-library e `refactor/messaging-config` no engine-tax. Nada de merge sem pedido.
- No engine-tax, `lifecycle-worker/src/main/resources/application.yaml` tem edição local do usuário (linhas em branco): editar por cima, nunca `checkout`/`restore`.

## Review Focus

1. Chave antiga esquecida (YAML, `INBOX_*` em env, Helm values) → startup falha dizendo a chave nova; nunca é ignorada em silêncio. Teste na Task 1 (`messaging.destinations`) e na Task 4 (`inbox.*`).
2. `receivers-enabled: false` com `enabled: true` no receiver → nenhum receiver inicia. Teste na Task 5.
3. Receiver desligado com `@MessageListener` → sem erro de "receiver inexistente", configuração inválida dele ainda falha, nenhuma conexão. Testes nas Tasks 5 e 6.
4. Tag `destination` das métricas igual ao nome do receiver, mesmo com `destination` em URL. Teste na Task 5.
5. Nome de fila SQS inexistente → falha no startup com o nome e o provider, não no primeiro receive. Teste na Task 3.

---

### Task 1: `MessagingProperties` com `providers` + `senders`, endereço e binding estrito

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingProperties.java`
- Create: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/Endpoints.java`
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingAutoConfiguration.java`
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/EndpointsTest.java` (novo), `MessagingAutoConfigurationTest.java`

**Interfaces:**
- Produces:
  - `MessagingProperties(Map<String, Provider> providers, Map<String, Sender> senders)` — sem `@ConfigurationProperties`; `static MessagingProperties bind(Binder binder)` liga `messaging.providers` e `messaging.senders` com `NoUnboundElementsBindHandler` e lança `IllegalStateException` se houver chaves antigas (regra 8, só `messaging.destinations` aqui).
  - `enum Kind { QUEUE, TOPIC }`
  - `record Options(Set<Requirement> require, Boolean sessions, Redelivery redelivery, Long maxMessageBytes, Duration ackDeadline, Boolean ordered, Boolean exactlyOnce, Boolean deadLetterQueue, Duration lease, String routingKey, Boolean routingByOrderingKey, Duration confirmTimeout)` com `static final Options NONE`.
  - `record Endpoint(String destination, String subscription, Kind kind, Options options)`; `options == null` → `Options.NONE`, `kind == null` → `null` (ausente; regra 5 decide).
  - `record Sender(String provider, String destination, Kind kind, Options options)` com `Endpoint endpoint()`.
  - `Destination` continua o mesmo record (modelo interno dos `ProviderConnection`), sem o campo `provider` e sem binding.
  - `final class Endpoints`: `static Destination toDestination(String key, Type type, Endpoint e, boolean receive, List<String> problems)` aplica a tabela da regra 5 e copia `options`; `static String provider(String key, String explicit, String fallback, Map<String, Provider> providers, List<String> problems)` aplica a regra 3.
  - `MessagingAutoConfiguration` expõe `@Bean MessagingProperties messagingProperties(Environment env)` via `bind(Binder.get(env))`.

- [ ] **Step 1: Testes que falham** em `EndpointsTest`:
  - `awsReceiveUrlIsQueueUrl`: `https://sqs.sa-east-1.amazonaws.com/1/q` → `queueUrl()` igual, `queue()` null.
  - `awsReceiveNameIsQueue`: `q` → `queue() == "q"`, `queueUrl()` null.
  - `awsSendArnIsTopic`: `arn:aws:sns:sa-east-1:1:t` → `topicArn()`.
  - `awsTopicKindWithoutArnFails`: problema contém `kind: topic exige um ARN do SNS`.
  - `awsSubscriptionFails`, `gcpSubscriptionFails`, `gcpExplicitQueueKindFails`: problema contém `não se aplica a este provedor`.
  - `brokerReceiveWithSubscriptionIsTopic` (para AZURE, ARTEMIS, ACTIVEMQ_CLASSIC, RABBITMQ, `@EnumSource`): `topic() == "orders"`, `subscription() == "billing"`.
  - `brokerSendTopicKind`: `kind: TOPIC` → `topic()`; sem `kind` → `queue()`.
  - `gcpReceiveIsSubscription` / `gcpSendIsTopic`.
  - `providerFallsBackToTheOnlyOne`; `providerRequiredWithSeveral`: problema `messaging.senders.s.provider é obrigatório com mais de um provider`; `unknownProvider`: `provider 'x' não existe em messaging.providers`.
  Em `MessagingAutoConfigurationTest`: `unknownSenderKeyFails` (`messaging.senders.s.queue-url=…` → falha citando a chave) e `legacyDestinationsFail` (`messaging.destinations.x.provider=aws` → mensagem contém `messaging.destinations foi removido; use messaging.senders e messaging.receivers`).
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest='EndpointsTest,MessagingAutoConfigurationTest'` → FAIL (compilação).
- [ ] **Step 3:** Implementar as interfaces acima. Ajustar `validateShape` (Task 2 usa) para aceitar `queue` no AWS (nome a resolver). O módulo precisa compilar ao fim da task: `MessagingDestinations` passa a iterar `senders` (via `Endpoints.toDestination`) no lugar de `destinations`; o resto dele é a Task 2.
- [ ] **Step 4:** mesmo comando → PASS.
- [ ] **Step 5:** `git commit -m "refactor(starter): providers e senders com endereço próprio em messaging.*"`

### Task 2: `MessagingDestinations` por nome de sender e por endereço

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingDestinations.java`, `MessagingHealthAutoConfiguration.java`
- Test: `MessagingAutoConfigurationTest.java`, `jms/ArtemisAutoConfigurationTest.java`, `jms/ActiveMqAutoConfigurationTest.java`, `rabbitmq/RabbitAutoConfigurationTest.java`

**Interfaces:**
- Consumes: Task 1.
- Produces (públicos em `MessagingDestinations`):
  - `MessageSender sender(String name)` — sender de `messaging.senders` (como hoje, agora só senders).
  - `List<String> validate(String key, String provider, Endpoint endpoint, boolean receive)` — resolve provider (regra 3) e formato (regra 5) e roda `validateShape`; não abre conexão.
  - `MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter)` — receiver novo por chamada, métrica com tag = `name`; registra `name` para o health.
  - `MessageSender sender(String name, String provider, Endpoint endpoint)` — criado uma vez por `name` e reaproveitado; métrica/observação com tag = `name`.
  - `Set<String> names()` — senders + nomes registrados por `receiver(...)`; `checkAccess(name)` cobre os dois.
  - Lança `IllegalStateException("Configuração inválida de messaging.*: …")` com todos os problemas, como hoje.

- [ ] **Step 1:** Migrar os testes de provider para `messaging.senders` + chamadas `receiver(name, provider, endpoint, null)`, mantendo as asserções atuais de envio/recebimento. Novo `receiverByEndpointIsInHealth`: depois de `receiver("r", …)`, `names()` contém `"r"`.
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest='*AutoConfigurationTest'` → FAIL.
- [ ] **Step 3:** Implementar. Os senders de `messaging.senders` continuam criados e checados (`require`) no construtor; os por endereço, na primeira chamada.
- [ ] **Step 4:** mesmo comando → PASS (Artemis/RabbitMQ/ActiveMQ precisam de Docker).
- [ ] **Step 5:** `git commit -m "refactor(starter): senders e receivers a partir do endereço"`

### Task 3: nome de fila SQS resolvido no startup

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/aws/AwsProviderConnection.java`
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/inbox/spring/SqsLocalStackTest.java`

**Interfaces:**
- Consumes: `Destination.queue()` preenchido pelo `Endpoints` para nome de fila AWS.
- Produces: `sender`/`receiver` do AWS aceitam `queue` e resolvem com `sqs.getQueueUrl(r -> r.queueName(queue))`, uma vez por nome (cache na conexão).

- [ ] **Step 1:** Testes em `SqsLocalStackTest`: `receiverByQueueName` (fila criada no LocalStack, receiver configurado com o nome, mensagem enviada pela URL chega) e `unknownQueueNameFailsAtStartup` (mensagem contém o nome da fila e `messaging.receivers.<n>.destination`).
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest=SqsLocalStackTest` → FAIL.
- [ ] **Step 3:** Implementar; `QueueDoesNotExistException` vira problema de configuração, não `MessagingException` em runtime.
- [ ] **Step 4:** mesmo comando → PASS.
- [ ] **Step 5:** `git commit -m "feat(aws): fila SQS por nome, resolvida no startup"`

### Task 4: `InboxProperties` em `messaging.receivers` e mescla com `receiver-defaults`

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/inbox/spring/InboxProperties.java`, `InboxConfig.java`, `InboxAutoConfiguration.java`
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/inbox/spring/InboxConfigTest.java`

**Interfaces:**
- Consumes: `MessagingProperties.Endpoint`, `Options`, `Kind` (Task 1).
- Produces:
  - `InboxProperties` sem `@ConfigurationProperties`, sem `Broker`/`brokers`; `static InboxProperties bind(Binder binder)` liga `messaging.receivers` (mapa), `messaging.receiver-defaults` e `messaging.receivers-enabled` (padrão `true`), cada um com `NoUnboundElementsBindHandler`, e falha com as chaves antigas `inbox.*` (regra 8).
  - `ConsumerOptions` ganha `Boolean enabled`, `String provider`, `String subscription`, `MessagingProperties.Options options`, `Integer maxMessagesPerPoll`, `Duration pollWait`, `Duration maxProcessing`, `Duration shutdownTimeout`; perde `broker` e `configuration`.
  - `InboxConfig.Consumer` ganha `boolean enabled`, `String provider` (resolvido, pode ser null = único provider) e `Endpoint endpoint`; `destination`, `deadLetter`, `failed` e `QueueBand.destination` passam a ser endereços (texto). Remove `CONFIGURATION_KEYS`, `parse`, `unknownConfiguration`, `MESSAGING`.
  - `InboxAutoConfiguration`: `@ConditionalOnProperty` sai (o interruptor é `receivers-enabled`, avaliado na Task 5); `@Bean InboxProperties inboxProperties(Environment env)`.

- [ ] **Step 1:** Reescrever `InboxConfigTest` para ligar via `InboxProperties.bind(new Binder(new MapConfigurationPropertySource(values)))` (o mesmo caminho de produção) e com prefixo `messaging.receivers.orders.`. Testes:
  - `receiverWinsOverDefaults` (pollWait 10s no receiver, 5s nos defaults → 10s; maxMessagesPerPoll 4 dos defaults).
  - `libraryDefaults` (pollWait 20s, maxProcessing 15m, shutdownTimeout 30s, maxMessagesPerPoll 1, enabled true).
  - `invalidDurationFails` (`poll-wait=soon` → `BindException` citando `messaging.receivers.orders.poll-wait`).
  - `unknownKeyFails` (`messaging.receivers.orders.visibility-timeout` → falha).
  - `forbiddenInDefaults` para cada chave da regra 10 → `messaging.receiver-defaults.<chave> belongs to one receiver`.
  - `destinationAndFailedRequired`.
  - `legacyInboxKeysFail` (`inbox.consumers.x.destination=q` → mensagem contém `inbox.* was removed; use messaging.receivers`).
  - `providerFromDefaults` (`receiver-defaults.provider=b` com dois providers → `provider() == "b"`).
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest=InboxConfigTest` → FAIL.
- [ ] **Step 3:** Implementar; trocar todo `"inbox.consumers."`/`"inbox.defaults"` por `"messaging.receivers."`/`"messaging.receiver-defaults"` nas mensagens do pacote (`InboxConfig`, `ConsumerBuilder`, `HandlerBinding`, `InboxAutoConfiguration.lazy`). O módulo precisa compilar ao fim da task: `ConsumerBuilder` passa a ler `endpoint`/`provider` do `Consumer` com a assinatura antiga de `InboxDestinations` (usando `endpoint().destination()` como nome); a troca de assinatura é a Task 5.
- [ ] **Step 4:** mesmo comando → PASS.
- [ ] **Step 5:** `git commit -m "refactor(inbox): receivers e receiver-defaults em messaging.*"`

### Task 5: receivers montados por endereço, interruptores e tag de métrica

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/inbox/spring/InboxDestinations.java`, `ConsumerBuilder.java`, `InboxLifecycle.java`, `InboxAutoConfiguration.java`, `MicrometerConsumerObserver.java`
- Test: `InboxAutoConfigurationTest.java`, `ObservabilityTest.java`, `WithBootJacksonAndIdempotencyTest.java`, `SqsLocalStackTest.java`

**Interfaces:**
- Consumes: Task 2 (`validate`, `receiver(name, provider, endpoint, deadLetter)`, `sender(name, provider, endpoint)`), Task 4 (`InboxConfig.Consumer`).
- Produces:
  - `InboxDestinations`: `MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter)`, `MessageSender sender(String name, String provider, Endpoint endpoint)`, `List<String> validate(String key, String provider, Endpoint endpoint, boolean receive)`. O padrão delega a `MessagingDestinations`; o fake dos testes (sobre `InMemoryMessaging`) usa `endpoint.destination()` como nome e `validate` vazio.
  - `ConsumerBuilder` nomeia as cópias `<receiver>.dead-letter`, `<receiver>.failed`, `<receiver>.retry.<delay>` e passa o sender de dead-letter ao receiver.
  - `ConsumerSettings` recebe o nome do receiver no lugar de `destination` (tag `destination` = nome).
  - `InboxLifecycle`: valida todos os receivers (ligados e desligados) antes de abrir qualquer conexão; inicia só `receiversEnabled && consumer.enabled()`.

- [ ] **Step 1:** Testes em `InboxAutoConfigurationTest`:
  - `globalSwitchOffStartsNothing` (`receivers-enabled=false`, receiver com `enabled=true` → nenhum receive no fake).
  - `disabledReceiverIsValidatedButNotStarted` (receiver `enabled=false` sem `failed` → falha; com config válida → contexto sobe e o fake não recebeu `receiver(...)`).
  - `copiesUseReceiverScopedNames` (falha final vai para o sender `orders.failed`).
  Em `ObservabilityTest`: `destinationTagIsTheReceiverName` com `destination=https://sqs…/orders-q` → tag `destination="orders"`.
  Migrar os demais testes para `messaging.receivers.*`.
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest='InboxAutoConfigurationTest,ObservabilityTest,WithBootJacksonAndIdempotencyTest'` → FAIL.
- [ ] **Step 3:** Implementar.
- [ ] **Step 4:** mesmo comando, e depois `SqsLocalStackTest` → PASS.
- [ ] **Step 5:** `git commit -m "refactor(inbox): receivers por endereço, enabled por receiver e interruptor geral"`

### Task 6: `@MessageListener(receiver = …)`

**Files:**
- Create: `messaging-spring-boot-starter/src/main/java/com/example/messaging/inbox/spring/MessageListener.java`
- Delete: `messaging-spring-boot-starter/src/main/java/com/example/messaging/inbox/spring/InboxListener.java`
- Modify: `HandlerBinding.java`, `package-info.java`
- Test: `InboxAutoConfigurationTest.java` (e todo teste que usa `@InboxListener`)

**Interfaces:**
- Produces: `@MessageListener` (METHOD, RUNTIME, Documented) com `String receiver()`; mesmas assinaturas de método aceitas que a `@InboxListener` (javadoc copiado). `HandlerBinding` reconhece receivers desligados como existentes.

- [ ] **Step 1:** Trocar `@InboxListener(consumer=…)` por `@MessageListener(receiver=…)` nos testes; novos `listenerOfDisabledReceiverIsAccepted` e `listenerOfUnknownReceiverFails` (`@MessageListener … receiver 'x' does not exist in messaging.receivers`).
- [ ] **Step 2:** `mvn -pl messaging-spring-boot-starter test -Dtest=InboxAutoConfigurationTest` → FAIL.
- [ ] **Step 3:** Implementar.
- [ ] **Step 4:** `mvn clean install` na raiz → BUILD SUCCESS (com Docker).
- [ ] **Step 5:** `git commit -m "refactor(inbox)!: @MessageListener(receiver) no lugar de @InboxListener"`

### Task 7: documentação

**Files:**
- Modify: `messaging-inbox/docs/adr/0008-configuracao-de-brokers-e-consumers.md` (revisão 2026-11-06: substitui Modelo, Brokers implícitos e Precedência de `configuration`; registra a falha de valor inválido no binding), `docs/adr/0008-spring-boot-e-observabilidade.md`, `README.md`, `messaging-inbox/docs/incorporacao.md`, `messaging-inbox/docs/plan.md`, ADRs 0002, 0010, 0011, 0013, 0014, 0015, 0016 da messaging-inbox (só os exemplos de chave), javadocs de `MessagingProperties`/`InboxProperties`.

- [ ] **Step 1:** Atualizar os exemplos para a Especificação.
- [ ] **Step 2:** `grep -rnE 'inbox\.(enabled|consumers|defaults|brokers)|messaging\.destinations|InboxListener' --include='*.md' --include='*.java' . | grep -v /target/ | grep -v superpowers/plans` → só ocorrências históricas na seção de revisão da ADR-0008.
- [ ] **Step 3:** `git commit -m "docs: configuração unificada em messaging.*"`

### Task 8: migração do engine-tax

**Files:**
- Modify: `engine-tax/lifecycle-worker/src/main/resources/application.yaml`, `engine-tax/outcome-api/src/main/resources/application.yaml`, `lifecycle-worker/.../config/MessagingConfiguration.java:55`, `outcome-api/.../config/MessagingConfiguration.java:56`, `README.md:26`, `platform/platform-support/pom.xml:16`, `docs/metricas.md:28`, `docs/lifecycle-design.md:23`, `ops/prometheus/alerts.yml:126` (só comentários e docs)

**Interfaces:**
- Consumes: Tasks 1–6 instaladas no repositório Maven local (`mvn install` da messaging-library).

- [ ] **Step 1:** Reescrever os dois YAMLs. `outcome-api`:
  ```yaml
  messaging:
    providers:
      aws: { type: aws }
    receivers-enabled: ${outcome.consumer:true}
    receiver-defaults:
      key-lanes: 10
      max-messages-per-poll: 10
      poll-wait: 10s
      shutdown-timeout: 15s
    receivers:
      outcome-status:
        destination: ${OUTCOME_STATUS_QUEUE_URL}
        dead-letter: ${OUTCOME_STATUS_DLQ_URL}
        failed: ${OUTCOME_STATUS_FAILED_QUEUE_URL}
      events-status:
        destination: ${OUTCOME_EVENTS_QUEUE_URL}
        dead-letter: ${OUTCOME_EVENTS_DLQ_URL}
        failed: ${OUTCOME_EVENTS_FAILED_QUEUE_URL}
  ```
  `lifecycle-worker`: igual, com `receivers-enabled: ${lifecycle.consumer:true}` e o receiver `lifecycle-received` (`LIFECYCLE_RECEIVED_QUEUE_URL`, `_DLQ_URL`, `_FAILED_QUEUE_URL`). Manter os comentários existentes que ainda valem.
- [ ] **Step 2:** `grep -rnE 'INBOX_|inbox\.(enabled|consumers|defaults|brokers)|^inbox:|messaging\.destinations' --exclude-dir=target --exclude-dir=.git .` no engine-tax (inclui Helm/compose/test resources) → só nomes de métrica `inbox_*`/`inbox.*`.
- [ ] **Step 3:** `mvn clean verify` no engine-tax → BUILD SUCCESS; `promtool test rules ops/prometheus/alerts_test.yml` se o `promtool` estiver instalado → SUCCESS.
- [ ] **Step 4:** `git commit -m "chore: configuração do messaging na forma unificada (receivers)"`
