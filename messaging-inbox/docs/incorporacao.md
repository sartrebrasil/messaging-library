# Incorporação do consumo (inbox) na messaging-library

Status: decisões D1 a D7 aceitas em 2026-11-05. Fatias 0 a 10 feitas em 2026-11-05 (transação `join` adiada, ver Pendências; a 10 numa branch do engine-tax, sem merge nem implantação).

Objetivo: trazer o `QueueConsumer` do engine-tax
(`engine-tax/platform-support/.../QueueConsumer.java`) para a messaging-library
e evoluí-lo até o "pronto quando" da F2 do [plano da inbox](plan.md), seguindo
os ADRs 0001 a 0017 deste diretório.

## Ponto de partida

| O quê | Onde | Estado |
|---|---|---|
| `QueueConsumer` | engine-tax `platform-support` | Loop numa virtual thread, `receive(10, 10s)`, envelope da outbox no body, `nack(retryAfter)` fixo, DLQ pela redrive policy do SQS (5 entregas), métricas `xmlplatform.consumer.*`, trace pelo `headers.traceparent` do body, guarda que impede a thread de morrer (T-88/B20), gauge `last.receive.age` |
| Motor de retry | `../inbox-library/inbox-core/.../retry` | Portado da acme-messaging: `BackoffSpec`, `RetryPolicy`, `ImmediateRetrier`, `ExceptionClassifier`, `FailureResolver`, `AttemptState`, `FailureAttribute`, `ErrorFingerprint`, `HandlerResult`, anotações. Cerca de 1.400 linhas, com testes |
| ADRs e plano | `messaging-inbox/docs` (movidos de `messaging-core/docs`), cópia idêntica de `../inbox-library/docs` | Aceitos em 2026-10-08 |
| Idempotência | `../idempotency-multi-store` | Boot 4 e Jackson 3 já feitos. **Sem owner token** em `reserve`/`complete`/`release` (pré-requisito do ADR-0004). `idempotency-core` sem Spring |
| Broker em memória | `messaging-testkit` `InMemoryMessaging` | Fila com `delayedRedelivery`, `deliveryCount`, lease, `keepAlive`, ordem opcional. Basta para `broker-delay`, `hold` e `retry-queues` (cópia para outra fila) |

## Decisões (aceitas em 2026-11-05)

| # | Tema | Proposta | Por quê |
|---|---|---|---|
| D1 | Módulos | `messaging-inbox` (Java puro: envelope, pipeline, retry, loop sobre `MessageReceiver`) e `messaging-inbox-spring-boot-starter`. Os dois entram no `messaging-bom` | Sem Kafka, a separação `inbox-core`/`inbox-messaging`/`inbox-testkit` não tem segundo adapter que a justifique. O SPI `BrokerAdapter` e o `BrokerAdapterContract` saem, porque teriam uma implementação só. O `Outcome` fica, porque separa a decisão do efeito e facilita o teste |
| D2 | Pacote e propriedades | `com.example.messaging.inbox`; prefixo `inbox.*` (ADR-0008), que referencia `messaging.destinations` | `messaging.*` já é do starter atual. `inbox.*` mantém os ADRs válidos |
| D3 | Kafka | Fica fora. `inbox-kafka` continua no `../inbox-library` ou é abandonado | O `docs/adr/0001-escopo.md` da messaging-lib exclui Kafka de propósito |
| D4 | Docs | `messaging-core/docs/{plan.md,adr/}` vão para `messaging-inbox/docs/`, com a numeração própria. Revisão nos dois ADR-0001 (escopo da messaging-lib e adapters da inbox) e no ADR-0006 (coordenadas) | Os ADRs falam da inbox, não do core. A numeração 0001–0017 colide com a de `docs/adr` |
| D5 | `../inbox-library` | O código de `retry` é copiado com o pacote novo, sem preservar histórico (o histórico lá tem 3 commits). O repo antigo ganha um README apontando para cá e é arquivado | `git subtree` para 3 commits não compensa |
| D6 | Precedência do `traceparent` | Revisar o ADR-0007: o `headers.traceparent` do body da outbox vence o do transporte, como no engine-tax | O ADR-0007 diz o contrário (transporte primeiro). Ordem nova: body da outbox, CloudEvents, transporte |
| D7 | Liveness | Gauge novo `inbox_last_receive_age_seconds{consumer}`, adicionado ao ADR-0007 | Veio de um incidente real (T-88/B20) e não tem equivalente no ADR-0007 |

## Fatias da F2

Cada fatia fecha com build verde e o próprio teste. A ordem segue as
dependências.

| # | Fatia | Pronto quando |
|---|---|---|
| 0 | Scaffold: módulos, BOM, docs movidos (D4), revisões de ADR (D4, D6, D7) | `mvn verify` verde com os módulos vazios |
| 1 | `OutgoingMessage.copyOf(ReceivedMessage)` no `messaging-core` (item 1 de F1 da messaging-lib); os adapters passam a usá-lo no `deadLetter` | Teste no `CoreModelTest`; contratos dos adapters inalterados |
| 2 | Porte do motor de retry da `../inbox-library` | Os testes portados passam sem mudança de lógica |
| 3 | `InboxMessage`, `TypedMessage`, `delivery()`, envelope em cascata `cloudevents → outbox → raw` (ADR-0003), SPI `PayloadDeserializer` | Fixtures: um body real do engine-tax (`messageId`, `eventType`, `aggregateId`, `aggregateVersion`, `payload`, `headers`), um CloudEvents binário e um raw |
| 4 | Pipeline e `Outcome` (sem idempotência): envelope → nível 1 → classificação → `HandlerResult` (ADR-0014) | Testes unitários da tabela do ADR-0014 e da classificação pela cadeia de causas |
| 5 | Loop (ADR-0009) sobre `InMemoryMessaging`: workers em platform thread, `keepAlive` com orçamento, aplicação do `Outcome`, backoff do `receive` (retryable e não retryable), worker que nunca morre, shutdown em 4 passos. O `SmartLifecycle` fica para o starter | Casos: exceção no `receive` não mata o worker; shutdown com mensagem em mãos devolve por `Release`; `ack` com lease vencido |
| 6 | Nível 2: `broker-delay`, `hold`, `retry-queues`, escolha `auto` pelas `Capabilities`, contagem `l2` (`inbox_attempts` + `deliveryCount` + `hold`), validação no startup | Matriz das três estratégias no broker em memória; `strict` com `hold`; startup falha nas combinações do ADR-0009 |
| 7 | `failed` e DLQ como destinos comuns, cópias com `deduplicationId`, `inbox_attempts`/`inbox_failure`/`dead_letter`, orçamento de 16 atributos, cópia antes do `ack` | Crash simulado entre a cópia e o `ack` gera duplicata, nunca perda; `NonRetryable` com uma execução só |
| 8a | Owner token em `/Users/sartre.brasil/Sources/pessoal/idempotency-multi-store` (ver Pré-requisitos) | Contrato de store com owner token verde em memory, JDBC e Redis |
| 8 | Idempotência (ADR-0004) | Depende da 8a. Duplicata, `in_progress` concorrente, conflito de fingerprint, ordem `reserve → handler → complete → ack` |
| 9 | Starter: `inbox.defaults`/`inbox.consumers` validados (ADR-0008), `@InboxListener` e modelo funcional, Jackson 3, Micrometer Observation `inbox.consume`, métricas `inbox_*` e o gauge D7, span `CONSUMER` filho do trace | Teste com contexto Spring e broker em memória; trace id preservado de ponta a ponta |
| 10 | Migração do engine-tax (ver abaixo) | lifecycle e outcome-api consumindo pelo starter; dashboards e alarmes atualizados |

Ficam para depois, como no plano da inbox: nível 3 e redrive (F5); circuit
breaker, batch, stale check, expiração e lanes (F6); a matriz dos seis
provedores nos emuladores (F3).

## Pré-requisitos fora deste repo

| Repo | Item | Bloqueia |
|---|---|---|
| idempotency-multi-store (fatia 8a, feita por nós) | Owner token em `reserve`/`complete`/`release` (Redis com Lua, JDBC com `WHERE owner = ?`), transação `join`/`requires-new` no JDBC, `spring-web` opcional no starter | Fatia 8 |
| outbox-library | Headers CloudEvents binários | Nada: o fallback para o body da outbox cobre o engine-tax |

## Migração do engine-tax (fatia 10)

Mudanças que o engine-tax vai sentir:

1. **Fila `failed` obrigatória** por consumer. Também a DLQ como destino comum,
   se o nível 3 vier depois. Mexe no `localstack-init.sh` e na infra
   implantada.
2. **`maxReceiveCount` do SQS**: hoje é 5. Com `broker-delay`, a inbox precisa
   de `1 + delayed.max-attempts` entregas (6 com os padrões), mais as de
   `Release`. O ADR-0002 recomenda o dobro: 12.
3. **Retry**: o `nack(retryAfter)` fixo vira o nível 1 (3 tentativas em
   memória) mais o nível 2 com backoff exponencial (30 s a 15 min).
4. **Métricas**: `xmlplatform.consumer.messages`, `.handle.duration` e
   `.last.receive.age` passam a ser `inbox_messages_processed_total`,
   `inbox_processing_duration` e `inbox_last_receive_age_seconds`. Os
   dashboards e alarmes de `docs/metricas.md` precisam acompanhar.
5. **Threads**: virtual thread vira platform thread (ADR-0009). É indiferente
   no SQS.
6. **Padrões do `receive`**: de 10 mensagens / 10 s para 1 / 20 s.
   Configurar `max-messages-per-poll: 10` e `poll-wait: 10s` para manter o
   comportamento atual.
7. **Handler**: `Consumer<Event>` vira `InboxHandler<T>` ou `@InboxListener`.
   O `Event` do engine-tax é coberto por `InboxMessage` (`id`, `type`,
   `subject`, `aggregateVersion`, `payload`).
8. **Tracing**: o `Traces.continued` dá lugar à Observation da inbox, com o
   comportamento de D6.

## Tarefas

Cada tarefa é um commit (Conventional Commits) com build verde. "Teste" é o que
prova a tarefa. As dependências entre fatias são as da tabela acima. 8a corre
em paralelo com 1 a 7.

### Fatia 0: scaffold

| # | Tarefa | Teste |
|---|---|---|
| T0.1 | Módulos `messaging-inbox` (depende de `messaging-core`; `messaging-testkit` em `test`) e `messaging-inbox-spring-boot-starter` com `pom.xml` e `package-info.java`; registrar no `pom.xml` raiz e no `messaging-bom` | `mvn -q verify` verde |
| T0.2 | `git mv messaging-core/docs/{plan.md,adr,incorporacao.md}` para `messaging-inbox/docs/`; corrigir links relativos | Nenhum link quebrado (`grep` dos `](` contra os arquivos) |
| T0.3 | Revisões: ADR-0001 da messaging-lib (`docs/adr/0001-escopo.md`: o loop passa a ter módulo próprio, fora do core); ADR-0001 e ADR-0006 da inbox (dois módulos, sem Kafka, sem `BrokerAdapter`, coordenadas `com.example:messaging-inbox*`, pacote `com.example.messaging.inbox`) | Revisão por leitura |
| T0.4 | ADR-0007: ordem do `traceparent` (body da outbox, CloudEvents, transporte) e gauge `inbox_last_receive_age_seconds{consumer}` | Revisão por leitura |
| T0.5 | `plan.md` da inbox: fases reescritas para esta incorporação (F3 só com os provedores da messaging-lib; F4 Kafka removida) e entrada em Revisões | Revisão por leitura |

### Fatia 1: `OutgoingMessage.copyOf`

| # | Tarefa | Teste |
|---|---|---|
| T1.1 | `OutgoingMessage.copyOf(ReceivedMessage)`: body, atributos do usuário, `contentType`, `orderingKey`, `traceparent`; `deduplicationId` e `deadLetter` vazios, decididos por quem chama (os adapters já diferem entre si; a inbox põe `deduplicationId = messageId` na fatia 7) | `CoreModelTest`: campos copiados, `deduplicationId` e `deadLetter` vazios |
| T1.2 | Usar o `copyOf` no `deadLetter` dos receivers que montam a cópia à mão (SQS, Pub/Sub, JMS, RabbitMQ, `InMemoryMessaging`); conferir o caso do Service Bus (`ServiceBusCodec`) | Contratos dos adapters inalterados (`mvn verify` com Docker) |

### Fatia 2: porte do motor de retry

| # | Tarefa | Teste |
|---|---|---|
| T2.1 | Copiar `HandlerResult`, `NonRetryable`, `SkipImmediate`, `RetryAfterHint`, `PayloadDeserializationException` para `com.example.messaging.inbox` | Compila |
| T2.2 | Copiar `retry/*` (11 classes) e os 7 testes para `com.example.messaging.inbox.retry` | Os 7 testes verdes sem mudar a lógica |
| T2.3 | README do `../inbox-library` apontando para cá; marcar o repo como arquivado | Revisão por leitura |

### Fatia 3: mensagem e envelope

| # | Tarefa | Teste |
|---|---|---|
| T3.1 | `InboxMessage` (id, type, source, time, subject, aggregateType, aggregateVersion, traceparent, headers, payload, destination, `delivery()`) e `Delivery(level, attempt)` | Invariantes do record (nulos, cópia defensiva) |
| T3.2 | Leitor do atributo `inbox_attempts` (`l2=;l3=;redrive=`) e escrita correspondente, tolerante a valor inválido. Já vem pronto no `AttemptState` portado na fatia 2 | `AttemptStateTest`: ida e volta; valor corrompido vira zero (sem log) |
| T3.3 | `EnvelopeReader` com modos `auto`, `cloudevents`, `outbox`, `raw` (ADR-0003); `inbox_*` fora de `headers()` | Fixtures: body real do engine-tax, CloudEvents binário, raw, JSON inválido (vira `PayloadDeserializationException`) |
| T3.4 | `traceparent` na ordem de D6: `headers.traceparent` do body, `traceparent` do CloudEvents, `ReceivedMessage.traceparent` | Um caso para cada fonte e um com as três presentes |
| T3.5 | SPI `PayloadDeserializer` e `TypedMessage<T>` | Deserializer de teste com `String` |

### Fatia 4: pipeline e `Outcome`

| # | Tarefa | Teste |
|---|---|---|
| T4.1 | `Outcome` (`Ack`, `Retry`, `DeadLetter`, `Failed`, `Release`) com `Reason` e `FailureReason` (ADR-0001, ADR-0007) | Compila |
| T4.2 | `InboxHandler<T>` e `InboxResultHandler<T>` | Compila |
| T4.3 | `Pipeline.handle(ReceivedMessage)`: envelope → deserialização → nível 1 (`ImmediateRetrier`) → classificação → `HandlerResult`. `Clock` e aleatoriedade injetados | Tabela do ADR-0014 (`Ack`, `RetryAfter` pula o nível 1, `Reject` vai para `Failed`, `Discard`); `NonRetryable` numa execução só; timeout pontual com sucesso na segunda tentativa; exceção embrulhada classificada pela causa |
| T4.4 | Validação no startup das listas de exceções (classe inexistente ou que não é `Throwable` falha). Já vem pronto no `ThrowableTypes` portado na fatia 2 | `ErrorFingerprintTest` (`com.acme.Typo`) |

### Fatia 5: loop sobre `MessageReceiver` (ADR-0009)

| # | Tarefa | Teste |
|---|---|---|
| T5.1 | `ConsumerSettings` (concurrency, max-messages-per-poll, poll-wait, max-processing, shutdown-timeout) com os padrões do ADR-0008 e `poll-wait` limitado ao `shutdown-timeout` | Padrões e limite |
| T5.2 | `Worker`: platform thread, receiver próprio, `receive`, `keepAlive` por mensagem com orçamento `max-processing x posição`, pipeline em sequência, `KeepAlive` fechado depois do efeito | Mensagem lenta não perde o lease no `InMemoryMessaging` |
| T5.3 | `OutcomeApplier` para `Ack` e `Release`; `LeaseExpiredException` no `ack` vira aviso | `ack` com lease vencido não derruba o worker |
| T5.4 | Erros do `receive`: backoff 1 s, x2, máximo 30 s (retryable) ou 1 min (não retryable), jitter 0.2; o worker nunca morre (guarda do T-88/B20) | Receiver falso que lança `MessagingException` e `RuntimeException`; o worker continua lendo depois |
| T5.5 | Marca de liveness por consumer (instante do último `receive` concluído, vazio ou não), lida depois pelo gauge D7 | O valor avança com fila vazia e para com o `receive` falhando |
| T5.6 | `InboxConsumer.start/stop` com o shutdown em 4 passos (para de buscar, `Release` do que chegou, interrompe esperas, espera até `shutdown-timeout`, fecha receivers) | Shutdown com mensagem em mãos e com handler lento |

### Fatia 6: nível 2

| # | Tarefa | Teste |
|---|---|---|
| T6.1 | Contagem `l2 = inbox_attempts + (deliveryCount - 1) + hold` (ADR-0002) | Cada termo sozinho e combinado |
| T6.2 | `broker-delay`: `nack(delay)` com o teto `maxRedeliveryDelay`; `nack` com `IllegalArgumentException` vai para a DLQ; sem `deliveryCount`, `inbox_retry_skipped` e DLQ | Entregas atrasadas no `InMemoryMessaging`; esgota e vai para a DLQ |
| T6.3 | `hold`: espera com o lease renovado e reexecuta o pipeline no mesmo worker; interrompível pelo shutdown | Sucesso na terceira execução; shutdown durante a espera devolve por `Release` |
| T6.4 | `retry-queues`: menor faixa com `delay` >= atraso; cópia com `inbox_attempts`, depois `ack` | Faixa escolhida; cópia na fila certa |
| T6.5 | Escolha `auto` pelas `Capabilities` e validação no startup (tabela do ADR-0009); log da estratégia escolhida e do motivo | Todas as linhas das duas tabelas |

### Fatia 7: DLQ e `failed`

| # | Tarefa | Teste |
|---|---|---|
| T7.1 | Cópia para DLQ e `failed` pelo `copyOf`, com `inbox_attempts`, `inbox_failure` (fingerprint do ADR-0017, corte em 1024 bytes) e `withDeadLetter`; `include-exception-message` | Atributos esperados; corte do `inbox_failure` |
| T7.2 | Orçamento de 16 atributos: sai `inbox_failure`, depois `inbox_attempts` (com a consequência do ADR-0002) | Produtor com 14, 15 e 16 atributos |
| T7.3 | Ordem cópia → `ack`; falha na cópia vira `nack(0)`; `failed` obrigatória; sem `dead-letter`, `DeadLetter` vira `Failed`. Cópia que não pode ser montada (`orderingKey` recebido fora das regras de envio, corpo vazio): log de erro e `Release`; a política nativa de DLQ encerra (ADR-0002, revisão de 2026-11-05). `deduplicationId` inválido vira SHA-256 | Sender que falha: a mensagem volta e não se perde; o crash entre cópia e `ack` gera duplicata |

### Fatia 8a: owner token na idempotency-multi-store

Repo: `/Users/sartre.brasil/Sources/pessoal/idempotency-multi-store`.

| # | Tarefa | Teste |
|---|---|---|
| T8a.1 | `IdempotencyRecord` com `owner`; `complete`/`release` recebem o owner e não fazem nada (ou sinalizam) quando o dono é outro | `AbstractIdempotencyStoreContractTest` ganha o caso "reserva expirada e reassumida: o `complete` do dono antigo não apaga" |
| T8a.2 | `InMemoryIdempotencyStore` e `IdempotencyEngine` com owner | Contrato verde no store em memória |
| T8a.3 | JDBC com `WHERE owner = ?` e migração da coluna; modo de transação `join`/`requires-new` | Contrato verde com Postgres (Testcontainers) |
| T8a.4 | Redis com script Lua atômico | Contrato verde com Redis (Testcontainers) |
| T8a.5 | Mongo com filtro por owner | Contrato verde com Mongo (Testcontainers) |
| T8a.6 | `spring-web` opcional no starter. Feita com a fatia 9: `ResponseEntity` numa classe carregada só com `spring-web` presente; beans web numa configuração com `@ConditionalOnClass` | `WithoutSpringWebTest`: contexto sobe sem `spring-web` no classpath |

### Fatia 8: idempotência na inbox

| # | Tarefa | Teste |
|---|---|---|
| T8.1 | Middleware sobre o `IdempotencyEngine`: chave = `InboxMessage.id`, escopo = nome do consumer, fingerprint SHA-256 do payload | Duplicata vira `Ack` sem handler; conflito vira `Failed(idempotency_conflict)` |
| T8.2 | Ordem `reserve → handler → complete → ack`; `release` na falha e antes do `ack` da cópia | Falha do handler libera a chave; cópia para a `failed` libera a chave |
| T8.3 | `IN_PROGRESS` vira `Retry` pela estratégia do nível 2 | Duas instâncias concorrentes na mesma mensagem |
| T8.4 | Validação: TTL de reserva maior que `max-processing` (+ `hold`); aviso com `envelope: raw` | Startup falha; log de aviso |

### Fatia 9: starter

| # | Tarefa | Teste |
|---|---|---|
| T9.1 | `InboxProperties` (`inbox.defaults`, `inbox.consumers`, `configuration` do broker `messaging`) com precedência e chaves protegidas ou desconhecidas falhando (ADR-0008). `delayed.max` não configurado vira `min(15m, maxRedeliveryDelay)` (senão todo consumer do Pub/Sub falha no startup); `deadLetterEnabled` vem de haver `dead-letter` | `ApplicationContextRunner` com os casos de erro; Pub/Sub sem `delayed.max` sobe |
| T9.2 | Autoconfig: um `InboxConsumer` por consumer, receivers e senders do `MessagingDestinations`, `SmartLifecycle` parando antes da messaging-lib. O mesmo `StopSignal` no `Pipeline` e no `InboxConsumer`; `start` depois de `stop` reconstrói o consumer (o `StopSignal` não reinicia). Transação do ADR-0004 com JDBC em `join`: hoje uma reserva cobre o nível 1 inteiro, e uma tentativa que falha faz rollback da transação; ou o guard roda dentro de cada tentativa, ou o nível 1 fica desligado nesse modo | Contexto com `InMemoryMessaging` consome uma mensagem; `stop`/`start` do contexto volta a consumir |
| T9.3 | Modelo funcional (bean `InboxHandler` ligado pelo nome do consumer) | Contexto com bean funcional |
| T9.4 | `@InboxListener` (método com `T` e `InboxMessage`, retorno `void` ou `HandlerResult`) | Contexto com listener anotado |
| T9.5 | `PayloadDeserializer` com o `JsonMapper` (Jackson 3) da aplicação | Payload do engine-tax desserializado |
| T9.6 | Observation `inbox.consume` e métricas `inbox_*` da F2 (received, processed por `outcome`, retry, dead_letter, failed, duration, message_age, effect_failed, retry_skipped, attribute_budget_exceeded) e o gauge `inbox_last_receive_age_seconds` | `SimpleMeterRegistry`: um `received` e um `processed` por entrega |
| T9.7 | Span `CONSUMER` filho do contexto recebido, ativo durante o handler e os retries do nível 1; cópias levam o `traceparent` original | Trace id preservado de ponta a ponta com um handler que publica |
| T9.8 | Validação dos `Capabilities` dos destinos no startup (fatias 6 e 7) ligada às properties | Startup falha com `failed` ausente |

### Fatia 10: migração do engine-tax

Repo: `/Users/sartre.brasil/Sources/pessoal/engine-tax`.

| # | Tarefa | Teste |
|---|---|---|
| T10.1 | Filas `failed` (e DLQ comum) no `localstack-init.sh`; `maxReceiveCount` de 5 para 12 | Ambiente local sobe com as filas |
| T10.2 | Infra implantada com as mesmas filas e o `maxReceiveCount` novo | Plano da infra revisado |
| T10.3 | `inbox.consumers` do lifecycle e do outcome-api (`max-messages-per-poll: 10`, `poll-wait: 10s`, `failed`); handlers de `Consumer<Event>` para `InboxHandler<T>` | Testes de integração dos dois serviços verdes |
| T10.4 | Remover `QueueConsumer`, `QueueConsumerTest` e a montagem no `MessagingConfiguration` | Build verde sem eles |
| T10.5 | Dashboards e alarmes de `docs/metricas.md` para os nomes `inbox_*` | Painéis mostrando dado no ambiente local |

## Registro de execução

| Fatias | Data | Notas |
|---|---|---|
| 0–3 | 2026-11-05 | Contratos do refactor do `copyOf` verdes (SQS/SNS, Pub/Sub, Artemis, Classic, RabbitMQ), rodados junto com a 4–7. `ServiceBusSubscriptionContractTest.deadLetterMovesMessageToDeadLetterQueue` falha também na `main` (emulator; não é desta branch) |
| 4–7 | 2026-11-05 | Código das fatias 5 a 7 num commit só (as classes dependem umas das outras para compilar). Sem métricas: o `OutcomeApplier` devolve `Applied` (efeito, nível 2 pulado, orçamento excedido) para a fatia 9. O `SmartLifecycle` fica para o starter |
| 8a, 8 | 2026-11-05 | idempotency-multi-store na branch `feat/owner-token` (4 commits, com a T8a.6; integrada na `main` dela por fast-forward em 2026-11-05, sem push): owner token, expiração do concluído e modo `join` no JDBC; contrato verde em memória, JDBC, Redis e Mongo (Testcontainers). Além do plano: `complete` com nova expiração, para os dois TTLs do ADR-0004. Na inbox: store direto em vez do engine, `release` antes do efeito (ADR-0004, revisão de 2026-11-05). O primeiro commit da idempotency-multi-store não compila o starter sozinho; o segundo corrige |
| 9 | 2026-11-05 | Starter: properties mescladas e validadas, autoconfig, `InboxLifecycle` (paradas em paralelo, reconstrução no restart), handlers funcional e `@InboxListener`, Jackson 3, Micrometer e tracing (`ConsumerObserver` novo no core), limite de `delayed.max`, idempotência pelo bean `IdempotencyStore`. 21 testes no starter, 109 no core. Tracing testado com o bridge OTel |
| 9+ | 2026-11-05 | `key-lanes` (ADR-0016) antecipado da F6 a pedido do usuário, para o engine-tax manter o paralelismo da T-128: lanes por `orderingKey`, em paralelo, e o próximo `receive` espera todas |
| 10 | 2026-11-05 | engine-tax, branch `feat/inbox-consumer` (1 commit, sem merge): `InboxHandler<JsonNode>` por consumer montando o mesmo `Event` (serviços intactos), `QueueConsumer` removido, `key-lanes: 10` com 10 mensagens por poll (paralelismo da T-128), filas `-failed`, DLQ da inbox = a `-dlq` existente, `maxReceiveCount` 12, `poll-wait` 10 s e `shutdown-timeout` 15 s (dentro dos 30 s do pod), `inbox.enabled` ligado às flags `lifecycle.consumer`/`outcome.consumer`, alertas, painéis, alarmes e docs com `inbox_*`. Sem idempotência (handlers já idempotentes). Não foi rodado contra a stack local (havia pods em uso); o caminho SQS real tem o `SqsLocalStackTest` |

## Pendências conhecidas

- `hold` esperando `in_progress` não tem limite próprio: só termina quando a
  reserva do outro dono vence, e a validação do startup põe esse TTL acima do
  orçamento do `keepAlive`. Se o dono caiu, o worker segura a mensagem sem lease
  até lá. Limite simples: `Release` quando as esperas passarem do orçamento do
  `hold`.
- Transação do ADR-0004 com o JDBC em `join` não está ligada: o starter não abre
  transação em volta do handler, e com uma transação só, uma reserva cobre o
  nível 1 inteiro. Sem transação em volta, o store em `join` se comporta como
  `requires-new`. Fica para depois da migração do engine-tax.
- `spring.lifecycle.timeout-per-shutdown-phase` (padrão 30 s do Spring) precisa
  ser maior que `shutdown-timeout` mais `poll-wait`; senão o Spring segue o
  shutdown com workers ainda rodando. Registrar no README do starter.
- O `InboxDestinations` padrão sobre um `MessagingDestinations` real só tem
  teste com SQS (`SqsLocalStackTest`, LocalStack); os outros provedores ficam
  para a F3.
- engine-tax: as filas `-failed` e o `maxReceiveCount` 12 só existem em
  LocalStack novo (o `localstack-init.sh` roda na criação do container). Com o
  LocalStack atual, criar as filas e trocar a redrive antes de implantar.
- engine-tax: o `OldestMessageSeconds` (5 min) do CloudWatch dispara junto com
  o retry de nível 2 a partir da 4ª tentativa; o limite ficou como estava.
- Mover da DLQ de volta com `start-message-move-task` mantém `inbox_attempts`:
  a mensagem volta só com o nível 1. O redrive que zera as tentativas é a F5.
