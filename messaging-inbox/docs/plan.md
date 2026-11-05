# Plano da library

Biblioteca de consumo de mensagens para Spring Boot, com a mesma semântica de
retry, backoff, DLQ, idempotência e observabilidade em Kafka e em todos os
provedores da messaging-lib: SQS, SNS (via SQS), Service Bus, Pub/Sub,
ActiveMQ (Classic e Artemis) e RabbitMQ. É o lado consumidor da
outbox-library.

## Decisões

| Tema | Decisão | ADR |
|---|---|---|
| Base técnica | `inbox-messaging` sobre a messaging-lib; `inbox-kafka` sobre spring-kafka; core sem Spring; SPI e pipeline síncronos | [0001](adr/0001-adapters-messaging-lib-e-spring-kafka.md) |
| Retry | Três níveis (memória, atraso, passada lenta na DLQ), estratégia do nível 2 por capability, fila `failed` para falha definitiva; `max-attempts` não conta a primeira execução | [0002](adr/0002-retry-em-tres-niveis.md) |
| Envelope | CloudEvents binário, fallback para body da outbox, fallback raw | [0003](adr/0003-contrato-de-envelope.md) |
| Idempotência | `IdempotencyEngine` direto, chave = id do envelope, escopo = consumer (compartilhado com nível 3 e redrive) | [0004](adr/0004-idempotencia.md) |
| Ordem | `strict`/`relaxed` por consumer, validado pelas `Capabilities` | [0005](adr/0005-ordenacao.md) |
| Coordenadas | `com.example:inbox-*`, Java 21, Boot 4, Apache-2.0 | [0006](adr/0006-coordenadas-licenca-publicacao.md) |
| Observabilidade | Micrometer Observation `inbox.consume`, métricas `inbox_*`, tracing OpenTelemetry | [0007](adr/0007-observabilidade.md) |
| Configuração | `inbox.brokers` (`kafka`, `messaging`), `inbox.defaults` e `inbox.consumers`, validados no startup | [0008](adr/0008-configuracao-de-brokers-e-consumers.md) |
| Loop de polling | Um receiver e uma platform thread por worker, `keepAlive` por entrega, shutdown em quatro passos | [0009](adr/0009-loop-de-polling-sobre-message-receiver.md) |
| Circuit breaker | Pausa o `receive` em vez de falhar mensagens; gancho `ConsumptionGate` no core, Resilience4j no módulo opcional `inbox-resilience4j` | [0010](adr/0010-circuit-breaker.md) |
| Batch | `InboxBatchHandler` com `BatchResult` por mensagem, `bisect` opcional, `max-wait`; pipeline continua por mensagem | [0011](adr/0011-handler-em-batch.md) |
| Redrive | `failed`/DLQ → principal/DLQ, zera tentativas, filtro e relatório por fingerprint, limite de taxa; API Java e endpoint Actuator desligado por padrão | [0012](adr/0012-redrive.md) |
| Stale check | Última `aggregateVersion` por aggregate; memória, JDBC e Redis | [0013](adr/0013-stale-check.md) |
| Resultado do handler e classificação | `HandlerResult` (`RetryAfter`, `Reject`, `Discard`); cadeia de causas, `@NonRetryable`, `@SkipImmediate`, `RetryAfterHint`, allowlist | [0014](adr/0014-resultado-do-handler-e-classificacao.md) |
| Expiração | Descarte por idade (`max-age`) antes da idempotência; padrão pelo instante do evento | [0015](adr/0015-expiracao-por-idade.md) |
| Lanes por chave | `key-lanes`: paralelismo por chave dentro de um `receive`/`poll` | [0016](adr/0016-lanes-por-chave.md) |
| Fingerprint de erro | Causa raiz + 5 frames sem linha, em `inbox_failure`; nunca em métrica | [0017](adr/0017-fingerprint-de-erro.md) |
| Modelo de programação | Anotação e funcional, os dois na v1 | este plano |
| Reativo | Fora do escopo | este plano |

## API alvo (v1)

```java
// Modelo por anotação
@InboxListener(consumer = "billing-orders")
void on(OrderCreated event, InboxMessage message) { ... }

@InboxListener(consumer = "billing-orders-batch", batch = true)
BatchResult on(List<TypedMessage<OrderCreated>> messages) { ... }

// Modelo funcional: o bean é ligado ao consumer pelo nome em inbox.consumers.<nome>
@Bean
InboxHandler<OrderCreated> billingOrders() {
    return message -> billing.register(message.payload());
}
```

```java
// core
public interface InboxHandler<T> { void handle(TypedMessage<T> message) throws Exception; }

public interface InboxResultHandler<T> { HandlerResult handle(TypedMessage<T> message) throws Exception; }

public sealed interface HandlerResult {   // ADR-0014
    record Ack() ...; record RetryAfter(Duration delay, Throwable cause) ...;
    record Reject(String reason, Throwable cause) ...; record Discard(String reason) ...;
}

public interface InboxBatchHandler<T> { BatchResult handle(List<TypedMessage<T>> messages) throws Exception; }

public interface InboxMiddleware { Outcome handle(InboxMessage message, Chain next); }

public sealed interface Outcome {
    record Ack() ...; record Retry(Duration delay, Reason reason) ...;
    record DeadLetter(Throwable cause) ...; record Failed(Throwable cause, FailureReason reason) ...;
    record Release() ...;
}

public interface ConsumptionGate { ... }         // ADR-0010
public interface AggregateVersionStore { ... }   // ADR-0013
public interface InboxRedrive { RedriveReport redrive(RedriveRequest request); }   // ADR-0012

// SPI do adapter
public interface BrokerAdapter { void start(Pipeline pipeline); void stop(Duration timeout); }
```

Ordem fixa do pipeline:

```
observation -> envelope -> expiração -> idempotência -> stale check -> retry nível 1 [ circuit breaker -> handler -> classificação ]
```

Configuração (exemplo):

```yaml
messaging:                         # starter da messaging-lib (ADR-0008 dela)
  providers:
    aws: { type: aws, region: sa-east-1 }
  destinations:
    orders:        { provider: aws, queue-url: https://sqs.sa-east-1.amazonaws.com/123/orders }
    orders-dlq:    { provider: aws, queue-url: https://sqs.sa-east-1.amazonaws.com/123/orders-dlq }
    orders-failed: { provider: aws, queue-url: https://sqs.sa-east-1.amazonaws.com/123/orders-failed }

inbox:
  defaults:                        # comum a todos os consumers (ADR-0008)
    idempotency: { enabled: true, ttl: P7D }
  consumers:
    billing-orders:
      broker: messaging            # implícito quando só ele existe (ADR-0008)
      destination: orders
      dead-letter: orders-dlq
      failed: orders-failed
      concurrency: 4
      ordering: relaxed
      envelope: auto
      retry:
        immediate:   { max-attempts: 3, initial: 100ms, multiplier: 2, max: 2s, jitter: 0.2 }
        delayed:     { max-attempts: 5, initial: 30s, multiplier: 2, max: 15m, jitter: 0.2, strategy: auto }
        dead-letter: { max-attempts: 3, interval: 15m, max-per-pass: 100, concurrency: 1 }
        non-retryable: [com.acme.InvalidOrderException]
        skip-immediate: [com.acme.RateLimitedException]
      stale-check: { enabled: true, store: jdbc }
      expiration: { enabled: false, max-age: 10m }
      circuit-breaker: { enabled: true, name: billing-api }
      batch: { enabled: false, max-size: 10 }
      configuration:               # específico do broker (ADR-0008)
        poll-wait: 20s
        max-processing: 10m
```

## Regras de semântica

| Tema | Regra |
|---|---|
| Níveis | Memória → atraso (`broker-delay`, `retry-queues` ou `hold`, escolhido pelas `Capabilities`) → passada lenta na DLQ → `failed`. `NonRetryable` vai direto para `failed`. Pior caso com os padrões: 36 execuções ([ADR-0002](adr/0002-retry-em-tres-niveis.md)). |
| Backoff | `min(initial * multiplier^attempt, max) * random(1 - jitter, 1)`, jitter padrão 0.2. Não se aplica a `retry-queues` (TTL fixo por faixa). |
| Ack | Só depois do handler e do `complete` da idempotência terem sucesso. |
| Cópias | Envio antes do `ack` (duplicata, nunca perda); `deduplicationId` = `messageId` de origem; atributos `inbox_attempts` e `inbox_failure`; produtor com até 14 atributos. |
| Duplicata | `Ack` sem chamar o handler; `outcome=duplicate`. |
| Stale | `Ack` sem chamar o handler; `outcome=stale`. |
| Expirada | `Ack` (ou `failed`) sem chamar o handler nem o store de idempotência; `outcome=expired`. Mensagens do redrive não expiram. |
| Resultado do handler | `RetryAfter` pula o nível 1; `Reject` vai para `failed`; `Discard` confirma com `outcome=discarded` ([ADR-0014](adr/0014-resultado-do-handler-e-classificacao.md)). |
| `IN_PROGRESS` | `Retry(delay)` pela estratégia do nível 2; conta tentativa só em `broker-delay`. |
| Falha de desserialização | `NonRetryable`, vai direto à `failed` com o body original. |
| DLQ e `failed` | Destinos comuns (nunca a DLQ nativa do Service Bus). Body original intacto. Liberam a chave de idempotência. `failed` é obrigatória. |
| Circuit breaker | Aberto: nenhum `receive`, passada do nível 3 encerrada. Mensagens já em mãos não queimam tentativas onde a inbox controla a contagem. |
| Shutdown | Para de buscar, interrompe esperas, espera o handler até `shutdown-timeout`, fecha os receivers sem confirmar o que não terminou. |
| Observabilidade | Recebida, processada (por `outcome`), retry (por `level` e `strategy`), DLQ, `failed` (por `reason`), circuit breaker, batch, redrive; span `CONSUMER` filho do `traceparent` ([ADR-0007](adr/0007-observabilidade.md)). |

## Testes

- `inbox-testkit`: `BrokerAdapterContract` abstrato. Broker em memória sobre o
  `InMemoryMessaging` do `messaging-testkit`, via `inbox-messaging`.
- `inbox-messaging`: contrato com cada provedor da messaging-lib, nos mesmos
  emuladores dela (LocalStack, Service Bus emulator, Pub/Sub emulator, ActiveMQ
  Classic, Artemis e RabbitMQ com Testcontainers). Matriz por estratégia do
  nível 2: `broker-delay` (SQS, Pub/Sub, Artemis `SCHEDULED`), `hold`
  (Service Bus com sessions, ActiveMQ `IMMEDIATE`), `retry-queues` (RabbitMQ).
- Kafka: Testcontainers com imagem `apache/kafka`.
- Compatibilidade: fixtures de mensagens reais da outbox-library (body e
  headers) lidas pelo envelope.
- Idempotência e stale check: ITs com Postgres (transação em `join`) e Redis.
- Casos obrigatórios do contrato: crash entre cópia e `ack` (duplicata, nunca
  perda); fim de passada do nível 3 pelas três condições; circuito abrindo com
  mensagem no backoff do nível 1; batch com falha no meio em `strict`; redrive
  zerando tentativas; shutdown com mensagem em `hold`; trace id preservado de
  ponta a ponta (handler que grava na outbox).
- Casos de referência herdados da acme-messaging (ARCHITECTURE §14): exceção
  `NonRetryable` com uma execução só; timeout pontual com sucesso na segunda
  tentativa; 429 com `Retry-After` pulando o nível 1; dependência fora com o
  circuito pausando sem gastar tentativas; duplicata concorrente em
  `in_progress`; mensagem expirada; batch de 100 com 3 falhas; `bisect`
  isolando a mensagem envenenada; redrive filtrado por fingerprint; rolling
  deploy sem perda.
- Testes determinísticos: `Clock` e fonte de aleatoriedade injetados, como no
  core da acme-messaging.

## Fases

Desde 2026-11-05 a inbox mora na messaging-library (ADR-0001 e ADR-0006,
revisões de 2026-11-05), e a execução segue as fatias e tarefas de
[incorporacao.md](incorporacao.md). Mudanças nas fases abaixo: F2 inclui o
loop do ADR-0009 sobre o `InMemoryMessaging`; F3 é só a matriz dos provedores
da messaging-lib; F4 (`inbox-kafka`) sai do escopo; os módulos `inbox-*` viram
`messaging-inbox` e `messaging-inbox-spring-boot-starter`.

| Fase | Entrega | Pronto quando | Status |
|---|---|---|---|
| F0 | Fundação do repo | Build verde no CI com módulos vazios; ADRs aceitos | Feita, exceto o CI: build local verde e ADRs aceitos em 2026-10-08; o CI roda no primeiro push (sem remoto ainda) |
| F1 | Pré-requisitos nas libs irmãs | Itens de F1 abaixo com testes verdes | Pendente |
| F2 | Core + testkit + starter | Pipeline completo (níveis 1 e 2, `failed`, classificação e `HandlerResult`, idempotência, envelope, `inbox.defaults`, anotação e funcional, métricas e tracing) passa no contrato com o broker em memória. Ponto de partida: o motor de retry da acme-messaging, portado | Pendente |
| F3 | `inbox-messaging` | Contrato verde com os seis provedores da messaging-lib e as três estratégias do nível 2; valores padrão de DLQ nativa de cada broker conferidos | Pendente |
| F4 | `inbox-kafka` | Contrato verde com Kafka; `strict` e `relaxed` testados | Pendente |
| F5 | Nível 3, fingerprint e redrive | Passadas da DLQ e redrive (API e Actuator, filtro por fingerprint, limite de taxa) verdes no contrato dos dois adapters | Pendente |
| F6 | Circuit breaker, batch, stale check, expiração, lanes | Os cinco verdes no contrato; stale check com JDBC em `join` e Redis; `bisect` e `key-lanes` testados no Kafka e no SQS FIFO | Pendente |
| F7 | Release 0.1.0 | README (com tabela de DLQ nativa, exemplo de topologia do RabbitMQ, policy mínima), exemplo outbox → inbox ponta a ponta, tag | Pendente |
| F8+ | Claim-check (storage-lib), virtual threads com Java 24+, circuit breaker distribuído, CLI de redrive | A detalhar | Futuro |

### F0: Fundação

1. Repo `inbox-library` com parent, `inbox-bom`, `inbox-core`,
   `inbox-testkit`, `inbox-messaging`, `inbox-kafka`, `inbox-resilience4j`,
   `inbox-spring-boot-starter` (os adapters ficam vazios até F3/F4).
2. `LICENSE` Apache-2.0, `README.md` esqueleto em inglês, `.gitignore`.
3. Maven Wrapper; compiler, surefire, failsafe, jar com `Automatic-Module-Name`.
4. GitHub Actions: `mvn verify` com Docker disponível para os ITs.
5. Spike de versões: confirmar spring-kafka 4.x com Spring Boot 4 e a
   convivência com o `messaging-bom` (ordem de BOMs do ADR-0008 da
   messaging-lib) e o suporte do Resilience4j a Spring Boot 4 (ADR-0010).
   Registrar no ADR-0006. Feito em 2026-10-07: Boot 4.1.1, spring-kafka
   4.1.1, Resilience4j 2.4.0, JUnit 6.0.3.
6. Revisar e aceitar os ADRs 0001 a 0017. Feito em 2026-10-08, depois da
   revisão de consistência de 2026-10-07.

Itens 1 a 5 feitos em 2026-10-07. O motor de retry da acme-messaging já está
no `inbox-core` (adiantado da F2). Os outros módulos têm só `pom.xml` e
`package-info.java`; as dependências entram com o código (spring-kafka na F4,
messaging-core na F3, Resilience4j na F6, Spring Boot na F2).

### F1: Pré-requisitos nas libs irmãs

idempotecy-multi-store:

1. Migrar para Spring Boot 4 e Jackson 3.
2. Owner token em `reserve`/`complete`/`release`; contrato de store atualizado;
   Redis com script Lua atômico; JDBC com `WHERE owner = ?`. Conferido em
   2026-10-07: ainda não existe.
3. Modo de transação do JDBC configurável (`join` | `requires-new`).
4. `spring-web` opcional no starter.

outbox-library:

1. Migrar para Spring Boot 4 e Jackson 3.
2. Escrever headers CloudEvents binários (ADR-0003) em Kafka, SQS, SNS e
   RabbitMQ, mantendo o body atual. Nomes no formato da messaging-lib
   (`ce_id`, `ce_type`, ...) e no máximo 14 atributos por mensagem, para caber
   o orçamento da inbox (ADR-0002).
3. Aplicar `max-attempts` (hoje ignorado) e corrigir a tag `broker` fixa em
   `"core"` na métrica `outbox_published_total`.
4. Gerar fixtures de mensagens publicadas para o teste de compatibilidade da
   inbox.
5. Garantir que o `SpringOutboxWriter`, chamado dentro de um handler, grava o
   `traceparent` do span corrente (hoje usa a OpenTelemetry API direto).
   Testar com Micrometer Tracing + bridge OTel: o contexto precisa ser o
   mesmo. Também escrever `tracestate` e `baggage` nos headers (ADR-0007).

messaging-lib:

1. Avaliar `OutgoingMessage.copyOf(ReceivedMessage)` no core: os adapters já
   montam essa cópia no `deadLetter`, e a inbox faria o mesmo em cinco lugares.
2. Documentar por adapter quando `enqueuedAt` vem nulo (o nível 3 usa o campo,
   ADR-0002).
3. Confirmar a afinidade de `orderingKey` no Pub/Sub com vários receivers de
   pull unário (ADR-0005).

storage-lib: sem mudanças até o claim-check (F8+).

## Revisões

- 2026-10-01: plano inicial e ADRs 0001 a 0006 propostos.
- 2026-10-02: ADR-0002 detalha `immediate`, `delayed` e jitter (padrão 0.2).
  Cada reentrega do nível 2 repete o nível 1 (até 24 execuções com os padrões).
- 2026-10-02: ADR-0007 (observabilidade) proposto.
- 2026-10-02: ADR-0007 define a continuidade do trace (contexto corrente no
  handler, headers copiados em retry e DLQ); item 5 da outbox em F1.
- 2026-10-02: ADR-0008 (configuração de brokers e consumers via
  `configuration`) proposto.
- 2026-10-07: ADR-0001 revisado: `inbox-messaging` sobre a messaging-lib
  substitui os adapters SQS, RabbitMQ e JMS sobre containers Spring; Kafka
  continua com spring-kafka.
- 2026-10-07: ADR-0002 revisado: nível 3 (passada lenta na DLQ), fila
  `failed`, estratégias `broker-delay`/`retry-queues`/`hold`, headers
  `inbox_*` no formato da messaging-lib e orçamento de 16 atributos.
- 2026-10-07: ADR-0004 revisado: chave pelo envelope, escopo compartilhado
  com o nível 3 e o redrive, `IN_PROGRESS` conta tentativa em `broker-delay`.
- 2026-10-07: ADRs 0003, 0005 a 0008 ajustados; ADRs 0009 (loop de polling),
  0010 (circuit breaker), 0011 (batch), 0012 (redrive) e 0013 (stale check)
  propostos. Anotação, circuit breaker, stale check, batch e redrive entram na
  v1.
- 2026-10-07: acme-messaging incorporada (nome e coordenadas da inbox-library,
  ADR-0006). ADR-0001: SPI e pipeline síncronos. ADR-0002: `max-attempts` não
  conta a primeira execução. ADR-0010 reescrito: Resilience4j no módulo
  opcional `inbox-resilience4j`, core só com o gancho `ConsumptionGate`.
- 2026-10-07: ADRs 0014 (resultado do handler e classificação), 0015
  (expiração por idade), 0016 (lanes por chave) e 0017 (fingerprint de erro)
  propostos, com origem na acme-messaging. ADR-0008 ganha `inbox.defaults`;
  ADR-0011 troca o resultado por índice pelo resultado por mensagem, com
  `bisect` e `max-wait`; ADR-0012 ganha filtro por fingerprint, limite de taxa
  e `inbox_redrive`.
- 2026-10-07: revisão de consistência dos ADRs 0001 a 0017. Circuit breaker
  dentro do nível 1 na ordem do pipeline; `Outcome.Release` e
  `outcome=circuit_open`; `bisect` divide com qualquer exceção; DLQ e `failed`
  do Kafka por campo explícito; mensagens puladas no redrive voltam para o fim
  da origem; marca de redrive dentro de `inbox_attempts`; `hold` e passada do
  nível 3 no core; orçamento do `keepAlive` por `receive`; `Release` conta
  tentativa onde o provedor conta entregas.
- 2026-10-08: ADRs 0001 a 0017 aceitos. F0 feita, exceto a primeira execução
  do CI, que depende do remoto.
- 2026-11-05: inbox incorporada na messaging-library (`messaging-inbox`,
  `messaging-inbox-spring-boot-starter`), sem Kafka e sem SPI de adapter.
  ADR-0007: `traceparent` do body da outbox primeiro; gauge
  `inbox_last_receive_age_seconds`. Plano de execução em
  [incorporacao.md](incorporacao.md).
