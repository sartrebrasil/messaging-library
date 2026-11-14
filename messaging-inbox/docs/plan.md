# Plano da messaging-inbox

Consumo de mensagens com a mesma semântica de retry, backoff, DLQ, idempotência e observabilidade em todos os
provedores da messaging-lib: SQS, SNS (via SQS), Service Bus, Pub/Sub, ActiveMQ (Classic e Artemis) e RabbitMQ. É o
lado consumidor da outbox-library.

Como usar e configurar: [README da inbox](../README.md). Este arquivo guarda as decisões, o estado das fases e o que
falta.

## Decisões

| Tema | Decisão | ADR |
|---|---|---|
| Base técnica | `messaging-inbox` (Java puro) sobre o `MessageReceiver` da messaging-lib; Spring no `messaging-spring-boot-starter`; pipeline síncrono; sem Kafka | [0001](adr/0001-adapters-messaging-lib-e-spring-kafka.md) |
| Retry | Três níveis (memória, atraso, passada lenta na DLQ), estratégia do nível 2 por capability, fila `failed` para falha definitiva; `max-attempts` não conta a primeira execução | [0002](adr/0002-retry-em-tres-niveis.md) |
| Envelope | CloudEvents binário, fallback para body da outbox, fallback raw | [0003](adr/0003-contrato-de-envelope.md) |
| Idempotência | `IdempotencyStore` da idempotency-multi-store, chave = id do envelope, escopo = receiver (compartilhado com nível 3 e redrive) | [0004](adr/0004-idempotencia.md) |
| Ordem | `strict`/`relaxed` por receiver, validado pelas `Capabilities` | [0005](adr/0005-ordenacao.md) |
| Coordenadas | `com.example:messaging-inbox`, pacote `com.example.messaging.inbox`, Java 21, Boot 4, Apache-2.0 | [0006](adr/0006-coordenadas-licenca-publicacao.md) |
| Observabilidade | Micrometer Observation `inbox.consume`, métricas `inbox_*`, gauge `inbox_last_receive_age_seconds` | [0007](adr/0007-observabilidade.md) |
| Configuração | `messaging.receivers`, `messaging.receiver-defaults` e `messaging.receivers-enabled`, validados no startup | [0008](adr/0008-configuracao-de-brokers-e-consumers.md) |
| Loop de polling | Um receiver e uma platform thread por worker, `keepAlive` por entrega, shutdown em quatro passos | [0009](adr/0009-loop-de-polling-sobre-message-receiver.md) |
| Circuit breaker | Pausa o `receive` em vez de falhar mensagens; gancho no core | [0010](adr/0010-circuit-breaker.md) |
| Batch | Handler em batch com resultado por mensagem, `bisect` opcional, `max-wait` | [0011](adr/0011-handler-em-batch.md) |
| Redrive | `failed`/DLQ → principal/DLQ, zera tentativas, filtro e relatório por fingerprint, limite de taxa; API Java e endpoint Actuator desligado por padrão | [0012](adr/0012-redrive.md) |
| Stale check | Última `aggregateVersion` por aggregate; memória, JDBC e Redis | [0013](adr/0013-stale-check.md) |
| Resultado do handler | `HandlerResult` (`RetryAfter`, `Reject`, `Discard`); cadeia de causas, `@NonRetryable`, `@SkipImmediate`, `RetryAfterHint`, allowlist | [0014](adr/0014-resultado-do-handler-e-classificacao.md) |
| Expiração | Descarte por idade (`max-age`) antes da idempotência | [0015](adr/0015-expiracao-por-idade.md) |
| Lanes por chave | `key-lanes`: paralelismo por `orderingKey` dentro de um `receive` | [0016](adr/0016-lanes-por-chave.md) |
| Fingerprint de erro | Causa raiz + 5 frames sem linha, em `inbox_failure`; nunca em métrica | [0017](adr/0017-fingerprint-de-erro.md) |
| Modelo de programação | `@MessageListener` e bean funcional (`InboxHandler`/`InboxResultHandler`) | ADR-0008, revisão de 2026-11-06 |
| Reativo e Kafka | Fora do escopo | ADR-0001 |

## API

Existe hoje (pacote `com.example.messaging.inbox`, starter em `com.example.messaging.inbox.spring`):

```java
@MessageListener(receiver = "billing-orders")            // void ou HandlerResult; T, (T, InboxMessage) ou TypedMessage<T>
void on(OrderCreated event, InboxMessage message) { ... }

public interface InboxHandler<T> { void handle(TypedMessage<T> message) throws Exception; }
public interface InboxResultHandler<T> { HandlerResult handle(TypedMessage<T> message) throws Exception; }

public sealed interface HandlerResult { Ack, RetryAfter(delay, cause), Reject(reason, cause), Discard(reason) }
public sealed interface Outcome { Ack, Retry, DeadLetter, Failed, Release }     // decisão do pipeline, interna

public interface InboxRedrive { RedriveReport redrive(RedriveRequest request); }
public interface PayloadDeserializer { ... }                // Jackson 3 no starter
```

Ordem do pipeline implementada:

```
observation -> envelope -> desserialização -> idempotência (reserve) -> nível 1 [ handler -> classificação ]
            -> complete | release -> efeito no broker (cópia antes do ack)
```

Planejado (F6), ainda sem código: `ConsumptionGate` (circuit breaker, entre o nível 1 e o handler),
`InboxBatchHandler` + `BatchResult`, `AggregateVersionStore` (stale check, depois da idempotência) e expiração (antes
da idempotência). As chaves `circuit-breaker`, `batch`, `stale-check`, `expiration` e `retry.dead-letter.concurrency`
existem nas properties só para o startup falhar com uma mensagem clara.

Configuração: [`exemplos/application-completo.yaml`](exemplos/application-completo.yaml), validada pelo
`DocumentedConfigTest`.

## Fases

Desde 2026-11-05 a inbox mora na messaging-library (ADR-0001 e ADR-0006, revisões). Kafka (`inbox-kafka`) saiu do
escopo e os módulos `inbox-*` viraram `messaging-inbox`; em 2026-11-06 o starter próprio foi incorporado ao
`messaging-spring-boot-starter`.

| Fase | Entrega | Status |
|---|---|---|
| F0 | Fundação, ADRs aceitos | Feita em 2026-10-08 |
| F1 | Pré-requisitos nas libs irmãs | Parcial: idempotency-multi-store com owner token, expiração do concluído e modo `join` (2026-11-05); `OutgoingMessage.copyOf` no core. Pendentes os itens da outbox-library (abaixo) |
| F2 | Core + starter: loop, níveis 1 e 2, `failed`, classificação, `HandlerResult`, idempotência, envelope, anotação e funcional, métricas e tracing | Feita em 2026-11-05, com `key-lanes` (ADR-0016) antecipado da F6 |
| F3 | Contrato nos seis provedores e nas três estratégias do nível 2; DLQ nativa medida | Feita em 2026-11-06: `InboxContract` verde no InMemory e em 7 subclasses (SQS, Service Bus fila e subscription, Pub/Sub, Classic, Artemis, RabbitMQ) |
| F4 | `inbox-kafka` | Fora do escopo |
| F5 | Nível 3, fingerprint e redrive | Feita em 2026-11-07/08: `DeadLetterPass`, `InboxRedrive`, endpoint `inboxredrive`, métricas, contrato nos seis brokers (Pub/Sub sem origem legível para redrive) |
| F6 | Circuit breaker, batch, stale check, expiração | **Pendente.** Circuit breaker em rascunho, pausado na Seção 1: [`../../docs/superpowers/specs/2026-11-08-f6-circuit-breaker-design.md`](../../docs/superpowers/specs/2026-11-08-f6-circuit-breaker-design.md) |
| F7 | Release 0.1.0 | **Pendente.** README com tabela de DLQ nativa, topologia do RabbitMQ e permissões já escrito; faltam o exemplo outbox → inbox ponta a ponta e a tag |
| F8+ | Claim-check (storage-lib), virtual threads com Java 24+, circuit breaker distribuído, CLI de redrive | Futuro |

## Pendências

Na messaging-library:

- Transação do ADR-0004 com o JDBC em `join` não está ligada: o starter não abre transação em volta do handler, e com
  uma transação só, uma reserva cobriria o nível 1 inteiro. Sem transação em volta, o store em `join` se comporta como
  `requires-new`.
- Pelo starter (`InboxDestinations` sobre um `MessagingDestinations` real), só o SQS tem teste ponta a ponta
  (`SqsLocalStackTest`); os outros brokers são cobertos no nível dos adapters pelo `InboxContract`.
- `retry.dead-letter.concurrency`: as passadas do nível 3 processam uma mensagem por vez.
- Documentar por adapter quando `enqueuedAt` vem nulo (o filtro por data do redrive e o nível 3 usam o campo).
- Confirmar a afinidade de `orderingKey` no Pub/Sub com vários receivers de pull unário (ADR-0005).
- Padronizar nomes, corrigir e completar as métricas: [backlog de métricas](../../docs/metricas-backlog.md).

Na outbox-library (F1):

1. Escrever headers CloudEvents binários (ADR-0003) com no máximo 14 atributos por mensagem (orçamento do ADR-0002).
   Não bloqueia: o fallback para o body da outbox cobre o engine-tax.
2. Aplicar `max-attempts` (hoje ignorado) e corrigir a tag `broker` fixa em `"core"` em `outbox_published_total`.
3. Gerar fixtures de mensagens publicadas para o teste de compatibilidade da inbox.
4. `SpringOutboxWriter` dentro de um handler gravar o `traceparent` do span corrente, mais `tracestate` e `baggage`.

No engine-tax:

- Branch `feat/inbox-consumer` (migração do `QueueConsumer` para a inbox) e, por cima, `refactor/messaging-config`
  (configuração na forma `messaging.receivers`). Sem merge nem implantação; a primeira sozinha usa as chaves
  `inbox.*`, que hoje derrubam o startup.
- As filas `-failed` e o `maxReceiveCount` 12 só existem em LocalStack novo (o `localstack-init.sh` roda na criação do
  container). Com o LocalStack atual, criar as filas e trocar a redrive policy antes de implantar; o mesmo na infra.
- O alarme `OldestMessageSeconds` (5 min) do CloudWatch dispara junto com o nível 2 a partir da 4ª tentativa; o limite
  ficou como estava.

## Revisões

- 2026-10-01 a 2026-10-08: plano inicial, ADRs 0001 a 0017 propostos, revisados e aceitos (histórico em cada ADR).
- 2026-11-05: inbox incorporada na messaging-library, sem Kafka e sem SPI de adapter; `traceparent` do body da outbox
  primeiro; gauge `inbox_last_receive_age_seconds`. Fatias da F2 executadas no mesmo dia.
- 2026-11-06: configuração unificada em `messaging.*` e um starter só (ADR-0008); F3 feita.
- 2026-11-07/08: F5 feita (nível 3 e redrive).
- 2026-11-09: documentação revista; README da inbox como guia de uso; plano de incorporação e planos executados
  removidos (o conteúdo vive nos ADRs, no README e nas pendências acima).
