# F5 (parte 1): nível 3, passadas na DLQ

- Data: 2026-11-07
- Fase: F5 do `messaging-inbox/docs/plan.md`, primeiro de dois ciclos (o redrive é o segundo)
- Decisão base: ADR-0002 da inbox, seção "Nível 3"; ADR-0004 (`in_progress` numa passada)
- Abordagem: A ajustada pelo conselho de 2026-11-07 (componente próprio no core, modo passada como parâmetro)

## Objetivo

A DLQ comum deixa de ser só estacionamento: em passadas espaçadas, cada instância relê a DLQ e reprocessa as
mensagens com o mesmo handler, com o nível 1 e sem o nível 2. Funciona no core (sem Spring) e pelo starter, e o
contrato prova o comportamento nos seis brokers.

## Fora do escopo

- Redrive (ADR-0012): segundo ciclo da F5, sobre o mecanismo de passada deste.
- Coordenação entre instâncias: uma passada por instância, sem lock (decidido).
- `dead-letter.concurrency`: fixo em 1; a chave é recusada no startup ("not supported yet").
- "Só começar a passada com o circuit breaker fechado" (ADR-0002): depende da F6.

## Comportamento

### Agendamento

- `DeadLetterPass`, classe package-private em `com.example.messaging.inbox`, criada pelo `InboxConsumer` quando há
  DLQ (`Destinations.deadLetter() != null`) e `RetryPolicy.deadLetterMaxAttempts() > 0`.
- Uma thread de `ScheduledExecutorService` com `scheduleWithFixedDelay(interval)`; a primeira passada começa um
  `interval` depois do `start`. Passadas nunca se sobrepõem. Passada mais longa que o `interval`: `WARN`, e a
  próxima começa um `interval` depois do fim dela.

### Uma passada (`runPass()`)

1. `passStart = clock.instant()`; abre um receiver da DLQ (`Destinations.deadLetterReceivers()`); conjunto de chaves
   vistas nesta passada.
2. Laço: `receive(1, poll-wait)`.
3. Antes de processar, a passada termina — e a mensagem que a encerrou volta com `nack(0)` — quando:
   - `enqueuedAt != null && enqueuedAt >= passStart` (`end=newer_than_pass`);
   - a chave de idempotência (ADR-0004; sem idempotência, o `messageId`) já apareceu nesta passada
     (`end=repeated_key`);
   - `max-per-pass` mensagens já foram processadas (`end=max_per_pass`);
   - o `receive` volta vazio (`end=empty`; sem mensagem a devolver);
   - o consumer está parando (`end=stopping`).
4. Processa: `pipeline.process(message, destination, 0, observer, PassMode.DEAD_LETTER)`. O modo é parâmetro, nunca
   estado do `Pipeline` (compartilhado com os workers da fila principal). No modo passada: `resolveDeadLetterPass`
   no lugar de `resolve`, `Delivery.Level.DEAD_LETTER`.
5. Aplica pelo `OutcomeApplier`:
   - sucesso → `ack`;
   - falha retryable com `l3 < max-attempts` → cópia para o fim da DLQ com `l3 + 1`, depois `ack`;
   - `l3 = max-attempts`, ou falha não retryable → cópia para `failed` (`retries_exhausted`, `non_retryable` etc.),
     depois `ack`;
   - `in_progress` (outra instância com a chave) → cópia para o fim da DLQ **sem** somar `l3` (ADR-0004).
6. `keepAlive` da mensagem em processamento com o orçamento de `max-processing`, como no `Worker`.
7. Fecha o receiver.

A cópia para o fim é uma mensagem nova: o `enqueuedAt` dela é posterior a `passStart` e encerra a passada quando
volta. No SQS standard (sem ordem) a cópia pode voltar antes de outras: a passada termina mais cedo, o que é
benigno. Sem `enqueuedAt` e sem idempotência, o limite é a chave por `messageId`; a cópia tem `messageId` novo, então
o teto real é `max-per-pass` (documentar).

Com os padrões (`max-attempts` 3, `interval` 15 min) uma mensagem que sempre falha fica ~45 min no nível 3 antes de
`failed`.

### Ciclo de vida

- `InboxConsumer.start()`: sobe o `DeadLetterPass` depois dos workers. `stop()`: para o `DeadLetterPass` primeiro —
  sinaliza parada, interrompe o `receive`, espera a mensagem em andamento até `shutdown-timeout`, fecha a thread.
- Receiver com `enabled: false`: nem workers nem passadas.

## Configuração

```yaml
messaging:
  receivers:
    billing-orders:
      dead-letter: orders-dlq
      retry:
        dead-letter: { max-attempts: 3, interval: 15m, max-per-pass: 100 }
```

| Chave | Padrão | Validação |
|---|---|---|
| `retry.dead-letter.max-attempts` | 3 | ≥ 0; `0` = DLQ como estacionamento |
| `retry.dead-letter.interval` | 15m | > 0 |
| `retry.dead-letter.max-per-pass` | 100 | ≥ 1 |
| `retry.dead-letter.concurrency` | — | recusada: "not supported yet" |

- Padrão ligado sempre que há `dead-letter` (ADR-0002). Sem `dead-letter`, nível 3 desligado, esgotamento do nível 2
  vai para `failed` (como hoje).
- Aceito também em `receiver-defaults`, com a mescla por propriedade dos outros blocos.
- Core: `RetryPolicy.deadLetterMaxAttempts` vem da configuração (hoje fixo em 3 no `ConsumerBuilder`);
  `ConsumerSettings` ganha `deadLetterInterval` e `deadLetterMaxPerPass`; `Destinations` ganha
  `Supplier<MessageReceiver> deadLetterReceivers` (o starter usa o nome `<receiver>.dead-letter`, o mesmo do sender da
  DLQ).

## Observabilidade

- Métricas por mensagem pelo mesmo `ConsumerObserver`, com `consumer=<receiver>.dead-letter`; `level=dead_letter` nos
  retries.
- Contador novo `inbox.dead.letter.passes{consumer,end}` (`end` = `empty`, `newer_than_pass`, `repeated_key`,
  `max_per_pass`, `stopping`).
- Log `INFO` por passada: processadas, confirmadas, devolvidas ao fim, enviadas a `failed`, motivo do fim, duração.
  `WARN` quando passa do `interval`. Sem payload.
- Span existente, `inbox.level=dead_letter`.

## Testes

1. `DeadLetterPassTest` (core, InMemory, `runPass()` direto): sucesso; retryable → fim da DLQ com `l3+1`; esgotado →
   `failed` `retries_exhausted`; não retryable → `failed`; `in_progress` → fim sem somar `l3`; cada condição de fim
   (`enqueuedAt`, chave repetida sem `enqueuedAt`, `max-per-pass`, vazia); `stop` no meio; passada longa → `WARN`,
   sem sobreposição.
2. `InboxConsumerTest`: agendamento real com `interval` curto; `max-attempts: 0` sem passadas.
3. Starter: `InboxConfigTest` (chaves, padrões, `concurrency` recusado, inválidos); `InboxAutoConfigurationTest` ponta
   a ponta com o fake.
4. `InboxContract`: `deadLetterPassReprocesses` (mensagem na DLQ com `l2` esgotado; handler conclui; DLQ vazia) e
   `deadLetterPassEndsOnItsOwnCopy` (handler sempre falha; uma passada executa a mensagem uma vez só; cópia com
   `l3=1`) nos seis brokers.
5. `mvn clean install` verde.

## Engine-tax

- Os três receivers já têm `dead-letter`: com o padrão, passam a ter passadas (`max-attempts` 3, `interval` 15m,
  `max-per-pass` 100). Decidido: **ligado no padrão**, sem chave nova no YAML. A DLQ nativa do SQS
  (`maxReceiveCount` 12) segue como rede de segurança.
- Atualizar `docs/metricas.md` (contador de passadas, `consumer=<receiver>.dead-letter`) e o comentário do YAML.

## Documentação

- ADR-0002: revisão de 2026-11-07 — implementação do nível 3, `concurrency` fixo em 1, "uma passada por instância"
  como risco aceito (N pods = até N × `max-per-pass` por intervalo, N leitores na DLQ), dependência do circuit breaker
  adiada para a F6.
- README: nível 3 na seção da inbox; `messaging-inbox/docs/plan.md`: F5 parte 1 feita.

## Riscos

- Uma passada por instância: N pods multiplicam a carga do nível 3 sobre a dependência.
- Sem `enqueuedAt` e sem idempotência (RabbitMQ sem `timestamp`), a passada depende do `max-per-pass` para terminar.
- O emulator do Service Bus tem a falha intermitente conhecida no `complete` da DLQ nativa; o nível 3 usa DLQ comum,
  não a nativa.
