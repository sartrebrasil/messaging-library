# ADR-0005: Garantia de ordem por aggregate

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-10-07 (provedores da messaging-lib, nível 3, batch, stale check no escopo)
- Revisão: 2026-10-07 (Kafka `strict` por padrão e `key-lanes`)

## Contexto

A outbox-library publica em ordem estrita por `(aggregateId, destination)`:
key do record no Kafka, `orderingKey` na messaging-lib (`MessageGroupId` no
SQS FIFO, `SessionId` no Service Bus, `orderingKey` no Pub/Sub,
`JMSXGroupID` no ActiveMQ). Retry que não bloqueia (retry topic, cópia para
outra fila, cópia agendada) quebra essa ordem.

A messaging-lib garante só a ordem de entrega e informa isso em
`Capabilities.orderedDelivery`. Processar em ordem é da inbox.

## Decisão

Configuração por consumer: `ordering: strict | relaxed`.

| Provedor | `strict` | `relaxed` |
|---|---|---|
| Kafka | Seek + pausa da partição | Retry topic; a partição segue |
| SQS FIFO | `broker-delay`: o grupo fica bloqueado enquanto a mensagem está invisível | Não se aplica (FIFO é sempre `strict`) |
| Pub/Sub com ordering | `broker-delay`: o `nack` reentrega a mensagem e as seguintes da chave | Padrão sem ordering |
| Service Bus com sessions | `hold`: `abandon` volta para o início da session, mas não tem atraso | Não se aplica |
| ActiveMQ `IMMEDIATE` | `hold` | Padrão |
| SQS standard, Service Bus sem sessions, ActiveMQ `SCHEDULED`, RabbitMQ | Não suportado: `orderedDelivery = false`, o startup falha | Padrão |

- Padrão: `strict` quando o destino tem `orderedDelivery = true`, `relaxed`
  nos outros. O Kafka entra nesse padrão como `strict`: quem liga
  `key-lanes` no Kafka precisa declarar `ordering: relaxed` (ADR-0016).
- A estratégia do nível 2 em `strict` é sempre a que mantém a ordem (ADR-0002):
  `broker-delay` onde o provedor bloqueia a chave, `hold` nos outros.
  `retry-queues` com `strict` faz o startup falhar.
- Em `strict`, uma mensagem com falha segura a chave (ou a partição, no Kafka,
  ou a session, no Service Bus) durante todo o nível 2.

### Esgotamento em `strict`

`on-exhausted: dead-letter | block`:

- `dead-letter` (padrão): vai para a DLQ e a chave segue. Aceita um buraco na
  sequência. O nível 3 e o redrive reprocessam a mensagem **fora de ordem**,
  depois de versões mais novas do mesmo aggregate. O stale check (ADR-0013)
  é recomendado para esse caso.
- `block`: retry infinito no nível 2 com alerta via métrica, igual à
  quarentena da outbox. O nível 3 não se aplica.

### Concorrência

- `concurrency` > 1 em `strict` só é seguro onde o provedor não entrega duas
  mensagens da mesma chave ao mesmo tempo: SQS FIFO (grupo bloqueado com
  mensagem em voo), Service Bus com sessions (uma session por receiver,
  ADR-0009), Kafka (uma partição por thread), ActiveMQ com message groups (o
  grupo fica preso a uma session, e cada session tem no máximo uma mensagem
  pendente, ADR-0009 da messaging-lib).
- Pub/Sub com ordering: um pull pode trazer várias mensagens da mesma chave, e
  a afinidade de chave entre vários receivers com pull unário não está
  confirmada. Até a F3 confirmar, `strict` no Pub/Sub usa um receiver só, e
  as mensagens de um mesmo pull com a mesma `orderingKey` são processadas em
  sequência.
- Batch em `strict`: ADR-0011.
- Paralelismo por chave dentro de um `receive` ou `poll` (`key-lanes`):
  ADR-0016.

## Consequências

- `strict` troca vazão por ordem: uma mensagem envenenada para a chave inteira
  durante o nível 2, e no Kafka para a partição.
- Os níveis 3 e de redrive nunca preservam ordem. O padrão é documentado com
  destaque no README.
