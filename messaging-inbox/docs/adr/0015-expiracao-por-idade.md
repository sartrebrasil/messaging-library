# ADR-0015: Expiração por idade da mensagem

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (marca de redrive em `inbox_attempts`; ordem do pipeline)

## Contexto

Algumas mensagens perdem o valor com o tempo: cotação, posição, notificação
ao usuário, comando com prazo. Processá-las depois de minutos ou horas é
inútil ou prejudicial. Elas chegam velhas pelo lag da fila, pelo circuit
breaker aberto (ADR-0010), pelo nível 2 e principalmente pelo nível 3, que
devolve mensagens horas depois (ADR-0002).

O stale check do ADR-0013 não cobre o caso: ele compara a versão do
aggregate, não a idade, e precisa de store. A acme-messaging, incorporada a
este projeto (ADR-0006), tinha esse descarte por idade (`stale.max-age`).

## Decisão

### Regra

- Middleware opcional, logo depois do envelope. Não faz I/O, então vem antes
  da idempotência e evita a ida ao store:

  ```
  observation -> envelope -> expiração -> idempotência -> stale check -> retry nível 1 [ circuit breaker -> handler -> classificação ]
  ```

- Expirada quando `now - instante > max-age + clock-skew`.
- O instante vem de `source`:

  | `source` | Valor | Igual nas cópias |
  |---|---|---|
  | `event-time` (padrão) | `InboxMessage.time` (`ce_time` ou `occurredAt` da outbox, ADR-0003) | Sim |
  | `enqueued-at` | `ReceivedMessage.enqueuedAt`; no Kafka, o timestamp do record | Não: retry-queues, DLQ, `RESCHEDULE` e `SCHEDULED` criam mensagem nova |
  | `header:<nome>` | Atributo do usuário, ISO-8601 ou epoch em milissegundos | Sim |

- Mensagem sem o instante (ou com valor inválido): a expiração não se aplica,
  e a mensagem segue. Log em `DEBUG`.
- Expirada: `action`:
  - `discard` (padrão): `Ack` sem chamar o handler, `outcome=expired`.
  - `failed`: vai para a `failed`, `reason=expired`.
- A idempotência não é tocada (a expiração vem antes do `reserve`).
- Vale em toda entrega: primeira, nível 2, nível 3 e cada volta do `hold`.
- Mensagens trazidas pelo redrive (marca `redrive=<id>` no `inbox_attempts`,
  ADR-0012) **não** expiram: o operador pediu o reprocessamento. A marca
  acompanha a mensagem em todas as entregas e cópias seguintes, em qualquer
  estratégia (ADR-0002).

### Configuração

```yaml
messaging:
  receivers:
    quotes:
      expiration:
        enabled: true
        max-age: 10m
        source: event-time          # event-time | enqueued-at | header:<nome>
        clock-skew: 5s
        action: discard             # discard | failed
```

### Validação no startup

- `max-age` menor que a soma dos atrasos do nível 2: log de aviso. As
  mensagens expiram antes de esgotar o retry, e o nível 2 vira só espera.
- `source: enqueued-at` com nível 2 por `retry-queues` ou com nível 3: log de
  aviso, porque cada cópia reinicia a idade.

## Alternativas descartadas

- `source: broker-timestamp` como padrão, como na acme-messaging: as cópias
  ganham timestamp novo e uma mensagem velha pareceria nova a cada retry.
- Reaproveitar o nome `stale` da acme-messaging: o ADR-0013 já usa stale para
  versão do aggregate. Dois conceitos com o mesmo nome confundem métricas e
  configuração.
- Expirar também as mensagens do redrive: o operador teria que desligar a
  expiração para reprocessar a `failed`.

## Consequências

- Mensagens com prazo saem da fila sem chamar o handler e sem ir ao store de
  idempotência.
- Novos valores nas métricas (ADR-0007): `outcome=expired` e
  `reason=expired`. Descarte sempre gera métrica; descarte silencioso parece
  perda.
- O produtor precisa preencher o instante do evento. Com a outbox-library, já
  preenche (`occurredAt`).
