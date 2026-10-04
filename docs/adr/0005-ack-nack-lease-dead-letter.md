# ADR-0005: Semântica de ack, nack, extensão de lease e dead letter

- Status: aceito
- Data: 2026-10-04

## Contexto

Confirmar e estender lease existem nos quatro provedores. Devolver com atraso e
mandar para dead letter, não:

| | `nack` com atraso | Atraso máximo | Dead letter explícito | DLQ automática |
|---|---|---|---|---|
| SQS | `ChangeMessageVisibility` | 12 h contadas do primeiro receive, não do `nack` | Não | Redrive policy por `maxReceiveCount` |
| Service Bus | Não: `abandon` devolve na hora | Não se aplica | `deadLetter(reason)` | Por `MaxDeliveryCount` |
| Pub/Sub | `modifyAckDeadline(delay)` e deixar vencer; conta como tentativa de entrega | 600 s | Não | Dead-letter topic por `maxDeliveryAttempts` (5–100) |
| OCI Queue | `UpdateMessage` (visibility) | 12 h | Não | Por `maxDeliveryAttempts` |

## Decisão

- `ack`: confirma. Depois do lease vencido, lança `LeaseExpiredException`, nos
  provedores que informam isso. Onde o provedor não informa, o adapter não
  inventa:

  | Provedor | `ack` com lease vencido |
  |---|---|
  | SQS | **Pode responder sucesso sem apagar** (`DeleteMessage` com receipt handle antigo). A mensagem é reentregue e quem consome precisa ser idempotente. `ChangeMessageVisibility` informa: `ReceiptHandleIsInvalid`, `MessageNotInflight` ou `InvalidParameterValue` "receipt handle has expired" |
  | Service Bus | `MESSAGE_LOCK_LOST` ou `SESSION_LOCK_LOST`; o lock também cai antes do vencimento em queda de conexão ou update do serviço |
  | Pub/Sub | Só com exactly-once: `PERMANENT_FAILURE_INVALID_ACK_ID` no `ErrorInfo`. Sem exactly-once, o ack tardio é aceito em silêncio |

- `nack(message, redeliverAfter)`: devolve para nova entrega depois de
  `redeliverAfter` (zero = imediato). Atraso acima do máximo do provedor lança
  `IllegalArgumentException`; quem chama limita pelo valor em
  `Capabilities.maxRedeliveryDelay`. No SQS o máximo é o que resta das 12 h desde
  o primeiro receive (`ApproximateFirstReceiveTimestamp`), por isso o adapter
  valida por mensagem. No Pub/Sub, `nack(0)` é `modifyAckDeadline(0)`, e a retry
  policy da subscription, se houver, ainda acrescenta o backoff dela.
- Service Bus: o adapter recebe `ServiceBusRedelivery`:
  - `ABANDON` (padrão): `abandon` imediato, o atraso é ignorado e a capability
    `delayedRedelivery` é `false`.
  - `RESCHEDULE`: envia uma cópia agendada (`scheduledEnqueueTime`) com o
    atributo `delivery_count` (reservado, usado só no Service Bus, onde não há
    limite prático de atributos) e depois faz `complete` da original. O envio vem
    antes do `complete`: uma falha entre os dois gera duplicata, nunca perda.
    O `deliveryCount` do broker zera, então o adapter passa a usar o atributo.
    Proibido em entidade com sessions, porque quebraria a ordem (ADR-0006).
    `scheduleMessage` existe no cliente síncrono e não tem horizonte máximo
    documentado (no emulator, o TTL máximo de 1 h limita os testes).
- `extendLease(message, lease)`: estende o lease em pelo menos `lease` a partir
  de agora. No Service Bus, a renovação usa o lock duration da entidade e
  `lease` só é validado contra ele.
- `deadLetter(message, reason)`:
  - Service Bus: nativo, com `reason` no dead letter reason.
  - Outros: o adapter recebe um `MessageSender` opcional para a DLQ. Envia uma
    cópia com o atributo reservado `dead_letter` (`<messageId original>;<reason>`,
    `reason` reduzido a ASCII e truncado ao limite do provedor) e depois faz
    `ack`. Mesma ordem do `RESCHEDULE`: duplicata, nunca perda.
  - Sem sender configurado: `UnsupportedOperationException`. O starter falha no
    startup se um receiver com `dead-letter` declarado não tiver destino
    (ADR-0008).
- A DLQ automática por contagem de entregas continua sendo configuração do
  destino (Terraform), não da lib.

## Consequências

- Backoff entre tentativas funciona em SQS, Pub/Sub (até 10 min) e OCI sem
  truque. No Service Bus exige escolher entre perder o atraso e aceitar uma
  cópia.
- `deadLetter` fora do Service Bus usa um atributo reservado (ADR-0004), então a
  cópia sempre cabe no limite de atributos do SQS: os do usuário vão nativos
  (até 7) ou empacotados. `reason` é truncado para o valor caber em 1024 bytes.
- Os contratos do testkit testam `nack` com atraso só onde
  `delayedRedelivery` é `true`.
