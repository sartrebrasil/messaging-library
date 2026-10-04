# ADR-0007: Capabilities e hierarquia de erros

- Status: aceito
- Data: 2026-10-04

## Contexto

Os ADRs 0004 a 0006 deixam diferenças que não dá para igualar: tamanho máximo,
atraso de `nack`, dead letter nativo, ordem, deduplicação. Quem usa a lib
precisa saber disso no startup, não na primeira falha em produção. Na
storage-lib isso ficou só no README e em `UnsupportedOperationException`, o que
basta para um CRUD. Para mensageria, consumidores como a inbox-library mudam
de comportamento conforme o provedor (por exemplo, retry por `nack` com atraso
ou por reenvio).

Os erros de cada SDK também são diferentes (`SqsException`,
`ServiceBusException`, `ApiException`, `BmcException`), e quem chama precisa
saber se vale tentar de novo.

## Decisão

`Capabilities` é um record imutável, devolvido por `sender.capabilities()` e
`receiver.capabilities()`:

| Campo | SQS | SNS | Service Bus | Pub/Sub | OCI Queue |
|---|---|---|---|---|---|
| `maxMessageBytes` | 1 MiB (corpo + atributos) | `MaximumMessageSize` do tópico: 256 KiB padrão, até 1 MiB (configurado no adapter) | Standard 256 KB; Premium 1 MB padrão, até 100 MB (configurado no adapter) | 10 MB | 256 KB |
| `maxBatchSize` | 10 (e 1 MiB no lote) | 10 | Lote de 1 MB | 1000 (e 10 MB) | 20 |
| `delayedRedelivery` | Sim | Não se aplica | Só em `RESCHEDULE`, sem sessions | Sim | Sim |
| `maxRedeliveryDelay` | 12 h desde o primeiro receive | Não se aplica | Agendamento | 600 s | 12 h |
| `nativeDeadLetter` | Não | Não se aplica | Sim | Não | Não |
| `orderedDelivery` | Só FIFO | Só FIFO | Só com sessions | Sim | Não |
| `publisherDeduplication` | Só FIFO | Só FIFO | Sim | Não | Não |
| `reportsLeaseExpired` | Parcial: `extendLease`/`nack` sim, `ack` não | Não se aplica | Sim | Só exactly-once | A confirmar quando o OCI entrar |

Mapeamento de erros do SDK (F0):

| Exceção da lib | SQS/SNS | Service Bus | Pub/Sub |
|---|---|---|---|
| `LeaseExpiredException` | `ReceiptHandleIsInvalid`, `MessageNotInflight`, `InvalidParameterValue` com "receipt handle" | `MESSAGE_LOCK_LOST`, `SESSION_LOCK_LOST` | `PERMANENT_FAILURE_INVALID_ACK_ID` |
| `DestinationNotFoundException` | `QueueDoesNotExist`, `NotFound` | `MESSAGING_ENTITY_NOT_FOUND` | `NOT_FOUND` |
| `AccessDeniedException` | `AccessDenied`, `AuthorizationError` | `UNAUTHORIZED` | `PERMISSION_DENIED` |
| `ThrottledException` | `RequestThrottled`, `Throttling` | `SERVICE_BUSY`, `QUOTA_EXCEEDED` | `RESOURCE_EXHAUSTED` |
| `MessageTooLargeException` | Validação antes da chamada; `BatchRequestTooLong`, `InvalidParameter` (SNS) como rede de segurança | `MESSAGE_SIZE_EXCEEDED` | `INVALID_ARGUMENT` por tamanho |

Só a linha de `LeaseExpiredException` foi pesquisada no F0. As outras linhas
são o mapeamento esperado e ainda não foram verificadas; os testes de contrato
da F2–F4 confirmam cada código.

Os valores dizem o que o adapter suporta com a configuração recebida. Não dizem
o que o destino tem configurado na nuvem (ADR-0006).

Erros:

```
MessagingException (unchecked; retryable(): boolean; provider(): String)
├── DestinationNotFoundException
├── AccessDeniedException
├── MessageTooLargeException
├── LeaseExpiredException
└── ThrottledException          (retryable = true)
```

- O SDK já faz retry dos erros transitórios. O que sobra vira
  `MessagingException` com `retryable` indicando se tentar de novo faz sentido
  (timeout, 5xx, throttling = `true`; 4xx de validação = `false`).
- A exceção original do SDK fica como `cause`.
- `IllegalArgumentException` para erro de uso detectável antes da chamada
  (atributo inválido, atraso acima do máximo, FIFO sem `orderingKey`);
  `UnsupportedOperationException` para operação que o adapter não oferece.

## Consequências

- Um consumidor decide a estratégia uma vez, no startup, lendo as capabilities.
- O contrato do testkit pula casos conforme as capabilities, então cada adapter
  testa exatamente o que promete.
- Adicionar capability é mudança compatível; remover é breaking change.
