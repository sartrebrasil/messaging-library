# ADR-0004: Contrato da mensagem: corpo em bytes, atributos portáveis

- Status: aceito
- Data: 2026-10-04

## Contexto

Limites e formatos variam por provedor:

| | SQS | SNS | Service Bus | Pub/Sub | OCI Queue (depois da v1) |
|---|---|---|---|---|---|
| Tamanho máximo | 1 MiB (desde 08/2025), **incluindo atributos** | 256 KiB padrão; `MaximumMessageSize` do tópico até 1 MiB | Standard 256 KB; Premium 1 MB padrão, até 100 MB | 10 MB | 256 KB |
| Atributos | Até 10 | Até 10; com raw delivery, acima de 10 a mensagem é **descartada** na entrega | Sem limite de quantidade; 32 KB por propriedade, 64 KB no header | Até 100; chave até 256 bytes, valor até 1024 bytes; prefixo `goog` reservado | Custom properties (limites a confirmar) |
| Corpo | Texto (UTF-8), mínimo 1 byte | Texto | Binário | Binário; mensagem sem dados e sem atributos é rejeitada | Texto |
| Contador de entregas | `ApproximateReceiveCount` | Não se aplica | `deliveryCount`, **0 na primeira entrega no SDK Java** | `deliveryAttempt`: 0 sem dead letter policy, 1 na primeira entrega com policy | `deliveryCount` |

O limite de 10 atributos do SQS/SNS não comporta CloudEvents em modo binário
(`ce_id`, `ce_source`, `ce_type`, `ce_specversion`, `ce_time`, `ce_subject` e
extensões) mais os atributos que a própria lib usa.

## Decisão

### Corpo

- Corpo é `byte[]` **não vazio** (o SQS exige ao menos 1 byte e o Pub/Sub
  rejeita mensagem sem dados e sem atributos), validado no core.
  Serialização é de quem usa; `contentType` é um campo
  informativo, transportado como atributo reservado `content_type` onde o
  provedor não tem campo próprio (no Service Bus e no Pub/Sub o adapter usa o
  campo nativo quando existir).
- SQS e SNS só aceitam texto. A codificação é decidida pelo `contentType`, sem
  atributo extra:
  - `contentType` textual (`text/*`, `application/json`, `application/xml`,
    `*+json`, `*+xml`): corpo enviado como texto; precisa ser UTF-8 válido,
    senão `IllegalArgumentException`.
  - `contentType` não textual: corpo em Base64; o receiver decodifica.
  - Sem `contentType`: texto se for UTF-8 válido; senão o adapter usa
    `application/octet-stream` e Base64.
- O receiver de qualquer provedor devolve sempre os bytes originais.

### Atributos

| Regra | Valor |
|---|---|
| Quantidade | Até 16 por mensagem, em qualquer provedor |
| Chave | `[a-z_][a-z0-9_]*`, até 64 caracteres, sem prefixo `goog` |
| Valor | ASCII imprimível, até 1024 bytes |
| Reservadas pela lib | `content_type`, `traceparent`, `dead_letter`, `delivery_count`, `attributes` |

- O core valida antes de qualquer chamada ao SDK e lança
  `IllegalArgumentException`. Quem envia não pode usar chaves reservadas; o
  receiver as remove de `ReceivedMessage.attributes()`.
- SQS e SNS: até 7 atributos do usuário vão como atributos nativos
  (10 − `content_type` − `traceparent` − `dead_letter`). Acima de 7, **todos**
  os atributos do usuário vão juntos no atributo reservado `attributes`, um
  objeto JSON de strings, e o receiver desempacota. Regra de tudo ou nada: a
  mensagem tem só atributos nativos ou só o pacote, nunca uma mistura. O F0
  confirmou que isso é necessário: com raw delivery, o SNS descarta em silêncio
  mensagem com mais de 10 atributos.
- Service Bus e Pub/Sub: sempre atributos nativos.
- OCI (depois da v1): decidir quando os limites de custom properties forem
  confirmados; a mesma regra de empacotamento serve se o limite for baixo.

### Tamanho

- Cada adapter conhece o limite do seu provedor (corpo + atributos, como o
  provedor conta) e lança `MessageTooLargeException` antes de chamar o SDK.
- `Capabilities.maxMessageBytes` informa o limite. Dois provedores dependem da
  configuração do destino, que o adapter recebe por parâmetro: no SNS, o
  `MaximumMessageSize` do tópico (padrão 256 KiB); no Service Bus, o tier e o
  limite da entidade (padrão 256 KB). O adapter conta o aumento de cerca de 33%
  do Base64, o tamanho do pacote `attributes` e, no SQS, nome, tipo e valor de
  cada atributo.
- Limite portável: 256 KiB com folga para atributos. Acima disso depende do
  provedor e de claim-check (fora do escopo, ADR-0001).

### Mensagem recebida

`ReceivedMessage` traz:

| Campo | Origem |
|---|---|
| `messageId` | Id do provedor |
| `body` | Bytes originais (Base64 desfeito) |
| `attributes` | Atributos do usuário (pacote desfeito, reservados removidos) |
| `contentType` | Campo nativo ou `content_type` |
| `orderingKey` | `MessageGroupId`, `SessionId` ou `orderingKey` |
| `deliveryCount` | `OptionalInt`, normalizado para 1 na primeira entrega (no Service Bus o adapter soma 1 ao valor do SDK Java); vazio no Pub/Sub sem dead letter policy (`deliveryAttempt = 0`) |
| `enqueuedAt` | Horário de entrada no provedor |
| `leaseExpiresAt` | Vencimento do lease calculado no receive |
| handle nativo | Opaco, usado só pelo receiver |

Não existe id de mensagem definido pela aplicação. Id de negócio (por exemplo,
`ce_id`) é atributo; `deduplicationId` cobre a deduplicação do provedor
(ADR-0006).

## Consequências

- O mesmo `OutgoingMessage` funciona nos três provedores da v1, desde que caiba
  em 256 KiB, inclusive com CloudEvents binário completo.
- Base64 e o pacote `attributes` são transparentes para quem usa a lib. Um
  consumidor sem a lib lendo a mesma fila SQS precisa saber das duas regras: o
  `content_type` indica Base64, e o atributo `attributes` indica o pacote.
  Documentar no README com um exemplo de leitura sem a lib.
- Filtros de subscription do SNS por atributo não enxergam atributos
  empacotados. Quem filtra no SNS mantém até 7 atributos.
- `tracestate` e `baggage` do W3C não são propagados; só `traceparent`
  (ADR-0008).
