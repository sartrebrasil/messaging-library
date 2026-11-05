# ADR-0003: Contrato de envelope (CloudEvents binário com fallback para a outbox)

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-10-07 (atributos da messaging-lib; id do modo raw muda nas cópias)
- Revisão: 2026-10-07 (`delivery()` e `TypedMessage`)
- Revisão: 2026-11-05 (leitura da outbox só com o JDK; regras do `auto` e dos `headers`)

## Contexto

O principal produtor é a outbox-library. Hoje ela coloca o envelope só no
body JSON (`messageId`, `aggregateType`, `aggregateId`, `aggregateVersion`,
`eventType`, `occurredAt`, `headers`, `payload`) e não usa headers nativos.
Para rotear, deduplicar e propagar trace, o consumidor precisa desserializar o
body inteiro.

## Decisão

`InboxMessage` (core) expõe: `id`, `type`, `source`, `time`, `subject`
(aggregateId), `aggregateType`, `aggregateVersion`, `traceparent`, `headers`,
`payload` (bytes), `destination` e `delivery()`.

- `delivery()` diz em que ponto do retry a entrega está: `level`
  (`immediate`, `delayed`, `dead_letter`) e `attempt`. Vem do `inbox_attempts`
  (ADR-0002), que não aparece em `headers`.
- `TypedMessage<T>` é o `InboxMessage` com o payload já desserializado
  (`payload()` devolve `T`). É o que os handlers recebem (ADRs 0011 e 0014).

Leitura em cascata, configurável por consumer (`envelope: auto|cloudevents|outbox|raw`,
padrão `auto`):

1. **CloudEvents binário** nos headers nativos: `ce_id`, `ce_type`,
   `ce_source`, `ce_time`, `ce_subject`, extensões `aggregatetype`,
   `aggregateversion`, e `traceparent`. Kafka usa headers de record; os
   provedores da messaging-lib usam atributos com os mesmos nomes, que já
   seguem o formato `[a-z_][a-z0-9_]*` (ADR-0004 da messaging-lib). O
   `traceparent` chega pelo campo `ReceivedMessage.traceparent`.
2. **Body da outbox**: JSON com `messageId` + `eventType` + `payload`.
3. **Raw**: sem envelope. `id` vem do id nativo (`ReceivedMessage.messageId`;
   Kafka `topic-partition-offset`), payload é o body inteiro. O id nativo muda
   em toda cópia (retry-queues, DLQ, `RESCHEDULE`, `SCHEDULED`, redrive), então
   em modo raw a idempotência só vale dentro da mesma mensagem física. O
   startup registra um aviso quando um consumer com idempotência usa
   `envelope: raw` (ADR-0004).

A outbox-library passa a escrever os headers CloudEvents (fase F1 do plano),
mantendo o body atual para não quebrar consumidores existentes.

Compatibilidade entre repos: fixtures de referência (body + headers) da outbox
copiadas para os testes da inbox. Sem artifact compartilhado por enquanto.

Desserialização do payload: SPI `PayloadDeserializer` no core; o starter
fornece a implementação com Jackson 3 usando o mapper da aplicação.

### Revisão de 2026-11-05: implementação no `messaging-inbox`

- O `messaging-inbox` só usa o JDK (ADR-0006). O body da outbox é lido por um
  scanner mínimo (`OutboxJson`) que entende só o formato do
  `EnvelopeJsonWriter` da outbox-library: membros string, `aggregateVersion`,
  `headers` como objeto plano de strings e `payload` guardado como JSON bruto,
  sem validar (quem lê é o `PayloadDeserializer`). O scanner não usa
  recursão, para que um body muito aninhado não estoure a pilha do worker. Se
  a inbox precisar ler estruturas aninhadas, ele dá lugar ao `jackson-core`,
  com nova revisão deste ADR.
- Extensões CloudEvents como atributos: `ce_aggregatetype` e
  `ce_aggregateversion`. O `traceparent` não vira atributo `ce_*`: é reservado
  na messaging-lib e chega por `ReceivedMessage.traceparent`.
- `auto`: CloudEvents quando há `ce_id`; outbox quando o body é um objeto JSON
  com `messageId`, `eventType` e `payload`; raw nos outros casos. Formato
  reconhecido com campo quebrado (`ce_time`, `occurredAt`, versão não inteira)
  falha como desserialização em vez de cair em raw, porque raw trocaria o id e
  a idempotência deixaria de valer.
- `headers()`: atributos do transporte mais os `headers` do body da outbox; o
  valor do body vence, como no `traceparent` (ADR-0007). `inbox_*` ficam fora;
  `ce_*` ficam.

## Consequências

- Mensagens antigas da outbox continuam legíveis.
- Produtores que não são a outbox funcionam via CloudEvents ou raw.
- Artifact `envelope` compartilhado só se os dois repos divergirem na prática.
