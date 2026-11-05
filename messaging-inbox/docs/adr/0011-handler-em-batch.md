# ADR-0011: Handler em batch

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (`BatchResult` por mensagem com `HandlerResult`, `bisect`, `max-wait`; vindos da
  acme-messaging)
- Revisão: 2026-10-07 (`bisect` divide com qualquer exceção, inclusive `NonRetryable`)

## Contexto

Alguns handlers rendem muito mais em lote: insert em batch no banco, chamada
de API que aceita vários itens, agregação. Processar uma mensagem por vez
nesses casos multiplica o número de round trips.

O pipeline (idempotência, stale check, retry, filas de falha) é por mensagem.
O batch não pode mudar isso: o handler precisa ter como evitar que uma
mensagem ruim mande o lote inteiro para a DLQ ou para a `failed`.

## Decisão

### API

```java
public interface InboxBatchHandler<T> {
    BatchResult handle(List<TypedMessage<T>> messages) throws Exception;
}

public final class BatchResult {
    public static BatchResult success() { ... }
    public static Builder builder() { ... }

    public static final class Builder {
        public Builder failed(TypedMessage<?> message, Throwable cause) { ... }
        public Builder result(TypedMessage<?> message, HandlerResult result) { ... }   // ADR-0014
        public BatchResult build() { ... }
    }
}
```

```java
@InboxListener(consumer = "billing-orders", batch = true)
BatchResult on(List<TypedMessage<OrderCreated>> messages) {
    var result = BatchResult.builder();
    for (var m : messages) {
        try { billing.register(m.payload()); }
        catch (InvalidOrderException e) { result.result(m, HandlerResult.reject("invalid")); }
    }
    return result.build();
}
```

- O resultado é indexado pela **própria** mensagem recebida (identidade do
  objeto), não por índice nem por id. Índice quebra quando o handler filtra ou
  reordena a lista; id pode repetir no mesmo lote quando não há idempotência.
- Mensagem que não aparece no resultado deu certo (`Ack`).
- Mensagem que não pertence ao lote: `IllegalArgumentException` no `Builder`.
- `failed(m, e)` segue a classificação do ADR-0014; `result(m, r)` aplica o
  resultado explícito. Cada mensagem segue os níveis do ADR-0002 como se
  tivesse sido processada sozinha.

### Exceção lançada pelo handler

`batch.on-exception`:

- `fail-all` (padrão): todas as mensagens do lote falharam com a exceção, e a
  classificação dela vale para todas. Uma exceção `NonRetryable` manda o lote
  inteiro para a `failed`. É o comportamento de um handler que não sabe dizer
  qual mensagem falhou; quem quer isolar a mensagem ruim devolve
  `BatchResult` ou usa `bisect`. O README destaca essa consequência.
- `bisect`: com **qualquer** exceção, `NonRetryable` inclusive, o lote é
  dividido ao meio e cada metade chama o handler de novo, recursivamente, até
  isolar a mensagem que lança a exceção. As metades que dão certo são
  confirmadas. Só a mensagem isolada recebe a classificação da exceção: com
  `NonRetryable`, só ela vai para a `failed`; com `Retryable`, segue o nível 1
  normalmente.
  - As chamadas do `bisect` não gastam tentativas do nível 1. No pior caso, um
    lote de `n` mensagens chama o handler `2n - 1` vezes.
  - Só é seguro se cada chamada do handler for atômica (transação em `join`,
    ADR-0004): uma chamada que falha não pode deixar efeito parcial.
  - As chamadas do `bisect` cabem no `max-processing` do lote (ADR-0009).

### Formação do batch

- Um batch é o resultado de um `receive` (`batch.max-size` vira o
  `max-messages-per-poll`, limitado ao `maxBatchSize` do receiver). O
  `receive` já espera até `poll-wait`.
- `batch.max-wait` (padrão `0`): acima de zero, o worker faz `receive`
  adicionais até juntar `max-size` mensagens ou até `max-wait` passar. Serve
  para lotes maiores que o `maxBatchSize` do provedor (10 no SQS). Cada
  mensagem tem `keepAlive` desde o seu `receive`, então a espera não estoura o
  lease; o custo é latência. Limitado a `poll-wait`.
- Kafka: o resultado de um `poll`, com `max.poll.records` = `batch.max-size`.
  `max-wait` não se aplica (`fetch.max.wait.ms` faz esse papel).
- RabbitMQ e ActiveMQ têm `maxBatchSize = 1` na messaging-lib, mas o `receive`
  devolve até `maxMessages`. O limite que vale é o de mensagens pendentes do
  receiver (`max-in-flight` no ActiveMQ, prefetch no RabbitMQ).

### Pipeline por mensagem

```
para cada mensagem: observation -> envelope -> expiração -> idempotência -> stale check
handler(lote das mensagens que sobraram)
para cada mensagem: complete ou release, Outcome, efeito no broker
```

- Duplicatas, expiradas e stale saem do lote antes do handler e são
  confirmadas na hora.
- Falha de desserialização vai para `failed` e sai do lote.
- Lote vazio depois dos filtros: o handler não é chamado.
- Um `keepAlive` por mensagem, aberto logo após o `receive`, com orçamento de
  `max-wait` mais `max-processing` (ADR-0009).

### Retry

- Nível 1: só as mensagens que falharam formam o próximo lote. As que deram
  certo já são confirmadas.
- Nível 2 e adiante: por mensagem. Cada uma segue a estratégia do consumer.
  Com `hold`, as mensagens com falha esperam juntas e voltam juntas como um
  lote menor.
- Nível 3: a passada da DLQ forma lotes do mesmo jeito.

### Ordem (`strict`)

Uma mensagem com falha bloqueia as seguintes da mesma chave no lote:

- Mensagens da mesma `orderingKey` (ou partição, no Kafka) que vêm depois da
  que falhou **não são confirmadas**, mesmo que tenham dado certo no handler.
  Elas recebem o mesmo `Retry` que a mensagem com falha.
- Kafka: seek para o offset da primeira falha de cada partição.
- Cada mensagem arrastada gasta uma entrega no broker em `broker-delay`.
  Handlers em batch com `strict` precisam ser idempotentes (a mensagem que deu
  certo e foi arrastada roda de novo). Com idempotência ligada, o `complete`
  dela não é feito, para que a reentrega processe de novo, na ordem.
- `bisect` em `strict`: as metades seguem a ordem do lote, e a regra de
  arrasto vale para a mensagem isolada.

### Idempotência e transação

- `reserve` por mensagem, antes do handler.
- Com store JDBC em `join`: uma transação por chamada do handler. O `complete`
  das que deram certo e o `release` das que falharam entram na mesma transação.
  Se o handler lançar exceção, a transação inteira volta e todas as chaves do
  lote daquela chamada são liberadas.

### Observabilidade

- Um span `<destination> process` por chamada do handler, com um span link
  para o contexto de cada mensagem (ADR-0007).
- `received` e `processed` continuam por mensagem; `inbox_batch_size` mede o
  tamanho do lote entregue ao handler, inclusive nas chamadas do `bisect`.

### Configuração

```yaml
inbox:
  consumers:
    billing-orders:
      batch:
        enabled: true
        max-size: 100
        max-wait: 200ms
        on-exception: fail-all      # fail-all | bisect
```

## Alternativas descartadas

- Lote tudo ou nada como modo próprio (`all-or-nothing` da acme-messaging):
  é o `fail-all` com a exceção lançada. Um resultado que confirma nada mesmo
  com falhas parciais levaria o lote inteiro para a DLQ.
- Resultado indexado por posição (versão anterior deste ADR) ou por id da
  mensagem (acme-messaging): ver a seção API.
- `bisect` gastando tentativas do nível 1: com lotes grandes, o nível 1
  acabaria antes de isolar a mensagem.

## Consequências

- O handler em batch devolve o resultado das mensagens que falharam. Lançar
  exceção é aceito: com `fail-all` trata o lote inteiro como falha (com
  `NonRetryable`, o lote inteiro vai para a `failed`), com `bisect` isola a
  mensagem envenenada.
- Em `strict`, uma falha no meio do lote gera reprocessamento das mensagens
  seguintes da mesma chave.
- Código de origem: `BatchResult` e `BatchFailureMode` da acme-messaging,
  trocando a chave por id pela identidade da mensagem.
