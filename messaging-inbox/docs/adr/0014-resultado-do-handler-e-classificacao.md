# ADR-0014: Resultado explícito do handler e classificação de exceções

- Status: aceito (2026-10-08)
- Data: 2026-10-07

## Contexto

O ADR-0002 classifica exceções em duas classes (`NonRetryable` pela lista
`retry.non-retryable`, `Retryable` para o resto) e olha só a exceção lançada.
Faltam casos comuns:

- HTTP 429 ou 503 com `Retry-After`: repetir em 100 ms (nível 1) só piora, e o
  servidor já disse quanto esperar.
- Exceções embrulhadas (`CompletionException`, exceções de framework): a
  causa real fica escondida e a classificação erra.
- Times que preferem dizer o que é retryable e mandar todo o resto para a
  `failed`.
- O handler sabe que a mensagem deve ser descartada (evento irrelevante para
  este consumer) ou rejeitada (regra de negócio), sem lançar exceção.

A acme-messaging, incorporada a este projeto (ADR-0006), já resolve esses
casos com `Outcome` do handler, `@NonRetryable`, `@SkipImmediate`,
`RetryAfterHint`, modo allowlist e leitura da cadeia de causas, com testes.

## Decisão

### Resultado explícito do handler

```java
public sealed interface HandlerResult {
    record Ack() implements HandlerResult {}
    record RetryAfter(Duration delay, Throwable cause) implements HandlerResult {}
    record Reject(String reason, Throwable cause) implements HandlerResult {}
    record Discard(String reason) implements HandlerResult {}

    static HandlerResult ack() { ... }
    static HandlerResult retryAfter(Duration delay) { ... }
    static HandlerResult reject(String reason) { ... }
    static HandlerResult discard(String reason) { ... }
}
```

```java
@InboxListener(consumer = "billing-orders")
HandlerResult on(OrderCreated event, InboxMessage message) { ... }

@Bean
InboxResultHandler<OrderCreated> billingOrders() { ... }   // modelo funcional com resultado
```

- Handler `void` que retorna normalmente equivale a `Ack`. `InboxHandler<T>`
  continua `void`; `InboxResultHandler<T>` devolve `HandlerResult`.
- Não existe `Retry` sem atraso: é o mesmo que lançar a exceção. A
  acme-messaging tinha `Outcome.Retry` porque o handler dela era assíncrono.
- O resultado explícito vence a classificação de exceções.

| Resultado | Efeito | Métricas |
|---|---|---|
| `Ack` | Igual a sucesso | `outcome=success` |
| `RetryAfter(delay)` | Pula o nível 1 e vai para o nível 2 com `delay` no lugar do backoff calculado. Gasta uma tentativa do nível 2. Nível 2 esgotado ou desligado: DLQ. No nível 3: volta para o fim da DLQ | `outcome=retry_delayed` |
| `Reject(reason)` | `failed`, sem retry. Libera a chave de idempotência, como toda ida para a `failed` (ADR-0004) | `outcome=failed`, `reason=rejected` |
| `Discard(reason)` | `Ack`, com `complete` da idempotência. Log em `INFO` com o `reason`, sem payload | `outcome=discarded` |

`delay` do `RetryAfter` é limitado a `retry.delayed.max` (o teto real do
ADR-0002) e, em `broker-delay`, ao `maxRedeliveryDelay` do provedor. Em
`retry-queues`, escolhe a faixa como qualquer atraso.

### Classificação de exceções

A classificação percorre a cadeia de `getCause()` (até 32 elos, protegida
contra ciclo) e aplica, nesta ordem:

1. `@NonRetryable` na classe, ou a classe em `retry.non-retryable`, em
   **qualquer** elo: `NonRetryable`, vai para `failed`
   (`reason=non_retryable`).
2. `retry.retryable` não vazia (modo allowlist) e nenhum elo nela nem em
   `skip-immediate`: `NonRetryable`.
3. `@SkipImmediate` na classe, ou a classe em `retry.skip-immediate`, em
   qualquer elo: pula o nível 1 e vai direto para o nível 2.
4. O resto: `Retryable`.

- `@NonRetryable` e `@SkipImmediate` ficam no `inbox-core`, com `@Inherited`,
  para as exceções da aplicação se marcarem sem configuração.
- Exceção que implementa `RetryAfterHint` em qualquer elo define o atraso do
  próximo nível 2, com o mesmo limite do `RetryAfter`. Ela não pula o nível 1
  sozinha; para isso, a classe também leva `@SkipImmediate`.
- Falha de desserialização continua `NonRetryable` (ADR-0002).
- Nomes de classe nas listas são carregados no startup. Classe inexistente ou
  que não é `Throwable` faz o startup falhar: erro de digitação numa lista de
  exceções é um bug silencioso.

```yaml
inbox:
  consumers:
    billing-orders:
      retry:
        non-retryable: [com.acme.InvalidOrderException]
        skip-immediate: [com.acme.RateLimitedException]
        retryable: []                    # preenchida = modo allowlist
```

### Circuit breaker (ADR-0010)

| Resultado | Chamada no `ConsumptionGate` |
|---|---|
| `Ack`, `Discard` | `onSuccess` |
| `RetryAfter` com `cause` | `onError(cause)`. Um 429 persistente pode abrir o circuito, o que pausa o consumo enquanto a dependência limita |
| `RetryAfter` sem `cause` | `ignore` |
| `Reject`, exceção `NonRetryable` | `ignore` |
| Exceção `Retryable` ou `SkipImmediate` | `onError` |

### Execuções no pior caso

`SkipImmediate` e `RetryAfter` tiram o nível 1 da entrega: o pior caso dessas
mensagens é `1 x (1 + delayed) + 1 x dead-letter`.

## Alternativas descartadas

- `Outcome` do handler com o mesmo nome do `Outcome` do pipeline (ADR-0001):
  são coisas diferentes. O do pipeline é o efeito no broker, que o handler não
  escolhe.
- `Reject` indo para a DLQ, como na acme-messaging: com o nível 3, a DLQ é
  retry lento. Rejeição é definitiva e vai para a `failed`.
- Anotações de configuração no handler (`@StaleAfter`, `@IdempotencyKey` da
  acme-messaging): a configuração fica só no YAML (ADR-0008), para o SRE
  ajustar sem build.
- `RetryAfter` acima de `delayed.max`: um `Retry-After` de horas viraria uma
  espera fora de qualquer orçamento de lease e de TTL de idempotência.

## Consequências

- Rate limit e `Retry-After` funcionam sem queimar o nível 1.
- A classificação acerta exceções embrulhadas. Uma exceção de framework que
  embrulha um `IllegalArgumentException` passa a ir para a `failed`.
- Novos valores nas métricas (ADR-0007): `outcome=discarded`,
  `reason=rejected`.
- Código de origem: `ExceptionClassifier`, `ThrowableTypes` e
  `RetryAfterHint` da acme-messaging, com o `FailureResolver` ajustado aos
  três níveis.
