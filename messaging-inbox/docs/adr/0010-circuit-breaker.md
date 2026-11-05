# ADR-0010: Circuit breaker que pausa o consumo, sobre o Resilience4j

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (máquina de estados do Resilience4j num módulo opcional; o core só tem o gancho)
- Revisão: 2026-10-07 (`outcome=circuit_open`; nível 3 só com o circuito fechado; posição no pipeline)

## Contexto

Quando a dependência do handler cai (banco, API, outro serviço), toda mensagem
falha. Sem circuit breaker, o consumer continua recebendo, gasta as tentativas
dos níveis 1 e 2 de mensagens saudáveis e enche a DLQ. Quando a dependência
volta, o trabalho está espalhado entre a fila de retry, a DLQ e a `failed`.

Num consumer, falhar rápido não ajuda: a mensagem falharia do mesmo jeito. O
que ajuda é **parar de buscar mensagens** enquanto a dependência está fora.

A versão anterior deste ADR escolheu uma máquina de estados própria no
`inbox-core`, sem Resilience4j. Os motivos não se sustentaram:

- "O Resilience4j modela falha rápida, não pausa": a pausa é só reagir à
  mudança de estado. A máquina de estados é a mesma.
- "A máquina de estados tem poucas linhas": janela deslizante,
  `minimumNumberOfCalls`, permissões no meio aberto, espera exponencial no
  aberto e thread-safety somam umas 150 linhas mais testes. O Resilience4j
  cobre todos os parâmetros que a versão anterior definia.
- Fica de fora um ganho real: a aplicação muitas vezes já tem um
  `CircuitBreaker` do Resilience4j no cliente HTTP da mesma dependência.
  Usando a mesma instância, o consumer pausa quando ele abre, sem segunda
  configuração.

O motivo que continua valendo é o `inbox-core` ser Java puro, sem
dependências (ADR-0001).

## Decisão

### Divisão

| Módulo | Conteúdo |
|---|---|
| `inbox-core` | SPI `ConsumptionGate` e o uso dele no loop e no pipeline. Sem o módulo abaixo, não há circuit breaker |
| `inbox-resilience4j` | `ConsumptionGate` sobre um `io.github.resilience4j.circuitbreaker.CircuitBreaker`. Depende só de `resilience4j-circuitbreaker`, sem Spring |
| `inbox-spring-boot-starter` | Liga o módulo quando ele está no classpath. Usa o bean `CircuitBreakerRegistry` da aplicação, se existir; senão cria um registry próprio |

```java
public interface ConsumptionGate {
    enum State { CLOSED, HALF_OPEN, OPEN }

    State state();
    void onStateChange(Consumer<State> listener);

    /** Antes de cada execução do handler. {@code false}: o circuito não permite. */
    boolean tryAcquire();
    void onSuccess(long durationNanos);
    void onError(long durationNanos, Throwable error);
    /** A execução autorizada não conta (NonRetryable). */
    void ignore();
}
```

O mapeamento para o Resilience4j é direto: `tryAcquirePermission`,
`onSuccess`, `onError`, `releasePermission` e
`getEventPublisher().onStateTransition`.

- Desligado por padrão. Ligado por consumer.
- `circuit-breaker.name` (padrão = nome do consumer) é o nome da instância no
  `CircuitBreakerRegistry`. Consumers com o mesmo nome, e qualquer outro uso
  da mesma instância na aplicação, compartilham o estado.
- O estado é por instância da aplicação, não distribuído.

### Configuração

Os parâmetros são os do Resilience4j. A inbox não repete propriedades.

```yaml
inbox:
  consumers:
    billing-orders:
      circuit-breaker:
        enabled: true
        name: billing-api

resilience4j:                      # opcional, só com o starter do Resilience4j
  circuitbreaker:
    instances:
      billing-api:
        sliding-window-type: COUNT_BASED
        sliding-window-size: 20
        minimum-number-of-calls: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        enable-exponential-backoff: true
        exponential-max-wait-duration-in-open-state: 5m
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions: [com.acme.OptimisticLockException]
```

- Instância que não existe no registry: a inbox cria com os valores acima como
  padrão.
- `automatic-transition-from-open-to-half-open-enabled = false` faz o startup
  falhar. Com o circuito aberto o consumer não executa nada, então o
  Resilience4j nunca sairia do aberto.

### O que conta

| Resultado de uma execução do handler | Chamada |
|---|---|
| Sucesso, `Discard` | `onSuccess` |
| Exceção `Retryable` ou `SkipImmediate`, `RetryAfter` com `cause` | `onError`. O Resilience4j aplica `record-exceptions` e `ignore-exceptions` |
| Exceção `NonRetryable`, `Reject`, `RetryAfter` sem `cause` | `ignore` |
| `duplicate`, `stale`, `expired`, `in_progress` | Nenhuma: o handler não rodou e não pediu permissão |

Os resultados explícitos do handler estão no ADR-0014.

Cada execução pede permissão (`tryAcquire`), inclusive as do nível 1: três
falhas seguidas no nível 1 são três sinais de que a dependência caiu. Por isso
o circuit breaker fica **dentro** do nível 1 no pipeline, em volta de cada
execução do handler:

```
observation -> envelope -> expiração -> idempotência -> stale check -> retry nível 1 [ circuit breaker -> handler -> classificação ]
```

### Estados

| Estado | Comportamento da inbox |
|---|---|
| Fechado | Normal |
| Aberto | Nenhum worker chama `receive`; os workers esperam o evento de mudança de estado. A passada do nível 3 não começa, e a que estiver em andamento termina (`end=circuit_open`) |
| Meio aberto | Um worker só chama `receive`, com `max-messages-per-poll = 1`. O Resilience4j decide se fecha ou reabre depois de `permitted-number-of-calls-in-half-open-state` execuções, pela taxa de falha. A passada do nível 3 também não começa: ela só começa com o circuito fechado |

No meio aberto, uma falha não reabre sozinha como na versão anterior: vale a
taxa de falha das execuções de teste. Com 3 execuções e limite de 50%, duas
falhas reabrem.

### Mensagens em andamento quando o circuito abre

- Antes de cada tentativa do nível 1, o `tryAcquire` é chamado de novo. Se
  devolver `false`, a mensagem não roda as tentativas restantes: vira
  `Retry(wait-duration-in-open-state)` pela estratégia do nível 2, com
  `outcome=circuit_open`. Não conta tentativa onde a inbox controla a contagem
  (mesma regra do `in_progress`, ADR-0004). Em `broker-delay` conta, porque o
  `deliveryCount` cresce. Não é `Release`: um `nack(0)` traria a mensagem de
  volta na hora, para um consumer que está parado.
- Numa passada do nível 3, a passada termina (`end=circuit_open`) e a mensagem
  volta para o fim da DLQ sem somar `l3`.
- Em `hold`, a espera dura até o circuito sair de aberto.
- O handler que já está rodando termina normalmente.

### Kafka

Aberto: `pause` em todas as partições do consumer, que continua chamando `poll`
para não sair do grupo. Meio aberto: `resume` de uma partição. Fechado:
`resume` de todas.

### Métricas

`inbox_circuit_state` e `inbox_circuit_transitions_total` (ADR-0007) são
alimentadas pelo evento de mudança de estado. As métricas do
`resilience4j-micrometer`, se a aplicação as tiver, continuam valendo e não
são duplicadas pela inbox.

## Alternativas descartadas

- Máquina de estados própria no `inbox-core` (versão anterior deste ADR):
  reimplementa o Resilience4j e não compartilha estado com os breakers que a
  aplicação já tem.
- Resilience4j direto no `inbox-core`: quebra a regra de core sem
  dependências, e quem não usa circuit breaker carregaria a lib.
- Circuit breaker distribuído (estado no Redis): uma instância que ainda não
  viu a falha pode abrir por causa de outra, mas isso exige store e consistência
  para um ganho de segundos.
- Abrir/fechar manual por endpoint: fica fora até alguém precisar. O
  Resilience4j já tem `transitionToForcedOpenState`, se for preciso.

## Consequências

- Com a dependência fora, as mensagens ficam na fila principal, onde
  estavam, em vez de irem para a DLQ.
- O lag da fila cresce enquanto o circuito está aberto. O alerta de idade da
  mensagem (ADR-0007) precisa considerar `inbox_circuit_state`.
- Uma mensagem envenenada que falha sempre com exceção `Retryable` também pode
  abrir o circuito se for a maioria da janela. `minimum-number-of-calls` e
  `NonRetryable` bem configurados reduzem esse risco.
- Compartilhar a instância com o cliente HTTP faz o consumer pausar também por
  falhas que não vieram dele. É o comportamento desejado quando a dependência
  é a mesma.
- Suporte do Resilience4j a Spring Boot 4: conferido no spike da F0
  (ADR-0006). O `resilience4j-spring-boot4` 2.4.0 existe e converge com o
  Boot 4.1.1. O `inbox-resilience4j` continua sem depender de Spring.
