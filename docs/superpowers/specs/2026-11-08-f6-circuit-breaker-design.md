# F6 (parte 1): circuit breaker — RASCUNHO

- Data: 2026-11-08
- Estado: **rascunho, pausado a pedido**. A brainstorming parou na Seção 1 (apresentada, sem aprovação). As Seções 2
  e 3 são propostas ainda não apresentadas. Retomar pela aprovação da Seção 1, depois spec final, plano e execução.
- Fase: F6 do `messaging-inbox/docs/plan.md`, primeiro item (os outros: batch, stale check, expiração; lanes já
  existem)
- Decisão base: ADR-0010 (circuit breaker), ADR-0002 (nível 3 só com o circuito fechado), ADR-0004 (regra do
  `in_progress`), ADR-0007 (métricas)

## Objetivo

Quando a dependência do handler cai, o consumer **para de buscar mensagens** em vez de queimar os níveis 1 e 2 e encher
a DLQ; quando ela volta, retoma sozinho. Funciona igual nos seis brokers (provado pelo contrato), desligado por padrão,
ligado por receiver.

## Decisões tomadas

| Decisão | Escolha | Origem |
|---|---|---|
| engine-tax neste ciclo | **Só a lib.** O engine-tax não liga o circuit breaker; em seguida desenhamos o que ajustar lá | usuário, 2026-11-08, opção (a) |
| Embalagem | **B**: gancho `ConsumptionGate` no core; adaptador do Resilience4j como classe **interna** do starter, dependência opcional. Vira módulo próprio (`messaging-inbox-resilience4j`) só quando houver usuário sem Spring | conselho de 2026-11-08 (Skeptic, Pragmatist e Critic contra a A inicial); usuário: "sim, segue com a B" |
| Compartilhar instância | **Uma instância por receiver por padrão.** Compartilhar só com `name` explícito; o README avisa que outros usos da instância gastam as permissões do meio aberto e abrem/fecham o circuito do consumer | conselho (premissa contestada) |
| Kafka | Fora do escopo (F4 pendente) | ADR-0010 |
| Versão do Resilience4j | **Em aberto.** Padrão proposto: 2.2.0 (a única no repositório local; builds offline), dependência opcional, testada nessa versão. A ADR cita 2.4.0 | pergunta ao usuário sem resposta |

Revisão necessária na ADR-0010: divisão por módulo (B no lugar do módulo `inbox-resilience4j`) e regra de
compartilhamento.

## Seção 1: core (apresentada, aguardando aprovação)

### Gancho

`ConsumptionGate`, público em `com.example.messaging.inbox`, sem dependências: os métodos da ADR (`state()`,
`onStateChange`, `tryAcquire`, `onSuccess`, `onError`, `ignore`) mais `Duration retryDelay()` (o atraso de quem volta
com o circuito aberto; o starter dá o `wait-duration-in-open-state`). `ConsumptionGate.NONE` = sempre fechado; sem
gancho nada muda.

### Pipeline (nível 1)

- `tryAcquire()` antes de **cada** execução do handler, inclusive as repetições do nível 1.
- `false` → não roda as tentativas restantes; a mensagem vira `Retry(retryDelay, mesmas tentativas,
  RetryReason.CIRCUIT_OPEN)`. `CIRCUIT_OPEN` segue a regra existente do `IN_PROGRESS`
  (`Outcome.RetryReason`): não gasta tentativa em `hold` e `retry-queues`; em `broker-delay` o `deliveryCount` cresce
  (aceito pela ADR). `outcome=circuit_open` nas métricas.
- Depois da execução, pela tabela da ADR:

| Resultado | Chamada |
|---|---|
| Sucesso, `Discard` | `onSuccess` |
| Exceção retentável, `SkipImmediate`, `RetryAfter` com `cause` | `onError` |
| `NonRetryable`, `Reject`, `RetryAfter` sem `cause` | `ignore` |
| `duplicate`, `stale`, `expired`, `in_progress` | nenhuma (o handler não rodou) |

### Laço dos workers (`Worker` / `InboxConsumer`)

Trava interna alimentada por `onStateChange`:

| Estado | Comportamento |
|---|---|
| Fechado | Normal |
| Aberto | Nenhum worker chama `receive`; esperam a mudança de estado (ou a parada) |
| Meio aberto | Só o worker 0 recebe, com `maxMessages = 1`; os outros esperam |

Depende da transição automática aberto → meio aberto (com o consumo parado, ninguém chama o breaker): o starter
exige `automatic-transition-from-open-to-half-open-enabled` (startup falha sem ela, como na ADR).

### Mensagens em mãos quando o circuito abre

Num lote, as próximas recebem `tryAcquire = false` e voltam pela estratégia do nível 2 (acima). Em `hold`, a espera
dura até o circuito sair de aberto, com o lease renovado como o `Worker` já faz. O handler em execução termina
normalmente.

### Nível 3 (`DeadLetterPass`)

- A passada só começa com o circuito **fechado**; senão é pulada e registrada com `end=circuit_open`.
- Mensagem da passada com `CIRCUIT_OPEN` → volta ao fim da DLQ sem somar `l3`, e a passada termina.
- `PassEnd` ganha `CIRCUIT_OPEN`.

### Observabilidade

`ConsumerObserver.circuitStateChanged(state)` (default vazio, repassado pelo `SafeObserver`).

## Seção 2: starter (proposta, não apresentada)

- Configuração (ADR-0010): `messaging.receivers.<n>.circuit-breaker: { enabled, name }` (`name` padrão = nome do
  receiver), também em `receiver-defaults`. Hoje a chave é recusada com "not supported yet" (`InboxConfig`): passa a
  valer.
- Adaptador interno `Resilience4jConsumptionGate` sobre `io.github.resilience4j.circuitbreaker.CircuitBreaker`:
  `tryAcquirePermission`, `onSuccess`, `onError`, `releasePermission`, `getEventPublisher().onStateTransition`;
  estados `DISABLED`/`FORCED_OPEN`/`METRICS_ONLY` mapeados explicitamente (a definir: `FORCED_OPEN` → aberto,
  `DISABLED`/`METRICS_ONLY` → fechado).
- Registry: bean `CircuitBreakerRegistry` da aplicação se existir; senão um próprio. Instância inexistente: criada com os
  padrões da ADR-0010.
- `enabled: true` sem o Resilience4j no classpath → startup falha com mensagem clara.
- Toda referência a tipos do Resilience4j fica atrás de `@ConditionalOnClass`/classe carregada só com ele presente.
- Métricas (ADR-0007): `inbox_circuit_state{circuit}` (0/1/2) e `inbox_circuit_transitions_total{circuit,to}`; as do
  `resilience4j-micrometer`, se existirem, não são duplicadas.

## Seção 3: testes (proposta, não apresentada)

1. Core (InMemory, gate falso escrito com a semântica do Resilience4j: permissão negada, meio aberto com N permissões):
   aberto → nenhum `receive`; meio aberto → um worker, uma mensagem; fechado → normal; `tryAcquire=false` no meio do
   nível 1 → `Retry(CIRCUIT_OPEN)` sem gastar tentativa em `hold`/`retry-queues`; tabela de `onSuccess`/`onError`/
   `ignore`; passada do nível 3 não começa com o circuito aberto e termina com `CIRCUIT_OPEN`.
2. Starter: adaptador sobre um `CircuitBreaker` real (2.2.0); registry da aplicação reaproveitado; transição automática
   desligada → startup falha; **contexto sem o Resilience4j no classpath sobe** (sem `NoClassDefFoundError`);
   métricas.
3. Contrato nos seis brokers: `circuitOpenPausesConsumption` (abre → nada é recebido; fecha → consome) e, o risco que o
   Critic mandou verificar primeiro, **mensagem em mãos quando o circuito abre não cai na DLQ enquanto ele está aberto**
   (lease expirado e `deliveryCount` do broker: Service Bus lock, RabbitMQ/ActiveMQ redelivery).
4. `mvn clean install` verde (com `caffeinate`: o Mac dorme na bateria e derruba os testes de tempo).

## Riscos levantados pelo conselho

- Dependência opcional vazando para classes carregadas no startup → `NoClassDefFoundError` em apps sem Resilience4j
  (teste 2).
- Gancho desenhado sem implementação real → semânticas que o Resilience4j impõe (`FORCED_OPEN`, `DISABLED`, permissão
  negada que muda estado) — por isso o adaptador entra neste ciclo.
- Versão: app com outro Resilience4j escolhe a versão; a lib testa com 2.2.0.
- Meio aberto: `permitted-number-of-calls-in-half-open-state` maior que 1 com um worker e uma mensagem por vez só
  alonga o teste; avaliar se o starter deve avisar.
- Mensagem envenenada retentável pode abrir o circuito se for a maioria da janela (ADR-0010); `minimum-number-of-calls`
  e `NonRetryable` bem configurados reduzem.

## Depois deste ciclo: engine-tax

Desenhar o que ajustar no engine-tax (pedido do usuário): onde o circuit breaker faz sentido nos receivers do
`lifecycle-worker` e do `outcome-api`, e a relação com o disjuntor caseiro por SEFAZ do `gateway-api`
(`CircuitBreakers`): um circuito no consumer pausaria todas as UFs quando só uma cai.
