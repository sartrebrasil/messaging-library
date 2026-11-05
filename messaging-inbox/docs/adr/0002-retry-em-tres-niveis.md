# ADR-0002: Retry em três níveis, classificação de erro e filas de falha

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-11-05 (`deduplicationId` inválido vira hash; cópia impossível volta por `Release`)
- Revisão: 2026-10-07 (nível 3 na DLQ, fila `failed`, estratégias do nível 2 por capability,
  headers no formato da messaging-lib)
- Revisão: 2026-10-07 (`max-attempts` não conta a primeira execução, explicitado)
- Revisão: 2026-10-07 (classificação detalhada no ADR-0014; fingerprint e `inbox_redrive` nos atributos)
- Revisão: 2026-10-07 (marca de redrive dentro de `inbox_attempts`; `Release` conta tentativa em
  `broker-delay`; DLQ e `failed` do Kafka por campo explícito; passada só com circuito fechado)

## Contexto

Retry só em memória prende a thread e a partição por muito tempo. Retry só
pelo broker gera latência e custo para falhas que se resolvem em
milissegundos. Falhas longas, como uma dependência fora do ar por horas,
esgotam qualquer backoff razoável e enchem a DLQ com mensagens que dariam
certo mais tarde.

Cada provedor tem um mecanismo diferente de reentrega com atraso, e alguns não
têm nenhum. A messaging-lib informa isso em `Capabilities.delayedRedelivery`:

| Provedor | `delayedRedelivery` | `orderedDelivery` | `deliveryCount` |
|---|---|---|---|
| SQS standard | Sim (12 h desde o primeiro receive) | Não | Sim |
| SQS FIFO | Sim | Sim | Sim |
| Pub/Sub | Sim (600 s) | Com ordering | Só com dead letter policy |
| Service Bus `ABANDON` | Não | Não | Sim |
| Service Bus `RESCHEDULE` | Sim (cópia agendada) | Não | Sim (atributo `delivery_count`) |
| Service Bus com sessions | Não | Sim | Sim |
| ActiveMQ `IMMEDIATE` | Não | Sim | Sim |
| ActiveMQ `SCHEDULED` | Sim (cópia agendada, 12 h) | Não | Sim (atributo `delivery_count`) |
| RabbitMQ | Não | Não | Só em quorum queue; em classic queue, vazio na reentrega |

## Decisão

### Classificação

Toda exceção do handler é classificada nesta ordem:

1. `NonRetryable`: exceções configuradas em `retry.non-retryable`. O padrão
   inclui falha de desserialização, `IllegalArgumentException` e validação.
   Vai direto para `failed`.
2. `Retryable`: todo o resto.

O ADR-0014 completa a classificação: cadeia de causas, `@NonRetryable`,
`@SkipImmediate` (pula o nível 1), `RetryAfterHint`, modo allowlist e o
resultado explícito do handler (`RetryAfter`, `Reject`, `Discard`).

### Visão geral

```
fila principal:  nível 1 (memória) -> nível 2 (atraso) -> DLQ
DLQ:             nível 3: passada lenta -> nível 1 -> volta para o fim da DLQ ou vai para failed
failed:          só redrive manual (ADR-0012)
NonRetryable em qualquer nível -> failed
```

| | Nível 1 `immediate` | Nível 2 `delayed` | Nível 3 `dead-letter` |
|---|---|---|---|
| Onde espera | Thread do consumer | Broker, fila de retry ou thread (por estratégia) | DLQ, entre passadas |
| Escala | Milissegundos a poucos segundos | Segundos a minutos | Minutos a horas |
| Bom para | Falha transitória rápida | Dependência lenta ou fora do ar por pouco tempo | Indisponibilidade longa |
| Padrão | 3 tentativas, 100 ms, x2, máx. 2 s | 5 tentativas, 30 s, x2, máx. 15 min | 3 passadas, a cada 15 min |

Os nomes dos níveis 1 e 2 vêm do NServiceBus (*immediate retries* e *delayed
retries*).

### Nível 1: `immediate` (em memória)

Tentativas novas na mesma thread, com a mesma mensagem já recebida, sem
devolver nada ao broker. O lease é renovado por `keepAlive` durante todo o
nível 1 (ADR-0009), então o backoff não estoura o visibility timeout.

1. O handler falha com erro `Retryable`.
2. A thread espera o backoff curto.
3. O handler roda de novo com a mesma mensagem.
4. Sucesso: ack normal. O broker não fica sabendo da falha.
5. Esgotou `immediate.max-attempts`: passa ao nível 2 (na fila principal) ou
   volta para o fim da DLQ (no nível 3).

Atrasos com os padrões, sem jitter: 100 ms, 200 ms, 400 ms. Total até desistir:
cerca de 0,7 s mais o tempo das execuções.

### Nível 2: `delayed` (reentrega com atraso)

A mensagem volta a ser processada depois do atraso, passando pelo pipeline
inteiro de novo, inclusive pelo nível 1. Esgotou `delayed.max-attempts`: vai
para a DLQ.

Atrasos com os padrões, sem jitter: 30 s, 1 min, 2 min, 4 min, 8 min. Total
até a DLQ: cerca de 15,5 min.

#### Estratégias

| Estratégia | Como funciona | Ordem | Durável | Requisito |
|---|---|---|---|---|
| `broker-delay` | `receiver.nack(message, delay)` | Mantém onde o provedor mantém (SQS FIFO, Pub/Sub com ordering) | Sim | `delayedRedelivery = true` |
| `retry-queues` | Cópia para uma fila de retry com TTL fixo, que devolve para a principal ao expirar; depois `ack` da original | Quebra | Sim | Filas declaradas em `delayed.queues` |
| `hold` | A thread espera com o lease renovado e roda o pipeline de novo, sem devolver ao broker | Mantém | Não: em crash, a reentrega é imediata | Nenhum |

Escolha com `delayed.strategy: auto` (padrão):

| `ordering` | Condição | Estratégia |
|---|---|---|
| `strict` | `delayedRedelivery` e `orderedDelivery` | `broker-delay` |
| `strict` | Outros casos | `hold` |
| `relaxed` | `delayedRedelivery` | `broker-delay` |
| `relaxed` | `delayed.queues` configurado | `retry-queues` |
| `relaxed` | Outros casos | `hold` |

Uma estratégia explícita é validada no startup: `broker-delay` sem a
capability, `retry-queues` sem filas ou com `ordering: strict`, e `strict` com
`broker-delay` onde `orderedDelivery = false` fazem o startup falhar.

Na prática, por provedor:

| Provedor | `auto` em `relaxed` | Recomendação |
|---|---|---|
| SQS, Pub/Sub | `broker-delay` | Padrão |
| Service Bus | `hold` | `redelivery: RESCHEDULE` no destino da messaging-lib, se a ordem não importa (vira `broker-delay`) |
| ActiveMQ | `hold` | Artemis: `redelivery: SCHEDULED` (vira `broker-delay`). Classic: só com `schedulerSupport="true"` no broker |
| RabbitMQ | `hold` | `retry-queues`, com quorum queues |

#### `broker-delay`

- O atraso é limitado por `Capabilities.maxRedeliveryDelay`. Sem
  `delayed.max` explícito, o padrão é `min(15 min, maxRedeliveryDelay)`; no
  Pub/Sub fica 10 min. Um `delayed.max` explícito acima do limite faz o
  startup falhar.
- No SQS o limite é por mensagem (12 h desde o primeiro receive). Se o `nack`
  lançar `IllegalArgumentException` por isso, a mensagem vai para a DLQ.

#### `retry-queues`

```yaml
retry:
  delayed:
    strategy: retry-queues
    queues:                        # destinos da messaging-lib
      - { delay: 30s, destination: orders-retry-30s }
      - { delay: 2m,  destination: orders-retry-2m }
      - { delay: 15m, destination: orders-retry-15m }
```

- A inbox escolhe a menor faixa com `delay` maior ou igual ao atraso
  calculado; acima da maior faixa, usa a maior.
- A topologia é da infra (Terraform ou definição do broker). No RabbitMQ: uma
  fila por faixa com `x-message-ttl` igual ao `delay` e
  `x-dead-letter-exchange` apontando para a fila principal. O TTL precisa ser
  **por fila**: o TTL por mensagem só expira na cabeça da fila e trava as que
  estão atrás. A lib não consegue verificar o TTL; o README traz o exemplo de
  definição.
- O jitter não se aplica: as cópias enviadas juntas expiram juntas.
- Plugin `rabbitmq-delayed-message-exchange` foi descartado: fica fora do core
  do RabbitMQ, guarda as mensagens agendadas num nó só e não é indicado para
  volume alto.

#### `hold`

- A thread fica com a mensagem, renovando o lease, durante o atraso. Depois
  roda o pipeline de novo, sem chamar o broker. As tentativas são contadas em
  memória.
- Ocupa um slot de in-flight durante a espera: `max-in-flight` no ActiveMQ,
  prefetch no RabbitMQ, a session inteira no Service Bus com sessions.
- No RabbitMQ, o `consumer_timeout` da fila (padrão 30 min) precisa ser maior
  que a soma dos atrasos do nível 2 mais o tempo de processamento. Senão o
  broker fecha o channel e devolve todas as mensagens dele.
- No shutdown, a espera é interrompida e a mensagem volta com `nack(0)`
  (ADR-0009).

### Nível 3: `dead-letter` (passada lenta na DLQ)

A DLQ não é o fim da linha. Um consumer interno, `<consumer>.dead-letter`,
lê a DLQ em passadas espaçadas e reprocessa as mensagens com o mesmo handler.

1. A cada `dead-letter.interval` (padrão 15 min), começa uma passada com
   `passStart = now`.
2. Cada mensagem passa pelo pipeline com o nível 1, mas **sem** o nível 2.
3. Sucesso: `ack`.
4. Falha `Retryable` com `l3 < dead-letter.max-attempts`: cópia para o **fim**
   da DLQ com `l3 + 1`, depois `ack` da original.
5. Falha `Retryable` com `l3 = dead-letter.max-attempts`: vai para `failed`
   com `reason=retries_exhausted`.
6. Falha `NonRetryable`: vai para `failed`.

A passada termina, e a mensagem que a encerrou volta com `nack(0)`, quando:

- chega uma mensagem com `enqueuedAt >= passStart` (cópia desta passada ou
  mensagem que entrou durante ela);
- chega uma mensagem cuja chave de idempotência (ADR-0004) já apareceu nesta
  passada. É a proteção quando o provedor não informa `enqueuedAt` (RabbitMQ
  sem `timestamp`) ou o relógio dele está atrasado;
- `dead-letter.max-per-pass` (padrão 100) mensagens foram processadas;
- o circuit breaker abre (ADR-0010).

Regras:

- `dead-letter.concurrency` padrão 1: a passada não deve competir com a fila
  principal pela dependência que está se recuperando. Pelo mesmo motivo, a
  passada só começa com o circuit breaker fechado (ADR-0010).
- A lógica da passada fica no core, igual para os dois adapters (ADR-0001).
- A DLQ é sempre um destino comum, enviado por `MessageSender` (cópia + ack).
  A DLQ nativa do Service Bus não serve: não dá para enviar para
  `$DeadLetterQueue`, então a mensagem que falha no nível 3 não teria como
  voltar para a fila.
- `dead-letter` é opcional. Sem ele, o nível 3 fica desligado e o esgotamento
  do nível 2 vai direto para `failed`. `dead-letter.max-attempts: 0` mantém a
  DLQ como fila de estacionamento, sem passadas.
- `failed` é obrigatório: sem ele, o startup falha.
- Em SQS FIFO, a DLQ e a `failed` precisam ser FIFO: a cópia leva a
  `orderingKey`.

### Execuções no pior caso

Com os padrões, uma mensagem que sempre falha executa o handler:

```
fila principal: (1 + 3) x (1 + 5) = 24
DLQ:            (1 + 3) x 3       = 12
total:                              36
```

O nível 3 repete o nível 1, mas não o nível 2. Com `0` em `max-attempts` de
qualquer nível, o nível fica desligado.

### Semântica de `max-attempts`

`max-attempts` conta **tentativas novas**, nunca a primeira execução, em todos
os níveis:

| Nível | `max-attempts: 3` significa |
|---|---|
| `immediate` | Até 3 execuções a mais na mesma entrega (4 no total) |
| `delayed` | Até 3 reentregas com atraso depois da primeira entrega |
| `dead-letter` | Até 3 passadas pela DLQ |

Fórmula: `execuções = (1 + immediate) x (1 + delayed) + (1 + immediate) x dead-letter`.

A acme-messaging, incorporada a este projeto, contava a primeira execução no
`immediate` (`3` = 3 execuções, mínimo 1). Foi descartado: a mesma palavra
teria significados diferentes por nível, e `0` não desligaria o nível 1. O
`RetryPolicy` e o `ImmediateRetrier` portados de lá passam a aceitar `0` e a
parar em `n > max-attempts`.

### Contagem de tentativas

Uma fórmula serve para todas as estratégias:

```
l2 = l2 do atributo inbox_attempts (0 se ausente)
   + (deliveryCount - 1)            (0 se o provedor não informa)
   + tentativas em memória do hold
```

- `broker-delay`: o `deliveryCount` cresce a cada `nack`. Nas cópias de
  `RESCHEDULE` e `SCHEDULED`, a messaging-lib mantém a contagem pelo atributo
  `delivery_count`.
- `retry-queues`: cada cópia é uma mensagem nova (`deliveryCount = 1`), e a
  contagem vai no atributo.
- `hold`: em memória. O `deliveryCount` só cresce em crash ou perda de lease,
  e essas entregas também contam como tentativa.
- `broker-delay` sem `deliveryCount` (Pub/Sub sem dead letter policy): não dá
  para contar. A mensagem vai para a DLQ depois do nível 1, com log de erro e
  `inbox_retry_skipped_total{reason="delivery_count_unavailable"}`. Para ter
  nível 2 no Pub/Sub, a subscription precisa de dead letter policy.
- Toda entrega pelo broker conta, inclusive as de `in_progress` (ADR-0004) e
  as que voltam por `Release` (`nack(0)` no shutdown, ADR-0009). Em
  `broker-delay`, cada rolling deploy pode gastar uma tentativa do nível 2 das
  mensagens que estavam em mãos. A inbox não desconta: guardar os `Release` num
  store para descontar depois custaria um store em todo consumer para um caso
  raro. Em `hold` e `retry-queues` o efeito é o mesmo, porque o
  `deliveryCount` também entra na fórmula.

### DLQ nativa do broker

A política nativa (`maxReceiveCount`, `MaxDeliveryCount`,
`maxDeliveryAttempts`, `maximumRedeliveries`, `delivery-limit`) continua
configurada como rede de segurança para crash e bug. Ela precisa permitir mais
entregas que a inbox, senão o broker tira a mensagem antes:

| Estratégia | Entregas na fila principal por mensagem |
|---|---|
| `broker-delay` | `1 + delayed.max-attempts` (6 com os padrões) mais as de `in_progress` e de `Release` |
| `hold`, `retry-queues` | 1 por mensagem física, mais as de crash e de `Release` |

`Release` acontece a cada shutdown com mensagem em mãos. Com rolling deploys
frequentes, a política nativa precisa de folga além do mínimo; o README
recomenda o dobro.

A lib não enxerga essa configuração. O README traz a tabela de valores mínimos
por provedor, e os valores padrão de cada broker são conferidos na F3.

### Cópias e atributos

Toda cópia (retry-queues, DLQ, reenfileiramento do nível 3, `failed`) leva:

- corpo, `contentType`, `orderingKey` e `traceparent` originais;
- todos os atributos do usuário, inclusive os `ce_*` do envelope (ADR-0003);
- `deduplicationId` = `messageId` da mensagem de origem. Assim, se o envio for
  repetido depois de uma falha antes do `ack`, a cópia é deduplicada onde o
  provedor deduplica. É a mesma regra do `deadLetter` da messaging-lib;
- os atributos da inbox:

| Atributo | Valor | Onde |
|---|---|---|
| `inbox_attempts` | `l2=<n>;l3=<n>`, mais `;redrive=<redriveId>` depois de um redrive (ADR-0012) | Toda cópia |
| `inbox_failure` | `consumer=<c>;destination=<d>;exception=<classe>;fingerprint=<hex>;first_failure_at=<ISO-8601>` (fingerprint do ADR-0017) | DLQ, nível 3 e `failed` |
| `dead_letter` (reservado da messaging-lib, via `OutgoingMessage.withDeadLetter`) | `<messageId de origem>;<mensagem da exceção>` | DLQ e `failed` |

- A marca de redrive fica dentro de `inbox_attempts` e não num atributo
  próprio: assim ela acompanha a mensagem em todas as estratégias (o `nack`
  preserva os atributos, e as cópias reescrevem `inbox_attempts`) sem gastar
  orçamento.
- A messaging-lib limita cada valor a 1024 bytes ASCII. Se o `inbox_failure`
  passar disso, `exception` usa o nome simples da classe; se ainda passar,
  `destination` e `consumer` são cortados no fim. O `dead_letter` é cortado pela
  messaging-lib.
- `dead_letter` é reservado e não conta no limite de 16 atributos do usuário
  (ADR-0004 da messaging-lib).

Os nomes seguem o formato da messaging-lib (`[a-z_][a-z0-9_]*`). Os nomes com
hífen da versão anterior (`inbox-attempt`, `inbox-exception-class`, ...) seriam
recusados pelo `OutgoingMessage`. O Kafka usa os mesmos nomes nos headers.

A mensagem da exceção pode conter dado do payload. `retry.include-exception-message`
(padrão `true`) permite tirá-la do `dead_letter`.

Limite de 16 atributos da messaging-lib: o produtor pode usar até 14, para
sobrar espaço para os dois da inbox. Quando a cópia passaria do limite:

1. `inbox_failure` sai primeiro.
2. Se ainda passar, `inbox_attempts` sai, e a cópia não pode mais carregar a
   contagem: em `retry-queues` a mensagem vai para a DLQ, e no nível 3 vai para
   `failed`. Nunca há loop sem contagem.
3. Métrica `inbox_attribute_budget_exceeded_total`.

Os atributos `inbox_*` não aparecem em `InboxMessage.headers()`. O handler vê
a tentativa em `InboxMessage.delivery()` (`level`, `attempt`).

### Falha ao executar o efeito

Revisão de 2026-11-05:

- `deduplicationId` = `messageId` de origem quando ele cabe nas regras de
  envio (até 128 caracteres ASCII imprimíveis, sem espaço); senão, o SHA-256
  hexadecimal dele (64 caracteres). O RabbitMQ aceita qualquer `message-id`
  do produtor.
- Mensagem que não pode ser copiada (corpo vazio, `orderingKey` recebido fora
  das regras de envio, como um `JMSXGroupID` ou `SessionId` longo): log de
  erro e `Release`, sem `ack`. A política nativa de DLQ do broker encerra a
  repetição. O `orderingKey` não é descartado, porque uma `failed` FIFO precisa
  dele.

- A cópia é enviada **antes** do `ack` da original. Uma falha entre os dois
  gera duplicata, nunca perda (mesma regra da messaging-lib, ADR-0005).
- Se o envio da cópia falhar, a original não é confirmada e volta pelo
  vencimento do lease ou por `nack(0)`. Métrica `inbox_effect_failed_total`.
- `LeaseExpiredException` no `ack`: a mensagem será reentregue. A
  idempotência (ADR-0004) transforma a reentrega em `duplicate`.

### Backoff e jitter

Fórmula (mesma da outbox-library, mais jitter), nos níveis 1 e 2:
`delay = min(initial * multiplier^attempt, max) * random(1 - jitter, 1)`.
`jitter` vale 0.2 por padrão e aceita de 0 a 1.

Jitter é uma variação aleatória aplicada ao atraso. Sem ele, 1000 mensagens
que falharam juntas (o banco caiu por 1 s) esperam exatamente o mesmo tempo,
voltam juntas e derrubam o banco de novo. É o efeito manada (*thundering
herd*). Com jitter 0.2, cada uma sorteia um fator entre 0.8 e 1.0:

| Tentativa | Sem jitter | Com jitter 0.2 |
|---|---|---|
| 1 | 100 ms | entre 80 e 100 ms |
| 2 | 200 ms | entre 160 e 200 ms |
| 3 | 400 ms | entre 320 e 400 ms |

- `0`: sem variação.
- `0.2` (padrão): até 20% a menos; espalha bem e mantém o atraso previsível.
- `1`: entre 0 e o atraso total ("full jitter", como descrito pela AWS).

O sorteio só reduz o atraso, nunca aumenta. `max` é o teto real.

### Kafka

| Tema | `strict` | `relaxed` |
|---|---|---|
| Nível 2 | Seek + pausa da partição (equivale a `hold`) | Retry topics `<topic>.retry` (equivale a `retry-queues`) |
| Contagem | Em memória; perdida em rebalance | Header `inbox_attempts` |
| DLQ | Tópico declarado em `dead-letter`, lido pelo nível 3 num consumer group próprio | Igual |
| `failed` | Tópico declarado em `failed` | Igual |

No Kafka, `dead-letter` e `failed` são nomes de tópico declarados no consumer,
como os destinos do broker `messaging` (ADR-0008). As regras acima valem sem
exceção: `dead-letter` opcional, `failed` obrigatória. Só os retry topics do
`relaxed` são derivados do tópico principal, por `retry-topic-suffix`.

No nível 3 do Kafka, `enqueuedAt` é o timestamp do record.

### Configuração

```yaml
inbox:
  consumers:
    billing-orders:
      destination: orders
      dead-letter: orders-dlq
      failed: orders-failed
      retry:
        immediate:   { max-attempts: 3, initial: 100ms, multiplier: 2, max: 2s, jitter: 0.2 }
        delayed:     { max-attempts: 5, initial: 30s, multiplier: 2, max: 15m, jitter: 0.2, strategy: auto }
        dead-letter: { max-attempts: 3, interval: 15m, max-per-pass: 100, concurrency: 1 }
        non-retryable: [com.acme.InvalidOrderException]
        include-exception-message: true
```

## Alternativas descartadas

- `RetryTemplate` do Spring Framework 7: o core não depende de Spring, e a
  política tem poucas linhas.
- Nível 3 com `nack(0)` na DLQ: a mensagem voltaria na hora e seria
  reprocessada na mesma passada.
- DLQ nativa do Service Bus como nível 3: não dá para reenfileirar nela.
- Repetir o nível 2 dentro do nível 3: 120 execuções no pior caso com os
  padrões, e o nível 3 já é o atraso longo.
- Agendamento em banco para o nível 2: exige banco em todo consumer e duplica
  a outbox-library.

## Consequências

- Mesma configuração de retry em todos os brokers. O que muda por provedor é
  só a estratégia escolhida no startup, registrada no log.
- O nível 3 segura falhas longas sem intervenção, e só falhas definitivas ou
  muito persistentes chegam à `failed`.
- Mensagens do nível 3 chegam horas depois e fora de ordem: o stale check
  (ADR-0013) evita aplicar uma versão velha de um aggregate.
- Produtores com mais de 14 atributos perdem os metadados de falha nas cópias.
