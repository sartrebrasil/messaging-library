# ADR-0008: Configuração de brokers e consumers (`configuration`)

- Status: aceito (2026-10-08)
- Data: 2026-10-02
- Revisão: 2026-10-07 (broker `messaging` no lugar de `sqs`)
- Revisão: 2026-10-07 (bloco `inbox.defaults`, vindo da acme-messaging)
- Revisão: 2026-10-07 (DLQ e `failed` do Kafka por campo explícito; `on-exhausted`)

## Contexto

Cada broker tem opções próprias (no Kafka, `max.poll.records` e
`max.poll.interval.ms`; no SQS, `poll-timeout` e `visibility-timeout`). A
aplicação precisa ajustá-las por broker e por consumer sem que a lib crie uma
propriedade tipada para cada opção de cada cliente.

O Spring Cloud Stream resolve isso com:

- `binders.<nome>` (várias instâncias, por exemplo dois clusters);
- `<binder>.binder.configuration` (propriedades cruas para todo o binder);
- `<binder>.bindings.<binding>.consumer.configuration` (propriedades cruas
  por binding);
- `ListenerContainerCustomizer` para o que não cabe em properties.

## Decisão

### Modelo

```yaml
inbox:
  brokers:
    kafka-a:
      type: kafka
      configuration:               # defaults de todos os consumers deste broker
        bootstrap.servers: a:9092
        max.poll.records: 100
    filas:
      type: messaging              # destinos de messaging.destinations (starter da messaging-lib)
      configuration:
        max-messages-per-poll: 1
  consumers:
    billing-orders:
      broker: kafka-a
      destination: orders          # tópico
      dead-letter: orders.dlt      # tópico; opcional (ADR-0002)
      failed: orders.failed        # tópico; obrigatório
      retry: {...}                 # comum a todo broker (ADR-0002)
      configuration:               # específico do broker; sobrescreve o do broker
        group: billing
        retry-topic-suffix: .retry
        max.poll.interval.ms: 600000
    nfe-events:
      broker: filas
      destination: nfe-events      # nome em messaging.destinations
      dead-letter: nfe-events-dlq  # idem
      failed: nfe-events-failed    # idem
      configuration:
        poll-wait: 20s
        max-processing: 10m
```

- Opções comuns a todos os brokers (`destination`, `retry`, `ordering`,
  `on-exhausted`, `envelope`, `concurrency`, `key-lanes`, `dead-letter`,
  `failed`, `idempotency`, `circuit-breaker`, `stale-check`, `expiration`,
  `batch`) são tipadas no consumer.
- `destination`, `dead-letter` e `failed` funcionam igual nos dois brokers:
  nomes de tópico no Kafka, nomes de `messaging.destinations` no `messaging`.
- No broker `messaging`, `destination`, `dead-letter`, `failed` e as filas de
  `retry.delayed.queues` são nomes de `messaging.destinations`. Endereço,
  provedor, credenciais e opções do provedor (`redelivery`, `sessions`,
  `lease`) ficam na messaging-lib; a inbox não repete nada disso.
- O `dead-letter` declarado no destino da messaging-lib (ADR-0005 dela) não é
  usado pela inbox: ela envia as cópias pelos senders de `dead-letter` e
  `failed` declarados no consumer.
- O que é específico do broker fica em `configuration`, um `Map<String, String>`
  que o adapter interpreta.
- O campo que referencia o broker chama `broker`. Não usamos `binder`, para não
  sugerir compatibilidade com o Spring Cloud Stream.

### Defaults dos consumers

Com vários consumers, a mesma política de retry, idempotência e circuit
breaker se repete em cada um. `inbox.defaults` define as opções comuns uma
vez:

```yaml
inbox:
  defaults:
    retry:
      immediate: { max-attempts: 3, initial: 100ms }
      non-retryable: [com.acme.InvalidOrderException]
    idempotency: { enabled: true, ttl: P7D }
    circuit-breaker: { enabled: true }
  consumers:
    billing-orders:
      destination: orders
      failed: orders-failed
      retry:
        immediate: { max-attempts: 5 }   # só isso muda; initial vem de defaults
```

- Precedência das opções comuns: `inbox.consumers.<nome>` > `inbox.defaults`
  > padrões da lib.
- A mescla é por propriedade, em profundidade: um consumer que define
  `retry.immediate.max-attempts` herda o resto de `retry` dos defaults.
- Listas são substituídas, não somadas: `non-retryable: []` no consumer limpa
  a lista dos defaults.
- Não são aceitas em `defaults`, porque são de um consumer só ou de um broker
  só: `destination`, `dead-letter`, `failed`, `retry.delayed.queues` e
  `configuration` (os defaults do broker ficam em
  `inbox.brokers.<nome>.configuration`). Se aparecerem, o startup falha.
- No starter, as properties dos consumers têm campos anuláveis (`null` = não
  definido), e a mescla é feita depois do binding. O binding do Spring Boot
  sozinho substitui o objeto inteiro.

### Precedência de `configuration` (da maior para a menor)

1. `inbox.consumers.<nome>.configuration`
2. `inbox.brokers.<nome>.configuration`
3. Autoconfig do Spring Boot (`spring.kafka.*`) e da messaging-lib
   (`messaging.*`)
4. Padrões da lib

### Brokers implícitos e múltiplos

- Sem bloco `inbox.brokers`, existem os brokers implícitos `kafka` (conexão
  do Spring Boot) e `messaging` (bean `MessagingDestinations`), cada um só
  quando a dependência está no classpath. Ninguém configura conexão duas
  vezes.
- Um consumer sem `broker` usa o único broker implícito disponível. Com os
  dois disponíveis, `broker` é obrigatório.
- Para só definir defaults, basta declarar `inbox.brokers.kafka.configuration`
  sem dados de conexão.
- Vários clusters ou contas (`kafka-a`, `kafka-b`) entram no MVP: o modelo de
  properties é barato, e mudá-lo depois quebraria a configuração de quem já
  usa a lib.

### Interpretação de `configuration` por adapter

| Adapter | Chaves da lib (kebab-case) | Passagem crua ao cliente | Chave desconhecida |
|---|---|---|---|
| Kafka | `group` (padrão = nome do consumer), `retry-topic-suffix` (padrão `.retry`, só em `relaxed`) | Chaves com ponto que constam em `ConsumerConfig.configNames()` | Falha no startup |
| messaging | `max-messages-per-poll` (padrão 1; com batch, o tamanho do batch), `poll-wait` (padrão 20 s), `max-processing` (padrão 15 min, ADR-0009), `shutdown-timeout` (padrão 30 s) | Não existe: opções do provedor ficam em `messaging.destinations` | Falha no startup |

A lista exata de chaves de cada adapter é fechada nas fases F3
(`inbox-messaging`) e F4 (Kafka) e documentada no README.

### Chaves protegidas

Chaves que a lib controla não podem ser definidas em `configuration`. Se
aparecerem, o startup **falha** com uma mensagem que diz qual chave usar no
lugar.

| Adapter | Chave protegida | Motivo |
|---|---|---|
| Kafka | `enable.auto.commit` | O ack é da lib |
| Kafka | `group.id` | Usar `group` |
| Kafka | `key.deserializer`, `value.deserializer` | A lib lê bytes e desserializa no pipeline (ADR-0003) |

### Validação

- Chave desconhecida ou protegida: o startup falha e a mensagem lista as
  chaves aceitas pelo adapter.
- Referência a um broker inexistente em `consumers.<nome>.broker`: o startup
  falha.
- `failed` ausente, em qualquer broker: o startup falha.
- Broker `messaging`: destino inexistente em `messaging.destinations`, ou
  combinação que as `Capabilities` não suportam (ADR-0002, ADR-0005): o
  startup falha.

### Escape programático

Para o que não dá para expressar em properties, existe o bean
`InboxKafkaContainerCustomizer`, que recebe o nome do consumer e o container
nativo, e roda depois da aplicação de `configuration`. O broker `messaging`
não tem container; o que for do provedor se ajusta no cliente nativo, que a
messaging-lib aceita como bean do usuário.

## Alternativas descartadas

- Bloco com o nome do broker (`kafka:`, `sqs:`): dá autocomplete na IDE, mas
  obriga a lib a criar uma propriedade tipada por opção de cliente e diverge
  do modelo do Spring Cloud Stream.
- Sobrescrever chaves protegidas com aviso no log: esconde erro de
  configuração.

## Consequências

- Não há autocomplete da IDE para `configuration`. A validação no startup
  compensa e impede que um erro de digitação seja ignorado.
- Novas opções do cliente Kafka funcionam sem release da lib.
