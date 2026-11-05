# ADR-0016: Lanes por chave de ordenação

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (orçamento do `keepAlive` pela maior lane; Kafka exige `ordering: relaxed`)

## Contexto

O paralelismo da inbox vem de `concurrency`: um receiver e uma thread por
worker (ADR-0009). Dentro de um worker, as mensagens de um `receive` (ou de um
`poll` do Kafka) são processadas em sequência.

Isso limita a vazão quando uma unidade de entrega carrega muitas chaves
independentes:

- Kafka: uma partição é processada por uma thread. Uma partição com milhares
  de aggregates anda no ritmo da mensagem mais lenta, e o número de partições
  limita o paralelismo.
- SQS FIFO: um `receive` traz até 10 mensagens de grupos diferentes, que hoje
  rodam uma depois da outra.
- Pub/Sub com ordering: um pull traz mensagens de várias `orderingKey`, e o
  ADR-0005 limita o `strict` a um receiver só.

A acme-messaging, incorporada a este projeto (ADR-0006), tinha
`ordering: key` com um keyed executor: lanes por hash da chave, em série
dentro da lane.

## Decisão

### Regra

- `key-lanes` por consumer, padrão 1 (desligado).
- Com `key-lanes: N > 1`, o worker separa as mensagens de cada `receive` (ou
  `poll`) por chave: `lane = hash(orderingKey) mod N`. No Kafka, a chave do
  record.
- Mensagens da mesma lane rodam em série, na ordem de chegada. Lanes rodam em
  paralelo.
- Mensagem sem chave vai para a lane com menos mensagens.
- O worker espera **todas** as lanes terminarem, aplica os efeitos no broker e
  só então faz o próximo `receive` (ou devolve o controle ao container do
  Kafka, que comita). Não há fila entre o `receive` e as lanes, então não há
  backpressure a controlar nem watermark de offset.
- `key-lanes` não muda `ordering` (ADR-0005). Muda só quantas chaves de uma
  mesma entrega rodam ao mesmo tempo.

### Com `ordering: strict`

- Uma mensagem com falha segura a sua lane: o nível 2 em `strict` é sempre a
  estratégia que mantém a ordem (ADR-0002). Em `hold`, a lane espera. Em
  `broker-delay`, as mensagens seguintes da mesma chave na mesma lane recebem o
  mesmo `Retry`, pela regra de arrasto do ADR-0011.
- Outras chaves que caíram na mesma lane por colisão de hash também esperam.
  Mais lanes, menos colisão.
- Kafka com `strict` e `key-lanes > 1`: o startup **falha** na v1. O nível 2 em
  `strict` no Kafka é seek com pausa da partição; dentro de uma lane, o seek
  reentregaria as mensagens que outras lanes já concluíram, e um `hold` na lane
  bloquearia o `poll` além do `max.poll.interval.ms`.

### Com `ordering: relaxed`

- Equivale ao `ordering: key` da acme-messaging: a ordem por chave vale entre
  as execuções da mesma entrega, e o nível 2 sem bloqueio (retry topic,
  `retry-queues`, `broker-delay`) tira a mensagem com falha da sequência.
- Kafka: o listener passa a receber o `poll` inteiro (modo batch do
  spring-kafka) para distribuir nas lanes. `max.poll.interval.ms` precisa ser
  maior que `max-processing` (ADR-0009); o startup valida.

### Threads

- `concurrency x key-lanes` platform threads por consumer. As lanes só rodam o
  handler, não o `receive`, mas o handler pode ter `synchronized` com I/O
  (drivers JDBC) e prender a carrier thread no Java 21. Virtual threads ficam
  para o Java 24+, como no ADR-0009.
- O `keepAlive` de cada mensagem é aberto logo após o `receive`, como hoje,
  com orçamento de `max-processing` vezes o tamanho da maior lane (ADR-0009):
  uma mensagem esperando na lane não perde o lease.

### Validação no startup

| Combinação | Resultado |
|---|---|
| `key-lanes > 1` com `batch.enabled` | Falha: o batch já agrupa a entrega inteira (ADR-0011) |
| Kafka, `strict`, `key-lanes > 1` | Falha. Como o Kafka é `strict` por padrão (ADR-0005), a mensagem de erro pede `ordering: relaxed` |
| `key-lanes > 1` com `max-messages-per-poll = 1` | Aviso: com uma mensagem por `receive` não há o que paralelizar |

### Configuração

```yaml
inbox:
  consumers:
    billing-orders:
      ordering: relaxed
      key-lanes: 8
      configuration:
        max-messages-per-poll: 10
```

## Alternativas descartadas

- Lanes com fila própria e `receive` contínuo, como no keyed executor da
  acme-messaging: exige créditos de backpressure e, no Kafka, commit por
  watermark (`asyncAcks`). A espera por todas as lanes antes do próximo
  `receive` dá a maior parte do ganho sem essas peças.
- `ordering: key` como terceiro modo: mistura duas decisões (paralelismo
  dentro da entrega e retry que bloqueia ou não). `key-lanes` com `strict` ou
  `relaxed` cobre as combinações.
- Kafka `strict` com lanes por seek parcial: a combinação volta a ser
  avaliada se o Kafka em `strict` precisar de mais vazão.

## Consequências

- Kafka em `relaxed` e SQS FIFO ganham paralelismo dentro da partição ou do
  `receive`, sem mais receivers.
- A entrega mais lenta de um `receive` atrasa o próximo `receive` do worker.
  Para handlers com duração muito variável, `concurrency` maior continua sendo
  a alternativa.
- `inbox_processing_duration` continua por mensagem. Não há métrica por lane.
