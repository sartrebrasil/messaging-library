# messaging-loadtest: teste de carga do `InboxConsumer` — design

## Intenção

Achar gargalos do listener (`messaging-inbox`) e chegar a profiles de execução recomendados por broker. O teste produz carga com o `MessageSender`, sobe o `InboxConsumer` real e mede onde o tempo vai. Segue o `outbox-loadtest` (outbox-library): profiles em `.properties`, uma fixture por broker, relatório JSON/CSV, só roda com `-Ploadtest`.

## Objetivos

- Carga customizável: volume, produtores, chaves de ordenação, tamanho do payload, taxa (aberta ou máxima) e *preload* (enche a fila antes de iniciar o consumer, para isolar o throughput do consumo).
- Consumer configurável por profile: todo o `ConsumerSettings` (`concurrency`, `maxMessagesPerPoll`, `keyLanes`, `pollWait`, `ordering`, `strategy`) e a `RetryPolicy`.
- Handler que simula trabalho real. Padrão = caso perfeito (latência 0, nenhuma falha).
- Brokers nesta entrega: `inmemory` (linha de base), `rabbit`, `sqs` (LocalStack), `jms-classic` (ActiveMQ Classic, via `messaging-jms`; Artemis fica para depois). Somar um broker = implementar `BrokerFixture` e registrar em `create`.
- Relatório que aponta o gargalo, sem exigir que se conheça os nomes das métricas.

## Não objetivos

- Sem limiar de throughput/latência: não é gate de build (falha só se o profile não drenar no `timeoutSeconds`).
- Sem Azure/GCP agora, sem `main`/CLI contra broker externo, sem Spring, sem rodar no CI normal.
- Sem alterar `messaging-inbox`: a medição entra pelo `ConsumerObserver`.

## Antiobjetivo

Um resultado bonito que não é comparável entre execuções, ou que mede o próprio harness (contenção no tracker, handler que serializa). Por isso a falha é determinística por `seed` e as estruturas de medição são lock-free.

## Abordagens consideradas

| | |
|---|---|
| **A. Espelho do outbox** (escolhida) | Módulo `messaging-loadtest`; IT fino sobre um `LoadRunner` independente de JUnit. |
| B. A + `main` para broker externo | Descartada por ora; o runner sem JUnit deixa a porta aberta. |
| C. JMH | Descartada: mede código isolado, não fluxo de broker com consumidores concorrentes. |

## Desenho

**Módulo** `messaging-loadtest` (novo `<module>` no pom raiz). Dependências: `messaging-inbox`, `messaging-testkit`, `messaging-aws`, `messaging-rabbitmq`, `messaging-jms`, testcontainers (rabbitmq, localstack, ActiveMQ). Surefire exclui `*IT`; failsafe com `skipITs=true`, ligado por `-Ploadtest`; propriedades `loadtest.profiles`, `loadtest.profilesDir`, `loadtest.reportDir` como no outbox. Um container por tipo de broker, reaproveitado entre os profiles do mesmo broker.

**Classes** (pacote `com.example.messaging.loadtest`, em `src/test`, como no outbox): `LoadProfile` (properties), `BrokerFixture` (+ implementações), `LoadRunner`, `HandlerModel`, `Tracker`, `MessagingLoadIT`.

**`BrokerFixture`** entrega o necessário para montar `Destinations`: `MessageSender` do principal, `Supplier<MessageReceiver>` (um por worker), `failed`, `deadLetter` e bandas de `retryQueues`, mais o `Capabilities` de cada adapter. As filas e a configuração de redelivery/lease/DLQ seguem as das `*InboxContractTest` de cada adapter (são o modelo de como montar cada broker). O profile pode ajustar `lease`.

**Produção**: `producers` threads enviam `messages` mensagens no formato de envelope da inbox, com `orderingKey` entre `orderingKeys` chaves (uma chave = uma tarefa, em ordem, como o agregado do outbox), `payloadBytes`, e `occurredAt` no corpo para medir latência ponta a ponta. `ratePerSec=0` = o mais rápido possível. `preload=true` envia tudo antes de `consumer.start()`.

**Handler (`HandlerModel`)**, tudo opcional:
- `handlerLatency`: `fixed:20ms` | `uniform:5-50ms` | `lognormal:p50=20ms,p99=200ms` (padrão `0`).
- `transientFailureRate` + `transientFailAttempts`: fração que falha nas N primeiras tentativas e depois passa (exercita retry imediato e atrasado).
- `poisonRate`: fração que falha sempre e termina em `failed`/DLQ.
- A decisão de falha é um hash de (`seed`, id da mensagem), não um sorteio por entrega; o resultado é reproduzível. Latência usa gerador por thread (sem contenção).
- `consume=false` roda só a produção.

**Medição**: `ConsumerObserver` (`received`, `handled`, `delivered`, `receiveFailed`, `immediateRetry`) alimenta o `Tracker`, que registra, por execução: throughput de produção e de consumo, latência ponta a ponta (p50/p95/p99/max), duração do handler, duração do efeito (ack/copy), contagem por `Effect`/`Outcome` (ack, retry, failed, dead-letter), `receiveFailed`, duplicatas, fora de ordem por `orderingKey`, backlog amostrado a cada 500 ms e **utilização dos workers** (tempo de handler somado ÷ `concurrency` × tempo de parede). Utilização baixa com throughput baixo aponta o receive/ack ou o broker; utilização alta aponta o handler ou `concurrency` insuficiente. O relatório também despeja todas as métricas do observer, como o outbox faz com `relayMeters`.

**Relatório**: `target/loadtest/<timestamp>/report.json` e `summary.csv` (uma linha por profile, com as colunas de configuração relevantes e os principais resultados). O log mostra uma linha por profile.

**Profiles** em `src/test/resources/profiles/<broker>-<variação>.properties`; o nome do arquivo é o nome do profile. Chaves: `broker`, `messages`, `producers`, `orderingKeys`, `payloadBytes`, `ratePerSec`, `preload`, `consumer.concurrency`, `consumer.maxMessagesPerPoll`, `consumer.keyLanes`, `consumer.pollWaitMs`, `consumer.ordering`, `consumer.strategy`, `handlerLatency`, `transientFailureRate`, `transientFailAttempts`, `poisonRate`, `seed`, `timeoutSeconds`. Entrega inicial: um profile de caso perfeito e um com trabalho real por broker (inmemory, rabbit, sqs, jms-classic).

**Execução**:
```
mvn -pl messaging-loadtest -am verify -Ploadtest
mvn -pl messaging-loadtest -am verify -Ploadtest -Dloadtest.profiles=inmemory-perfeito,rabbit-real
```

## Decisões minhas (a conferir)

- JMS entra só como `jms-classic` por ora (`jms-artemis` adiado a pedido); o Classic usa `SCHEDULED` para o atraso do nível 2, como o contrato.
- Falha determinística por hash em vez de sorteio.
- Utilização dos workers como métrica de primeira classe.
- Código em `src/test`, como no outbox.

## Fica para quem constrói

Versões das imagens de container (seguir as já usadas nos testes dos adapters), nomes exatos de filas, formato do CSV, valores dos profiles iniciais, e como o `Tracker` obtém o id da mensagem para o hash de falha.
