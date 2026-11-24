# messaging-loadtest

Teste de carga do `InboxConsumer` (`messaging-inbox`) sobre brokers reais. Produtores enviam pelo `MessageSender`, um consumer real processa com um handler simulado, e o relatório mostra onde o tempo vai, para achar gargalos e chegar a profiles de execução por broker. Nos moldes do `outbox-loadtest`: é um relatório, não um gate de build. Só falha se um profile não drenar dentro do próprio `timeoutSeconds`.

## Como rodar

Precisa de Docker para `rabbit`, `sqs` (LocalStack) e `jms-classic`. O `-Dtest=none` evita que o `-am` rode os testes dos módulos de que este depende.

```
mvn -pl messaging-loadtest -am verify -Ploadtest -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false
mvn ... -Dloadtest.profiles=inmemory-perfeito,rabbit-real       # só alguns profiles
mvn ... -Dloadtest.profilesDir=/caminho/meus-profiles           # profiles de outro diretório
```

O relatório vai para `messaging-loadtest/target/loadtest/<timestamp>/` (`report.json` completo, `summary.csv` uma linha por profile).

## Profiles

Um profile é um `.properties` em `src/test/resources/profiles/`; o nome do arquivo é o nome do profile. Tudo é opcional; o padrão é o caso perfeito (handler instantâneo, nenhuma falha).

| Chave | Padrão | O que faz |
|---|---|---|
| `broker` | `inmemory` | `inmemory`, `rabbit`, `sqs`, `jms-classic` |
| `messages` | 1000 | total de mensagens |
| `producers` | 2 | threads produtoras |
| `orderingKeys` | 100 | chaves de ordenação (uma chave = uma tarefa, em ordem) |
| `payloadBytes` | 256 | tamanho do corpo |
| `ratePerSec` | 0 | taxa total de produção; 0 = o mais rápido possível |
| `preload` | false | enche a fila antes de iniciar o consumer (mede só o consumo); não combina com `ratePerSec` |
| `consume` | true | `false` só produz |
| `consumer.concurrency` | 1 | workers |
| `consumer.maxMessagesPerPoll` | 1 | mensagens por `receive` |
| `consumer.keyLanes` | 1 | lanes por chave |
| `consumer.pollWaitMs` | 1000 | espera do `receive` |
| `consumer.ordering` | `RELAXED` | `STRICT` ou `RELAXED` |
| `consumer.strategy` | `AUTO` | `AUTO`, `BROKER_DELAY`, `RETRY_QUEUES`, `HOLD` |
| `retry.delayMs` | 100 | atraso do nível 2 (2 tentativas); 1 retry imediato de 10 ms |
| `handlerLatency` | `0` | `fixed:20ms`, `uniform:5-50ms`, `lognormal:p50=20ms,p99=200ms` |
| `transientFailureRate` | 0 | fração que falha nas primeiras `transientFailAttempts` tentativas e depois passa |
| `transientFailAttempts` | 1 | |
| `poisonRate` | 0 | fração que falha sempre e termina na DLQ |
| `seed` | 1 | a falha é hash de (`seed`, id): mesma execução, mesmos alvos |
| `timeoutSeconds` | 120 | prazo para drenar |

`poisonRate + transientFailureRate` deve somar até 1, e `orderingKeys` não pode passar de `messages`.

## Resumo por broker e varredura

`./run-loadtest.sh [profile1,profile2]` roda o comando acima e imprime o resultado broker a broker. `./run-loadtest.sh sweep [rabbit,sqs,jms-classic]` gera profiles temporários que variam `concurrency` (1, 4, 16), `maxMessagesPerPoll` (1, 10) e `keyLanes` (4) com `preload=true` e handler lognormal p50=5ms/p99=50ms, e aponta a melhor combinação de cada broker. O JMS Classic não roda `maxMessagesPerPoll=10` (veja "Achados").

## Lendo o resultado

- `workerUtilization` (tempo de handler ÷ `concurrency` × tempo de parede) **baixo** com throughput baixo: o tempo está no receive, no ack ou no broker. **Alto** (perto de 1): o handler é o limite; suba `consumer.concurrency`.
- `maxBacklog` crescendo: o consumo é mais lento que a produção.
- `e2eMs` alto com `handlerMs` baixo: a mensagem espera na fila ou no retry.
- `terminalByEffect`: `ACKED`, `FAILED`, `DEAD_LETTERED`; `REDELIVERY_DELAYED`/`RETRY_QUEUED` são tentativas do nível 2.
- `duplicates` e `outOfOrder`: com várias `concurrency`/`keyLanes` e retries, a reordenação aparece aqui; não é falha do teste.

## Achados já medidos

- **ActiveMQ Classic, receive em lote:** com `consumer.maxMessagesPerPoll=10` o consumo cai para ~13 msg/s; com `1` sobe para ~1.700 msg/s (4 workers, handler instantâneo, mesma máquina). O limite está no receive do adapter `messaging-jms`, não no handler: a utilização dos workers fica em ~0. Os profiles `jms-classic-*` usam `1`; `jms-classic-multipoll` guarda o caso lento para comparar depois de uma correção.
- **RabbitMQ com `retry-queues`:** o `AUTO` escolhe `retry-queues` e a lib não confere se a fila de retry devolve ao `main`. A fixture a declara com TTL de 1 s e dead-letter de volta ao `main`; quem usar esse modo precisa configurar o mesmo, senão a mensagem fica parada na banda.
- **Retry atrasado:** o handler tem 1 retry imediato; para exercitar o nível atrasado, `transientFailAttempts` precisa ser 2 ou mais (os profiles `-real` usam 2).

## Somando um broker

Implemente `BrokerFixture` (cria as filas `<nome>-main|failed|dlq|retry` e devolve `Endpoints`) e registre em `BrokerFixture.create`. Falta por enquanto: ActiveMQ Artemis, Azure Service Bus, Pub/Sub.
