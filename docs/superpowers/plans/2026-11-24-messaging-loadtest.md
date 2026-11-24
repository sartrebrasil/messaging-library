# messaging-loadtest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Módulo `messaging-loadtest` que produz carga com `MessageSender`, sobe um `InboxConsumer` real e mede o listener, por profile e por broker (inmemory, rabbit, sqs, jms-classic; Artemis fica para depois).

**Architecture:** Espelho do `outbox-loadtest`: `LoadRunner` sem JUnit orquestra produção, consumer e medição; `MessagingLoadIT` (só com `-Ploadtest`) carrega profiles `.properties`, roda e grava `report.json`/`summary.csv`. Brokers entram por `BrokerFixture`. A medição usa o `ConsumerObserver` e um handler simulado (`HandlerModel`); nada em `messaging-inbox` muda.

**Tech Stack:** Java 21, JUnit 5 + AssertJ, Testcontainers, `messaging-inbox`/`-testkit`/`-aws`/`-rabbitmq`/`-jms`. Sem Jackson: o corpo é texto e o JSON do relatório é escrito à mão.

**Spec:** `docs/superpowers/specs/2026-11-24-messaging-loadtest-design.md`

**Commits:** só quando o usuário pedir; os passos abaixo não commitam.

## Global Constraints

- Pacote `com.example.messaging.loadtest`, todo em `src/test` do módulo, `packaging` jar (como o outbox).
- Surefire exclui `**/*IT.java`; failsafe com `skipITs=true`, ligado por `-Ploadtest`; propriedades de sistema `loadtest.profiles`, `loadtest.profilesDir`, `loadtest.reportDir` (padrão `target/loadtest`).
- Não é gate: o IT só falha se um profile não drenar dentro de `timeoutSeconds`. Nenhum limiar de throughput/latência.
- Padrão do profile = caso perfeito: `handlerLatency=0`, `transientFailureRate=0`, `poisonRate=0`.
- Falha por hash de (`seed`, id da mensagem), nunca sorteio por entrega. Estruturas de medição sem lock global.
- Nenhuma alteração em `messaging-inbox`, `messaging-core` ou nos adapters.
- Chaves de profile: `broker`, `messages`, `producers`, `orderingKeys`, `payloadBytes`, `ratePerSec`, `preload`, `consume`, `consumer.concurrency`, `consumer.maxMessagesPerPoll`, `consumer.keyLanes`, `consumer.pollWaitMs`, `consumer.ordering`, `consumer.strategy`, `retry.delayMs`, `handlerLatency`, `transientFailureRate`, `transientFailAttempts`, `poisonRate`, `seed`, `timeoutSeconds`.
- Corpo da mensagem: `id;key;seq;sentAtMs;padding` em UTF-8 (`id` = `<n>`, único por execução; `seq` cresce por `key`).

## Review Focus

- `preload=true` junto com `ratePerSec>0` é contraditório: `IllegalArgumentException` na validação do profile, antes de subir broker.
- `poisonRate + transientFailureRate > 1`, taxa negativa ou `orderingKeys > messages`: `IllegalArgumentException` com o nome da chave.
- `consume=false`: o runner só produz e termina sem esperar drenagem (`drained` reflete só a produção).
- Não drenar no prazo: `drained=false` com contagens parciais, consumer parado e broker liberado (nada de thread viva).
- Várias `concurrency` com `orderingKeys` e `consumer.ordering=RELAXED`: reordenação aparece em `outOfOrder`, não como falha.
- Broker de container indisponível (sem Docker): o IT falha com mensagem que nomeia o broker, sem ocultar os demais resultados já gravados.

---

### Task 1: Módulo, pom e `LoadProfile`

**Files:**
- Modify: `pom.xml` (raiz): `<module>messaging-loadtest</module>` depois de `messaging-inbox`; entrada no `dependencyManagement` não é necessária.
- Create: `messaging-loadtest/pom.xml`
- Create: `messaging-loadtest/src/test/java/com/example/messaging/loadtest/LoadProfile.java`
- Test: `messaging-loadtest/src/test/java/com/example/messaging/loadtest/LoadProfileTest.java`

**Interfaces:**
- Produces: `record LoadProfile(String name, Map<String,String> props)` com `static LoadProfile load(Path file)`, `static LoadProfile of(String name, Map<String,String> props)`, `String broker()`, `String str(String key, String def)`, `int num(String key, int def)`, `double rate(String key)` (0 se ausente), `boolean flag(String key, boolean def)`, `void validate()` (lança `IllegalArgumentException` citando a chave).

- [ ] **Step 1: Escrever `LoadProfileTest`** (surefire normal): `load` usa o nome do arquivo sem extensão; `num("messages", 1000)` devolve o padrão quando ausente; `validate()` rejeita `preload=true`+`ratePerSec=100` (mensagem contém `preload`), `poisonRate=0.7`+`transientFailureRate=0.5` (mensagem contém `poisonRate`), `poisonRate=-0.1`, `orderingKeys=50`+`messages=10` (mensagem contém `orderingKeys`); um profile só com `broker=inmemory` valida.
- [ ] **Step 2: Rodar e ver falhar**: `mvn -pl messaging-loadtest -am test -Dtest=LoadProfileTest` → FAIL (módulo/classe inexistente).
- [ ] **Step 3: Criar `messaging-loadtest/pom.xml`** copiando a estrutura do `outbox-loadtest/pom.xml` (outbox-library): pai `messaging-parent`, `skipITs=true`, `loadtest.profiles` vazio, `loadtest.profilesDir`, surefire excluindo `**/*IT.java`, failsafe com as três propriedades de sistema, profile `loadtest` com `skipITs=false`. Dependências: `messaging-inbox`, `messaging-testkit`, `messaging-aws`, `messaging-rabbitmq`, `messaging-jms` (compile, como o outbox faz com os adapters), `junit-jupiter`, `assertj-core`, `testcontainers` + `rabbitmq` + `localstack` (test), `slf4j-simple` (test). Versões já existem no pai/BOM; se faltar `assertj` ou os módulos de testcontainers no `dependencyManagement`, copiar de `messaging-aws/pom.xml`/`messaging-rabbitmq/pom.xml`.
- [ ] **Step 4: Implementar `LoadProfile`** conforme a assinatura acima; `load` espelha o do outbox (`Properties` + `TreeMap`, valores com `trim`).
- [ ] **Step 5: Rodar** `mvn -pl messaging-loadtest -am test -Dtest=LoadProfileTest` → PASS. Rodar também `mvn -q -DskipTests compile` na raiz para garantir que o módulo novo entra no reactor.

### Task 2: `HandlerModel`

**Files:**
- Create: `.../loadtest/HandlerModel.java`
- Test: `.../loadtest/HandlerModelTest.java`

**Interfaces:**
- Consumes: `LoadProfile` (Task 1).
- Produces: `final class HandlerModel` com `static HandlerModel from(LoadProfile p)`; `void handle(String id) throws Exception`, que dorme a latência e lança `IllegalStateException("poison")`/`IllegalStateException("transient")` conforme a decisão; `boolean isPoison(String id)`; `boolean isTransient(String id)`; `static long parseLatencyNanos(...)` não é público: o formato é interno. Latência aceita `0`, `fixed:20ms`, `uniform:5-50ms`, `lognormal:p50=20ms,p99=200ms`; formato inválido → `IllegalArgumentException` citando `handlerLatency`.

- [ ] **Step 1: Escrever `HandlerModelTest`**: (a) `isPoison` é determinístico: mesmo `seed`+id, mesmo resultado em 1000 chamadas, e com `seed` diferente o conjunto muda; (b) com `poisonRate=0.1` sobre 100000 ids, a fração fica em 0.1 ± 0.01; (c) `transientFailureRate=1`, `transientFailAttempts=2`: as duas primeiras chamadas de `handle("a")` lançam, a terceira passa, e a contagem de tentativas é por id; (d) um id nunca é poison e transient ao mesmo tempo (poison tem precedência); (e) `fixed:20ms` faz `handle` demorar ≥ 20 ms; `lognormal` com p50=10ms/p99=100ms gera amostras com mediana entre 7 e 14 ms (usar o método de amostragem interno exposto como `long sampleNanos(ThreadLocalRandom r)` package-private); (f) latência inválida lança `IllegalArgumentException`; (g) padrão (`0`, taxas 0) nunca dorme nem lança.
- [ ] **Step 2: Rodar e ver falhar**: `mvn -pl messaging-loadtest -am test -Dtest=HandlerModelTest`.
- [ ] **Step 3: Implementar `HandlerModel`**. Hash: mistura de 64 bits de `seed` com `id.hashCode()` (ex.: `Long.hashCode`/mix tipo splitmix) mapeada para `[0,1)`; transient usa o mesmo hash com sal diferente. Tentativas por id em `ConcurrentHashMap<String,AtomicInteger>`. Latência com `ThreadLocalRandom`; `lognormal` deriva `mu`/`sigma` de p50 e p99 (`sigma = ln(p99/p50)/2.3263`). Dormir com `Thread.sleep(Duration)`/`LockSupport.parkNanos`, sem busy-wait.
- [ ] **Step 4: Rodar** o teste → PASS.

### Task 3: `Tracker` (observer e estatísticas)

**Files:**
- Create: `.../loadtest/Tracker.java`
- Test: `.../loadtest/TrackerTest.java`

**Interfaces:**
- Produces: `final class Tracker implements ConsumerObserver` com `Tracker(int concurrency)`; a produção é medida no runner, não aqui. Métodos: `void handled(String id, String key, long seq, long sentAtMs, boolean success, long handlerNanos)` (chamado pelo handler do runner, não pelo observer, porque o observer não tem a chave e o seq); sobrescreve `delivered(Report)`, `handled(InboxMessage, Duration, Throwable)`, `receiveFailed(RuntimeException)`; `long terminal()` (ACKED + FAILED + DEAD_LETTERED); `Map<String,Object> snapshot(double wallSeconds)` com: `consumed`, `terminalByEffect` (mapa por `Effect`), `duplicates`, `outOfOrder`, `receiveFailed`, `e2eMs`/`handlerMs`/`effectMs` (`p50`,`p95`,`p99`,`max`), `workerUtilization` (= soma do tempo de handler ÷ (`concurrency` × `wallSeconds`/1), limitado a 2 casas), `throughputMsgPerSec`; `static Map<String,Double> percentiles(long[] sortedOrUnsorted, double divisor)`.
- Regras: duplicata = segundo `handled(..., success=true)` do mesmo id; fora de ordem = `seq` menor que o último `seq` de sucesso da mesma `key`; `delivered` conta `Report.effect()` e acumula `Report.duration()` em `effectMs`.

- [ ] **Step 1: Escrever `TrackerTest`**: sucesso duplicado do mesmo id incrementa `duplicates` e não `consumed`; `seq` 5 depois de 7 na mesma chave incrementa `outOfOrder`, chaves distintas não interferem; `delivered` com `Effect.ACKED`/`FAILED`/`DEAD_LETTERED` soma em `terminal()` e `REDELIVERY_DELAYED` não; `percentiles` de `1..100` dá p50=51, p95=96, p99=100, max=100 (mesma fórmula do outbox: índice `min(n-1, (int)(n*q))`), e vetor vazio devolve mapa vazio; `workerUtilization` com `concurrency=2`, 1 s de handler somado e `wallSeconds=1` = 0.5; chamadas concorrentes (8 threads × 10000) não perdem contagem.
- [ ] **Step 2: Rodar e ver falhar**: `mvn -pl messaging-loadtest -am test -Dtest=TrackerTest`.
- [ ] **Step 3: Implementar `Tracker`** com `LongAdder`/`ConcurrentHashMap`/`ConcurrentLinkedQueue<Long>` (como o `ConsumptionTracker` do outbox); a ordem por chave com `ConcurrentHashMap.merge` como no outbox; ids vistos em `ConcurrentHashMap.newKeySet()`.
- [ ] **Step 4: Rodar** o teste → PASS.

### Task 4: Runner, relatório e fixture `inmemory` (primeiro ponta a ponta)

**Files:**
- Create: `.../loadtest/BrokerFixture.java`, `.../loadtest/LoadRunner.java`, `.../loadtest/Report.java`, `.../loadtest/MessagingLoadIT.java`
- Create: `messaging-loadtest/src/test/resources/profiles/inmemory-perfeito.properties`, `inmemory-real.properties`
- Test: `.../loadtest/ReportTest.java`

**Interfaces:**
- Consumes: `LoadProfile`, `HandlerModel`, `Tracker`.
- Produces:
  - `interface BrokerFixture extends AutoCloseable` com `record Endpoints(MessageSender main, Supplier<MessageReceiver> receivers, MessageSender failed, MessageSender deadLetter, List<RetryQueue> retryQueues, Supplier<MessageReceiver> deadLetterReceivers, Duration lease)`; `Endpoints endpoints(String name) throws Exception` cria as filas `<name>-main|failed|dlq|retry` (nome já normalizado) e devolve; `static BrokerFixture create(String broker) throws Exception` (switch; broker desconhecido → `IllegalArgumentException` listando os válidos); `void close() throws Exception`.
  - `LoadRunner(): Map<String,Object> run(LoadProfile p, BrokerFixture fixture) throws Exception`: valida o profile, cria endpoints com o nome do profile, produz, roda e devolve o resultado (`drained`, `total`, `produceSeconds`, `producedPerSec`, `wallSeconds`, tudo de `Tracker.snapshot`, `backlog` amostrado a cada 500 ms com `tMs`, `consumed`, `terminal`).
  - `Report.write(Path dir, List<Map<String,Object>> rows)`: grava `report.json` e `summary.csv` (colunas: `profile,broker,messages,producers,orderingKeys,concurrency,maxMessagesPerPoll,keyLanes,handlerLatency,transientFailureRate,poisonRate,producedPerSec,throughputMsgPerSec,e2eP50,e2eP95,e2eP99,handlerP50,handlerP99,workerUtilization,duplicates,outOfOrder,receiveFailed,maxBacklog,drained`); `static String json(Object v)` escreve Map/List/String/Number/Boolean/null (JSON válido, strings escapadas).
  - `MessagingLoadIT`: carrega profiles de `loadtest.profilesDir` filtrando por `loadtest.profiles` (como o `OutboxLoadIT`), agrupa por broker reaproveitando uma fixture por tipo, grava o relatório em `<reportDir>/<timestamp>/` e afirma `drained=true` em todos.

- [ ] **Step 1: Escrever `ReportTest`**: `json` escapa aspas, barra invertida e quebra de linha, serializa `Map` aninhado e `List`, e `null`; `summary.csv` tem o cabeçalho acima e uma linha por profile, com campo ausente (ex.: sem `e2eMs` quando `consume=false`) como vazio.
- [ ] **Step 2: Rodar e ver falhar**: `mvn -pl messaging-loadtest -am test -Dtest=ReportTest`.
- [ ] **Step 3: Implementar `Report`** → o teste passa.
- [ ] **Step 4: Implementar `BrokerFixture` + `InMemory`** sobre `InMemoryMessaging` (testkit): fila principal com `QueueOptions.defaults().withLease(...)` e `withDeadLetterQueue`; `failed`/`dlq`/`retry` como filas simples; `receivers` devolve `inMemory.receiver("<name>-main")`.
- [ ] **Step 5: Implementar `LoadRunner.run`** na ordem: `p.validate()`; `fixture.endpoints(...)`; `HandlerModel.from(p)`; `Tracker`; montar `ConsumerSettings` a partir de `consumer.*` (padrões: concurrency 1, maxMessagesPerPoll 1, keyLanes 1, pollWait 1000 ms, `maxProcessing` = 15 min, `shutdownTimeout` 30 s, `Ordering.RELAXED`, `DelayedStrategy.AUTO`, `includeExceptionMessage=false`, passes de DLQ com intervalo longo); `RetryPolicy` com `retry.delayMs` curto (padrão 100) e poucas tentativas, para que `poisonRate` termine em `failed`/DLQ dentro do `timeoutSeconds` (copiar a construção de `InboxContract.policy(...)`, linhas ~490-500); `Pipeline<String>` com `EnvelopeReader(EnvelopeMode.AUTO)`, deserializador `(m,t)->m.payloadAsString()` e o `StopSignal` como sleeper; handler do pipeline: parseia `id;key;seq;sentAtMs`, mede, chama `model.handle(id)`, registra no `Tracker` e devolve `HandlerResult.ack()` (falha: relança). `InboxConsumer(settings, pipeline, destinations, stop, Clock.systemUTC(), () -> 0.0, tracker)`.
  Produção: `producers` threads dividem `orderingKeys` chaves; cada chave envia `seq` crescente por uma só thread (ordem por chave); `OutgoingMessage` com `orderingKey=key` e `deduplicationId=id` (necessário em FIFO/SQS), `payloadBytes` ajusta o padding; `ratePerSec>0` limita por `LockSupport.parkNanos` (aberta, sem esperar o consumo); `preload=true` produz tudo antes de `consumer.start()`, senão `start()` antes. Laço de espera até `tracker.terminal() >= messages` ou `timeoutSeconds`, amostrando `backlog` a cada 500 ms; `consume=false` pula consumer e espera. `finally`: `consumer.stop()` e fechar senders.
- [ ] **Step 6: Implementar `MessagingLoadIT`** e criar os dois profiles inmemory: `inmemory-perfeito` (`broker=inmemory`, `messages=20000`, `orderingKeys=100`, `consumer.concurrency=4`, `consumer.maxMessagesPerPoll=10`, `timeoutSeconds=60`) e `inmemory-real` (mesmos, `handlerLatency=lognormal:p50=5ms,p99=50ms`, `transientFailureRate=0.05`, `transientFailAttempts=1`, `poisonRate=0.01`, `seed=42`).
- [ ] **Step 7: Rodar sem Docker**: `mvn -pl messaging-loadtest -am verify -Ploadtest -Dloadtest.profiles=inmemory-perfeito,inmemory-real` → BUILD SUCCESS, `target/loadtest/<ts>/report.json` e `summary.csv` existem, os dois profiles com `drained=true`; no `inmemory-real`, `terminalByEffect` contém `FAILED` ou `DEAD_LETTERED` > 0 e `duplicates=0`.
- [ ] **Step 8: Rodar `mvn -pl messaging-loadtest -am verify`** (sem `-Ploadtest`) → o IT não executa e os testes unitários passam.

### Task 5: Fixture `rabbit`

**Files:**
- Modify: `.../loadtest/BrokerFixture.java` (nova classe aninhada `Rabbit`, registrada em `create` como `"rabbit"`)
- Create: `profiles/rabbit-perfeito.properties`, `profiles/rabbit-real.properties`

**Interfaces:** Consumes `BrokerFixture.Endpoints` (Task 4).

- [ ] **Step 1: Implementar `Rabbit`** modelado em `RabbitMqInboxContractTest` e no `Rabbit` do outbox-loadtest: `RabbitMQContainer` (imagem `rabbitmq:3.13-management-alpine`, a do outbox), uma `Connection`, filas quorum `<name>-main|failed|dlq|retry`, `RabbitMessageSender.forQueue(connection, queue, DEFAULT_MAX_MESSAGE_BYTES)`, receivers `new RabbitMessageReceiver(connection, queue, lease, sender-da-dlq)` com lease de 5 s; `RetryQueue(lease, sender-da-retry)`. `close` fecha a conexão e para o container.
- [ ] **Step 2: Criar os profiles** `rabbit-perfeito` e `rabbit-real` (mesmos valores dos inmemory, `broker=rabbit`, `messages=10000`, `timeoutSeconds=180`).
- [ ] **Step 3: Rodar** `mvn -pl messaging-loadtest -am verify -Ploadtest -Dloadtest.profiles=rabbit-perfeito,rabbit-real` (com Docker) → ambos `drained=true`; `rabbit-real` com `terminalByEffect` mostrando falhas terminais.

### Task 6: Fixture `sqs` (LocalStack)

**Files:**
- Modify: `.../loadtest/BrokerFixture.java` (classe `Sqs`, `"sqs"` em `create`)
- Create: `profiles/sqs-perfeito.properties`, `profiles/sqs-real.properties`

- [ ] **Step 1: Implementar `Sqs`** modelado em `SqsInboxContractTest` e no `Aws` do outbox-loadtest: `LocalStackContainer` (`localstack/localstack:3.8`, serviço SQS), `SqsClient` com `endpointOverride`; filas standard `<name>-main|failed|dlq|retry`; `SqsMessageSender(sqs, url)` e `SqsMessageReceiver(sqs, url)`. `deduplicationId` é ignorado em fila standard (aceitável); não usar FIFO nesta entrega.
- [ ] **Step 2: Criar os profiles** `sqs-perfeito`/`sqs-real` (`broker=sqs`, `messages=5000`, `consumer.maxMessagesPerPoll=10`).
- [ ] **Step 3: Rodar** com `-Dloadtest.profiles=sqs-perfeito,sqs-real` → ambos `drained=true`.

### Task 7: Fixture `jms-classic`

**Files:**
- Modify: `.../loadtest/BrokerFixture.java` (classe `JmsClassic`, chave `"jms-classic"`)
- Create: `profiles/jms-classic-perfeito.properties`, `jms-classic-real.properties`
- Modify: `messaging-loadtest/pom.xml` (se faltar: declarar `activemq-client` no escopo test; pode ser `optional` em `messaging-jms`)

- [ ] **Step 1: Implementar `JmsClassic`** copiando a subida do container de `messaging-jms/src/test/.../ActiveMqClassic.java` (imagem `apache/activemq-classic:6.2.0`, `schedulerSupport=true`, `admin/admin`, auto-create de filas abrindo e fechando um consumer) e a montagem de `JmsInboxContract`: `JmsMessageSender.forQueue(connection, JmsDialect.ACTIVEMQ_CLASSIC, queue, DEFAULT_MAX_MESSAGE_BYTES)`, receiver principal `new JmsMessageReceiver(connection, dialect, "<name>-main", lease, 10, JmsMessageReceiver.Redelivery.SCHEDULED, null)` (lease 5 s), demais `new JmsMessageReceiver(connection, dialect, queue)`.
- [ ] **Step 2: Criar os dois profiles** (`messages=5000`, `consumer.concurrency=4`; o `-real` com as mesmas falhas dos outros).
- [ ] **Step 3: Rodar** `-Dloadtest.profiles=jms-classic-perfeito,jms-classic-real` → todos `drained=true`.

### Task 8: README e verificação final

**Files:**
- Create: `messaging-loadtest/README.md`

- [ ] **Step 1: Escrever o README** (português, curto): para que serve, como rodar (os dois comandos do design), tabela das chaves do profile com padrões, como ler o relatório (`workerUtilization` baixo + throughput baixo ⇒ receive/ack/broker; alto ⇒ handler ou `concurrency` curta; `maxBacklog` crescendo ⇒ consumo mais lento que produção; `outOfOrder`/`duplicates`), e como somar um broker (implementar `BrokerFixture`, registrar em `create`).
- [ ] **Step 2: Rodar a suíte completa** `mvn -pl messaging-loadtest -am verify -Ploadtest` (com Docker) e conferir `summary.csv` com uma linha por profile; rodar `mvn verify` na raiz sem `-Ploadtest` e confirmar que nada dos outros módulos mudou (BUILD SUCCESS).
