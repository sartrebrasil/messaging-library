# ADR-0006: Coordenadas, licença, baseline e publicação

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-10-07 (`inbox-messaging` substitui `inbox-sqs`)
- Revisão: 2026-10-07 (incorpora a acme-messaging; módulo `inbox-resilience4j`)
- Revisão: 2026-10-07 (`hold` e nível 3 no core; BOMs por módulo)

## Decisão

- Coordenadas: `com.example:inbox-*`, pacote base `com.example.inbox`,
  versão inicial `0.1.0-SNAPSHOT`.
- A acme-messaging (`com.acme.messaging:acme-messaging-*`) é incorporada a
  este projeto: o nome `inbox-library` e as coordenadas acima valem para o
  resultado. O código portado de lá muda de pacote para `com.example.inbox`,
  e os headers `x-acme-*` dão lugar aos atributos `inbox_*` (ADR-0002).
- Baseline: Java 21, Spring Boot 4.x (Spring Framework 7, Jackson 3,
  spring-kafka 4.x) e a messaging-lib (`com.example:messaging-bom`) na mesma
  linha de Boot (4.1.x). Spring Cloud AWS saiu com a revisão do ADR-0001.
- O core não importa o BOM do Spring Boot. `inbox-kafka` e o starter
  importam. `inbox-messaging` depende só de `messaging-core` e não importa o
  BOM do Boot.
- Licença: Apache-2.0.
- CI: GitHub Actions com build, testes unitários e ITs com Testcontainers.
- Idioma: código, javadoc e README em inglês (projeto open source); planos e
  ADRs em português, como nas libs irmãs.

Módulos:

| Módulo | Conteúdo |
|---|---|
| `inbox-bom` | Versões de todos os módulos |
| `inbox-core` | Envelope, pipeline, middlewares, retry (inclusive a espera do `hold` e a passada do nível 3), gancho do circuit breaker (`ConsumptionGate`), stale check (SPI e store em memória), redrive, SPI do adapter. Java puro |
| `inbox-resilience4j` | `ConsumptionGate` sobre o Resilience4j (ADR-0010). Depende só de `resilience4j-circuitbreaker` |
| `inbox-testkit` | `BrokerAdapterContract`, broker em memória |
| `inbox-messaging` | Adapter sobre `messaging-core`: loop de polling, workers, `keepAlive`, efeitos `broker-delay` e `retry-queues` (ADR-0009) |
| `inbox-kafka` | Adapter sobre spring-kafka |
| `inbox-spring-boot-starter` | Autoconfig, properties (`inbox.*`), `@InboxListener`, Jackson 3, Micrometer, idempotência, stores JDBC e Redis do stale check, endpoint de redrive |

## Spike de versões (F0, 2026-10-07)

Resolvidos juntos num projeto de teste, sem erro de convergência
(`maven-enforcer-plugin`, regra `dependencyConvergence`):

| Componente | Versão | Observação |
|---|---|---|
| Spring Boot | 4.1.1 | Mesma linha da messaging-lib (`messaging-spring-boot-starter`) |
| spring-kafka | 4.1.1 | Gerenciado pelo BOM do Boot; `kafka-clients` 4.2.1 |
| Spring Framework | 7.0.x | Pelo BOM do Boot |
| Jackson | 3.1.5 | Pelo BOM do Boot |
| Micrometer | 1.17.1 | Igual ao da messaging-lib |
| messaging-lib | 0.1.0-SNAPSHOT | `messaging-bom` importado depois do BOM do Boot, como pede o ADR-0008 da messaging-lib |
| Resilience4j | 2.4.0 | `resilience4j-spring-boot4` existe e é compilado contra o Boot 4.0.0. O `resilience4j-bom` 2.4.0 não gerencia esse artefato: a versão vai explícita |
| JUnit | 6.0.3 | A do BOM do Boot 4.1.1; o build inteiro usa a mesma |

BOMs por módulo, quando as dependências entrarem:

| Módulo | BOMs, na ordem | Observação |
|---|---|---|
| `inbox-core` | Nenhum | |
| `inbox-testkit`, `inbox-messaging` | `messaging-bom` | Sem o BOM do Boot. O testkit usa o `InMemoryMessaging` do `messaging-testkit` |
| `inbox-kafka` | BOM do Boot | Não usa a messaging-lib |
| `inbox-resilience4j` | `resilience4j-bom` | Só `resilience4j-circuitbreaker` |
| `inbox-spring-boot-starter` | BOM do Boot, depois `messaging-bom`, depois `resilience4j-bom` | `resilience4j-spring-boot4` com versão explícita e `optional` |

O build usa as versões de plugin do Boot 4.1.1 (compiler 3.15.0, surefire e
failsafe 3.5.6, jar 3.5.1) e o Maven Wrapper com Maven 3.9.9 do Maven Central.

## Riscos registrados

- A messaging-lib não está publicada: só existe no repositório Maven local.
  O CI do GitHub Actions passa enquanto o `inbox-messaging` não depende dela
  (até a F3). Antes da F3, publicar a messaging-lib no GitHub Packages ou
  instalá-la no próprio job.
- `com.example` não pode ser publicado no Maven Central (o namespace precisa
  ser verificável). Até trocar o groupId, a distribuição é `mvn install` local
  ou GitHub Packages.
- As libs irmãs nasceram de demandas Oobj/Avalara. Antes de abrir o código,
  confirmar com a política de open source da Avalara se ele pode ser liberado
  e sob qual titularidade.
