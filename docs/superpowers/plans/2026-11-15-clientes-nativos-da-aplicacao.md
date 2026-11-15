# Clientes nativos da aplicação: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** O starter para de criar o cliente nativo de cada provider: a aplicação declara o objeto raiz (bean por tipo), o YAML do provider fica só com `type` (e `project` no GCP) e só existe um provider por tipo de cliente.

**Architecture:** `ClientBeans` passa a ter só `require`/`optional` por tipo. Cada `*ProviderConnection` recebe a raiz do bean e continua criando os objetos por destino. `MessagingDestinations.validateProviders` ganha a regra de um provider por grupo, e `MessagingProperties` encolhe os records, com uma mensagem de migração para as chaves removidas. Os adapters e a inbox não mudam.

**Tech Stack:** Java 21, Spring Boot 4.1.x (`Binder`, `ApplicationContextRunner`, `ObjectProvider`), JUnit 5, AssertJ, Testcontainers. Maven multi-módulo.

**Spec:** [`docs/adr/0011-clientes-nativos-da-aplicacao.md`](../../adr/0011-clientes-nativos-da-aplicacao.md)

## Global Constraints

- Java 21; Spring Boot 4.1.x; sem Spring Cloud (ADR-0001).
- Os adapters (`messaging-aws`, `-azure`, `-gcp`, `-jms`, `-rabbitmq`) e a inbox não mudam; só o starter e a documentação.
- As classes de um provider só são carregadas depois de `validateProviders` checar o adapter (`MessagingDestinations.connect`).
- Mensagens de erro em português, com a chave completa (`messaging.providers.<nome>`), o nome qualificado do bean a declarar e `(ADR-0011)`. Problemas de forma são listados todos de uma vez.
- O starter nunca chama `close()` num bean da aplicação. Ele fecha só o que criou: a `Connection` de JMS e RabbitMQ, o `TopicAdminClient` do GCP e os senders e receivers.
- Tipos de bean exatos: `software.amazon.awssdk.services.sqs.SqsClient`, `software.amazon.awssdk.services.sns.SnsClient`, `com.azure.messaging.servicebus.ServiceBusClientBuilder`, `com.google.api.gax.core.CredentialsProvider`, `com.google.api.gax.rpc.TransportChannelProvider`, `jakarta.jms.ConnectionFactory`, `com.rabbitmq.client.ConnectionFactory`.
- Chaves removidas: `region`, `endpoint`, `access-key`, `secret-key`, `connection-string`, `emulator-host`, `broker-url`, `user`, `password`.
- Commits em Conventional Commits (`feat(starter)!: ...` nas tasks que quebram configuração, `docs: ...`). Não incluir `.DS_Store` nem `docs/superpowers/specs/2026-11-13-m16-profundidade-da-fila-design.md`.
- Comando de teste: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='<Classes>' -Dsurefire.failIfNoSpecifiedTests=false`. `SqsLocalStackTest`, `ArtemisAutoConfigurationTest` e `RabbitAutoConfigurationTest` exigem Docker.

## Review Focus

1. **Aplicação que só publica no SNS, sem bean `SqsClient`:** o startup passa (Task 3, `snsOnlyApplicationNeedsNoSqsClient`).
2. **Dois beans `SqsClient`** (um da aplicação para outro uso e um para a lib): sem `@Primary` o startup falha com a chave do provider; com `@Primary` usa o primário (Task 2, `twoBeansWithoutPrimaryFail`, `primaryBeanWins`).
3. **`CachingConnectionFactory` do Boot como único `ConnectionFactory` JMS:** o envio e o consumo funcionam, mas o health check falha. O README avisa, e um teste fixa a mensagem do health (Task 5, `cachingConnectionFactoryFailsOnlyTheHealthCheck`).
4. **YAML antigo com `region` e `broker-url`:** uma falha só, com as duas chaves e o bean a declarar, não o "chave desconhecida" do binding estrito (Task 6, `removedKeysAreReportedTogether`).
5. **`activemq-classic` e `artemis` juntos:** são o mesmo grupo e falham pela regra de um provider por tipo (Task 2, `jmsTypesShareOneGroup`).

---

### Task 1: Aceitar o ADR-0011

**Files:**
- Modify: `docs/adr/0011-clientes-nativos-da-aplicacao.md` (Status: `aceito`)
- Modify: `docs/adr/0010-configuracao-tipada-dos-providers.md:3` (Status: `aceito; a seção 1 foi substituída pelo ADR-0011`)
- Modify: `docs/plan.md:26`: na linha "Spring e observabilidade", trocar "Starter monta clientes e destinos por nome" por "Starter monta destinos por nome sobre os clientes nativos da aplicação" e apontar também para [0011](adr/0011-clientes-nativos-da-aplicacao.md)

- [ ] **Step 1: Editar os três arquivos**
- [ ] **Step 2: Commit**

```bash
git add docs/adr/0011-clientes-nativos-da-aplicacao.md docs/adr/0010-configuracao-tipada-dos-providers.md docs/plan.md docs/superpowers/plans/2026-11-15-clientes-nativos-da-aplicacao.md
git commit -m "docs: ADR-0011 clientes nativos fornecidos pela aplicação e plano"
```

---

### Task 2: `ClientBeans` por tipo e um provider por grupo

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/ClientBeans.java` (reescrita)
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingDestinations.java:335-362` (`validateProviders`, remover `onlyOfItsType`)
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/ClientBeansTest.java` (reescrita)

**Interfaces:**
- Produces:
  - `static <T> T ClientBeans.require(ListableBeanFactory beans, Class<T> type, String providerName)`: devolve o bean único ou o `@Primary`. Sem bean, lança `IllegalStateException("messaging.providers." + providerName + ": declare um bean " + type.getName() + " (ADR-0011)")`. Com vários e nenhum primário, lança `IllegalStateException` com a mesma chave e o texto `"há mais de um bean " + type.getName() + "; marque um com @Primary"`.
  - `static <T> T ClientBeans.optional(ListableBeanFactory beans, Class<T> type, String providerName)`: igual, mas devolve `null` sem bean.
  - Os construtores de `*ProviderConnection` perdem o parâmetro `boolean onlyOfItsType`: `(String providerName, XxxProvider provider, ListableBeanFactory beans)`.
  - `namingHint` e `find` saem.

- [ ] **Step 1: Escrever os testes que falham** em `ClientBeansTest`, com `ApplicationContextRunner` e `DefaultListableBeanFactory`:
  - `missingBeanFailsWithTheProviderKey`: `require` sem bean → mensagem contém `messaging.providers.aws: declare um bean software.amazon.awssdk.services.sqs.SqsClient (ADR-0011)`.
  - `twoBeansWithoutPrimaryFail`: dois `SqsClient` → mensagem contém `marque um com @Primary`.
  - `primaryBeanWins`: dois `SqsClient`, um `@Primary` → `require` devolve o primário.
  - `optionalReturnsNullWithoutBean`.
  - `secondProviderOfTheSameGroupFails` (contexto completo): `messaging.providers.a.type=aws` e `messaging.providers.b.type=aws` → o startup falha com `messaging.providers: a e b usam o mesmo cliente nativo; um provider por tipo (ADR-0011)`.
  - `jmsTypesShareOneGroup`: `activemq-classic` e `artemis` → a mesma falha.
  - `providersOfDifferentGroupsCoexist`: `aws` e `azure`, com os dois beans → o startup passa.
- [ ] **Step 2: Rodar e ver falhar**: `-Dtest='ClientBeansTest'`, FAIL de compilação (`require` não existe).
- [ ] **Step 3: Implementar.** `require`/`optional` usam `beans.getBeanProvider(type)` e `getIfAvailable()`, traduzindo `NoUniqueBeanDefinitionException` para a mensagem do `@Primary`. Em `validateProviders`, agrupar os providers por `MODULES.get(type)` e adicionar um problema por grupo com mais de um nome (nomes em ordem de declaração, juntados por `" e "`). Remover `onlyOfItsType` e o argumento nas chamadas de `connect`. Para compilar sem mudar o comportamento ainda, as conexões trocam `find(...)` por `optional(...)`, que também devolve `null` sem bean. As Tasks 3 a 5 trocam pelo `require` e removem o caminho por YAML. Remover de `ClientBeansTest` os testes de convenção de nomes e de "properties vencem o bean".
- [ ] **Step 4: Rodar e ver passar**: `-Dtest='ClientBeansTest'`, PASS.
- [ ] **Step 5: Commit**: `feat(starter)!: bean de cliente por tipo e um provider por tipo de cliente`

---

### Task 3: AWS só com beans

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/aws/AwsProviderConnection.java`
- Modify: `messaging-spring-boot-starter/src/test/java/com/example/messaging/inbox/spring/SqsLocalStackTest.java` (declarar `SqsClient`/`SnsClient` com o endpoint do LocalStack em vez de `endpoint`/`region`/chaves no YAML)
- Modify: os testes de `MessagingAutoConfigurationTest`, `InboxAutoConfigurationTest`, `InboxConfigTest`, `InboxProviderOptionsTest` e `InboxRedriveEndpointTest` que definem `region`/`endpoint` (remover a chave; declarar o bean quando o contexto cria sender ou receiver AWS)
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/aws/AwsProviderConnectionTest.java` (novo)

**Interfaces:**
- Consumes: `ClientBeans.require` (Task 2).
- Produces: `AwsProviderConnection(String providerName, AwsProvider provider, ListableBeanFactory beans)`.

- [ ] **Step 1: Escrever os testes que falham**:
  - `snsOnlyApplicationNeedsNoSqsClient`: só o bean `SnsClient` (mock) e um sender com `destination=arn:aws:sns:sa-east-1:123:eventos` → o contexto sobe.
  - `sqsReceiverWithoutSqsClientFails`: só o `SnsClient` e um receiver SQS → a falha contém `declare um bean software.amazon.awssdk.services.sqs.SqsClient`.
  - `starterNeverClosesTheApplicationsClient`: mock de `SqsClient`; fechar o contexto → `verify(sqs, never()).close()`.
- [ ] **Step 2: Rodar e ver falhar**: `-Dtest='AwsProviderConnectionTest'`.
- [ ] **Step 3: Implementar.** Guardar `beans` e `providerName`. `sqs()` e `sns()` resolvem com `ClientBeans.require` no primeiro uso (campo `volatile` ou um `Supplier` memoizado). Remover `configure`, `credentials`, `warnIgnoredBean`, `rejectConnectionFields`, `hasConnectionFields`, os flags `owns*` e os imports do Apache client. `close()` não faz nada; o cache de URL fica.
- [ ] **Step 4: Rodar e ver passar**: os testes da Task e os modificados, mais `SqsLocalStackTest` com Docker.
- [ ] **Step 5: Commit**: `feat(starter)!: AWS usa os beans SqsClient e SnsClient da aplicação`

---

### Task 4: Azure e GCP só com beans

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/azure/AzureProviderConnection.java`
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/gcp/GcpProviderConnection.java`
- Modify: `MessagingDestinations.connect`: `new GcpProviderConnection(name, p, beans)`
- Modify: `MessagingAutoConfigurationTest.java:185-250` (trocar `emulator-host` por beans `TransportChannelProvider`, um `FixedTransportChannelProvider` sobre um canal sem TLS, e `NoCredentialsProvider`)
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/gcp/GcpProviderConnectionTest.java` (novo); casos de Azure em `ClientBeansTest`

**Interfaces:**
- Produces: `AzureProviderConnection(String, AzureProvider, ListableBeanFactory)`, `GcpProviderConnection(String, GcpProvider, ListableBeanFactory)`.

- [ ] **Step 1: Escrever os testes que falham**:
  - `azureWithoutBuilderFails`: a falha contém `declare um bean com.azure.messaging.servicebus.ServiceBusClientBuilder`.
  - `gcpWithoutBeansUsesSdkDefaults`: sem beans, a conexão é criada sem lançar (`TopicAdminClient` com as settings padrão).
  - `gcpAppliesChannelAndCredentialsBeans`: com os dois beans, `sender(...)` cria o `Publisher` sobre eles (conferir por um `TransportChannelProvider` espião que registra `getTransportChannel()`).
  - `gcpNeverClosesTheApplicationsChannel`: fechar a conexão não chama `shutdown()` no `ManagedChannel` do bean.
- [ ] **Step 2: Rodar e ver falhar.**
- [ ] **Step 3: Implementar.** Azure: `builder = ClientBeans.require(beans, ServiceBusClientBuilder.class, providerName)`; remover o ramo de `connection-string`. GCP: `channel = ClientBeans.optional(..., TransportChannelProvider.class, ...)` e `credentials = ClientBeans.optional(..., CredentialsProvider.class, ...)`; aplicar cada um que não for `null` em `TopicAdminSettings`, `Publisher.Builder` e `SubscriberStubSettings`, nos mesmos pontos de hoje. Remover `emulatorChannel`, e `close()` fecha só o `admin`.
- [ ] **Step 4: Rodar e ver passar.**
- [ ] **Step 5: Commit**: `feat(starter)!: Azure e GCP usam os beans da aplicação`

---

### Task 5: JMS e RabbitMQ só com beans

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/jms/JmsProviderConnection.java`
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/rabbitmq/RabbitProviderConnection.java`
- Modify: `ActiveMqAutoConfigurationTest.java`, `ArtemisAutoConfigurationTest.java`, `RabbitAutoConfigurationTest.java` (trocar `broker-url`/`user`/`password` por um bean: `JmsDialect.ACTIVEMQ_CLASSIC.connectionFactory("vm://...")`, `JmsDialect.ARTEMIS.connectionFactory(url)` e um `com.rabbitmq.client.ConnectionFactory` com `setUri`)
- Test: casos novos em `ActiveMqAutoConfigurationTest` e `ClientBeansTest`

**Interfaces:**
- Produces: `JmsProviderConnection(String, JmsProvider, ListableBeanFactory)`, `RabbitProviderConnection(String, RabbitProvider, ListableBeanFactory)`.

- [ ] **Step 1: Escrever os testes que falham**:
  - `jmsWithoutConnectionFactoryFails` e `rabbitWithoutConnectionFactoryFails`: a falha contém o nome qualificado do tipo.
  - `cachingConnectionFactoryFailsOnlyTheHealthCheck` (ActiveMQ Classic `vm://`): bean `CachingConnectionFactory` sobre a factory do dialeto; envio e recebimento funcionam, e o health `messaging` fica `DOWN` com o detalhe que o `checkAccess` produz hoje. Conferir a mensagem real antes de fixar a asserção.
  - `connectionCreatedByTheStarterIsClosed`: fechar o contexto fecha a `Connection` (a factory espiã registra o `close` da conexão que criou).
- [ ] **Step 2: Rodar e ver falhar.**
- [ ] **Step 3: Implementar.** JMS: `factory = ClientBeans.require(beans, ConnectionFactory.class, providerName)`; `factory.createConnection()` sem usuário (as credenciais são da factory); manter o `connection.start()` e a mensagem "não conectou ao broker". Rabbit: `factory = ClientBeans.require(...)`; remover `setUri`/`setUsername`/`setPassword` e o `catch` de `URISyntaxException | GeneralSecurityException`; manter `newConnection("messaging-" + providerName)`.
- [ ] **Step 4: Rodar e ver passar**, inclusive os testes com Docker.
- [ ] **Step 5: Commit**: `feat(starter)!: JMS e RabbitMQ usam a ConnectionFactory da aplicação`

---

### Task 6: Records sem campos de conexão e mensagem de migração

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingProperties.java:126-166` (records) e `bindProviders`
- Modify: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/TestProviders.java`
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/MessagingAutoConfigurationTest.java`

**Interfaces:**
- Produces: `AwsProvider(Type type)`, `AzureProvider(Type type)`, `GcpProvider(Type type, String project)`, `JmsProvider(Type type)`, `RabbitProvider(Type type)`.

- [ ] **Step 1: Escrever os testes que falham**:
  - `removedKeysAreReportedTogether`: `aws.region=sa-east-1` e `r.broker-url=amqp://x` (com `r.type=rabbitmq`) → uma `IllegalStateException` que contém `messaging.providers.aws.region foi removida (ADR-0011): configure o bean SqsClient na aplicação` e `messaging.providers.r.broker-url foi removida (ADR-0011): configure o bean com.rabbitmq.client.ConnectionFactory na aplicação`.
  - `gcpProjectStillBinds`: `type=gcp`, `project=p` → `GcpProvider(GCP, "p")`.
- [ ] **Step 2: Rodar e ver falhar.**
- [ ] **Step 3: Implementar.** Encolher os records. Em `bindProviders`, antes do bind estrito de cada provider, ler as chaves do nó (o mesmo `mapOf(String.class, Object.class)` já usado para os nomes) e juntar as que estão em `REMOVED = Set.of("region", "endpoint", "accesskey", "secretkey", "connectionstring", "emulatorhost", "brokerurl", "user", "password")`, comparando sem hífen e em minúsculas, como `ROOTS`. Lançar uma exceção só, com todas as chaves. O bean citado vem de `Map<Type, String>`: `SqsClient` (AWS), `ServiceBusClientBuilder` (Azure), `TransportChannelProvider/CredentialsProvider` (GCP), `jakarta.jms.ConnectionFactory` (JMS), `com.rabbitmq.client.ConnectionFactory` (RabbitMQ). Atualizar `TestProviders`.
- [ ] **Step 4: Rodar a suíte inteira do starter** (sem `-Dtest`): PASS.
- [ ] **Step 5: Commit**: `feat(starter)!: providers sem campos de conexão (ADR-0011)`

---

### Task 7: Documentação

**Files:**
- Modify: `README.md:195-315` (seção Spring Boot: YAML sem campos de conexão; tabela "tipo → bean" do ADR-0011; um `@Configuration` de exemplo por provider; os invariantes da seção 4 do ADR; remover as regras de nomes de bean e de "properties e bean juntos")
- Modify: `messaging-inbox/docs/exemplos/application-completo.yaml:11-23` e `messaging-inbox/README.md:~100` (remover `region`, `broker-url`, `user`, `password`, `connection-string`)
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/inbox/spring/DocumentedConfigTest.java` (sem mudança; precisa continuar verde)

- [ ] **Step 1: Escrever o exemplo AWS do README** com `ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30))` e uma frase: o read timeout precisa ficar acima dos 20 s do long poll. Antes de escrever, conferir na documentação do SDK v2 se o padrão (que se acredita ser 30 s) já atende, e dizer isso no README.
- [ ] **Step 2: Exemplos de JMS** com `JmsDialect.<X>.connectionFactory(url)` e o aviso de `CachingConnectionFactory` (`spring.jms.cache.enabled=false` ou um bean próprio). Conferir no Boot 4.1 qual bean `ConnectionFactory` ele expõe com o cache desligado antes de recomendar.
- [ ] **Step 3: Rodar** `-Dtest='DocumentedConfigTest'`: PASS.
- [ ] **Step 4: Commit**: `docs: clientes nativos declarados pela aplicação (ADR-0011)`
