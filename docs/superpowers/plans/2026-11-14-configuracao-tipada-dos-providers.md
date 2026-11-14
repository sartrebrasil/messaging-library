# Configuração tipada dos providers: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tornar a autoconfiguração dos providers do starter previsível: uma regra única para o bean de cliente da aplicação, properties de provider tipadas por provider e opções validadas por uma tabela de campos aceitos.

**Architecture:** Três mudanças no `messaging-spring-boot-starter`, em ordem de risco: (1) um helper `ClientBeans` aplica a mesma regra de bean nos 4 providers que aceitam bean; (2) `MessagingProperties.Provider` vira uma interface selada com um record por provider, ligado em dois passos (`type` primeiro); (3) `Endpoints.validateOptions` troca as checagens `if` por uma tabela `opção → providers aceitos`. O YAML não muda, e a inbox não é tocada.

**Tech Stack:** Java 21, Spring Boot 4.1.x (`Binder`, `ApplicationContextRunner`), JUnit 5, AssertJ. Maven multi-módulo.

**Spec:** [`docs/adr/0010-configuracao-tipada-dos-providers.md`](../../adr/0010-configuracao-tipada-dos-providers.md) (a Task 1 a emenda).

## Desvios da ADR (decididos na revisão do plano)

- **`options` não vira record por tipo.** A inbox liga `messaging.receivers.*.options` em `InboxProperties.ConsumerOptions` antes de o provider ser conhecido (ele pode vir de `receiver-defaults`). `Options` continua um record achatado e a Task 9 valida por tabela. `Destination` continua existindo.
- **Conflito bean × properties só falha no AWS.** No AWS o bean hoje vence e as properties se perdem em silêncio. No Azure, JMS e RabbitMQ as properties já vencem, e o bean ignorado é inofensivo. Falhar ali derrubaria quem tem um `com.rabbitmq.client.ConnectionFactory` ou `jakarta.jms.ConnectionFactory` criado pelo Boot e configura `broker-url` de propósito.

## Global Constraints

- Java 21; Spring Boot 4.1.x; sem dependência de Spring Cloud (ADR-0008).
- Os adapters (`messaging-aws`, `-azure`, `-gcp`, `-jms`, `-rabbitmq`) continuam sem Spring; só o starter muda.
- As dependências dos adapters no starter continuam `optional`; as classes de um provider só são carregadas depois de `validateProviders` checar o adapter (`MessagingDestinations.connect`).
- Mensagens de erro do starter em português, com a chave completa (`messaging.providers.<nome>.<campo>`), listando todos os problemas de uma vez quando a validação é de forma.
- A inbox (`InboxDestinations`, `InboxConfig`, `ConsumerBuilder`, `InboxLifecycle`) não muda: ela consome `MessagingDestinations.validate/sender/receiver`, `Endpoint` e `Options` com as mesmas assinaturas, e `Provider.type()` em `MicrometerSupport`. Toda task que altera validação ou `Provider` roda os testes da inbox.
- O YAML de `messaging.*` não muda de formato. Chave desconhecida continua derrubando o startup.
- Commits em Conventional Commits (`feat(starter): ...`, `docs: ...`). Não incluir `docs/superpowers/specs/2026-11-13-m16-profundidade-da-fila-design.md` nem `.DS_Store` (já estão não rastreados e não são desta mudança).
- Comando de teste do módulo: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='<Classes>' -Dsurefire.failIfNoSpecifiedTests=false`. Testes com Testcontainers (`SqsLocalStackTest`, `ArtemisAutoConfigurationTest`, `RabbitAutoConfigurationTest`) exigem Docker.

## Review Focus

Entradas que a ADR implica mas nenhum teste óbvio cobre; cada uma tem seu teste na task indicada.

1. **Nome de provider com maiúscula ou `_`** (`Aws_Prod`): o binding por nome do provider (Task 8) deve aceitar o que o `mapOf` aceita hoje, não lançar `InvalidConfigurationPropertyNameException`.
2. **Provider vindo de `receiver-defaults`**: uma opção que o provider resolvido não aceita (`sessions` num receiver que herda um provider `aws`) deve falhar com a chave do receiver (Task 9).
3. **`options.sessions=false` num provider que não é Service Bus**: é o valor padrão, então deve passar; só valor diferente do padrão conta como "definido" (Task 9).
4. **Bean criado pelo Boot junto com `broker-url`** (JMS e RabbitMQ): `broker-url` vence sem falhar o startup (Tasks 5 e 6).
5. **Bean da aplicação nunca é fechado pelo starter**: `MessagingDestinations.close()` não chama `close()` num `SqsClient` que veio de bean (Task 2).

---

### Task 1: Emendar a ADR-0010

**Files:**
- Modify: `docs/adr/0010-configuracao-tipada-dos-providers.md`
- Create (já escrito, ainda não rastreado): o próprio arquivo acima

- [ ] **Step 1: Emendar "Decisão" e "Alternativas"**
  - Na seção "2. Um tipo de properties por provider", manter os records de provider e trocar o parágrafo de `options` por: `Options` continua achatado, porque a inbox o liga antes de conhecer o provider; `Endpoints.validateOptions` passa a usar uma tabela `opção → providers aceitos`, o que também rejeita opções hoje ignoradas em silêncio (`ack-deadline`, `ordered`, `exactly-once` fora do GCP; `redelivery` no AWS e no GCP; `max-message-bytes` no GCP). Remover a frase que diz que `Destination` deixa de existir.
  - Na seção 1, trocar "Provider com esses campos **e** bean ... derruba o startup" por: derruba o startup só no AWS; no Azure, JMS e RabbitMQ as properties continuam vencendo o bean, porque o Boot cria `ConnectionFactory`s que não são da lib.
  - Em "Alternativas", acrescentar "Records de options por tipo": descartada porque exige ligar `options` em duas fases e mexer em `Endpoint`, `InboxProperties` e `InboxConfig`.
  - Em "Consequências", acrescentar que configs com opções inaplicáveis (hoje ignoradas) passam a falhar.

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0010-configuracao-tipada-dos-providers.md docs/superpowers/plans/2026-11-14-configuracao-tipada-dos-providers.md
git commit -m "docs: ADR-0010 configuração tipada dos providers e plano"
```

---

### Task 2: `ClientBeans` e a regra do bean no AWS

**Files:**
- Create: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/ClientBeans.java`
- Modify: `.../spring/aws/AwsProviderConnection.java`, `.../spring/MessagingDestinations.java` (`connect`, `:348`)
- Test: `messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/ClientBeansTest.java`

**Interfaces:**
- Produces (usado pelas Tasks 4 a 6):
  - `public final class ClientBeans` com `public static <T> T find(ListableBeanFactory beans, Class<T> type, String providerName, boolean onlyOfItsType)`: devolve o bean chamado `providerName` ou `providerName + type.getSimpleName()` (um provider AWS precisa de `SqsClient` e `SnsClient`, e dois beans não têm o mesmo nome) se for do tipo `type` (`beans.containsBean(name) && beans.isTypeMatch(name, type)`); senão, se `onlyOfItsType`, `beans.getBeanProvider(type).getIfAvailable()`; senão `null`.
  - `MessagingDestinations.connect` calcula `onlyOfItsType` = nenhum outro provider com o mesmo valor em `MODULES` (assim `activemq-classic` e `artemis`, que usam o mesmo `jakarta.jms.ConnectionFactory`, contam como um grupo) e o passa ao construtor de cada conexão.
  - `AwsProviderConnection(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans, boolean onlyOfItsType)`.

- [ ] **Step 1: Escrever os testes** em `ClientBeansTest` (`ApplicationContextRunner` com `MessagingAutoConfiguration`, sem rede). O helper `countingSqs()` cria um `SqsClient` com `Proxy.newProxyInstance`, registra numa lista o nome de cada método chamado (inclusive `close`) e responde `getQueueUrl` com `GetQueueUrlResponse.builder().queueUrl("http://localhost:4566/0/q").build()`.
  - `awsBeanIsUsedWhenTheProviderHasNoConnectionFields`: provider `aws` só com `type`, beans `SqsClient` e `SnsClient` (proxy); `destinations.sender("s", null, new Endpoint("nome-da-fila", null, null, null))` chama `getQueueUrl` no proxy.
  - `awsBeanWithConnectionFieldsFailsTheStartup`: o mesmo com `messaging.providers.aws.region=us-east-1`; o contexto falha e `rootCause()` contém `messaging.providers.aws` e `SqsClient`.
  - `awsUnnamedBeanIsIgnoredWithTwoProviders`: providers `a` e `b` (`type=aws`, `region=us-east-1`, `endpoint=http://localhost:4566`, `access-key=test`, `secret-key=test`) e um `SqsClient` sem nome; criar `sender("s", "a", new Endpoint("http://localhost:4566/0/q", null, null, null))` não chama nada no proxy e não falha por conflito.
  - `awsBeanNamedAfterTheProviderIsUsedWithTwoProviders`: os mesmos providers, sem `region` em `a`, bean `SqsClient` com o nome `a`; `sender("s", "a", Endpoint("nome", ...))` chama `getQueueUrl` no proxy.
  - `starterNeverClosesTheApplicationsClient` (Review Focus 5): depois de usar o bean, `destinations.close()` não registra `close` no proxy.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest=ClientBeansTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL de compilação (`ClientBeans` não existe).

- [ ] **Step 3: Implementar** `ClientBeans` conforme Interfaces. Em `AwsProviderConnection`, obter `SqsClient` e `SnsClient` com `ClientBeans.find`; se um bean será usado e `region`, `endpoint`, `accessKey` ou `secretKey` do provider está definido, lançar `IllegalStateException("messaging.providers.<nome>: defina region, endpoint, access-key e secret-key ou o bean <SqsClient|SnsClient>, não os dois")`. Calcular `onlyOfItsType` em `connect`.

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='ClientBeansTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "feat(starter): bean de cliente AWS segue regra única; conflito com properties falha"
```

---

### Task 3: Um só `configure` no `AwsProviderConnection`

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/aws/AwsProviderConnection.java`

**Interfaces:**
- Consumes: o construtor da Task 2. Nenhuma assinatura pública muda.

Refatoração sem comportamento novo; os testes da Task 2 e o `MessagingAutoConfigurationTest` são a rede.

- [ ] **Step 1: Rodar a base**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='ClientBeansTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 2: Implementar** `private static <B extends AwsClientBuilder<B, C> & AwsSyncClientBuilder<B, C>, C> B configure(B builder, MessagingProperties.Provider provider)` com o corpo comum dos dois `configure` atuais (credenciais, `ApacheHttpClient` com socket timeout de 30 s, `region`, `endpoint`), e apagar os dois métodos duplicados.

- [ ] **Step 3: Rodar e ver passar**

Mesmo comando do Step 1. Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "refactor(starter): um configure só para os clientes SQS e SNS"
```

---

### Task 4: Regra do bean no Azure

**Files:**
- Modify: `.../spring/azure/AzureProviderConnection.java`, `.../spring/MessagingDestinations.java` (`connect`)
- Test: `.../spring/ClientBeansTest.java`

**Interfaces:**
- Consumes: `ClientBeans.find` e `onlyOfItsType` (Task 2).
- Produces: `AzureProviderConnection(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans, boolean onlyOfItsType)`. `connection-string` continua vencendo o bean.

- [ ] **Step 1: Escrever os testes** em `ClientBeansTest`:
  - `azureUnnamedBeanIsIgnoredWithTwoProviders`: providers `a1` e `a2` (`type=azure`, sem `connection-string`) e um `ServiceBusClientBuilder` sem nome: o startup falha com `messaging.providers.a1: defina connection-string ou declare um bean`.
  - `azureNamedBeanIsUsedWithTwoProviders`: bean `ServiceBusClientBuilder` com o nome `a1`, `connection-string` só em `a2`; o contexto sobe.
  - `azureConnectionStringWinsOverTheBean`: um provider com `connection-string` e um bean: o contexto sobe.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest=ClientBeansTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL em `azureUnnamedBeanIsIgnoredWithTwoProviders` (hoje o bean sem nome é aceito).

- [ ] **Step 3: Implementar** trocando `getBeanProvider(ServiceBusClientBuilder.class).getIfAvailable()` por `ClientBeans.find(beans, ServiceBusClientBuilder.class, providerName, onlyOfItsType)`, e passando `onlyOfItsType` em `connect`.

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='ClientBeansTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "feat(starter): regra única de bean no Azure"
```

---

### Task 5: Regra do bean no JMS (ActiveMQ Classic e Artemis)

**Files:**
- Modify: `.../spring/jms/JmsProviderConnection.java`, `.../spring/MessagingDestinations.java` (`connect`)
- Test: `.../spring/ClientBeansTest.java`

**Interfaces:**
- Consumes: `ClientBeans.find` e `onlyOfItsType` (Task 2).
- Produces: `JmsProviderConnection(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans, boolean onlyOfItsType)`. `broker-url` continua vencendo o bean.

- [ ] **Step 1: Escrever os testes** em `ClientBeansTest`, com um `jakarta.jms.ConnectionFactory` por `Proxy` cujo `createConnection` lança `jakarta.jms.JMSException`. A mensagem distingue se o bean foi usado (`não conectou ao broker`) ou não (`defina broker-url ou declare um bean`):
  - `jmsNamedBeanIsUsedWithTwoProviders`: providers `j1` (`type=artemis`) e `j2` (`type=activemq-classic`), bean com o nome `j1`; o startup falha com `messaging.providers.j1: não conectou ao broker`.
  - `jmsUnnamedBeanIsIgnoredWithTwoProviders`: o mesmo bean sem nome; falha com `messaging.providers.j1: defina broker-url ou declare um bean`.
  - `jmsBrokerUrlWinsOverAnUnnamedBean` (Review Focus 4): um provider com `broker-url=tcp://localhost:1` e um bean sem nome; a falha vem da `broker-url`, não de conflito.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest=ClientBeansTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL em `jmsUnnamedBeanIsIgnoredWithTwoProviders`.

- [ ] **Step 3: Implementar** com `ClientBeans.find(beans, ConnectionFactory.class, providerName, onlyOfItsType)` e `onlyOfItsType` vindo de `connect`.

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='ClientBeansTest,ActiveMqAutoConfigurationTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. Com Docker, acrescentar `ArtemisAutoConfigurationTest`.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "feat(starter): regra única de bean no JMS"
```

---

### Task 6: Regra do bean no RabbitMQ

**Files:**
- Modify: `.../spring/rabbitmq/RabbitProviderConnection.java`, `.../spring/MessagingDestinations.java` (`connect`)
- Test: `.../spring/ClientBeansTest.java`
- Modify: `README.md` (item "Clientes:" da seção Spring Boot)

**Interfaces:**
- Consumes: `ClientBeans.find` e `onlyOfItsType` (Task 2).
- Produces: `RabbitProviderConnection(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans, boolean onlyOfItsType)`. `broker-url` continua vencendo o bean.

- [ ] **Step 1: Escrever os testes** em `ClientBeansTest`, com um `com.rabbitmq.client.ConnectionFactory` (`setHost("localhost")`, `setPort(1)`):
  - `rabbitNamedBeanIsUsedWithTwoProviders`: providers `r1` e `r2` (`type=rabbitmq`), bean com o nome `r1`; falha com `messaging.providers.r1: não conectou ao broker`.
  - `rabbitUnnamedBeanIsIgnoredWithTwoProviders`: o mesmo bean sem nome; falha com `messaging.providers.r1: defina broker-url ou declare um bean`.
  - `rabbitBrokerUrlWinsOverAnUnnamedBean` (Review Focus 4): `broker-url=amqp://localhost:1` com um bean sem nome; a falha é `não conectou ao broker` e não de conflito.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest=ClientBeansTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL em `rabbitUnnamedBeanIsIgnoredWithTwoProviders`.

- [ ] **Step 3: Implementar** com `ClientBeans.find(beans, ConnectionFactory.class, providerName, onlyOfItsType)`. README: descrever a regra completa do bean (vale quando o provider não define os campos de conexão; com mais de um provider do mesmo tipo, só o bean com o nome do provider; no AWS, bean junto com `region`/`endpoint`/credenciais derruba o startup; no Azure, JMS e RabbitMQ, `connection-string`/`broker-url` vencem o bean).

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='ClientBeansTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. Com Docker, acrescentar `RabbitAutoConfigurationTest`.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter README.md
git commit -m "feat(starter): regra única de bean no RabbitMQ; README"
```

---

### Task 7: Remover os `return null` inalcançáveis

**Files:**
- Modify: `.../spring/azure/AzureProviderConnection.java`, `.../spring/jms/JmsProviderConnection.java`, `.../spring/rabbitmq/RabbitProviderConnection.java`, `.../spring/gcp/GcpProviderConnection.java`, `.../spring/ProviderConnection.java` (javadoc)

**Interfaces:**
- Produces: `ProviderConnection.sender` e `.receiver` nunca devolvem `null`.

Refatoração sem comportamento novo. `Endpoints.resolve` já registra problema para os casos que levavam a `null` (`dead-letter-queue` no sender Azure, endereço sem queue nem topic), e `resolveOrThrow` os transforma em exceção antes da conexão.

- [ ] **Step 1: Rodar a base**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='MessagingAutoConfigurationTest,ActiveMqAutoConfigurationTest,EndpointsTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 2: Implementar.** Remover os ramos `return null`: Azure `sender` com `deadLetterQueue` e sem topic nem queue; JMS e RabbitMQ `sender` e `receiver` sem queue nem topic; GCP `sender` sem topic e `receiver` sem subscription. Onde o `null` era o único ramo alternativo, deixar o `new` direto. No javadoc de `ProviderConnection`, dizer que o `Destination` chega validado e que os métodos nunca devolvem `null`.

- [ ] **Step 3: Rodar e ver passar**

Mesmo comando do Step 1. Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "refactor(starter): ProviderConnection não devolve mais null"
```

---

### Task 8: `Provider` tipado por provider

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/MessagingProperties.java` (`Provider` `:99`, `bind`)
- Modify: `.../spring/Endpoints.java`, `.../spring/MessagingDestinations.java`, os cinco `*ProviderConnection`
- Modify: `.../inbox/spring/MicrometerSupport.java` (só se o compilador pedir)
- Modify: testes que constroem `Provider`: `EndpointsTest` (helper `provider(Type)`), `inbox/spring/MicrometerSupportTest` (`:25`)
- Test: `.../spring/MessagingAutoConfigurationTest.java`

**Interfaces:**
- Consumes: os construtores das Tasks 2, 4, 5 e 6 (agora recebem o record do tipo em vez de `Provider`).
- Produces, em `MessagingProperties`:
  - `public sealed interface Provider permits AwsProvider, AzureProvider, GcpProvider, JmsProvider, RabbitProvider { Type type(); }`
  - `record AwsProvider(Type type, String region, URI endpoint, String accessKey, String secretKey)`
  - `record AzureProvider(Type type, String connectionString)`
  - `record GcpProvider(Type type, String project, String emulatorHost)`
  - `record JmsProvider(Type type, String brokerUrl, String user, String password)` (`type` é `ARTEMIS` ou `ACTIVEMQ_CLASSIC`)
  - `record RabbitProvider(Type type, String brokerUrl, String user, String password)`
  - `type` é o primeiro componente de cada record, porque o binding estrito exige onde a chave `type` do YAML caia. `Type`, `Endpoint`, `Sender`, `Options` e `Destination` ficam como estão.

Esta task não se divide: trocar o record por interface selada quebra a compilação de todos os consumidores de uma vez.

- [ ] **Step 1: Escrever os testes** em `MessagingAutoConfigurationTest`:
  - `providerFieldOfAnotherTypeFails`: `messaging.providers.r.type=rabbitmq` com `messaging.providers.r.region=us-east-1`; `rootCause()` contém `messaging.providers.r.region`.
  - `providerWithoutTypeFails`: `messaging.providers.x.region=us-east-1` sem `type`; `rootCause()` contém `messaging.providers.x.type é obrigatório (aws, azure, gcp, activemq-classic, artemis ou rabbitmq)`.
  - `providerNamesWithUppercaseAndUnderscoreBind` (Review Focus 1): `messaging.providers.Aws_Prod.type=aws` e `.region=us-east-1`; o contexto sobe e `providers()` tem a chave `Aws_Prod`.
  - `typedProviderBindsFromEnvironmentVariables`: um `SystemEnvironmentPropertySource` com `MESSAGING_PROVIDERS_AWS_TYPE=aws` e `MESSAGING_PROVIDERS_AWS_REGION=us-east-1`; `providers().get("aws")` é um `AwsProvider` com `region` `us-east-1`.
  - Os testes existentes continuam sem mudar de assert; só os helpers com `new Provider(...)` mudam.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest=MessagingAutoConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL (`region` em `rabbitmq` é aceito hoje).

- [ ] **Step 3: Implementar**
  - `MessagingProperties.bind`: ligar `messaging.providers` como `Map<String, Object>` só para listar os nomes; para cada nome, ler `messaging.providers.<nome>.type` como `Type`; sem `type`, lançar `IllegalStateException` com a mensagem do teste; ligar o nó ao record do tipo com o handler estrito já usado (`NoUnboundElementsBindHandler`). O nome do nó vem de `ConfigurationPropertyName.of("messaging.providers").append(nome)`; se `append` rejeitar o nome, usar `ConfigurationPropertyName.adapt("messaging.providers." + nome, '.')`.
  - `MessagingDestinations.connect`: trocar o `switch (provider.type())` por `switch (provider)` sobre a interface selada. `validateProviders` deixa de checar `type == null`.
  - `Endpoints`: `project` só no `case GCP`, via `((GcpProvider) provider).project()`.
  - Javadoc de `Provider`: remover "cada tipo usa só os seus campos" e apontar para os records.

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='MessagingAutoConfigurationTest,EndpointsTest,MicrometerSupportTest,DocumentedConfigTest,ClientBeansTest,InboxAutoConfigurationTest,InboxConfigTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter
git commit -m "feat(starter): Provider tipado por provider; campo de outro tipo derruba o startup"
```

---

### Task 9: Opções validadas por tabela de campos aceitos

**Files:**
- Modify: `messaging-spring-boot-starter/src/main/java/com/example/messaging/spring/Endpoints.java` (`validateOptions`, `:123`)
- Modify: `.../spring/EndpointsTest.java`, `.../spring/MessagingAutoConfigurationTest.java` (asserts de texto que mudam)
- Test: `.../inbox/spring/InboxAutoConfigurationTest.java`
- Modify: `README.md` (parágrafo de `options`)

**Interfaces:**
- Consumes: `Endpoints.resolve(String key, String providerName, Provider provider, Endpoint e, boolean receive, List<String> problems)` com `Provider` tipado (Task 8).
- Produces, em `Endpoints`: `static final Map<String, Set<Type>> ACCEPTED` (chave: nome da opção em kebab-case; valor: tipos que a aceitam), pacote-visível:

  | Opção | Aceita por |
  |---|---|
  | `require` | todos |
  | `max-message-bytes` | `AWS`, `AZURE`, `ACTIVEMQ_CLASSIC`, `ARTEMIS`, `RABBITMQ` |
  | `sessions`, `dead-letter-queue` | `AZURE` |
  | `redelivery` | `AZURE`, `ACTIVEMQ_CLASSIC`, `ARTEMIS`, `RABBITMQ` |
  | `ack-deadline`, `ordered`, `exactly-once` | `GCP` |
  | `lease` | `ACTIVEMQ_CLASSIC`, `ARTEMIS`, `RABBITMQ` |
  | `routing-key`, `routing-by-ordering-key`, `confirm-timeout` | `RABBITMQ` |

  Uma opção conta como definida quando o valor não é `null`, não é `Boolean.FALSE` e não é um `Set` vazio. Mensagem: `"<key>.options.<opção> não se aplica ao provedor <tag>; vale para: <tags>"`.

- [ ] **Step 1: Escrever os testes**
  - `EndpointsTest.optionNotAcceptedByTheProviderFails` (parametrizado): `sessions` em `AWS`; `redelivery` em `GCP`; `ack-deadline` em `AZURE`; `max-message-bytes` em `GCP`; `lease` em `AZURE`; `confirm-timeout` em `ARTEMIS`. Cada um deixa um problema com `KEY + ".options.<opção> não se aplica ao provedor"`.
  - `EndpointsTest.optionNotAcceptedByTheResolvedProviderFails` (Review Focus 2): `Endpoints.provider(KEY, null, "b", {a: aws, b: aws}, problems)` resolve `b`; um `Endpoint` com `sessions=true` resolvido contra esse provider gera o problema com `KEY`.
  - `EndpointsTest.falseAndEmptyOptionsAreNotSet` (Review Focus 3): `sessions=false` em `AWS` e `require` vazio em `GCP` não geram problema.
  - `EndpointsTest.everyOptionIsInTheTable`: para cada componente de `Options.class.getRecordComponents()`, o nome em kebab-case é chave de `Endpoints.ACCEPTED`.
  - `InboxAutoConfigurationTest.receiverOptionNotAcceptedByItsProviderFailsTheStartup` (o caminho da inbox: `InboxLifecycle.afterSingletonsInstantiated` → `ConsumerBuilder.validate` → `MessagingDestinations.validate`): provider `aws` (como nos outros testes da classe) e um receiver com `options.sessions=true`; o contexto falha com `Invalid messaging.receivers configuration` e `messaging.receivers.<nome>.options.sessions não se aplica ao provedor aws`. Um segundo receiver inválido na mesma config aparece na mesma mensagem (a inbox lista todos os problemas).
  - `InboxAutoConfigurationTest.deadLetterAndFailedCopiesCarryNoOptions`: receiver válido com `options.max-message-bytes` num provider que a aceita e `dead-letter` e `failed` definidos sobe sem problema (as cópias de `ConsumerBuilder.copy` usam `Options.NONE`, então a tabela não as atinge).
  - Em `MessagingAutoConfigurationTest.invalidConfigurationFailsStartupListingEveryProblem` e em `EndpointsTest.optionsAreValidatedByProviderType`, trocar `"sessions só vale para o Service Bus"` por `"options.sessions não se aplica ao provedor aws"`. `azureDeadLetterQueueIsReceiveOnly` continua com `"dead-letter-queue só vale para receber"`.

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='EndpointsTest,MessagingAutoConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL (a tabela não existe; `ack-deadline` em AZURE passa hoje).

- [ ] **Step 3: Implementar** em `Endpoints.validateOptions`:
  - Antes dos demais checks, percorrer `Options.class.getRecordComponents()`, ler cada valor com `getAccessor()` e, para cada opção definida cujo tipo do provider não está em `ACCEPTED`, registrar o problema.
  - Remover as checagens `só vale para` que dependiam só do tipo (`sessions`, `dead-letter-queue`, `lease`, `confirm-timeout` e a parte de tipo de `routing-key` e `routing-by-ordering-key`). Os checks de combinação continuam: `RESCHEDULE` no Azure e no RabbitMQ, `dead-letter-queue` só para receber, `routing-key` e `routing-by-ordering-key` exigem `topic` e não combinam entre si, `lease` e `confirm-timeout` positivos, `ack-deadline` obrigatório no GCP.
  - README: reescrever o parágrafo de `options` dizendo que opção que o provedor não aceita derruba o startup, com a tabela acima.

- [ ] **Step 4: Rodar e ver passar**

Run: `mvn -q -pl messaging-spring-boot-starter -am test -Dtest='EndpointsTest,MessagingAutoConfigurationTest,ActiveMqAutoConfigurationTest,InboxAutoConfigurationTest,InboxConfigTest,DocumentedConfigTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add messaging-spring-boot-starter README.md
git commit -m "feat(starter): opções validadas por tabela de provedores aceitos"
```

---

### Task 10: Fechamento

**Files:**
- Modify: `docs/adr/0010-configuracao-tipada-dos-providers.md` (Status)

- [ ] **Step 1: Rodar a suíte completa do módulo**

Run: `mvn -q -pl messaging-spring-boot-starter -am test`
Expected: PASS. Sem Docker, anotar quais testes (`SqsLocalStackTest`, Artemis, RabbitMQ) foram pulados, e rodá-los antes do merge.

- [ ] **Step 2: Conferir o README** contra a ADR: regra do bean, `options` e o aviso de que chave inaplicável derruba o startup aparecem uma vez cada, sem texto antigo ("beans ... da aplicação são reaproveitados" e "Chave desconhecida em ..." coerentes).

- [ ] **Step 3: ADR-0010: Status passa a `aceito`; commit**

```bash
git add docs/adr/0010-configuracao-tipada-dos-providers.md README.md
git commit -m "docs: ADR-0010 aceita"
```

---

## Self-review

- **Cobertura da ADR:** regra do bean (Tasks 2, 4, 5, 6), bean nomeado (2, 4, 5, 6), `configure` único (3), `null` inalcançáveis (7), records de provider (8), tabela de options no lugar de records (9, desvio explícito), `DocumentedConfigTest` e README (8, 9, 10).
- **Ordem e dependências:** 2 define `ClientBeans` e `onlyOfItsType`, usados por 4, 5 e 6; 3 e 7 são refatorações sem teste novo e podem ir em qualquer ponto depois de 2; 8 muda os construtores de 2, 4, 5 e 6 e por isso vem depois deles; 9 depende do `Provider` tipado de 8.
- **Cada task fecha com a suíte relevante verde e um commit**, então cada uma pode virar um PR ou ser revertida sozinha. A Task 8 é a única grande, porque trocar o record por interface selada quebra todos os consumidores de uma vez.
