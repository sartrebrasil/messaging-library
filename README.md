# messaging-lib

Abstração de filas e tópicos para Java 21: uma API única sobre os SDKs oficiais da AWS
(SQS, SNS), da Azure (Service Bus) e do Google Cloud (Pub/Sub), e sobre o cliente Jakarta Messaging
do ActiveMQ (Classic e Artemis) e do RabbitMQ. Trocar de provedor muda a
dependência e a configuração, não o código. O que não dá para igualar entre provedores fica
explícito em `Capabilities`.

> Versão 0.x: a API ainda pode mudar entre versões menores.
> Plano e decisões em [`docs/plan.md`](docs/plan.md) e [`docs/adr/`](docs/adr/).

O core não é um framework de consumo: não tem listener, pool de threads, retry por política nem
idempotência. Ele entrega `receive`, `ack`, `nack`, lease e dead letter; o loop de consumo é do módulo
`messaging-inbox` (ou de quem usa o core direto).

## Módulos

| Artefato | Conteúdo |
|---|---|
| `messaging-core` | `MessageSender`, `MessageReceiver`, `OutgoingMessage`, `ReceivedMessage`, `Capabilities`, exceções. Pacote `spi` para quem escreve adapter. Somente JDK. |
| `messaging-testkit` | `InMemoryMessaging` e `MessagingContract` (testes de contrato para implementações). |
| `messaging-aws` | SQS (fila standard e FIFO) e SNS. SDK v2 síncrono com `apache-client`, sem Netty. |
| `messaging-azure` | Service Bus: fila, tópico, subscription, sessions e DLQ nativa. |
| `messaging-gcp` | Pub/Sub: publisher com ordem e pull síncrono. |
| `messaging-jms` | ActiveMQ Classic e Artemis por Jakarta Messaging (`JmsDialect`): fila e tópico, com lease controlado pela lib (ADR-0009). Exige o cliente do broker na aplicação: `org.apache.activemq:activemq-client` ou `artemis-jakarta-client`. |
| `messaging-rabbitmq` | RabbitMQ (AMQP 0-9-1, `amqp-client` sem Netty): fila e exchange, receive por `basicGet`, lease controlado pela lib (ADR-0009). |
| `messaging-spring-boot-starter` | Providers, senders e receivers por properties (`messaging.*`), métricas, `traceparent` e health; os receivers (`messaging.receivers`) sobem quando `messaging-inbox` e Jackson 3 estão na aplicação. Liga cada provedor cujo adapter está no classpath; os adapters não dependem de Spring. Spring Boot 4. |
| `messaging-inbox` | Consumo sobre `MessageReceiver`: loop de polling, envelope (CloudEvents, outbox, raw), retry em três níveis, DLQ e `failed`, idempotência e redrive. Somente JDK. Guia em [`messaging-inbox/README.md`](messaging-inbox/README.md). Em desenvolvimento. |
| `messaging-bom` | Alinha as versões dos módulos acima. |

Cada adapter traz só o SDK do seu provedor.

## Build e instalação local

```bash
mvn install
```

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.example</groupId>
      <artifactId>messaging-bom</artifactId>
      <version>0.1.0-SNAPSHOT</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.example</groupId>
    <artifactId>messaging-aws</artifactId>
  </dependency>
  <dependency>
    <groupId>com.example</groupId>
    <artifactId>messaging-testkit</artifactId>
    <scope>test</scope>
  </dependency>
</dependencies>
```

Os contratos sobem emuladores com Testcontainers (Docker): LocalStack, Service Bus emulator (com
SQL Server) e Pub/Sub emulator. Sem Docker, são pulados.

## Uso

```java
// AWS: fila SQS (o read timeout do cliente HTTP precisa passar dos 20 s do long poll)
MessageSender sender = new SqsMessageSender(sqsClient, queueUrl);
MessageReceiver receiver = new SqsMessageReceiver(sqsClient, queueUrl, dlqSender);
// AWS: tópico SNS; quem recebe é uma fila SQS inscrita com RawMessageDelivery=true
MessageSender topic = new SnsMessageSender(snsClient, topicArn);

// Azure: os métodos for* criam o cliente com as opções que o adapter exige
MessageSender sender = ServiceBusMessageSender.forQueue(builder, "pedidos", false, 256 * 1024);
MessageReceiver receiver = ServiceBusMessageReceiver.forQueue(builder, "pedidos", Redelivery.ABANDON, 256 * 1024);
MessageReceiver ordered = ServiceBusSessionMessageReceiver.forQueue(builder, "pedidos-ordenados", 256 * 1024);

// GCP
MessageSender sender = PubSubMessageSender.create(Publisher.newBuilder(topicName), topicAdminClient);
MessageReceiver receiver = PubSubMessageReceiver.create(subscriberStubSettings, subscriptionName,
        PubSubMessageReceiver.Options.ackDeadline(Duration.ofSeconds(60)).withDeadLetterSender(dlqSender));

// ActiveMQ: a Connection é de quem a criou (do cliente do broker, sem pool, para o checkAccess)
JmsDialect dialect = JmsDialect.ACTIVEMQ_CLASSIC;          // ou JmsDialect.ARTEMIS
Connection connection = dialect.connectionFactory(brokerUrl).createConnection(user, password);
MessageSender sender = JmsMessageSender.forQueue(connection, dialect, "pedidos", JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
MessageSender topic = JmsMessageSender.forTopic(connection, dialect, "VirtualTopic.eventos", 1024 * 1024);
MessageReceiver receiver = new JmsMessageReceiver(connection, dialect,
        dialect.subscriptionQueue("VirtualTopic.eventos", "faturamento"),   // Artemis: "eventos::faturamento"
        Duration.ofSeconds(60), 10, JmsMessageReceiver.Redelivery.IMMEDIATE, dlqSender);

// RabbitMQ: a Connection é de quem a criou; a subscription é a fila ligada ao exchange
MessageSender sender = RabbitMessageSender.forQueue(connection, "pedidos", RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
MessageSender topic = RabbitMessageSender.forExchange(connection, "eventos", "pedido.criado", 1024 * 1024);
MessageReceiver receiver = new RabbitMessageReceiver(connection, "faturamento", Duration.ofSeconds(60), dlqSender);
```

O mesmo código vale para qualquer provedor:

```java
sender.send(OutgoingMessage.ofText(json)
        .withContentType("application/json")
        .withAttribute("tenant", "acme")
        .withOrderingKey(pedidoId));

for (ReceivedMessage message : receiver.receive(10, Duration.ofSeconds(20))) {
    try (KeepAlive lease = receiver.keepAlive(message, Duration.ofMinutes(30))) {
        process(message);
        receiver.ack(message);
    } catch (Exception e) {
        receiver.nack(message, Duration.ofSeconds(30));
    }
}
```

Regras comuns:

- `receive` bloqueia até ter mensagens ou até `maxWait`; lista vazia não é erro.
- O lease é o do destino (visibility timeout, lock duration, ack deadline). `keepAlive` renova
  quando falta 1/3 dele, até `close()`, `ack`/`nack`/`deadLetter` ou `maxTotal`.
- `ack`, `nack` e `deadLetter` ficam no receiver que entregou a mensagem.
- Senders e receivers são thread-safe e não fecham o cliente nativo que receberam.
- **Quem consome precisa ser idempotente**: `ack` no SQS com receipt handle antigo pode responder
  sucesso sem apagar, e no Pub/Sub sem exactly-once o ack tardio passa em silêncio.

## Diferenças por provedor

`sender.capabilities()` e `receiver.capabilities()` devolvem isso em tempo de execução, e o
starter pode exigir no startup (`require`).

| | SQS | SNS | Service Bus | Pub/Sub | ActiveMQ (Classic e Artemis) | RabbitMQ |
|---|---|---|---|---|---|---|
| Tamanho máximo | 1 MiB (corpo + atributos) | `MaximumMessageSize` do tópico (256 KiB padrão, até 1 MiB) | 256 KB Standard; Premium configurável | 10 MB | Configurado no sender (Classic: `maxFrameSize` do broker, 100 MiB padrão) | Configurado no sender (`max_message_size` do broker, 16 MiB padrão no 4.x) |
| `nack` com atraso | Sim, até 12 h desde o primeiro receive | Não se aplica | Só em `RESCHEDULE` (cópia agendada), sem sessions | Sim, até 600 s | Só em `SCHEDULED` (cópia agendada; no Classic, exige `schedulerSupport` no broker), até 12 h | Não: `nack` é sempre imediato |
| Dead letter explícito | Cópia para o sender de DLQ + ack | Não se aplica | Nativo | Cópia para o sender de DLQ + ack | Cópia para o sender de DLQ + ack | Cópia para o sender de DLQ + ack |
| Ordem por `orderingKey` | Só FIFO | Só FIFO | Só com sessions | Sim (subscription com ordering) | `JMSXGroupID`, só em `IMMEDIATE` | Não |
| Deduplicação por `deduplicationId` | Só FIFO (5 min) | Só FIFO | Sim (duplicate detection na entidade) | Não | Só Artemis (`_AMQ_DUPL_ID`) | Não |
| `ack` com lease vencido lança erro | Não | Não se aplica | Sim | Só com exactly-once | Sim (lease local) | Sim (lease local) |
| Lease | Visibility timeout da fila | Não se aplica | Lock duration (máx. 5 min); com sessions, o lock da session | Ack deadline da subscription (10–600 s) | Prazo local do receiver (padrão 60 s): vencido, a session fecha e o broker reentrega | Prazo local do receiver (padrão 60 s): vencido, `basicNack(requeue)` |

O Service Bus com sessions segura uma session por instância de receiver: paralelismo entre
sessions é ter mais instâncias. Como ele não tem atraso no `nack` com ordem, quem consome espera
antes de chamar `nack`, segurando o lock com `keepAlive`.

No ActiveMQ, o broker não tem lease: o receiver conta o prazo e usa uma session por mensagem
pendente (`maxInFlight`, padrão 10). O consumer precisa de prefetch 0. No Classic, o adapter
acrescenta isso ao nome da fila. No Artemis, a connection factory precisa de
`consumerWindowSize=0`, e o receiver recusa uma conexão sem isso.

Os dois brokers criam destinos no primeiro uso, por padrão. Com auto-create ligado, só o
`checkAccess` informa destino inexistente. No Artemis, a fila multicast de uma subscription precisa
existir antes do primeiro envio. No Java 21, o `receive` dos clientes prende a carrier thread de uma
virtual thread durante o `maxWait`; no Java 24+ não prende.

No RabbitMQ, o receiver faz pull com `basicGet`, sem consumer registrado. Com a fila vazia, tenta
de novo a cada 100 ms até `maxWait`. Um receiver parado não segura mensagens. `deliveryCount` só
existe em quorum queue; em classic queue, fica vazio a partir da segunda entrega. O sender de fila
usa `mandatory`, então fila inexistente lança `DestinationNotFoundException`. O sender de exchange
não usa `mandatory`: exchange sem fila ligada descarta, como um tópico sem subscriptions.

## Mensagem

| Regra | Valor |
|---|---|
| Corpo | `byte[]` não vazio; serialização é de quem usa |
| Atributos | Até 16; chave `[a-z_][a-z0-9_]*` (até 64, sem prefixo `goog`); valor ASCII imprimível até 1024 bytes |
| Reservados pela lib | `content_type`, `traceparent`, `dead_letter`, `delivery_count`, `attributes` |
| `orderingKey`, `deduplicationId` | ASCII sem espaço, até 128 |
| `traceparent` | W3C; o starter preenche |

### Formato no fio do SQS e do SNS

Quem lê a mesma fila sem a lib precisa saber de duas regras (ADR-0004):

- **Corpo:** com `content_type` textual (`text/*`, `application/json`, `application/xml`,
  `*+json`, `*+xml`) ou sem `content_type`, o corpo vai como texto. Com `content_type` não
  textual (inclusive `application/octet-stream`, que a lib usa para binário sem tipo), vai em Base64.
- **Atributos:** até 7 atributos do usuário vão como atributos SQS nativos. Acima de 7, todos vão
  juntos no atributo `attributes`, um objeto JSON de strings. Filtros de subscription do SNS não
  enxergam atributos empacotados.

```java
// leitura sem a lib
String type = attrs.get("content_type");
byte[] body = type == null || isTextual(type) ? text.getBytes(UTF_8) : Base64.getDecoder().decode(text);
Map<String, String> user = attrs.containsKey("attributes") ? parseJson(attrs.get("attributes")) : attrs;
```

No Service Bus e no Pub/Sub, corpo e atributos vão nativos, sem transformação.

## Spring Boot

Adicione o starter e os adapters dos provedores usados:

```yaml
messaging:
  providers:                   # só o tipo: o cliente de cada um é um bean da aplicação (veja "Clientes nativos")
    aws:
      type: aws                # aws | azure | gcp | activemq-classic | artemis | rabbitmq
    azure:
      type: azure
    gcp:
      type: gcp
      project: meu-projeto     # completa nomes curtos de tópico e subscription
    amq:
      type: activemq-classic
  senders:                     # provider opcional quando só existe um
    pedidos:
      provider: aws
      destination: https://sqs.sa-east-1.amazonaws.com/123/pedidos.fifo   # ou o nome da fila
      options: { require: [ordered-delivery] }
    eventos:
      provider: azure
      destination: eventos
      kind: topic                # envia ao tópico; sem kind, à fila
      options: { sessions: true }
    notas:
      provider: gcp
      destination: notas         # no GCP, o envio é sempre a um tópico
    notas-amq:
      provider: amq
      destination: VirtualTopic.notas
      kind: topic
```

Senders ficam em `messaging.senders`; receivers, em `messaging.receivers`, consumidos pela
messaging-inbox (veja abaixo). Os dois usam o mesmo endereço: `destination`, mais `subscription` para
receber de um tópico, `kind: topic` para enviar a um, e `options` com as opções do provedor.

| Provedor | Enviar (`destination` + `kind`) | Receber (`destination` [+ `subscription`]) |
|---|---|---|
| `aws` | ARN do SNS, URL da fila ou nome da fila | URL ou nome da fila (o nome é resolvido com `GetQueueUrl`, que exige `sqs:GetQueueUrl`) |
| `azure` | `kind: topic`: tópico; senão fila | com `subscription`: tópico + subscription; senão fila |
| `gcp` | tópico | subscription (exige `options.ack-deadline`) |
| `activemq-classic` | `kind: topic`: Virtual Topic (`VirtualTopic.*`); senão fila | com `subscription`: fila `Consumer.<subscription>.<topic>`; senão fila |
| `artemis` | `kind: topic`: endereço multicast; senão fila | com `subscription`: FQQN `<topic>::<subscription>`; senão fila |
| `rabbitmq` | `kind: topic`: exchange (com `options.routing-key`); senão fila | com `subscription`: a fila ligada ao exchange; senão fila |

`options`: `require`, `max-message-bytes` (SNS, Service Bus, ActiveMQ e RabbitMQ), `sessions` e
`dead-letter-queue` (Service Bus; esta só para receber, da DLQ nativa), `redelivery: abandon |
reschedule` (Service Bus e ActiveMQ), `ack-deadline`, `ordered` e `exactly-once` (Pub/Sub), `lease`
(ActiveMQ e RabbitMQ, padrão 60 s), `routing-key`, `routing-by-ordering-key` e `confirm-timeout`
(RabbitMQ, padrão 30 s). Cada opção vale só para os provedores listados; uma opção definida num
provedor que não a aceita derruba o startup (`messaging.receivers.<n>.options.<opção> não se aplica ao
provedor <tipo>; vale para: ...`). `false`, `abandon` e conjunto vazio contam como não definidos. No RabbitMQ, com
`routing-by-ordering-key: true`, a routing key de cada envio é o `orderingKey` da mensagem, e o
`deduplicationId`, quando existe, vira o `message-id`; `redelivery: reschedule` é recusado no
startup.

```java
@Autowired MessagingDestinations destinations;

destinations.sender("pedidos").send(message);            // mesma instância sempre
```

Receivers (`messaging.receivers`) são consumidos pela messaging-inbox: retry em três níveis, DLQ e `failed`,
idempotência, redrive e métricas. Guia completo, com exemplos de configuração e diagramas, em
[`messaging-inbox/README.md`](messaging-inbox/README.md).

`type` é obrigatório. Chave desconhecida em `providers`, `senders`, `receivers` ou `receiver-defaults`, ou campo de
outro tipo (`project` num provider `rabbitmq`), derruba o startup, assim como as chaves removidas
`messaging.destinations` e `inbox.*` (ADR-0008 da messaging-inbox) e as chaves de conexão do provider (`region`,
`endpoint`, `access-key`, `secret-key`, `connection-string`, `emulator-host`, `broker-url`, `user`, `password`;
ADR-0011), com a mensagem dizendo qual bean declarar no lugar.

### Clientes nativos

O starter não cria cliente de provedor: a aplicação declara um bean por provider, e o starter monta sobre ele os
senders e receivers de cada fila ou tópico (ADR-0011). Região, credenciais, endpoint, timeouts e a URL do broker são
da aplicação, junto do resto da infraestrutura dela.

| `type` | Bean da aplicação | Obrigatório |
|---|---|---|
| `aws` | `software.amazon.awssdk.services.sqs.SqsClient` | Com algum sender ou receiver SQS |
| `aws` | `software.amazon.awssdk.services.sns.SnsClient` | Com algum sender SNS |
| `azure` | `com.azure.messaging.servicebus.ServiceBusClientBuilder` | Sim |
| `gcp` | `com.google.api.gax.core.CredentialsProvider` | Não: sem ele, Application Default Credentials |
| `gcp` | `com.google.api.gax.rpc.TransportChannelProvider` | Não: sem ele, o canal padrão do SDK |
| `activemq-classic`, `artemis` | `jakarta.jms.ConnectionFactory` | Sim |
| `rabbitmq` | `com.rabbitmq.client.ConnectionFactory` | Sim |

No GCP, o starter cria o cliente de administração de tópicos no startup: sem `CredentialsProvider` e sem
Application Default Credentials na máquina, o startup falha mesmo que o provider ainda não tenha destinos.

Sem o bean obrigatório, o startup falha com a chave do provider e o tipo do bean. Com mais de um bean do mesmo tipo,
vale o `@Primary`. Só pode haver um provider por tipo de cliente (`activemq-classic` e `artemis` contam como um); um
provider de outro tipo convive normalmente (AWS e Azure durante uma migração). O starter fecha só o que criou: a
`Connection` de JMS e RabbitMQ, o `TopicAdminClient` do GCP e os senders e receivers. Os beans são da aplicação.

Cada provedor tem uma classe de exemplo, compilada e testada, em
[`messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/),
com a forma de produção e, onde existe, a de emulador. Os testes de auto-configuração as usam, então o que está lá
compila e sobe:

| `type` | Exemplo | Produção | Emulador |
|---|---|---|---|
| `aws` | [`AwsClientsExample`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/AwsClientsExample.java) | Credenciais da cadeia padrão, renovadas em segundo plano, num provider só para `SqsClient` e `SnsClient`; read timeout de 30 s | LocalStack em `localhost:4566` |
| `azure` | [`AzureClientsExample`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/AzureClientsExample.java) | Entra ID (`DefaultAzureCredential`, exige `azure-identity`) ou connection string | Service Bus Emulator |
| `gcp` | [`GcpClientsExample`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/GcpClientsExample.java) | Nenhum bean (Application Default Credentials) | Pub/Sub emulator em `localhost:8085`, canal sem TLS e `NoCredentialsProvider` |
| `activemq-classic`, `artemis` | [`JmsClientsExample`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/JmsClientsExample.java) | `JmsDialect.<X>.connectionFactory(url)` e as credenciais na factory | `vm://` (Classic) e Testcontainers (Artemis) nos testes |
| `rabbitmq` | [`RabbitClientsExample`](messaging-spring-boot-starter/src/test/java/com/example/messaging/spring/exemplos/RabbitClientsExample.java) | `com.rabbitmq.client.ConnectionFactory` com a URI e as credenciais | Testcontainers nos testes |

O essencial do AWS, que é o que mais se erra (o `SnsClient` é igual):

```java
@Bean
SqsClient sqsClient(AwsCredentialsProvider credentials) {
    return SqsClient.builder().credentialsProvider(credentials)
            .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30)))   // acima dos 20 s do long poll
            .build();                                                                               // região e endpoint: AWS_REGION, AWS_ENDPOINT_URL_SQS
}
```

O que passa a ser da aplicação, e a lib não consegue validar:

- **AWS, read timeout acima do long poll.** O receiver SQS espera até 20 s por mensagem. O padrão do SDK (30 s,
  conferido no AWS SDK 2.55.6) serve; um `SqsClient` configurado com read ou socket timeout abaixo de 20 s corta o
  long poll e o receive falha de forma intermitente. O SDK não expõe o timeout de um cliente pronto, então o starter
  não tem como checar.
- **JMS, factory sem pool nem cache.** O `checkAccess` precisa da conexão do cliente do broker. Com um
  `CachingConnectionFactory` (Spring) ou um `JmsPoolConnectionFactory` por cima, o health check falha com
  `UnsupportedOperationException`, e a checagem de `consumerWindowSize=0` do Artemis é pulada, porque a conexão não é
  a do cliente do broker. No ActiveMQ Classic, envio e consumo continuam funcionando (rodado com o
  `CachingConnectionFactory` do Spring 7.0.9 e as propriedades que o Boot aplica por padrão).
  - O autoconfigure do Spring Boot (ActiveMQ e Artemis, conferido nas fontes do 4.1.1) só declara
    `jmsConnectionFactory` quando a aplicação **não** declara nenhum `jakarta.jms.ConnectionFactory`
    (`@ConditionalOnMissingBean`). Declarar o bean da lib, como nos exemplos, desliga o do Boot: não precisa de
    `@Primary`.
  - Sem bean próprio, o starter usaria o do Boot, que por padrão é um `CachingConnectionFactory`
    (`spring.jms.cache.enabled`, padrão `true`) ou, com `spring.activemq.pool.enabled=true` /
    `spring.artemis.pool.enabled=true`, um `JmsPoolConnectionFactory`. É o caso acima. Evite: declare a factory da lib.
  - O bean da lib é o `ConnectionFactory` da aplicação inteira: o resto do código JMS dela (`JmsTemplate`,
    `@JmsListener`) o usa também.
- **Artemis, janela do consumer.** A factory precisa de `consumerWindowSize=0`; sem ela, `receiver(nome)` lança
  `IllegalArgumentException`, desde que a conexão seja a do cliente do broker (com cache ou pool por cima, a checagem
  é pulada, e o padrão do cliente Artemis é 1 MiB: o broker manda mensagens a mais para o buffer do consumer, presas
  sem lease; o Boot não define a janela). `JmsDialect.ARTEMIS.connectionFactory(url)` já aplica.
- **Reconexão e credenciais** de JMS e RabbitMQ são da factory: o starter chama `createConnection()` sem usuário nem senha. `JmsDialect.<X>.connectionFactory(url)` cuida da
  reconexão: no Classic, embrulha a URL em `failover:(...)`, salvo `failover:` e `vm:`; no Artemis, acrescenta
  `reconnectAttempts=-1`. Nos dois, o startup ainda falha com o broker fora.

### Migrando do ADR-0010

O starter deixou de criar clientes e de aceitar as chaves de conexão do provider (ADR-0011). Cada uma tem um
substituto na aplicação:

| Antes (YAML de `messaging.providers.<nome>`) | Agora |
|---|---|
| `aws`: `region`, `endpoint`, `access-key`, `secret-key` | Beans `SqsClient` e `SnsClient`, com região, endpoint e credenciais no builder |
| `azure`: `connection-string` | Bean `ServiceBusClientBuilder` |
| `gcp`: `emulator-host` | Beans `TransportChannelProvider` (canal sem TLS) e `NoCredentialsProvider` |
| `gcp`: `project` | Continua no YAML |
| `activemq-classic`, `artemis`: `broker-url`, `user`, `password` | Bean `jakarta.jms.ConnectionFactory`, com `JmsDialect.<X>.connectionFactory(url)` e as credenciais na factory |
| `rabbitmq`: `broker-url`, `user`, `password` | Bean `com.rabbitmq.client.ConnectionFactory` |
| Bean com o nome do provider (`aws`) ou `<provider><Tipo>` (`awsSnsClient`) | Qualquer nome; vale o bean do tipo, e `@Primary` desempata |
| Bean ignorado com aviso (AWS) ou falha (Azure, JMS, RabbitMQ) | Não existe mais: sem o bean obrigatório, o startup falha |
| Properties de conexão e bean juntos no AWS derrubavam o startup | Não há properties de conexão, então não há conflito |
| Dois providers do mesmo tipo (duas contas AWS, `artemis` e `activemq-classic`) | Derruba o startup: um provider por tipo de cliente. Providers de tipos diferentes convivem |

Na API Java, os records de `MessagingProperties` (`AwsProvider`, `AzureProvider`, `GcpProvider`, `JmsProvider`,
`RabbitProvider`) e os construtores dos `*ProviderConnection` mudaram.

Os erros de startup de `messaging.providers`, todos em português e com a chave completa:

| Mensagem | Quando |
|---|---|
| `messaging.providers.<nome>: declare um bean <tipo> (ADR-0011)` | Falta o bean obrigatório da tabela de "Clientes nativos" |
| `messaging.providers.<nome>: há mais de um bean <tipo>; marque um com @Primary (ADR-0011)` | Dois beans do tipo e nenhum `@Primary` |
| `messaging.providers: a e b usam o mesmo cliente nativo; um provider por tipo (ADR-0011)` | Dois providers do mesmo grupo (`activemq-classic` e `artemis` contam como um) |
| `messaging.providers.<nome>.<chave> foi removida (ADR-0011): configure <beans> na aplicação` | O YAML ainda tem `region`, `endpoint`, `access-key`, `secret-key`, `connection-string`, `emulator-host`, `broker-url`, `user` ou `password` |

Chaves removidas, chave desconhecida e campo de outro tipo, de todos os providers, saem juntos na mesma mensagem
(`Configuração inválida de messaging.providers:`). `type` ausente aborta no primeiro erro.

### Limites conhecidos

- O read timeout de um `SqsClient` da aplicação não é validado: o SDK não o expõe. O padrão (30 s) serve; abaixo de
  20 s, o long poll é cortado.
- No GCP, o cliente de administração de tópicos é criado no startup: sem credenciais padrão nem `CredentialsProvider`,
  o startup falha mesmo sem destinos.
- Verificado: o read timeout padrão do AWS SDK 2.55.6; as fontes do autoconfigure de JMS do Spring Boot 4.1.1; o
  ActiveMQ Classic atrás de um `CachingConnectionFactory` (Spring 7.0.9).
- Não verificado: o Artemis e o `pooled-jms` atrás de cache ou pool (só lido no código), e o efeito, no `JmsTemplate`
  da aplicação, de usar a factory da lib. Detalhes e pendências no
  [ADR-0011](docs/adr/0011-clientes-nativos-da-aplicacao.md#implementação-2026-11-15).

### Validação, métricas, tracing e health

- O startup falha listando todos os erros de configuração, e em seguida confere o `require`
  (`ordered-delivery`, `delayed-redelivery`, `native-dead-letter`, `publisher-deduplication`,
  `lease-expired-on-ack`) contra as capabilities dos adapters.
- Métricas (com `MeterRegistry`; referência completa em [docs/metricas.md](docs/metricas.md)): timers `messaging.send.duration`, `messaging.receive.duration`, `messaging.ack.duration`,
  `messaging.nack.duration`, `messaging.dead.letter.duration` com `provider`, `destination`, `exception` (nome simples da classe lançada, ou `none`) e `outcome`
  (`success`, `error` e, em lote com falha parcial, `partial`; `send` e `ack` têm também `batch`), a
  distribuição `messaging.receive.messages`, o counter `messaging.messages.failed` (uma contagem por mensagem que
  falhou, com `operation` = `send`, `ack`, `nack` ou `dead_letter`, inclusive item de lote) e a distribuição
  `messaging.message.size` (bytes do corpo enviado).
- Tracing (com `ObservationRegistry`): o envio abre a observation `messaging.publish` e grava o
  `traceparent` na mensagem. Com o `DefaultMeterObservationHandler`, ela gera também `messaging_publish_seconds` e
  `messaging_publish_active_seconds`, com as tags `provider` e `destination` dos meters `messaging.*` (mais o
  `error` do Micrometer); o span leva `messaging.system` e `messaging.destination.name`. No receive, `ReceivedMessage.traceparent()` traz o contexto de quem
  publicou; abrir o span de processamento é de quem consome.
- Health `messaging`: opt-in com `management.health.messaging.enabled=true`; chama `checkAccess()`
  de cada sender e receiver, sem consumir mensagens.

## Permissões mínimas

| Destino | Enviar | Receber | Health check |
|---|---|---|---|
| SQS | `sqs:SendMessage` | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes` | `sqs:GetQueueAttributes` |
| SNS | `sns:Publish` | (fila SQS) | `sns:GetTopicAttributes` |
| Service Bus | Send | Listen | Send (sender) / Listen (receiver) |
| Pub/Sub | `roles/pubsub.publisher` | `roles/pubsub.subscriber` | Nenhuma extra (`TestIamPermissions`) |
| ActiveMQ Classic | `write` | `read` (e `write` na fila, para o `nack` em `SCHEDULED`) | Leitura das advisories (`ActiveMQ.Advisory.>`) |
| ActiveMQ Artemis | `send` | `consume` (e `send` na fila, para o `nack` em `SCHEDULED`) | Nenhuma extra (`queueQuery`/`addressQuery`) |
| RabbitMQ | `write` no exchange | `read` na fila (e `write` no exchange da DLQ, para `deadLetter`) | Acesso ao vhost (`queueDeclarePassive`/`exchangeDeclarePassive`; permissão exigida a confirmar) |

O receiver SQS lê o visibility timeout da fila com `GetQueueAttributes`. A DLQ automática por
contagem de entregas (redrive policy, `MaxDeliveryCount`, dead letter topic), FIFO, sessions,
duplicate detection e ordering são configuração do destino (Terraform), não da lib.

## Versões testadas

| Componente | Versão | Contrato |
|---|---|---|
| Java | 21 | |
| Spring Boot | 4.1.1 | Testes de auto-configuração verdes |
| AWS SDK v2 | 2.44.7 | Verde no LocalStack 4.14.0 (última imagem sem `LOCALSTACK_AUTH_TOKEN`) |
| `azure-messaging-servicebus` | 7.18.0 | Verde no Service Bus emulator 2.0.0 (`MaxDeliveryCount` até 10; teste da DLQ nativa pulado por bug do emulator) |
| Google `libraries-bom` | 26.90.0 (`google-cloud-pubsub` 1.157.0) | Verde no `google-cloud-cli:587.0.0-emulators` |
| `activemq-client` | 6.2.5 | Verde no `apache/activemq-classic:6.2.0` com `schedulerSupport` |
| `artemis-jakarta-client` | 2.40.0 | Verde no `apache/activemq-artemis:2.40.0` sem auto-create |
| `amqp-client` | 5.35.0 (sem Netty) | Verde no `rabbitmq:4.1-management-alpine` (4.1.8) |
| Testcontainers | 2.0.5 | |

Na aplicação, importe o BOM do Spring Boot antes dos BOMs dos SDKs (ADR-0008).
