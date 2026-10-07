# messaging-lib

Abstração de filas e tópicos para Java 21: uma API única sobre os SDKs oficiais da AWS
(SQS, SNS), da Azure (Service Bus) e do Google Cloud (Pub/Sub), e sobre o cliente Jakarta Messaging
do ActiveMQ (Classic e Artemis) e do RabbitMQ. Trocar de provedor muda a
dependência e a configuração, não o código. O que não dá para igualar entre provedores fica
explícito em `Capabilities`.

> Versão 0.x: a API ainda pode mudar entre versões menores.
> Plano e decisões em [`docs/plan.md`](docs/plan.md) e [`docs/adr/`](docs/adr/).

Não é um framework de consumo: não tem listener, pool de threads, retry por política nem
idempotência. A lib entrega `receive`, `ack`, `nack`, lease e dead letter; o loop de consumo é de
quem usa (a inbox-library, por exemplo).

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
| `messaging-spring-boot-starter` | Destinos nomeados por properties (`messaging.*`), métricas, `traceparent` e health. Spring Boot 4. |
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
  providers:
    aws:
      type: aws                # aws | azure | gcp | activemq-classic | artemis | rabbitmq
      region: sa-east-1
    azure:
      type: azure
      connection-string: ${SERVICEBUS_CONNECTION_STRING}
    gcp:
      type: gcp
      project: meu-projeto
    amq:
      type: activemq-classic   # sem broker-url, usa o bean jakarta.jms.ConnectionFactory
      broker-url: failover:(tcp://amq-1:61616,tcp://amq-2:61616)
      user: ${AMQ_USER}
      password: ${AMQ_PASSWORD}
  destinations:
    pedidos:
      provider: aws
      queue-url: https://sqs.sa-east-1.amazonaws.com/123/pedidos.fifo
      dead-letter: pedidos-dlq
      require: [ordered-delivery]
    pedidos-dlq:
      provider: aws
      queue-url: https://sqs.sa-east-1.amazonaws.com/123/pedidos-dlq.fifo
    eventos:
      provider: azure
      topic: eventos
      subscription: faturamento
      sessions: true
    eventos-dlq:               # Service Bus: só recebe, da DLQ nativa da subscription
      provider: azure
      topic: eventos
      subscription: faturamento
      dead-letter-queue: true
    notas:
      provider: gcp
      topic: notas
      subscription: notas-sub
      ack-deadline: 60s
    notas-amq:
      provider: amq
      topic: VirtualTopic.notas  # envia ao Virtual Topic
      subscription: faturamento  # recebe da fila Consumer.faturamento.VirtualTopic.notas
      lease: 60s
      redelivery: reschedule     # nack com atraso; exige schedulerSupport no broker
      dead-letter: notas-amq-dlq
    notas-amq-dlq:
      provider: amq
      queue: notas-dlq
```

No RabbitMQ (`type: rabbitmq`), `broker-url` é a URI AMQP (`amqp://host:5672/vhost`), `topic` é o
exchange, `routing-key` a routing key do envio e `subscription` a fila ligada ao exchange.
`redelivery: reschedule` é recusado no startup, porque o RabbitMQ não tem atraso no `nack`.

No Artemis (`type: artemis`), `topic` é o endereço multicast e `topic` + `subscription` recebe do
FQQN `<topic>::<subscription>`. A `broker-url` ganha `consumerWindowSize=0` quando não define a
janela.

```java
@Autowired MessagingDestinations destinations;

destinations.sender("pedidos").send(message);            // mesma instância sempre
try (MessageReceiver receiver = destinations.receiver("pedidos")) {   // instância nova a cada chamada
    ...
}
```

| Destino | Envia para | Recebe de |
|---|---|---|
| `aws` | `topic-arn`, senão `queue-url` | `queue-url` |
| `azure` | `topic`, senão `queue` | `topic` + `subscription`, senão `queue` |
| `gcp` | `topic` | `subscription` (exige `ack-deadline`) |
| `activemq-classic` | `topic` (`VirtualTopic.*`), senão `queue` | `topic` + `subscription` (fila `Consumer.<subscription>.<topic>`), senão `queue` |
| `artemis` | `topic` (endereço multicast), senão `queue` | `topic` + `subscription` (FQQN `<topic>::<subscription>`), senão `queue` |
| `rabbitmq` | `topic` (exchange, com `routing-key`), senão `queue` | `topic` + `subscription` (a fila ligada ao exchange), senão `queue` |

Outras propriedades do destino: `max-message-bytes` (SNS, Service Bus, ActiveMQ e RabbitMQ),
`sessions` (Service Bus), `redelivery: abandon | reschedule` (Service Bus e ActiveMQ), `ordered` e
`exactly-once` (Pub/Sub), `lease` (ActiveMQ e RabbitMQ, padrão 60 s), `routing-key` (RabbitMQ).

No ActiveMQ, o starter abre uma conexão por provider no startup e a fecha no shutdown. A lib não
recria a conexão: a reconexão vem da URL. Com `broker-url`, o `JmsDialect` cuida disso:
- no Classic, embrulha a URL em `failover:(...)`, salvo `failover:` e `vm:`;
- no Artemis, acrescenta `reconnectAttempts=-1`;
- nos dois, o startup ainda falha com o broker fora.

Num bean `ConnectionFactory` próprio, a reconexão é responsabilidade de quem o configura. Se o bean
`ConnectionFactory` tiver pool por cima (`pooled-jms`, `CachingConnectionFactory`), o health check
falha, porque o `checkAccess` precisa da `ActiveMQConnection`. Nesse caso, use `broker-url`.

- O startup falha listando todos os erros de configuração, e em seguida confere o `require`
  (`ordered-delivery`, `delayed-redelivery`, `native-dead-letter`, `publisher-deduplication`,
  `lease-expired-on-ack`) contra as capabilities dos adapters.
- Clientes: beans `SqsClient`/`SnsClient` e `ServiceBusClientBuilder` da aplicação são
  reaproveitados; sem eles, o starter cria e fecha os seus. No GCP, credenciais padrão do Google,
  ou `emulator-host` para o emulator.
- Métricas (com `MeterRegistry`): timers `messaging.send`, `messaging.receive`, `messaging.ack`,
  `messaging.nack`, `messaging.dead.letter` com `provider`, `destination` e `outcome`, e a
  distribuição `messaging.receive.messages`.
- Tracing (com `ObservationRegistry`): o envio abre a observation `messaging.publish` e grava o
  `traceparent` na mensagem. No receive, `ReceivedMessage.traceparent()` traz o contexto de quem
  publicou; abrir o span de processamento é de quem consome.
- Health `messaging`: opt-in com `management.health.messaging.enabled=true`; chama `checkAccess()`
  de cada destino, sem consumir mensagens.

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
| `azure-messaging-servicebus` | 7.18.0 | Pendente: Service Bus emulator 2.0.0 (`MaxDeliveryCount` até 10) |
| Google `libraries-bom` | 26.90.0 (`google-cloud-pubsub` 1.157.0) | Pendente: `google-cloud-cli:587.0.0-emulators` |
| `activemq-client` | 6.2.5 | Verde no `apache/activemq-classic:6.2.0` com `schedulerSupport` |
| `artemis-jakarta-client` | 2.40.0 | Verde no `apache/activemq-artemis:2.40.0` sem auto-create |
| `amqp-client` | 5.35.0 (sem Netty) | Verde no `rabbitmq:4.1-management-alpine` (4.1.8) |
| Testcontainers | 2.0.5 | |

Na aplicação, importe o BOM do Spring Boot antes dos BOMs dos SDKs (ADR-0008).
