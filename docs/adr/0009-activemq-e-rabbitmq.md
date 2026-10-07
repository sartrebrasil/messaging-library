# ADR-0009: ActiveMQ e RabbitMQ: adapters de broker próprio, com lease controlado pela lib

- Status: aceito
- Data: 2026-10-06
- Altera: ADR-0001 (escopo)

## Contexto

O ADR-0001 deixou RabbitMQ e ActiveMQ fora da v1 "sem compromisso", porque não
são serviços de nuvem com SDK próprio. Integrações próximas precisam de
ActiveMQ, e a outbox-library já publica em RabbitMQ (`com.rabbitmq:amqp-client`).
A pergunta é se a API da v1 aguenta esses brokers sem mudar.

Os dois têm cliente oficial em Java:

| Broker | Cliente oficial | Protocolo |
|---|---|---|
| ActiveMQ Classic 6.x | `org.apache.activemq:activemq-client` (já Jakarta; o `activemq-client-jakarta` é da linha 5.18) | OpenWire, API Jakarta Messaging 3 |
| ActiveMQ Artemis 2.x | `org.apache.activemq:artemis-jakarta-client` | Core, API Jakarta Messaging 3 |
| RabbitMQ 3.x e 4.x | `com.rabbitmq:amqp-client` 5.x | AMQP 0-9-1 |
| RabbitMQ 4.x | `com.rabbitmq.client:amqp-client` | AMQP 1.0 |

A diferença central é o lease. Os provedores de nuvem devolvem a mensagem quando
um prazo vence (ADR-0002). Nos dois brokers não há prazo: a mensagem não
confirmada fica com o consumidor até o canal fechar (session JMS, channel AMQP).
O RabbitMQ tem só o `consumer_timeout` (padrão 30 min), que derruba o channel
inteiro. O contrato do testkit cobre esse comportamento em
`unackedMessageIsRedeliveredAfterLease`, `extendLeaseKeepsMessageInvisible`,
`keepAliveStopsAtMaxTotal` e `ackAfterLeaseExpiredFails`.

O JMS tem outros dois desencaixes:

- `CLIENT_ACKNOWLEDGE` confirma todas as mensagens já consumidas na session, não
  só uma. O ack individual existe, mas com constante própria de cada broker
  (Classic `4`, Artemis `101`).
- Não existe nack por mensagem: `session.recover()` devolve tudo o que a session
  tem pendente.

## Decisão

- Dois módulos novos, com a mesma receita dos adapters de nuvem: `messaging-jms`
  e `messaging-rabbitmq`. O core e o contrato não mudam.
- A ordem é ActiveMQ primeiro, depois RabbitMQ.
- Usar JMS aqui não contradiz o ADR-0001. Lá o JMS foi recusado como abstração
  comum entre nuvens; aqui ele é só o cliente oficial do ActiveMQ dentro de um
  adapter.

### Lease controlado pela lib

Nos dois adapters o lease é um prazo local, contado pelo receiver:

| Tema | Regra |
|---|---|
| Duração | Configurada no adapter (`lease`, padrão 60 s). `leaseExpiresAt = receivedAt + lease`. |
| Vencimento | Um agendador do receiver devolve a mensagem ao broker quando o prazo vence. JMS: fecha a session da mensagem. RabbitMQ: `basicNack(requeue=true)`. |
| `extendLease` | Só move o prazo local. Não chama o broker. |
| `keepAlive` | `LeaseKeeper` do SPI, sem mudança. |
| `ack` depois do vencimento | `LeaseExpiredException`, decidido localmente. Por isso `reportsLeaseExpiredOnAck = true`. |
| Queda do processo ou da conexão | O broker devolve as mensagens sozinho, porque o canal fechou. |
| Limite do broker | No RabbitMQ, o `consumer_timeout` da fila precisa ser maior que o maior `maxTotal` de `keepAlive`. Senão o broker fecha o channel e devolve todas as mensagens dele. |

### ActiveMQ: `messaging-jms`

Um adapter sobre `jakarta.jms`, que recebe a `Connection` pronta. A conexão é
thread-safe; o adapter cria as próprias sessions e não fecha a conexão (ADR-0002).
O que não é padrão JMS fica no `JmsDialect` (`ACTIVEMQ_CLASSIC`, `ARTEMIS`),
passado na construção de senders e receivers. As classes de cada cliente só são
carregadas pelo dialeto que as usa, e os dois clientes são dependências
opcionais. Os achados que mudaram esta seção estão nos spikes
[F7](../spikes/f7-activemq-classic.md) (Classic) e
[F8](../spikes/f8-activemq-artemis.md) (Artemis).

| Tema | Regra |
|---|---|
| Receiver | Um pool de sessions `CLIENT_ACKNOWLEDGE`, cada uma com um consumer e no máximo uma mensagem pendente. Com uma mensagem por session, o ack confirma só ela e o adapter fica no JMS padrão. O tamanho do pool (`max-in-flight`, padrão 10) limita as mensagens pendentes por receiver e reduz `maxMessages`. |
| Prefetch | Precisa ser 0: com prefetch, o broker manda mensagens a mais para o buffer do consumer, e elas ficam presas sem lease. Classic: o adapter acrescenta `?consumer.prefetchSize=0` ao nome da fila. Artemis: `consumerWindowSize=0` na connection factory. O receiver recusa uma conexão com janela (`IllegalArgumentException`), e o starter acrescenta o parâmetro à `broker-url`. |
| `receive` | `consumer.receive(restante)` na primeira session livre; depois `receiveNoWait` nas outras até `maxMessages`. |
| Threads | Cada session é usada por uma mensagem por vez, protegida por `ReentrantLock` (não `synchronized`, por causa das virtual threads). Assim o `ack` de uma mensagem não espera o `receive` de outra. |
| `ack` | `message.acknowledge()` e a session volta ao pool. |
| `nack(0)` e lease vencido | Fecha a session e abre outra no lugar. O broker devolve a mensagem com o mesmo `JMSMessageID` e soma uma entrega. |
| `nack` com atraso | Escolhido no adapter (`Redelivery`), como no Service Bus. `IMMEDIATE` (padrão): ignora o atraso. `SCHEDULED`: cópia agendada para a mesma fila (no Artemis, para o FQQN da subscription, nunca para o endereço multicast), depois `ack` da original. É a mesma ordem do `RESCHEDULE` (ADR-0005): uma falha no meio gera duplicata, nunca perda. A cópia ganha `JMSMessageID` novo e o atributo reservado `delivery_count`. Artemis: `deliveryDelay` do JMS 2.0, sem configuração no broker. Classic: não aceita `deliveryDelay`; usa `AMQ_SCHEDULED_DELAY`, que exige `schedulerSupport="true"` no broker. Sem ele, o broker entrega a cópia na hora sem avisar, e o adapter não consegue detectar isso. `maxRedeliveryDelay` é 12 h. |
| `deadLetter` | Cópia para o sender de DLQ configurado e depois `ack` (ADR-0005). `nativeDeadLetter = false`: o JMS não tem dead letter explícito. |
| DLQ automática do broker | A política de reentrega do broker (`maximumRedeliveries` no Classic, `max-delivery-attempts` no Artemis) continua valendo para o `nack(0)` e para o lease vencido. As cópias de `nack` com atraso zeram a contagem do broker, então quem consome decide pela `deliveryCount`. |
| `deliveryCount` | Atributo `delivery_count` quando existe; senão `JMSXDeliveryCount`. |
| Corpo e atributos | Sempre `BytesMessage`. Atributos como string properties: as chaves do ADR-0004 são identificadores JMS válidos. `contentType` vai no atributo reservado `content_type`. Na leitura, as propriedades `JMS*`, `_AMQ*` e `__AMQ*` são do broker e ficam fora dos atributos. |
| `orderingKey` | `JMSXGroupID` (message groups). Nos dois brokers, a mensagem devolvida por session fechada volta para a frente do grupo (F7, F8), então `orderedDelivery = true` em `IMMEDIATE`. Em `SCHEDULED` é `false`, porque a cópia agendada vai para o fim da fila. Envio sem `orderingKey` é aceito. |
| `deduplicationId` | Classic: ignorado (`publisherDeduplication = false`). Artemis: `_AMQ_DUPL_ID`; a duplicata responde sucesso e é descartada em silêncio. A cópia de `nack` não leva o id, senão o broker a descartaria. |
| Sender | Uma session por sender, protegida por lock. Envio persistente e síncrono: no Classic, `useAsyncSend=false`; no Artemis, `blockOnDurableSend=true` (padrão). `maxBatchSize = 1`; o `sendAll` padrão do core basta. |
| Tamanho | Não há limite fixo (`wireFormat.maxFrameSize` no Classic, 100 MiB por padrão; large messages no Artemis): `maxMessageBytes` é configurado no sender, como no SNS. |
| `checkAccess` | Abrir producer, consumer ou browser cria o destino. Por isso o adapter consulta o broker sem criar nada. Classic: procura o destino no `DestinationSource` da conexão (advisories); um destino inexistente custa 2 s de espera. Artemis: `queueQuery` e `addressQuery` da session Core, que respondem na hora. Os dois exigem a conexão do cliente do broker: atrás de pool (`pooled-jms`), lançam `UnsupportedOperationException`. |
| Java 21 | O `receive(timeout)` dos dois clientes espera dentro de `synchronized` e prende a carrier thread de uma virtual thread durante o `maxWait`. No Java 24+ (JEP 491) isso não acontece. |
| Ciclo de vida | `close()` fecha as sessions do adapter. As mensagens pendentes voltam ao broker. |
| Reconexão | A lib não recria a `Connection` (ADR-0002: ela é de quem a passou). Por isso, `JmsDialect.connectionFactory` já monta a URL com reconexão. Classic: `failover:(...)?startupMaxReconnectAttempts=0&timeout=30000`. Artemis: `reconnectAttempts=-1`. Verificado com restart do broker nos dois, e o startup continua falhando com o broker fora. |

Topologia, completando o ADR-0003:

| Broker | Sender de fila | Sender de tópico | Receiver |
|---|---|---|---|
| Classic | Fila | Virtual Topic `VirtualTopic.<t>` | Fila (`Consumer.<s>.VirtualTopic.<t>` para subscription) |
| Artemis | Endereço anycast | Endereço multicast | Fila; subscription por FQQN `<endereço>::<fila>` |

Subscription JMS durável não é usada. O receiver sempre lê de uma fila, como no
SNS → SQS.

Erros, completando o ADR-0007:

| Exceção da lib | JMS |
|---|---|
| `DestinationNotFoundException` | `InvalidDestinationException` |
| `AccessDeniedException` | `JMSSecurityException` |
| `ThrottledException` | `ResourceAllocationException` (producer flow control) |
| `LeaseExpiredException` | `jakarta.jms.IllegalStateException` no `acknowledge` (session fechada); lease vencido localmente |
| `MessagingException` retryable | Falha de conexão |

Os dois brokers criam o destino no primeiro uso por padrão (auto-create). Por
isso, envio e receive para destino inexistente não falham, e só o `checkAccess`
informa que o destino não existe. No Classic, desligar o auto-create exige o
plugin de autorização. Isso fica na configuração do broker, que faz o papel do
Terraform. O contrato roda com auto-create ligado e pula
`missingDestinationIsReported`.

### RabbitMQ: `messaging-rabbitmq`

Cliente `com.rabbitmq:amqp-client` 5.x, o mesmo da outbox-library. Funciona em
RabbitMQ 3.x e 4.x. O cliente AMQP 1.0 só entra se todos os brokers forem 4.x.
Os achados que mudaram esta seção estão no [spike F9](../spikes/f9-rabbitmq.md).

| Tema | Regra |
|---|---|
| Receiver | Um channel por receiver e pull de verdade: `basicGet` até `maxMessages`. Com a fila vazia, tenta de novo a cada 100 ms até `maxWait`. Não há consumer registrado: com `basicConsume` e prefetch, a mensagem devolvida no fim do lease voltaria ao buffer do mesmo receiver (F9), e um receiver parado seguraria mensagens. |
| `ack` / `nack(0)` / lease vencido | `basicAck` ou `basicNack(requeue=true)` no channel da entrega, protegido por lock. Um delivery tag desconhecido derruba o channel (406) e devolve tudo o que está pendente nele. Por isso o adapter tira o tag da lista de pendentes antes de confirmar, e nunca confirma um tag duas vezes. |
| `nack` com atraso | Não há atraso nativo: o plugin de delayed exchange é da comunidade, e a fila de retry com TTL e DLX é topologia. Fica `delayedRedelivery = false`, como o `ABANDON` do Service Bus. O atraso é ignorado. |
| `deadLetter` | Cópia para o sender de DLQ e depois `ack`. O `basicReject(requeue=false)` para a DLX perderia o `reason`. Mensagem lida de uma DLQ da DLX traz `x-first-death-reason` em `deadLetter().reason()`. |
| `deliveryCount` | Quorum queue: `x-delivery-count + 1`, porque o header conta as entregas anteriores e falta na primeira. Classic queue: 1 na primeira entrega, vazio depois. No 4.x, a quorum queue tem `delivery-limit` 20 por padrão. |
| `messageId` | O broker não gera: o sender grava um UUID em `message-id`. Ele se mantém no requeue. Mensagem de outro produtor sem `message-id` recebe um UUID a cada entrega. |
| Sender | Publisher confirms. Fila: exchange padrão, routing key igual ao nome da fila e `mandatory` com `ReturnListener`. O `NO_ROUTE` chega antes do confirm e vira `DestinationNotFoundException`. Tópico: exchange com routing key fixa na construção e sem `mandatory`: exchange sem fila ligada descarta, como tópico sem subscriptions. Um channel por sender, protegido por lock e recriado depois de erro. |
| Atributos | Headers AMQP. Na leitura, os headers `x-*` são do broker e ficam fora dos atributos. `orderingKey` e `deduplicationId` são ignorados. |
| Ordem e deduplicação | `orderedDelivery = false`, `publisherDeduplication = false`. |
| Reconexão e channel fechado | Cada channel tem uma geração. Quando ele fecha (queda, reconexão automática, erro do broker), as confirmações das mensagens dele lançam `LeaseExpiredException`, e o próximo `receive` abre outro channel. O channel antigo, se a reconexão automática o reabrir, é abortado. |
| Alarme de memória ou disco | `connection.blocked` trava o publish. Um confirm que não chega em 30 s lança `MessagingException` com `retryable = true`. |
| `consumer_timeout` | Não verificado se o broker aplica o timeout a entregas de `basic.get`. Por segurança, mantenha o maior `maxTotal` de `keepAlive` abaixo dele (padrão 30 min). |
| `checkAccess` | `queueDeclarePassive` ou `exchangeDeclarePassive` num channel descartável, porque um 404 fecha o channel. |
| Tamanho | `max_message_size` do broker, 16 MiB por padrão no 4.x: `maxMessageBytes` é configurado no sender. Acima do limite do broker, o 406 vira `MessageTooLargeException`. |
| Erros | Reply code 404 = `DestinationNotFoundException`, 403 = `AccessDeniedException`, 406 com "max size" = `MessageTooLargeException`, 406 ou channel fechado numa confirmação = `LeaseExpiredException`. |
| Dependência | O `amqp-client` 5.35 declara Netty 4.2 como transporte opcional, e ele se mistura ao Netty 4.1 do Artemis no mesmo classpath. O módulo exclui `io.netty:*` e usa o IO por socket padrão. |

### Capabilities

| Campo | ActiveMQ Classic | ActiveMQ Artemis | RabbitMQ |
|---|---|---|---|
| `maxMessageBytes` | Configurado no adapter | Configurado no adapter | Configurado no adapter (16 MiB padrão) |
| `maxBatchSize` | 1 | 1 | 1 |
| `delayedRedelivery` | Só em `SCHEDULED`, por cópia | Só em `SCHEDULED`, por cópia | Não |
| `maxRedeliveryDelay` | 12 h em `SCHEDULED`; zero em `IMMEDIATE` | 12 h em `SCHEDULED`; zero em `IMMEDIATE` | Zero |
| `nativeDeadLetter` | Não | Não | Não |
| `orderedDelivery` | Só em `IMMEDIATE` | Só em `IMMEDIATE` | Não |
| `publisherDeduplication` | Não | Sim | Não |
| `reportsLeaseExpiredOnAck` | Sim (lease local) | Sim (lease local) | Sim (lease local) |

### Testes e starter

- O contrato roda com broker real no CI. Classic: `GenericContainer` com
  `apache/activemq-classic:6.2.0` e `schedulerSupport` ligado; o
  `testcontainers-activemq` 2.x não está no repositório local, e o container
  genérico basta. Artemis: `GenericContainer` com `apache/activemq-artemis:2.40.0`
  criado com `--no-autocreate`. Os testes criam endereços e filas, e o contrato
  de destino inexistente roda. RabbitMQ: `GenericContainer` com
  `rabbitmq:4.1-management-alpine` e quorum queues; o contrato de destino
  inexistente roda.
- No starter, os tipos de provedor são `activemq-classic`, `artemis` e
  `rabbitmq`. A conexão vem de um bean da aplicação (`ConnectionFactory` JMS,
  `com.rabbitmq.client.ConnectionFactory`) ou da configuração.
- Dependências: `messaging-jms` só depende de `jakarta.jms:jakarta.jms-api`; o
  cliente do broker (`activemq-client` ou `artemis-jakarta-client`) é opcional e
  fica com quem usa. `messaging-rabbitmq` depende de
  `com.rabbitmq:amqp-client`, sem o Netty.

### Pendências

Os spikes F7 ([Classic](../spikes/f7-activemq-classic.md)) e F8
([Artemis](../spikes/f8-activemq-artemis.md)) responderam as do ActiveMQ, e o F9
([RabbitMQ](../spikes/f9-rabbitmq.md)) as do RabbitMQ. Continua aberta só a
aplicação do `consumer_timeout` a entregas de `basic.get`.

O starter ganhou os tipos `activemq-classic`, `artemis` e `rabbitmq`, completando
o ADR-0008:

- A conexão vem de `broker-url` (com `user` e `password`) ou do bean
  `jakarta.jms.ConnectionFactory`. É uma por provider, aberta no startup e
  fechada no shutdown.
- `topic` + `subscription` recebe da fila `Consumer.<subscription>.<topic>` no
  Classic e do FQQN `<topic>::<subscription>` no Artemis.
- No Artemis, a `broker-url` ganha `consumerWindowSize=0` quando não define a
  janela.
- RabbitMQ:
  - a `broker-url` é a URI AMQP; sem ela, o starter usa o bean
    `com.rabbitmq.client.ConnectionFactory`;
  - `topic` é o exchange e `routing-key` a routing key do envio;
  - `subscription` é a fila ligada ao exchange;
  - `redelivery: reschedule` é recusado no startup.
- `redelivery: reschedule` vira `SCHEDULED`.
- `lease` é o prazo local, com padrão de 60 s.
- O pool de sessions usa `max-in-flight` 10, fixo por enquanto.

## Consequências

- Trocar de nuvem para ActiveMQ ou RabbitMQ muda dependência e configuração, não
  código, dentro do que as capabilities informam.
- O lease local é mais previsível que o dos provedores (o `ack` tardio sempre
  falha), mas só vale dentro do processo. Outro consumidor do mesmo broker, sem a
  lib, segura a mensagem até fechar o canal.
- No ActiveMQ, as mensagens pendentes por receiver ficam limitadas pelo pool de
  sessions. Cada `nack(0)` custa fechar e abrir uma session.
- No RabbitMQ, retry com atraso fica com quem consome, pela capability
  `delayedRedelivery`, como no Service Bus em `ABANDON`.
- O adapter JMS deixa aberto o caminho para IBM MQ e outros brokers JMS, como mais
  um valor do enum de dialeto.
