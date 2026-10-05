# Plano da library

Abstração de filas e tópicos para Java 21: uma API única sobre os SDKs de
mensageria da AWS (SQS, SNS), Azure (Service Bus) e Google Cloud (Pub/Sub) na
v1, e OCI (Queue) depois. É o equivalente da storage-lib para mensageria: core sem
dependências, um módulo fino por provedor e um contrato de testes comum.

Não é um framework de consumo. Não tem listener, pool de threads, retry,
idempotência nem DLQ por política. Essas responsabilidades ficam com quem usa a
lib (por exemplo, a inbox-library, que vai usá-la como uma implementação do seu
`BrokerAdapter`).

## Decisões

| Tema | Decisão | ADR |
|---|---|---|
| Escopo | Só data plane de filas e tópicos de nuvem; sem Kafka, sem criação de recursos | [0001](adr/0001-escopo.md) |
| Provedores da v1 | AWS, Azure e GCP, todos com emulador e contrato no CI; OCI depois da v1 | [0001](adr/0001-escopo.md) |
| Corte da v1 | Core, testkit, adapters, starter, métricas, `traceparent` e health check | este plano |
| Modelo de API | `MessageSender` e `MessageReceiver` síncronos; receive por pull com o lease do destino; `keepAlive` opt-in; ack no receiver; sem `sendAsync` | [0002](adr/0002-pull-com-lease-sincrono.md) |
| Topologia | Fila, tópico e subscription mapeados por provedor; OCI, quando entrar, só com Queue | [0003](adr/0003-topologia-por-provedor.md) |
| Mensagem | Corpo em `byte[]`; até 16 atributos portáveis, empacotados no SQS/SNS acima de 7; Base64 por `contentType` | [0004](adr/0004-contrato-da-mensagem.md) |
| Confirmação | `ack`, `nack(delay)`, `extendLease`, `deadLetter` com fallback explícito | [0005](adr/0005-ack-nack-lease-dead-letter.md) |
| Ordem e duplicidade | `orderingKey` e `deduplicationId` opcionais; garantia informada por capability; sessions do Service Bus na v1 | [0006](adr/0006-ordenacao-e-deduplicacao.md) |
| Capabilities e erros | `Capabilities` por receiver/sender; hierarquia `MessagingException` | [0007](adr/0007-capabilities-e-erros.md) |
| Spring e observabilidade | Starter monta clientes e destinos por nome; métricas e tracing como decorators | [0008](adr/0008-spring-boot-e-observabilidade.md) |
| Coordenadas | `com.example:messaging-*`, pacote `com.example.messaging`, Java 21, Spring Boot 4.x | este plano |
| Distribuição | Igual à storage-lib: `mvn install` local; publicação decidida depois | este plano |
| SDKs | SDK oficial de cada provedor, sem Spring Cloud AWS/Azure/GCP/OCI | [0001](adr/0001-escopo.md) |

## API alvo (v1)

```java
public interface MessageSender extends AutoCloseable {
    SendResult send(OutgoingMessage message);
    BatchSendResult sendAll(List<OutgoingMessage> messages);   // não atômico; devolve as falhas
    void checkAccess();                                         // base do health check, sem efeito colateral
    Capabilities capabilities();
}

public interface MessageReceiver extends AutoCloseable {
    List<ReceivedMessage> receive(int maxMessages, Duration maxWait);
    void ack(ReceivedMessage message);
    AckResult ackAll(List<ReceivedMessage> messages);           // não atômico; devolve as falhas
    void nack(ReceivedMessage message, Duration redeliverAfter);
    void extendLease(ReceivedMessage message, Duration lease);
    KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal);   // renova até close(), ack/nack ou maxTotal
    void deadLetter(ReceivedMessage message, String reason);
    void checkAccess();
    Capabilities capabilities();
}

public record OutgoingMessage(byte[] body, Map<String, String> attributes, String contentType,
                              String orderingKey, String deduplicationId, String traceparent) { ... }

public record ReceivedMessage(String messageId, byte[] body, Map<String, String> attributes,
                              String contentType, String orderingKey, OptionalInt deliveryCount,
                              Instant enqueuedAt, Instant receivedAt, Instant leaseExpiresAt,
                              String traceparent, DeadLetterInfo deadLetter, Object handle) { ... }

public record Capabilities(long maxMessageBytes, int maxBatchSize, boolean delayedRedelivery,
                           Duration maxRedeliveryDelay, boolean nativeDeadLetter, boolean orderedDelivery,
                           boolean publisherDeduplication, boolean reportsLeaseExpiredOnAck) { }
```

`traceparent` é campo próprio, não atributo, porque `traceparent` é chave reservada
(o starter preenche). `deadLetter` traz o id original e o motivo quando a mensagem
foi lida de uma DLQ.

Pacote `com.example.messaging.spi`, para quem escreve adapter: `ReservedAttributes`,
`AttributeCodec` (empacotamento em `attributes`), `BodyCodec` (texto ou Base64 por
`contentType`) e `LeaseKeeper` (implementação de `keepAlive`).

Exceções: `MessagingException` > `DestinationNotFoundException`,
`AccessDeniedException`, `MessageTooLargeException`, `LeaseExpiredException`,
`ThrottledException`.

## Regras de semântica

| Tema | Regra |
|---|---|
| Destino | Cada sender/receiver é ligado a um único destino na construção, como o `ObjectStorage` a um bucket. |
| Clientes | Adapters recebem o cliente nativo pronto (`SqsClient`, `ServiceBusSenderClient`, ...); credenciais e região são de quem constrói. |
| `receive` | Bloqueia até `maxWait` ou até ter mensagens; lista vazia não é erro. `maxMessages` acima do limite do provedor é reduzido ao limite. |
| Lease | Duração definida pelo destino (visibility timeout, lock duration, ack deadline); `leaseExpiresAt` informa quando vence. `keepAlive` renova quando falta 1/3, até `maxTotal`. |
| Ciclo de vida | Senders e receivers thread-safe; o adapter não fecha cliente nativo recebido; uso depois do `close()` lança `IllegalStateException`. |
| `ack` / `nack` / `extendLease` | Após o lease vencer lançam `LeaseExpiredException` onde o provedor informa. Exceções: `ack` no SQS (`DeleteMessage` com handle antigo pode responder sucesso) e no Pub/Sub sem exactly-once. Quem consome precisa ser idempotente. |
| Corpo | Não vazio, validado no core. |
| `nack` com delay | Delay acima do suportado lança `IllegalArgumentException`. No Service Bus, o comportamento é escolhido no adapter (ADR-0005). |
| `deadLetter` | Nativo onde existe; senão, envio de cópia a um sender configurado + `ack`; sem sender, `UnsupportedOperationException`. |
| Atributos | Até 16; chaves `[a-z_][a-z0-9_]*` (até 64, sem prefixo `goog`), valores ASCII até 1024 bytes, validados no core. Reservados: `content_type`, `traceparent`, `dead_letter`, `delivery_count`, `attributes`. No SQS/SNS, acima de 7 todos vão empacotados em `attributes` (JSON). |
| Corpo binário | SQS/SNS: Base64 quando o `contentType` não é textual; o receiver devolve os bytes originais. |
| Tamanho | Cada adapter valida o limite do provedor antes de enviar e lança `MessageTooLargeException`. |
| Lotes | `sendAll`, `ackAll` não atômicos; o adapter quebra em lotes do tamanho do provedor. |
| Serialização | Fora da lib: corpo é `byte[]`, `contentType` é informativo. |

## Testes

- `messaging-testkit`: `MessagingContract` abstrato (um só, porque testar o receiver
  exige enviar); `InMemoryMessaging` (fila e tópico em memória) precisa passar. Cada
  teste marca as próprias mensagens com o atributo `contract_run` e descarta as de
  outros testes, então o destino pode ser compartilhado (os entities do Service Bus
  emulator são fixos).
- AWS: Testcontainers (`testcontainers-localstack` 2.x) + LocalStack (SQS
  standard e FIFO, SNS → SQS com raw delivery). Imagem fixada em
  `localstack/localstack:4.14.0`, a última que roda sem `LOCALSTACK_AUTH_TOKEN`
  (sem correções novas; licença para uso comercial a confirmar com o jurídico). Os limites (10 atributos, `MaximumMessageSize`) não são
  confiáveis no LocalStack e são testados com mocks.
- Azure: `testcontainers-azure` 2.x, `ServiceBusEmulatorContainer` +
  `MSSQLServerContainer` (fila, tópico e subscription, com e sem sessions,
  duplicate detection). O emulator limita a mensagem a 256 KB e o TTL a 1 h;
  Premium (mensagem grande) fica sem teste automatizado.
- GCP: `testcontainers-gcloud` 2.x, `PubSubEmulatorContainer` com versão de
  imagem fixada (ordering, DLQ, retry policy). Exactly-once e IAM (health
  check) não existem no emulator: testados com mocks.
- OCI (depois da v1): sem emulador; mocks no build e contrato contra fila real
  rodado manualmente.
- Teste de fumaça com os três adapters no mesmo classpath.
- Cada adapter declara as capabilities que o contrato pula (por exemplo, `nack`
  com delay no Service Bus em modo `ABANDON`).

## Fases

| Fase | Entrega | Pronto quando | Status |
|---|---|---|---|
| F0 | Spike de SDKs e emuladores ([achados](spikes/f0-sdks-e-emuladores.md)) | Pendências respondidas e registradas nos ADRs | Concluída |
| F1 | Core, exceções, validação, `InMemoryMessaging`, contratos | InMemory passa nos contratos | Concluída: 34 testes no core, 27 no contrato do InMemory |
| F2 | `messaging-aws` (SQS + SNS) | Contrato verde com LocalStack, standard e FIFO | Concluída: contrato verde no LocalStack 4.14.0 (SQS standard, FIFO, DLQ por cópia; SNS → SQS raw), 1 MiB validado. SDK 2.44.7 (cache local); subir para 2.55.x com acesso ao Nexus |
| F3 | `messaging-azure` (Service Bus fila, tópico e subscription, com e sem sessions) | Contrato verde com o emulator nos dois modos | Concluída: contrato verde no emulator 2.0.0 (fila em `ABANDON` e `RESCHEDULE`, subscription, sessions, DLQ nativa). `deadLetterMovesMessageToDeadLetterQueue` falha às vezes: o emulator força o detach dos receivers da conexão no `complete` da DLQ ("InnerMessageReceiver was closed", "Entity size became negative" no log dele) |
| F4 | `messaging-gcp` (Pub/Sub) | Contrato verde com o emulator | Concluída: contrato verde no emulator; envio sem `orderingKey` no tópico ordenado é aceito, como manda o ADR-0006 |
| F5 | Starter Spring Boot 4.x, métricas, `traceparent` e health check | Teste de auto-configuração por provedor | Código escrito; 8 testes verdes (auto-configuração e validação com AWS sem rede, regras de Azure e GCP, `traceparent` e métricas). Leitura da DLQ nativa do Service Bus pelo starter ainda não existe |
| F6 | `mvn install` da 0.1.0 | inbox-library usa a lib em um adapter de prova | README escrito; contratos da F2 a F4 verdes. O adapter de prova espera a inbox-library ter código (hoje só tem docs) |

Depois da v1, sem data: `messaging-oci` (Queue; mocks e contrato manual em fila
real), streaming pull no Pub/Sub atrás da mesma API, envio com atraso
(`deliverAfter`), OCI Notifications (ONS) como sender.

### Pendências do F0

| Pendência | Resultado | Onde impacta |
|---|---|---|
| Valor inicial do `deliveryCount` no Service Bus | 0 no SDK Java; adapter soma 1. Validar em teste | ADR-0004 |
| Pull síncrono do Pub/Sub com deadline curto e com ordering key | Timeout por chamada; `DeadlineExceededException` = vazio; ordem garantida | ADR-0002, ADR-0006 |
| Recursos dos emuladores | Pub/Sub: DLQ e ordering sim; exactly-once e IAM não. Service Bus: sessions e duplicate detection sim; 256 KB e TTL 1 h | Testes |
| `acceptNextSession` sem session | Bloqueia pelo `tryTimeout` e lança `IllegalStateException`; cliente de sessions com timeout próprio | ADR-0002, ADR-0006 |
| Testcontainers para o Service Bus emulator | `testcontainers-azure` 2.x, com MSSQL | Testes |
| SDKs com Spring Boot 4 | AWS sem Netty (`apache-client`); Azure aceita Netty 4.2/Reactor 3.8; gRPC shaded | ADR-0008 |
| Limites de custom properties do OCI Queue | Adiado para quando o OCI entrar | ADR-0004 |
| ONS | Adiado para depois da v1 | ADR-0003 |

Ainda abertas:

| Pendência | Opções |
|---|---|
| Licença do LocalStack 4.14.0 para uso comercial | Decidido fixar a 4.14.0 (2026-10-04). Confirmar com o jurídico; se não cobrir, licença paga (Base) ou ElasticMQ só para SQS |
| Publicação do `azure-messaging-servicebus` 7.18.0 | O changelog tem data de 2026-10-06; confirmar no Maven Central antes de fixar |
| Peek em entidade com sessions por receiver comum | Não coberto pelo contrato (o `checkAccess` testado é o do receiver sem sessions); validar no emulator |
| DLQ intermitente no Service Bus emulator | Bug do emulator, não do adapter; validar contra Service Bus real antes de tratar no teste |
| SQS 1 MiB no LocalStack escolhido | Validado na F2 (`acceptsMessageCloseToOneMebibyte`) |
