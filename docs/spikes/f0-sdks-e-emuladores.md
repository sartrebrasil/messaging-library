# F0: SDKs e emuladores

- Data: 2026-10-04
- Método: documentação oficial, código-fonte dos SDKs e POMs do Maven Central.
  "Confirmado" = lido em doc ou código; "inferido" = deduzido dessas fontes.
  Nenhum teste foi executado ainda; os itens marcados "validar em teste" entram
  nos contratos da F1–F4.

## Versões de referência

| Artefato | Versão |
|---|---|
| Spring Boot | 4.1.1 (Framework 7.0.9, Micrometer 1.17.1, micrometer-tracing 1.7.1) |
| AWS SDK v2 BOM | 2.55.11 |
| `azure-messaging-servicebus` | 7.18.0 (changelog datado de 2026-10-06; confirmar publicação antes de fixar) |
| `azure-sdk-bom` | 1.3.8 |
| `libraries-bom` (Google) | 26.90.0, com `google-cloud-pubsub` 1.157.0 |
| Testcontainers | 2.0.5 (módulos renomeados para `testcontainers-*`) |

## AWS (SQS e SNS)

| Tema | Achado | Confiança |
|---|---|---|
| Tamanho SQS | 1 MiB, **incluindo atributos** (nome, tipo e valor). `SendMessageBatch`: 1 MiB no total e 10 entradas; falha parcial vem em `Failed` com HTTP 200 | Confirmado |
| Corpo SQS | Mínimo 1 byte | Confirmado |
| Nomes de atributo SQS | `A-Z a-z 0-9 _ - .`, até 256, sem prefixo `AWS.`/`Amazon.` | Confirmado |
| Visibility | Máx. 12 h **contadas do primeiro receive**; estender não zera. Pedir além do restante gera erro | Confirmado |
| Tamanho SNS | Padrão 256 KiB; atributo de tópico `MaximumMessageSize` vai até 1 MiB (acima de 256 KiB: até 100 subscriptions, só SQS/Lambda/Firehose) | Confirmado |
| SNS → SQS raw | Atributos viram atributos SQS, **máx. 10**; acima disso a mensagem é **descartada** na entrega | Confirmado |
| `MessageGroupId` em fila standard | Aceito (fair queues): só identifica tenant, sem ordem e sem dedup | Confirmado |
| Lease perdido | `ReceiptHandleIsInvalid`, `MessageNotInflight`, ou `InvalidParameterValue` com "receipt handle has expired" | Confirmado / inferido (o último) |
| `DeleteMessage` com handle antigo | **Pode responder sucesso sem apagar** | Confirmado |
| Health check | `GetQueueAttributes` (`sqs:GetQueueAttributes`); SNS `GetTopicAttributes` (`sns:GetTopicAttributes`, permissão extra além de `sns:Publish`; devolve `MaximumMessageSize` se configurado) | Confirmado |
| LocalStack | Desde 2026-03-23 a imagem `latest` **exige `LOCALSTACK_AUTH_TOKEN`**; plano gratuito proíbe uso comercial (Base: US$ 39/mês por seat). Tags até `4.14.0` rodam sem token, mas congeladas | Confirmado |
| LocalStack: recursos | SQS completo (FIFO, DLQ, visibility); SNS com raw delivery e FIFO. Não confiar na aplicação dos limites (10 atributos, `MaximumMessageSize`) | Confirmado / inferido |
| Testcontainers | `org.testcontainers:testcontainers-localstack:2.0.5`, classe `org.testcontainers.localstack.LocalStackContainer` | Confirmado |

## Azure (Service Bus)

| Tema | Achado | Confiança |
|---|---|---|
| `deliveryCount` | **0 na primeira entrega no SDK Java** (o .NET soma 1). Incrementa em `abandon` e em lock vencido; não incrementa em lock perdido por queda de conexão nem ao fechar session com mensagens pendentes | Inferido do código (alta) / confirmado |
| `acceptNextSession` sem session | Bloqueia por cerca do `tryTimeout` (padrão 60 s) e lança `IllegalStateException`; o timeout só é configurável pelo `AmqpRetryOptions` do builder (vale para todas as operações daquele cliente) | Confirmado (código) |
| Custo de aceitar/liberar session | Um attach/detach de link AMQP (dezenas de ms); o custo real é o bloqueio quando não há session | Inferido |
| Renovação | `renewSessionLock()` no cliente síncrono devolvido pelo `acceptNextSession` | Confirmado |
| `abandon` em session | A mensagem volta ao início da session; acima de `MaxDeliveryCount` vai para a DLQ e a session segue | Confirmado |
| Lock da session | Regido pelo `LockDuration` da entidade (padrão 1 min, máx. 5 min) | Inferido |
| Lease perdido | `ServiceBusException` com `MESSAGE_LOCK_LOST` ou `SESSION_LOCK_LOST`; lock pode cair antes do vencimento (queda de conexão, update do serviço) | Confirmado |
| Tamanho | Standard 256 KB; Premium 1 MB por padrão, configurável até 100 MB; lote 1 MB | Confirmado |
| Propriedades | Sem limite de quantidade; 32 KB por propriedade e **64 KB no header inteiro** | Confirmado |
| `MessageId`, `SessionId` | Até 128 caracteres | Confirmado |
| `scheduleMessage` | No cliente síncrono; sem horizonte máximo documentado | Confirmado |
| Health check | Receiver: `peekMessage()` (Listen, sem lock nem efeito); sender: `createMessageBatch()` (abre o link e autentica sem enviar). Peek em entidade com sessions: validar em teste | Confirmado / inferido |
| Emulator 2.0.0 | Sessions, duplicate detection, DLQ por `MaxDeliveryCount`, tópicos com filtros: sim. **256 KB fixo, TTL máx. 1 h**, 50 entidades, config só no startup, exige SQL sidecar | Confirmado |
| Testcontainers | `org.testcontainers:testcontainers-azure:2.0.5`, `ServiceBusEmulatorContainer` + `MSSQLServerContainer` na mesma `Network`, `.acceptLicense()`, `.withConfig(...)` | Confirmado |

## GCP (Pub/Sub)

| Tema | Achado | Confiança |
|---|---|---|
| Pull síncrono e `maxWait` | Timeout por chamada (`GrpcCallContext.withTimeoutDuration`). Ao estourar: resposta vazia ou `DeadlineExceededException`; tratar os dois como lista vazia. O padrão do stub é 60 s | Confirmado / inferido |
| Mensagens por pull | Até 1000 e 10 MB; o servidor pode devolver menos mesmo com backlog | Confirmado |
| Throughput | Pull unário não garante baixa latência nem alto throughput; o Google recomenda StreamingPull | Confirmado |
| Ack deadline | Subscription: 10–600 s (padrão 10 s). `modifyAckDeadline`: 0–600 s **a partir de agora**; 0 = nack | Confirmado |
| `nack` com atraso | Não existe nativo: `modifyAckDeadline(delay)` e deixar vencer; conta como tentativa de entrega | Inferido |
| Ordem no pull síncrono | Garantida por chave; uma só leva por chave em aberto. `nack`/vencimento reentrega a mensagem e as seguintes da chave | Confirmado |
| Publisher com ordem | Exige `setEnableMessageOrdering(true)`; sem isso, publicar com chave lança `IllegalStateException`. Falha de uma chave pausa a chave até `resumePublish` | Confirmado |
| `deliveryAttempt` | 0 sem dead letter policy; 1 na primeira entrega com policy | Confirmado |
| Exactly-once | Ack com lease vencido: `INVALID_ARGUMENT` com `ErrorInfo` por ack id (`PERMANENT_FAILURE_INVALID_ACK_ID`) | Confirmado |
| Limites | 10 MB de dados; 100 atributos (chave 256 B, valor 1024 B, sem prefixo `goog`); **mensagem vazia (sem dados e sem atributos) é rejeitada**; publish: 1000 mensagens e 10 MB; ack: 512 KB por requisição | Confirmado |
| Batching | `publish()` N vezes e esperar as futures agrupa; padrão Java: 100 mensagens, **1000 bytes**, 1 ms | Confirmado |
| Health check | Papéis subscriber/publisher **não** têm `get` de subscription/tópico. Usar `TestIamPermissions` (`pubsub.subscriptions.consume` / `pubsub.topics.publish`); recurso inexistente devolve conjunto vazio | Confirmado |
| Emulator | DLQ, ordering, retry policy e filtros: sim. IAM: não (health check não testável nele). Exactly-once: não documentado; testar só em GCP real | Confirmado / inferido |
| Testcontainers | `org.testcontainers:testcontainers-gcloud:2.0.5`, `org.testcontainers.gcloud.PubSubEmulatorContainer`, imagem `gcr.io/google.com/cloudsdktool/google-cloud-cli:<versão>-emulators` (fixar versão) | Confirmado |

## Spring Boot 4 no mesmo classpath

| Tema | Achado | Confiança |
|---|---|---|
| Netty | Boot gerencia 4.2.17; AWS e Azure são compilados contra 4.1.138. A Azure testa a combinação (trilha `springboot4_`); a AWS não | Confirmado / inferido |
| Reactor | Boot gerencia 3.8.7 / reactor-netty 1.3.7; Azure usa 3.7 / 1.2 (mesma trilha de testes) | Confirmado |
| gRPC (Pub/Sub) | `grpc-netty-shaded`: não é afetado pelo Netty do Boot | Confirmado |
| Jackson | 2 (`com.fasterxml`) e 3 (`tools.jackson`) convivem; Boot gerencia os dois | Confirmado |
| Rebaixamentos pelo BOM do Boot | Gson 2.14.0 → 2.13.2; Jackson 2 da AWS 2.21.7 → 2.21.5; OpenTelemetry 1.65 → 1.62 | Confirmado (impacto inferido baixo) |
| Tracing | `SenderContext`/`ReceiverContext` com `Propagator.Setter/Getter` sobre `Map<String,String>`; os handlers do Boot injetam e extraem `traceparent` | Confirmado |
| Spring Cloud de cada nuvem | Spring Cloud Azure 7.4.0 e spring-cloud-gcp 8.2.1 usam Boot 4.1; fixam BOMs de SDK mais antigos | Confirmado |

## Fontes principais

- SQS: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
- SQS visibility: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- SQS DeleteMessage: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
- SQS fair queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues.html
- SNS large payloads: https://docs.aws.amazon.com/sns/latest/dg/large-message-payloads.html
- SNS raw delivery: https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html
- LocalStack: https://blog.localstack.cloud/the-road-ahead-for-localstack/ e https://www.localstack.cloud/pricing
- Service Bus locks: https://learn.microsoft.com/azure/service-bus-messaging/message-transfers-locks-settlement
- Service Bus sessions: https://learn.microsoft.com/azure/service-bus-messaging/message-sessions
- Service Bus quotas: https://learn.microsoft.com/azure/service-bus-messaging/service-bus-quotas
- Service Bus emulator: https://learn.microsoft.com/azure/service-bus-messaging/overview-emulator
- Testcontainers Azure: https://java.testcontainers.org/modules/azure/
- Pub/Sub pull: https://docs.cloud.google.com/pubsub/docs/pull
- Pub/Sub ordering: https://docs.cloud.google.com/pubsub/docs/ordering
- Pub/Sub quotas: https://docs.cloud.google.com/pubsub/quotas
- Pub/Sub exactly-once: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery
- Pub/Sub emulator: https://docs.cloud.google.com/pubsub/docs/emulator
- Pub/Sub access control: https://docs.cloud.google.com/pubsub/docs/access-control
- Spring Boot 4.1.1 BOM: https://repo1.maven.org/maven2/org/springframework/boot/spring-boot-dependencies/4.1.1/spring-boot-dependencies-4.1.1.pom
