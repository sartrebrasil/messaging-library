# ADR-0008: Starter Spring Boot e observabilidade

- Status: aceito
- Data: 2026-10-04

## Contexto

O starter da storage-lib cria um único `ObjectStorage` a partir de
`storage.*`. Uma aplicação de mensageria normalmente fala com vários destinos
(filas de entrada, tópicos de saída, DLQs), às vezes de mais de um provedor
durante uma migração.

A storage-lib tem métricas (`ObjectStorageMetrics`) e health check no starter.
Mensageria precisa também de propagação de trace entre quem publica e quem
consome.

## Decisão

Configuração por destinos nomeados:

```yaml
messaging:
  providers:
    aws:
      type: aws              # aws | azure | gcp | oci
      region: sa-east-1
    azure:
      type: azure
      namespace: oobj-prd.servicebus.windows.net
  destinations:
    pedidos-entrada:
      provider: aws
      queue-url: https://sqs.sa-east-1.amazonaws.com/123/pedidos.fifo
      dead-letter: pedidos-dlq
      require: [ordered-delivery]
    pedidos-dlq:
      provider: aws
      queue-url: https://sqs.sa-east-1.amazonaws.com/123/pedidos-dlq.fifo
    eventos-saida:
      provider: azure
      topic: eventos
    eventos-faturamento:
      provider: azure
      topic: eventos
      subscription: faturamento
      sessions: true         # entidade com sessions no Terraform
```

- O starter cria um cliente nativo por provedor e expõe um bean
  `MessagingDestinations` com `sender(nome)` e `receiver(nome)`. Não cria um
  bean por destino: nomes vêm da configuração, não do código.
- O cliente nativo pode vir de um bean do usuário (`SqsClient`,
  `ServiceBusClientBuilder`, ...) quando existir. Credenciais seguem a cadeia
  padrão de cada SDK.
- Validação no startup: provedor inexistente, `dead-letter` apontando para
  destino inexistente, `require` com capability que o adapter não tem,
  `sessions: true` em destino que não é do Service Bus, `RESCHEDULE` junto com
  `sessions: true` (ADR-0006).
- `receiver(nome)` devolve uma instância nova a cada chamada, compartilhando o
  cliente nativo. Instâncias são baratas; no Service Bus com sessions, cada uma
  segura uma session (ADR-0006).
- Spring Boot 4.1.x e Java 21. Nenhuma dependência de Spring Cloud.

Dependências (achados do F0, [spike](../spikes/f0-sdks-e-emuladores.md)):

| Tema | Decisão |
|---|---|
| AWS | Cliente síncrono com `apache-client` (ou `url-connection-client`); `netty-nio-client` excluído. A API já é síncrona, e assim o SDK da AWS não roda sobre o Netty 4.2 que o Boot 4 força e que a AWS não testa. |
| Azure | Aceita o Netty 4.2 e o Reactor 3.8 do Boot 4: a Azure testa essa combinação na trilha do Spring Cloud Azure 7. Teste de fumaça no CI com o BOM do Boot. |
| GCP | gRPC usa Netty shaded; sem conflito. |
| BOMs | Na ordem: Boot primeiro, depois os BOMs dos SDKs. Rebaixamentos conhecidos (Gson 2.14 → 2.13, Jackson 2 da AWS 2.21.7 → 2.21.5) checados com `dependency:tree` na F5. |
| Jackson | A API pública não expõe tipos Jackson. O pacote `attributes` (ADR-0004) é JSON simples de strings, escrito sem Jackson no core. |
| Matriz testada | O README publica a matriz de versões testadas (Boot, BOM de cada SDK) em vez de fixar versões para quem usa. |

Health check por provedor, com a menor permissão possível:

| Destino | Operação | Permissão |
|---|---|---|
| SQS (sender e receiver) | `GetQueueAttributes` (`QueueArn`) | `sqs:GetQueueAttributes` |
| SNS (sender) | `GetTopicAttributes`; também lê `MaximumMessageSize` | `sns:GetTopicAttributes`, além de `sns:Publish` |
| Service Bus receiver | `peekMessage()`: sem lock e sem efeito | Listen |
| Service Bus sender | `createMessageBatch()`: abre o link e autentica sem enviar | Send |
| Pub/Sub subscription | `TestIamPermissions(pubsub.subscriptions.consume)` | Nenhuma extra (os papéis subscriber/publisher não leem a subscription nem o tópico) |
| Pub/Sub tópico | `TestIamPermissions(pubsub.topics.publish)` | Nenhuma extra |

Recurso inexistente no `TestIamPermissions` devolve conjunto vazio, que vira
`DestinationNotFoundException` ou `AccessDeniedException` (não dá para
distinguir). O emulator do Pub/Sub não implementa IAM, então o health check do
Pub/Sub só é testado com mocks.

Observabilidade, como decorators no starter (o core continua sem
dependências):

- Métricas Micrometer: `messaging_send_duration_seconds`, `messaging_receive_duration_seconds`, `messaging_ack_duration_seconds`,
  `messaging_nack_duration_seconds`, `messaging_dead_letter_duration_seconds` (timers), com tags `batch` (só em `send` e `ack`: `true` para `sendAll` e `ackAll`),
  `provider`, `destination`, `exception` (simples da classe lançada, ou `none`), `outcome` (`success`, `error` e `partial`, num lote em que só parte falhou).
  `messaging_receive_messages`
  (distribution summary) mede o tamanho dos lotes. `messaging_messages_failed_total` conta uma vez cada mensagem cuja
  chamada falhou (`operation` = `send`, `ack`, `nack`, `dead_letter`; item de lote que falhou conta, e chamada que
  lança conta todas as mensagens dela), com `provider`, `destination`, `operation` e `exception`.
  `messaging_message_size_bytes` mede o corpo de cada mensagem enviada.
- Tracing com Micrometer Observation: o sender abre uma observation com
  `SenderContext<Map<String,String>>` sobre os atributos, e os handlers de
  tracing do Boot injetam o `traceparent` (W3C) como atributo reservado. O receiver expõe o contexto extraído em
  `ReceivedMessage`. Abrir o span de processamento é de quem consome, porque a
  lib não sabe quando o processamento termina. Revisão de 2026-11-13: com o `DefaultMeterObservationHandler`, a
  observation `messaging.publish` gera também `messaging_publish_seconds` e `messaging_publish_active_seconds`, com
  as tags `provider` e `destination` dos meters acima (mais o `error` do Micrometer, que os meters explícitos chamam
  de `exception`). Os nomes do OpenTelemetry (`messaging.system`, `messaging.destination.name`) ficam só no span.
- Health check: `checkAccess()` por destino, como na storage-lib, ligado só
  quando `management.health.messaging.enabled=true`. Receber mensagem não pode
  fazer parte do health check, porque consumiria mensagens.

## Consequências

- Migração entre provedores é troca de `provider` e de endereço no YAML.
- O health check do SNS pede uma permissão a mais (`sns:GetTopicAttributes`)
  do que só publicar. Sem ela, o check falha com `AccessDeniedException`; o
  README documenta a policy mínima por provedor.
- A combinação Azure SDK + Netty 4.2 + Reactor 3.8 não é a base oficial do SDK
  avulso, só da trilha Spring. Se aparecer problema, a saída é fixar Netty e
  Reactor da Azure no módulo de teste e reportar.

## Revisão (2026-11-06): um starter só

- `messaging-inbox-spring-boot-starter` foi incorporado ao `messaging-spring-boot-starter`.
  A autoconfiguração `inbox.*` liga com `messaging-inbox` e `jackson-databind` (Jackson 3) no
  classpath; as duas dependências são `optional` no starter, e a aplicação que consome declara
  as duas.
- A ligação com cada provedor fica no starter, em `com.example.messaging.spring.<provedor>`
  (`aws`, `azure`, `gcp`, `jms`, `rabbitmq`), uma `ProviderConnection` por tipo. Os adapters
  continuam sem Spring: o starter depende deles como `optional` e só cria a conexão de um tipo
  cujo adapter está no classpath.
- Tentamos o contrário (cada adapter com a sua fábrica, descoberta por `ServiceLoader`) e
  voltamos atrás: amarrava os adapters ao starter e ao Spring.

## Revisão (2026-11-06): endereço em quem usa

`messaging.destinations` foi removido. Senders ficam em `messaging.senders.<nome>` e receivers em
`messaging.receivers.<nome>` (consumidos pela messaging-inbox), cada um com o próprio endereço:
`provider` (opcional com um provider só), `destination`, `subscription` (receber de tópico),
`kind: topic` (enviar a tópico) e `options` (`require`, `sessions`, `redelivery`, `lease`, ...).
`MessagingDestinations` expõe `sender(nome)` para os senders configurados e `sender`/`receiver` por
endereço para a inbox. No AWS, `destination` aceita o nome da fila, resolvido com `GetQueueUrl`.
Cada raiz faz binding estrito, e chaves removidas derrubam o startup. Detalhes na ADR-0008 da
messaging-inbox (revisão de 2026-11-06).
