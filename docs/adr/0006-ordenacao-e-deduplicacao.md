# ADR-0006: Ordenação e deduplicação

- Status: aceito
- Data: 2026-10-04

## Contexto

A outbox-library publica em ordem por aggregate (`MessageGroupId` no SQS
FIFO). Cada provedor oferece ordem e deduplicação de um jeito, e várias delas
dependem de configuração do destino que a lib não enxerga:

| | Ordem por chave | Condição | Deduplicação no envio |
|---|---|---|---|
| SQS | `MessageGroupId` | Fila FIFO (`.fifo`); obrigatório nela | `MessageDeduplicationId`, janela de 5 min, só FIFO |
| SNS | `MessageGroupId` | Tópico FIFO, entregando em fila FIFO | `MessageDeduplicationId`, só FIFO |
| Service Bus | `SessionId` | Entidade com sessions; envio exige `SessionId` e receive exige session receiver | `MessageId` + duplicate detection na entidade |
| Pub/Sub | `orderingKey` | Publisher e subscription com ordering habilitado | Não tem no envio (exactly-once é do lado do ack) |
| OCI Queue | Não tem | Channels separam fluxos, mas não garantem ordem | Não tem |

## Decisão

- `OutgoingMessage.orderingKey` e `deduplicationId` são opcionais e sempre
  aceitos. O adapter mapeia onde o provedor suporta e ignora onde não suporta:

  | | `orderingKey` | `deduplicationId` |
  |---|---|---|
  | SQS/SNS FIFO | `MessageGroupId` | `MessageDeduplicationId` |
  | SQS/SNS standard | `MessageGroupId` (fair queues, sem ordem) | Ignorado |
  | Service Bus | `SessionId` | `MessageId` |
  | Pub/Sub | `orderingKey` (publisher criado com ordering) | Ignorado |
  | OCI | `channelId` | Ignorado |

- SQS e SNS FIFO: envio sem `orderingKey` lança `IllegalArgumentException`,
  porque o provedor exige. FIFO é detectado pelo sufixo `.fifo`.
- Pub/Sub: o publisher é sempre criado com `setEnableMessageOrdering(true)`,
  porque sem isso publicar com chave lança `IllegalStateException`. Mensagem sem
  chave não é afetada. Quando um envio com `orderingKey` falha, o publisher
  pausa a chave; o adapter chama `resumePublish` e relança o erro. A próxima
  tentativa decide quem chama. Para a ordem valer, todo envio da chave precisa
  sair da mesma região (endpoint regional fixo no adapter).
- SQS/SNS standard: `MessageGroupId` em fila standard ativa fair queues, que só
  equilibram tenants, sem ordem e sem dedup. No SNS standard ele só chega a
  subscriptions SQS standard.
- As garantias ficam em `Capabilities` (`orderedDelivery`,
  `publisherDeduplication`), com o valor que o provedor suporta. A configuração
  real do destino (FIFO, sessions, ordering na subscription, duplicate detection)
  fica com o Terraform. O README diz isso em uma tabela, e o starter permite
  declarar `require: [ordered-delivery]` para falhar no startup se o adapter não
  suportar (ADR-0008).
- A lib garante só a ordem de entrega do provedor. Processar em ordem
  (concorrência, ack em sequência) é de quem consome.

### Service Bus com sessions (v1)

Entidade com sessions habilitadas só aceita envio com `SessionId` e só pode ser
consumida por um session receiver. O adapter recebe `sessions: true` para
sender e receiver dessa entidade; o valor precisa bater com a configuração da
entidade no Terraform.

| Tema | Regra |
|---|---|
| Envio | `orderingKey` obrigatório (`IllegalArgumentException` sem ele), como no SQS FIFO. |
| Receive | Sem session ativa, o receiver chama `acceptNextSession` e passa a receber dessa session. O SDK não aceita timeout por chamada: o adapter usa um cliente com `tryTimeout` próprio (`session-accept-timeout`, padrão 20 s) e trata o `IllegalStateException` de "nenhuma session" como lista vazia (ADR-0002). Quando um `receive` volta vazio, o receiver libera a session e o próximo `receive` aceita outra. Aceitar e liberar custa um attach/detach de link AMQP (dezenas de ms). |
| Uma session por instância | Cada `MessageReceiver` segura no máximo uma session. Chamadas concorrentes de `receive` na mesma instância são serializadas. Paralelismo entre sessions = mais instâncias (o starter cria uma nova a cada `receiver(nome)`, compartilhando o cliente nativo). |
| `orderingKey` recebido | É o `SessionId`. |
| Lease | É o lock da session, não da mensagem, regido pelo `LockDuration` da entidade (padrão 1 min, máx. 5 min). `leaseExpiresAt` informa o vencimento do lock da session; `extendLease` chama `renewSessionLock`, que cobre todas as mensagens recebidas dela. |
| Lock perdido | `ack`/`nack`/`extendLease` lançam `LeaseExpiredException` e o receiver descarta a session; o próximo `receive` aceita outra. |
| `nack` | `abandon`: a mensagem volta para o início da session e é reentregue na hora para a mesma instância. A ordem se mantém. |
| `RESCHEDULE` | Proibido com sessions (`IllegalStateException` na construção): a cópia agendada iria para o fim da session e quebraria a ordem. Com sessions, `delayedRedelivery` é `false`. |
| `close()` | Libera a session; mensagens não confirmadas voltam a ser entregues, e fechar sem confirmar **não** incrementa o `deliveryCount`. Confirmações depois do `close()` falham. |
| Session state | Fora do escopo (`setSessionState`/`getSessionState` não são expostos). |

Comparação com os outros provedores na mesma situação:

| | Reentrega com ordem | Atraso entre tentativas mantendo a ordem |
|---|---|---|
| SQS FIFO | Sim: o grupo fica bloqueado enquanto a mensagem está invisível | Sim (`nack` com delay) |
| Pub/Sub com ordering | Sim: o `nack` reentrega a mensagem e as seguintes da chave | Sim, até 600 s |
| Service Bus com sessions | Sim: `abandon` volta para o início da session | Não: quem consome espera antes de chamar `nack`, segurando o lock (e renovando com `extendLease`) |

## Consequências

- A mesma mensagem com `orderingKey` sai ordenada em SQS FIFO e Pub/Sub, e sem
  ordem no OCI, sem mudar código. Quem depende de ordem declara isso no
  `require`.
- Com sessions na v1, a ordem por aggregate que a outbox publica tem
  equivalente nos três provedores da v1.
- No Service Bus com sessions, backoff entre tentativas é responsabilidade de
  quem consome: esperar com o lock seguro e só então chamar `nack`. A
  inbox-library precisa tratar esse caso pela capability `delayedRedelivery`.
- O paralelismo no Service Bus com sessions é limitado pelo número de
  instâncias de receiver, não por `maxMessages`.
