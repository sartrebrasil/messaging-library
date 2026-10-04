# ADR-0002: API síncrona, receive por pull com lease

- Status: aceito
- Data: 2026-10-04

## Contexto

Os quatro provedores entregam mensagens no mesmo modelo de base: o consumidor
recebe a mensagem, ela fica invisível para os outros por um tempo (lease) e o
consumidor confirma ou devolve. Se o lease vence sem confirmação, a mensagem
volta a ser entregue.

| Operação | SQS | Service Bus | Pub/Sub | OCI Queue |
|---|---|---|---|---|
| Receber | `ReceiveMessage` (long poll até 20 s) | `receiveMessages` em `PEEK_LOCK` | `pull` síncrono | `GetMessages` (long poll até 20 s) |
| Confirmar | `DeleteMessage` | `complete` | `acknowledge` | `DeleteMessage` |
| Devolver | `ChangeMessageVisibility` | `abandon` | `modifyAckDeadline(0)` | `UpdateMessage` |
| Estender lease | `ChangeMessageVisibility` | `renewMessageLock` | `modifyAckDeadline` | `UpdateMessage` |
| Lease definido por | Fila (pode ser sobrescrito no receive) | Entidade (lock duration, máx. 5 min) | Subscription (ack deadline) | Fila (pode ser sobrescrito no receive) |

O Pub/Sub e o Service Bus também têm clientes por callback (`Subscriber` com
streaming pull, `ServiceBusProcessorClient`), com controle de fluxo e renovação
automática de lease. Cada um tem seu modelo de threads, seu jeito de parar e
seu jeito de sinalizar erro.

Opções avaliadas:

| Opção | Prós | Contras |
|---|---|---|
| **Pull síncrono** (`receive` devolve uma lista) | Mínimo denominador comum; fácil de testar e de contratar; quem chama controla threads (virtual threads) | No Pub/Sub, pull síncrono rende menos que streaming pull; renovação de lease fica com quem chama |
| Callback (`subscribe(handler)`) | Usa o melhor cliente de cada provedor | Concorrência, parada e erros diferentes por provedor vazam; contrato de teste difícil; vira um framework de consumo (fora do escopo, ADR-0001) |
| Reativo | Backpressure natural | Fora do escopo; nenhum consumidor atual pede |

## Decisão

- API síncrona e bloqueante. Paralelismo é de quem chama, com virtual threads.
  Mesma escolha da storage-lib.
- `MessageSender` (fila e tópico) e `MessageReceiver` (fila e subscription) são
  interfaces separadas: um tópico não recebe e uma subscription não envia
  (ADR-0003).
- `receive(maxMessages, maxWait)` bloqueia até ter ao menos uma mensagem ou até
  `maxWait`; lista vazia não é erro. `maxMessages` acima do limite do provedor
  (10 no SQS, 20 no OCI) é reduzido ao limite, sem erro.
- O lease é o do destino. `ReceivedMessage.leaseExpiresAt` informa o vencimento
  calculado pelo adapter no momento do receive.
- O lease não é configurável por chamada de `receive`. Só o SQS e o OCI
  aceitariam; no Service Bus e no Pub/Sub ele é da entidade. Quem precisa de
  mais tempo usa `extendLease` ou `keepAlive`.
- Renovação de lease, manual ou por helper opt-in:

  ```java
  try (KeepAlive lease = receiver.keepAlive(message, Duration.ofMinutes(30))) {
      process(message);
      receiver.ack(message);
  }
  ```

  | Regra | Detalhe |
  |---|---|
  | Quando renova | Quando falta 1/3 da duração do lease, calculada no receive (`leaseExpiresAt - recebimento`). |
  | `maxTotal` | Obrigatório. Depois dele o helper para de renovar e o lease vence normalmente. Evita segurar uma mensagem para sempre se o processamento travar. |
  | Fim | `close()` para a renovação. `ack`, `nack` e `deadLetter` também param a renovação daquela mensagem. |
  | Falha | Se a renovação falha, o helper para e `lease.lost()` passa a ser `true`; `close()` não lança. O `ack` seguinte lança `LeaseExpiredException` normalmente. |
  | Threads | Um agendador por receiver, com virtual threads, criado só no primeiro `keepAlive` e encerrado no `close()` do receiver. |
  | Sessions | Renova o lock da session (ADR-0006). Vários `keepAlive` na mesma session não fazem mal. |

- Envio só síncrono: `send` e `sendAll`. Sem `sendAsync`. Paralelismo com
  virtual threads; volume com `sendAll`. No Pub/Sub, `send` espera o
  `publish().get()`; `sendAll` publica todas e espera as futures juntas, e com
  isso aproveita o batching do publisher.
- `ack`, `nack`, `extendLease`, `deadLetter` e `keepAlive` ficam no receiver,
  não na mensagem. `ReceivedMessage` é um record só com dados (mais o handle
  nativo opaco). O receiver lança `IllegalArgumentException` se a mensagem
  veio de outro receiver.

Convenções de ciclo de vida:

| Tema | Regra |
|---|---|
| Thread-safety | Senders e receivers são thread-safe. Exceção: no receiver com sessions, chamadas de `receive` na mesma instância são serializadas (ADR-0006). |
| Cliente nativo | Quem passa o cliente nativo é dono dele: o `close()` do adapter não fecha o cliente. O starter fecha os clientes que ele mesmo criou. |
| `close()` do receiver | Para os `keepAlive`, libera a session (se houver) e não confirma nada. Mensagens pendentes voltam quando o lease vence. |
| Uso depois do `close()` | `IllegalStateException`. |

Receive por provedor (achados do F0, [spike](../spikes/f0-sdks-e-emuladores.md)):

| Provedor | Como `maxWait` é respeitado | Observação |
|---|---|---|
| SQS | `WaitTimeSeconds = min(maxWait, 20 s)`; o read timeout do cliente HTTP precisa ser maior que isso | `maxWait` acima de 20 s vira 20 s |
| Service Bus sem sessions | `receiveMessages(max, maxWait)` | — |
| Service Bus com sessions | `acceptNextSession` não aceita timeout por chamada: usa o `tryTimeout` do cliente (padrão 60 s) e lança `IllegalStateException` quando não há session. O adapter cria o cliente de sessions com `tryTimeout` próprio (`session-accept-timeout`, padrão 20 s) e trata a exceção como lista vazia | Um `receive` sem session ativa pode bloquear até `session-accept-timeout`, mesmo com `maxWait` menor |
| Pub/Sub | `SubscriberStub` com pull síncrono e timeout por chamada (`GrpcCallContext.withTimeoutDuration(maxWait)`); `DeadlineExceededException` e resposta vazia viram lista vazia | O servidor pode devolver menos que `maxMessages` mesmo com backlog |

- No Pub/Sub, pull unário não garante baixa latência nem alto throughput (o
  Google recomenda streaming pull). Se o throughput medido no F4 não bastar, o
  adapter pode passar a ler de um buffer alimentado por streaming pull sem mudar
  a API. O cuidado é o lease já correr enquanto a mensagem está no buffer.
- `keepAlive` respeita os tetos de cada provedor: no SQS, o visibility timeout
  não passa de 12 h contadas do primeiro receive (atributo
  `ApproximateFirstReceiveTimestamp`); no Pub/Sub cada renovação é de no máximo
  600 s a partir de agora; no Service Bus a renovação estende pelo lock duration
  da entidade.

## Alternativas descartadas

| Alternativa | Por que não |
|---|---|
| `sendAsync` com `CompletableFuture` | Dobra a API e o contrato; virtual threads e `sendAll` cobrem os casos atuais |
| Lease por chamada (`ReceiveOptions.lease`) | Só dois dos quatro provedores suportam; nos outros seria emulado com `extendLease` |
| `message.ack()` | A mensagem deixaria de ser um record puro; testes passariam a precisar de receiver fake |
| Só `extendLease` manual | Todo consumidor com processamento longo reimplementaria a mesma renovação |

## Consequências

- Um contrato de teste único cobre os quatro provedores.
- A inbox-library (ou outro consumidor) escreve um único loop de polling sobre
  `MessageReceiver`.
- O desempenho do Pub/Sub fica abaixo do cliente nativo até a otimização com
  streaming pull. Medir no F4 antes de decidir.
