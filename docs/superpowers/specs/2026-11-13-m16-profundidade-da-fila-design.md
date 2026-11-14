# M16: profundidade da fila e da DLQ (SQS e RabbitMQ)

- Data: 2026-11-13
- Estado: **desenho, aguardando aprovação** (revisado depois da leitura de um revisor; uma pergunta ao dono em aberto, [P1](#pendência-do-dono))
- Origem: item M16 de `docs/metricas-backlog.md`
- Escopo desta rodada: **SQS e RabbitMQ**. Service Bus, Pub/Sub e JMS ficam para depois (veja [Fora do escopo](#fora-do-escopo))

## Intenção

Hoje as métricas mostram o **fluxo** (`inbox_messages_dead_lettered_total`, `inbox_messages_failed_total`: quantas
mensagens entraram), mas não o **estoque**: quantas mensagens estão paradas na fila principal, na DLQ e na `failed`
agora. Sem o estoque não dá para alertar "a `failed` tem mensagens esperando alguém" nem ver uma DLQ que parou de
esvaziar. O pedido original do backlog era o alerta "DLQ crescendo", o mais pedido de quem opera filas.

O dono pediu SQS e RabbitMQ primeiro, e a abordagem A (leitura periódica no starter, método novo no receiver).

## Objetivos

1. Um gauge `messaging.queue.depth{provider,destination,state}` com a profundidade das filas que a inbox usa: a
   principal, a DLQ, a `failed` e as filas de retry.
2. A leitura funciona em **SQS e RabbitMQ**, e um broker novo entra implementando um único método.
3. O `destination` é o mesmo nome que `inbox_*` e `messaging_*` já usam, então as três famílias se cruzam.
4. **Desligado por padrão**: cada leitura é uma chamada ao broker, e não pode virar custo ou carga sem escolha.

## Não-objetivos

- Service Bus, Pub/Sub e JMS (Classic e Artemis). Eles devolvem "não suportado" e ficam sem série.
- Alertas prontos ou dashboard: a rodada entrega a métrica e a documentação.
- Profundidade de filas que a aplicação não passou pela inbox (por exemplo, um `messaging.senders.<x>`). A API pública
  `watchDepth` permite registrar, mas **nenhuma configuração** liga isso nesta rodada.
- Medir idade da mensagem mais antiga (`ApproximateAgeOfOldestMessage` do SQS). É um bom passo seguinte.

## Anti-objetivos (o que tornaria isto um fracasso mesmo que "funcione")

- A leitura **atrapalhar o consumo**: lenta, travando um worker, ou derrubando o startup. Ela roda numa thread própria,
  com receivers próprios, e qualquer falha vira métrica, nunca exceção para quem consome.
- **Esconder** uma fila cheia: erro de leitura nunca vira 0. Vira `NaN`.
- Tag de cardinalidade sem limite: todas vêm de configuração (`provider`, `destination`) ou de um conjunto fixo (`state`).

## Restrições encontradas no código

| Fato | Onde | Consequência |
|---|---|---|
| O receiver do SQS já exige `GetQueueAttributes` (lê o visibility timeout, e o `checkAccess` também chama), e o do RabbitMQ já faz `queueDeclarePassive` | `SqsMessageReceiver:233,256`, `RabbitMessageReceiver:243` | Para as filas que a aplicação **lê** (a principal, a DLQ com o nível 3 ligado, a `failed` com redrive), a profundidade não pede permissão nova |
| Quem só **envia** ao SQS precisa de `sqs:SendMessage` e nada mais | `README.md:304` | Para as filas só de envio (retry, `failed` sem redrive, DLQ sem nível 3), a profundidade **exige permissão nova**. É a pendência [P1](#pendência-do-dono) |
| `Capabilities` é um record de 8 componentes posicionais, criado em cerca de 10 lugares | `Capabilities.java` | Um flag novo nele quebraria todos. O suporte é descoberto por `UnsupportedOperationException`, como o `deadLetter` |
| O starter não tem agendador; as únicas threads são as dos workers, no core | `InboxLifecycle:115` | A leitura periódica traz uma thread nova |
| Receivers são criados **por chamada**; um consumer com `concurrency` N tem N receivers do mesmo nome | `MessagingDestinations.receiver` | Ler pelo receiver de trabalho repetiria a chamada N vezes. A leitura usa um receiver **próprio por nome** |
| `MessagingDestinations` já guarda um receiver "de lado" por nome para o health check | `healthReceivers`, `checkAccess(name)` | É o molde para os receivers da leitura |
| O receiver da DLQ só nasce na primeira passada e o da `failed` só num redrive | `ConsumerBuilder:139-141`, `routes()` | A leitura não pode depender deles: a `failed` ficaria sem métrica até um redrive |
| As chaves de `messaging.*` são validadas em modo estrito; chave desconhecida derruba o startup | `MessagingProperties.ROOTS` | `metrics` precisa entrar na lista |
| Receivers são embrulhados duas vezes (`Forgetting` e `MeteredMessageReceiver`), e o `InboxDestinations` é uma classe anônima mais um wrapper `lazy` | `MessagingDestinations`, `InboxAutoConfiguration:56,108` | Todos precisam repassar o método novo |
| SQS conta de forma aproximada e com atraso (cerca de 1 minuto); RabbitMQ não informa as não confirmadas no `queueDeclarePassive` | documentação dos brokers | O gauge é uma **estimativa**; o SQS tem 3 estados, o RabbitMQ só 1 |

## Abordagens

**A. Capacidade no receiver, leitura periódica no starter. Escolhida.** `MessageReceiver.depth()` com implementação
padrão que lança `UnsupportedOperationException`; uma thread do starter lê a cada N segundos e guarda o valor; o gauge
lê o cache. Reaproveita o nome `destination` do transporte, funciona para qualquer app com receivers e um broker novo
é um método.

**B. Gauge que consulta o broker a cada scrape.** Sem thread nem cache. Descartada: um SQS lento atrasa o scrape inteiro
e cada scrape de cada instância vira uma chamada ao broker.

**C. Só dentro da inbox**, com `inbox.queue.depth{consumer,queue}`. Descartada: o método no core é necessário do mesmo
jeito, e a profundidade ficaria restrita a quem usa a inbox, sem o cruzamento com `messaging_*`.

Escolha do dono: **A**.

## Decisões

Do dono: escopo SQS e RabbitMQ; abordagem A; desligado por padrão; intervalo de 30 s; erro de leitura vira `NaN`; o
SQS é aproximado e isso será documentado.

Do desenho (minhas escolhas, para você conferir):

| # | Escolha | Por quê |
|---|---|---|
| D1 | Propriedades `messaging.metrics.queue-depth.enabled` (padrão `false`) e `.interval` (padrão `30s`, mínimo `5s`) | Duas chaves explícitas; o mínimo evita um intervalo que vira custo no SQS sem querer |
| D2 | Filas de retry (`<receiver>.retry.<i>`) entram junto com a principal, a DLQ e a `failed` | São filas reais com mensagens esperando; o custo é a mesma chamada |
| D3 | Contador `messaging.depth.reads.failed{provider,destination,exception}` | O `NaN` mostra que algo falhou; o contador mostra por quê e quantas vezes |
| D4 | O gauge de um estado só é registrado quando o broker informa o estado | RabbitMQ não tem `in_flight`: uma série ausente é honesta, uma série 0 não é |
| D5 | Broker sem suporte: avisa uma vez no log, para de ler aquele destino e **não** registra série | Evita ruído e evita um gauge que parece "fila vazia" |
| D6 | Mais de uma instância da aplicação lê a mesma fila e cada uma publica a sua série | Documentar: agregar com `max by (destination)`, não `sum` |
| D7 | `visible` é registrado como `NaN` já no `watchDepth`, antes da primeira leitura; `in_flight` e `delayed` só depois que o broker os informa | Uma fila ilegível (sem permissão, por exemplo) precisa aparecer como `NaN` e não como "recurso desligado". Sem isso o pior caso seria invisível |
| D8 | Alvos que apontam para o **mesmo endereço** são lidos uma vez por rodada e o valor vai para o gauge de cada nome | `failed` e DLQ na mesma fila, ou várias bandas de retry numa fila só, não multiplicam as chamadas, e cada `destination` continua com a sua série para o cruzamento |
| D9 | `watchDepth` é chamado no **fim** de um `build` bem-sucedido, e não há `unwatch`: o alvo vale até o `close()` do `MessagingDestinations` | A profundidade é da fila, não do consumer: um consumer parado com a fila crescendo é justamente quando a métrica importa. Um `build` que falha não registra nada |
| D10 | O primeiro alvo de cada **provider** serve de sonda: se o `depth()` dele não é suportado, o provider inteiro é marcado e os outros alvos dele nem criam receiver. O receiver da sonda é fechado | Evita criar clientes de Service Bus, Pub/Sub ou JMS só para descobrir que não há métrica, sem uma lista fixa de tipos que envelheceria |
| D11 | Erro de acesso negado é um erro de leitura como outro qualquer: não há estado terminal. Cada falha soma em `messaging.depth.reads.failed`, o log é um WARNING por sequência de falhas e a série continua `NaN` | Um estado terminal pararia o contador, e o alerta em `increase(...) > 0` deixaria de disparar numa permissão faltando para sempre. `NaN` persistente é detectável por `messaging_queue_depth != messaging_queue_depth` |
| D12 | A tag `provider` é o **tipo resolvido** (`MessagingProperties.Type.tag()`: `aws`, `rabbitmq`), nunca o nome do provider na configuração | É o valor dos `messaging_*`; um nome de configuração quebraria o cruzamento em silêncio. `watchDepth` recebe o nome (ou `null` para o único provider) e resolve o tipo |

## Desenho

### 1. Core (`messaging-core`)

```java
public record QueueDepth(long visible, OptionalLong inFlight, OptionalLong delayed) { }
```

- `visible`: mensagens que um `receive` pegaria agora.
- `inFlight`: entregues e ainda sem `ack` (SQS: `ApproximateNumberOfMessagesNotVisible`). Vazio quando o broker não informa.
- `delayed`: ainda não disponíveis por atraso (SQS: `ApproximateNumberOfMessagesDelayed`). Vazio quando não informa.
- O construtor rejeita valores negativos. Todos os valores são **aproximados**.

`MessageReceiver` ganha `default QueueDepth depth() { throw new UnsupportedOperationException(...); }`, com a mesma
forma do `deadLetter`. `Capabilities` **não muda**.

### 2. SQS (`messaging-aws`)

`SqsMessageReceiver.depth()` chama `getQueueAttributes` com os três `ApproximateNumberOfMessages*` e converte as strings
em `long`. Erros passam por `AwsErrors.map("GetQueueAttributes " + queueUrl, e)`. Se faltar o atributo de `visible`,
lança `MessagingException` (resposta inesperada); se faltar `NotVisible` ou `Delayed`, o estado fica vazio. Funciona em
filas padrão e FIFO. Permissão: `sqs:GetQueueAttributes`, a mesma do `checkAccess`.

### 3. RabbitMQ (`messaging-rabbitmq`)

`RabbitMessageReceiver.depth()` abre um channel descartável, faz `queueDeclarePassive(queue)` e devolve
`getMessageCount()` como `visible`, sem `inFlight` nem `delayed`. É o padrão do `checkAccess`; um channel descartável é
necessário porque o `queueDeclarePassive` numa fila inexistente fecha o channel. Erros passam por `RabbitErrors.map`.

### 4. Testkit (`messaging-testkit`)

O `InMemoryMessaging` implementa `depth()` a partir do estado das entradas (visíveis agora, com lease, adiadas).
O `MessagingContract` ganha um teste de profundidade, ligado por `protected boolean reportsDepth()` (padrão `false`;
SQS, RabbitMQ e a memória devolvem `true`). O teste envia 3, confere `visible == 3`, recebe 1 e confere `visible == 2`
(com `inFlight == 1` onde o broker informa). Como o SQS é eventualmente consistente, o teste espera até um prazo em vez
de comparar na primeira leitura.

### 5. Starter, transporte (`com.example.messaging.spring`)

- `messaging.metrics.queue-depth.{enabled,interval}` é ligado por uma classe própria (`QueueDepthSettings`), com
  binding estrito, sem mexer no record `MessagingProperties`. `metrics` entra em `ROOTS` e na mensagem de erro de
  chave desconhecida. O intervalo abaixo de 5 s derruba o startup com uma mensagem clara.
- `MessagingDestinations` ganha `watchDepth(String name, String provider, Endpoint endpoint)`: resolve o endereço como
  receiver (`resolveOrThrow`), guarda em um mapa `depthTargets` (nome → endereço) e, na primeira chamada, inicia o
  poller. O mesmo nome com outro endereço lança `IllegalStateException`, como o `sender`. Não faz nada quando a leitura
  está desligada ou não há `MeterRegistry`. Registra a série `visible` como `NaN` na hora (D7). O mapa é **separado** do `receivers`, para não pôr a DLQ e a `failed` no
  health check.
- O poller (`QueueDepthPoller`) é um `ScheduledExecutorService` de uma thread de plataforma, nomeada
  `messaging-queue-depth`, com atraso fixo entre rodadas. A cada rodada lê cada alvo, em sequência, por um receiver
  próprio do alvo (`newReceiver(name, r, null)`, criado na primeira leitura e fechado no `close()`):
  - **sucesso**: atualiza o cache; `visible` já existe desde o `watchDepth` (D7), e `in_flight` e `delayed` são
    registrados na primeira vez que vêm;
  - **`UnsupportedOperationException`**: loga uma vez em INFO, marca o **provider** como não suportado (D10), remove a
    série `visible` registrada no `watchDepth` (D5) e nunca mais lê alvos dele;
  - **outro erro, inclusive acesso negado** (D11): cache `NaN` em todos os estados registrados,
    `messaging.depth.reads.failed` +1, WARNING só na primeira falha de uma sequência, e uma linha de INFO quando se
    recupera. O alvo continua sendo lido a cada rodada;
  - alvos com o mesmo endereço são lidos uma vez por rodada (D8). A chave é o provider mais os campos de endereço, sem
    as opções (`require` e afins), que não mudam a fila;
  - nada escapa da tarefa: um erro inesperado é pego, logado e não mata o agendamento.
- `MessagingDestinations.close()` para o executor antes de fechar os receivers.
- `MeteredMessageReceiver` e o `Forgetting` repassam `depth()` sem cronometrar.

### 6. Starter, inbox (`com.example.messaging.inbox.spring`)

- `InboxDestinations` ganha `default void watchDepth(String name, String provider, Endpoint endpoint) { }`. O bean
  anônimo do `InboxAutoConfiguration` delega ao `MessagingDestinations`; o wrapper `lazy` repassa.
- `ConsumerBuilder.build` registra, para cada consumer: `c.name()` com `c.endpoint()`; `<c>.dead-letter` com
  `c.deadLetter()` e `<c>.failed` com `c.failed()`; e `<c>.retry.<i>` com o destino de cada banda. Só registra o que
  `validate(..., receive=true)` aceita: um endereço que não se lê (a DLQ como tópico no Pub/Sub) fica de fora, como o nível
  3 já faz. Os nomes são **os mesmos** que os receivers e senders reais usam, para as séries se cruzarem. O registro é a última
  coisa de um `build` bem-sucedido (D9).

### 7. Métricas

| Meter | Prometheus | Tipo | Tags |
|---|---|---|---|
| `messaging.queue.depth` | `messaging_queue_depth` | gauge | `provider` (o tipo resolvido, D12), `destination`, `state` (`visible`, `in_flight`, `delayed`) |
| `messaging.depth.reads.failed` | `messaging_depth_reads_failed_total` | counter | `provider`, `destination`, `exception` |

O gauge guarda referência forte ao cache. As duas entram nos testes-guarda de nome (`MeterNamesTest`).

### 8. Documentação

`docs/metricas.md` (índice, seção do transporte, armadilhas, alertas), ADR-0008 da lib, README, e
`docs/metricas-backlog.md` (M16 feito para SQS e RabbitMQ; os outros brokers continuam, com o que cada um exige).
Documentar: as propriedades, que o gauge é estimativa, que o RabbitMQ não tem `in_flight`, que a série some quando o
broker não suporta, e a regra de agregação do D6.

## Testes

| Nível | O quê | Roda aqui? |
|---|---|---|
| Unidade, SQS | `depth()` com um `SqsClient` falso: conversão dos três atributos, atributo ausente, erro mapeado | sim |
| Unidade, poller | com receivers falsos: sucesso, `Unsupported` (sonda única por provider, sem série, sem receiver para os outros alvos), erro e acesso negado (`NaN` persistente e contador a cada rodada), recuperação, `NaN` antes da primeira leitura, mesmo endereço sob dois nomes (uma leitura, duas séries), desligado (nenhuma thread), mesmo nome com endereço diferente | sim |
| Unidade, config | intervalo abaixo do mínimo; chave desconhecida em `messaging.metrics`; padrão desligado | sim |
| Guard | `MeterNamesTest` com os dois meters novos | sim |
| Inbox | `ConsumerBuilder` registra os 4 tipos de alvo com os nomes certos e pula o que não se lê | sim |
| Contrato | `MessagingContract.depth` contra InMemory, LocalStack (SQS) e RabbitMQ | Docker existe nesta máquina; as imagens podem precisar ser baixadas |

## O que fica para quem constrói

Nomes das classes e pacotes novos (`QueueDepth`, `QueueDepthPoller`, `QueueDepthSettings` são sugestões); o mapeamento
exato de visível, com lease e adiado no `InMemoryMessaging`; os prazos de espera dos testes; a mensagem de erro dos
validadores; se o poller usa `Clock` injetável para o teste (recomendado).

## Pendência do dono

**P1. As filas só de envio no SQS precisam de `sqs:GetQueueAttributes`.** Um app que só envia para o retry, a
`failed` ou a DLQ tem hoje apenas `sqs:SendMessage` nelas. Para medir a profundidade, a permissão nova entra nas filas
que o app **não** lê. Isso vai para o IAM (no engine-tax, o Terraform) e é de quem opera a infraestrutura.

Recomendação: **aceitar e documentar**. A feature é opt-in, então quem a liga concede a permissão; sem ela, a série
fica `NaN` e o contador de falhas mostra a causa (D11), em vez de a fila cheia passar despercebida. As filas que o app
lê não mudam. A alternativa (medir só o que o app já lê) tira a `failed` do caso comum, e a `failed` é a fila que mais
interessa medir.

## Fora do escopo

Registrado em `docs/metricas-backlog.md` para a rodada seguinte, com o que cada broker exige:

| Broker | Como seria | Obstáculo |
|---|---|---|
| Service Bus | `ServiceBusAdministrationClient.getQueueRuntimeProperties` (ativas, DLQ, agendadas) | Exige o papel *Manage* ou *Data Owner*, e o receiver hoje só precisa de *Data Receiver*: é escalada de permissão |
| Pub/Sub | Cloud Monitoring, métrica `subscription/num_undelivered_messages` | Dependência nova (`google-cloud-monitoring`), permissão nova (`monitoring.timeSeries.list`) e 1 a 3 minutos de atraso |
| JMS Artemis | API de gerenciamento pelo endereço `activemq.management` | Específico do dialeto e exige permissão de gerenciamento |
| JMS Classic | Plugin de estatísticas do broker, ou `QueueBrowser` | O plugin precisa estar ligado; o `QueueBrowser` é O(n) |
